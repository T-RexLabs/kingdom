package worker

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
	"kingdom/internal/execution/remote"
	"kingdom/internal/testutil/remoteledger"
)

type disposalGate struct{ deny bool }

func (g *disposalGate) RequireClosed(context.Context, remote.Owner, remote.Reference) error {
	if g.deny {
		return errors.New("capability or workspace open")
	}
	return nil
}

type disposalWire struct {
	pb.ManagedExecutionsClient
	archive          *pb.RuntimeArchive
	calls, deletes   int
	pending, foreign bool
}

func (w *disposalWire) FinalizeRuntime(context.Context, *pb.FinalizeRuntimeRequest, ...grpc.CallOption) (*pb.RuntimeArchive, error) {
	w.calls++
	return w.archive, nil
}
func (w *disposalWire) DeleteFinalizedRuntime(_ context.Context, req *pb.DeleteFinalizedRuntimeRequest, _ ...grpc.CallOption) (*pb.DeleteFinalizedRuntimeResponse, error) {
	w.deletes++
	if req.ArchiveId != w.archive.ArchiveId || !proto.Equal(req.Runtime, w.archive.Manifest.Runtime) {
		return nil, errors.New("wrong runtime")
	}
	if w.pending {
		return &pb.DeleteFinalizedRuntimeResponse{State: pb.RuntimeDeletionState_RUNTIME_DELETION_STATE_PENDING}, nil
	}
	identity := w.archive.Manifest.Commands[0].Identity
	receipt := &pb.RuntimeDeletionReceipt{SchemaVersion: 1, GatewayId: identity.GatewayId, WorkspaceId: identity.WorkspaceId, SubjectId: identity.SubjectId, Runtime: proto.Clone(identity.Runtime).(*pb.RuntimeBinding), ArchiveId: w.archive.ArchiveId}
	if w.foreign {
		receipt.Runtime.SandboxId = "replacement"
	}
	receipt.ReceiptSha256, _ = managed.DeletionDigest(receipt)
	return &pb.DeleteFinalizedRuntimeResponse{State: pb.RuntimeDeletionState_RUNTIME_DELETION_STATE_ABSENT, Receipt: receipt}, nil
}
func disposalFixture(t *testing.T) (*Disposer, remote.Reservation, *disposalWire, *disposalGate) {
	t.Helper()
	w, p, _ := fixture(t)
	ctx := context.Background()
	r, e := w.Reserve(ctx, p, "binding", uuid.NewString())
	if e != nil {
		t.Fatal(e)
	}
	r, owner, e := w.Provision(ctx, p, r)
	if e != nil {
		t.Fatal(e)
	}
	remoteledger.SeedImport(t, w.db, r.Reference.Allocation, owner)
	if e = w.transaction(ctx, func(tx *sql.Tx) error { return remote.CloseAdmissionTx(ctx, tx, r.Owner, r.Reference) }); e != nil {
		t.Fatal(e)
	}
	_, ids, e := remote.DisposalInventory(ctx, w.db, r.Owner, r.Reference)
	if e != nil {
		t.Fatal(e)
	}
	m := &pb.RuntimeArchiveManifest{SchemaVersion: 1, GatewayId: owner.GatewayID, WorkspaceId: owner.WorkspaceID, Runtime: owner.Runtime}
	for _, id := range ids {
		receipt, _, e := remote.ReadManagedReceipt(ctx, w.db, r.Owner, r.Reference, id.RequestId)
		if e != nil {
			t.Fatal(e)
		}
		m.Commands = append(m.Commands, &pb.ArchivedCommand{Identity: id, ReceiptSha256: receipt.ReceiptSha256, Output: receipt.Output})
	}
	digest, _ := managed.ArchiveDigest(m)
	w.wire = &disposalWire{archive: &pb.RuntimeArchive{ArchiveId: hex.EncodeToString(digest), ManifestSha256: digest, Manifest: m}}
	wire := w.wire.(*disposalWire)
	gate := &disposalGate{}
	d, e := NewDisposer(w, gate)
	if e != nil {
		t.Fatal(e)
	}
	return d, r, wire, gate
}
func TestDisposalRejectsForeignTypedDeletion(t *testing.T) {
	d, r, w, _ := disposalFixture(t)
	w.foreign = true
	if e := d.Dispose(context.Background(), r); e == nil {
		t.Fatal("foreign runtime proof accepted")
	}
	if e := remote.RequireRuntimeDisposed(context.Background(), d.worker.db, r.Owner, r.Reference); e == nil {
		t.Fatal("replacement certified absent")
	}
}
func TestDisposalRequiresCompletedDeletionAndPrerequisites(t *testing.T) {
	for _, mode := range []string{"pending", "capability"} {
		t.Run(mode, func(t *testing.T) {
			d, r, w, g := disposalFixture(t)
			w.pending = mode == "pending"
			g.deny = mode == "capability"
			if e := d.Dispose(context.Background(), r); e == nil {
				t.Fatal("missing evidence disposed runtime")
			}
			if mode == "capability" && w.deletes != 0 {
				t.Fatal("deleted without prerequisites")
			}
			if e := remote.RequireRuntimeDisposed(context.Background(), d.worker.db, r.Owner, r.Reference); e == nil {
				t.Fatal("pending deletion became absence")
			}
		})
	}
}
func TestDisposalRetainsArchiveAndAbsenceAcrossReplay(t *testing.T) {
	d, r, n, _ := disposalFixture(t)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if e := d.Dispose(ctx, r); e != nil {
			t.Fatal(e)
		}
		if e := remote.RequireRuntimeDisposed(ctx, d.worker.db, r.Owner, r.Reference); e != nil {
			t.Fatal(e)
		}
	}
	var sequence int
	var databaseName, databasePath string
	if e := d.worker.db.QueryRow(`PRAGMA database_list`).Scan(&sequence, &databaseName, &databasePath); e != nil {
		t.Fatal(e)
	}
	reopened, e := sql.Open("sqlite", databasePath)
	if e != nil {
		t.Fatal(e)
	}
	defer reopened.Close()
	if e = remote.RequireRuntimeDisposed(ctx, reopened, r.Owner, r.Reference); e != nil {
		t.Fatal("restart lost evidence", e)
	}
	if n.deletes != 2 || d.worker.wire.(*disposalWire).calls != 1 {
		t.Fatal("replay did not use exact archived deletion")
	}
	if _, e := d.worker.db.Exec(`DROP TRIGGER remote_archive_immutable`); e != nil {
		t.Fatal(e)
	}
	if _, e := d.worker.db.Exec(`UPDATE remote_runtime_archives SET archive=x'00'`); e != nil {
		t.Fatal(e)
	}
	if e := remote.RequireRuntimeDisposed(ctx, d.worker.db, r.Owner, r.Reference); e == nil {
		t.Fatal("corrupt archive accepted as disposal")
	}
}
