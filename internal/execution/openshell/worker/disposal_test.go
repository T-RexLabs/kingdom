package worker

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"testing"

	native "github.com/NVIDIA/OpenShell/sdk/go/proto/openshellv1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

type disposalNativeFixture struct {
	*nativeFixture
	absent, replacement bool
	deletes             int
}

func (n *disposalNativeFixture) GetSandbox(ctx context.Context, r *native.GetSandboxRequest, o ...grpc.CallOption) (*native.SandboxResponse, error) {
	if n.absent {
		return nil, status.Error(codes.NotFound, "absent")
	}
	v, e := n.nativeFixture.GetSandbox(ctx, r, o...)
	if e == nil && n.replacement {
		v.Sandbox.Metadata.Id = "replacement"
	}
	return v, e
}
func (n *disposalNativeFixture) DeleteSandbox(context.Context, *native.DeleteSandboxRequest, ...grpc.CallOption) (*native.DeleteSandboxResponse, error) {
	n.deletes++
	n.absent = true
	return &native.DeleteSandboxResponse{}, nil
}

type disposalWire struct {
	pb.ManagedExecutionsClient
	archive *pb.RuntimeArchive
	calls   int
}

func (w *disposalWire) FinalizeRuntime(context.Context, *pb.FinalizeRuntimeRequest, ...grpc.CallOption) (*pb.RuntimeArchive, error) {
	w.calls++
	return w.archive, nil
}
func disposalFixture(t *testing.T) (*Disposer, remote.Reservation, *disposalNativeFixture, *disposalGate) {
	t.Helper()
	w, p, n := fixture(t)
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
	native := &disposalNativeFixture{nativeFixture: n}
	w.native = native
	gate := &disposalGate{}
	d, e := NewDisposer(w, gate)
	if e != nil {
		t.Fatal(e)
	}
	return d, r, native, gate
}
func TestDisposalRejectsObservedReplacementBeforeNativeDelete(t *testing.T) {
	d, r, n, _ := disposalFixture(t)
	n.replacement = true
	if e := d.Dispose(context.Background(), r); !errors.Is(e, remote.ErrConflict) {
		t.Fatal(e)
	}
	if n.deletes != 0 {
		t.Fatal("foreign runtime deleted")
	}
	if e := remote.RequireRuntimeDisposed(context.Background(), d.worker.db, r.Owner, r.Reference); e == nil {
		t.Fatal("replacement certified absent")
	}
}
func TestDisposalRequiresStableObservationAndPrerequisites(t *testing.T) {
	for _, mode := range []string{"absent", "capability"} {
		t.Run(mode, func(t *testing.T) {
			d, r, n, g := disposalFixture(t)
			n.absent = mode == "absent"
			g.deny = mode == "capability"
			if e := d.Dispose(context.Background(), r); e == nil {
				t.Fatal("missing evidence disposed runtime")
			}
			if n.deletes != 0 {
				t.Fatal("deleted without prerequisites")
			}
			if e := remote.RequireRuntimeDisposed(context.Background(), d.worker.db, r.Owner, r.Reference); e == nil {
				t.Fatal("absence without stable identity")
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
	if n.deletes != 1 || d.worker.wire.(*disposalWire).calls != 1 {
		t.Fatal("replay mutated deleted runtime")
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
