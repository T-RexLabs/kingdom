package remote

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	fixtureledger "kingdom/internal/testutil/remoteledger"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
)

func managedRequest(t *testing.T, r Reservation) (managed.Owner, *pb.StartExecutionRequest, *pb.CommandIdentity) {
	t.Helper()
	runtime := &pb.RuntimeBinding{SandboxId: r.Reference.SandboxID, BoundaryGeneration: r.Reference.Generation, MainProcessInstanceId: r.Reference.MainInstanceID, DeploymentProfileSha256: bytes.Repeat([]byte{1}, 32)}
	owner := managed.Owner{GatewayID: r.Reference.GatewayID, WorkspaceID: r.Reference.WorkspaceID, SubjectID: "native-worker", Runtime: runtime}
	request := &pb.StartExecutionRequest{Execution: &pb.ExecutionSelector{Workspace: "workspace", Sandbox: "allocated-name", RequestId: uuid.NewString(), Runtime: runtime}, Command: &pb.CommandSpec{Argv: []string{"/bin/server"}, Workdir: "/sandbox", Environment: map[string]string{"MODEL_TOKEN": "private-fixture-secret"}, OutputLimitBytes: 16}}
	original, _ := managed.Digest(request)
	effective, _ := managed.Digest(request.Command)
	identity := &pb.CommandIdentity{SchemaVersion: 1, GatewayId: owner.GatewayID, WorkspaceId: owner.WorkspaceID, SubjectId: owner.SubjectID, Method: managed.StartMethod, RequestId: request.Execution.RequestId, OriginalRequestSha256: original, EffectiveSpecSha256: effective, BoundaryRequestId: request.Execution.RequestId, Runtime: runtime}
	return owner, request, identity
}
func prepareManaged(t *testing.T, db *sql.DB, r Reservation, purpose ExecutionPurpose, expected managed.Owner, request *pb.StartExecutionRequest) {
	t.Helper()
	if purpose == ProviderServer {
		fixtureledger.SeedImport(t, db, r.Reference.Allocation, expected)
	}
	transaction(t, db, func(tx *sql.Tx) error {
		_, e := PrepareManagedExecutionTx(ctx, tx, r.Owner, r.Reference, purpose, expected, request)
		return e
	})
}
func dispatchManaged(t *testing.T, db *sql.DB, r Reservation, id string) {
	t.Helper()
	transaction(t, db, func(tx *sql.Tx) error {
		sent, e := ClaimDispatchTx(ctx, tx, r.Owner, r.Reference, id)
		if e == nil && !sent {
			t.Fatal("not dispatched")
		}
		return e
	})
}
func managedReceipt(identity *pb.CommandIdentity, chunks ...*pb.OutputChunk) *pb.TerminalReceipt {
	h := sha256.New()
	var total uint64
	for _, c := range chunks {
		b, _ := managed.Canonical(c)
		var prefix [4]byte
		binary.BigEndian.PutUint32(prefix[:], uint32(len(b)))
		h.Write(prefix[:])
		h.Write(b)
		total += uint64(len(c.Data))
	}
	r := &pb.TerminalReceipt{SchemaVersion: 1, Identity: identity, ExitStatus: &pb.TerminalReceipt_ExitCode{}, GroupClosed: true, ProcessClean: true, InputComplete: true, Output: &pb.OutputManifest{LastSequence: uint64(len(chunks)), TotalBytes: total, Sha256: h.Sum(nil), Complete: true}}
	r.ReceiptSha256, _ = managed.ReceiptDigest(r)
	return r
}
func TestManagedEvidenceRequiresExactDispatchAndDurableOutput(t *testing.T) {
	db := openLedger(t, filepath.Join(t.TempDir(), "evidence.db"))
	r := bound(t, db)
	expected, request, identity := managedRequest(t, r)
	prepareManaged(t, db, r, ProviderServer, expected, request)
	reject(t, db, ErrConflict, func(tx *sql.Tx) error { return RecordManagedIdentityTx(ctx, tx, r.Owner, r.Reference, identity) })
	dispatchManaged(t, db, r, identity.RequestId)
	transaction(t, db, func(tx *sql.Tx) error { return RecordManagedIdentityTx(ctx, tx, r.Owner, r.Reference, identity) })
	changed := proto.Clone(identity).(*pb.CommandIdentity)
	changed.Runtime.BoundaryGeneration = "replacement"
	reject(t, db, ErrConflict, func(tx *sql.Tx) error { return RecordManagedIdentityTx(ctx, tx, r.Owner, r.Reference, changed) })
	chunk := &pb.OutputChunk{Sequence: 1, Stream: pb.OutputStream_OUTPUT_STREAM_STDOUT, Data: []byte("result")}
	receipt := managedReceipt(identity, chunk)
	reject(t, db, ErrConflict, func(tx *sql.Tx) error {
		return RecordManagedReceiptTx(ctx, tx, r.Owner, r.Reference, identity.RequestId, receipt)
	})
	transaction(t, db, func(tx *sql.Tx) error { return CloseAdmissionTx(ctx, tx, r.Owner, r.Reference) })
	for n := 0; n < 2; n++ {
		transaction(t, db, func(tx *sql.Tx) error {
			return AppendManagedOutputTx(ctx, tx, r.Owner, r.Reference, identity.RequestId, chunk)
		})
	}
	transaction(t, db, func(tx *sql.Tx) error {
		return RecordManagedReceiptTx(ctx, tx, r.Owner, r.Reference, identity.RequestId, receipt)
	})
	transaction(t, db, func(tx *sql.Tx) error {
		return RecordManagedReceiptTx(ctx, tx, r.Owner, r.Reference, identity.RequestId, receipt)
	})
	changedChunk := proto.Clone(chunk).(*pb.OutputChunk)
	changedChunk.Data = []byte("forged")
	reject(t, db, ErrConflict, func(tx *sql.Tx) error {
		return AppendManagedOutputTx(ctx, tx, r.Owner, r.Reference, identity.RequestId, changedChunk)
	})
	chunk2 := &pb.OutputChunk{Sequence: 2, Stream: chunk.Stream, Data: []byte("late")}
	reject(t, db, ErrConflict, func(tx *sql.Tx) error {
		return AppendManagedOutputTx(ctx, tx, r.Owner, r.Reference, identity.RequestId, chunk2)
	})
	for _, table := range []string{"managed_executions", "managed_execution_identities", "managed_execution_output", "managed_execution_receipts"} {
		reject(t, db, nil, func(tx *sql.Tx) error { _, e := tx.Exec(`DELETE FROM ` + table); return e })
	}
	var pin []byte
	if e := db.QueryRow(`SELECT runtime||request_sha256||effective_sha256 FROM managed_executions`).Scan(&pin); e != nil {
		t.Fatal(e)
	}
	if bytes.Contains(pin, []byte("private-fixture-secret")) {
		t.Fatal("credential retained")
	}
}
func TestManagedExportRequiresFencedClosedProcesses(t *testing.T) {
	db := openLedger(t, ":memory:")
	r := bound(t, db)
	expected, start, identity := managedRequest(t, r)
	prepareManaged(t, db, r, ProviderServer, expected, start)
	dispatchManaged(t, db, r, identity.RequestId)
	transaction(t, db, func(tx *sql.Tx) error { return RecordManagedIdentityTx(ctx, tx, r.Owner, r.Reference, identity) })
	_, export, _ := managedRequest(t, r)
	reject(t, db, ErrConflict, func(tx *sql.Tx) error {
		_, e := PrepareManagedExecutionTx(ctx, tx, r.Owner, r.Reference, WorkspaceExport, expected, export)
		return e
	})
	transaction(t, db, func(tx *sql.Tx) error { return CloseAdmissionTx(ctx, tx, r.Owner, r.Reference) })
	reject(t, db, ErrConflict, func(tx *sql.Tx) error {
		_, e := PrepareManagedExecutionTx(ctx, tx, r.Owner, r.Reference, WorkspaceExport, expected, export)
		return e
	})
	receipt := managedReceipt(identity)
	receipt.TerminationRequested = true
	receipt.ProcessClean = false
	receipt.ReceiptSha256, _ = managed.ReceiptDigest(receipt)
	transaction(t, db, func(tx *sql.Tx) error {
		return RecordManagedReceiptTx(ctx, tx, r.Owner, r.Reference, identity.RequestId, receipt)
	})
	prepareManaged(t, db, r, WorkspaceExport, expected, export)
	dispatchManaged(t, db, r, export.Execution.RequestId)
	_, newServer, _ := managedRequest(t, r)
	reject(t, db, ErrAdmissionClosed, func(tx *sql.Tx) error {
		_, e := PrepareManagedExecutionTx(ctx, tx, r.Owner, r.Reference, ProviderServer, expected, newServer)
		return e
	})
}
func TestManagedOutputRejectsGapsAndRequestedLimit(t *testing.T) {
	db := openLedger(t, ":memory:")
	r := bound(t, db)
	expected, request, identity := managedRequest(t, r)
	prepareManaged(t, db, r, ProviderServer, expected, request)
	dispatchManaged(t, db, r, identity.RequestId)
	transaction(t, db, func(tx *sql.Tx) error { return RecordManagedIdentityTx(ctx, tx, r.Owner, r.Reference, identity) })
	for _, c := range []*pb.OutputChunk{{Sequence: 2, Stream: pb.OutputStream_OUTPUT_STREAM_STDOUT, Data: []byte("gap")}, {Sequence: 1, Stream: pb.OutputStream_OUTPUT_STREAM_STDOUT, Data: bytes.Repeat([]byte{1}, 17)}} {
		reject(t, db, ErrConflict, func(tx *sql.Tx) error {
			return AppendManagedOutputTx(ctx, tx, r.Owner, r.Reference, identity.RequestId, c)
		})
	}
}
func TestManagedToolRequiresCanonicalClaim(t *testing.T) {
	db := openLedger(t, ":memory:")
	r := bound(t, db)
	expected, request, identity := managedRequest(t, r)
	reject(t, db, nil, func(tx *sql.Tx) error {
		_, e := PrepareManagedExecutionTx(ctx, tx, r.Owner, r.Reference, Tool, expected, request)
		return e
	})
	transaction(t, db, func(tx *sql.Tx) error {
		_, e := PrepareCommandTx(ctx, tx, r.Owner, CommandReference{Runtime: r.Reference, OwnerBinding: r.Owner.BindingID, ClaimID: uuid.NewString(), CommandID: identity.RequestId, PayloadDigest: hex.EncodeToString(identity.OriginalRequestSha256), Sequence: 1})
		if e != nil {
			return e
		}
		_, e = PrepareManagedExecutionTx(ctx, tx, r.Owner, r.Reference, Tool, expected, request)
		return e
	})
}

func TestManagedPlanningServerAndRestartRetention(t *testing.T) {
	path := filepath.Join(t.TempDir(), "planning-evidence.db")
	db := openLedger(t, path)
	owner := Owner{Kind: "planning", InvocationID: uuid.NewString(), BindingID: uuid.NewString(), LeaseOwner: "planner", LeaseVersion: 1}
	var r Reservation
	transaction(t, db, func(tx *sql.Tx) error {
		var e error
		r, e = ReserveRuntimeTx(ctx, tx, owner, Target{"worker", "gateway", "workspace"}, uuid.NewString(), digest)
		return e
	})
	dispatchManaged(t, db, r, r.Reference.CreationID)
	r.Reference.SandboxID = uuid.NewString()
	r.Reference.Generation = uuid.NewString()
	r.Reference.MainInstanceID = uuid.NewString()
	transaction(t, db, func(tx *sql.Tx) error { return BindRuntimeTx(ctx, tx, owner, r.Reference) })
	expected, request, identity := managedRequest(t, r)
	prepareManaged(t, db, r, ProviderServer, expected, request)
	dispatchManaged(t, db, r, identity.RequestId)
	transaction(t, db, func(tx *sql.Tx) error { return RecordManagedIdentityTx(ctx, tx, owner, r.Reference, identity) })
	if _, found, e := ReadManagedReceipt(ctx, db, owner, r.Reference, identity.RequestId); e != nil || found {
		t.Fatal("fabricated receipt", found, e)
	}
	receipt := managedReceipt(identity)
	receipt.InputComplete = false
	receipt.ReceiptSha256, _ = managed.ReceiptDigest(receipt)
	transaction(t, db, func(tx *sql.Tx) error {
		return RecordManagedReceiptTx(ctx, tx, owner, r.Reference, identity.RequestId, receipt)
	})
	db.Close()
	reopened := openLedger(t, path)
	got, found, e := ReadManagedReceipt(ctx, reopened, owner, r.Reference, identity.RequestId)
	if e != nil || !found || !proto.Equal(got, receipt) || managed.SuccessfulProcess(got) {
		t.Fatal("restart changed closure/input facts", found, e)
	}
}
