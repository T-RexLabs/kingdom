package worker

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
	"kingdom/internal/execution/remote"
	"testing"
)

func TestClosePreparedTransferNeverDispatches(t *testing.T) {
	w, p, _ := fixture(t)
	ctx := context.Background()
	r, e := w.Reserve(ctx, p, "binding", uuid.NewString())
	if e != nil {
		t.Fatal(e)
	}
	r, o, e := w.Provision(ctx, p, r)
	if e != nil {
		t.Fatal(e)
	}
	id := uuid.NewString()
	request := &pb.StartExecutionRequest{Execution: &pb.ExecutionSelector{Workspace: w.config.Workspace, Sandbox: nativeName(r.Reference), RequestId: id, Runtime: o.Runtime}, Command: &pb.CommandSpec{Argv: []string{"/fixed/helper"}, Workdir: "/workspace", OutputLimitBytes: 1024}}
	e = w.transaction(ctx, func(tx *sql.Tx) error {
		_, e := remote.PrepareManagedExecutionTx(ctx, tx, r.Owner, r.Reference, remote.WorkspaceImport, o, request)
		return e
	})
	if e != nil {
		t.Fatal(e)
	}
	// The default fixture cannot execute commands. Any Start/Get/Cancel attempt
	// would fail this cleanup; a prepared transfer needs only canonical proof.
	if e = w.CloseTransfers(ctx, p, r, o); e != nil {
		t.Fatal(e)
	}
	closure, e := remote.ReadProcessClosure(ctx, w.db, r.Owner, r.Reference, id)
	if e != nil || !closure.NotStarted || closure.Successful {
		t.Fatalf("closure=%+v err=%v", closure, e)
	}
	if e = w.CloseTransfers(ctx, p, r, o); e != nil {
		t.Fatal(e)
	}
}

type cleanupWire struct {
	*managedFixture
	identity *pb.CommandIdentity
	receipt  *pb.TerminalReceipt
	cancels  []string
	absent   bool
}

func (f *cleanupWire) GetExecution(context.Context, *pb.GetExecutionRequest, ...grpc.CallOption) (*pb.ExecutionStatus, error) {
	if f.absent {
		return nil, errors.New("not found")
	}
	if f.receipt != nil {
		return &pb.ExecutionStatus{Identity: f.identity, State: pb.ExecutionState_EXECUTION_STATE_CLOSED, Terminal: f.receipt}, nil
	}
	return &pb.ExecutionStatus{Identity: f.identity, State: pb.ExecutionState_EXECUTION_STATE_RUNNING}, nil
}
func (f *cleanupWire) CancelExecution(_ context.Context, r *pb.CancelExecutionRequest, _ ...grpc.CallOption) (*pb.ExecutionStatus, error) {
	f.cancels = append(f.cancels, r.RequestId)
	empty := sha256.Sum256(nil)
	f.receipt = &pb.TerminalReceipt{SchemaVersion: 1, Identity: f.identity, GroupClosed: true, ProcessClean: false, TerminationRequested: true, ForcedCleanup: true, InputComplete: true, ExitStatus: &pb.TerminalReceipt_ExitCode{ExitCode: 0}, Output: &pb.OutputManifest{Complete: true, Sha256: empty[:]}}
	f.receipt.ReceiptSha256, _ = managed.ReceiptDigest(f.receipt)
	return &pb.ExecutionStatus{Identity: f.identity, State: pb.ExecutionState_EXECUTION_STATE_CLOSED, Terminal: f.receipt}, nil
}
func TestCloseUncertainTransferRetainsForcedClosureAndNeverRestarts(t *testing.T) {
	for _, absent := range []bool{false, true} {
		t.Run(fmt.Sprint(absent), func(t *testing.T) {
			w, p, _ := fixture(t)
			ctx := context.Background()
			r, e := w.Reserve(ctx, p, "binding", uuid.NewString())
			if e != nil {
				t.Fatal(e)
			}
			r, o, e := w.Provision(ctx, p, r)
			if e != nil {
				t.Fatal(e)
			}
			id := uuid.NewString()
			request := &pb.StartExecutionRequest{Execution: &pb.ExecutionSelector{Workspace: w.config.Workspace, Sandbox: nativeName(r.Reference), RequestId: id, Runtime: o.Runtime}, Command: &pb.CommandSpec{Argv: []string{"/fixed/helper"}, Workdir: "/workspace", OutputLimitBytes: 1024}}
			e = w.transaction(ctx, func(tx *sql.Tx) error {
				if _, e := remote.PrepareManagedExecutionTx(ctx, tx, r.Owner, r.Reference, remote.WorkspaceImport, o, request); e != nil {
					return e
				}
				if e := w.requests.SaveManagedTx(ctx, tx, r.Owner, r.Reference, request); e != nil {
					return e
				}
				_, e := w.requests.ClaimManagedDispatchTx(ctx, tx, r.Owner, r.Reference, id)
				return e
			})
			if e != nil {
				t.Fatal(e)
			}
			original, _ := managed.Digest(request)
			effective, _ := managed.Digest(request.Command)
			f := &cleanupWire{managedFixture: &managedFixture{workspace: o.WorkspaceID}, absent: absent, identity: &pb.CommandIdentity{SchemaVersion: 1, GatewayId: o.GatewayID, WorkspaceId: o.WorkspaceID, SubjectId: o.SubjectID, Method: managed.StartMethod, RequestId: id, BoundaryRequestId: id, Runtime: o.Runtime, OriginalRequestSha256: original, EffectiveSpecSha256: effective}}
			w.wire = f
			e = w.CloseTransfers(ctx, p, r, o)
			if absent {
				if e == nil {
					t.Fatal("absence closed uncertain command")
				}
				if _, e = remote.ReadProcessClosure(ctx, w.db, r.Owner, r.Reference, id); e == nil {
					t.Fatal("invented closure")
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			closed, e := remote.ReadProcessClosure(ctx, w.db, r.Owner, r.Reference, id)
			if e != nil || closed.Successful || !closed.ForcedCleanup {
				t.Fatalf("closure=%+v err=%v", closed, e)
			}
			if e = w.CloseTransfers(ctx, p, r, o); e != nil {
				t.Fatal(e)
			}
			if len(f.cancels) != 1 {
				t.Fatal("repeated cancellation after closure")
			}
		})
	}
}
