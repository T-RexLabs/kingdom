package remote

import (
	"context"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
)

// ProcessClosure describes verified process-group closure, never sandbox
// disposal, credential revocation, or canonical workflow success.
type ProcessClosure struct {
	NotStarted           bool
	OperationID          string
	Purpose              ExecutionPurpose
	TerminationRequested bool
	ForcedCleanup        bool
	InputComplete        bool
	OutputComplete       bool
	Successful           bool
}

// ReadProcessClosure revalidates identity, output, and terminal evidence. A
// missing receipt remains an error even when transport admission succeeded.
func ReadProcessClosure(ctx context.Context, q Reader, owner Owner, ref Reference, id string) (ProcessClosure, error) {
	never, err := ReadExecutionNotStarted(ctx, q, owner, ref, id)
	if err != nil {
		return ProcessClosure{}, err
	}
	if never {
		var purpose ExecutionPurpose
		if err = q.QueryRowContext(ctx, `SELECT purpose FROM managed_executions WHERE operation_id=?`, id).Scan(&purpose); err != nil {
			return ProcessClosure{}, err
		}
		return ProcessClosure{OperationID: id, Purpose: purpose, NotStarted: true}, nil
	}
	identity, purpose, err := ReadManagedIdentity(ctx, q, owner, ref, id)
	if err != nil {
		return ProcessClosure{}, err
	}
	receipt, found, err := ReadManagedReceipt(ctx, q, owner, ref, id)
	if err != nil {
		return ProcessClosure{}, err
	}
	if !found {
		return ProcessClosure{}, ErrConflict
	}
	if receipt.Identity.RequestId != identity.RequestId {
		return ProcessClosure{}, ErrConflict
	}
	return processClosure(id, purpose, receipt), nil
}
func processClosure(id string, purpose ExecutionPurpose, r *pb.TerminalReceipt) ProcessClosure {
	return ProcessClosure{OperationID: id, Purpose: purpose, TerminationRequested: r.TerminationRequested, ForcedCleanup: r.ForcedCleanup, InputComplete: r.InputComplete, OutputComplete: r.Output.Complete, Successful: managed.SuccessfulProcess(r)}
}

// ReadWriterClosures requires fenced admission and verifies every process that
// could write the workspace. A prepared or uncertain launch is still debt.
// An export is excluded only when its exact retained purpose is workspace_export.
// This does not authorize export: caller-owned credential and native tool debts
// must also be closed by their canonical owner.
func ReadWriterClosures(ctx context.Context, q Reader, owner Owner, ref Reference) ([]ProcessClosure, error) {
	r, err := requireOwner(ctx, q, owner, ref, true)
	if err != nil {
		return nil, err
	}
	if !r.AdmissionClosed {
		return nil, ErrAdmissionClosed
	}
	rows, err := q.QueryContext(ctx, `SELECT o.operation_id,COALESCE(m.purpose,'') FROM remote_operations o LEFT JOIN managed_executions m ON m.operation_id=o.operation_id WHERE o.allocation=? AND o.kind='execute' ORDER BY o.operation_id`, ref.Allocation)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		var purpose ExecutionPurpose
		if err = rows.Scan(&id, &purpose); err != nil {
			rows.Close()
			return nil, err
		}
		if purpose != WorkspaceExport && purpose != NativeReceipts {
			ids = append(ids, id)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	result := make([]ProcessClosure, 0, len(ids))
	for _, id := range ids {
		closed, e := ReadProcessClosure(ctx, q, owner, ref, id)
		if e != nil {
			return nil, e
		}
		if closed.Purpose != ProviderServer && closed.Purpose != Tool && closed.Purpose != NativeTool && closed.Purpose != WorkspaceImport {
			return nil, ErrConflict
		}
		result = append(result, closed)
	}
	return result, nil
}
