package remote

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"google.golang.org/protobuf/proto"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/filetoolhelper"

	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
)

// PrepareNativeToolTx links one exact canonical native grant to a managed
// helper process. Callers must authorize that grant in this same transaction.
func PrepareNativeToolTx(ctx context.Context, tx *sql.Tx, owner Owner, ref Reference, grantID string, expected managed.Owner, request *pb.StartExecutionRequest) (Operation, error) {
	if _, err := requireOwner(ctx, tx, owner, ref, true); err != nil {
		return Operation{}, err
	}
	if managed.ValidateStart(request) != nil || request.Command.Scope != pb.CommandScope_COMMAND_SCOPE_PROJECT_TOOL {
		return Operation{}, ErrConflict
	}
	var binding, tool, input string
	if err := tx.QueryRowContext(ctx, `SELECT binding_id,tool,input_json FROM provider_native_grants WHERE grant_id=?`, grantID).Scan(&binding, &tool, &input); err != nil {
		return Operation{}, err
	}
	if binding != owner.BindingID {
		return Operation{}, ErrConflict
	}
	job, err := filetoolhelper.DecodeJob(bytes.NewReader(request.Command.Stdin))
	if err != nil || job.Tool != tool {
		return Operation{}, ErrConflict
	}
	canonical, err := providerexecution.CanonicalToolInput(job.Input)
	if err != nil || canonical != input {
		return Operation{}, ErrConflict
	}
	spec, err := filetoolhelper.Command(job)
	if err != nil || !proto.Equal(spec, request.Command) {
		return Operation{}, ErrConflict
	}
	id := request.Execution.RequestId
	digest, err := managed.Digest(request)
	if err != nil {
		return Operation{}, err
	}
	var savedGrant, savedBinding, savedTool string
	var allocation int64
	err = tx.QueryRowContext(ctx, `SELECT grant_id,allocation,binding_id,tool FROM remote_native_tools WHERE operation_id=?`, id).Scan(&savedGrant, &allocation, &savedBinding, &savedTool)
	if err == nil {
		if savedGrant != grantID || allocation != ref.Allocation || savedBinding != binding || savedTool != tool {
			return Operation{}, ErrConflict
		}
		return PrepareManagedExecutionTx(ctx, tx, owner, ref, NativeTool, expected, request)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Operation{}, err
	}
	if err = RequireToolSlotTx(ctx, tx, owner, ref, ""); err != nil {
		return Operation{}, err
	}
	op := Operation{ID: id, Allocation: ref.Allocation, Kind: Execute, PayloadDigest: hex.EncodeToString(digest), Delivery: Prepared}
	if err = insertOperation(ctx, tx, op); err != nil {
		return op, err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO remote_native_tools(operation_id,grant_id,allocation,binding_id,tool) VALUES(?,?,?,?,?)`, id, grantID, ref.Allocation, binding, tool); err != nil {
		return op, err
	}
	return PrepareManagedExecutionTx(ctx, tx, owner, ref, NativeTool, expected, request)
}
