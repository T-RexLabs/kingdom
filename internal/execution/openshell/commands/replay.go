package commands

import (
	"context"
	"database/sql"

	"google.golang.org/protobuf/proto"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
	"kingdom/internal/providers/opencode"
)

// Replay returns only retained, revalidated bytes. It needs no live sandbox.
func (e *Executor) replay(ctx context.Context, in opencode.ManagedCommandInput, spec *pb.CommandSpec) (result opencode.ManagedCommandResult, found bool, err error) {
	err = e.transaction(ctx, func(tx *sql.Tx) error {
		b, r, err := providerrequest.ActiveRemoteCommandTx(ctx, tx, in.Identity, e.config.Now())
		if err != nil {
			return err
		}
		var id string
		err = tx.QueryRowContext(ctx, `SELECT x.operation_id FROM provider_command_calls c JOIN provider_remote_command_completions x ON x.claim_id=c.claim_id WHERE c.binding_id=? AND c.message_id=? AND c.call_id=?`, b.ID, in.MessageID, in.CallID).Scan(&id)
		if err == sql.ErrNoRows {
			return nil
		}
		if err != nil {
			return err
		}
		claim, err := providerrequest.ClaimCommandTx(ctx, tx, providerrequest.NativeCall{BindingID: b.ID, Identity: in.Identity, MessageID: in.MessageID, CallID: in.CallID, Tool: "shell", Input: in.ToolInput}, e.config.Now())
		if err != nil {
			return err
		}
		request, err := e.requests.ReadManaged(ctx, tx, r.Owner, r.Reference, id)
		if err != nil || !proto.Equal(request.GetCommand(), spec) {
			return ErrDenied
		}
		if err = providerrequest.CompleteRemoteCommandTx(ctx, tx, claim.ID, claim.Digest, id, e.config.Now()); err != nil {
			return err
		}
		receipt, ok, err := remote.ReadManagedReceipt(ctx, tx, r.Owner, r.Reference, id)
		if err != nil || !ok {
			return ErrUnresolved
		}
		rows, err := tx.QueryContext(ctx, `SELECT chunk FROM managed_execution_output WHERE operation_id=? ORDER BY sequence`, id)
		if err != nil {
			return err
		}
		defer rows.Close()
		var output []byte
		for rows.Next() {
			var raw []byte
			if err = rows.Scan(&raw); err != nil {
				return err
			}
			chunk := &pb.OutputChunk{}
			if proto.Unmarshal(raw, chunk) != nil || len(output)+len(chunk.Data) > managed.MaxOutputBytes {
				return ErrUnresolved
			}
			output = append(output, chunk.Data...)
		}
		if err = rows.Err(); err != nil {
			return err
		}
		result = commandResult(id, receipt, output)
		found = true
		return nil
	})
	return
}
func commandResult(id string, receipt *pb.TerminalReceipt, output []byte) opencode.ManagedCommandResult {
	result := opencode.ManagedCommandResult{ExecutionID: id, Output: string(output), Closed: true, Truncated: !receipt.Output.Complete}
	if exit, ok := receipt.ExitStatus.(*pb.TerminalReceipt_ExitCode); ok {
		result.Exit = int(exit.ExitCode)
	} else {
		result.Exit = 1
	}
	if receipt.TerminationRequested || receipt.ForcedCleanup || !receipt.InputComplete || !receipt.Output.Complete {
		if result.Exit == 0 {
			result.Exit = 1
		}
	}
	return result
}
