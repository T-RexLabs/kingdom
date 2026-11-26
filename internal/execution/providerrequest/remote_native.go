package providerrequest

import (
	"bytes"
	"context"
	"database/sql"
	"time"

	"kingdom/internal/execution/openshell/managed"
	"kingdom/internal/execution/remote"
	"kingdom/internal/filetoolhelper"
)

// CompleteRemoteNativeTx derives the outcome from retained exact helper output
// and process proof, never a callback-provided started/outcome assertion.
func CompleteRemoteNativeTx(ctx context.Context, tx *sql.Tx, grantID, digest, operationID string, now time.Time) (filetoolhelper.Result, error) {
	var result filetoolhelper.Result
	var binding, actual, tool, linkedGrant, linkedBinding string
	var allocation int64
	if err := tx.QueryRowContext(ctx, `SELECT binding_id,identity_digest,tool FROM provider_native_grants WHERE grant_id=?`, grantID).Scan(&binding, &actual, &tool); err != nil {
		return result, err
	}
	if digest != actual || now.IsZero() {
		return result, remote.ErrConflict
	}
	b, err := Get(ctx, tx, binding)
	if err != nil {
		return result, err
	}
	r, err := remoteBindingOwner(ctx, tx, b)
	if err != nil {
		return result, err
	}
	if err = tx.QueryRowContext(ctx, `SELECT grant_id,binding_id,allocation FROM remote_native_tools WHERE operation_id=?`, operationID).Scan(&linkedGrant, &linkedBinding, &allocation); err != nil {
		return result, err
	}
	if linkedGrant != grantID || linkedBinding != binding || allocation != r.Reference.Allocation {
		return result, remote.ErrConflict
	}
	closure, err := remote.ReadProcessClosure(ctx, tx, r.Owner, r.Reference, operationID)
	if err != nil || closure.Purpose != remote.NativeTool {
		return result, remote.ErrConflict
	}
	started := !closure.NotStarted
	outcome, effect := "aborted", "none"
	if started {
		receipt, ok, err := remote.ReadManagedReceipt(ctx, tx, r.Owner, r.Reference, operationID)
		if err != nil || !ok {
			return result, remote.ErrConflict
		}
		outcome = "failed"
		effect = "unknown"
		if managed.SuccessfulProcess(receipt) {
			raw, err := remote.ReadManagedStdout(ctx, tx, r.Owner, r.Reference, operationID, filetoolhelper.MaxResultBytes)
			if err != nil {
				return result, err
			}
			decoded, err := filetoolhelper.DecodeResult(bytes.NewReader(raw), tool)
			if err == nil {
				result = decoded
				if decoded.OK {
					outcome = "completed"
					effect = "complete"
				} else {
					effect = decoded.Error.Effect
				}
			}
		}
	}
	if result.Tool == "" {
		result = filetoolhelper.Result{Version: 1, Tool: tool, OK: false, Error: &filetoolhelper.Failure{Code: "execution_unverified", Message: "File operation did not complete with verified output.", Effect: effect}}
	}
	var savedID, savedEffect string
	err = tx.QueryRowContext(ctx, `SELECT operation_id,effect FROM provider_remote_native_completions WHERE grant_id=?`, grantID).Scan(&savedID, &savedEffect)
	if err == nil {
		if savedID != operationID || savedEffect != effect {
			return result, remote.ErrConflict
		}
	} else if err == sql.ErrNoRows {
		if _, err = tx.ExecContext(ctx, `INSERT INTO provider_remote_native_completions(grant_id,operation_id,effect,completed_at) VALUES(?,?,?,?)`, grantID, operationID, effect, now.UTC().Format(time.RFC3339Nano)); err != nil {
			return result, err
		}
	} else {
		return result, err
	}
	if err = finishNativeTx(ctx, tx, NativeReceipt{GrantID: grantID, Digest: digest, Outcome: outcome, Started: &started}, now); err != nil {
		return result, err
	}
	return result, nil
}
