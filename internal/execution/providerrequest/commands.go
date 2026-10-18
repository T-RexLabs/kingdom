package providerrequest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

type CommandClaim struct {
	Grant
	Ordinal     int64
	GuardID     string
	CompletedAt string
}

func ClaimCommandTx(ctx context.Context, tx *sql.Tx, call NativeCall, now time.Time) (CommandClaim, error) {
	var claim CommandClaim
	if call.Tool != "shell" {
		return claim, fmt.Errorf("unsupported managed command tool")
	}
	b, err := Get(ctx, tx, call.BindingID)
	if err != nil {
		return claim, err
	}
	input, digest, err := callDigest(b, call)
	if err != nil {
		return claim, err
	}
	err = tx.QueryRowContext(ctx, `SELECT claim_id,identity_digest,command_ordinal,COALESCE(guard_id,''),COALESCE(completed_at,'') FROM provider_command_calls WHERE binding_id=? AND message_id=? AND call_id=?`, b.ID, call.MessageID, call.CallID).Scan(&claim.ID, &claim.Digest, &claim.Ordinal, &claim.GuardID, &claim.CompletedAt)
	if err == nil {
		if claim.Digest != digest {
			return CommandClaim{}, fmt.Errorf("conflicting command replay")
		}
		return claim, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return claim, err
	}
	var other int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM provider_native_grants WHERE binding_id=? AND message_id=? AND call_id=?`, b.ID, call.MessageID, call.CallID).Scan(&other); err != nil {
		return claim, err
	}
	if other != 0 {
		return claim, fmt.Errorf("tool call already belongs to native execution")
	}
	if err := requireAdmission(ctx, tx, b, now); err != nil {
		return claim, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(command_ordinal),0)+1 FROM provider_command_calls WHERE binding_id=?`, b.ID).Scan(&claim.Ordinal); err != nil {
		return claim, err
	}
	claim.Grant = Grant{ID: "command_" + digest, Digest: digest, Execute: true}
	_, err = tx.ExecContext(ctx, `INSERT INTO provider_command_calls(claim_id,binding_id,message_id,call_id,tool,input_json,identity_digest,command_ordinal,created_at) VALUES(?,?,?,?,?,?,?,?,?)`, claim.ID, b.ID, call.MessageID, call.CallID, call.Tool, input, digest, claim.Ordinal, now.UTC().Format(time.RFC3339Nano))
	return claim, err
}

// CompleteCommandTx accepts only the canonical guardian record for this exact
// claimed command. Caller-supplied process-complete flags have no authority.
func CompleteCommandTx(ctx context.Context, tx *sql.Tx, id, digest, guardID string, now time.Time) error {
	if id == "" || digest == "" || guardID == "" || now.IsZero() {
		return fmt.Errorf("complete command receipt identity is required")
	}
	var bindingID, actual string
	var ordinal int64
	var recorded sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT binding_id,identity_digest,command_ordinal,guard_id FROM provider_command_calls WHERE claim_id=?`, id).Scan(&bindingID, &actual, &ordinal, &recorded); err != nil {
		return err
	}
	if digest != actual {
		return fmt.Errorf("command receipt identity mismatch")
	}
	if recorded.Valid {
		if recorded.String != guardID {
			return fmt.Errorf("conflicting command receipt")
		}
		return nil
	}
	b, err := Get(ctx, tx, bindingID)
	if err != nil {
		return err
	}
	owner, err := loadOwner(ctx, tx, b.Owner)
	if err != nil {
		return err
	}
	lineage, args := guardLineage(b, owner)
	args = append([]any{guardID, ordinal}, args...)
	var matching int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM provider_process_guards g WHERE g.guard_id=? AND g.command_ordinal=? AND `+lineage+` AND g.lifecycle='quiescent' AND g.quiescent_at IS NOT NULL AND g.quiescence_evidence_sha256<>''`, args...).Scan(&matching); err != nil {
		return err
	}
	if matching != 1 {
		return fmt.Errorf("command requires exact canonical guardian quiescence")
	}
	_, err = tx.ExecContext(ctx, `UPDATE provider_command_calls SET guard_id=?,completed_at=? WHERE claim_id=? AND guard_id IS NULL`, guardID, now.UTC().Format(time.RFC3339Nano), id)
	return err
}
