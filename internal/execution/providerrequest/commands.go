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
	AbortedAt   string
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
	if b.Owner.Kind != OwnerRun {
		return claim, fmt.Errorf("only run requests may claim managed commands")
	}
	input, digest, err := callDigest(b, call)
	if err != nil {
		return claim, err
	}
	err = tx.QueryRowContext(ctx, `SELECT claim_id,identity_digest,command_ordinal,COALESCE(guard_id,''),COALESCE(completed_at,''),COALESCE(aborted_at,'') FROM provider_command_calls WHERE binding_id=? AND message_id=? AND call_id=?`, b.ID, call.MessageID, call.CallID).Scan(&claim.ID, &claim.Digest, &claim.Ordinal, &claim.GuardID, &claim.CompletedAt, &claim.AbortedAt)
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
	b, err := Get(ctx, tx, bindingID)
	if err != nil {
		return err
	}
	if err := requireLocalBinding(ctx, tx, b); err != nil {
		return err
	}
	if recorded.Valid {
		if recorded.String != guardID {
			return fmt.Errorf("conflicting command receipt")
		}
		return nil
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

// AbortCommandTx closes a claim whose launch ended before the guardian released
// the command. The caller establishes that fact from the guardian's own launch
// protocol; a started command can only be completed by its quiescent guardian.
func AbortCommandTx(ctx context.Context, tx *sql.Tx, id, digest string, now time.Time) error {
	if id == "" || digest == "" || now.IsZero() {
		return fmt.Errorf("complete command abort identity is required")
	}
	var actual, bindingID string
	var ordinal int64
	var guard, aborted sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT binding_id,identity_digest,command_ordinal,guard_id,aborted_at FROM provider_command_calls WHERE claim_id=?`, id).Scan(&bindingID, &actual, &ordinal, &guard, &aborted); err != nil {
		return err
	}
	if digest != actual {
		return fmt.Errorf("command abort identity mismatch")
	}
	if guard.Valid {
		return fmt.Errorf("a command with a guardian cannot be aborted")
	}
	b, err := Get(ctx, tx, bindingID)
	if err != nil {
		return err
	}
	if err := requireLocalBinding(ctx, tx, b); err != nil {
		return err
	}
	var guardians int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM provider_process_guards WHERE provider_invocation_id=? AND command_ordinal=?`, b.Owner.InvocationID, ordinal).Scan(&guardians); err != nil {
		return err
	}
	if guardians != 0 {
		return fmt.Errorf("a command whose guardian was acknowledged cannot be aborted")
	}
	if aborted.Valid {
		return nil
	}
	_, err = tx.ExecContext(ctx, `UPDATE provider_command_calls SET aborted_at=? WHERE claim_id=? AND guard_id IS NULL AND aborted_at IS NULL`, now.UTC().Format(time.RFC3339Nano), id)
	return err
}
