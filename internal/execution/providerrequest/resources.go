package providerrequest

import (
	"context"
	"fmt"
)

// RequireResourcesClosedTx is intentionally independent of terminal response
// validity. Native receipts and command ownership remain debts on cancellation.
func RequireResourcesClosedTx(ctx context.Context, q Reader, id string) error {
	b, err := Get(ctx, q, id)
	if err != nil {
		return err
	}
	if b.AdmissionClosedAt == "" {
		return fmt.Errorf("%w: admission is open", ErrResourcesOpen)
	}
	var pending int
	if err := q.QueryRowContext(ctx, `SELECT count(*) FROM provider_native_grants WHERE binding_id=? AND outcome IS NULL`, id).Scan(&pending); err != nil {
		return err
	}
	if pending != 0 {
		return fmt.Errorf("%w: native receipts are pending", ErrResourcesOpen)
	}
	if err := q.QueryRowContext(ctx, `SELECT count(*) FROM provider_command_calls WHERE binding_id=? AND completed_at IS NULL AND aborted_at IS NULL`, id).Scan(&pending); err != nil {
		return err
	}
	if pending != 0 {
		return fmt.Errorf("%w: command claims are pending", ErrResourcesOpen)
	}
	owner, err := loadOwner(ctx, q, b.Owner)
	if err != nil {
		return err
	}
	lineage, args := guardLineage(b, owner)
	queryArgs := append([]any{b.Owner.InvocationID}, args...)
	if err := q.QueryRowContext(ctx, `SELECT count(*) FROM provider_process_guards g WHERE g.provider_invocation_id=? AND NOT (`+lineage+` AND g.lifecycle='quiescent' AND g.quiescent_at IS NOT NULL AND g.quiescence_evidence_sha256<>'')`, queryArgs...).Scan(&pending); err != nil {
		return err
	}
	if pending != 0 {
		return fmt.Errorf("%w: command guardians are unresolved or mismatched", ErrResourcesOpen)
	}
	queryArgs = append([]any{id}, args...)
	if err := q.QueryRowContext(ctx, `SELECT count(*) FROM provider_command_calls c LEFT JOIN provider_process_guards g ON g.guard_id=c.guard_id WHERE c.binding_id=? AND c.aborted_at IS NULL AND (g.guard_id IS NULL OR NOT (`+lineage+` AND g.command_ordinal=c.command_ordinal AND g.lifecycle='quiescent' AND g.quiescent_at IS NOT NULL AND g.quiescence_evidence_sha256<>''))`, queryArgs...).Scan(&pending); err != nil {
		return err
	}
	if pending != 0 {
		return fmt.Errorf("%w: command completion lacks exact guardian closure", ErrResourcesOpen)
	}
	if err := q.QueryRowContext(ctx, `SELECT count(*) FROM provider_process_guards g WHERE g.provider_invocation_id=? AND NOT EXISTS (SELECT 1 FROM provider_command_calls c WHERE c.binding_id=? AND c.guard_id=g.guard_id AND c.command_ordinal=g.command_ordinal)`, b.Owner.InvocationID, id).Scan(&pending); err != nil {
		return err
	}
	if pending != 0 {
		return fmt.Errorf("%w: guardian has no exact command claim", ErrResourcesOpen)
	}
	return nil
}
