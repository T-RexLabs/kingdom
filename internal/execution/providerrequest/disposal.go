package providerrequest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"kingdom/internal/core/providerexecution"
)

// RequireDisposedTx permits releasing an invocation's resources only after its
// own exact canonical settlement. Mutable owner pointers may already refer to a
// later retry; they cannot substitute for this invocation's historical facts.
func RequireDisposedTx(ctx context.Context, q Reader, owner Owner) error {
	if err := RequireOwnerRemoteDisposed(ctx, q, owner); err != nil {
		return err
	}
	b, found, err := ForOwner(ctx, q, owner)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("provider request disposal requires its binding")
	}
	if err := RequireResourcesClosedTx(ctx, q, b.ID); err != nil {
		return err
	}
	return requireBoundSettlement(ctx, q, b)
}

// requireBoundSettlement checks immutable workflow settlement independently of
// remote deletion, which happens after export and may be retried after restart.
func requireBoundSettlement(ctx context.Context, q Reader, b Binding) error {
	owner := b.Owner
	canonical, err := loadOwner(ctx, q, owner)
	if err != nil {
		return err
	}
	if canonical.session != b.Identity.SessionID || canonical.logicalSession != b.LogicalSessionID {
		return fmt.Errorf("provider disposal conflicts with immutable invocation identity")
	}
	if owner.Kind == OwnerRun && (canonical.invocationState == "superseded" || canonical.invocationState == "invalidated") {
		if abandoned, err := isAbandoned(b); err != nil || abandoned {
			return err
		}
	}
	if b.DispatchIssuedAt == "" {
		if owner.Kind != OwnerRun || canonical.invocationState != "invalidated" || b.TerminalReference != nil {
			return fmt.Errorf("%w: undispatched request lacks explicit abandonment", ErrResourcesOpen)
		}
		var claims int
		if err := q.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM provider_native_grants WHERE binding_id=?)+(SELECT count(*) FROM provider_command_calls WHERE binding_id=?)`, b.ID, b.ID).Scan(&claims); err != nil {
			return err
		}
		if claims != 0 {
			return fmt.Errorf("undispatched abandonment has admitted effects")
		}
		return nil
	}
	if b.TerminalReference == nil {
		return fmt.Errorf("%w: provider request requires immutable terminal evidence", ErrResourcesOpen)
	}
	if err := b.TerminalReference.Validate(owner.InvocationID); err != nil {
		return err
	}
	var outcome string
	switch owner.Kind {
	case OwnerRun:
		err = q.QueryRowContext(ctx, `SELECT s.outcome FROM provider_completion_settlements s JOIN provider_completion_evidence e ON e.evidence_id=s.primary_evidence_id WHERE s.provider_invocation_id=? AND s.attempt_id=? AND s.run_id=? AND s.node_run_id=? AND e.provider_invocation_id=s.provider_invocation_id AND e.attempt_id=s.attempt_id AND e.run_id=s.run_id AND e.node_run_id=s.node_run_id AND e.evidence_kind='stream_observation'`, owner.InvocationID, canonical.id, canonical.parent, canonical.node).Scan(&outcome)
	case OwnerPlanning:
		err = q.QueryRowContext(ctx, `SELECT s.outcome FROM planning_provider_completion_settlements s JOIN planning_provider_completion_evidence e ON e.evidence_id=s.primary_evidence_id WHERE s.planning_provider_invocation_id=? AND s.planning_operation_id=? AND s.planning_session_id=? AND e.planning_provider_invocation_id=s.planning_provider_invocation_id AND e.planning_operation_id=s.planning_operation_id AND e.planning_session_id=s.planning_session_id AND e.evidence_kind='stream_observation'`, owner.InvocationID, canonical.id, canonical.parent).Scan(&outcome)
	default:
		return fmt.Errorf("unknown provider owner kind")
	}
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: provider request lacks exact historical settlement", ErrResourcesOpen)
	}
	if err != nil {
		return err
	}
	expected := map[string]string{"success": "settled_success", "failed": "settled_failed", "cancelled": "settled_cancelled"}[outcome]
	if expected == "" || canonical.invocationState != expected {
		return fmt.Errorf("%w: provider request settlement conflicts with its terminal state", ErrResourcesOpen)
	}
	return nil
}

// RequireAttemptDisposedTx checks every bound invocation belonging to a run
// attempt, including historical invocations no longer selected by that attempt.
// Preparation reservations remain debt even before a request is bound.
func RequireAttemptDisposedTx(ctx context.Context, q Reader, attemptID string) error {
	var preparing int
	if err := q.QueryRowContext(ctx, `SELECT count(*) FROM provider_preparations r JOIN provider_invocations p ON p.provider_invocation_id=r.invocation_id WHERE r.owner_kind='run' AND p.attempt_id=? AND r.admission_closed=0 AND p.invocation_state='issued'`, attemptID).Scan(&preparing); err != nil {
		return err
	}
	if preparing != 0 {
		return openResources(OpenUnproven, "provider preparation remains open")
	}
	if err := requireRemoteOwnersDisposed(ctx, q, `SELECT DISTINCT r.owner_kind,r.invocation_id FROM remote_runtimes r JOIN provider_invocations p ON p.provider_invocation_id=r.invocation_id WHERE r.owner_kind='run' AND p.attempt_id=?`, attemptID); err != nil {
		return err
	}

	rows, err := q.QueryContext(ctx, `SELECT pr.run_invocation_id FROM provider_requests pr JOIN provider_invocations pi ON pi.provider_invocation_id=pr.run_invocation_id WHERE pi.attempt_id=?`, attemptID)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := RequireDisposedTx(ctx, q, Owner{Kind: OwnerRun, InvocationID: id}); err != nil {
			return err
		}
	}
	return nil
}

// CloseAttemptAdmissionTx is the operator cancellation fence. Its attempt ID
// resolves all immutable request bindings; no caller-supplied lease can target a
// newer owner. The cancellation transition must call it before publishing stop.
func CloseAttemptAdmissionTx(ctx context.Context, tx *sql.Tx, attemptID, closedAt string) error {
	if _, err := time.Parse(time.RFC3339Nano, closedAt); err != nil {
		return fmt.Errorf("provider admission close time is required")
	}
	if _, err := tx.ExecContext(ctx, `UPDATE provider_requests SET admission_closed_at=? WHERE run_invocation_id IN (SELECT provider_invocation_id FROM provider_invocations WHERE attempt_id=?) AND admission_closed_at IS NULL`, closedAt, attemptID); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `UPDATE remote_runtimes SET admission_closed=1 WHERE owner_kind='run' AND invocation_id IN (SELECT provider_invocation_id FROM provider_invocations WHERE attempt_id=?)`, attemptID)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE provider_preparations SET admission_closed=1 WHERE owner_kind='run' AND invocation_id IN (SELECT provider_invocation_id FROM provider_invocations WHERE attempt_id=?)`, attemptID); err != nil {
		return err
	}
	var id string
	err = tx.QueryRowContext(ctx, `SELECT p.provider_invocation_id FROM attempt_executions a JOIN provider_invocations p ON p.provider_invocation_id=a.active_provider_invocation_id JOIN provider_preparations r ON r.owner_kind='run' AND r.invocation_id=p.provider_invocation_id WHERE a.attempt_id=? AND p.invocation_state='issued' AND NOT EXISTS(SELECT 1 FROM remote_runtimes rr WHERE rr.owner_kind='run' AND rr.invocation_id=p.provider_invocation_id)`, attemptID).Scan(&id)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	if err = AbandonPreparationTx(ctx, tx, Owner{Kind: OwnerRun, InvocationID: id}); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE provider_invocations SET invocation_state='invalidated',invalid_reason='preparation_cancelled',updated_at=? WHERE provider_invocation_id=?`, closedAt, id); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE attempt_executions SET active_provider_invocation_id=NULL WHERE attempt_id=? AND active_provider_invocation_id=?`, attemptID, id)

	return err
}

// RequireRunDisposedTx gates shared workspace release across every reserved or bound run
// invocation, including siblings and historical attempts.
func RequireRunDisposedTx(ctx context.Context, q Reader, runID string) error {
	var preparing int
	if err := q.QueryRowContext(ctx, `SELECT count(*) FROM provider_preparations r JOIN provider_invocations p ON p.provider_invocation_id=r.invocation_id WHERE r.owner_kind='run' AND p.run_id=? AND r.admission_closed=0 AND p.invocation_state='issued'`, runID).Scan(&preparing); err != nil {
		return err
	}
	if preparing != 0 {
		return openResources(OpenUnproven, "provider preparation remains open")
	}
	if err := requireRemoteOwnersDisposed(ctx, q, `SELECT DISTINCT r.owner_kind,r.invocation_id FROM remote_runtimes r JOIN provider_invocations p ON p.provider_invocation_id=r.invocation_id WHERE r.owner_kind='run' AND p.run_id=?`, runID); err != nil {
		return err
	}

	rows, err := q.QueryContext(ctx, `SELECT pi.provider_invocation_id FROM provider_requests pr JOIN provider_invocations pi ON pi.provider_invocation_id=pr.run_invocation_id WHERE pi.run_id=?`, runID)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := RequireDisposedTx(ctx, q, Owner{Kind: OwnerRun, InvocationID: id}); err != nil {
			return err
		}
	}
	return nil
}

// isAbandoned reports whether the request was closed without a provider
// answer. Abandonment requires every admitted effect to be closed, so a replaced
// invocation in that state leaves no effect to dispose of.
func isAbandoned(b Binding) (bool, error) {
	if b.TerminalReference == nil {
		return false, nil
	}
	abandoned, err := providerexecution.AbandonedRequestTerminal(b.Owner.InvocationID).Reference()
	if err != nil {
		return false, err
	}
	return *b.TerminalReference == abandoned, nil
}
