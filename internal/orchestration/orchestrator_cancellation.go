package orchestration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"kingdom/internal/execution/providerrequest"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/state"
)

const attemptCancelOutboxWriteTimeout = 5 * time.Second

type workspaceHandoffBlockedError struct{}

func (workspaceHandoffBlockedError) Error() string {
	return "another workspace-owning attempt is still active"
}

func (o *orchestrationLoop) requestAttemptCancelStop(ctx context.Context, rec *executionstore.AttemptCancelRecord, publishedAt string) (bool, error) {
	o.cancelExecutionPreparation(rec.AttemptID)
	quiescent, err := o.executionStore.AttemptCommandGuardiansQuiescent(ctx, rec.AttemptID)
	if err != nil {
		return false, err
	}
	if quiescent || rec.ExecutionMode != executionstore.ExecutionModeProviderBacked || strings.TrimSpace(rec.SessionID) == "" {
		return true, nil
	}
	if rec.HandoffState == "guardian_evidence_recovery_requested" {
		quarantined, err := o.executionStore.AttemptCommandGuardianEvidenceQuarantined(ctx, rec.AttemptID)
		if err != nil {
			return false, err
		}
		if !quarantined {
			return false, fmt.Errorf("guardian evidence recovery for attempt %q no longer has a durable absence quarantine", rec.AttemptID)
		}
		return true, nil
	}
	if rec.HandoffState == "awaiting_workspace_quiescence" || rec.HandoffState == "workspace_restore_failed" {
		return false, o.recordAttemptCancelFailure(ctx, *rec, publishedAt)
	}
	if o.provider == nil {
		return false, fmt.Errorf("execution provider is not configured")
	}
	dispatchedState, err := o.transitionService().ClaimAttemptCancelHandoffDispatch(ctx, rec.CancelOutboxID, publishedAt)
	if err != nil || dispatchedState == "" {
		return false, err
	}
	rec.HandoffState = dispatchedState
	quiescent, err = o.executionStore.AttemptCommandGuardiansQuiescent(ctx, rec.AttemptID)
	if err != nil {
		return false, err
	}
	if quiescent {
		return true, nil
	}
	cancelCtx, cancel := context.WithTimeout(ctx, defaultDuration(o.cancelGrace, 5*time.Second))
	cancelErr := o.provider.Cancel(cancelCtx, strings.TrimSpace(rec.AttemptID))
	cancel()
	quiescent, err = o.executionStore.AttemptCommandGuardiansQuiescent(ctx, rec.AttemptID)
	if err != nil {
		return false, err
	}
	// Durable guardian proof is authoritative even if the bounded stop request
	// returned an error. The local worker is a separate handoff gate below.
	if quiescent {
		return true, nil
	}
	if ctx.Err() != nil && (errors.Is(cancelErr, context.Canceled) || errors.Is(cancelErr, context.DeadlineExceeded)) {
		return false, ctx.Err()
	}
	if cancelErr == nil {
		// The request is interrupted. Its worker settles it, and the next
		// maintenance pass completes the cancellation once it has.
		return false, o.recordAttemptCancelFailureMessage(ctx, *rec, "Kingdom is waiting for the provider request to settle before completing cancellation.", "awaiting_quiescence", o.now().UTC().Format(time.RFC3339Nano))
	}
	slog.Warn("failed to dispatch provider cancel for terminalized attempt", "attempt_id", rec.AttemptID, "run_id", rec.RunID, "error", cancelErr)
	return false, o.recordAttemptCancelFailure(ctx, *rec, o.now().UTC().Format(time.RFC3339Nano))
}

func (o *orchestrationLoop) completeAttemptCancelHandoff(ctx context.Context, rec executionstore.AttemptCancelRecord, publishedAt string) (bool, error) {
	// Execute can have persisted process proof before the outer worker finishes
	// its lease and terminal bookkeeping. This applies to response-only work too.
	if o.executionAttemptInflight(rec.AttemptID) {
		handoffState := "awaiting_workspace_quiescence"
		if rec.HandoffState == "guardian_evidence_recovery_requested" {
			handoffState = rec.HandoffState
		}
		return false, o.recordAttemptCancelFailureMessage(ctx, rec, "Kingdom is waiting for the provider worker to finish before completing cancellation.", handoffState, publishedAt)
	}
	if restoreErr := o.restoreTrustedWorkspaceAfterCancellation(ctx, rec, publishedAt); restoreErr != nil {
		if errors.Is(restoreErr, providerrequest.ErrResourcesOpen) {
			return false, restoreErr
		}
		publishedAt = o.now().UTC().Format(time.RFC3339Nano)
		slog.Warn("failed to restore trusted workspace after cancellation", "attempt_id", rec.AttemptID, "run_id", rec.RunID, "error", restoreErr)
		var blocked workspaceHandoffBlockedError
		if errors.As(restoreErr, &blocked) {
			if rec.HandoffState == "guardian_evidence_recovery_requested" {
				return false, o.recordAttemptCancelFailureMessage(ctx, rec, "Kingdom is waiting for another active step to release the shared workspace.", "guardian_evidence_recovery_requested", publishedAt)
			}
			return false, o.recordAttemptWorkspaceQuiescenceFailure(ctx, rec, publishedAt)
		}
		if rec.HandoffState == "guardian_evidence_recovery_requested" {
			return false, o.recordAttemptCancelFailureMessage(ctx, rec, "Kingdom could not restore the trusted workspace after cancellation.", "guardian_evidence_recovery_requested", publishedAt)
		}
		return false, o.recordAttemptWorkspaceRestoreFailure(ctx, rec, publishedAt)
	}
	publishedAt = o.now().UTC().Format(time.RFC3339Nano)
	if rec.ReasonCode == "agent_unresponsive" {
		released, err := o.transitionService().ReleaseFencedStaleRetryAfterCancellation(ctx, rec.AttemptID, publishedAt)
		if err != nil || released {
			return released, err
		}
	}
	if err := o.publishAttemptCancelAfterProjection(ctx, rec, publishedAt); err != nil {
		return false, err
	}
	return true, nil
}

func (o *orchestrationLoop) restoreTrustedWorkspaceAfterCancellation(ctx context.Context, rec executionstore.AttemptCancelRecord, restoredAt string) error {
	if o == nil || strings.TrimSpace(rec.RunID) == "" {
		return nil
	}
	spec := executionSpecForNode(rec.NodeID, rec.NodeClass)
	if spec.WorkspaceKind != workspaceCanonical {
		return nil
	}
	if o.fsStore == nil {
		return fmt.Errorf("runtime filesystem store is required for workspace restoration")
	}
	baseSnapshotID := strings.TrimSpace(rec.BaseSnapshotID)
	unlock, acquired := o.tryReserveCompletionWorkspace(ctx, rec.RunID)
	if !acquired {
		return workspaceHandoffBlockedError{}
	}
	defer unlock()
	if o.executionAttemptInflight(rec.AttemptID) {
		return workspaceHandoffBlockedError{}
	}
	if baseSnapshotID == "" {
		unprepared, err := o.cancelledAttemptHasNoWorkspaceOwnership(ctx, rec)
		if err != nil {
			return err
		}
		if !unprepared {
			return fmt.Errorf("workspace-owning attempt has no trusted base snapshot")
		}
	}
	activeAttempts, err := o.executionStore.ListActiveRunAttempts(ctx, rec.RunID, rec.AttemptID)
	if err != nil {
		return err
	}
	for _, active := range activeAttempts {
		if executionSpecForNode(active.NodeID, active.NodeClass).WorkspaceKind != workspaceCanonical {
			continue
		}
		if !active.CancellationPending {
			return workspaceHandoffBlockedError{}
		}
		quiescent, err := o.executionStore.AttemptCommandGuardiansQuiescent(ctx, active.AttemptID)
		if err != nil {
			return err
		}
		if !quiescent || o.executionAttemptInflight(active.AttemptID) {
			return workspaceHandoffBlockedError{}
		}
	}

	target, err := o.fsStore.GetExecutionTarget(ctx, rec.RunID)
	if errors.Is(err, sql.ErrNoRows) {
		if baseSnapshotID == "" {
			return nil
		}
		return fmt.Errorf("workspace-owning attempt has no execution target")
	}
	if err != nil {
		return fmt.Errorf("load execution target for cancellation handoff: %w", err)
	}
	if baseSnapshotID == "" {
		if target.State != "trusted" {
			return fmt.Errorf("unprepared cancellation found an untrusted execution target without a base snapshot")
		}
		return nil
	}
	if strings.TrimSpace(target.ExecutionRoot) == "" {
		return fmt.Errorf("workspace-owning attempt has no execution root")
	}
	if err := o.captureCancelledResumeCheckpoint(ctx, rec.AttemptID, rec.RunID, rec.NodeID, rec.NodeClass, target.ExecutionRoot, baseSnapshotID); err != nil {
		return fmt.Errorf("preserve cancelled workspace checkpoint: %w", err)
	}
	if err := o.restoreExecutionTarget(ctx, rec.RunID, target.ExecutionRoot, baseSnapshotID, restoredAt); err != nil {
		return fmt.Errorf("restore trusted workspace after cancellation: %w", err)
	}
	return nil
}

// Before preparation pins a base, a queued attempt has never owned the mutable
// target. Missing metadata after invocation or launch is not that safe case.
func (o *orchestrationLoop) cancelledAttemptHasNoWorkspaceOwnership(ctx context.Context, rec executionstore.AttemptCancelRecord) (bool, error) {
	var unprepared bool
	err := o.db.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1 FROM attempt_executions ae
		WHERE ae.attempt_id = ? AND ae.run_id = ? AND ae.node_run_id = ?
		  AND ae.execution_mode = 'provider_backed'
		  AND ae.state IN ('queued', 'committed_cancelled', 'committed_failure')
		  AND COALESCE(ae.base_snapshot_id, '') = '' AND COALESCE(ae.result_snapshot_id, '') = ''
		  AND COALESCE(ae.launch_started_at, '') = '' AND COALESCE(ae.started_at, '') = ''
		  AND COALESCE(ae.active_provider_invocation_id, '') = '' AND COALESCE(ae.provider_session_id, '') = ''
		  AND NOT EXISTS (SELECT 1 FROM provider_invocations pi WHERE pi.attempt_id = ae.attempt_id)
	)`, rec.AttemptID, rec.RunID, rec.NodeRunID).Scan(&unprepared)
	if err != nil {
		return false, fmt.Errorf("inspect unprepared cancellation ownership: %w", err)
	}
	return unprepared, nil
}

func (o *orchestrationLoop) publishAttemptCancelAfterProjection(ctx context.Context, rec executionstore.AttemptCancelRecord, publishedAt string) error {
	if o != nil && o.db != nil && strings.TrimSpace(rec.RunID) != "" && strings.TrimSpace(rec.NodeRunID) != "" {
		if _, err := o.transitionService().ProjectNodeTerminalFromDurableAttempt(ctx, state.DurableAttemptNodeProjectionRequest{
			RunID:      rec.RunID,
			NodeRunID:  rec.NodeRunID,
			AttemptID:  rec.AttemptID,
			ActorID:    "runtime_orchestrator",
			OccurredAt: publishedAt,
		}); err != nil {
			return err
		}
	}
	return o.markAttemptCancelPublished(ctx, rec.CancelOutboxID, publishedAt)
}

func (o *orchestrationLoop) markAttemptCancelPublished(ctx context.Context, cancelOutboxID, publishedAt string) error {
	writeCtx, cancel := attemptCancelOutboxWriteContext(ctx)
	defer cancel()
	return o.executionStore.MarkAttemptCancelPublished(writeCtx, cancelOutboxID, publishedAt)
}

func (o *orchestrationLoop) recordAttemptCancelFailure(ctx context.Context, rec executionstore.AttemptCancelRecord, updatedAt string) error {
	return o.recordAttemptCancelFailureMessage(ctx, rec, "Kingdom could not confirm that the previous provider process stopped.", "awaiting_quiescence", updatedAt)
}

func (o *orchestrationLoop) recordAttemptWorkspaceRestoreFailure(ctx context.Context, rec executionstore.AttemptCancelRecord, updatedAt string) error {
	return o.recordAttemptCancelFailureMessage(ctx, rec, "Kingdom could not restore the trusted workspace after cancellation.", "workspace_restore_failed", updatedAt)
}

func (o *orchestrationLoop) recordAttemptWorkspaceQuiescenceFailure(ctx context.Context, rec executionstore.AttemptCancelRecord, updatedAt string) error {
	return o.recordAttemptCancelFailureMessage(ctx, rec, "Kingdom is waiting for another active step to release the shared workspace.", "awaiting_workspace_quiescence", updatedAt)
}

func (o *orchestrationLoop) recordAttemptCancelFailureMessage(ctx context.Context, rec executionstore.AttemptCancelRecord, message, handoffState, updatedAt string) error {
	writeCtx, cancel := attemptCancelOutboxWriteContext(ctx)
	defer cancel()
	_, err := o.executionStore.RecordAttemptCancelFailure(writeCtx, executionstore.RecordAttemptCancelFailureRequest{
		CancelOutboxID: rec.CancelOutboxID, ExpectedHandoffState: rec.HandoffState,
		LastError: message, HandoffState: handoffState, UpdatedAt: updatedAt,
	})
	return err
}

func attemptCancelOutboxWriteContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(firstNonNilContext(ctx)), attemptCancelOutboxWriteTimeout)
}

func firstNonNilContext(ctx context.Context) context.Context {
	if ctx != nil {
		return ctx
	}
	return context.Background()
}
