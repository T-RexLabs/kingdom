package orchestration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"
)

const attemptCancelOutboxWriteTimeout = 5 * time.Second

type workspaceHandoffBlockedError struct{}

func (workspaceHandoffBlockedError) Error() string {
	return "another workspace-owning attempt is still active"
}

func (o *orchestrationLoop) drainAttemptCancelOutbox(ctx context.Context) (int, error) {
	if o == nil || o.executionStore == nil {
		return 0, nil
	}
	o.attemptCancelMu.Lock()
	defer o.attemptCancelMu.Unlock()
	ctx = firstNonNilContext(ctx)

	const batchSize = 64
	if o.attemptCancelHighWatermark.CancelOutboxID == "" {
		end, err := o.executionStore.PendingAttemptCancelHighWatermark(ctx)
		if err != nil || end.CancelOutboxID == "" {
			return 0, err
		}
		o.attemptCancelHighWatermark = end
	}
	pending, err := o.executionStore.ListPendingAttemptCancels(ctx, o.attemptCancelCursor, o.attemptCancelHighWatermark, batchSize)
	if err != nil {
		return 0, err
	}
	publishedAt := o.now().UTC().Format(time.RFC3339)
	processed := 0
	for _, rec := range pending {
		if err := ctx.Err(); err != nil {
			return processed, err
		}
		completed, err := o.drainAttemptCancel(ctx, rec, publishedAt)
		if err != nil {
			return processed, err
		}
		// Waiting rows retain their durable eligibility without monopolizing the
		// first batch on every dispatch tick. The cursor is only a scan position.
		o.attemptCancelCursor = executionstore.AttemptCancelCursor{CreatedAt: rec.CreatedAt, CancelOutboxID: rec.CancelOutboxID}
		if completed {
			processed++
		}
	}
	if len(pending) < batchSize || o.attemptCancelCursor == o.attemptCancelHighWatermark {
		o.attemptCancelCursor = executionstore.AttemptCancelCursor{}
		o.attemptCancelHighWatermark = executionstore.AttemptCancelCursor{}
	}
	return processed, nil
}

func (o *orchestrationLoop) drainAttemptCancel(ctx context.Context, rec executionstore.AttemptCancelRecord, publishedAt string) (bool, error) {
	quiescent, err := o.executionStore.AttemptProcessGuardsQuiescent(ctx, rec.AttemptID)
	if err != nil {
		return false, err
	}
	if quiescent || rec.ExecutionMode != executionstore.ExecutionModeProviderBacked || strings.TrimSpace(rec.SessionID) == "" {
		return o.completeAttemptCancelHandoff(ctx, rec, publishedAt)
	}
	if rec.HandoffState == "guardian_evidence_recovery_requested" {
		quarantined, err := o.executionStore.AttemptProcessGuardEvidenceQuarantined(ctx, rec.AttemptID)
		if err != nil {
			return false, err
		}
		if !quarantined {
			return false, fmt.Errorf("guardian evidence recovery for attempt %q no longer has a durable absence quarantine", rec.AttemptID)
		}
		return o.completeAttemptCancelHandoff(ctx, rec, publishedAt)
	}
	if rec.HandoffState == "awaiting_workspace_quiescence" || rec.HandoffState == "workspace_restore_failed" {
		return false, o.recordAttemptCancelFailure(ctx, rec, publishedAt)
	}
	if o.adapter == nil {
		return false, fmt.Errorf("execution adapter is not configured")
	}
	dispatchedState, err := o.transitionService().ClaimAttemptCancelHandoffDispatch(ctx, rec.CancelOutboxID, publishedAt)
	if err != nil || dispatchedState == "" {
		return false, err
	}
	rec.HandoffState = dispatchedState
	quiescent, err = o.executionStore.AttemptProcessGuardsQuiescent(ctx, rec.AttemptID)
	if err != nil {
		return false, err
	}
	if quiescent {
		return o.completeAttemptCancelHandoff(ctx, rec, publishedAt)
	}
	cancelCtx, cancel := context.WithTimeout(ctx, defaultDuration(o.cancelGrace, 5*time.Second))
	cancelErr := o.adapter.Cancel(cancelCtx, attemptCancelRequest(rec, publishedAt))
	cancel()
	quiescent, err = o.executionStore.AttemptProcessGuardsQuiescent(ctx, rec.AttemptID)
	if err != nil {
		return false, err
	}
	// Durable guardian proof is authoritative even if the bounded stop request
	// returned an error. The local worker is a separate handoff gate below.
	if quiescent {
		return o.completeAttemptCancelHandoff(ctx, rec, publishedAt)
	}
	if ctx.Err() != nil && (errors.Is(cancelErr, context.Canceled) || errors.Is(cancelErr, context.DeadlineExceeded)) {
		return false, ctx.Err()
	}
	if cancelErr == nil {
		cancelErr = fmt.Errorf("managed provider process quiescence has not been proven")
	}
	slog.Warn("failed to dispatch provider cancel for terminalized attempt", "attempt_id", rec.AttemptID, "run_id", rec.RunID, "error", cancelErr)
	return false, o.recordAttemptCancelFailure(ctx, rec, publishedAt)
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
	if !spec.RequiresWorkspace {
		return nil
	}
	if o.fsStore == nil {
		return fmt.Errorf("runtime filesystem store is required for workspace restoration")
	}
	baseSnapshotID := strings.TrimSpace(rec.BaseSnapshotID)
	if baseSnapshotID == "" {
		return fmt.Errorf("workspace-owning attempt has no trusted base snapshot")
	}
	unlock := o.lockRunWorkspace(rec.RunID)
	defer unlock()
	if o.executionAttemptInflight(rec.AttemptID) {
		return workspaceHandoffBlockedError{}
	}
	activeAttempts, err := o.executionStore.ListActiveRunAttempts(ctx, rec.RunID, rec.AttemptID)
	if err != nil {
		return err
	}
	for _, active := range activeAttempts {
		if !executionSpecForNode(active.NodeID, active.NodeClass).RequiresWorkspace {
			continue
		}
		if !active.CancellationPending {
			return workspaceHandoffBlockedError{}
		}
		quiescent, err := o.executionStore.AttemptProcessGuardsQuiescent(ctx, active.AttemptID)
		if err != nil {
			return err
		}
		if !quiescent || o.executionAttemptInflight(active.AttemptID) {
			return workspaceHandoffBlockedError{}
		}
	}

	target, err := o.fsStore.GetExecutionTarget(ctx, rec.RunID)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("workspace-owning attempt has no execution target")
	}
	if err != nil {
		return fmt.Errorf("load execution target for cancellation handoff: %w", err)
	}
	if strings.TrimSpace(target.ExecutionRoot) == "" {
		return fmt.Errorf("workspace-owning attempt has no execution root")
	}
	if err := o.restoreExecutionTarget(ctx, rec.RunID, target.ExecutionRoot, baseSnapshotID, restoredAt); err != nil {
		return fmt.Errorf("restore trusted workspace after cancellation: %w", err)
	}
	return nil
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

func attemptCancelRequest(rec executionstore.AttemptCancelRecord, fallbackOccurredAt string) opencode.CancelRequest {
	return opencode.CancelRequest{
		SessionID:  strings.TrimSpace(rec.SessionID),
		AttemptID:  strings.TrimSpace(rec.AttemptID),
		Reason:     firstNonEmpty(strings.TrimSpace(rec.ReasonText), strings.TrimSpace(rec.ReasonCode), "run terminalized"),
		OccurredAt: firstNonEmpty(strings.TrimSpace(rec.RequestedAt), strings.TrimSpace(fallbackOccurredAt)),
	}
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
