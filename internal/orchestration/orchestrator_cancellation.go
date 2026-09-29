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

	pending, err := o.executionStore.ListPendingAttemptCancels(ctx, 64)
	if err != nil {
		return 0, err
	}
	if len(pending) == 0 {
		return 0, nil
	}
	publishedAt := o.now().UTC().Format(time.RFC3339)
	cancelTimeout := defaultDuration(o.cancelGrace, 5*time.Second)
	processed := 0
	for _, rec := range pending {
		if ctx != nil {
			select {
			case <-ctx.Done():
				return processed, ctx.Err()
			default:
			}
		}

		quiescent, queryErr := o.executionStore.AttemptProcessGuardsQuiescent(ctx, rec.AttemptID)
		if queryErr != nil {
			return processed, queryErr
		}
		if o.executionAttemptInflight(rec.AttemptID) {
			quiescent = false
		}
		if quiescent || rec.ExecutionMode != executionstore.ExecutionModeProviderBacked || strings.TrimSpace(rec.SessionID) == "" {
			completed, err := o.completeAttemptCancelHandoff(ctx, rec, publishedAt)
			if err != nil {
				return processed, err
			}
			if completed {
				processed++
			}
			continue
		}
		if rec.HandoffState == "guardian_evidence_recovery_requested" {
			quarantined, err := o.executionStore.AttemptProcessGuardEvidenceQuarantined(ctx, rec.AttemptID)
			if err != nil {
				return processed, err
			}
			if !quarantined {
				return processed, fmt.Errorf("guardian evidence recovery for attempt %q no longer has a durable absence quarantine", rec.AttemptID)
			}
			if o.executionAttemptInflight(rec.AttemptID) {
				if err := o.recordAttemptCancelFailureMessage(ctx, rec.CancelOutboxID, "The provider worker is still active, so Kingdom is waiting before restoring the shared workspace.", "awaiting_quiescence", publishedAt); err != nil {
					return processed, err
				}
				continue
			}
			completed, err := o.completeAttemptCancelHandoff(ctx, rec, publishedAt)
			if err != nil {
				return processed, err
			}
			if completed {
				processed++
			}
			continue
		}
		if rec.HandoffState == "awaiting_workspace_quiescence" || rec.HandoffState == "workspace_restore_failed" {
			if err := o.recordAttemptCancelFailure(ctx, rec.CancelOutboxID, publishedAt); err != nil {
				return processed, err
			}
			continue
		}
		if o.adapter == nil {
			return processed, fmt.Errorf("execution adapter is not configured")
		}
		var claimed bool
		claimed, err = o.transitionService().ClaimAttemptCancelHandoffDispatch(ctx, rec.CancelOutboxID, publishedAt)
		if err != nil {
			return processed, err
		}
		if !claimed {
			continue
		}
		quiescent, queryErr = o.executionStore.AttemptProcessGuardsQuiescent(ctx, rec.AttemptID)
		if queryErr != nil {
			return processed, queryErr
		}
		if o.executionAttemptInflight(rec.AttemptID) {
			quiescent = false
		}
		if quiescent {
			completed, completeErr := o.completeAttemptCancelHandoff(ctx, rec, publishedAt)
			if completeErr != nil {
				return processed, completeErr
			}
			if completed {
				processed++
			}
			continue
		}
		request := attemptCancelRequest(rec, publishedAt)
		cancelCtx, cancel := context.WithTimeout(firstNonNilContext(ctx), cancelTimeout)
		err := o.adapter.Cancel(cancelCtx, request)
		cancel()
		quiescent, queryErr = o.executionStore.AttemptProcessGuardsQuiescent(ctx, rec.AttemptID)
		if queryErr != nil {
			return processed, queryErr
		}
		if o.executionAttemptInflight(rec.AttemptID) {
			quiescent = false
		}
		var missing *opencode.AttemptNotFoundError
		if quiescent && (err == nil || errors.As(err, &missing)) {
			completed, completeErr := o.completeAttemptCancelHandoff(ctx, rec, publishedAt)
			if completeErr != nil {
				return processed, completeErr
			}
			if completed {
				processed++
			}
			continue
		}
		if ctx != nil && ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			return processed, ctx.Err()
		}

		if err == nil && !quiescent {
			err = fmt.Errorf("managed provider process quiescence has not been proven")
		}
		slog.Warn("failed to dispatch provider cancel for terminalized attempt", "attempt_id", rec.AttemptID, "run_id", rec.RunID, "error", err)
		if recordErr := o.recordAttemptCancelFailure(ctx, rec.CancelOutboxID, publishedAt); recordErr != nil {
			return processed, recordErr
		}
	}

	return processed, nil
}

func (o *orchestrationLoop) completeAttemptCancelHandoff(ctx context.Context, rec executionstore.AttemptCancelRecord, publishedAt string) (bool, error) {
	if restoreErr := o.restoreTrustedWorkspaceAfterCancellation(ctx, rec, publishedAt); restoreErr != nil {
		slog.Warn("failed to restore trusted workspace after cancellation", "attempt_id", rec.AttemptID, "run_id", rec.RunID, "error", restoreErr)
		var blocked workspaceHandoffBlockedError
		if errors.As(restoreErr, &blocked) {
			if rec.HandoffState == "guardian_evidence_recovery_requested" {
				return false, o.recordAttemptCancelFailureMessage(ctx, rec.CancelOutboxID, "Kingdom is waiting for another active step to release the shared workspace.", "guardian_evidence_recovery_requested", publishedAt)
			}
			return false, o.recordAttemptWorkspaceQuiescenceFailure(ctx, rec.CancelOutboxID, publishedAt)
		}
		if rec.HandoffState == "guardian_evidence_recovery_requested" {
			return false, o.recordAttemptCancelFailureMessage(ctx, rec.CancelOutboxID, "Kingdom could not restore the trusted workspace after cancellation.", "guardian_evidence_recovery_requested", publishedAt)
		}
		return false, o.recordAttemptWorkspaceRestoreFailure(ctx, rec.CancelOutboxID, publishedAt)
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

func (o *orchestrationLoop) recordAttemptCancelFailure(ctx context.Context, cancelOutboxID, updatedAt string) error {
	return o.recordAttemptCancelFailureMessage(ctx, cancelOutboxID, "Kingdom could not confirm that the previous provider process stopped.", "awaiting_quiescence", updatedAt)
}

func (o *orchestrationLoop) recordAttemptWorkspaceRestoreFailure(ctx context.Context, cancelOutboxID, updatedAt string) error {
	return o.recordAttemptCancelFailureMessage(ctx, cancelOutboxID, "Kingdom could not restore the trusted workspace after cancellation.", "workspace_restore_failed", updatedAt)
}

func (o *orchestrationLoop) recordAttemptWorkspaceQuiescenceFailure(ctx context.Context, cancelOutboxID, updatedAt string) error {
	return o.recordAttemptCancelFailureMessage(ctx, cancelOutboxID, "Kingdom is waiting for another active step to release the shared workspace.", "awaiting_workspace_quiescence", updatedAt)
}

func (o *orchestrationLoop) recordAttemptCancelFailureMessage(ctx context.Context, cancelOutboxID, message, handoffState, updatedAt string) error {
	writeCtx, cancel := attemptCancelOutboxWriteContext(ctx)
	defer cancel()
	return o.executionStore.RecordAttemptCancelFailure(writeCtx, cancelOutboxID, message, handoffState, updatedAt)
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
