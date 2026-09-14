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
	if o.adapter == nil {
		return 0, fmt.Errorf("execution adapter is not configured")
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

		if rec.ExecutionMode != executionstore.ExecutionModeProviderBacked || strings.TrimSpace(rec.SessionID) == "" {
			if err := o.publishAttemptCancelAfterProjection(ctx, rec, publishedAt); err != nil {
				return processed, err
			}
			processed++
			continue
		}

		request := attemptCancelRequest(rec, publishedAt)
		cancelCtx, cancel := context.WithTimeout(firstNonNilContext(ctx), cancelTimeout)
		err := o.adapter.Cancel(cancelCtx, request)
		cancel()

		quiescent, queryErr := o.executionStore.AttemptProcessGuardsQuiescent(ctx, rec.AttemptID)
		if queryErr != nil {
			return processed, queryErr
		}
		var missing *opencode.AttemptNotFoundError
		if quiescent && (err == nil || errors.As(err, &missing)) {
			if restoreErr := o.restoreTrustedWorkspaceAfterCancellation(ctx, rec, publishedAt); restoreErr != nil {
				if recordErr := o.recordAttemptCancelFailure(ctx, rec.CancelOutboxID, restoreErr, publishedAt); recordErr != nil {
					return processed, recordErr
				}
				continue
			}
			if rec.ReasonCode == "agent_unresponsive" {
				released, releaseErr := o.transitionService().ReleaseFencedStaleRetryAfterCancellation(ctx, rec.AttemptID, publishedAt)
				if releaseErr != nil {
					return processed, releaseErr
				}
				if released {
					processed++
					continue
				}
			}
			if err := o.publishAttemptCancelAfterProjection(ctx, rec, publishedAt); err != nil {
				return processed, err
			}
			processed++
			continue
		}
		if ctx != nil && ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			return processed, ctx.Err()
		}

		if err == nil && !quiescent {
			err = fmt.Errorf("managed provider process quiescence has not been proven")
		}
		slog.Warn("failed to dispatch provider cancel for terminalized attempt", "attempt_id", rec.AttemptID, "run_id", rec.RunID, "error", err)
		if recordErr := o.recordAttemptCancelFailure(ctx, rec.CancelOutboxID, err, publishedAt); recordErr != nil {
			return processed, recordErr
		}
	}

	return processed, nil
}

func (o *orchestrationLoop) restoreTrustedWorkspaceAfterCancellation(ctx context.Context, rec executionstore.AttemptCancelRecord, restoredAt string) error {
	if o == nil || o.fsStore == nil || strings.TrimSpace(rec.RunID) == "" {
		return nil
	}
	unlock := o.lockRunWorkspace(rec.RunID)
	defer unlock()

	target, err := o.fsStore.GetExecutionTarget(ctx, rec.RunID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("load execution target for cancellation handoff: %w", err)
	}
	if strings.TrimSpace(target.TrustedSnapshotID) == "" || strings.TrimSpace(target.ExecutionRoot) == "" {
		return nil
	}
	if err := o.restoreExecutionTarget(ctx, rec.RunID, target.ExecutionRoot, target.TrustedSnapshotID, restoredAt); err != nil {
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

func (o *orchestrationLoop) recordAttemptCancelFailure(ctx context.Context, cancelOutboxID string, cause error, updatedAt string) error {
	message := "attempt cancel failed"
	if cause != nil {
		message = strings.TrimSpace(cause.Error())
	}
	writeCtx, cancel := attemptCancelOutboxWriteContext(ctx)
	defer cancel()
	return o.executionStore.RecordAttemptCancelFailure(writeCtx, cancelOutboxID, message, updatedAt)
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
