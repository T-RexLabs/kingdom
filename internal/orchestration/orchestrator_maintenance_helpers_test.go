package orchestration

// Synchronous scan helpers are test scaffolding only. Production admission
// always uses bounded maintenance workers; the single-record executors are shared.
import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	executionstore "kingdom/internal/execution/store"
)

func (o *orchestrationLoop) lockRunWorkspace(runID string) func() {
	if o == nil || strings.TrimSpace(runID) == "" {
		return func() {}
	}
	value, _ := o.workspaceLocks.LoadOrStore(runID, &sync.Mutex{})
	lock := value.(*sync.Mutex)
	lock.Lock()
	return lock.Unlock
}

func (o *orchestrationLoop) waitForMaintenance(ctx context.Context) error {
	for {
		o.execMu.Lock()
		if len(o.maintenance.active) == 0 {
			o.execMu.Unlock()
			return nil
		}
		changed := o.maintenance.changed
		o.execMu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (o *orchestrationLoop) drainExecutionOutbox(ctx context.Context) (int, error) {
	_, processed, err := o.drainExecutionOutboxPass(ctx, executionOutboxPassLimit)
	return processed, err
}

func (o *orchestrationLoop) drainExecutionOutboxPass(ctx context.Context, limit int) (int, int, error) {
	if o == nil || o.executionStore == nil {
		return 0, 0, nil
	}

	pending, err := o.executionStore.ListPendingOutbox(ctx, limit)
	if err != nil {
		return 0, 0, err
	}
	if len(pending) == 0 {
		return 0, 0, nil
	}

	processed := 0
	for _, rec := range pending {
		published, err := o.projectExecutionOutboxRecord(ctx, rec)
		if err != nil {
			return len(pending), processed, err
		}
		if published {
			processed++
		}
	}
	return len(pending), processed, nil
}

func (o *orchestrationLoop) recoverProviderTerminalHandoffFileCleanupsWithLimit(ctx context.Context, limit int) (int, error) {
	if o == nil || o.executionStore == nil {
		return 0, fmt.Errorf("provider terminal handoff file cleanup is not configured")
	}
	if o.fsStore == nil {
		return 0, nil
	}
	if limit <= 0 {
		return 0, fmt.Errorf("limit must be positive")
	}
	o.maintenance.scanMu.Lock()
	defer o.maintenance.scanMu.Unlock()
	candidates, err := o.executionStore.ListProviderTerminalHandoffFileCleanupCandidates(ctx, o.terminalHandoffCleanupCursor, limit)
	if err != nil {
		return 0, err
	}
	visited := 0
	for _, candidate := range candidates {
		if err := o.cleanupProviderTerminalHandoffFile(ctx, candidate); err != nil {
			slog.Warn("provider terminal handoff cleanup replay deferred", "provider_invocation_id", candidate.ProviderInvocationID, "error", err)
		}
		o.terminalHandoffCleanupCursor = candidate.ProviderInvocationID
		visited++
	}
	if visited < limit {
		o.terminalHandoffCleanupCursor = ""
	}
	return visited, nil
}

func (o *orchestrationLoop) recoverProviderTerminalHandoffFilesWithLimit(ctx context.Context, limit int) (int, error) {
	if o == nil || o.executionStore == nil {
		return 0, fmt.Errorf("provider terminal handoff file recovery is not configured")
	}
	if o.fsStore == nil {
		return 0, nil
	}
	if limit <= 0 {
		return 0, fmt.Errorf("limit must be positive")
	}
	o.maintenance.scanMu.Lock()
	defer o.maintenance.scanMu.Unlock()
	candidates, err := o.executionStore.ListProviderTerminalHandoffFileRecoveryCandidates(ctx, o.terminalHandoffFileCursor, limit)
	if err != nil {
		return 0, err
	}
	visited := 0
	for _, candidate := range candidates {
		if err := o.recoverProviderTerminalHandoffFile(ctx, candidate); err != nil {
			return visited, fmt.Errorf("recover provider terminal handoff file %q: %w", candidate.ProviderInvocationID, err)
		}
		o.terminalHandoffFileCursor = candidate.ProviderInvocationID
		visited++
	}
	return visited, nil
}

func (o *orchestrationLoop) convergeProviderTerminalHandoffs(ctx context.Context) (int, error) {
	return o.convergeProviderTerminalHandoffsWithLimit(ctx, providerTerminalHandoffPassLimit)
}

func (o *orchestrationLoop) convergeProviderTerminalHandoffsWithLimit(ctx context.Context, limit int) (int, error) {
	if o == nil || o.executionStore == nil {
		return 0, fmt.Errorf("provider terminal handoff convergence is not configured")
	}
	o.maintenance.scanMu.Lock()
	defer o.maintenance.scanMu.Unlock()

	handoffs, err := o.executionStore.ListPendingProviderTerminalHandoffs(ctx, o.terminalHandoffCursor, limit)
	if err != nil {
		return 0, err
	}
	if len(handoffs) == 0 {
		o.terminalHandoffCursor = ""
		return 0, nil
	}
	visited := 0
	for _, handoff := range handoffs {
		if err := o.convergeProviderTerminalHandoff(ctx, handoff); err != nil {
			return visited, fmt.Errorf("converge provider terminal handoff %q: %w", handoff.HandoffID, err)
		}
		o.terminalHandoffCursor = handoff.ProviderInvocationID
		visited++
	}
	return visited, nil
}

func (o *orchestrationLoop) recoverResponseOnlyWorkspaceCleanupsWithLimit(ctx context.Context, limit int) (int, error) {
	if o == nil || o.fsStore == nil || o.executionStore == nil {
		return 0, nil
	}
	if limit <= 0 {
		return 0, fmt.Errorf("limit must be positive")
	}
	candidates, err := o.fsStore.ListPendingResponseOnlyWorkspaceCleanups(ctx, o.responseOnlyWorkspaceCleanupCursor, limit)
	if err != nil {
		return 0, err
	}
	visited := 0
	for _, candidate := range candidates {
		func() {
			if o.executionAttemptInflight(candidate.AttemptID) {
				o.responseOnlyWorkspaceCleanupCursor = candidate.AttemptID
				visited++
				return
			}
			quiescent, quiescenceErr := o.executionStore.AttemptProcessGuardsQuiescent(ctx, candidate.AttemptID)
			if quiescenceErr != nil {
				slog.Warn("response-only workspace cleanup replay deferred because process quiescence is unknown", "attempt_id", candidate.AttemptID, "error", quiescenceErr)
				o.responseOnlyWorkspaceCleanupCursor = candidate.AttemptID
				visited++
				return
			}
			if !quiescent {
				o.responseOnlyWorkspaceCleanupCursor = candidate.AttemptID
				visited++
				return
			}
			if err := o.removeResponseOnlyWorkspaceAndClear(ctx, candidate.RunID, candidate.AttemptID, candidate.MarkedAt); err != nil {
				slog.Warn("response-only workspace cleanup replay deferred", "attempt_id", candidate.AttemptID, "error", err)
				o.responseOnlyWorkspaceCleanupCursor = candidate.AttemptID
				visited++
				return
			}
			o.responseOnlyWorkspaceCleanupCursor = candidate.AttemptID
			visited++
		}()
	}
	if visited < limit {
		o.responseOnlyWorkspaceCleanupCursor = ""
	}
	return visited, nil
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

func (r *Runtime) drainExecutionOutboxUntilIdle(ctx context.Context) error {
	if r == nil || r.orchestrator == nil {
		return nil
	}
	// Loopless fixtures must admit the same finalization phase that the
	// completion loop normally drives before an outbox record can exist.
	if _, err := r.orchestrator.scheduleProviderTerminalFinalization(ctx); err != nil {
		return err
	}
	if err := r.orchestrator.waitForMaintenance(ctx); err != nil {
		return err
	}
	for {
		processed, err := r.orchestrator.drainExecutionOutbox(ctx)
		if err != nil {
			return err
		}
		if processed == 0 {
			return nil
		}
	}
}

func (r *Runtime) drainExecutionOutboxUntilDrained(ctx context.Context) error {
	if r == nil || r.orchestrator == nil {
		return nil
	}
	for {
		processed, err := r.orchestrator.drainExecutionOutbox(ctx)
		if err != nil {
			return err
		}
		pending, err := r.hasPendingExecutionOutbox(ctx)
		if err != nil {
			return err
		}
		if !pending {
			return nil
		}
		if processed > 0 {
			continue
		}
		if err := sleepWithContext(ctx, nil, r.completionProjectionInterval()); err != nil {
			return err
		}
	}
}

func (r *Runtime) drainProviderTerminalHandoffs(ctx context.Context) error {
	if r == nil || r.orchestrator == nil || r.orchestrator.executionStore == nil {
		return nil
	}
	for {
		visited, err := r.orchestrator.convergeProviderTerminalHandoffs(ctx)
		if err != nil {
			return err
		}
		if visited == 0 {
			return nil
		}
	}
}

// Synchronous test scaffolding composes the same bounded stop and completion
// phases used by independently admitted production workers.
func (o *orchestrationLoop) drainAttemptCancel(ctx context.Context, rec executionstore.AttemptCancelRecord, publishedAt string) (bool, error) {
	ready, err := o.requestAttemptCancelStop(ctx, &rec, publishedAt)
	if err != nil || !ready {
		return false, err
	}
	return o.completeAttemptCancelHandoff(ctx, rec, publishedAt)
}

func (o *orchestrationLoop) drainRunWorktreeCleanup(ctx context.Context) error {
	if o == nil || o.runWorktrees == nil {
		return nil
	}
	now := o.now().UTC().Format(time.RFC3339)
	if err := o.runWorktrees.ReclaimAbandonedCleanupClaims(ctx, now); err != nil {
		return fmt.Errorf("reclaim abandoned run worktree cleanup claims: %w", err)
	}
	records, err := o.runWorktrees.ListDueForCleanup(ctx, now, runWorktreeCleanupBatchSize)
	if err != nil {
		return fmt.Errorf("list due run worktrees for cleanup: %w", err)
	}
	for _, record := range records {
		if _, err := o.cleanupRunWorktree(ctx, record, now); err != nil {
			return err
		}
	}
	return nil
}
