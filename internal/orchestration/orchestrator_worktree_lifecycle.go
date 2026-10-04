package orchestration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"kingdom/internal/securefs"
	"kingdom/internal/workspace"
)

const runWorktreeCleanupBatchSize = 20
const runWorktreeRemovalBatchSize = 128
const runWorktreeCleanupWaitingForExecutionReason = "run execution projection or cancellation handoff is still pending"

func (o *orchestrationLoop) scheduleRunWorktreeCleanup(ctx context.Context) error {
	if o == nil || o.runWorktrees == nil || workspace.RunWorktreeCleanupDisabled(o.worktreeRetention) {
		return nil
	}
	cleanupAt := o.now().UTC()
	cleanupAfter := cleanupAt.Add(o.worktreeRetention).Format(time.RFC3339)
	if err := o.runWorktrees.ScheduleTerminalCleanup(ctx, cleanupAfter, cleanupAt.Format(time.RFC3339)); err != nil {
		return fmt.Errorf("schedule terminal run worktree cleanup: %w", err)
	}
	return nil
}

func (o *orchestrationLoop) cleanupRunWorktree(ctx context.Context, record workspace.RunWorktreeRecord, now string) (bool, error) {
	return o.cleanupManagedRunWorktree(ctx, record.RunID, now, false)
}

func (o *orchestrationLoop) cleanupRunWorktreeForManualRecovery(ctx context.Context, record workspace.RunWorktreeRecord, now string) (bool, error) {
	return o.cleanupManagedRunWorktree(ctx, record.RunID, now, true)
}

func (o *orchestrationLoop) cleanupManagedRunWorktree(ctx context.Context, runID, now string, manualRecovery bool) (complete bool, returnErr error) {
	if o == nil || o.runWorktrees == nil {
		return false, fmt.Errorf("run worktree store is required")
	}
	owner, err := generateRunWorktreeCleanupOwner()
	if err != nil {
		return false, err
	}
	var (
		record  workspace.RunWorktreeRecord
		claim   workspace.RunWorktreeCleanupClaim
		claimed bool
	)
	if manualRecovery {
		record, claim, claimed, err = o.runWorktrees.ClaimManualRecoveryCleanup(ctx, runID, owner)
	} else {
		record, claim, claimed, err = o.runWorktrees.ClaimDueForCleanup(ctx, runID, now, owner)
	}
	if err != nil {
		return false, fmt.Errorf("claim managed run worktree cleanup: %w", err)
	}
	if !claimed {
		return false, nil
	}

	claimReleased := false
	defer func() {
		if claimReleased {
			return
		}
		releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := o.runWorktrees.ReleaseCleanupClaim(releaseCtx, claim); err != nil && returnErr == nil {
			returnErr = fmt.Errorf("release managed run worktree cleanup claim: %w", err)
		}
	}()
	failCleanup := func(reason string) (bool, error) {
		updated, err := o.markRunWorktreeCleanupFailed(ctx, claim, reason)
		if err != nil {
			return false, err
		}
		if updated {
			claimReleased = true
		}
		return false, nil
	}
	if o.db != nil {
		var pendingExecutionWork int
		query := `SELECT EXISTS(
			SELECT 1 FROM attempt_outbox WHERE run_id = ? AND status = 'pending'
		) OR EXISTS(
			SELECT 1 FROM attempt_cancel_outbox WHERE run_id = ? AND status = 'pending'
		) OR EXISTS(
			SELECT 1 FROM provider_terminal_handoffs pth
			WHERE pth.run_id = ? AND NOT EXISTS (
				SELECT 1 FROM provider_completion_settlements pcs WHERE pcs.provider_invocation_id = pth.provider_invocation_id
			)
		) OR EXISTS(
			SELECT 1 FROM provider_invocations pi
			JOIN attempt_executions ae ON ae.active_provider_invocation_id = pi.provider_invocation_id
			WHERE ae.run_id = ? AND ae.state IN ('starting', 'running', 'terminal_observed')
			  AND (pi.terminal_handoff_pending_at IS NOT NULL OR pi.terminal_recovery_required_at IS NOT NULL)
		)`
		args := []any{runID, runID, runID, runID}
		if !manualRecovery {
			query += ` OR EXISTS(SELECT 1 FROM attempt_executions WHERE run_id = ? AND state IN ('starting', 'running', 'terminal_observed'))`
			args = append(args, runID)
		}
		if err := o.db.QueryRowContext(ctx, query, args...).Scan(&pendingExecutionWork); err != nil {
			return failCleanup(fmt.Sprintf("inspect pending run execution work failed: %v", err))
		}
		if pendingExecutionWork != 0 {
			return failCleanup(runWorktreeCleanupWaitingForExecutionReason)
		}
	}

	trimmedMainWorktreeRoot := strings.TrimSpace(record.MainWorktreeRoot)
	trimmedPath := strings.TrimSpace(record.WorktreePath)
	if trimmedMainWorktreeRoot == "" || trimmedPath == "" {
		return failCleanup("run worktree record is incomplete")
	}
	if err := validateManagedRunWorktreeBoundary(record); err != nil {
		return failCleanup(fmt.Sprintf("managed worktree boundary is invalid: %v", err))
	}
	if o.executionStore != nil {
		quiescent, err := o.executionStore.RunProviderProcessGuardsQuiescent(ctx, record.RunID)
		if err != nil {
			_, markErr := failCleanup(fmt.Sprintf("inspect managed provider process quiescence failed: %v", err))
			return false, errors.Join(err, markErr)
		}
		if !quiescent || o.hasInflightRunExecution(record.RunID) {
			return failCleanup("managed provider process quiescence has not been proven")
		}
	}
	pathExists, err := managedWorktreePathExists(trimmedPath)
	if err != nil {
		return failCleanup(fmt.Sprintf("inspect worktree path failed: %v", err))
	}
	registeredBefore, err := workspace.ListRegisteredWorktrees(trimmedMainWorktreeRoot, o.git())
	if err != nil {
		return failCleanup(fmt.Sprintf("registered git worktrees could not be listed: %v", err))
	}
	entry, wasRegistered := registeredBefore.Lookup(trimmedPath)
	if !wasRegistered {
		if pathExists {
			return failCleanup("managed worktree path is present but not registered with git; refusing to remove an unregistered path")
		}
		pathExists, err = managedWorktreePathExists(trimmedPath)
		if err != nil || pathExists {
			if err == nil {
				err = fmt.Errorf("managed worktree path appeared during cleanup")
			}
			return failCleanup(fmt.Sprintf("verify absent deregistered worktree path: %v", err))
		}
		updated, err := o.runWorktrees.MarkCleaned(ctx, claim, now)
		if err != nil {
			return false, fmt.Errorf("mark absent deregistered worktree cleaned: %w", err)
		}
		if !updated {
			return false, nil
		}
		claimReleased = true
		return true, nil
	}
	if entry.Locked {
		return failCleanup("managed git worktree is locked and cannot be cleaned automatically")
	}
	// Bounded deletion can remove .git before the checkout directory. Resume
	// that deletion only after its durable, fenced boundary has been crossed.
	if entry.Prunable && pathExists && !record.CleanupStarted {
		return failCleanup("managed git worktree is prunable while its path is present; refusing cleanup")
	}
	started, err := o.runWorktrees.BeginCleanupDeletion(ctx, claim)
	if err != nil {
		return false, fmt.Errorf("start managed run worktree deletion: %w", err)
	}
	if !started {
		return false, nil
	}
	if entry.Prunable && !pathExists {
		return o.pruneAndMarkRunWorktreeCleaned(ctx, claim, trimmedMainWorktreeRoot, trimmedPath, now, failCleanup)
	}

	progress, err := removeManagedWorktreePathBatch(trimmedPath, record.CleanupCursor, runWorktreeRemovalBatchSize)
	if err != nil {
		return failCleanup(fmt.Sprintf("remove worktree directory batch failed: %v", err))
	}
	if !progress.Complete {
		updated, err := o.runWorktrees.SaveCleanupProgress(ctx, claim, progress.Cursor, now)
		if err != nil {
			return false, fmt.Errorf("save run worktree removal progress: %w", err)
		}
		if !updated {
			return false, nil
		}
		claimReleased = true
		return false, nil
	}
	return o.pruneAndMarkRunWorktreeCleaned(ctx, claim, trimmedMainWorktreeRoot, trimmedPath, now, failCleanup)
}

func (o *orchestrationLoop) pruneAndMarkRunWorktreeCleaned(ctx context.Context, claim workspace.RunWorktreeCleanupClaim, mainWorktreeRoot, worktreePath, now string, failCleanup func(string) (bool, error)) (bool, error) {
	pathExists, err := managedWorktreePathExists(worktreePath)
	if err != nil {
		return failCleanup(fmt.Sprintf("inspect removed worktree path failed: %v", err))
	}
	if pathExists {
		return failCleanup("managed worktree path still exists before Git prune")
	}
	// The checkout is absent before Git is asked to prune its administrative
	// registration; provider-controlled checkout removal is never delegated to Git.
	if err := o.pruneWorktreesNow(mainWorktreeRoot); err != nil {
		return failCleanup(fmt.Sprintf("git worktree prune failed: %v", err))
	}
	registeredAfter, err := workspace.ListRegisteredWorktrees(mainWorktreeRoot, o.git())
	if err != nil {
		return failCleanup(fmt.Sprintf("registered git worktrees could not be listed after cleanup: %v", err))
	}
	if _, stillRegistered := registeredAfter.Lookup(worktreePath); stillRegistered {
		return failCleanup("git worktree is still registered after cleanup")
	}
	pathExists, err = managedWorktreePathExists(worktreePath)
	if err != nil {
		return failCleanup(fmt.Sprintf("inspect worktree directory failed: %v", err))
	}
	if pathExists {
		return failCleanup("managed worktree path appeared after deregistration")
	}
	updated, err := o.runWorktrees.MarkCleaned(ctx, claim, now)
	if err != nil {
		return false, fmt.Errorf("mark run worktree cleaned: %w", err)
	}
	return updated, nil
}

// discardQuarantinedManagedWorktreeForManualRecovery permits a pruned record
// only after its deterministic path is confirmed absent. Present unregistered
// paths are not Kingdom-owned and must never be touched by recovery.
func (o *orchestrationLoop) discardQuarantinedManagedWorktreeForManualRecovery(ctx context.Context, record workspace.RunWorktreeRecord, now string) (bool, error) {
	if o == nil || o.runWorktrees == nil {
		return false, fmt.Errorf("run worktree store is required")
	}
	if err := validateManagedRunWorktreeBoundary(record); err != nil {
		return false, fmt.Errorf("managed worktree boundary is invalid: %w", err)
	}
	if record.State == workspace.RunWorktreeStateCleaned {
		registered, err := workspace.ListRegisteredWorktrees(record.MainWorktreeRoot, o.git())
		if err != nil {
			return false, fmt.Errorf("registered git worktrees could not be listed: %w", err)
		}
		if _, ok := registered.Lookup(record.WorktreePath); ok {
			return false, fmt.Errorf("cleaned managed worktree is still registered with git")
		}
		pathExists, err := managedWorktreePathExists(record.WorktreePath)
		if err != nil || pathExists {
			if err == nil {
				err = fmt.Errorf("cleaned managed worktree path is present")
			}
			return false, fmt.Errorf("inspect cleaned managed worktree path: %w", err)
		}
		return true, nil
	}
	complete, err := o.cleanupRunWorktreeForManualRecovery(ctx, record, now)
	if err != nil || complete {
		return complete, err
	}
	updated, err := o.runWorktrees.GetByRun(ctx, record.RunID)
	if err != nil {
		return false, fmt.Errorf("reload managed worktree cleanup state: %w", err)
	}
	if updated.State == workspace.RunWorktreeStateCleanupFailed {
		if strings.TrimSpace(updated.FailureReason) == runWorktreeCleanupWaitingForExecutionReason {
			return false, nil
		}
		return false, fmt.Errorf("managed worktree cleanup failed: %s", strings.TrimSpace(updated.FailureReason))
	}
	return false, nil
}

func validateManagedRunWorktreeBoundary(record workspace.RunWorktreeRecord) error {
	if strings.TrimSpace(record.MainWorktreeRoot) == "" || strings.TrimSpace(record.WorktreeName) == "" {
		return fmt.Errorf("main worktree root and worktree name are required")
	}
	wantPath := workspace.ManagedRunWorktreePath(record.MainWorktreeRoot, record.WorktreeName)
	if filepath.Clean(strings.TrimSpace(record.WorktreePath)) != filepath.Clean(wantPath) {
		return fmt.Errorf("worktree path is outside the managed worktree root")
	}
	return nil
}

func removeManagedWorktreePathBatch(worktreePath, cursor string, batchSize int) (securefs.TreeRemovalProgress, error) {
	parent := filepath.Dir(strings.TrimSpace(worktreePath))
	root, err := securefs.OpenRoot(parent)
	if errors.Is(err, os.ErrNotExist) {
		return securefs.TreeRemovalProgress{Complete: true}, nil
	}
	if err != nil {
		return securefs.TreeRemovalProgress{}, err
	}
	defer root.Close()
	return root.RemoveTreeBatch(filepath.Base(worktreePath), cursor, batchSize)
}

func managedWorktreePathExists(worktreePath string) (bool, error) {
	_, err := os.Lstat(worktreePath)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (o *orchestrationLoop) markRunWorktreeCleanupFailed(ctx context.Context, claim workspace.RunWorktreeCleanupClaim, reason string) (bool, error) {
	if o == nil || o.runWorktrees == nil {
		return false, fmt.Errorf("run worktree store is required")
	}
	nextAttemptAt := o.now().UTC().Add(workspace.DefaultCleanupRetryBackoff).Format(time.RFC3339)
	updated, err := o.runWorktrees.MarkCleanupFailed(ctx, claim, nextAttemptAt, reason)
	if err != nil {
		return false, fmt.Errorf("mark run worktree cleanup failed: %w", err)
	}
	return updated, nil
}
