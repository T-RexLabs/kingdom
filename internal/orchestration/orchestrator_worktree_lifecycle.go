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

func (o *orchestrationLoop) scheduleRunWorktreeCleanup(ctx context.Context) error {
	if o == nil || o.runWorktrees == nil || workspace.RunWorktreeCleanupDisabled(o.worktreeRetention) {
		return nil
	}
	cleanupAfter := o.now().UTC().Add(o.worktreeRetention).Format(time.RFC3339)
	if err := o.runWorktrees.ScheduleTerminalCleanup(ctx, cleanupAfter); err != nil {
		return fmt.Errorf("schedule terminal run worktree cleanup: %w", err)
	}
	return nil
}

func (o *orchestrationLoop) drainRunWorktreeCleanup(ctx context.Context) error {
	if o == nil || o.runWorktrees == nil {
		return nil
	}
	now := o.now().UTC().Format(time.RFC3339)
	records, err := o.runWorktrees.ListDueForCleanup(ctx, now, runWorktreeCleanupBatchSize)
	if err != nil {
		return fmt.Errorf("list due run worktrees for cleanup: %w", err)
	}
	for _, record := range records {
		if err := o.cleanupRunWorktree(ctx, record, now); err != nil {
			return err
		}
	}
	return nil
}

func (o *orchestrationLoop) cleanupRunWorktree(ctx context.Context, record workspace.RunWorktreeRecord, now string) error {
	trimmedMainWorktreeRoot := strings.TrimSpace(record.MainWorktreeRoot)
	trimmedPath := strings.TrimSpace(record.WorktreePath)
	if trimmedMainWorktreeRoot == "" || trimmedPath == "" {
		return o.markRunWorktreeCleanupFailed(ctx, record.RunID, now, "run worktree record is incomplete")
	}
	if err := validateManagedRunWorktreeBoundary(record); err != nil {
		return o.markRunWorktreeCleanupFailed(ctx, record.RunID, now, fmt.Sprintf("managed worktree boundary is invalid: %v", err))
	}
	if o.executionStore != nil {
		quiescent, err := o.executionStore.RunProviderProcessGuardsQuiescent(ctx, record.RunID)
		if err != nil {
			return err
		}
		if !quiescent {
			return o.markRunWorktreeCleanupFailed(ctx, record.RunID, now, "managed provider process quiescence has not been proven")
		}
	}
	registeredBefore, err := workspace.ListRegisteredWorktrees(trimmedMainWorktreeRoot, o.git())
	if err != nil {
		return o.markRunWorktreeCleanupFailed(ctx, record.RunID, now, fmt.Sprintf("registered git worktrees could not be listed: %v", err))
	}
	entry, wasRegistered := registeredBefore.Lookup(trimmedPath)
	if !wasRegistered {
		return o.markRunWorktreeCleanupFailed(ctx, record.RunID, now, "managed worktree is not registered with git; refusing to remove an unregistered path")
	}
	if wasRegistered && entry.Locked {
		return o.markRunWorktreeCleanupFailed(ctx, record.RunID, now, "managed git worktree is locked and cannot be cleaned automatically")
	}
	if wasRegistered && entry.Prunable {
		if err := o.pruneWorktreesNow(trimmedMainWorktreeRoot); err != nil {
			return o.markRunWorktreeCleanupFailed(ctx, record.RunID, now, fmt.Sprintf("git worktree prune failed: %v", err))
		}
		registeredBefore, err = workspace.ListRegisteredWorktrees(trimmedMainWorktreeRoot, o.git())
		if err != nil {
			return o.markRunWorktreeCleanupFailed(ctx, record.RunID, now, fmt.Sprintf("registered git worktrees could not be listed after prune: %v", err))
		}
		entry, wasRegistered = registeredBefore.Lookup(trimmedPath)
		if !wasRegistered {
			return o.markRunWorktreeCleanupFailed(ctx, record.RunID, now, "managed worktree is no longer registered with git; refusing to remove an unregistered path")
		}
	}
	if _, err := o.runGitCommand(trimmedMainWorktreeRoot, "worktree", "remove", "--force", trimmedPath); err != nil {
		return o.markRunWorktreeCleanupFailed(ctx, record.RunID, now, fmt.Sprintf("git worktree remove failed: %v", err))
	}
	if err := o.pruneWorktreesNow(trimmedMainWorktreeRoot); err != nil {
		return o.markRunWorktreeCleanupFailed(ctx, record.RunID, now, fmt.Sprintf("git worktree prune failed: %v", err))
	}
	registeredAfter, err := workspace.ListRegisteredWorktrees(trimmedMainWorktreeRoot, o.git())
	if err != nil {
		return o.markRunWorktreeCleanupFailed(ctx, record.RunID, now, fmt.Sprintf("registered git worktrees could not be listed after cleanup: %v", err))
	}
	if _, stillRegistered := registeredAfter.Lookup(trimmedPath); stillRegistered {
		return o.markRunWorktreeCleanupFailed(ctx, record.RunID, now, "git worktree is still registered after cleanup")
	}
	if err := removeManagedWorktreePath(trimmedPath); err != nil {
		return o.markRunWorktreeCleanupFailed(ctx, record.RunID, now, fmt.Sprintf("remove worktree directory failed: %v", err))
	}
	if _, err := os.Stat(trimmedPath); err == nil {
		return o.markRunWorktreeCleanupFailed(ctx, record.RunID, now, "managed worktree path still exists after cleanup")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return o.markRunWorktreeCleanupFailed(ctx, record.RunID, now, fmt.Sprintf("inspect worktree directory failed: %v", err))
	}
	if err := o.runWorktrees.MarkCleaned(ctx, record.RunID, now); err != nil {
		return fmt.Errorf("mark run worktree cleaned: %w", err)
	}
	return nil
}

// discardQuarantinedManagedWorktreeForManualRecovery permits a pruned record
// only after its deterministic path is confirmed absent. Present unregistered
// paths are not Kingdom-owned and must never be touched by recovery.
func (o *orchestrationLoop) discardQuarantinedManagedWorktreeForManualRecovery(ctx context.Context, record workspace.RunWorktreeRecord, now string) error {
	if o == nil || o.runWorktrees == nil {
		return fmt.Errorf("run worktree store is required")
	}
	if err := validateManagedRunWorktreeBoundary(record); err != nil {
		return fmt.Errorf("managed worktree boundary is invalid: %w", err)
	}
	registered, err := workspace.ListRegisteredWorktrees(record.MainWorktreeRoot, o.git())
	if err != nil {
		return fmt.Errorf("registered git worktrees could not be listed: %w", err)
	}
	if _, ok := registered.Lookup(record.WorktreePath); ok {
		return o.cleanupRunWorktree(ctx, record, now)
	}
	if _, err := os.Lstat(record.WorktreePath); err == nil {
		return fmt.Errorf("managed worktree path is present but not registered with git; refusing to remove an unregistered path")
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect unregistered managed worktree path: %w", err)
	}
	if err := o.runWorktrees.MarkCleaned(ctx, record.RunID, now); err != nil {
		return fmt.Errorf("mark absent managed worktree record cleaned: %w", err)
	}
	return nil
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

func removeManagedWorktreePath(worktreePath string) error {
	parent := filepath.Dir(strings.TrimSpace(worktreePath))
	root, err := securefs.OpenRoot(parent)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer root.Close()
	return root.RemoveTree(filepath.Base(worktreePath))
}

func (o *orchestrationLoop) markRunWorktreeCleanupFailed(ctx context.Context, runID, now, reason string) error {
	if o == nil || o.runWorktrees == nil {
		return fmt.Errorf("run worktree store is required")
	}
	nextAttemptAt := o.now().UTC().Add(workspace.DefaultCleanupRetryBackoff).Format(time.RFC3339)
	if err := o.runWorktrees.MarkCleanupFailed(ctx, runID, nextAttemptAt, reason); err != nil {
		return fmt.Errorf("mark run worktree cleanup failed: %w", err)
	}
	return nil
}
