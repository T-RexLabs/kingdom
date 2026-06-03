package orchestration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

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
	registeredBefore, err := workspace.ListRegisteredWorktrees(trimmedMainWorktreeRoot, o.git())
	if err != nil {
		return o.markRunWorktreeCleanupFailed(ctx, record.RunID, now, fmt.Sprintf("registered git worktrees could not be listed: %v", err))
	}
	entry, wasRegistered := registeredBefore.Lookup(trimmedPath)
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
	}
	if wasRegistered {
		if _, err := o.runGitCommand(trimmedMainWorktreeRoot, "worktree", "remove", "--force", trimmedPath); err != nil {
			return o.markRunWorktreeCleanupFailed(ctx, record.RunID, now, fmt.Sprintf("git worktree remove failed: %v", err))
		}
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
	if err := os.RemoveAll(trimmedPath); err != nil && !errors.Is(err, os.ErrNotExist) {
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
