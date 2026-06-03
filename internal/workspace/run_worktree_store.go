package workspace

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

const (
	RunWorktreeStateActive         = "active"
	RunWorktreeStateCleanupPending = "cleanup_pending"
	RunWorktreeStateCleanupFailed  = "cleanup_failed"
	RunWorktreeStateCleaned        = "cleaned"
	DefaultRunWorktreeRetention    = time.Hour
	DefaultCleanupRetryBackoff     = time.Minute
)

type RunWorktreeRecord struct {
	RunID            string
	RepoCommonDir    string
	MainWorktreeRoot string
	ProjectRelPath   string
	WorktreeName     string
	WorktreePath     string
	SeedCommitOID    string
	State            string
	CreatedAt        string
	LastUsedAt       string
	CleanupAfter     sql.NullString
	CleanedAt        sql.NullString
	FailureReason    string
}

type RunWorktreeStore struct {
	db *sql.DB
}

func NewRunWorktreeStore(db *sql.DB) *RunWorktreeStore {
	if db == nil {
		return nil
	}
	return &RunWorktreeStore{db: db}
}

func (s *RunWorktreeStore) GetByRun(ctx context.Context, runID string) (RunWorktreeRecord, error) {
	if s == nil || s.db == nil {
		return RunWorktreeRecord{}, fmt.Errorf("run worktree store is required")
	}
	trimmedRunID := strings.TrimSpace(runID)
	var record RunWorktreeRecord
	err := s.db.QueryRowContext(ctx, `SELECT run_id, repo_common_dir, main_worktree_root, project_relpath, worktree_name, seed_commit_oid, state, created_at, last_used_at, cleanup_after, cleaned_at, failure_reason FROM run_worktrees WHERE run_id = ?`, trimmedRunID).Scan(
		&record.RunID,
		&record.RepoCommonDir,
		&record.MainWorktreeRoot,
		&record.ProjectRelPath,
		&record.WorktreeName,
		&record.SeedCommitOID,
		&record.State,
		&record.CreatedAt,
		&record.LastUsedAt,
		&record.CleanupAfter,
		&record.CleanedAt,
		&record.FailureReason,
	)
	if err != nil {
		return RunWorktreeRecord{}, err
	}
	record.WorktreePath = ManagedRunWorktreePath(record.MainWorktreeRoot, record.WorktreeName)
	return record, nil
}

func (s *RunWorktreeStore) UpsertActive(ctx context.Context, runID, repoCommonDir, mainWorktreeRoot, projectRelPath, worktreeName, seedCommitOID, now string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("run worktree store is required")
	}
	trimmedRunID := strings.TrimSpace(runID)
	trimmedRepoCommonDir := strings.TrimSpace(repoCommonDir)
	trimmedMainWorktreeRoot := strings.TrimSpace(mainWorktreeRoot)
	trimmedProjectRelPath := strings.TrimSpace(projectRelPath)
	trimmedWorktreeName := strings.TrimSpace(worktreeName)
	trimmedSeedCommitOID := strings.TrimSpace(seedCommitOID)
	trimmedNow := strings.TrimSpace(now)
	if trimmedRunID == "" || trimmedRepoCommonDir == "" || trimmedMainWorktreeRoot == "" || trimmedProjectRelPath == "" || trimmedWorktreeName == "" || trimmedSeedCommitOID == "" || trimmedNow == "" {
		return fmt.Errorf("run worktree record fields are required")
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO run_worktrees (run_id, repo_common_dir, main_worktree_root, project_relpath, worktree_name, seed_commit_oid, state, created_at, last_used_at, cleanup_after, cleaned_at, failure_reason)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, '')
ON CONFLICT(run_id) DO UPDATE SET
  repo_common_dir = excluded.repo_common_dir,
  main_worktree_root = excluded.main_worktree_root,
  project_relpath = excluded.project_relpath,
  worktree_name = excluded.worktree_name,
  seed_commit_oid = excluded.seed_commit_oid,
  state = ?,
  last_used_at = excluded.last_used_at,
  cleanup_after = NULL,
  cleaned_at = NULL,
  failure_reason = ''`,
		trimmedRunID,
		trimmedRepoCommonDir,
		trimmedMainWorktreeRoot,
		trimmedProjectRelPath,
		trimmedWorktreeName,
		trimmedSeedCommitOID,
		RunWorktreeStateActive,
		trimmedNow,
		trimmedNow,
		RunWorktreeStateActive,
	)
	return err
}

func (s *RunWorktreeStore) ScheduleTerminalCleanup(ctx context.Context, cleanupAfter string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("run worktree store is required")
	}
	trimmedCleanupAfter := strings.TrimSpace(cleanupAfter)
	if trimmedCleanupAfter == "" {
		return fmt.Errorf("cleanup_after is required")
	}
	_, err := s.db.ExecContext(ctx, `
UPDATE run_worktrees
SET state = ?, cleanup_after = ?, failure_reason = ''
WHERE state = ?
  AND run_id IN (
    SELECT run_id FROM workflow_runs WHERE run_status IN ('succeeded', 'failed', 'cancelled')
  )`,
		RunWorktreeStateCleanupPending,
		trimmedCleanupAfter,
		RunWorktreeStateActive,
	)
	return err
}

func (s *RunWorktreeStore) ListDueForCleanup(ctx context.Context, dueAt string, limit int) ([]RunWorktreeRecord, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("run worktree store is required")
	}
	if limit <= 0 {
		limit = 50
	}
	trimmedDueAt := strings.TrimSpace(dueAt)
	if trimmedDueAt == "" {
		return nil, fmt.Errorf("due_at is required")
	}
	rows, err := s.db.QueryContext(ctx, `
SELECT run_id, repo_common_dir, main_worktree_root, project_relpath, worktree_name, seed_commit_oid, state, created_at, last_used_at, cleanup_after, cleaned_at, failure_reason
FROM run_worktrees
WHERE state IN (?, ?)
  AND cleanup_after IS NOT NULL
  AND cleanup_after <= ?
ORDER BY cleanup_after ASC, run_id ASC
LIMIT ?`, RunWorktreeStateCleanupPending, RunWorktreeStateCleanupFailed, trimmedDueAt, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]RunWorktreeRecord, 0, limit)
	for rows.Next() {
		var record RunWorktreeRecord
		if err := rows.Scan(&record.RunID, &record.RepoCommonDir, &record.MainWorktreeRoot, &record.ProjectRelPath, &record.WorktreeName, &record.SeedCommitOID, &record.State, &record.CreatedAt, &record.LastUsedAt, &record.CleanupAfter, &record.CleanedAt, &record.FailureReason); err != nil {
			return nil, err
		}
		record.WorktreePath = ManagedRunWorktreePath(record.MainWorktreeRoot, record.WorktreeName)
		items = append(items, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *RunWorktreeStore) MarkCleaned(ctx context.Context, runID, cleanedAt string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("run worktree store is required")
	}
	_, err := s.db.ExecContext(ctx, `
UPDATE run_worktrees
SET state = ?, cleaned_at = ?, cleanup_after = NULL, failure_reason = ''
WHERE run_id = ?`, RunWorktreeStateCleaned, strings.TrimSpace(cleanedAt), strings.TrimSpace(runID))
	return err
}

func (s *RunWorktreeStore) MarkCleanupFailed(ctx context.Context, runID, nextAttemptAt, reason string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("run worktree store is required")
	}
	trimmedReason := strings.TrimSpace(reason)
	if trimmedReason == "" {
		trimmedReason = "run worktree cleanup failed"
	}
	_, err := s.db.ExecContext(ctx, `
UPDATE run_worktrees
SET state = ?, cleanup_after = ?, failure_reason = ?
WHERE run_id = ?`, RunWorktreeStateCleanupFailed, strings.TrimSpace(nextAttemptAt), trimmedReason, strings.TrimSpace(runID))
	return err
}
