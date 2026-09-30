package workspace

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"kingdom/internal/processguardian"
)

const (
	RunWorktreeStateActive          = "active"
	RunWorktreeStateCleanupPending  = "cleanup_pending"
	RunWorktreeStateCleanupFailed   = "cleanup_failed"
	RunWorktreeStateCleaned         = "cleaned"
	DefaultRunWorktreeRetention     = time.Hour
	DefaultCleanupRetryBackoff      = time.Minute
	runWorktreeClaimReadbackTimeout = time.Second
)

type RunWorktreeRecord struct {
	RunID                     string
	RepoCommonDir             string
	MainWorktreeRoot          string
	ProjectRelPath            string
	WorktreeName              string
	WorktreePath              string
	SeedCommitOID             string
	State                     string
	CleanupCursor             string
	CleanupOwner              string
	CleanupOwnerPID           int
	CleanupOwnerIdentityToken string
	CleanupFence              int64
	CleanupStarted            bool
	CreatedAt                 string
	LastUsedAt                string
	CleanupAfter              sql.NullString
	CleanedAt                 sql.NullString
	FailureReason             string
}

type RunWorktreeCleanupPurpose string

const (
	RunWorktreeCleanupPurposeTerminal       RunWorktreeCleanupPurpose = "terminal"
	RunWorktreeCleanupPurposeManualRecovery RunWorktreeCleanupPurpose = "manual_recovery"
)

// RunWorktreeCleanupClaim is a durable owner/fence pair for one synchronous
// cleanup pass. The fence advances whenever ownership changes or activation
// invalidates a pre-deletion claim.
type RunWorktreeCleanupClaim struct {
	RunID   string
	Owner   string
	Fence   int64
	Purpose RunWorktreeCleanupPurpose
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
	record, err := scanRunWorktreeRecord(s.db.QueryRowContext(ctx, runWorktreeSelectByRun, trimmedRunID))
	if err != nil {
		return RunWorktreeRecord{}, err
	}
	return record, nil
}

const runWorktreeSelectByRun = `SELECT run_id, repo_common_dir, main_worktree_root, project_relpath, worktree_name, seed_commit_oid, state, cleanup_cursor, cleanup_owner, cleanup_owner_pid, cleanup_owner_identity_token, cleanup_fence, cleanup_started, created_at, last_used_at, cleanup_after, cleaned_at, failure_reason FROM run_worktrees WHERE run_id = ?`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanRunWorktreeRecord(row rowScanner) (RunWorktreeRecord, error) {
	var record RunWorktreeRecord
	var cleanupStarted int
	err := row.Scan(
		&record.RunID,
		&record.RepoCommonDir,
		&record.MainWorktreeRoot,
		&record.ProjectRelPath,
		&record.WorktreeName,
		&record.SeedCommitOID,
		&record.State,
		&record.CleanupCursor,
		&record.CleanupOwner,
		&record.CleanupOwnerPID,
		&record.CleanupOwnerIdentityToken,
		&record.CleanupFence,
		&cleanupStarted,
		&record.CreatedAt,
		&record.LastUsedAt,
		&record.CleanupAfter,
		&record.CleanedAt,
		&record.FailureReason,
	)
	if err != nil {
		return RunWorktreeRecord{}, err
	}
	record.CleanupStarted = cleanupStarted != 0
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
	result, err := s.db.ExecContext(ctx, `
INSERT INTO run_worktrees (run_id, repo_common_dir, main_worktree_root, project_relpath, worktree_name, seed_commit_oid, state, cleanup_cursor, cleanup_owner, cleanup_fence, cleanup_started, created_at, last_used_at, cleanup_after, cleaned_at, failure_reason)
VALUES (?, ?, ?, ?, ?, ?, ?, '', '', 0, 0, ?, ?, NULL, NULL, '')
ON CONFLICT(run_id) DO UPDATE SET
  repo_common_dir = excluded.repo_common_dir,
  main_worktree_root = excluded.main_worktree_root,
  project_relpath = excluded.project_relpath,
  worktree_name = excluded.worktree_name,
  seed_commit_oid = excluded.seed_commit_oid,
  state = ?,
  last_used_at = excluded.last_used_at,
  cleanup_cursor = '',
  cleanup_owner = '',
  cleanup_owner_pid = 0,
  cleanup_owner_identity_token = '',
  cleanup_fence = run_worktrees.cleanup_fence + 1,
  cleanup_started = 0,
  cleanup_after = NULL,
  cleaned_at = NULL,
	  failure_reason = ''
WHERE run_worktrees.state IN (?, ?)
  AND run_worktrees.cleanup_owner = ''
  AND run_worktrees.cleanup_started = 0`,
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
		RunWorktreeStateActive,
		RunWorktreeStateCleaned,
	)
	if err != nil {
		return err
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("verify run worktree activation: %w", err)
	}
	if updated != 1 {
		return fmt.Errorf("run worktree %q cannot be activated while cleanup is pending or has started", trimmedRunID)
	}
	return nil
}

func (s *RunWorktreeStore) ScheduleTerminalCleanup(ctx context.Context, cleanupAfter, cleanupAt string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("run worktree store is required")
	}
	trimmedCleanupAfter := strings.TrimSpace(cleanupAfter)
	if trimmedCleanupAfter == "" {
		return fmt.Errorf("cleanup_after is required")
	}
	trimmedCleanupAt := strings.TrimSpace(cleanupAt)
	if trimmedCleanupAt == "" {
		return fmt.Errorf("cleanup_at is required")
	}
	// Normal terminal worktrees receive the retention delay. A manual cleanup that
	// already crossed its deletion boundary is instead made due immediately once
	// its reopened run becomes terminal; its cursor and started bit stay intact.
	_, err := s.db.ExecContext(ctx, `
UPDATE run_worktrees
SET state = CASE WHEN state = ? AND cleanup_started = 0 THEN ? ELSE state END,
    cleanup_cursor = CASE WHEN state = ? AND cleanup_started = 0 THEN '' ELSE cleanup_cursor END,
    cleanup_owner = CASE WHEN state = ? AND cleanup_started = 0 THEN '' ELSE cleanup_owner END,
    cleanup_owner_pid = CASE WHEN state = ? AND cleanup_started = 0 THEN 0 ELSE cleanup_owner_pid END,
    cleanup_owner_identity_token = CASE WHEN state = ? AND cleanup_started = 0 THEN '' ELSE cleanup_owner_identity_token END,
    cleanup_fence = CASE WHEN state = ? AND cleanup_started = 0 THEN cleanup_fence + 1 ELSE cleanup_fence END,
    cleanup_started = CASE WHEN state = ? AND cleanup_started = 0 THEN 0 ELSE cleanup_started END,
    cleanup_after = CASE
      WHEN state = ? AND cleanup_started = 0 THEN ?
      WHEN state IN (?, ?) AND cleanup_started = 1 AND cleanup_after IS NULL THEN ?
      ELSE cleanup_after
    END,
    cleaned_at = CASE WHEN state = ? AND cleanup_started = 0 THEN NULL ELSE cleaned_at END,
    failure_reason = CASE WHEN state = ? AND cleanup_started = 0 THEN '' ELSE failure_reason END
WHERE cleanup_owner = ''
  AND ((state = ? AND cleanup_started = 0) OR (state IN (?, ?) AND cleanup_started = 1 AND cleanup_after IS NULL))
  AND run_id IN (
    SELECT run_id FROM workflow_runs WHERE run_status IN ('succeeded', 'failed', 'cancelled')
  )`,
		RunWorktreeStateActive,
		RunWorktreeStateCleanupPending,
		RunWorktreeStateActive,
		RunWorktreeStateActive,
		RunWorktreeStateActive,
		RunWorktreeStateActive,
		RunWorktreeStateActive,
		RunWorktreeStateActive,
		RunWorktreeStateActive,
		trimmedCleanupAfter,
		RunWorktreeStateCleanupPending,
		RunWorktreeStateCleanupFailed,
		trimmedCleanupAt,
		RunWorktreeStateActive,
		RunWorktreeStateActive,
		RunWorktreeStateActive,
		RunWorktreeStateCleanupPending,
		RunWorktreeStateCleanupFailed,
	)
	return err
}

// ClaimDueForCleanup claims one due terminal-run cleanup item. The record is
// returned from the same transaction as the owner/fence write, so callers
// never perform filesystem work from a stale cursor snapshot.
func (s *RunWorktreeStore) ClaimDueForCleanup(ctx context.Context, runID, dueAt, owner string) (RunWorktreeRecord, RunWorktreeCleanupClaim, bool, error) {
	return s.claimCleanup(ctx, runID, dueAt, owner, RunWorktreeCleanupPurposeTerminal)
}

// ClaimManualRecoveryCleanup claims the explicit pre-dispatch cleanup of a
// quarantined worktree. This path is authorized only for an already-reopened
// run and is separate from delayed terminal cleanup.
func (s *RunWorktreeStore) ClaimManualRecoveryCleanup(ctx context.Context, runID, owner string) (RunWorktreeRecord, RunWorktreeCleanupClaim, bool, error) {
	return s.claimCleanup(ctx, runID, "", owner, RunWorktreeCleanupPurposeManualRecovery)
}

func (s *RunWorktreeStore) claimCleanup(ctx context.Context, runID, dueAt, owner string, purpose RunWorktreeCleanupPurpose) (RunWorktreeRecord, RunWorktreeCleanupClaim, bool, error) {
	if s == nil || s.db == nil {
		return RunWorktreeRecord{}, RunWorktreeCleanupClaim{}, false, fmt.Errorf("run worktree store is required")
	}
	trimmedRunID := strings.TrimSpace(runID)
	trimmedOwner := strings.TrimSpace(owner)
	if trimmedRunID == "" || trimmedOwner == "" {
		return RunWorktreeRecord{}, RunWorktreeCleanupClaim{}, false, fmt.Errorf("run_id and cleanup owner are required")
	}
	statusClause, err := cleanupRunStatusClause(purpose)
	if err != nil {
		return RunWorktreeRecord{}, RunWorktreeCleanupClaim{}, false, err
	}
	ownerPID := os.Getpid()
	ownerIdentityToken, err := processguardian.ProcessIdentityToken(ownerPID)
	if err != nil {
		return RunWorktreeRecord{}, RunWorktreeCleanupClaim{}, false, fmt.Errorf("identify run worktree cleanup owner: %w", err)
	}

	query := `UPDATE run_worktrees SET cleanup_owner = ?, cleanup_owner_pid = ?, cleanup_owner_identity_token = ?, cleanup_fence = cleanup_fence + 1
WHERE run_id = ? AND cleanup_owner = ''
  AND EXISTS (SELECT 1 FROM workflow_runs wr WHERE wr.run_id = run_worktrees.run_id AND ` + statusClause + `)`
	args := []any{trimmedOwner, ownerPID, ownerIdentityToken, trimmedRunID}
	switch purpose {
	case RunWorktreeCleanupPurposeTerminal:
		trimmedDueAt := strings.TrimSpace(dueAt)
		if trimmedDueAt == "" {
			return RunWorktreeRecord{}, RunWorktreeCleanupClaim{}, false, fmt.Errorf("due_at is required")
		}
		query += ` AND state IN (?, ?) AND cleanup_after IS NOT NULL AND cleanup_after <= ?`
		args = append(args, RunWorktreeStateCleanupPending, RunWorktreeStateCleanupFailed, trimmedDueAt)
	case RunWorktreeCleanupPurposeManualRecovery:
		query += ` AND state IN (?, ?, ?)`
		args = append(args, RunWorktreeStateActive, RunWorktreeStateCleanupPending, RunWorktreeStateCleanupFailed)
	default:
		return RunWorktreeRecord{}, RunWorktreeCleanupClaim{}, false, fmt.Errorf("unsupported run worktree cleanup purpose %q", purpose)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RunWorktreeRecord{}, RunWorktreeCleanupClaim{}, false, fmt.Errorf("begin run worktree cleanup claim: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return RunWorktreeRecord{}, RunWorktreeCleanupClaim{}, false, fmt.Errorf("claim run worktree cleanup: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return RunWorktreeRecord{}, RunWorktreeCleanupClaim{}, false, fmt.Errorf("verify run worktree cleanup claim: %w", err)
	}
	if updated != 1 {
		return RunWorktreeRecord{}, RunWorktreeCleanupClaim{}, false, nil
	}
	record, err := scanRunWorktreeRecord(tx.QueryRowContext(ctx, runWorktreeSelectByRun, trimmedRunID))
	if err != nil {
		return RunWorktreeRecord{}, RunWorktreeCleanupClaim{}, false, fmt.Errorf("load claimed run worktree cleanup: %w", err)
	}
	if err := tx.Commit(); err != nil {
		// A commit error may follow a durable owner/fence write. Confirm that
		// exact claim with a detached bounded read before reporting failure; a
		// lost claim cannot be rediscovered by live cleanup scans.
		readbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), runWorktreeClaimReadbackTimeout)
		defer cancel()
		stored, readErr := scanRunWorktreeRecord(s.db.QueryRowContext(readbackCtx, runWorktreeSelectByRun, trimmedRunID))
		if readErr != nil {
			return RunWorktreeRecord{}, RunWorktreeCleanupClaim{}, false, fmt.Errorf("commit run worktree cleanup claim: %w (readback failed: %v)", err, readErr)
		}
		if stored.CleanupOwner != trimmedOwner || stored.CleanupOwnerPID != ownerPID || stored.CleanupOwnerIdentityToken != ownerIdentityToken || stored.CleanupFence != record.CleanupFence || stored.CleanupStarted != record.CleanupStarted || stored.State != record.State {
			return RunWorktreeRecord{}, RunWorktreeCleanupClaim{}, false, fmt.Errorf("commit run worktree cleanup claim: %w", err)
		}
		committed = true
		claim := RunWorktreeCleanupClaim{RunID: trimmedRunID, Owner: trimmedOwner, Fence: stored.CleanupFence, Purpose: purpose}
		return stored, claim, true, nil
	}
	committed = true
	claim := RunWorktreeCleanupClaim{RunID: trimmedRunID, Owner: trimmedOwner, Fence: record.CleanupFence, Purpose: purpose}
	return record, claim, true, nil
}

func cleanupRunStatusClause(purpose RunWorktreeCleanupPurpose) (string, error) {
	switch purpose {
	case RunWorktreeCleanupPurposeTerminal:
		return `wr.run_status IN ('succeeded', 'failed', 'cancelled')`, nil
	case RunWorktreeCleanupPurposeManualRecovery:
		return `wr.run_status = 'running'`, nil
	default:
		return "", fmt.Errorf("unsupported run worktree cleanup purpose %q", purpose)
	}
}

func cleanupMutationRunStatusClause(purpose RunWorktreeCleanupPurpose) (string, error) {
	if purpose == RunWorktreeCleanupPurposeManualRecovery {
		// Manual recovery is initiated only for a running run, but once its
		// irreversible deletion boundary is crossed, terminalization must not
		// prevent the current batch from persisting progress or completion.
		return `wr.run_status IN ('running', 'succeeded', 'failed', 'cancelled')`, nil
	}
	return cleanupRunStatusClause(purpose)
}

// BeginCleanupDeletion is the irreversible boundary. Retry/Rerun may cancel a
// claimed item before this write commits, but must reject after it does.
func (s *RunWorktreeStore) BeginCleanupDeletion(ctx context.Context, claim RunWorktreeCleanupClaim) (bool, error) {
	if s == nil || s.db == nil {
		return false, fmt.Errorf("run worktree store is required")
	}
	if err := validateRunWorktreeCleanupClaim(claim); err != nil {
		return false, err
	}
	statusClause, err := cleanupRunStatusClause(claim.Purpose)
	if err != nil {
		return false, err
	}
	stateClause := `state IN (?, ?, ?)`
	stateArgs := []any{RunWorktreeStateActive, RunWorktreeStateCleanupPending, RunWorktreeStateCleanupFailed}
	if claim.Purpose == RunWorktreeCleanupPurposeTerminal {
		stateClause = `state IN (?, ?)`
		stateArgs = []any{RunWorktreeStateCleanupPending, RunWorktreeStateCleanupFailed}
	}
	args := []any{RunWorktreeStateCleanupPending, strings.TrimSpace(claim.RunID), strings.TrimSpace(claim.Owner), claim.Fence}
	args = append(args, stateArgs...)
	result, err := s.db.ExecContext(ctx, `
UPDATE run_worktrees SET state = ?, cleanup_started = 1, failure_reason = ''
WHERE run_id = ? AND cleanup_owner = ? AND cleanup_fence = ? AND `+stateClause+`
  AND EXISTS (SELECT 1 FROM workflow_runs wr WHERE wr.run_id = run_worktrees.run_id AND `+statusClause+`)`, args...)
	if err != nil {
		return false, fmt.Errorf("begin run worktree cleanup deletion: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("verify run worktree cleanup deletion start: %w", err)
	}
	return updated == 1, nil
}

func (s *RunWorktreeStore) ListDueForCleanup(ctx context.Context, dueAt string, limit int) ([]RunWorktreeRecord, error) {
	return s.ListDueForCleanupAfter(ctx, dueAt, "", "", limit)
}

func (s *RunWorktreeStore) ListDueForCleanupAfter(ctx context.Context, dueAt, afterCleanupAt, afterRunID string, limit int) ([]RunWorktreeRecord, error) {
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
SELECT rw.run_id, rw.repo_common_dir, rw.main_worktree_root, rw.project_relpath, rw.worktree_name, rw.seed_commit_oid, rw.state, rw.cleanup_cursor, rw.cleanup_owner, rw.cleanup_owner_pid, rw.cleanup_owner_identity_token, rw.cleanup_fence, rw.cleanup_started, rw.created_at, rw.last_used_at, rw.cleanup_after, rw.cleaned_at, rw.failure_reason
FROM run_worktrees rw
JOIN workflow_runs wr ON wr.run_id = rw.run_id
WHERE rw.state IN (?, ?)
  AND rw.cleanup_owner = ''
  AND wr.run_status IN ('succeeded', 'failed', 'cancelled')
  AND rw.cleanup_after IS NOT NULL
  AND rw.cleanup_after <= ?
  AND (rw.cleanup_after, rw.run_id) > (?, ?)
ORDER BY rw.cleanup_after ASC, rw.run_id ASC
LIMIT ?`, RunWorktreeStateCleanupPending, RunWorktreeStateCleanupFailed, trimmedDueAt, afterCleanupAt, afterRunID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]RunWorktreeRecord, 0, limit)
	for rows.Next() {
		record, err := scanRunWorktreeRecord(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *RunWorktreeStore) MarkCleaned(ctx context.Context, claim RunWorktreeCleanupClaim, cleanedAt string) (bool, error) {
	if s == nil || s.db == nil {
		return false, fmt.Errorf("run worktree store is required")
	}
	if err := validateRunWorktreeCleanupClaim(claim); err != nil {
		return false, err
	}
	statusClause, err := cleanupMutationRunStatusClause(claim.Purpose)
	if err != nil {
		return false, err
	}
	result, err := s.db.ExecContext(ctx, `
UPDATE run_worktrees
SET state = ?, cleanup_cursor = '', cleanup_owner = '', cleanup_owner_pid = 0, cleanup_owner_identity_token = '', cleanup_started = 0, cleaned_at = ?, cleanup_after = NULL, failure_reason = ''
WHERE run_id = ? AND cleanup_owner = ? AND cleanup_fence = ?
  AND state IN (?, ?, ?)
  AND (state <> ? OR cleanup_started = 0)
  AND EXISTS (SELECT 1 FROM workflow_runs wr WHERE wr.run_id = run_worktrees.run_id AND `+statusClause+`)`,
		RunWorktreeStateCleaned,
		strings.TrimSpace(cleanedAt),
		strings.TrimSpace(claim.RunID),
		strings.TrimSpace(claim.Owner),
		claim.Fence,
		RunWorktreeStateActive,
		RunWorktreeStateCleanupPending,
		RunWorktreeStateCleanupFailed,
		RunWorktreeStateActive,
	)
	if err != nil {
		return false, fmt.Errorf("mark run worktree cleaned: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("verify run worktree cleaned state: %w", err)
	}
	return updated == 1, nil
}

func (s *RunWorktreeStore) SaveCleanupProgress(ctx context.Context, claim RunWorktreeCleanupClaim, cursor, nextAttemptAt string) (bool, error) {
	if s == nil || s.db == nil {
		return false, fmt.Errorf("run worktree store is required")
	}
	if err := validateRunWorktreeCleanupClaim(claim); err != nil {
		return false, err
	}
	trimmedNextAttemptAt := strings.TrimSpace(nextAttemptAt)
	if trimmedNextAttemptAt == "" {
		return false, fmt.Errorf("cleanup_after is required")
	}
	statusClause, err := cleanupMutationRunStatusClause(claim.Purpose)
	if err != nil {
		return false, err
	}
	result, err := s.db.ExecContext(ctx, `
UPDATE run_worktrees
SET state = ?, cleanup_cursor = ?, cleanup_after = ?, cleanup_owner = '', cleanup_owner_pid = 0, cleanup_owner_identity_token = '', failure_reason = ''
WHERE run_id = ? AND cleanup_owner = ? AND cleanup_fence = ? AND state = ? AND cleanup_started = 1
  AND EXISTS (SELECT 1 FROM workflow_runs wr WHERE wr.run_id = run_worktrees.run_id AND `+statusClause+`)`,
		RunWorktreeStateCleanupPending,
		cursor,
		trimmedNextAttemptAt,
		strings.TrimSpace(claim.RunID),
		strings.TrimSpace(claim.Owner),
		claim.Fence,
		RunWorktreeStateCleanupPending,
	)
	if err != nil {
		return false, fmt.Errorf("save run worktree cleanup progress: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("verify run worktree cleanup progress: %w", err)
	}
	return updated == 1, nil
}

func (s *RunWorktreeStore) MarkCleanupFailed(ctx context.Context, claim RunWorktreeCleanupClaim, nextAttemptAt, reason string) (bool, error) {
	if s == nil || s.db == nil {
		return false, fmt.Errorf("run worktree store is required")
	}
	if err := validateRunWorktreeCleanupClaim(claim); err != nil {
		return false, err
	}
	trimmedReason := strings.TrimSpace(reason)
	if trimmedReason == "" {
		trimmedReason = "run worktree cleanup failed"
	}
	statusClause, err := cleanupMutationRunStatusClause(claim.Purpose)
	if err != nil {
		return false, err
	}
	result, err := s.db.ExecContext(ctx, `
UPDATE run_worktrees
SET state = ?, cleanup_after = ?, cleanup_owner = '', cleanup_owner_pid = 0, cleanup_owner_identity_token = '', failure_reason = ?
WHERE run_id = ? AND cleanup_owner = ? AND cleanup_fence = ?
  AND state IN (?, ?, ?)
  AND EXISTS (SELECT 1 FROM workflow_runs wr WHERE wr.run_id = run_worktrees.run_id AND `+statusClause+`)`,
		RunWorktreeStateCleanupFailed,
		strings.TrimSpace(nextAttemptAt),
		trimmedReason,
		strings.TrimSpace(claim.RunID),
		strings.TrimSpace(claim.Owner),
		claim.Fence,
		RunWorktreeStateActive,
		RunWorktreeStateCleanupPending,
		RunWorktreeStateCleanupFailed,
	)
	if err != nil {
		return false, fmt.Errorf("mark run worktree cleanup failed: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("verify run worktree cleanup failure state: %w", err)
	}
	return updated == 1, nil
}

func (s *RunWorktreeStore) ReleaseCleanupClaim(ctx context.Context, claim RunWorktreeCleanupClaim) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("run worktree store is required")
	}
	if err := validateRunWorktreeCleanupClaim(claim); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `UPDATE run_worktrees SET cleanup_owner = '', cleanup_owner_pid = 0, cleanup_owner_identity_token = '' WHERE run_id = ? AND cleanup_owner = ? AND cleanup_fence = ?`, strings.TrimSpace(claim.RunID), strings.TrimSpace(claim.Owner), claim.Fence)
	if err != nil {
		return fmt.Errorf("release run worktree cleanup claim: %w", err)
	}
	return nil
}

// ReclaimAbandonedCleanupClaims fences claims only after the exact process
// generation that owned them is no longer present. A second runtime sharing
// the database must not take ownership from a live filesystem cleanup batch.
func (s *RunWorktreeStore) ReclaimAbandonedCleanupClaims(ctx context.Context, reclaimedAt string) error {
	var cursor RunWorktreeCleanupClaimCursor
	for {
		next, err := s.ReclaimAbandonedCleanupClaimsBatch(ctx, reclaimedAt, cursor, 64)
		if err != nil {
			return err
		}
		if next == (RunWorktreeCleanupClaimCursor{}) {
			return nil
		}
		cursor = next
	}
}

// RunWorktreeCleanupClaimCursor fixes the upper run-ID boundary of one
// inspection cycle so new runs cannot continually extend that cycle.
type RunWorktreeCleanupClaimCursor struct {
	AfterRunID   string
	ThroughRunID string
}

// ReclaimAbandonedCleanupClaimsBatch inspects at most limit process identities.
// A zero returned cursor completes the cycle. Individual inspection failures
// leave ownership unchanged and do not prevent other owners from being checked.
func (s *RunWorktreeStore) ReclaimAbandonedCleanupClaimsBatch(ctx context.Context, reclaimedAt string, cursor RunWorktreeCleanupClaimCursor, limit int) (RunWorktreeCleanupClaimCursor, error) {
	if s == nil || s.db == nil {
		return cursor, fmt.Errorf("run worktree store is required")
	}
	reclaimedAt = strings.TrimSpace(reclaimedAt)
	if reclaimedAt == "" {
		return cursor, fmt.Errorf("cleanup claim reclamation timestamp is required")
	}
	if limit <= 0 {
		return cursor, fmt.Errorf("cleanup claim reclamation limit must be positive")
	}
	if cursor.ThroughRunID == "" {
		if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(run_id), '') FROM run_worktrees WHERE cleanup_owner <> ''`).Scan(&cursor.ThroughRunID); err != nil {
			return cursor, fmt.Errorf("read run worktree cleanup claim high watermark: %w", err)
		}
		if cursor.ThroughRunID == "" {
			return RunWorktreeCleanupClaimCursor{}, nil
		}
	}
	type persistedOwner struct {
		runID, owner, identityToken string
		pid                         int
		fence                       int64
	}
	rows, err := s.db.QueryContext(ctx, `SELECT run_id, cleanup_owner, cleanup_fence, cleanup_owner_pid, cleanup_owner_identity_token
FROM run_worktrees WHERE cleanup_owner <> '' AND run_id > ? AND run_id <= ? ORDER BY run_id LIMIT ?`, cursor.AfterRunID, cursor.ThroughRunID, limit)
	if err != nil {
		return cursor, fmt.Errorf("reclaim abandoned run worktree cleanup claims: %w", err)
	}
	var owners []persistedOwner
	for rows.Next() {
		var owner persistedOwner
		if err := rows.Scan(&owner.runID, &owner.owner, &owner.fence, &owner.pid, &owner.identityToken); err != nil {
			_ = rows.Close()
			return cursor, fmt.Errorf("read run worktree cleanup claim owner: %w", err)
		}
		owners = append(owners, owner)
	}
	rowsErr := rows.Err()
	closeErr := rows.Close()
	if err := errors.Join(rowsErr, closeErr); err != nil {
		return cursor, fmt.Errorf("finish reading run worktree cleanup claim owners: %w", err)
	}
	var reclaimErr error
	for _, owner := range owners {
		if err := ctx.Err(); err != nil {
			return cursor, errors.Join(reclaimErr, err)
		}
		cursor.AfterRunID = owner.runID
		if owner.pid <= 0 || strings.TrimSpace(owner.identityToken) == "" {
			continue
		}
		matches, err := processguardian.ProcessIdentityMatches(owner.pid, owner.identityToken)
		if err != nil {
			reclaimErr = errors.Join(reclaimErr, fmt.Errorf("verify run worktree cleanup owner %q: %w", owner.owner, err))
			continue
		}
		if matches {
			continue
		}
		_, err = s.db.ExecContext(ctx, `UPDATE run_worktrees
SET cleanup_owner = '', cleanup_owner_pid = 0, cleanup_owner_identity_token = '', cleanup_fence = cleanup_fence + 1,
    cleanup_after = CASE WHEN cleanup_started = 1 AND cleanup_after IS NULL AND EXISTS (
      SELECT 1 FROM workflow_runs wr WHERE wr.run_id = run_worktrees.run_id AND wr.run_status IN ('succeeded', 'failed', 'cancelled')
    ) THEN ? ELSE cleanup_after END
WHERE run_id = ? AND cleanup_owner = ? AND cleanup_fence = ? AND cleanup_owner_pid = ? AND cleanup_owner_identity_token = ?`, reclaimedAt, owner.runID, owner.owner, owner.fence, owner.pid, owner.identityToken)
		if err != nil {
			reclaimErr = errors.Join(reclaimErr, fmt.Errorf("fence abandoned run worktree cleanup owner %q: %w", owner.owner, err))
		}
	}
	if len(owners) < limit || cursor.AfterRunID == cursor.ThroughRunID {
		cursor = RunWorktreeCleanupClaimCursor{}
	}
	return cursor, reclaimErr
}

func validateRunWorktreeCleanupClaim(claim RunWorktreeCleanupClaim) error {
	if strings.TrimSpace(claim.RunID) == "" || strings.TrimSpace(claim.Owner) == "" || claim.Fence <= 0 {
		return fmt.Errorf("run_id, cleanup owner, and positive cleanup fence are required")
	}
	if _, err := cleanupRunStatusClause(claim.Purpose); err != nil {
		return err
	}
	return nil
}
