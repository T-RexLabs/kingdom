package runtimefs

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"kingdom/internal/providerlimits"
	"kingdom/internal/securefs"
	"kingdom/internal/trackedfs"
)

type Store struct {
	db          *sql.DB
	root        string
	blobRoot    string
	tempRoot    string
	syncBlobDir func(string) error
}

type RunExecutionTarget struct {
	RunID               string
	TargetMode          string
	SourceRoot          string
	ExecutionRoot       string
	PublishRoot         string
	SourceSnapshotID    string
	TrustedSnapshotID   string
	PublishedSnapshotID string
	State               string
	CleanupCursor       string
	CreatedAt           string
	UpdatedAt           string
}

type ResponseOnlyWorkspaceCleanupCandidate struct {
	RunID     string
	AttemptID string
	MarkedAt  string
}

const executionTargetRemovalBatchSize = 128

type SnapshotRecord struct {
	SnapshotID       string
	RunID            string
	ParentSnapshotID string
	SnapshotKind     string
	CreatedAt        string
}

type SnapshotEntry struct {
	Path       string
	EntryType  string
	BlobID     string
	SizeBytes  int64
	ModeBits   int64
	ModifiedNS int64
}

type SnapshotDiffEntry struct {
	Path       string
	EntryType  string
	BlobID     string
	SizeBytes  int64
	ModeBits   int64
	ModifiedNS int64
}

type EnsureExecutionTargetRequest struct {
	RunID               string
	TargetMode          string
	SourceRoot          string
	ExecutionRoot       string
	PublishRoot         string
	SourceSnapshotID    string
	TrustedSnapshotID   string
	PublishedSnapshotID string
	State               string
	CreatedAt           string
	UpdatedAt           string
}

type CaptureSnapshotRequest struct {
	SnapshotID       string
	RunID            string
	ParentSnapshotID string
	SnapshotKind     string
	RootPath         string
	CreatedAt        string
}

func NewStore(db *sql.DB, root string) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("runtime fs database is required")
	}
	cleanRoot := strings.TrimSpace(root)
	if cleanRoot == "" {
		return nil, fmt.Errorf("runtime fs root is required")
	}
	cleanRoot = filepath.Clean(cleanRoot)
	blobRoot := filepath.Join(cleanRoot, "snapshot-blobs")
	tempRoot := filepath.Join(cleanRoot, "snapshot-tmp")
	for _, path := range []string{cleanRoot, blobRoot, tempRoot} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			return nil, fmt.Errorf("create runtime fs path %q: %w", path, err)
		}
	}
	store := &Store{db: db, root: cleanRoot, blobRoot: blobRoot, tempRoot: tempRoot}
	store.syncBlobDir = store.syncBlobDirectoryDurably
	return store, nil
}

func (s *Store) RunTargetRoot(runID string) string {
	return filepath.Join(s.root, "run-targets", sanitizeToken(runID), "target")
}

func (s *Store) ResponseOnlyWorkspaceRoot(runID, attemptID string) string {
	if s == nil || strings.TrimSpace(s.root) == "" {
		return ""
	}
	relative, err := responseOnlyWorkspaceRelativePath(runID, attemptID)
	if err != nil {
		return ""
	}
	return filepath.Join(s.root, filepath.FromSlash(relative))
}

// PrepareResponseOnlyWorkspace records cleanup debt before resetting an
// attempt-scoped isolated copy path. It deliberately creates no execution
// target or filesystem snapshot rows.
func (s *Store) PrepareResponseOnlyWorkspace(ctx context.Context, runID, attemptID, markedAt string) (string, error) {
	if s == nil || s.db == nil || strings.TrimSpace(s.root) == "" {
		return "", fmt.Errorf("runtime fs store is required")
	}
	runID = strings.TrimSpace(runID)
	attemptID = strings.TrimSpace(attemptID)
	markedAt = strings.TrimSpace(markedAt)
	if runID == "" || attemptID == "" || markedAt == "" {
		return "", fmt.Errorf("run_id, attempt_id, and cleanup marker time are required")
	}
	if _, err := time.Parse(time.RFC3339Nano, markedAt); err != nil {
		return "", fmt.Errorf("parse response-only workspace cleanup marker time: %w", err)
	}
	result, err := s.db.ExecContext(ctx, `UPDATE attempt_executions
		SET response_only_workspace_cleanup_pending_at = ?, updated_at = ?
		WHERE attempt_id = ? AND run_id = ? AND execution_mode = 'provider_backed'
		  AND state IN ('queued', 'starting', 'running')`, markedAt, markedAt, attemptID, runID)
	if err != nil {
		return "", fmt.Errorf("mark response-only workspace cleanup pending: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return "", fmt.Errorf("read response-only workspace cleanup marker: %w", err)
	}
	if rows != 1 {
		return "", fmt.Errorf("attempt %q is not active for a response-only workspace", attemptID)
	}
	relative, err := responseOnlyWorkspaceRelativePath(runID, attemptID)
	if err != nil {
		return "", err
	}
	root, err := securefs.OpenRoot(s.root)
	if err != nil {
		return "", fmt.Errorf("open runtime fs root for response-only workspace: %w", err)
	}
	defer root.Close()
	if err := removeResponseOnlyWorkspaceTree(ctx, root, relative); err != nil {
		return "", err
	}
	if err := root.EnsureDir(relative, 0o700); err != nil {
		return "", fmt.Errorf("create response-only workspace: %w", err)
	}
	if err := root.SyncDir(filepath.Dir(relative)); err != nil {
		return "", fmt.Errorf("sync response-only workspace parent: %w", err)
	}
	return s.ResponseOnlyWorkspaceRoot(runID, attemptID), nil
}

// Private workspace cleanup follows durable terminalization and projection. It
// must not reserve an attempt ahead of its finalizer or cancellation handoff.
func (s *Store) ListPendingResponseOnlyWorkspaceCleanups(ctx context.Context, afterAttemptID string, limit int) ([]ResponseOnlyWorkspaceCleanupCandidate, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("runtime fs store is required")
	}
	if limit <= 0 {
		return nil, fmt.Errorf("limit must be positive")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT ae.run_id, ae.attempt_id, ae.response_only_workspace_cleanup_pending_at
		FROM attempt_executions ae
		WHERE ae.attempt_id > ? AND ae.response_only_workspace_cleanup_pending_at IS NOT NULL
		  AND ae.state IN ('committed_success', 'committed_failure', 'committed_cancelled')
		  AND NOT EXISTS (
		    SELECT 1 FROM attempt_outbox ao WHERE ao.attempt_id = ae.attempt_id AND ao.status = 'pending'
		  )
		  AND NOT EXISTS (
		    SELECT 1 FROM attempt_cancel_outbox aco WHERE aco.attempt_id = ae.attempt_id AND aco.status = 'pending'
		  )
	ORDER BY ae.attempt_id ASC
	LIMIT ?`, strings.TrimSpace(afterAttemptID), limit)
	if err != nil {
		return nil, fmt.Errorf("list pending response-only workspace cleanups: %w", err)
	}
	defer rows.Close()
	candidates := make([]ResponseOnlyWorkspaceCleanupCandidate, 0, limit)
	for rows.Next() {
		var candidate ResponseOnlyWorkspaceCleanupCandidate
		if err := rows.Scan(&candidate.RunID, &candidate.AttemptID, &candidate.MarkedAt); err != nil {
			return nil, fmt.Errorf("scan response-only workspace cleanup candidate: %w", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate response-only workspace cleanup candidates: %w", err)
	}
	return candidates, nil
}

func (s *Store) ClearResponseOnlyWorkspaceCleanupPending(ctx context.Context, attemptID, expectedMarkedAt, updatedAt string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("runtime fs store is required")
	}
	attemptID = strings.TrimSpace(attemptID)
	expectedMarkedAt = strings.TrimSpace(expectedMarkedAt)
	updatedAt = strings.TrimSpace(updatedAt)
	if attemptID == "" || expectedMarkedAt == "" || updatedAt == "" {
		return fmt.Errorf("attempt_id, expected marker time, and updated_at are required")
	}
	if _, err := time.Parse(time.RFC3339Nano, expectedMarkedAt); err != nil {
		return fmt.Errorf("parse expected response-only workspace cleanup marker time: %w", err)
	}
	if _, err := time.Parse(time.RFC3339Nano, updatedAt); err != nil {
		return fmt.Errorf("parse response-only workspace cleanup update time: %w", err)
	}
	_, err := s.db.ExecContext(ctx, `UPDATE attempt_executions
		SET response_only_workspace_cleanup_pending_at = NULL, updated_at = ?
		WHERE attempt_id = ? AND response_only_workspace_cleanup_pending_at = ?`, updatedAt, attemptID, expectedMarkedAt)
	if err != nil {
		return fmt.Errorf("clear response-only workspace cleanup marker: %w", err)
	}
	return nil
}

// RemoveResponseOnlyWorkspaceContext removes one attempt-scoped isolated copy without
// following provider-created links.
func (s *Store) RemoveResponseOnlyWorkspaceContext(ctx context.Context, runID, attemptID string) error {
	if s == nil || strings.TrimSpace(s.root) == "" {
		return fmt.Errorf("runtime fs store is required")
	}
	relative, err := responseOnlyWorkspaceRelativePath(runID, attemptID)
	if err != nil {
		return err
	}
	root, err := securefs.OpenRoot(s.root)
	if err != nil {
		return fmt.Errorf("open runtime fs root for response-only workspace cleanup: %w", err)
	}
	defer root.Close()
	if err := removeResponseOnlyWorkspaceTree(ctx, root, relative); err != nil {
		return err
	}
	if err := root.SyncDir(filepath.Dir(relative)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("sync response-only workspace cleanup: %w", err)
	}
	return nil
}

func responseOnlyWorkspaceRelativePath(runID, attemptID string) (string, error) {
	runID = strings.TrimSpace(runID)
	attemptID = strings.TrimSpace(attemptID)
	if runID == "" || attemptID == "" {
		return "", fmt.Errorf("run_id and attempt_id are required for a response-only workspace")
	}
	return filepath.ToSlash(filepath.Join("response-only", sanitizeToken(runID), sanitizeToken(attemptID))), nil
}

func removeResponseOnlyWorkspaceTree(ctx context.Context, root *securefs.Root, relative string) error {
	if err := root.RemoveTree(ctx, relative, executionTargetRemovalBatchSize); err != nil {
		return fmt.Errorf("remove stale response-only workspace: %w", err)
	}
	return nil
}

func (s *Store) EnsureExecutionTarget(ctx context.Context, req EnsureExecutionTargetRequest) (RunExecutionTarget, error) {
	if s == nil || s.db == nil {
		return RunExecutionTarget{}, fmt.Errorf("runtime fs store is required")
	}
	record := RunExecutionTarget{
		RunID:               strings.TrimSpace(req.RunID),
		TargetMode:          strings.TrimSpace(req.TargetMode),
		SourceRoot:          strings.TrimSpace(req.SourceRoot),
		ExecutionRoot:       strings.TrimSpace(req.ExecutionRoot),
		PublishRoot:         strings.TrimSpace(req.PublishRoot),
		SourceSnapshotID:    strings.TrimSpace(req.SourceSnapshotID),
		TrustedSnapshotID:   strings.TrimSpace(req.TrustedSnapshotID),
		PublishedSnapshotID: strings.TrimSpace(req.PublishedSnapshotID),
		State:               firstNonEmpty(strings.TrimSpace(req.State), "trusted"),
		CreatedAt:           strings.TrimSpace(req.CreatedAt),
		UpdatedAt:           strings.TrimSpace(req.UpdatedAt),
	}
	if record.RunID == "" || record.TargetMode == "" || record.ExecutionRoot == "" || record.CreatedAt == "" || record.UpdatedAt == "" {
		return RunExecutionTarget{}, fmt.Errorf("execution target fields are required")
	}
	if err := os.MkdirAll(record.ExecutionRoot, 0o755); err != nil {
		return RunExecutionTarget{}, fmt.Errorf("create execution root: %w", err)
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO run_execution_targets (run_id, target_mode, source_root, execution_root, publish_root, source_snapshot_id, trusted_snapshot_id, published_snapshot_id, state, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(run_id) DO UPDATE SET
  target_mode = excluded.target_mode,
  source_root = excluded.source_root,
  execution_root = excluded.execution_root,
  publish_root = excluded.publish_root,
  source_snapshot_id = CASE WHEN TRIM(run_execution_targets.source_snapshot_id) = '' THEN excluded.source_snapshot_id ELSE run_execution_targets.source_snapshot_id END,
  trusted_snapshot_id = CASE WHEN TRIM(excluded.trusted_snapshot_id) = '' THEN run_execution_targets.trusted_snapshot_id ELSE excluded.trusted_snapshot_id END,
  published_snapshot_id = CASE WHEN TRIM(excluded.published_snapshot_id) = '' THEN run_execution_targets.published_snapshot_id ELSE excluded.published_snapshot_id END,
  state = excluded.state,
  cleanup_cursor = CASE WHEN excluded.state = 'unknown' THEN run_execution_targets.cleanup_cursor ELSE '' END,
  updated_at = excluded.updated_at`,
		record.RunID,
		record.TargetMode,
		record.SourceRoot,
		record.ExecutionRoot,
		record.PublishRoot,
		record.SourceSnapshotID,
		record.TrustedSnapshotID,
		record.PublishedSnapshotID,
		record.State,
		record.CreatedAt,
		record.UpdatedAt,
	)
	if err != nil {
		return RunExecutionTarget{}, fmt.Errorf("upsert execution target: %w", err)
	}
	return s.GetExecutionTarget(ctx, record.RunID)
}

func (s *Store) GetExecutionTarget(ctx context.Context, runID string) (RunExecutionTarget, error) {
	if s == nil || s.db == nil {
		return RunExecutionTarget{}, fmt.Errorf("runtime fs store is required")
	}
	var record RunExecutionTarget
	err := s.db.QueryRowContext(ctx, `SELECT run_id, target_mode, source_root, execution_root, publish_root, source_snapshot_id, trusted_snapshot_id, published_snapshot_id, state, cleanup_cursor, created_at, updated_at FROM run_execution_targets WHERE run_id = ?`, strings.TrimSpace(runID)).Scan(
		&record.RunID,
		&record.TargetMode,
		&record.SourceRoot,
		&record.ExecutionRoot,
		&record.PublishRoot,
		&record.SourceSnapshotID,
		&record.TrustedSnapshotID,
		&record.PublishedSnapshotID,
		&record.State,
		&record.CleanupCursor,
		&record.CreatedAt,
		&record.UpdatedAt,
	)
	if err != nil {
		return RunExecutionTarget{}, err
	}
	return record, nil
}

func (s *Store) UpdateExecutionTargetState(ctx context.Context, runID, state, trustedSnapshotID, publishedSnapshotID, updatedAt string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("runtime fs store is required")
	}
	_, err := s.db.ExecContext(ctx, `
UPDATE run_execution_targets
SET state = ?,
    trusted_snapshot_id = CASE WHEN TRIM(?) = '' THEN trusted_snapshot_id ELSE ? END,
    published_snapshot_id = CASE WHEN TRIM(?) = '' THEN published_snapshot_id ELSE ? END,
    cleanup_cursor = CASE WHEN ? = 'unknown' THEN cleanup_cursor ELSE '' END,
    updated_at = ?
WHERE run_id = ?`, strings.TrimSpace(state), strings.TrimSpace(trustedSnapshotID), strings.TrimSpace(trustedSnapshotID), strings.TrimSpace(publishedSnapshotID), strings.TrimSpace(publishedSnapshotID), strings.TrimSpace(state), strings.TrimSpace(updatedAt), strings.TrimSpace(runID))
	if err != nil {
		return fmt.Errorf("update execution target state: %w", err)
	}
	return nil
}

func (s *Store) UpdateExecutionTargetCleanupCursor(ctx context.Context, runID, cursor string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("runtime fs store is required")
	}
	result, err := s.db.ExecContext(ctx, `
UPDATE run_execution_targets
SET cleanup_cursor = ?
WHERE run_id = ? AND state = 'unknown'`, cursor, strings.TrimSpace(runID))
	if err != nil {
		return fmt.Errorf("persist execution target cleanup cursor: %w", err)
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("verify execution target cleanup cursor: %w", err)
	}
	if updated != 1 {
		return fmt.Errorf("execution target %q is not quarantined for cleanup", strings.TrimSpace(runID))
	}
	return nil
}

// RecoverQuarantinedExecutionTarget securely discards an unsafe provider-owned
// direct target, then recreates it from trusted runtime-owned bytes. Terminal
// attempt and target records are the durable quarantine evidence; provider
// filesystem content is never retained.
func (s *Store) RecoverQuarantinedExecutionTarget(ctx context.Context, runID, restoredAt string) (RunExecutionTarget, error) {
	record, err := s.GetExecutionTarget(ctx, runID)
	if err != nil {
		return RunExecutionTarget{}, fmt.Errorf("load quarantined execution target: %w", err)
	}
	if strings.TrimSpace(record.State) != "unknown" {
		return record, nil
	}
	if strings.TrimSpace(record.TargetMode) != "direct" || filepath.Clean(record.ExecutionRoot) != filepath.Clean(s.RunTargetRoot(record.RunID)) {
		return RunExecutionTarget{}, fmt.Errorf("quarantined execution target is not a runtime-owned direct target")
	}
	if strings.TrimSpace(record.TrustedSnapshotID) == "" {
		return RunExecutionTarget{}, fmt.Errorf("quarantined execution target has no trusted snapshot")
	}
	quiescent, err := s.runProviderCommandGuardiansQuiescent(ctx, record.RunID)
	if err != nil {
		return RunExecutionTarget{}, err
	}
	if !quiescent {
		return RunExecutionTarget{}, fmt.Errorf("quarantined execution target recovery is waiting for provider process quiescence")
	}
	if err := s.RecoverMissingSnapshotBlobs(ctx, record.TrustedSnapshotID, record.PublishRoot, record.SourceRoot); err != nil {
		return RunExecutionTarget{}, fmt.Errorf("recover trusted snapshot blobs: %w", err)
	}

	runRoot, err := securefs.OpenRoot(filepath.Dir(s.RunTargetRoot(record.RunID)))
	if err != nil {
		return RunExecutionTarget{}, fmt.Errorf("open runtime target root: %w", err)
	}
	defer runRoot.Close()
	progress, err := runRoot.RemoveTreeBatch("target", record.CleanupCursor, executionTargetRemovalBatchSize)
	if err != nil {
		return RunExecutionTarget{}, fmt.Errorf("discard unsafe execution target: %w", err)
	}
	if err := s.UpdateExecutionTargetCleanupCursor(ctx, record.RunID, progress.Cursor); err != nil {
		return RunExecutionTarget{}, err
	}
	record.CleanupCursor = progress.Cursor
	if !progress.Complete {
		return record, nil
	}
	if err := runRoot.EnsureDir("target", 0o755); err != nil {
		return RunExecutionTarget{}, fmt.Errorf("create fresh execution target: %w", err)
	}
	if err := runRoot.SyncDir("."); err != nil {
		return RunExecutionTarget{}, fmt.Errorf("sync fresh execution target root: %w", err)
	}
	if err := s.RestoreSnapshot(ctx, record.TrustedSnapshotID, record.ExecutionRoot); err != nil {
		return RunExecutionTarget{}, fmt.Errorf("restore fresh execution target: %w", err)
	}
	if err := s.UpdateExecutionTargetState(ctx, record.RunID, "trusted", record.TrustedSnapshotID, "", restoredAt); err != nil {
		return RunExecutionTarget{}, err
	}
	record.State = "trusted"
	record.UpdatedAt = strings.TrimSpace(restoredAt)
	return record, nil
}

func (s *Store) runProviderCommandGuardiansQuiescent(ctx context.Context, runID string) (bool, error) {
	var pending int
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1
		FROM provider_process_guards ppg
		JOIN attempt_executions ae ON ae.attempt_id = ppg.attempt_id
		WHERE ae.run_id = ? AND ppg.lifecycle <> 'quiescent'
	)`, strings.TrimSpace(runID)).Scan(&pending); err != nil {
		return false, fmt.Errorf("load direct target provider command guardian quiescence: %w", err)
	}
	return pending == 0, nil
}

func (s *Store) CaptureSnapshot(ctx context.Context, req CaptureSnapshotRequest) (SnapshotRecord, error) {
	if s == nil || s.db == nil {
		return SnapshotRecord{}, fmt.Errorf("runtime fs store is required")
	}
	record := SnapshotRecord{
		SnapshotID:       strings.TrimSpace(req.SnapshotID),
		RunID:            strings.TrimSpace(req.RunID),
		ParentSnapshotID: strings.TrimSpace(req.ParentSnapshotID),
		SnapshotKind:     strings.TrimSpace(req.SnapshotKind),
		CreatedAt:        strings.TrimSpace(req.CreatedAt),
	}
	rootPath := strings.TrimSpace(req.RootPath)
	if record.SnapshotID == "" || record.RunID == "" || record.SnapshotKind == "" || record.CreatedAt == "" || rootPath == "" {
		return SnapshotRecord{}, fmt.Errorf("snapshot capture fields are required")
	}
	index, err := s.newWorkspaceIndex(ctx)
	if err != nil {
		return SnapshotRecord{}, err
	}
	defer index.Close()
	parentDepth, err := s.loadSnapshotIndex(ctx, index, 0, record.ParentSnapshotID)
	if err != nil {
		return SnapshotRecord{}, err
	}
	compact := parentDepth >= providerlimits.SnapshotLineageDepth
	if compact {
		// A full manifest cuts the immutable overlay chain without changing the
		// snapshot's resolved filesystem state.
		record.ParentSnapshotID = ""
	}
	secureRoot, err := securefs.OpenRoot(rootPath)
	if err != nil {
		return SnapshotRecord{}, fmt.Errorf("open snapshot root: %w", err)
	}
	defer secureRoot.Close()
	// Index metadata privately during filesystem I/O; publish only after every
	// blob is durable. Neither the parent nor current tree lives in a Go map.
	insertEntry := func(entry SnapshotEntry) error {
		return index.put(ctx, 1, entry)
	}
	verifier, err := s.newBlobVerifier()
	if err != nil {
		return SnapshotRecord{}, err
	}
	defer verifier.root.Close()
	err = trackedfs.VisitTrackedFilesRoot(ctx, secureRoot, func(file trackedfs.TrackedFile) error {
		base, hasBase, err := index.get(ctx, 0, file.RelativePath)
		if err != nil {
			return err
		}
		if hasBase {
			if err := index.remove(ctx, 0, file.RelativePath); err != nil {
				return err
			}
		}
		blobID, metadata, err := digestTrackedFile(ctx, secureRoot, file.RelativePath)
		if err != nil {
			return err
		}
		if hasBase && base.EntryType == "file" {
			if sameSnapshotContentAndMode(base, blobID, metadata) {
				if compact {
					return insertEntry(base)
				}
				return nil
			}
		}
		exists, err := verifier.verify(ctx, blobID, metadata.SizeBytes, true)
		if err != nil {
			return err
		}
		if !exists {
			blobID, metadata, err = s.writeBlobFromTrackedFile(ctx, secureRoot, file.RelativePath, blobID)
			if err != nil {
				return err
			}
		}
		return insertEntry(SnapshotEntry{
			Path:       file.RelativePath,
			EntryType:  "file",
			BlobID:     blobID,
			SizeBytes:  metadata.SizeBytes,
			ModeBits:   metadata.ModeBits,
			ModifiedNS: metadata.ModifiedNS,
		})
	})
	if err != nil {
		return SnapshotRecord{}, err
	}
	if err := index.visit(ctx, 0, func(e SnapshotEntry) error {
		if e.EntryType == "delete" {
			return nil
		}
		return insertEntry(SnapshotEntry{Path: e.Path, EntryType: "delete"})
	}); err != nil {
		return SnapshotRecord{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SnapshotRecord{}, fmt.Errorf("begin snapshot transaction: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO filesystem_snapshots (snapshot_id, run_id, parent_snapshot_id, snapshot_kind, created_at) VALUES (?, ?, NULLIF(?, ''), ?, ?)`, record.SnapshotID, record.RunID, record.ParentSnapshotID, record.SnapshotKind, record.CreatedAt); err != nil {
		return SnapshotRecord{}, fmt.Errorf("insert snapshot: %w", err)
	}
	entryStmt, err := tx.PrepareContext(ctx, `INSERT INTO filesystem_snapshot_entries (snapshot_id, path, entry_type, blob_id, size_bytes, mode_bits, mtime_ns) VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return SnapshotRecord{}, err
	}
	defer entryStmt.Close()
	blobStmt, err := tx.PrepareContext(ctx, `INSERT OR IGNORE INTO filesystem_blobs (blob_id, size_bytes, created_at) VALUES (?, ?, ?)`)
	if err != nil {
		return SnapshotRecord{}, err
	}
	defer blobStmt.Close()
	err = index.visit(ctx, 1, func(entry SnapshotEntry) error {
		if _, err := entryStmt.ExecContext(ctx, record.SnapshotID, entry.Path, entry.EntryType, entry.BlobID, entry.SizeBytes, entry.ModeBits, entry.ModifiedNS); err != nil {
			return fmt.Errorf("insert snapshot entry %q: %w", entry.Path, err)
		}
		if entry.EntryType == "file" {
			if _, err := blobStmt.ExecContext(ctx, entry.BlobID, entry.SizeBytes, record.CreatedAt); err != nil {
				return fmt.Errorf("insert snapshot blob %q: %w", entry.BlobID, err)
			}
		}
		return nil
	})
	if err != nil {
		return SnapshotRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return SnapshotRecord{}, fmt.Errorf("commit snapshot transaction: %w", err)
	}
	return record, nil
}

func (s *Store) snapshotLineage(ctx context.Context, snapshotID string) ([]string, error) {
	currentSnapshotID := strings.TrimSpace(snapshotID)
	lineage := make([]string, 0, providerlimits.SnapshotLineageDepth)
	seen := make(map[string]struct{}, providerlimits.SnapshotLineageDepth)
	for currentSnapshotID != "" {
		if len(lineage) >= providerlimits.SnapshotLineageDepth {
			return nil, &providerlimits.DepthExceededError{Resource: "snapshot lineage", Limit: providerlimits.SnapshotLineageDepth}
		}
		if _, exists := seen[currentSnapshotID]; exists {
			return nil, fmt.Errorf("snapshot lineage cycle at %q", currentSnapshotID)
		}
		seen[currentSnapshotID] = struct{}{}
		lineage = append(lineage, currentSnapshotID)

		var parentSnapshotID string
		if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(parent_snapshot_id, '') FROM filesystem_snapshots WHERE snapshot_id = ?`, currentSnapshotID).Scan(&parentSnapshotID); err != nil {
			return nil, fmt.Errorf("load snapshot %q: %w", currentSnapshotID, err)
		}
		currentSnapshotID = strings.TrimSpace(parentSnapshotID)
	}
	return lineage, nil
}

func (s *Store) CountChangesAgainstSnapshot(ctx context.Context, rootPath, snapshotID string) (int, error) {
	index, err := s.newWorkspaceIndex(ctx)
	if err != nil {
		return 0, err
	}
	defer index.Close()
	if _, err := s.loadSnapshotIndex(ctx, index, 0, snapshotID); err != nil {
		return 0, err
	}
	secureRoot, err := securefs.OpenRoot(rootPath)
	if err != nil {
		return 0, fmt.Errorf("open change-count root: %w", err)
	}
	defer secureRoot.Close()
	count := 0
	err = trackedfs.VisitTrackedFilesRoot(ctx, secureRoot, func(file trackedfs.TrackedFile) error {
		base, ok, err := index.get(ctx, 0, file.RelativePath)
		if err != nil {
			return err
		}
		if ok {
			if err := index.remove(ctx, 0, file.RelativePath); err != nil {
				return err
			}
		}
		blobID, metadata, err := digestTrackedFile(ctx, secureRoot, file.RelativePath)
		if err != nil {
			return err
		}
		if ok && base.EntryType == "file" && sameSnapshotContentAndMode(base, blobID, metadata) {
			return nil
		}
		count++
		return nil
	})
	if err != nil {
		return 0, err
	}
	var deleted int
	if err := index.tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM entries WHERE space = 0 AND entry_type = 'file'`).Scan(&deleted); err != nil {
		return 0, err
	}
	return count + deleted, nil
}

func (s *Store) RestoreSnapshot(ctx context.Context, snapshotID, rootPath string) error {
	root, err := securefs.OpenRoot(rootPath)
	if err != nil {
		return fmt.Errorf("open restore root: %w", err)
	}
	defer root.Close()
	return s.RestoreSnapshotRoot(ctx, snapshotID, root)
}

// RestoreSnapshotRoot restores through an already verified directory descriptor.
// The caller retains ownership of root. It never reopens a mutable host path.
func (s *Store) RestoreSnapshotRoot(ctx context.Context, snapshotID string, secureRoot *securefs.Root) error {
	if secureRoot == nil {
		return fmt.Errorf("restore root is required")
	}
	index, err := s.newWorkspaceIndex(ctx)
	if err != nil {
		return err
	}
	defer index.Close()
	if _, err := s.loadSnapshotIndex(ctx, index, 0, snapshotID); err != nil {
		return err
	}
	verifier, err := s.newBlobVerifier()
	if err != nil {
		return err
	}
	defer verifier.root.Close()
	if err := index.visit(ctx, 0, func(entry SnapshotEntry) error {
		if entry.EntryType == "delete" {
			return nil
		}
		path := entry.Path
		blobID, metadata, digestErr := digestTrackedFile(ctx, secureRoot, path)
		if digestErr == nil && sameSnapshotContentAndMode(entry, blobID, metadata) {
			// Canonical bytes must be valid even when the destination matches.
			if exists, err := verifier.verify(ctx, entry.BlobID, entry.SizeBytes, false); err != nil {
				return err
			} else if !exists {
				return fmt.Errorf("snapshot blob %q is missing", entry.BlobID)
			}
			return nil
		}
		modifiedAt := time.Unix(0, entry.ModifiedNS)
		if err := secureRoot.ReplaceRegularFrom(ctx, path, func(w io.Writer) error { return s.copySnapshotBlob(ctx, entry, w) }, os.FileMode(entry.ModeBits), 0o755, modifiedAt); err != nil {
			return fmt.Errorf("write restored file %q: %w", path, err)
		}
		return nil
	}); err != nil {
		return err
	}
	if err := index.visit(ctx, 0, func(entry SnapshotEntry) error {
		if entry.EntryType != "delete" {
			return nil
		}
		path := entry.Path
		requiredParent, err := index.hasLiveDescendant(ctx, 0, path)
		if err != nil {
			return err
		}
		if requiredParent {
			current, err := secureRoot.ReadEntry(path)
			if err != nil {
				return fmt.Errorf("inspect restored parent %q: %w", path, err)
			}
			if current.Type == securefs.EntryTypeDirectory {
				// This tombstone deletes the old file, not the directory that
				// now contains live files in the resolved snapshot.
				return nil
			}
			return fmt.Errorf("restored parent %q is no longer a directory", path)
		}
		if err := secureRoot.RemoveEntry(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("remove snapshot deletion %q: %w", path, err)
		}
		return nil
	}); err != nil {
		return err
	}
	return trackedfs.VisitTrackedEntriesRoot(ctx, secureRoot, func(entry trackedfs.TrackedEntry) error {
		path := entry.RelativePath
		expected, required, err := index.get(ctx, 0, path)
		if err != nil {
			return err
		}
		if required && expected.EntryType == "file" {
			return nil
		}
		if err := secureRoot.RemoveEntryIfIdentity(path, entry.Type, entry.Identity); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("remove restored file %q: %w", path, err)
		}
		return nil
	})
}

func (s *Store) RecoverMissingSnapshotBlobs(ctx context.Context, snapshotID string, sourceRoots ...string) error {
	return s.VisitSnapshotManifest(ctx, snapshotID, func(entry SnapshotEntry) error {
		path := entry.Path
		if err := ctx.Err(); err != nil {
			return err
		}
		hasBlob, err := s.hasBlob(entry.BlobID)
		if err != nil {
			return err
		}
		if hasBlob {
			return nil
		}
		recovered := false
		for _, sourceRoot := range sourceRoots {
			if strings.TrimSpace(sourceRoot) == "" {
				continue
			}
			root, openErr := securefs.OpenRoot(sourceRoot)
			if openErr != nil {
				continue
			}
			blobID, metadata, readErr := s.writeBlobFromTrackedFile(ctx, root, path, entry.BlobID)
			closeErr := root.Close()
			if readErr != nil {
				if err := ctx.Err(); err != nil {
					return err
				}
				continue
			}
			if closeErr != nil {
				return fmt.Errorf("close snapshot recovery source: %w", closeErr)
			}
			if metadata.SizeBytes != entry.SizeBytes {
				continue
			}
			if blobID != entry.BlobID {
				return fmt.Errorf("recovered snapshot blob mismatch for %q", path)
			}
			recovered = true
			break
		}
		if !recovered {
			return fmt.Errorf("recover missing snapshot blob %q for %q: no source matched its digest", entry.BlobID, path)
		}
		return nil
	})
}

func (s *Store) hasBlob(blobID string) (bool, error) {
	trimmedBlobID := strings.TrimSpace(blobID)
	blobPath, err := s.blobPath(trimmedBlobID)
	if err != nil {
		return false, err
	}
	if _, err := os.Stat(blobPath); err == nil {
		return true, nil
	} else if !os.IsNotExist(err) {
		return false, fmt.Errorf("stat blob %q: %w", trimmedBlobID, err)
	}
	return false, nil
}

// CopyBlob streams canonical bytes, verifying both stable identity and digest.
// The destination must remain staged until this method succeeds.
func (s *Store) CopyBlob(ctx context.Context, blobID string, dst io.Writer) (int64, error) {
	trimmedBlobID := strings.TrimSpace(blobID)
	if trimmedBlobID == "" {
		return 0, fmt.Errorf("blob id is required")
	}
	_, err := s.blobPath(trimmedBlobID)
	if err != nil {
		return 0, err
	}
	root, err := securefs.OpenRoot(s.blobRoot)
	if err != nil {
		return 0, fmt.Errorf("open blob root: %w", err)
	}
	defer root.Close()
	relativePath := filepath.Join(trimmedBlobID[:2], trimmedBlobID[2:4], trimmedBlobID)
	identity, _, err := root.CopyRegular(ctx, relativePath, dst)
	if err != nil {
		return 0, fmt.Errorf("read blob %q: %w", trimmedBlobID, err)
	}
	if actual := identity.ContentSHA256; actual != trimmedBlobID {
		return 0, fmt.Errorf("read blob %q: digest mismatch (got %s)", trimmedBlobID, actual)
	}
	return identity.SizeBytes, nil
}

func (s *Store) blobPath(blobID string) (string, error) {
	decoded, err := hex.DecodeString(blobID)
	if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != blobID {
		return "", fmt.Errorf("invalid blob id %q", blobID)
	}
	return filepath.Join(s.blobRoot, blobID[:2], blobID[2:4], blobID), nil
}

func (s *Store) writeBlobFromTrackedFile(ctx context.Context, root *securefs.Root, path, expectedDigest string) (string, trackedfs.Metadata, error) {
	tempFile, err := os.CreateTemp(s.tempRoot, "blob-*")
	if err != nil {
		return "", trackedfs.Metadata{}, fmt.Errorf("create temp blob: %w", err)
	}
	tempPath := tempFile.Name()
	committed := false
	defer func() {
		_ = tempFile.Close()
		if !committed {
			_ = os.Remove(tempPath)
		}
	}()
	identity, info, err := root.CopyRegular(ctx, path, tempFile)
	if err != nil {
		return "", trackedfs.Metadata{}, fmt.Errorf("read tracked file %q: %w", path, err)
	}
	metadata := trackedfs.MetadataFromInfo(info)
	blobID := identity.ContentSHA256
	if expectedDigest != "" && blobID != expectedDigest {
		return "", trackedfs.Metadata{}, fmt.Errorf("tracked file %q: digest mismatch", path)
	}
	blobPath, err := s.blobPath(blobID)
	if err != nil {
		return "", trackedfs.Metadata{}, err
	}
	if _, err := os.Stat(blobPath); err == nil {
		if _, err := s.CopyBlob(ctx, blobID, io.Discard); err != nil {
			return "", trackedfs.Metadata{}, err
		}
		if err := s.syncBlobDirectory(blobID); err != nil {
			return "", trackedfs.Metadata{}, err
		}
		return blobID, metadata, nil
	} else if !os.IsNotExist(err) {
		return "", trackedfs.Metadata{}, fmt.Errorf("stat blob %q: %w", blobID, err)
	}
	if err := tempFile.Sync(); err != nil {
		return "", trackedfs.Metadata{}, fmt.Errorf("sync temp blob for %q: %w", path, err)
	}
	if err := tempFile.Close(); err != nil {
		return "", trackedfs.Metadata{}, fmt.Errorf("close temp blob for %q: %w", path, err)
	}
	if err := s.ensureBlobDirectory(blobID); err != nil {
		return "", trackedfs.Metadata{}, fmt.Errorf("create blob directory %q: %w", blobID, err)
	}
	if err := os.Rename(tempPath, blobPath); err != nil {
		if _, statErr := os.Stat(blobPath); statErr != nil {
			return "", trackedfs.Metadata{}, fmt.Errorf("commit blob %q: %w", blobID, err)
		}
		if _, err := s.CopyBlob(ctx, blobID, io.Discard); err != nil {
			return "", trackedfs.Metadata{}, err
		}
		if err := s.syncBlobDirectory(blobID); err != nil {
			return "", trackedfs.Metadata{}, err
		}
		return blobID, metadata, nil
	}
	committed = true
	if err := s.syncBlobDirectory(blobID); err != nil {
		return "", trackedfs.Metadata{}, err
	}
	return blobID, metadata, nil
}

func (s *Store) syncBlobDirectory(blobID string) error {
	if s.syncBlobDir == nil {
		return s.syncBlobDirectoryDurably(blobID)
	}
	return s.syncBlobDir(blobID)
}

func (s *Store) ensureBlobDirectory(blobID string) error {
	if _, err := s.blobPath(blobID); err != nil {
		return err
	}
	root, err := securefs.OpenRoot(s.blobRoot)
	if err != nil {
		return fmt.Errorf("open snapshot blob root: %w", err)
	}
	defer root.Close()
	path := filepath.Join(blobID[:2], blobID[2:4])
	if err := root.EnsureDir(path, 0o755); err != nil {
		return fmt.Errorf("create snapshot blob directory for %q: %w", blobID, err)
	}
	return nil
}

func (s *Store) syncBlobDirectoryDurably(blobID string) error {
	if _, err := s.blobPath(blobID); err != nil {
		return err
	}
	root, err := securefs.OpenRoot(s.blobRoot)
	if err != nil {
		return fmt.Errorf("open snapshot blob root: %w", err)
	}
	defer root.Close()
	path := filepath.Join(blobID[:2], blobID[2:4])
	if err := root.SyncDir(path); err != nil {
		return fmt.Errorf("sync snapshot blob directory for %q: %w", blobID, err)
	}
	return nil
}

func digestTrackedFile(ctx context.Context, root *securefs.Root, path string) (string, trackedfs.Metadata, error) {
	identity, info, err := root.CopyRegular(ctx, path, io.Discard)
	if err != nil {
		return "", trackedfs.Metadata{}, fmt.Errorf("hash tracked file %q: %w", path, err)
	}
	return identity.ContentSHA256, trackedfs.MetadataFromInfo(info), nil
}

func validateSnapshotEntry(entry SnapshotEntry) error {
	if !fs.ValidPath(entry.Path) || entry.Path == "." || (entry.EntryType != "file" && entry.EntryType != "delete") || entry.SizeBytes < 0 {
		return fmt.Errorf("invalid snapshot entry %q", entry.Path)
	}
	if entry.EntryType == "delete" {
		return nil
	}
	digest, err := hex.DecodeString(entry.BlobID)
	if err != nil || len(digest) != sha256.Size || hex.EncodeToString(digest) != entry.BlobID {
		return fmt.Errorf("invalid snapshot digest for %q", entry.Path)
	}
	return nil
}

func (s *Store) copySnapshotBlob(ctx context.Context, entry SnapshotEntry, dst io.Writer) error {
	n, err := s.CopyBlob(ctx, entry.BlobID, dst)
	if err != nil {
		return err
	}
	if n != entry.SizeBytes {
		return fmt.Errorf("read blob %q: size mismatch", entry.BlobID)
	}
	return nil
}

func sameSnapshotContentAndMode(entry SnapshotEntry, blobID string, metadata trackedfs.Metadata) bool {
	return entry.BlobID == blobID && entry.ModeBits == metadata.ModeBits
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func sanitizeToken(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "runtime"
	}
	var b strings.Builder
	for _, r := range trimmed {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	cleaned := strings.Trim(b.String(), "_")
	if cleaned == "" {
		return "runtime"
	}
	return cleaned
}
