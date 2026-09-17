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
	"sort"
	"strings"
	"time"

	"kingdom/internal/providerlimits"
	"kingdom/internal/securefs"
	"kingdom/internal/trackedfs"
)

type Store struct {
	db       *sql.DB
	root     string
	blobRoot string
	tempRoot string
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
	CreatedAt           string
	UpdatedAt           string
}

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
	return &Store{db: db, root: cleanRoot, blobRoot: blobRoot, tempRoot: tempRoot}, nil
}

func (s *Store) RunTargetRoot(runID string) string {
	return filepath.Join(s.root, "run-targets", sanitizeToken(runID), "target")
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
	err := s.db.QueryRowContext(ctx, `SELECT run_id, target_mode, source_root, execution_root, publish_root, source_snapshot_id, trusted_snapshot_id, published_snapshot_id, state, created_at, updated_at FROM run_execution_targets WHERE run_id = ?`, strings.TrimSpace(runID)).Scan(
		&record.RunID,
		&record.TargetMode,
		&record.SourceRoot,
		&record.ExecutionRoot,
		&record.PublishRoot,
		&record.SourceSnapshotID,
		&record.TrustedSnapshotID,
		&record.PublishedSnapshotID,
		&record.State,
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
    updated_at = ?
WHERE run_id = ?`, strings.TrimSpace(state), strings.TrimSpace(trustedSnapshotID), strings.TrimSpace(trustedSnapshotID), strings.TrimSpace(publishedSnapshotID), strings.TrimSpace(publishedSnapshotID), strings.TrimSpace(updatedAt), strings.TrimSpace(runID))
	if err != nil {
		return fmt.Errorf("update execution target state: %w", err)
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
	quiescent, err := s.runProviderProcessGuardsQuiescent(ctx, record.RunID)
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
	if err := runRoot.RemoveTree("target"); err != nil {
		return RunExecutionTarget{}, fmt.Errorf("discard unsafe execution target: %w", err)
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

func (s *Store) runProviderProcessGuardsQuiescent(ctx context.Context, runID string) (bool, error) {
	var pending int
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(
		SELECT 1
		FROM provider_process_guards ppg
		JOIN attempt_executions ae ON ae.attempt_id = ppg.attempt_id
		WHERE ae.run_id = ? AND ppg.owner_kind = 'decree_attempt' AND ppg.lifecycle <> 'quiescent'
	)`, strings.TrimSpace(runID)).Scan(&pending); err != nil {
		return false, fmt.Errorf("load direct target provider process guard quiescence: %w", err)
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
	baseManifest, err := s.ResolveSnapshotManifest(ctx, record.ParentSnapshotID)
	if err != nil {
		return SnapshotRecord{}, err
	}
	if err := validateSnapshotManifestLimits(baseManifest); err != nil {
		return SnapshotRecord{}, err
	}
	secureRoot, err := securefs.OpenRoot(rootPath)
	if err != nil {
		return SnapshotRecord{}, fmt.Errorf("open snapshot root: %w", err)
	}
	defer secureRoot.Close()
	currentFiles, err := trackedfs.WalkTrackedFilesRoot(secureRoot)
	if err != nil {
		return SnapshotRecord{}, err
	}
	entries := make([]SnapshotEntry, 0)
	seen := make(map[string]struct{}, len(currentFiles))
	for _, file := range currentFiles {
		seen[file.RelativePath] = struct{}{}
		base, hasBase := baseManifest[file.RelativePath]
		var blobID string
		var metadata trackedfs.Metadata
		if hasBase && base.EntryType == "file" {
			blobID, metadata, err = digestTrackedFile(secureRoot, file.RelativePath)
			if err != nil {
				return SnapshotRecord{}, err
			}
			if sameSnapshotContentAndMode(base, blobID, metadata) {
				continue
			}
			if blobID != base.BlobID {
				blobID, metadata, err = s.writeBlobFromTrackedFile(secureRoot, file.RelativePath)
			}
		} else {
			blobID, metadata, err = s.writeBlobFromTrackedFile(secureRoot, file.RelativePath)
		}
		if err != nil {
			return SnapshotRecord{}, err
		}
		entries = append(entries, SnapshotEntry{
			Path:       file.RelativePath,
			EntryType:  "file",
			BlobID:     blobID,
			SizeBytes:  metadata.SizeBytes,
			ModeBits:   metadata.ModeBits,
			ModifiedNS: metadata.ModifiedNS,
		})
	}
	for path := range baseManifest {
		if _, ok := seen[path]; ok {
			continue
		}
		entries = append(entries, SnapshotEntry{Path: path, EntryType: "delete"})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SnapshotRecord{}, fmt.Errorf("begin snapshot transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	if _, err := tx.ExecContext(ctx, `INSERT INTO filesystem_snapshots (snapshot_id, run_id, parent_snapshot_id, snapshot_kind, created_at) VALUES (?, ?, NULLIF(?, ''), ?, ?)`, record.SnapshotID, record.RunID, record.ParentSnapshotID, record.SnapshotKind, record.CreatedAt); err != nil {
		return SnapshotRecord{}, fmt.Errorf("insert snapshot: %w", err)
	}
	for _, entry := range entries {
		if _, err := tx.ExecContext(ctx, `INSERT INTO filesystem_snapshot_entries (snapshot_id, path, entry_type, blob_id, size_bytes, mode_bits, mtime_ns) VALUES (?, ?, ?, ?, ?, ?, ?)`, record.SnapshotID, entry.Path, entry.EntryType, entry.BlobID, entry.SizeBytes, entry.ModeBits, entry.ModifiedNS); err != nil {
			return SnapshotRecord{}, fmt.Errorf("insert snapshot entry %q: %w", entry.Path, err)
		}
		if entry.EntryType == "file" {
			if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO filesystem_blobs (blob_id, size_bytes, created_at) VALUES (?, ?, ?)`, entry.BlobID, entry.SizeBytes, record.CreatedAt); err != nil {
				return SnapshotRecord{}, fmt.Errorf("insert snapshot blob %q: %w", entry.BlobID, err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return SnapshotRecord{}, fmt.Errorf("commit snapshot transaction: %w", err)
	}
	committed = true
	return record, nil
}

func (s *Store) ResolveSnapshotManifest(ctx context.Context, snapshotID string) (map[string]SnapshotEntry, error) {
	manifest, _, err := s.resolveSnapshotState(ctx, snapshotID)
	return manifest, err
}

func (s *Store) resolveSnapshotState(ctx context.Context, snapshotID string) (map[string]SnapshotEntry, map[string]struct{}, error) {
	manifest := make(map[string]SnapshotEntry)
	deleted := make(map[string]struct{})
	if s == nil || s.db == nil {
		return manifest, deleted, fmt.Errorf("runtime fs store is required")
	}
	trimmedSnapshotID := strings.TrimSpace(snapshotID)
	if trimmedSnapshotID == "" {
		return manifest, deleted, nil
	}
	var parentSnapshotID string
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(parent_snapshot_id, '') FROM filesystem_snapshots WHERE snapshot_id = ?`, trimmedSnapshotID).Scan(&parentSnapshotID)
	if err != nil {
		return nil, nil, fmt.Errorf("load snapshot %q: %w", trimmedSnapshotID, err)
	}
	if strings.TrimSpace(parentSnapshotID) != "" {
		parentManifest, parentDeleted, err := s.resolveSnapshotState(ctx, parentSnapshotID)
		if err != nil {
			return nil, nil, err
		}
		for path, entry := range parentManifest {
			manifest[path] = entry
		}
		for path := range parentDeleted {
			deleted[path] = struct{}{}
		}
	}
	rows, err := s.db.QueryContext(ctx, `SELECT path, entry_type, blob_id, size_bytes, mode_bits, mtime_ns FROM filesystem_snapshot_entries WHERE snapshot_id = ? ORDER BY path ASC`, trimmedSnapshotID)
	if err != nil {
		return nil, nil, fmt.Errorf("load snapshot entries %q: %w", trimmedSnapshotID, err)
	}
	defer rows.Close()
	for rows.Next() {
		var entry SnapshotEntry
		if err := rows.Scan(&entry.Path, &entry.EntryType, &entry.BlobID, &entry.SizeBytes, &entry.ModeBits, &entry.ModifiedNS); err != nil {
			return nil, nil, fmt.Errorf("scan snapshot entry %q: %w", trimmedSnapshotID, err)
		}
		if trackedfs.IsProtectedRelativePath(entry.Path) {
			delete(manifest, entry.Path)
			delete(deleted, entry.Path)
			continue
		}
		if entry.EntryType == "delete" {
			delete(manifest, entry.Path)
			deleted[entry.Path] = struct{}{}
			continue
		}
		manifest[entry.Path] = entry
		delete(deleted, entry.Path)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("iterate snapshot entries %q: %w", trimmedSnapshotID, err)
	}
	return manifest, deleted, nil
}

func (s *Store) CountChangesAgainstSnapshot(ctx context.Context, rootPath, snapshotID string) (int, error) {
	baseManifest, err := s.ResolveSnapshotManifest(ctx, snapshotID)
	if err != nil {
		return 0, err
	}
	secureRoot, err := securefs.OpenRoot(rootPath)
	if err != nil {
		return 0, fmt.Errorf("open change-count root: %w", err)
	}
	defer secureRoot.Close()
	currentFiles, err := trackedfs.WalkTrackedFilesRoot(secureRoot)
	if err != nil {
		return 0, err
	}
	count := 0
	seen := make(map[string]struct{}, len(currentFiles))
	for _, file := range currentFiles {
		seen[file.RelativePath] = struct{}{}
		blobID, metadata, err := digestTrackedFile(secureRoot, file.RelativePath)
		if err != nil {
			return 0, err
		}
		if base, ok := baseManifest[file.RelativePath]; ok && base.EntryType == "file" && sameSnapshotContentAndMode(base, blobID, metadata) {
			continue
		}
		count++
	}
	for path := range baseManifest {
		if _, ok := seen[path]; !ok {
			count++
		}
	}
	return count, nil
}

func (s *Store) DiffSnapshots(ctx context.Context, leftSnapshotID, rightSnapshotID string) ([]SnapshotDiffEntry, error) {
	leftManifest, err := s.ResolveSnapshotManifest(ctx, leftSnapshotID)
	if err != nil {
		return nil, err
	}
	rightManifest, err := s.ResolveSnapshotManifest(ctx, rightSnapshotID)
	if err != nil {
		return nil, err
	}
	paths := make(map[string]struct{}, len(leftManifest)+len(rightManifest))
	for path := range leftManifest {
		paths[path] = struct{}{}
	}
	for path := range rightManifest {
		paths[path] = struct{}{}
	}
	sortedPaths := make([]string, 0, len(paths))
	for path := range paths {
		sortedPaths = append(sortedPaths, path)
	}
	sort.Strings(sortedPaths)
	diffs := make([]SnapshotDiffEntry, 0)
	for _, path := range sortedPaths {
		left, leftOK := leftManifest[path]
		right, rightOK := rightManifest[path]
		switch {
		case !leftOK && rightOK:
			diffs = append(diffs, SnapshotDiffEntry(pathEntry(right)))
		case leftOK && !rightOK:
			diffs = append(diffs, SnapshotDiffEntry{Path: path, EntryType: "delete"})
		case leftOK && rightOK && !sameSnapshotEntries(left, right):
			diffs = append(diffs, SnapshotDiffEntry(pathEntry(right)))
		}
	}
	return diffs, nil
}

func (s *Store) RestoreSnapshot(ctx context.Context, snapshotID, rootPath string) error {
	manifest, deleted, err := s.resolveSnapshotState(ctx, snapshotID)
	if err != nil {
		return err
	}
	if err := validateSnapshotManifestLimits(manifest); err != nil {
		return err
	}
	secureRoot, err := securefs.OpenRoot(rootPath)
	if err != nil {
		return fmt.Errorf("open restore root: %w", err)
	}
	defer secureRoot.Close()
	currentEntries, err := trackedfs.WalkTrackedEntriesRoot(secureRoot)
	if err != nil {
		return err
	}
	current := make(map[string]trackedfs.TrackedEntry, len(currentEntries))
	for _, entry := range currentEntries {
		current[entry.RelativePath] = entry
	}
	manifestPaths := make([]string, 0, len(manifest))
	manifestParents := make(map[string]struct{})
	for path := range manifest {
		manifestPaths = append(manifestPaths, path)
		for separator := strings.LastIndexByte(path, '/'); separator >= 0; separator = strings.LastIndexByte(path[:separator], '/') {
			manifestParents[path[:separator]] = struct{}{}
		}
	}
	sort.Strings(manifestPaths)
	for _, path := range manifestPaths {
		entry := manifest[path]
		raw, err := s.readSnapshotBlob(entry)
		if err != nil {
			return err
		}
		listed := current[path].Type == securefs.EntryTypeRegular
		if listed {
			blobID, metadata, digestErr := digestTrackedFile(secureRoot, path)
			if digestErr == nil && sameSnapshotContentAndMode(entry, blobID, metadata) {
				continue
			}
		}
		modifiedAt := time.Unix(0, entry.ModifiedNS)
		if err := secureRoot.ReplaceRegular(path, raw, os.FileMode(entry.ModeBits), 0o755, modifiedAt); err != nil {
			return fmt.Errorf("write restored file %q: %w", path, err)
		}
	}
	deletedPaths := make([]string, 0, len(deleted))
	for path := range deleted {
		deletedPaths = append(deletedPaths, path)
	}
	sort.Strings(deletedPaths)
	for _, path := range deletedPaths {
		if err := secureRoot.RemoveEntry(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("remove snapshot deletion %q: %w", path, err)
		}
	}
	currentPaths := make([]string, 0, len(current))
	for path := range current {
		currentPaths = append(currentPaths, path)
	}
	sort.Strings(currentPaths)
	for _, path := range currentPaths {
		_, requiredFile := manifest[path]
		_, requiredParent := manifestParents[path]
		if requiredFile || requiredParent {
			continue
		}
		entry := current[path]
		if err := secureRoot.RemoveEntryIfIdentity(path, entry.Type, entry.Identity); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("remove restored file %q: %w", path, err)
		}
	}
	return nil
}

func (s *Store) RecoverMissingSnapshotBlobs(ctx context.Context, snapshotID string, sourceRoots ...string) error {
	manifest, err := s.ResolveSnapshotManifest(ctx, snapshotID)
	if err != nil {
		return err
	}
	if err := validateSnapshotManifestLimits(manifest); err != nil {
		return err
	}
	for path, entry := range manifest {
		hasBlob, err := s.hasBlob(entry.BlobID)
		if err != nil {
			return err
		}
		if hasBlob {
			continue
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
			raw, readErr := root.ReadRegular(path, providerlimits.TrackedWorkspaceFileBytes)
			closeErr := root.Close()
			if readErr != nil {
				if errors.Is(readErr, securefs.ErrLimitExceeded) {
					return &providerlimits.LimitExceededError{Resource: "snapshot recovery source", Limit: providerlimits.TrackedWorkspaceFileBytes}
				}
				continue
			}
			if closeErr != nil {
				return fmt.Errorf("close snapshot recovery source: %w", closeErr)
			}
			if int64(len(raw)) != entry.SizeBytes {
				continue
			}
			sum := sha256.Sum256(raw)
			if hex.EncodeToString(sum[:]) != entry.BlobID {
				continue
			}
			blobID, writeErr := s.writeBlob(raw)
			if writeErr != nil {
				return fmt.Errorf("recover snapshot blob for %q: %w", path, writeErr)
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
	}
	return nil
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

func (s *Store) ReadBlob(blobID string) ([]byte, error) {
	trimmedBlobID := strings.TrimSpace(blobID)
	if trimmedBlobID == "" {
		return nil, fmt.Errorf("blob id is required")
	}
	_, err := s.blobPath(trimmedBlobID)
	if err != nil {
		return nil, err
	}
	root, err := securefs.OpenRoot(s.blobRoot)
	if err != nil {
		return nil, fmt.Errorf("open blob root: %w", err)
	}
	defer root.Close()
	relativePath := filepath.Join(trimmedBlobID[:2], trimmedBlobID[2:4], trimmedBlobID)
	raw, err := root.ReadRegular(relativePath, providerlimits.TrackedWorkspaceFileBytes)
	if err != nil {
		if errors.Is(err, securefs.ErrLimitExceeded) {
			return nil, &providerlimits.LimitExceededError{Resource: "snapshot blob", Limit: providerlimits.TrackedWorkspaceFileBytes}
		}
		return nil, fmt.Errorf("read blob %q: %w", trimmedBlobID, err)
	}
	sum := sha256.Sum256(raw)
	if actual := hex.EncodeToString(sum[:]); actual != trimmedBlobID {
		return nil, fmt.Errorf("read blob %q: digest mismatch (got %s)", trimmedBlobID, actual)
	}
	return raw, nil
}

func (s *Store) blobPath(blobID string) (string, error) {
	decoded, err := hex.DecodeString(blobID)
	if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != blobID {
		return "", fmt.Errorf("invalid blob id %q", blobID)
	}
	return filepath.Join(s.blobRoot, blobID[:2], blobID[2:4], blobID), nil
}

func (s *Store) writeBlob(raw []byte) (string, error) {
	sum := sha256.Sum256(raw)
	blobID := hex.EncodeToString(sum[:])
	blobPath, err := s.blobPath(blobID)
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(blobPath); err == nil {
		return blobID, nil
	} else if !os.IsNotExist(err) {
		return "", fmt.Errorf("stat blob %q: %w", blobID, err)
	}
	if err := os.MkdirAll(filepath.Dir(blobPath), 0o755); err != nil {
		return "", fmt.Errorf("create blob directory %q: %w", blobID, err)
	}
	tempFile, err := os.CreateTemp(s.tempRoot, "blob-*")
	if err != nil {
		return "", fmt.Errorf("create temp blob: %w", err)
	}
	tempPath := tempFile.Name()
	if _, err := tempFile.Write(raw); err != nil {
		_ = tempFile.Close()
		_ = os.Remove(tempPath)
		return "", fmt.Errorf("write temp blob %q: %w", blobID, err)
	}
	if err := tempFile.Sync(); err != nil {
		_ = tempFile.Close()
		_ = os.Remove(tempPath)
		return "", fmt.Errorf("sync temp blob %q: %w", blobID, err)
	}
	if err := tempFile.Close(); err != nil {
		_ = os.Remove(tempPath)
		return "", fmt.Errorf("close temp blob %q: %w", blobID, err)
	}
	if err := os.Rename(tempPath, blobPath); err != nil {
		if _, statErr := os.Stat(blobPath); statErr == nil {
			_ = os.Remove(tempPath)
			return blobID, nil
		}
		_ = os.Remove(tempPath)
		return "", fmt.Errorf("commit blob %q: %w", blobID, err)
	}
	return blobID, nil
}

func (s *Store) writeBlobFromTrackedFile(root *securefs.Root, path string) (string, trackedfs.Metadata, error) {
	file, err := root.OpenRegular(path)
	if err != nil {
		return "", trackedfs.Metadata{}, fmt.Errorf("open tracked file %q: %w", path, err)
	}
	defer file.Close()
	before, err := file.Stat()
	if err != nil {
		return "", trackedfs.Metadata{}, fmt.Errorf("stat tracked file %q: %w", path, err)
	}
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
	hasher := sha256.New()
	written, err := providerlimits.CopyBounded(io.MultiWriter(tempFile, hasher), file, providerlimits.TrackedWorkspaceFileBytes, "tracked workspace file")
	if err != nil {
		return "", trackedfs.Metadata{}, fmt.Errorf("read tracked file %q: %w", path, err)
	}
	after, err := file.Stat()
	if err != nil {
		return "", trackedfs.Metadata{}, fmt.Errorf("restat tracked file %q: %w", path, err)
	}
	metadata := trackedfs.MetadataFromInfo(after)
	if !sameTrackedFileState(before, after) || written != metadata.SizeBytes {
		return "", trackedfs.Metadata{}, fmt.Errorf("tracked file %q changed while capturing", path)
	}
	if err := tempFile.Sync(); err != nil {
		return "", trackedfs.Metadata{}, fmt.Errorf("sync temp blob for %q: %w", path, err)
	}
	if err := tempFile.Close(); err != nil {
		return "", trackedfs.Metadata{}, fmt.Errorf("close temp blob for %q: %w", path, err)
	}
	blobID := hex.EncodeToString(hasher.Sum(nil))
	blobPath, err := s.blobPath(blobID)
	if err != nil {
		return "", trackedfs.Metadata{}, err
	}
	if _, err := os.Stat(blobPath); err == nil {
		return blobID, metadata, nil
	} else if !os.IsNotExist(err) {
		return "", trackedfs.Metadata{}, fmt.Errorf("stat blob %q: %w", blobID, err)
	}
	if err := os.MkdirAll(filepath.Dir(blobPath), 0o755); err != nil {
		return "", trackedfs.Metadata{}, fmt.Errorf("create blob directory %q: %w", blobID, err)
	}
	if err := os.Rename(tempPath, blobPath); err != nil {
		if _, statErr := os.Stat(blobPath); statErr != nil {
			return "", trackedfs.Metadata{}, fmt.Errorf("commit blob %q: %w", blobID, err)
		}
		return blobID, metadata, nil
	}
	committed = true
	return blobID, metadata, nil
}

func digestTrackedFile(root *securefs.Root, path string) (string, trackedfs.Metadata, error) {
	file, err := root.OpenRegular(path)
	if err != nil {
		return "", trackedfs.Metadata{}, fmt.Errorf("open tracked file %q: %w", path, err)
	}
	defer file.Close()
	before, err := file.Stat()
	if err != nil {
		return "", trackedfs.Metadata{}, fmt.Errorf("stat tracked file %q: %w", path, err)
	}
	hasher := sha256.New()
	read, err := providerlimits.CopyBounded(hasher, file, providerlimits.TrackedWorkspaceFileBytes, "tracked workspace file")
	if err != nil {
		return "", trackedfs.Metadata{}, fmt.Errorf("hash tracked file %q: %w", path, err)
	}
	after, err := file.Stat()
	if err != nil {
		return "", trackedfs.Metadata{}, fmt.Errorf("restat tracked file %q: %w", path, err)
	}
	metadata := trackedfs.MetadataFromInfo(after)
	if !sameTrackedFileState(before, after) || read != metadata.SizeBytes {
		return "", trackedfs.Metadata{}, fmt.Errorf("tracked file %q changed while hashing", path)
	}
	return hex.EncodeToString(hasher.Sum(nil)), metadata, nil
}

func sameTrackedFileState(left, right fs.FileInfo) bool {
	return left.Size() == right.Size() && left.Mode() == right.Mode() && left.ModTime().Equal(right.ModTime())
}

func validateSnapshotManifestLimits(manifest map[string]SnapshotEntry) error {
	var budget providerlimits.WorkspaceBudget
	for _, entry := range manifest {
		if entry.EntryType != "file" {
			continue
		}
		if err := budget.AddFile(entry.SizeBytes); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) readSnapshotBlob(entry SnapshotEntry) ([]byte, error) {
	raw, err := s.ReadBlob(entry.BlobID)
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) != entry.SizeBytes {
		return nil, fmt.Errorf("read blob %q: size mismatch", entry.BlobID)
	}
	return raw, nil
}

func pathEntry(entry SnapshotEntry) SnapshotDiffEntry {
	return SnapshotDiffEntry{
		Path:       entry.Path,
		EntryType:  entry.EntryType,
		BlobID:     entry.BlobID,
		SizeBytes:  entry.SizeBytes,
		ModeBits:   entry.ModeBits,
		ModifiedNS: entry.ModifiedNS,
	}
}

func sameSnapshotContentAndMode(entry SnapshotEntry, blobID string, metadata trackedfs.Metadata) bool {
	return entry.BlobID == blobID && entry.ModeBits == metadata.ModeBits
}

func sameSnapshotEntries(left, right SnapshotEntry) bool {
	return left.Path == right.Path && left.EntryType == right.EntryType && left.BlobID == right.BlobID && left.SizeBytes == right.SizeBytes && left.ModeBits == right.ModeBits && left.ModifiedNS == right.ModifiedNS
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
