package runtimefs

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"kingdom/internal/trackedfs"
)

type Store struct {
	db                *sql.DB
	root              string
	blobRoot          string
	blobFallbackRoots []string
	tempRoot          string
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

func (s *Store) AddBlobFallbackRoot(root string) error {
	if s == nil {
		return fmt.Errorf("runtime fs store is required")
	}
	cleanRoot, err := filepath.Abs(strings.TrimSpace(root))
	if err != nil {
		return fmt.Errorf("resolve blob fallback root: %w", err)
	}
	if strings.TrimSpace(root) == "" || cleanRoot == s.root {
		return nil
	}
	s.blobFallbackRoots = append(s.blobFallbackRoots, filepath.Join(cleanRoot, "snapshot-blobs"))
	return nil
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
	currentFiles, err := trackedfs.WalkTrackedFiles(rootPath)
	if err != nil {
		return SnapshotRecord{}, err
	}
	entries := make([]SnapshotEntry, 0)
	seen := make(map[string]struct{}, len(currentFiles))
	for _, file := range currentFiles {
		seen[file.RelativePath] = struct{}{}
		if base, ok := baseManifest[file.RelativePath]; ok && sameSnapshotEntryMetadata(base, file.Metadata) && base.EntryType == "file" {
			continue
		}
		raw, err := os.ReadFile(file.AbsolutePath)
		if err != nil {
			return SnapshotRecord{}, fmt.Errorf("read tracked file %q: %w", file.RelativePath, err)
		}
		blobID, err := s.writeBlob(raw)
		if err != nil {
			return SnapshotRecord{}, err
		}
		entries = append(entries, SnapshotEntry{
			Path:       file.RelativePath,
			EntryType:  "file",
			BlobID:     blobID,
			SizeBytes:  file.Metadata.SizeBytes,
			ModeBits:   file.Metadata.ModeBits,
			ModifiedNS: file.Metadata.ModifiedNS,
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
	manifest := make(map[string]SnapshotEntry)
	if s == nil || s.db == nil {
		return manifest, fmt.Errorf("runtime fs store is required")
	}
	trimmedSnapshotID := strings.TrimSpace(snapshotID)
	if trimmedSnapshotID == "" {
		return manifest, nil
	}
	var parentSnapshotID string
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(parent_snapshot_id, '') FROM filesystem_snapshots WHERE snapshot_id = ?`, trimmedSnapshotID).Scan(&parentSnapshotID)
	if err != nil {
		return nil, fmt.Errorf("load snapshot %q: %w", trimmedSnapshotID, err)
	}
	if strings.TrimSpace(parentSnapshotID) != "" {
		parentManifest, err := s.ResolveSnapshotManifest(ctx, parentSnapshotID)
		if err != nil {
			return nil, err
		}
		for path, entry := range parentManifest {
			manifest[path] = entry
		}
	}
	rows, err := s.db.QueryContext(ctx, `SELECT path, entry_type, blob_id, size_bytes, mode_bits, mtime_ns FROM filesystem_snapshot_entries WHERE snapshot_id = ? ORDER BY path ASC`, trimmedSnapshotID)
	if err != nil {
		return nil, fmt.Errorf("load snapshot entries %q: %w", trimmedSnapshotID, err)
	}
	defer rows.Close()
	for rows.Next() {
		var entry SnapshotEntry
		if err := rows.Scan(&entry.Path, &entry.EntryType, &entry.BlobID, &entry.SizeBytes, &entry.ModeBits, &entry.ModifiedNS); err != nil {
			return nil, fmt.Errorf("scan snapshot entry %q: %w", trimmedSnapshotID, err)
		}
		if trackedfs.IsProtectedRelativePath(entry.Path) {
			delete(manifest, entry.Path)
			continue
		}
		if entry.EntryType == "delete" {
			delete(manifest, entry.Path)
			continue
		}
		manifest[entry.Path] = entry
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate snapshot entries %q: %w", trimmedSnapshotID, err)
	}
	return manifest, nil
}

func (s *Store) CountChangesAgainstSnapshot(ctx context.Context, rootPath, snapshotID string) (int, error) {
	baseManifest, err := s.ResolveSnapshotManifest(ctx, snapshotID)
	if err != nil {
		return 0, err
	}
	currentFiles, err := trackedfs.WalkTrackedFiles(rootPath)
	if err != nil {
		return 0, err
	}
	count := 0
	seen := make(map[string]struct{}, len(currentFiles))
	for _, file := range currentFiles {
		seen[file.RelativePath] = struct{}{}
		if base, ok := baseManifest[file.RelativePath]; ok && sameSnapshotEntryMetadata(base, file.Metadata) && base.EntryType == "file" {
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
	manifest, err := s.ResolveSnapshotManifest(ctx, snapshotID)
	if err != nil {
		return err
	}
	currentFiles, err := trackedfs.WalkTrackedFiles(rootPath)
	if err != nil {
		return err
	}
	current := make(map[string]trackedfs.TrackedFile, len(currentFiles))
	for _, file := range currentFiles {
		current[file.RelativePath] = file
	}
	for path := range current {
		if _, ok := manifest[path]; ok {
			continue
		}
		resolved, err := trackedfs.ResolveWithinRoot(rootPath, path)
		if err != nil {
			return err
		}
		if err := os.Remove(resolved); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove restored file %q: %w", path, err)
		}
	}
	for path, entry := range manifest {
		currentFile, exists := current[path]
		if exists && sameSnapshotEntryMetadata(entry, currentFile.Metadata) {
			continue
		}
		resolved, err := trackedfs.ResolveWithinRoot(rootPath, path)
		if err != nil {
			return err
		}
		raw, err := s.ReadBlob(entry.BlobID)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(resolved), 0o755); err != nil {
			return fmt.Errorf("create restore directory for %q: %w", path, err)
		}
		if err := os.WriteFile(resolved, raw, os.FileMode(entry.ModeBits)); err != nil {
			return fmt.Errorf("write restored file %q: %w", path, err)
		}
		modifiedAt := time.Unix(0, entry.ModifiedNS)
		_ = os.Chtimes(resolved, modifiedAt, modifiedAt)
	}
	return nil
}

func (s *Store) RecoverMissingSnapshotBlobs(ctx context.Context, snapshotID string, sourceRoots ...string) error {
	manifest, err := s.ResolveSnapshotManifest(ctx, snapshotID)
	if err != nil {
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
			resolved, resolveErr := trackedfs.ResolveWithinRoot(sourceRoot, path)
			if resolveErr != nil {
				continue
			}
			raw, readErr := os.ReadFile(resolved)
			if readErr != nil {
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
	paths := make([]string, 0, len(s.blobFallbackRoots)+1)
	paths = append(paths, blobPath)
	for _, fallbackRoot := range s.blobFallbackRoots {
		paths = append(paths, filepath.Join(fallbackRoot, trimmedBlobID[:2], trimmedBlobID[2:4], trimmedBlobID))
	}
	for _, path := range paths {
		if _, err := os.Stat(path); err == nil {
			return true, nil
		} else if !os.IsNotExist(err) {
			return false, fmt.Errorf("stat blob %q: %w", trimmedBlobID, err)
		}
	}
	return false, nil
}

func (s *Store) ReadBlob(blobID string) ([]byte, error) {
	trimmedBlobID := strings.TrimSpace(blobID)
	if trimmedBlobID == "" {
		return nil, fmt.Errorf("blob id is required")
	}
	blobPath, err := s.blobPath(trimmedBlobID)
	if err != nil {
		return nil, err
	}
	raw, err := os.ReadFile(blobPath)
	if err == nil {
		return raw, nil
	}
	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read blob %q: %w", trimmedBlobID, err)
	}
	for _, fallbackRoot := range s.blobFallbackRoots {
		fallbackPath := filepath.Join(fallbackRoot, trimmedBlobID[:2], trimmedBlobID[2:4], trimmedBlobID)
		raw, fallbackErr := os.ReadFile(fallbackPath)
		if fallbackErr == nil {
			return raw, nil
		}
		if !os.IsNotExist(fallbackErr) {
			return nil, fmt.Errorf("read fallback blob %q: %w", trimmedBlobID, fallbackErr)
		}
	}
	return nil, fmt.Errorf("read blob %q: %w", trimmedBlobID, err)
}

func (s *Store) blobPath(blobID string) (string, error) {
	if len(blobID) < 4 {
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

func sameSnapshotEntryMetadata(entry SnapshotEntry, metadata trackedfs.Metadata) bool {
	return entry.SizeBytes == metadata.SizeBytes && entry.ModeBits == metadata.ModeBits && entry.ModifiedNS == metadata.ModifiedNS
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
