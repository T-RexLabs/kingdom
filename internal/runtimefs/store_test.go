package runtimefs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kingdom/internal/dbmigrations"
	"kingdom/internal/providerlimits"
	"kingdom/internal/state"

	_ "modernc.org/sqlite"
)

func TestCaptureRestoreAndDiffSnapshots(t *testing.T) {
	t.Parallel()

	db := openRuntimeFSTestDB(t)
	applyRuntimeFSMigrations(t, db)
	store, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	seedRuntimeFSRun(t, db, "run_1")

	root := t.TempDir()
	writeTestFile(t, root, "src/app.js", "console.log('v1')")
	writeTestFile(t, root, "docs/readme.md", "hello")

	base, err := store.CaptureSnapshot(context.Background(), CaptureSnapshotRequest{
		SnapshotID:   "snap_base",
		RunID:        "run_1",
		SnapshotKind: "baseline",
		RootPath:     root,
		CreatedAt:    "2026-03-01T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("capture base snapshot: %v", err)
	}

	writeTestFile(t, root, "src/app.js", "console.log('v2')")
	if err := os.Remove(filepath.Join(root, "docs", "readme.md")); err != nil {
		t.Fatalf("remove tracked file: %v", err)
	}
	writeTestFile(t, root, "src/new.js", "export const value = 1")

	result, err := store.CaptureSnapshot(context.Background(), CaptureSnapshotRequest{
		SnapshotID:       "snap_result",
		RunID:            "run_1",
		ParentSnapshotID: base.SnapshotID,
		SnapshotKind:     "attempt_result",
		RootPath:         root,
		CreatedAt:        "2026-03-01T12:01:00Z",
	})
	if err != nil {
		t.Fatalf("capture result snapshot: %v", err)
	}

	diffs, err := store.DiffSnapshots(context.Background(), base.SnapshotID, result.SnapshotID)
	if err != nil {
		t.Fatalf("diff snapshots: %v", err)
	}
	if len(diffs) != 3 {
		t.Fatalf("expected 3 diff entries, got %d", len(diffs))
	}
	if diffs[0].Path != "docs/readme.md" || diffs[0].EntryType != "delete" {
		t.Fatalf("unexpected delete diff %#v", diffs[0])
	}
	if diffs[1].Path != "src/app.js" || diffs[1].EntryType != "file" {
		t.Fatalf("unexpected update diff %#v", diffs[1])
	}
	if diffs[2].Path != "src/new.js" || diffs[2].EntryType != "file" {
		t.Fatalf("unexpected create diff %#v", diffs[2])
	}

	restoreRoot := t.TempDir()
	if err := store.RestoreSnapshot(context.Background(), result.SnapshotID, restoreRoot); err != nil {
		t.Fatalf("restore snapshot: %v", err)
	}
	assertTestFile(t, restoreRoot, "src/app.js", "console.log('v2')")
	assertTestFile(t, restoreRoot, "src/new.js", "export const value = 1")
	if _, err := os.Stat(filepath.Join(restoreRoot, "docs", "readme.md")); !os.IsNotExist(err) {
		t.Fatalf("expected deleted file to stay deleted, got err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(restoreRoot, ".kingdom")); !os.IsNotExist(err) {
		t.Fatalf("expected protected directory to remain untouched in restore target, got err=%v", err)
	}
}

func TestRecoverQuarantinedExecutionTargetDiscardsDirtyGeneration(t *testing.T) {
	t.Parallel()

	db := openRuntimeFSTestDB(t)
	applyRuntimeFSMigrations(t, db)
	store, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	const runID = "run_quarantined_target"
	const capturedAt = "2026-03-01T12:00:00Z"
	seedRuntimeFSRun(t, db, runID)
	sourceRoot := t.TempDir()
	writeTestFile(t, sourceRoot, "baseline.txt", "trusted baseline")
	snapshot, err := store.CaptureSnapshot(context.Background(), CaptureSnapshotRequest{
		SnapshotID: "snap_quarantined_target", RunID: runID, SnapshotKind: "baseline", RootPath: sourceRoot, CreatedAt: capturedAt,
	})
	if err != nil {
		t.Fatalf("capture trusted snapshot: %v", err)
	}
	targetRoot := store.RunTargetRoot(runID)
	if err := os.MkdirAll(targetRoot, 0o755); err != nil {
		t.Fatalf("create runtime target: %v", err)
	}
	writeTestFile(t, targetRoot, "dirty.txt", "provider-owned content")
	outsidePath := filepath.Join(t.TempDir(), "outside.txt")
	if err := os.WriteFile(outsidePath, []byte("outside stays unchanged"), 0o600); err != nil {
		t.Fatalf("write outside guard: %v", err)
	}
	if err := os.Symlink(outsidePath, filepath.Join(targetRoot, "provider-link")); err != nil {
		t.Fatalf("create provider symlink: %v", err)
	}
	if _, err := store.EnsureExecutionTarget(context.Background(), EnsureExecutionTargetRequest{
		RunID: runID, TargetMode: "direct", SourceRoot: sourceRoot, ExecutionRoot: targetRoot, PublishRoot: sourceRoot,
		SourceSnapshotID: snapshot.SnapshotID, TrustedSnapshotID: snapshot.SnapshotID, PublishedSnapshotID: snapshot.SnapshotID,
		State: "unknown", CreatedAt: capturedAt, UpdatedAt: capturedAt,
	}); err != nil {
		t.Fatalf("create quarantined execution target: %v", err)
	}
	oldTarget, err := os.Stat(targetRoot)
	if err != nil {
		t.Fatalf("stat dirty target: %v", err)
	}

	recovered, err := store.RecoverQuarantinedExecutionTarget(context.Background(), runID, "2026-03-01T12:01:00Z")
	if err != nil {
		t.Fatalf("recover quarantined execution target: %v", err)
	}
	if recovered.State != "trusted" || recovered.ExecutionRoot != targetRoot {
		t.Fatalf("recovered target=%#v, want trusted canonical target", recovered)
	}
	assertTestFile(t, targetRoot, "baseline.txt", "trusted baseline")
	if _, err := os.Lstat(filepath.Join(targetRoot, "dirty.txt")); !os.IsNotExist(err) {
		t.Fatalf("dirty target content remained in fresh target: %v", err)
	}
	newTarget, err := os.Stat(targetRoot)
	if err != nil {
		t.Fatalf("stat fresh target: %v", err)
	}
	if os.SameFile(oldTarget, newTarget) {
		t.Fatal("recovery reused the quarantined target directory")
	}
	if _, err := os.Lstat(filepath.Join(filepath.Dir(targetRoot), "quarantine")); !os.IsNotExist(err) {
		t.Fatalf("provider-owned quarantine content was retained: %v", err)
	}
	if raw, err := os.ReadFile(outsidePath); err != nil || string(raw) != "outside stays unchanged" {
		t.Fatalf("recovery touched provider symlink target: bytes=%q err=%v", raw, err)
	}
}

func TestResolveSnapshotManifestDropsNewlyProtectedHistoricalPaths(t *testing.T) {
	t.Parallel()

	db := openRuntimeFSTestDB(t)
	applyRuntimeFSMigrations(t, db)
	store, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	seedRuntimeFSRun(t, db, "run_protected_history")
	mustRuntimeFSExec(t, db, `INSERT INTO filesystem_snapshots (snapshot_id, run_id, snapshot_kind, created_at) VALUES ('snap_protected_history', 'run_protected_history', 'baseline', '2026-03-01T12:00:00Z')`)
	mustRuntimeFSExec(t, db, `INSERT INTO filesystem_blobs (blob_id, size_bytes, created_at) VALUES ('missing_protected_blob', 10, '2026-03-01T12:00:00Z')`)
	mustRuntimeFSExec(t, db, `INSERT INTO filesystem_snapshot_entries (snapshot_id, path, entry_type, blob_id, size_bytes, mode_bits, mtime_ns) VALUES ('snap_protected_history', '.playwright-mcp/page.yml', 'file', 'missing_protected_blob', 10, 420, 0)`)

	manifest, err := store.ResolveSnapshotManifest(context.Background(), "snap_protected_history")
	if err != nil {
		t.Fatalf("resolve protected historical manifest: %v", err)
	}
	if len(manifest) != 0 {
		t.Fatalf("expected protected historical path to be removed, got %#v", manifest)
	}
	if err := store.RestoreSnapshot(context.Background(), "snap_protected_history", t.TempDir()); err != nil {
		t.Fatalf("restore should not read a protected historical blob: %v", err)
	}
}

func TestReadBlobRejectsCorruptCanonicalBytes(t *testing.T) {
	t.Parallel()

	db := openRuntimeFSTestDB(t)
	applyRuntimeFSMigrations(t, db)
	store, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	seedRuntimeFSRun(t, db, "run_corrupt_blob")
	sourceRoot := t.TempDir()
	writeTestFile(t, sourceRoot, "src/app.txt", "canonical snapshot content")
	if _, err := store.CaptureSnapshot(context.Background(), CaptureSnapshotRequest{
		SnapshotID:   "snap_corrupt_blob",
		RunID:        "run_corrupt_blob",
		SnapshotKind: "baseline",
		RootPath:     sourceRoot,
		CreatedAt:    "2026-03-01T12:00:00Z",
	}); err != nil {
		t.Fatalf("capture snapshot: %v", err)
	}
	manifest, err := store.ResolveSnapshotManifest(context.Background(), "snap_corrupt_blob")
	if err != nil {
		t.Fatalf("resolve snapshot: %v", err)
	}
	entry := manifest["src/app.txt"]
	blobPath, err := store.blobPath(entry.BlobID)
	if err != nil {
		t.Fatalf("resolve blob path: %v", err)
	}
	if err := os.WriteFile(blobPath, []byte("corrupt snapshot content"), 0o644); err != nil {
		t.Fatalf("corrupt canonical blob: %v", err)
	}
	if _, err := store.ReadBlob(entry.BlobID); err == nil {
		t.Fatal("expected corrupt canonical blob read to fail")
	}
	if err := store.RestoreSnapshot(context.Background(), "snap_corrupt_blob", sourceRoot); err == nil {
		t.Fatal("expected restore to reject corrupt canonical blob")
	}
}

func TestCaptureSnapshotHashesFilesWithMatchingMetadata(t *testing.T) {
	t.Parallel()

	db := openRuntimeFSTestDB(t)
	applyRuntimeFSMigrations(t, db)
	store, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	seedRuntimeFSRun(t, db, "run_capture_digest")
	root := t.TempDir()
	writeTestFile(t, root, "same.txt", "aaaa")
	path := filepath.Join(root, "same.txt")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat original file: %v", err)
	}
	base, err := store.CaptureSnapshot(context.Background(), CaptureSnapshotRequest{
		SnapshotID: "snap_digest_base", RunID: "run_capture_digest", SnapshotKind: "baseline", RootPath: root, CreatedAt: "2026-03-01T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("capture base snapshot: %v", err)
	}
	if err := os.WriteFile(path, []byte("bbbb"), info.Mode().Perm()); err != nil {
		t.Fatalf("replace file content: %v", err)
	}
	if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatalf("restore file time: %v", err)
	}

	changes, err := store.CountChangesAgainstSnapshot(context.Background(), root, base.SnapshotID)
	if err != nil {
		t.Fatalf("count content changes: %v", err)
	}
	if changes != 1 {
		t.Fatalf("expected one digest change, got %d", changes)
	}
	child, err := store.CaptureSnapshot(context.Background(), CaptureSnapshotRequest{
		SnapshotID: "snap_digest_child", RunID: "run_capture_digest", ParentSnapshotID: base.SnapshotID, SnapshotKind: "attempt_result", RootPath: root, CreatedAt: "2026-03-01T12:01:00Z",
	})
	if err != nil {
		t.Fatalf("capture changed snapshot: %v", err)
	}
	baseManifest, err := store.ResolveSnapshotManifest(context.Background(), base.SnapshotID)
	if err != nil {
		t.Fatalf("resolve base manifest: %v", err)
	}
	childManifest, err := store.ResolveSnapshotManifest(context.Background(), child.SnapshotID)
	if err != nil {
		t.Fatalf("resolve child manifest: %v", err)
	}
	if baseManifest["same.txt"].BlobID == childManifest["same.txt"].BlobID {
		t.Fatal("matching size, mode, and mtime must not hide changed content")
	}
}

func TestCaptureSnapshotCompactsLineageAtDepthLimit(t *testing.T) {
	t.Parallel()

	db := openRuntimeFSTestDB(t)
	applyRuntimeFSMigrations(t, db)
	store, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	seedRuntimeFSRun(t, db, "run_lineage_compaction")
	root := t.TempDir()
	writeTestFile(t, root, "stable.txt", "stable")
	writeTestFile(t, root, "removed.txt", "remove me")

	parentSnapshotID := ""
	for index := 0; index <= providerlimits.SnapshotLineageDepth; index++ {
		if index == 1 {
			if err := os.Remove(filepath.Join(root, "removed.txt")); err != nil {
				t.Fatalf("remove inherited file: %v", err)
			}
		}
		writeTestFile(t, root, "changing.txt", fmt.Sprintf("version-%d", index))
		snapshot, err := store.CaptureSnapshot(context.Background(), CaptureSnapshotRequest{
			SnapshotID:       fmt.Sprintf("snap_lineage_%d", index),
			RunID:            "run_lineage_compaction",
			ParentSnapshotID: parentSnapshotID,
			SnapshotKind:     "attempt_result",
			RootPath:         root,
			CreatedAt:        "2026-03-01T12:00:00Z",
		})
		if err != nil {
			t.Fatalf("capture snapshot %d: %v", index, err)
		}
		parentSnapshotID = snapshot.SnapshotID
	}

	var compactedParent string
	if err := db.QueryRow(`SELECT COALESCE(parent_snapshot_id, '') FROM filesystem_snapshots WHERE snapshot_id = ?`, parentSnapshotID).Scan(&compactedParent); err != nil {
		t.Fatalf("load compacted snapshot parent: %v", err)
	}
	if compactedParent != "" {
		t.Fatalf("compacted snapshot parent=%q, want empty", compactedParent)
	}
	manifest, err := store.ResolveSnapshotManifest(context.Background(), parentSnapshotID)
	if err != nil {
		t.Fatalf("resolve compacted snapshot: %v", err)
	}
	stable := manifest["stable.txt"]
	if stable.BlobID == "" {
		t.Fatalf("compacted manifest omitted unchanged file: %#v", manifest)
	}
	changing := manifest["changing.txt"]
	if changing.BlobID == "" {
		t.Fatalf("compacted manifest omitted changed file: %#v", manifest)
	}
	if _, exists := manifest["removed.txt"]; exists {
		t.Fatalf("compacted manifest retained deleted file: %#v", manifest)
	}
	stableBytes, err := store.ReadBlob(stable.BlobID)
	if err != nil || string(stableBytes) != "stable" {
		t.Fatalf("compacted stable file=%q err=%v, want stable", stableBytes, err)
	}
	changingBytes, err := store.ReadBlob(changing.BlobID)
	if err != nil || string(changingBytes) != fmt.Sprintf("version-%d", providerlimits.SnapshotLineageDepth) {
		t.Fatalf("compacted changing file=%q err=%v", changingBytes, err)
	}
}

func TestResolveSnapshotManifestRejectsOverLimitLineage(t *testing.T) {
	t.Parallel()

	db := openRuntimeFSTestDB(t)
	applyRuntimeFSMigrations(t, db)
	store, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	seedRuntimeFSRun(t, db, "run_lineage_limit")

	parentSnapshotID := ""
	for index := 0; index <= providerlimits.SnapshotLineageDepth; index++ {
		snapshotID := fmt.Sprintf("snap_over_limit_%d", index)
		mustRuntimeFSExec(t, db, fmt.Sprintf(`INSERT INTO filesystem_snapshots (snapshot_id, run_id, parent_snapshot_id, snapshot_kind, created_at) VALUES ('%s', 'run_lineage_limit', NULLIF('%s', ''), 'attempt_result', '2026-03-01T12:00:00Z')`, snapshotID, parentSnapshotID))
		parentSnapshotID = snapshotID
	}

	_, err = store.ResolveSnapshotManifest(context.Background(), parentSnapshotID)
	var depthErr *providerlimits.DepthExceededError
	if !errors.As(err, &depthErr) {
		t.Fatalf("resolve over-limit lineage error=%v, want DepthExceededError", err)
	}
	if depthErr.Resource != "snapshot lineage" || depthErr.Limit != providerlimits.SnapshotLineageDepth {
		t.Fatalf("unexpected lineage limit error=%#v", depthErr)
	}
}

func TestRestoreSnapshotReplacesMatchingMetadataWithWrongContent(t *testing.T) {
	t.Parallel()

	db := openRuntimeFSTestDB(t)
	applyRuntimeFSMigrations(t, db)
	store, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	seedRuntimeFSRun(t, db, "run_restore_digest")
	sourceRoot := t.TempDir()
	writeTestFile(t, sourceRoot, "same.txt", "right")
	snapshot, err := store.CaptureSnapshot(context.Background(), CaptureSnapshotRequest{
		SnapshotID: "snap_restore_digest", RunID: "run_restore_digest", SnapshotKind: "baseline", RootPath: sourceRoot, CreatedAt: "2026-03-01T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("capture snapshot: %v", err)
	}
	manifest, err := store.ResolveSnapshotManifest(context.Background(), snapshot.SnapshotID)
	if err != nil {
		t.Fatalf("resolve manifest: %v", err)
	}
	entry := manifest["same.txt"]
	restoreRoot := t.TempDir()
	writeTestFile(t, restoreRoot, "same.txt", "wrong")
	destination := filepath.Join(restoreRoot, "same.txt")
	if err := os.Chmod(destination, os.FileMode(entry.ModeBits)); err != nil {
		t.Fatalf("set matching mode: %v", err)
	}
	modifiedAt := time.Unix(0, entry.ModifiedNS)
	if err := os.Chtimes(destination, modifiedAt, modifiedAt); err != nil {
		t.Fatalf("set matching time: %v", err)
	}
	if err := store.RestoreSnapshot(context.Background(), snapshot.SnapshotID, restoreRoot); err != nil {
		t.Fatalf("restore snapshot: %v", err)
	}
	assertTestFile(t, restoreRoot, "same.txt", "right")
}

func TestRestoreSnapshotReplacesSymlinkParentWithoutEscaping(t *testing.T) {
	t.Parallel()

	store, snapshotID := captureRestoreSecurityFixture(t, "run_parent_symlink", "snap_parent_symlink", "nested/file.txt")
	restoreRoot := t.TempDir()
	outside := t.TempDir()
	outsidePath := filepath.Join(outside, "file.txt")
	if err := os.WriteFile(outsidePath, []byte("outside stays unchanged"), 0o644); err != nil {
		t.Fatalf("write outside guard: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(restoreRoot, "nested")); err != nil {
		t.Fatalf("create parent symlink: %v", err)
	}
	for range 2 {
		if err := store.RestoreSnapshot(context.Background(), snapshotID, restoreRoot); err != nil {
			t.Fatalf("restore through parent symlink: %v", err)
		}
	}
	assertTestFile(t, restoreRoot, "nested/file.txt", strings.Repeat("canonical", 2))
	raw, err := os.ReadFile(outsidePath)
	if err != nil {
		t.Fatalf("read outside guard: %v", err)
	}
	if string(raw) != "outside stays unchanged" {
		t.Fatalf("outside bytes changed to %q", raw)
	}
}

func TestRestoreSnapshotReplacesFinalSymlinkWithoutEscaping(t *testing.T) {
	t.Parallel()

	store, snapshotID := captureRestoreSecurityFixture(t, "run_final_symlink", "snap_final_symlink", "file.txt")
	restoreRoot := t.TempDir()
	outsidePath := filepath.Join(t.TempDir(), "outside.txt")
	if err := os.WriteFile(outsidePath, []byte("outside stays unchanged"), 0o644); err != nil {
		t.Fatalf("write outside guard: %v", err)
	}
	if err := os.Symlink(outsidePath, filepath.Join(restoreRoot, "file.txt")); err != nil {
		t.Fatalf("create final symlink: %v", err)
	}
	for range 2 {
		if err := store.RestoreSnapshot(context.Background(), snapshotID, restoreRoot); err != nil {
			t.Fatalf("restore over final symlink: %v", err)
		}
	}
	assertTestFile(t, restoreRoot, "file.txt", strings.Repeat("canonical", 2))
	raw, err := os.ReadFile(outsidePath)
	if err != nil {
		t.Fatalf("read outside guard: %v", err)
	}
	if string(raw) != "outside stays unchanged" {
		t.Fatalf("outside bytes changed to %q", raw)
	}
}

func TestRestoreSnapshotReplacesNonDirectoryParent(t *testing.T) {
	t.Parallel()

	store, snapshotID := captureRestoreSecurityFixture(t, "run_parent_file", "snap_parent_file", "nested/file.txt")
	restoreRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(restoreRoot, "nested"), []byte("provider file"), 0o644); err != nil {
		t.Fatalf("write conflicting parent file: %v", err)
	}
	if err := store.RestoreSnapshot(context.Background(), snapshotID, restoreRoot); err != nil {
		t.Fatalf("restore over parent file: %v", err)
	}
	assertTestFile(t, restoreRoot, "nested/file.txt", strings.Repeat("canonical", 2))
}

func TestRestoreSnapshotDeletionUnlinksSymlinkWithoutFollowingIt(t *testing.T) {
	t.Parallel()

	db := openRuntimeFSTestDB(t)
	applyRuntimeFSMigrations(t, db)
	store, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	seedRuntimeFSRun(t, db, "run_delete_symlink")
	sourceRoot := t.TempDir()
	writeTestFile(t, sourceRoot, "deleted.txt", "base")
	base, err := store.CaptureSnapshot(context.Background(), CaptureSnapshotRequest{
		SnapshotID: "snap_delete_base", RunID: "run_delete_symlink", SnapshotKind: "baseline", RootPath: sourceRoot, CreatedAt: "2026-03-01T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("capture base snapshot: %v", err)
	}
	if err := os.Remove(filepath.Join(sourceRoot, "deleted.txt")); err != nil {
		t.Fatalf("remove source file: %v", err)
	}
	deleted, err := store.CaptureSnapshot(context.Background(), CaptureSnapshotRequest{
		SnapshotID: "snap_delete_child", RunID: "run_delete_symlink", ParentSnapshotID: base.SnapshotID, SnapshotKind: "attempt_result", RootPath: sourceRoot, CreatedAt: "2026-03-01T12:01:00Z",
	})
	if err != nil {
		t.Fatalf("capture deletion snapshot: %v", err)
	}

	restoreRoot := t.TempDir()
	outsidePath := filepath.Join(t.TempDir(), "outside.txt")
	if err := os.WriteFile(outsidePath, []byte("outside"), 0o644); err != nil {
		t.Fatalf("write outside guard: %v", err)
	}
	if err := os.Symlink(outsidePath, filepath.Join(restoreRoot, "deleted.txt")); err != nil {
		t.Fatalf("create deletion symlink: %v", err)
	}
	if err := store.RestoreSnapshot(context.Background(), deleted.SnapshotID, restoreRoot); err != nil {
		t.Fatalf("restore deletion snapshot: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(restoreRoot, "deleted.txt")); !os.IsNotExist(err) {
		t.Fatalf("deleted snapshot entry remains: %v", err)
	}
	raw, err := os.ReadFile(outsidePath)
	if err != nil {
		t.Fatalf("read outside guard: %v", err)
	}
	if string(raw) != "outside" {
		t.Fatalf("outside bytes changed to %q", raw)
	}
}

func TestRestoreSnapshotRemovesExtraSymlinkWithoutFollowingIt(t *testing.T) {
	t.Parallel()

	store, snapshotID := captureRestoreSecurityFixture(t, "run_extra_symlink", "snap_extra_symlink", "file.txt")
	restoreRoot := t.TempDir()
	outsidePath := filepath.Join(t.TempDir(), "outside.txt")
	if err := os.WriteFile(outsidePath, []byte("outside"), 0o644); err != nil {
		t.Fatalf("write outside guard: %v", err)
	}
	if err := os.Symlink(outsidePath, filepath.Join(restoreRoot, "escape")); err != nil {
		t.Fatalf("create extra symlink: %v", err)
	}

	if err := store.RestoreSnapshot(context.Background(), snapshotID, restoreRoot); err != nil {
		t.Fatalf("restore snapshot: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(restoreRoot, "escape")); !os.IsNotExist(err) {
		t.Fatalf("extra symlink remains: %v", err)
	}
	raw, err := os.ReadFile(outsidePath)
	if err != nil {
		t.Fatalf("read outside guard: %v", err)
	}
	if string(raw) != "outside" {
		t.Fatalf("outside bytes changed to %q", raw)
	}
}

func TestRestoreSnapshotDeletionDoesNotFollowSymlinkParent(t *testing.T) {
	t.Parallel()

	db := openRuntimeFSTestDB(t)
	applyRuntimeFSMigrations(t, db)
	store, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	seedRuntimeFSRun(t, db, "run_delete_parent_symlink")
	sourceRoot := t.TempDir()
	writeTestFile(t, sourceRoot, "nested/deleted.txt", "base")
	base, err := store.CaptureSnapshot(context.Background(), CaptureSnapshotRequest{
		SnapshotID: "snap_delete_parent_base", RunID: "run_delete_parent_symlink", SnapshotKind: "baseline", RootPath: sourceRoot, CreatedAt: "2026-03-01T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("capture base snapshot: %v", err)
	}
	if err := os.Remove(filepath.Join(sourceRoot, "nested", "deleted.txt")); err != nil {
		t.Fatalf("remove source file: %v", err)
	}
	deleted, err := store.CaptureSnapshot(context.Background(), CaptureSnapshotRequest{
		SnapshotID: "snap_delete_parent_child", RunID: "run_delete_parent_symlink", ParentSnapshotID: base.SnapshotID, SnapshotKind: "attempt_result", RootPath: sourceRoot, CreatedAt: "2026-03-01T12:01:00Z",
	})
	if err != nil {
		t.Fatalf("capture deletion snapshot: %v", err)
	}

	restoreRoot := t.TempDir()
	outside := t.TempDir()
	outsidePath := filepath.Join(outside, "deleted.txt")
	if err := os.WriteFile(outsidePath, []byte("outside"), 0o644); err != nil {
		t.Fatalf("write outside guard: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(restoreRoot, "nested")); err != nil {
		t.Fatalf("create deletion parent symlink: %v", err)
	}
	if err := store.RestoreSnapshot(context.Background(), deleted.SnapshotID, restoreRoot); err != nil {
		t.Fatalf("restore deletion through parent symlink: %v", err)
	}
	raw, err := os.ReadFile(outsidePath)
	if err != nil {
		t.Fatalf("read outside guard: %v", err)
	}
	if string(raw) != "outside" {
		t.Fatalf("outside bytes changed to %q", raw)
	}
}

func TestRestoreSnapshotPreservesRealDirectoryOnFileShapeConflict(t *testing.T) {
	t.Parallel()

	store, snapshotID := captureRestoreSecurityFixture(t, "run_directory_conflict", "snap_directory_conflict", "file.txt")
	restoreRoot := t.TempDir()
	writeTestFile(t, restoreRoot, "file.txt/guard.txt", "guard")
	if err := store.RestoreSnapshot(context.Background(), snapshotID, restoreRoot); err == nil {
		t.Fatal("expected directory/file shape conflict")
	}
	assertTestFile(t, restoreRoot, "file.txt/guard.txt", "guard")
}

func TestRestoreSnapshotPreservesModeAndDeletesExtraFiles(t *testing.T) {
	t.Parallel()

	db := openRuntimeFSTestDB(t)
	applyRuntimeFSMigrations(t, db)
	store, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	seedRuntimeFSRun(t, db, "run_restore_mode")
	sourceRoot := t.TempDir()
	writeTestFile(t, sourceRoot, "bin/tool", "#!/bin/sh\n")
	if err := os.Chmod(filepath.Join(sourceRoot, "bin", "tool"), 0o751); err != nil {
		t.Fatalf("chmod source: %v", err)
	}
	snapshot, err := store.CaptureSnapshot(context.Background(), CaptureSnapshotRequest{
		SnapshotID: "snap_restore_mode", RunID: "run_restore_mode", SnapshotKind: "baseline", RootPath: sourceRoot, CreatedAt: "2026-03-01T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("capture snapshot: %v", err)
	}
	restoreRoot := t.TempDir()
	writeTestFile(t, restoreRoot, "bin/tool", "#!/bin/sh\n")
	if err := os.Chmod(filepath.Join(restoreRoot, "bin", "tool"), 0o600); err != nil {
		t.Fatalf("chmod destination: %v", err)
	}
	writeTestFile(t, restoreRoot, "extra.txt", "remove me")
	if err := store.RestoreSnapshot(context.Background(), snapshot.SnapshotID, restoreRoot); err != nil {
		t.Fatalf("restore snapshot: %v", err)
	}
	info, err := os.Stat(filepath.Join(restoreRoot, "bin", "tool"))
	if err != nil {
		t.Fatalf("stat restored tool: %v", err)
	}
	if info.Mode().Perm() != 0o751 {
		t.Fatalf("restored mode = %04o, want 0751", info.Mode().Perm())
	}
	if _, err := os.Stat(filepath.Join(restoreRoot, "extra.txt")); !os.IsNotExist(err) {
		t.Fatalf("extra file was not deleted: %v", err)
	}
}

func TestRecoverMissingSnapshotBlobsRequiresMatchingSourceDigest(t *testing.T) {
	t.Parallel()

	db := openRuntimeFSTestDB(t)
	applyRuntimeFSMigrations(t, db)
	originalRoot := t.TempDir()
	originalStore, err := NewStore(db, originalRoot)
	if err != nil {
		t.Fatalf("new original runtime fs store: %v", err)
	}
	seedRuntimeFSRun(t, db, "run_source_recovery")
	sourceRoot := t.TempDir()
	writeTestFile(t, sourceRoot, "src/app.txt", "recoverable source")
	if _, err := originalStore.CaptureSnapshot(context.Background(), CaptureSnapshotRequest{
		SnapshotID:   "snap_source_recovery",
		RunID:        "run_source_recovery",
		SnapshotKind: "baseline",
		RootPath:     sourceRoot,
		CreatedAt:    "2026-03-01T12:00:00Z",
	}); err != nil {
		t.Fatalf("capture source recovery snapshot: %v", err)
	}

	durableStore, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new durable runtime fs store: %v", err)
	}
	if err := durableStore.RecoverMissingSnapshotBlobs(context.Background(), "snap_source_recovery", sourceRoot); err != nil {
		t.Fatalf("recover snapshot blob from matching source: %v", err)
	}
	restoreRoot := t.TempDir()
	if err := durableStore.RestoreSnapshot(context.Background(), "snap_source_recovery", restoreRoot); err != nil {
		t.Fatalf("restore source-recovered snapshot: %v", err)
	}
	assertTestFile(t, restoreRoot, "src/app.txt", "recoverable source")

	writeTestFile(t, sourceRoot, "src/app.txt", "different source")
	secondDurableStore, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new second durable runtime fs store: %v", err)
	}
	if err := secondDurableStore.RecoverMissingSnapshotBlobs(context.Background(), "snap_source_recovery", sourceRoot); err == nil {
		t.Fatal("expected digest-mismatched source recovery to fail")
	}
}

func TestCountChangesAgainstSnapshotIgnoresProtectedRoots(t *testing.T) {
	t.Parallel()

	db := openRuntimeFSTestDB(t)
	applyRuntimeFSMigrations(t, db)
	store, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	seedRuntimeFSRun(t, db, "run_1")

	root := t.TempDir()
	writeTestFile(t, root, "src/app.js", "console.log('v1')")
	base, err := store.CaptureSnapshot(context.Background(), CaptureSnapshotRequest{
		SnapshotID:   "snap_base",
		RunID:        "run_1",
		SnapshotKind: "baseline",
		RootPath:     root,
		CreatedAt:    "2026-03-01T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("capture base snapshot: %v", err)
	}
	writeTestFile(t, root, ".kingdom/provider-completion/receipts/x/receipt.json", "ignored")
	changes, err := store.CountChangesAgainstSnapshot(context.Background(), root, base.SnapshotID)
	if err != nil {
		t.Fatalf("count changes: %v", err)
	}
	if changes != 0 {
		t.Fatalf("expected protected runtime paths to be ignored, got %d changes", changes)
	}
}

func TestCaptureSnapshotRejectsOneByteWorkspaceFileOverflowWithoutPersistence(t *testing.T) {
	db := openRuntimeFSTestDB(t)
	applyRuntimeFSMigrations(t, db)
	store, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	seedRuntimeFSRun(t, db, "run_capture_limit")
	root := t.TempDir()
	path := filepath.Join(root, "too-large.bin")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("create oversized source: %v", err)
	}
	if err := os.Truncate(path, providerlimits.TrackedWorkspaceFileBytes+1); err != nil {
		t.Fatalf("truncate oversized source: %v", err)
	}

	_, err = store.CaptureSnapshot(context.Background(), CaptureSnapshotRequest{
		SnapshotID: "snap_too_large", RunID: "run_capture_limit", SnapshotKind: "baseline", RootPath: root, CreatedAt: "2026-03-01T12:00:00Z",
	})
	var limitErr *providerlimits.LimitExceededError
	if !errors.As(err, &limitErr) {
		t.Fatalf("one-byte snapshot overflow error=%v, want LimitExceededError", err)
	}
	var snapshots int
	if err := db.QueryRow(`SELECT COUNT(1) FROM filesystem_snapshots WHERE snapshot_id = 'snap_too_large'`).Scan(&snapshots); err != nil {
		t.Fatalf("count snapshots: %v", err)
	}
	if snapshots != 0 {
		t.Fatalf("limit failure persisted %d snapshots", snapshots)
	}
}

func TestReadBlobRejectsOneByteWorkspaceFileOverflow(t *testing.T) {
	db := openRuntimeFSTestDB(t)
	applyRuntimeFSMigrations(t, db)
	store, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	blobID := strings.Repeat("a", 64)
	blobPath, err := store.blobPath(blobID)
	if err != nil {
		t.Fatalf("blob path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(blobPath), 0o755); err != nil {
		t.Fatalf("create blob directory: %v", err)
	}
	if err := os.WriteFile(blobPath, nil, 0o600); err != nil {
		t.Fatalf("create oversized blob: %v", err)
	}
	if err := os.Truncate(blobPath, providerlimits.TrackedWorkspaceFileBytes+1); err != nil {
		t.Fatalf("truncate oversized blob: %v", err)
	}
	_, err = store.ReadBlob(blobID)
	var limitErr *providerlimits.LimitExceededError
	if !errors.As(err, &limitErr) {
		t.Fatalf("one-byte blob overflow error=%v, want LimitExceededError", err)
	}
}

func TestRecoverMissingSnapshotBlobsDoesNotFollowSourceSymlinks(t *testing.T) {
	db := openRuntimeFSTestDB(t)
	applyRuntimeFSMigrations(t, db)
	originalStore, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new original runtime fs store: %v", err)
	}
	seedRuntimeFSRun(t, db, "run_recovery_symlink")
	sourceRoot := t.TempDir()
	writeTestFile(t, sourceRoot, "artifact.txt", "canonical")
	if _, err := originalStore.CaptureSnapshot(context.Background(), CaptureSnapshotRequest{
		SnapshotID: "snap_recovery_symlink", RunID: "run_recovery_symlink", SnapshotKind: "baseline", RootPath: sourceRoot, CreatedAt: "2026-03-01T12:00:00Z",
	}); err != nil {
		t.Fatalf("capture snapshot: %v", err)
	}
	durableStore, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new durable store: %v", err)
	}
	outsidePath := filepath.Join(t.TempDir(), "artifact.txt")
	if err := os.WriteFile(outsidePath, []byte("canonical"), 0o600); err != nil {
		t.Fatalf("write external source: %v", err)
	}
	if err := os.Remove(filepath.Join(sourceRoot, "artifact.txt")); err != nil {
		t.Fatalf("remove source file: %v", err)
	}
	if err := os.Symlink(outsidePath, filepath.Join(sourceRoot, "artifact.txt")); err != nil {
		t.Fatalf("replace source with symlink: %v", err)
	}
	if err := durableStore.RecoverMissingSnapshotBlobs(context.Background(), "snap_recovery_symlink", sourceRoot); err == nil {
		t.Fatal("expected recovery through source symlink to fail")
	}
}

func openRuntimeFSTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("enable foreign keys: %v", err)
	}
	return db
}

func applyRuntimeFSMigrations(t *testing.T, db *sql.DB) {
	t.Helper()
	entries, err := dbmigrations.List()
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	migrations := make([]state.SQLMigration, 0, len(entries))
	for _, name := range entries {
		if name < "0001_initial_schema.up.sql" || filepath.Ext(name) != ".sql" {
			continue
		}
		raw, err := dbmigrations.Read(name)
		if err != nil {
			t.Fatalf("read migration %s: %v", name, err)
		}
		migrations = append(migrations, state.SQLMigration{Name: name, SQL: raw})
	}
	if err := state.ApplySQLMigrations(context.Background(), db, migrations, time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
}

func mustRuntimeFSExec(t *testing.T, db *sql.DB, statement string) {
	t.Helper()
	if _, err := db.Exec(statement); err != nil {
		t.Fatalf("execute runtime fs test statement: %v", err)
	}
}

func writeTestFile(t *testing.T, root, relativePath, body string) {
	t.Helper()
	resolved := filepath.Join(root, filepath.FromSlash(relativePath))
	if err := os.MkdirAll(filepath.Dir(resolved), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", relativePath, err)
	}
	if err := os.WriteFile(resolved, []byte(body), 0o644); err != nil {
		t.Fatalf("write %s: %v", relativePath, err)
	}
}

func captureRestoreSecurityFixture(t *testing.T, runID, snapshotID, relativePath string) (*Store, string) {
	t.Helper()
	db := openRuntimeFSTestDB(t)
	applyRuntimeFSMigrations(t, db)
	store, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	seedRuntimeFSRun(t, db, runID)
	sourceRoot := t.TempDir()
	writeTestFile(t, sourceRoot, relativePath, strings.Repeat("canonical", 2))
	if _, err := store.CaptureSnapshot(context.Background(), CaptureSnapshotRequest{
		SnapshotID: snapshotID, RunID: runID, SnapshotKind: "baseline", RootPath: sourceRoot, CreatedAt: "2026-03-01T12:00:00Z",
	}); err != nil {
		t.Fatalf("capture security fixture: %v", err)
	}
	return store, snapshotID
}

func assertTestFile(t *testing.T, root, relativePath, want string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(relativePath)))
	if err != nil {
		t.Fatalf("read %s: %v", relativePath, err)
	}
	if string(raw) != want {
		t.Fatalf("%s = %q, want %q", relativePath, string(raw), want)
	}
}

func seedRuntimeFSRun(t *testing.T, db *sql.DB, runID string) {
	t.Helper()
	stmts := []string{
		`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_runtimefs', 'Runtime FS', 'text', '{}', '2026-03-01T12:00:00Z')`,
		`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_runtimefs', 'royal-decree-v1', '1', '{}', '2026-03-01T12:00:00Z')`,
		`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('` + runID + `', NULL, 'decree_runtimefs', 'wfdef_runtimefs', 'running', 'approved', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("seed runtime fs run: %v", err)
		}
	}
}
