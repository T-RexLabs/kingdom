package runtimefs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSnapshotStreamsMoreThanTenThousandEntriesAndDeepPaths(t *testing.T) {
	db := openRuntimeFSTestDB(t)
	applyRuntimeFSMigrations(t, db)
	store, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	seedRuntimeFSRun(t, db, "run_stream")
	root := t.TempDir()
	for i := range 10001 {
		writeTestFile(t, root, fmt.Sprintf("file-%05d", i), "")
	}
	deep := strings.Repeat("d/", 65) + "file"
	writeTestFile(t, root, deep, "deep content")
	ctx := context.Background()
	if _, err := store.CaptureSnapshot(ctx, CaptureSnapshotRequest{SnapshotID: "snap_stream", RunID: "run_stream", SnapshotKind: "baseline", RootPath: root, CreatedAt: "2026-03-01T12:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM filesystem_snapshot_entries WHERE snapshot_id = 'snap_stream'`).Scan(&count); err != nil || count != 10002 {
		t.Fatalf("snapshot count=%d error=%v", count, err)
	}
	// Restore a large existing tree with both missing and corrupt content. This
	// exercises traversal and membership at scale without 10000 redundant fsyncs.
	restored := root
	if err := os.Remove(filepath.Join(restored, "file-00000")); err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, restored, deep, "corrupt content")
	if err := store.RestoreSnapshot(ctx, "snap_stream", restored); err != nil {
		t.Fatal(err)
	}
	if changes, err := store.CountChangesAgainstSnapshot(ctx, restored, "snap_stream"); err != nil || changes != 0 {
		t.Fatalf("changes=%d error=%v", changes, err)
	}
	if err := os.Remove(filepath.Join(restored, "file-10000")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CaptureSnapshot(ctx, CaptureSnapshotRequest{SnapshotID: "snap_stream_delta", ParentSnapshotID: "snap_stream", RunID: "run_stream", SnapshotKind: "attempt_result", RootPath: restored, CreatedAt: "2026-03-01T12:00:01Z"}); err != nil {
		t.Fatal(err)
	}
	diff, err := readTestDiff(store, "snap_stream", "snap_stream_delta")
	if err != nil || len(diff) != 1 || diff[0].Path != "file-10000" || diff[0].EntryType != "delete" {
		t.Fatalf("diff=%v error=%v", diff, err)
	}
}

func TestCancelledCapturePublishesNoSnapshotAndRemovesSpool(t *testing.T) {
	db := openRuntimeFSTestDB(t)
	applyRuntimeFSMigrations(t, db)
	store, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	seedRuntimeFSRun(t, db, "run_cancel_stream")
	root := t.TempDir()
	writeTestFile(t, root, "one", "one")
	writeTestFile(t, root, "two", "two")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store.syncBlobDir = func(string) error { cancel(); return nil }
	_, err = store.CaptureSnapshot(ctx, CaptureSnapshotRequest{SnapshotID: "snap_cancel_stream", RunID: "run_cancel_stream", SnapshotKind: "baseline", RootPath: root, CreatedAt: "2026-03-01T12:00:00Z"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("capture=%v", err)
	}
	for _, table := range []string{"filesystem_snapshots", "filesystem_snapshot_entries", "filesystem_blobs"} {
		var count int
		if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("%s count=%d error=%v", table, count, err)
		}
	}
	entries, err := os.ReadDir(store.tempRoot)
	if err != nil || len(entries) != 0 {
		t.Fatalf("staging leak: %v %v", entries, err)
	}
}

func TestResponseOnlyCleanupCompletesBeyondFormerEntryCap(t *testing.T) {
	db := openRuntimeFSTestDB(t)
	store, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := store.ResponseOnlyWorkspaceRoot("run_large", "attempt_large")
	if err := os.MkdirAll(root, 0700); err != nil {
		t.Fatal(err)
	}
	// Even empty directories require descent and removal actions, independently
	// of workspace eligibility. This exceeds the old 10000+128 action ceiling.
	for i := range 5200 {
		if err := os.Mkdir(filepath.Join(root, fmt.Sprintf("dir-%05d", i)), 0700); err != nil {
			t.Fatal(err)
		}
	}
	outside := t.TempDir()
	writeTestFile(t, outside, "keep", "outside")
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.RemoveResponseOnlyWorkspaceContext(ctx, "run_large", "attempt_large"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled cleanup=%v", err)
	}
	if err := store.RemoveResponseOnlyWorkspaceContext(context.Background(), "run_large", "attempt_large"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(root); !os.IsNotExist(err) {
		t.Fatalf("workspace still exists: %v", err)
	}
	if raw, err := os.ReadFile(filepath.Join(outside, "keep")); err != nil || string(raw) != "outside" {
		t.Fatalf("outside=%q error=%v", raw, err)
	}
}

func TestRestoreCorruptStreamDoesNotReplaceDestination(t *testing.T) {
	db := openRuntimeFSTestDB(t)
	applyRuntimeFSMigrations(t, db)
	store, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	seedRuntimeFSRun(t, db, "run_corrupt_stream")
	root := t.TempDir()
	writeTestFile(t, root, "file", "canonical")
	ctx := context.Background()
	if _, err := store.CaptureSnapshot(ctx, CaptureSnapshotRequest{SnapshotID: "snap_corrupt_stream", RunID: "run_corrupt_stream", SnapshotKind: "baseline", RootPath: root, CreatedAt: "2026-03-01T12:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	manifest, err := readTestManifest(store, "snap_corrupt_stream")
	if err != nil {
		t.Fatal(err)
	}
	path, err := store.blobPath(manifest["file"].BlobID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("corrupted"), 0600); err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, root, "file", "preserve on failure")
	if err := store.RestoreSnapshot(ctx, "snap_corrupt_stream", root); err == nil || !strings.Contains(err.Error(), "digest mismatch") {
		t.Fatalf("restore=%v", err)
	}
	if raw, err := os.ReadFile(filepath.Join(root, "file")); err != nil || string(raw) != "preserve on failure" {
		t.Fatalf("destination=%q error=%v", raw, err)
	}
	// A correct digest with corrupt manifest size must also fail before rename.
	if err := os.WriteFile(path, []byte("canonical"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE filesystem_snapshot_entries SET size_bytes = size_bytes + 1 WHERE snapshot_id = 'snap_corrupt_stream'`); err != nil {
		t.Fatal(err)
	}
	if err := store.RestoreSnapshot(ctx, "snap_corrupt_stream", root); err == nil || !strings.Contains(err.Error(), "size mismatch") {
		t.Fatalf("restore wrong size=%v", err)
	}
	if raw, err := os.ReadFile(filepath.Join(root, "file")); err != nil || string(raw) != "preserve on failure" {
		t.Fatalf("destination=%q error=%v", raw, err)
	}
}

func TestRestoreCorruptStreamPreservesAncestorWithoutRepair(t *testing.T) {
	db := openRuntimeFSTestDB(t)
	applyRuntimeFSMigrations(t, db)
	seedRuntimeFSRun(t, db, "run_ancestor")
	store, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	source, destination := t.TempDir(), t.TempDir()
	writeTestFile(t, source, "parent/child", "canonical")
	ctx := context.Background()
	if _, err := store.CaptureSnapshot(ctx, CaptureSnapshotRequest{SnapshotID: "snap_ancestor", RunID: "run_ancestor", SnapshotKind: "baseline", RootPath: source, CreatedAt: "2026-03-01T12:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	manifest, err := readTestManifest(store, "snap_ancestor")
	if err != nil {
		t.Fatal(err)
	}
	blobPath, err := store.blobPath(manifest["parent/child"].BlobID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blobPath, []byte("corrupted"), 0600); err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, destination, "parent", "ancestor sentinel")
	if err := store.RestoreSnapshot(ctx, "snap_ancestor", destination); err == nil || !strings.Contains(err.Error(), "digest mismatch") {
		t.Fatalf("restore error=%v", err)
	}
	if raw, err := os.ReadFile(filepath.Join(destination, "parent")); err != nil || string(raw) != "ancestor sentinel" {
		t.Fatalf("failed restoration destroyed ancestor: %q %v", raw, err)
	}
	if entries, err := os.ReadDir(destination); err != nil || len(entries) != 1 || entries[0].Name() != "parent" {
		t.Fatalf("restoration staging leaked: %v %v", entries, err)
	}
}
