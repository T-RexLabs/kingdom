package runtimefs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestWorkspaceIndexUsesDiskAndReleasesPagedCursors(t *testing.T) {
	db := openRuntimeFSTestDB(t)
	applyRuntimeFSMigrations(t, db)
	seedRuntimeFSRun(t, db, "index")
	store, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	plan, err := db.Query(`EXPLAIN QUERY PLAN SELECT `+entryColumns+` FROM filesystem_snapshot_entries WHERE snapshot_id = ? AND path > ? ORDER BY path LIMIT ?`, "index-base", "file-05000", metadataPageSize)
	if err != nil {
		t.Fatal(err)
	}
	indexed := false
	for plan.Next() {
		var id, parent, unused int
		var detail string
		if err := plan.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		indexed = indexed || strings.Contains(detail, "snapshot_id=? AND path>?")
	}
	if err := plan.Close(); err != nil {
		t.Fatal(err)
	}
	if !indexed {
		t.Fatal("metadata pages must seek by snapshot and path, not rescan preceding entries")
	}
	const total = 10001
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO filesystem_snapshots VALUES ('index-base', 'index', NULL, 'baseline', '2026-03-01T12:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.Prepare(`INSERT INTO filesystem_snapshot_entries VALUES ('index-base', ?, 'file', ?, 0, 384, 0)`)
	if err != nil {
		t.Fatal(err)
	}
	for n := range total {
		if _, err := stmt.Exec(fmt.Sprintf("file-%05d", n), strings.Repeat("a", 64)); err != nil {
			t.Fatal(err)
		}
	}
	stmt.Close()
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	view, err := store.OpenSnapshotManifest(ctx, "index-base")
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	for pragma, want := range map[string]int{"cache_size": -1024, "temp_store": 1, "mmap_size": 0} {
		var got int
		if err := view.index.tx.QueryRow("PRAGMA " + pragma).Scan(&got); err != nil || got != want {
			t.Fatalf("%s=%d want=%d error=%v", pragma, got, want, err)
		}
	}
	var pages int
	if err := view.index.tx.QueryRow(`PRAGMA page_count`).Scan(&pages); err != nil || pages < 256 {
		t.Fatalf("expected disk-sized index: pages=%d error=%v", pages, err)
	}
	visited := 0
	if err := view.Visit(ctx, func(entry SnapshotEntry) error {
		visited++
		// Both lookups would deadlock if either page cursor retained its only
		// connection while invoking the callback.
		if _, found, err := view.Lookup(ctx, entry.Path); err != nil || !found {
			return fmt.Errorf("lookup: found=%t err=%v", found, err)
		}
		var exists int
		return db.QueryRow(`SELECT 1 FROM filesystem_snapshots WHERE snapshot_id='index-base'`).Scan(&exists)
	}); err != nil || visited != total {
		t.Fatalf("visited=%d error=%v", visited, err)
	}
	view.Close()
	if _, err := os.Stat(view.index.path); !os.IsNotExist(err) {
		t.Fatalf("scratch index survived close: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	visited = 0
	err = store.VisitSnapshotDiff(ctx, "", "index-base", func(entry SnapshotDiffEntry) error {
		visited++
		if entry.Path != fmt.Sprintf("file-%05d", visited-1) {
			return fmt.Errorf("unordered diff %q", entry.Path)
		}
		if visited == metadataPageSize+1 {
			cancel()
		}
		return nil
	})
	cancel()
	if !errors.Is(err, context.Canceled) || visited != metadataPageSize+1 {
		t.Fatalf("diff cancellation: visited=%d error=%v", visited, err)
	}
	entries, err := os.ReadDir(store.tempRoot)
	if err != nil || len(entries) != 0 {
		t.Fatalf("scratch files leaked: %v %v", entries, err)
	}
}

func TestDuplicateBlobsDoNotSyncOrStagePerWorkspaceEntry(t *testing.T) {
	db := openRuntimeFSTestDB(t)
	applyRuntimeFSMigrations(t, db)
	seedRuntimeFSRun(t, db, "deduplicate")
	store, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	for n := range 300 {
		writeTestFile(t, root, fmt.Sprintf("file-%04d", n), "same bytes")
	}
	syncs := 0
	store.syncBlobDir = func(id string) error { syncs++; return store.syncBlobDirectoryDurably(id) }
	if _, err := store.CaptureSnapshot(context.Background(), CaptureSnapshotRequest{SnapshotID: "deduplicate", RunID: "deduplicate", SnapshotKind: "baseline", RootPath: root, CreatedAt: "2026-03-01T12:00:00Z"}); err != nil {
		t.Fatal(err)
	}
	if syncs > 2 {
		t.Fatalf("duplicate files caused per-entry syncs: %d", syncs)
	}
	verifier, err := store.newBlobVerifier()
	if err != nil {
		t.Fatal(err)
	}
	defer verifier.root.Close()
	var entry SnapshotEntry
	if err := store.VisitSnapshotManifest(context.Background(), "deduplicate", func(e SnapshotEntry) error { entry = e; return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.verify(context.Background(), entry.BlobID, entry.SizeBytes, false); err != nil {
		t.Fatal(err)
	}
	path, err := store.blobPath(entry.BlobID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("bad! bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.verify(context.Background(), entry.BlobID, entry.SizeBytes, false); err == nil {
		t.Fatal("cached verification accepted changed canonical bytes")
	}
}
