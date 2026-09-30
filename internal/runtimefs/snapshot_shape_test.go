package runtimefs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"kingdom/internal/providerlimits"
)

func TestRestoreSnapshotFileToDirectory(t *testing.T) {
	db := openRuntimeFSTestDB(t)
	applyRuntimeFSMigrations(t, db)
	seedRuntimeFSRun(t, db, "run_shape")
	store, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	source := t.TempDir()
	writeTestFile(t, source, "module", "old file")
	writeTestFile(t, source, "unchanged.txt", "keep")
	parent := ""
	capture := func(id string) string {
		t.Helper()
		snapshot, err := store.CaptureSnapshot(ctx, CaptureSnapshotRequest{
			SnapshotID: id, ParentSnapshotID: parent, RunID: "run_shape",
			SnapshotKind: "attempt_result", RootPath: source, CreatedAt: "2026-09-30T00:00:00Z",
		})
		if err != nil {
			t.Fatal(err)
		}
		parent = snapshot.SnapshotID
		return parent
	}
	capture("base")
	if err := os.Remove(filepath.Join(source, "module")); err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, source, "module/new.go", "first result")
	first := capture("first")
	if err := os.Remove(filepath.Join(source, "module/new.go")); err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, source, "module/new.go/deep.txt", "nested result")
	nested := capture("nested")
	for n := 3; n <= providerlimits.SnapshotLineageDepth; n++ {
		capture(fmt.Sprintf("checkpoint_%d", n))
	}
	var compactedParent string
	if err := db.QueryRow(`SELECT COALESCE(parent_snapshot_id, '') FROM filesystem_snapshots WHERE snapshot_id = ?`, parent).Scan(&compactedParent); err != nil || compactedParent != "" {
		t.Fatalf("expected compacted lineage, parent=%q err=%v", compactedParent, err)
	}
	for _, snapshot := range []struct{ name, id, path, content string }{
		{"single", first, "module/new.go", "first result"},
		{"nested", nested, "module/new.go/deep.txt", "nested result"},
		{"compacted", parent, "module/new.go/deep.txt", "nested result"},
	} {
		for _, destination := range []string{"fresh", "baseline", "already_correct"} {
			t.Run(snapshot.name+"/"+destination, func(t *testing.T) {
				root := t.TempDir()
				switch destination {
				case "baseline":
					writeTestFile(t, root, "module", "old file")
				case "already_correct":
					writeTestFile(t, root, snapshot.path, snapshot.content)
				}
				for replay := 0; replay < 2; replay++ {
					if err := store.RestoreSnapshot(ctx, snapshot.id, root); err != nil {
						t.Fatalf("restore %d: %v", replay, err)
					}
					assertTestFile(t, root, snapshot.path, snapshot.content)
					assertTestFile(t, root, "unchanged.txt", "keep")
				}
			})
		}
	}
}

func TestRestoreSnapshotDirectoryToFile(t *testing.T) {
	db := openRuntimeFSTestDB(t)
	applyRuntimeFSMigrations(t, db)
	seedRuntimeFSRun(t, db, "run_reverse_shape")
	store, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	source := t.TempDir()
	writeTestFile(t, source, "module/old.go", "old child")
	base, err := store.CaptureSnapshot(ctx, CaptureSnapshotRequest{SnapshotID: "base", RunID: "run_reverse_shape", SnapshotKind: "baseline", RootPath: source, CreatedAt: "2026-09-30T00:00:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(source, "module/old.go")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(source, "module")); err != nil {
		t.Fatal(err)
	}
	writeTestFile(t, source, "module", "new file")
	result, err := store.CaptureSnapshot(ctx, CaptureSnapshotRequest{SnapshotID: "result", ParentSnapshotID: base.SnapshotID, RunID: "run_reverse_shape", SnapshotKind: "attempt_result", RootPath: source, CreatedAt: "2026-09-30T00:00:01Z"})
	if err != nil {
		t.Fatal(err)
	}
	for _, root := range []string{source, t.TempDir()} {
		if err := store.RestoreSnapshot(ctx, result.SnapshotID, root); err != nil {
			t.Fatalf("restore directory-to-file result: %v", err)
		}
		assertTestFile(t, root, "module", "new file")
	}
	conflicting := t.TempDir()
	writeTestFile(t, conflicting, "module/operator.txt", "protect")
	if err := store.RestoreSnapshot(ctx, result.SnapshotID, conflicting); err == nil {
		t.Fatal("expected conflict with unexpected populated directory")
	}
	assertTestFile(t, conflicting, "module/operator.txt", "protect")
}

func TestSnapshotIndexLiveDescendants(t *testing.T) {
	db := openRuntimeFSTestDB(t)
	store, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	index, err := store.newWorkspaceIndex(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()
	for _, entry := range []SnapshotEntry{
		{Path: "module/child", EntryType: "file"},
		{Path: "module-more/child", EntryType: "file"},
		{Path: "deleted/child", EntryType: "delete"},
		{Path: "wild_%/child", EntryType: "file"},
	} {
		if err := index.put(ctx, 0, entry); err != nil {
			t.Fatal(err)
		}
	}
	if err := index.put(ctx, 1, SnapshotEntry{Path: "other/child", EntryType: "file"}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		path string
		want bool
	}{
		{"module", true}, {"mod", false}, {"module/child", false},
		{"module-more", true}, {"deleted", false}, {"other", false},
		{"wild_%", true}, {"wild", false}, {"wildXX", false},
	} {
		got, err := index.hasLiveDescendant(ctx, 0, tc.path)
		if err != nil || got != tc.want {
			t.Errorf("hasLiveDescendant(%q) = %v, %v; want %v", tc.path, got, err, tc.want)
		}
	}
}
