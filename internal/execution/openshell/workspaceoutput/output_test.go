package workspaceoutput

import (
	"context"
	"database/sql"
	"kingdom/internal/runtimefs"
	"kingdom/internal/securefs"
	"kingdom/internal/workspacearchive"
	_ "modernc.org/sqlite"
	"os"
	"path/filepath"
	"testing"
)

func TestPlanningEqualityIncludesAllContentAndModes(t *testing.T) {
	a := workspacearchive.Manifest{Version: 1, ArchiveSHA256: "a", ExpandedBytes: 3, Entries: []workspacearchive.Entry{{Path: "a", Kind: "file", Size: 3, SHA256: "hash"}}}
	b := a
	b.ArchiveSHA256 = "different-compression"
	if !sameContents(a, b) {
		t.Fatal("compression changed logical contents")
	}
	for _, change := range []func(*workspacearchive.Manifest){
		func(m *workspacearchive.Manifest) { m.Entries[0].Executable = true },
		func(m *workspacearchive.Manifest) { m.Entries[0].SHA256 = "changed" },
		func(m *workspacearchive.Manifest) { m.Entries[0].Path = "other" },
		func(m *workspacearchive.Manifest) { m.Entries = nil },
	} {
		b = a
		b.Entries = append([]workspacearchive.Entry(nil), a.Entries...)
		change(&b)
		if sameContents(a, b) {
			t.Fatal("changed planning output accepted")
		}
	}
}
func TestSnapshotMatchesVerifiedArchiveAndRestoresDeletesAndModes(t *testing.T) {
	ctx := context.Background()
	db, e := sql.Open("sqlite", filepath.Join(t.TempDir(), "test.db"))
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	_, e = db.Exec(`CREATE TABLE filesystem_snapshots(snapshot_id TEXT PRIMARY KEY,run_id TEXT,parent_snapshot_id TEXT,snapshot_kind TEXT,created_at TEXT);CREATE TABLE filesystem_snapshot_entries(snapshot_id TEXT,path TEXT,entry_type TEXT,blob_id TEXT,size_bytes INTEGER,mode_bits INTEGER,mtime_ns INTEGER,PRIMARY KEY(snapshot_id,path));CREATE TABLE filesystem_blobs(blob_id TEXT PRIMARY KEY,size_bytes INTEGER,created_at TEXT);`)
	if e != nil {
		t.Fatal(e)
	}
	store, e := runtimefs.NewStore(db, t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	source := t.TempDir()
	if e = os.WriteFile(filepath.Join(source, "new"), []byte("result"), 0755); e != nil {
		t.Fatal(e)
	}
	root, e := securefs.OpenRoot(source)
	if e != nil {
		t.Fatal(e)
	}
	defer root.Close()
	zip, e := os.CreateTemp(t.TempDir(), "archive")
	if e != nil {
		t.Fatal(e)
	}
	defer zip.Close()
	limits := workspacearchive.Limits{ArchiveBytes: 1 << 20, ExpandedBytes: 1 << 20, FileBytes: 1 << 20, Entries: 100, Depth: 10, PathBytes: 1000}
	_, e = workspacearchive.ExportZIP(ctx, root, zip, limits)
	if e != nil {
		t.Fatal(e)
	}
	info, _ := zip.Stat()
	staged, e := workspacearchive.ImportZIP(ctx, t.TempDir(), zip, info.Size(), limits)
	if e != nil {
		t.Fatal(e)
	}
	defer staged.Close()
	_, e = store.CaptureSnapshot(ctx, runtimefs.CaptureSnapshotRequest{SnapshotID: "snapshot", RunID: "run", SnapshotKind: "attempt_result", RootPath: staged.Path, CreatedAt: "2026-10-23T00:00:00Z"})
	if e != nil {
		t.Fatal(e)
	}
	o := Output{Config{Store: store}}
	if e = o.verifySnapshot(ctx, "snapshot", staged.Manifest); e != nil {
		t.Fatal(e)
	}
	target := t.TempDir()
	os.WriteFile(filepath.Join(target, "old"), []byte("delete"), 0600)
	targetRoot, e := securefs.OpenRoot(target)
	if e != nil {
		t.Fatal(e)
	}
	defer targetRoot.Close()
	if e = store.RestoreSnapshotRoot(ctx, "snapshot", targetRoot); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(filepath.Join(target, "old")); !os.IsNotExist(e) {
		t.Fatal("extra file retained", e)
	}
	info, e = os.Stat(filepath.Join(target, "new"))
	if e != nil || info.Mode()&0111 == 0 {
		t.Fatal("executable mode lost", e)
	}
	_, e = db.Exec(`UPDATE filesystem_snapshot_entries SET blob_id=? WHERE snapshot_id='snapshot'`, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if e != nil {
		t.Fatal(e)
	}
	if e = o.verifySnapshot(ctx, "snapshot", staged.Manifest); e == nil {
		t.Fatal("foreign snapshot accepted")
	}
}
