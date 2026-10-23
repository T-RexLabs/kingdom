package runtimefs

import (
	"context"
	"kingdom/internal/securefs"
	"os"
	"path/filepath"
	"testing"
)

func TestRestoreSnapshotRootNeverReopensReplacedPath(t *testing.T) {
	store, snapshot := captureRestoreSecurityFixture(t, "pinned_restore", "pinned_snapshot", "nested/file")
	parent := t.TempDir()
	path := filepath.Join(parent, "attempt")
	mustMkdir := func(p string) {
		t.Helper()
		if e := os.Mkdir(p, 0700); e != nil {
			t.Fatal(e)
		}
	}
	mustMkdir(path)
	root, e := securefs.OpenRoot(path)
	if e != nil {
		t.Fatal(e)
	}
	defer root.Close()
	moved := filepath.Join(parent, "retained")
	if e = os.Rename(path, moved); e != nil {
		t.Fatal(e)
	}
	mustMkdir(path)
	writeTestFile(t, path, "guard", "replacement")
	if e = store.RestoreSnapshotRoot(context.Background(), snapshot, root); e != nil {
		t.Fatal(e)
	}
	assertTestFile(t, moved, "nested/file", "canonicalcanonical")
	assertTestFile(t, path, "guard", "replacement")
	if _, e = os.Stat(filepath.Join(path, "nested")); !os.IsNotExist(e) {
		t.Fatal("restored through replaced path", e)
	}
}
