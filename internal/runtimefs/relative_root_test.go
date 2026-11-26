package runtimefs

import (
	"context"
	"os"
	"testing"
)

func TestRelativeRuntimeRootSupportsSnapshotCaptureAndRestore(t *testing.T) {
	db := openRuntimeFSTestDB(t)
	applyRuntimeFSMigrations(t, db)
	seedRuntimeFSRun(t, db, "relative")
	t.Chdir(t.TempDir())
	if err := os.Mkdir("source", 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile("source/result.txt", []byte("result"), 0600); err != nil {
		t.Fatal(err)
	}
	store, err := NewStore(db, "runtime")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, err = store.CaptureSnapshot(ctx, CaptureSnapshotRequest{SnapshotID: "snapshot", RunID: "relative", SnapshotKind: "attempt_result", RootPath: "source", CreatedAt: "2026-10-23T00:00:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir("restored", 0700); err != nil {
		t.Fatal(err)
	}
	if err = store.RestoreSnapshot(ctx, "snapshot", "restored"); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile("restored/result.txt")
	if err != nil || string(got) != "result" {
		t.Fatal(string(got), err)
	}
}
