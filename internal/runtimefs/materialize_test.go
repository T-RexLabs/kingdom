package runtimefs

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"kingdom/internal/filemode"
)

func TestMaterializeSnapshotPreservesMetadataAndIndependentContent(t *testing.T) {
	store, snapshot := captureRestoreSecurityFixture(t, "run_materialize", "materialize", "nested/file")
	modified := time.Unix(1700000000, 0)
	if _, err := store.db.Exec(`UPDATE filesystem_snapshot_entries SET mode_bits=489,mtime_ns=? WHERE snapshot_id=?`, modified.UnixNano(), snapshot); err != nil {
		t.Fatal(err)
	}
	first, second := t.TempDir(), t.TempDir()
	for _, target := range []string{first, second} {
		if err := store.MaterializeSnapshot(context.Background(), snapshot, target); err != nil {
			t.Fatal(err)
		}
		assertTestFile(t, target, "nested/file", "canonicalcanonical")
		info, err := os.Stat(filepath.Join(target, "nested/file"))
		if err != nil || !filemode.Equal(int64(info.Mode().Perm()), 0o751) || !info.ModTime().Equal(modified) {
			t.Fatalf("metadata=%v err=%v", info, err)
		}
	}
	writeTestFile(t, first, "nested/file", "private mutation")
	assertTestFile(t, second, "nested/file", "canonicalcanonical")
	manifest, err := readTestManifest(store, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	var canonical bytes.Buffer
	if _, err := store.CopyBlob(context.Background(), manifest["nested/file"].BlobID, &canonical); err != nil || canonical.String() != "canonicalcanonical" {
		t.Fatalf("canonical blob changed: %q %v", canonical.String(), err)
	}
}

func TestMaterializeSnapshotRejectsNonemptyAndCorruptInputs(t *testing.T) {
	store, snapshot := captureRestoreSecurityFixture(t, "run_materialize_fail", "materialize_fail", "nested/file")
	nonempty := t.TempDir()
	writeTestFile(t, nonempty, "keep", "untouched")
	if err := store.MaterializeSnapshot(context.Background(), snapshot, nonempty); err == nil {
		t.Fatal("nonempty destination accepted")
	}
	assertTestFile(t, nonempty, "keep", "untouched")
	manifest, err := readTestManifest(store, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	blob, err := store.blobPath(manifest["nested/file"].BlobID)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blob, []byte("corruptedcanonical"), 0o644); err != nil {
		t.Fatal(err)
	}
	target := t.TempDir()
	if err := store.MaterializeSnapshot(context.Background(), snapshot, target); err == nil {
		t.Fatal("corrupt canonical blob accepted")
	}
	if entries, err := os.ReadDir(target); err != nil || len(entries) != 0 {
		t.Fatalf("corrupt stream created ancestors or left staging: %v %v", entries, err)
	}
}
