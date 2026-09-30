package runtimefs

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

func TestGeneratedContentExcludedAcrossUmbrellaSnapshotLifecycle(t *testing.T) {
	t.Parallel()
	db := openRuntimeFSTestDB(t)
	applyRuntimeFSMigrations(t, db)
	seedRuntimeFSRun(t, db, "run_generated_content")
	store, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	source := t.TempDir()
	files := map[string]string{
		"repo-a/.gitignore":            "local.yaml\ndist/\n",
		"repo-a/local.yaml":            "local configuration",
		"repo-a/src/app.py":            "source",
		"repo-b/README.md":             "another repository",
		"repo-b/fixtures/node_modules": "ordinary file",
		"repo-b/fixtures/.venv":        "another ordinary file",
	}
	for path, body := range files {
		writeTestFile(t, source, path, body)
	}
	// Each nested repository has metadata at the source, absent in the target.
	for _, repo := range []string{"repo-a", "repo-b"} {
		writeTestFile(t, source, repo+"/.git/HEAD", "ref: refs/heads/main\n")
	}
	generated := []string{".venv", "__pycache__", ".ruff_cache", "node_modules", ".terraform"}
	for _, name := range generated {
		writeTestFile(t, source, "repo-a/tools/"+name+"/cache.bin", "excluded source cache")
	}
	base, err := store.CaptureSnapshot(ctx, CaptureSnapshotRequest{
		SnapshotID: "base", RunID: "run_generated_content", SnapshotKind: "baseline", RootPath: source, CreatedAt: "2026-09-30T00:00:00Z",
	})
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := readTestManifest(store, base.SnapshotID)
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest) != len(files) {
		t.Fatalf("baseline contains %d files, want %d: %v", len(manifest), len(files), manifest)
	}
	target := t.TempDir()
	if err := store.RestoreSnapshot(ctx, base.SnapshotID, target); err != nil {
		t.Fatal(err)
	}
	for path, body := range files {
		assertTestFile(t, target, path, body)
	}
	for _, repo := range []string{"repo-a", "repo-b"} {
		if _, err := os.Lstat(filepath.Join(target, repo, ".git")); !os.IsNotExist(err) {
			t.Fatalf("nested Git metadata was copied: %v", err)
		}
	}
	for _, name := range generated {
		writeTestFile(t, target, "repo-a/tools/"+name+"/cache.bin", "provider cache")
	}
	writeTestFile(t, target, "repo-a/.gitignore", "*\n")
	writeTestFile(t, target, "repo-a/src/app.py", "changed source")
	writeTestFile(t, target, "repo-a/dist/new-report.json", "new ignored deliverable")
	if count, err := store.CountChangesAgainstSnapshot(ctx, target, base.SnapshotID); err != nil || count != 3 {
		t.Fatalf("change count = %d, err=%v; want only three project changes", count, err)
	}
	result, err := store.CaptureSnapshot(ctx, CaptureSnapshotRequest{
		SnapshotID: "result", ParentSnapshotID: base.SnapshotID, RunID: "run_generated_content", SnapshotKind: "attempt_result", RootPath: target, CreatedAt: "2026-09-30T00:01:00Z",
	})
	if err != nil {
		t.Fatal(err)
	}
	diff, err := readTestDiff(store, base.SnapshotID, result.SnapshotID)
	if err != nil {
		t.Fatal(err)
	}
	var changed []string
	for _, entry := range diff {
		changed = append(changed, entry.Path)
	}
	want := []string{"repo-a/.gitignore", "repo-a/dist/new-report.json", "repo-a/src/app.py"}
	if !reflect.DeepEqual(changed, want) {
		t.Fatalf("artifact candidate paths = %v, want %v", changed, want)
	}
	// Reopen to exercise durable manifests, then restore without deleting caches.
	restarted, err := NewStore(db, store.root)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := restarted.RestoreSnapshot(ctx, result.SnapshotID, target); err != nil {
			t.Fatal(err)
		}
		for _, name := range generated {
			assertTestFile(t, target, "repo-a/tools/"+name+"/cache.bin", "provider cache")
		}
	}
	assertTestFile(t, target, "repo-a/local.yaml", "local configuration")
	assertTestFile(t, target, "repo-a/dist/new-report.json", "new ignored deliverable")
}

func TestRestoreGeneratedDirectorySymlinksWithoutFollowingTargets(t *testing.T) {
	t.Parallel()
	store, snapshotID := captureRestoreSecurityFixture(t, "run_generated_links", "generated_links", "src/app.txt")
	target := t.TempDir()
	outside := t.TempDir()
	writeTestFile(t, outside, "guard.txt", "outside unchanged")
	for _, name := range []string{"src", ".venv", "node_modules"} {
		if err := os.Symlink(outside, filepath.Join(target, name)); err != nil {
			if runtime.GOOS == "windows" {
				t.Skipf("symlink creation unavailable: %v", err)
			}
			t.Fatal(err)
		}
	}
	if err := store.RestoreSnapshot(context.Background(), snapshotID, target); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{".venv", "node_modules"} {
		if _, err := os.Lstat(filepath.Join(target, name)); !os.IsNotExist(err) {
			t.Fatalf("extra generated-directory symlink %q remains: %v", name, err)
		}
	}
	assertTestFile(t, target, "src/app.txt", "canonicalcanonical")
	assertTestFile(t, outside, "guard.txt", "outside unchanged")
	if _, err := os.Stat(filepath.Join(outside, "app.txt")); !os.IsNotExist(err) {
		t.Fatalf("restore wrote through symlink: %v", err)
	}
}

func TestSnapshotManifestDoesNotReclassifyGeneratedPaths(t *testing.T) {
	t.Parallel()
	store, snapshotID := captureRestoreSecurityFixture(t, "run_canonical_generated", "canonical_generated", "fixture.txt")
	// An accepted canonical record remains authoritative even if a directory
	// name is now excluded from new filesystem captures.
	if _, err := store.db.Exec(`UPDATE filesystem_snapshot_entries SET path = 'fixtures/node_modules/fixture.txt' WHERE snapshot_id = ?`, snapshotID); err != nil {
		t.Fatal(err)
	}
	manifest, err := readTestManifest(store, snapshotID)
	if err != nil {
		t.Fatal(err)
	}
	if _, found := manifest["fixtures/node_modules/fixture.txt"]; !found {
		t.Fatal("canonical manifest entry was silently filtered by generated-content policy")
	}
	target := t.TempDir()
	if err := store.RestoreSnapshot(context.Background(), snapshotID, target); err != nil {
		t.Fatal(err)
	}
	assertTestFile(t, target, "fixtures/node_modules/fixture.txt", "canonicalcanonical")
}
