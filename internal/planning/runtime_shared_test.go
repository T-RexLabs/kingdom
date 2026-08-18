package planning

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// One unreadable directory used to abort the whole walk, which failed the
// snapshot — and therefore the session that depends on it. It must now be
// recorded and stepped over like any other unreadable entry.
func TestScanProjectRootSurvivesUnreadableDirectory(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeTestFile(t, root, "README.md", "# Project\n")
	blocked := filepath.Join(root, "blocked")
	if err := os.Mkdir(blocked, 0o000); err != nil {
		t.Fatalf("create unreadable directory: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(blocked, 0o700) })
	if _, err := os.ReadDir(blocked); err == nil {
		t.Skip("directory permissions are not enforced for this user")
	}

	summary, err := scanProjectRoot(context.Background(), root)
	if err != nil {
		t.Fatalf("scan aborted on an unreadable directory: %v", err)
	}
	if len(summary.IncludedFiles) != 1 || summary.IncludedFiles[0].Path != "README.md" {
		t.Fatalf("readable files were lost with the unreadable one: %+v", summary.IncludedFiles)
	}
	var recorded bool
	for _, entry := range summary.ExcludedFiles {
		if entry.Path == "blocked" && entry.Reason == "walk_failed" {
			recorded = true
		}
	}
	if !recorded {
		t.Fatalf("unreadable directory was not recorded as excluded: %+v", summary.ExcludedFiles)
	}
}

// Dependency and build output would otherwise exhaust the file budget before
// the walk ever reached real source, and add one excluded row per file to
// every snapshot.
func TestScanProjectRootSkipsDependencyAndBuildDirectories(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeTestFile(t, root, "main.go", "package main\n")
	writeTestFile(t, root, filepath.Join("node_modules", "left-pad", "index.js"), "module.exports = 1\n")
	writeTestFile(t, root, filepath.Join("web", "node_modules", "nested", "index.js"), "module.exports = 2\n")
	writeTestFile(t, root, filepath.Join("dist", "bundle.js"), "console.log(1)\n")
	writeTestFile(t, root, filepath.Join("target", "debug", "binary.txt"), "binary\n")
	writeTestFile(t, root, filepath.Join("internal", "app", "app.go"), "package app\n")

	summary, err := scanProjectRoot(context.Background(), root)
	if err != nil {
		t.Fatalf("scan project root: %v", err)
	}

	included := make(map[string]bool, len(summary.IncludedFiles))
	for _, entry := range summary.IncludedFiles {
		included[entry.Path] = true
	}
	for _, want := range []string{"main.go", "internal/app/app.go"} {
		if !included[want] {
			t.Fatalf("expected %q in the snapshot, got %+v", want, summary.IncludedFiles)
		}
	}
	for path := range included {
		for _, skipped := range []string{"node_modules/", "dist/", "target/"} {
			if strings.Contains(path, skipped) {
				t.Fatalf("skipped directory leaked into the snapshot: %q", path)
			}
		}
	}
	// A skipped tree costs exactly one excluded row, not one per file.
	for _, entry := range summary.ExcludedFiles {
		if strings.Count(entry.Path, "node_modules") > 0 && entry.Reason != "excluded_directory" {
			t.Fatalf("expected node_modules to be excluded as a directory, got %+v", entry)
		}
	}
}
