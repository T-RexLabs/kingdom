package planning

import (
	"context"
	"fmt"
	"os"
	"os/exec"
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

// Inside a git work tree the repository's own ignore rules are the authority on
// what counts as project content. Kingdom keeping a competing skip list there
// was how build output and tool caches ate the file budget before the scan
// reached any source.
func TestScanProjectRootHonoursGitIgnoreRules(t *testing.T) {
	t.Parallel()

	root := initTestGitRepository(t)
	writeTestFile(t, root, ".gitignore", "artifacts/\nscratch.log\n")
	writeTestFile(t, root, "main.go", "package main\n")
	writeTestFile(t, root, filepath.Join("internal", "app", "app.go"), "package app\n")
	writeTestFile(t, root, filepath.Join("artifacts", "report.json"), "{}\n")
	writeTestFile(t, root, "scratch.log", "noise\n")

	summary, err := scanProjectRoot(context.Background(), root)
	if err != nil {
		t.Fatalf("scan project root: %v", err)
	}

	included := includedPaths(summary)
	for _, want := range []string{"main.go", "internal/app/app.go"} {
		if !included[want] {
			t.Fatalf("expected %q in the snapshot, got %+v", want, summary.IncludedFiles)
		}
	}
	for _, unwanted := range []string{"artifacts/report.json", "scratch.log"} {
		if included[unwanted] {
			t.Fatalf("ignored path %q leaked into the snapshot: %+v", unwanted, summary.IncludedFiles)
		}
	}
	// A file the operator has written but not committed yet is exactly the
	// thing they are about to plan about, so `--others` has to be in the query.
	if !included["main.go"] {
		t.Fatalf("uncommitted working-tree file was dropped: %+v", summary.IncludedFiles)
	}
}

func TestScanProjectRootPreservesGitPathBytes(t *testing.T) {
	t.Parallel()

	root := initTestGitRepository(t)
	files := map[string]string{
		" leading.txt":          "leading space\n",
		"trailing.txt ":         "trailing space\n",
		"embedded\nnewline.txt": "embedded newline\n",
		"collision.txt":         "plain collision\n",
		" collision.txt ":       "spaced collision with a different size\n",
	}
	for path, content := range files {
		writeTestFile(t, root, path, content)
	}

	summary, err := scanProjectRoot(context.Background(), root)
	if err != nil {
		t.Fatalf("scan project root: %v", err)
	}
	if len(summary.IncludedFiles) != len(files) {
		t.Fatalf("expected %d exact git paths, got %+v", len(files), summary.IncludedFiles)
	}

	included := make(map[string]int64, len(summary.IncludedFiles))
	for _, entry := range summary.IncludedFiles {
		included[entry.Path] = entry.Size
	}
	for path, content := range files {
		if size, ok := included[path]; !ok || size != int64(len(content)) {
			t.Errorf("path %q: got size %d, present=%t; included=%+v", path, size, ok, summary.IncludedFiles)
		}
	}
	for _, entry := range summary.ExcludedFiles {
		if entry.Reason == "stat_failed" {
			t.Errorf("exact git path was normalized before stat: %+v", entry)
		}
	}
}

// Enumeration has to stop at the budget rather than walk the rest of the
// repository to record excluded rows no prompt ever reads.
func TestScanProjectRootStopsAtTheFileBudget(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	for index := 0; index < defaultMaxSnapshotFiles+50; index++ {
		writeTestFile(t, root, fmt.Sprintf("file_%03d.go", index), "package main\n")
	}

	summary, err := scanProjectRoot(context.Background(), root)
	if err != nil {
		t.Fatalf("scan project root: %v", err)
	}
	if len(summary.IncludedFiles) != defaultMaxSnapshotFiles {
		t.Fatalf("expected the budget to cap captures, got %d", len(summary.IncludedFiles))
	}
	if len(summary.ExcludedFiles) != 0 {
		t.Fatalf("expected no excluded rows past the budget, got %+v", summary.ExcludedFiles)
	}
	// Paths past the capture budget still count towards the shape, so the
	// summary reports the whole project rather than the prefix it stored.
	if !strings.Contains(summary.SummaryMarkdown, fmt.Sprintf("- Captured files: %d of %d", defaultMaxSnapshotFiles, defaultMaxSnapshotFiles+50)) {
		t.Fatalf("expected the summary to report the full count, got:\n%s", summary.SummaryMarkdown)
	}
}

// The summary is the only part of a snapshot a planner prompt ever sees, so it
// has to describe the project rather than name five arbitrary files.
func TestScanProjectRootSummaryDescribesLayout(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	writeTestFile(t, root, "README.md", "# Project\n")
	writeTestFile(t, root, filepath.Join("internal", "app", "app.go"), "package app\n")
	writeTestFile(t, root, filepath.Join("internal", "api", "api.go"), "package api\n")
	writeTestFile(t, root, filepath.Join("web", "main.ts"), "export {}\n")

	summary, err := scanProjectRoot(context.Background(), root)
	if err != nil {
		t.Fatalf("scan project root: %v", err)
	}
	for _, want := range []string{"- Captured files: 4 of 4", "internal (2)", "web (1)", "(root) (1)"} {
		if !strings.Contains(summary.SummaryMarkdown, want) {
			t.Fatalf("expected %q in the summary, got:\n%s", want, summary.SummaryMarkdown)
		}
	}
}

func includedPaths(summary SnapshotSummary) map[string]bool {
	included := make(map[string]bool, len(summary.IncludedFiles))
	for _, entry := range summary.IncludedFiles {
		included[entry.Path] = true
	}
	return included
}

func initTestGitRepository(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not available")
	}
	root := t.TempDir()
	if output, err := exec.Command("git", "-C", root, "init").CombinedOutput(); err != nil {
		t.Skipf("git init failed: %v (%s)", err, strings.TrimSpace(string(output)))
	}
	return root
}
