package trackedfs

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWalkTrackedFilesSkipsProtectedRootsAndNonRegularEntries(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	files := map[string]string{
		"src/app.js":                    "console.log('ready')",
		"docs/notes.md":                 "hello",
		".git/config":                   "ignore",
		".kingdom/state.json":           "ignore",
		"kingdom-artifacts/run/out.txt": "ignore",
	}
	for relativePath, body := range files {
		resolved := filepath.Join(root, filepath.FromSlash(relativePath))
		if err := os.MkdirAll(filepath.Dir(resolved), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", relativePath, err)
		}
		if err := os.WriteFile(resolved, []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", relativePath, err)
		}
	}
	if err := os.Symlink(filepath.Join(root, "src", "app.js"), filepath.Join(root, "docs", "app-link.js")); err != nil {
		t.Fatalf("create symlink: %v", err)
	}

	tracked, err := WalkTrackedFiles(root)
	if err != nil {
		t.Fatalf("walk tracked files: %v", err)
	}
	if len(tracked) != 2 {
		t.Fatalf("expected 2 tracked files, got %d", len(tracked))
	}
	if tracked[0].RelativePath != "docs/notes.md" || tracked[1].RelativePath != "src/app.js" {
		t.Fatalf("unexpected tracked files: %#v", tracked)
	}
}

func TestIsProtectedRelativePath(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		path string
		want bool
	}{
		{path: ".git/config", want: true},
		{path: "./.kingdom/state.json", want: true},
		{path: "kingdom-artifacts/run/file.txt", want: true},
		{path: "src/app.js", want: false},
	}
	for _, tc := range testCases {
		if got := IsProtectedRelativePath(tc.path); got != tc.want {
			t.Fatalf("IsProtectedRelativePath(%q) = %v, want %v", tc.path, got, tc.want)
		}
	}
}

func TestResolveWithinRootRejectsEscapes(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	if _, err := ResolveWithinRoot(root, "../escape.txt"); err == nil {
		t.Fatal("expected escape path to fail")
	}
	resolved, err := ResolveWithinRoot(root, "docs/readme.md")
	if err != nil {
		t.Fatalf("resolve within root: %v", err)
	}
	if filepath.Dir(resolved) != filepath.Join(root, "docs") {
		t.Fatalf("unexpected resolved path %q", resolved)
	}
}
