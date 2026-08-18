package trackedfs

import (
	"os"
	"path/filepath"
	"testing"

	"kingdom/internal/securefs"
)

func TestWalkTrackedFilesSkipsProtectedRootsAndNonRegularEntries(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	files := map[string]string{
		"src/app.js":                    "console.log('ready')",
		"docs/notes.md":                 "hello",
		".git/config":                   "ignore",
		"ai-hub/.git/index":             "ignore nested git",
		".kingdom/state.json":           "ignore",
		"services/api/.kingdom/state":   "ignore nested kingdom",
		".opencode/cache/state":         "ignore tool cache",
		".playwright-mcp/page.yml":      "ignore browser artifact",
		".playwright-cli/page.yml":      "ignore browser artifact",
		"web/node_modules/pkg/index.js": "ignore dependencies",
		"kd-sandbox/run/output.txt":     "ignore sandbox",
		"build/.tmp/cache":              "ignore temp state",
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

func TestWalkTrackedFilesRootUsesPinnedNoFollowTraversal(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	rootPath := filepath.Join(parent, "root")
	movedPath := filepath.Join(parent, "moved")
	outside := t.TempDir()
	if err := os.Mkdir(rootPath, 0o755); err != nil {
		t.Fatalf("create root: %v", err)
	}
	if err := os.WriteFile(filepath.Join(rootPath, "inside.txt"), []byte("inside"), 0o644); err != nil {
		t.Fatalf("write inside file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(outside, "outside.txt"), []byte("outside"), 0o644); err != nil {
		t.Fatalf("write outside file: %v", err)
	}
	root, err := securefs.OpenRoot(rootPath)
	if err != nil {
		t.Fatalf("open secure root: %v", err)
	}
	defer root.Close()
	if err := os.Rename(rootPath, movedPath); err != nil {
		t.Fatalf("move root: %v", err)
	}
	if err := os.Symlink(outside, rootPath); err != nil {
		t.Fatalf("replace root with symlink: %v", err)
	}
	files, err := WalkTrackedFilesRoot(root)
	if err != nil {
		t.Fatalf("walk pinned root: %v", err)
	}
	if len(files) != 1 || files[0].RelativePath != "inside.txt" {
		t.Fatalf("unexpected pinned-root files: %#v", files)
	}
}

func TestIsProtectedRelativePath(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		path string
		want bool
	}{
		{path: ".git/config", want: true},
		{path: "ai-hub/.git/index", want: true},
		{path: "./.kingdom/state.json", want: true},
		{path: "services/api/.kingdom/state.json", want: true},
		{path: "kingdom-artifacts/run/file.txt", want: true},
		{path: ".playwright-mcp/page.yml", want: true},
		{path: ".playwright-cli/page.yml", want: true},
		{path: ".opencode/cache/state", want: true},
		{path: "web/node_modules/pkg/index.js", want: true},
		{path: "kd-sandbox/run/output.txt", want: true},
		{path: "build/.tmp/cache", want: true},
		{path: "src/app.js", want: false},
		{path: "docs/.gitignore", want: false},
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
