package trackedfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"kingdom/internal/securefs"
)

const (
	formerWorkspaceEntries   = 10000
	formerWorkspaceFileBytes = 32 * 1024 * 1024
	formerWorkspaceBytes     = 256 * 1024 * 1024
	formerWorkspaceDepth     = 64
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

	tracked, err := collectTrackedFiles(root)
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
	files, err := collectTrackedFilesRoot(root)
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

func TestWalkTrackedFilesAcceptsFormerWorkspaceEntryAndByteBounds(t *testing.T) {
	walkers := []struct {
		name string
		walk func(string) ([]TrackedFile, error)
	}{
		{name: "path", walk: collectTrackedFiles},
		{name: "root", walk: func(path string) ([]TrackedFile, error) {
			root, err := securefs.OpenRoot(path)
			if err != nil {
				return nil, err
			}
			defer root.Close()
			return collectTrackedFilesRoot(root)
		}},
	}
	for _, walker := range walkers {
		t.Run(walker.name, func(t *testing.T) {
			entryRoot := t.TempDir()
			for index := range formerWorkspaceEntries {
				if err := os.WriteFile(filepath.Join(entryRoot, fmt.Sprintf("entry-%05d", index)), nil, 0o600); err != nil {
					t.Fatalf("write entry %d: %v", index, err)
				}
			}
			tracked, err := walker.walk(entryRoot)
			if err != nil {
				t.Fatalf("exact entry boundary rejected: %v", err)
			}
			if len(tracked) != formerWorkspaceEntries {
				t.Fatalf("exact entry boundary returned %d files", len(tracked))
			}
			if err := os.WriteFile(filepath.Join(entryRoot, "one-too-many"), nil, 0o600); err != nil {
				t.Fatalf("write overflow entry: %v", err)
			}
			if files, err := walker.walk(entryRoot); err != nil || len(files) != 10001 {
				t.Fatalf("former entry limit: count=%d error=%v", len(files), err)
			}

			byteRoot := t.TempDir()
			for index := range int(formerWorkspaceBytes / formerWorkspaceFileBytes) {
				path := filepath.Join(byteRoot, fmt.Sprintf("file-%d", index))
				if err := os.WriteFile(path, nil, 0o600); err != nil {
					t.Fatalf("create exact byte file %d: %v", index, err)
				}
				if err := os.Truncate(path, formerWorkspaceFileBytes); err != nil {
					t.Fatalf("truncate exact byte file %d: %v", index, err)
				}
			}
			if _, err := walker.walk(byteRoot); err != nil {
				t.Fatalf("exact aggregate-byte boundary rejected: %v", err)
			}
			if err := os.WriteFile(filepath.Join(byteRoot, "one-byte-over"), []byte{'x'}, 0o600); err != nil {
				t.Fatalf("write aggregate overflow: %v", err)
			}
			if _, err := walker.walk(byteRoot); err != nil {
				t.Fatalf("former aggregate limit: %v", err)
			}

			fileRoot := t.TempDir()
			path := filepath.Join(fileRoot, "exact-file")
			if err := os.WriteFile(path, nil, 0o600); err != nil {
				t.Fatalf("create exact file: %v", err)
			}
			if err := os.Truncate(path, formerWorkspaceFileBytes); err != nil {
				t.Fatalf("truncate exact file: %v", err)
			}
			if _, err := walker.walk(fileRoot); err != nil {
				t.Fatalf("exact file boundary rejected: %v", err)
			}
			if err := os.Truncate(path, formerWorkspaceFileBytes+1); err != nil {
				t.Fatalf("truncate one-byte file overflow: %v", err)
			}
			if _, err := walker.walk(fileRoot); err != nil {
				t.Fatalf("former file limit: %v", err)
			}
		})
	}
}

func TestWalkTrackedFilesAcceptsFormerDirectorySymlinkAndDepthBounds(t *testing.T) {
	walkers := []struct {
		name string
		walk func(string) ([]TrackedFile, error)
	}{
		{name: "path", walk: collectTrackedFiles},
		{name: "root", walk: func(path string) ([]TrackedFile, error) {
			root, err := securefs.OpenRoot(path)
			if err != nil {
				return nil, err
			}
			defer root.Close()
			return collectTrackedFilesRoot(root)
		}},
	}

	for _, walker := range walkers {
		t.Run(walker.name, func(t *testing.T) {
			for _, tc := range []struct {
				name  string
				build func(string) error
				check func(error) bool
			}{
				{
					name: "directories",
					build: func(root string) error {
						for index := 0; index <= formerWorkspaceEntries; index++ {
							if err := os.Mkdir(filepath.Join(root, fmt.Sprintf("dir-%05d", index)), 0o700); err != nil {
								return err
							}
						}
						return nil
					},
					check: func(err error) bool {
						return err == nil
					},
				},
				{
					name: "symlinks",
					build: func(root string) error {
						for index := 0; index <= formerWorkspaceEntries; index++ {
							if err := os.Symlink("missing-target", filepath.Join(root, fmt.Sprintf("link-%05d", index))); err != nil {
								return err
							}
						}
						return nil
					},
					check: func(err error) bool {
						return err == nil
					},
				},
				{
					name: "depth",
					build: func(root string) error {
						current := root
						for index := 0; index <= formerWorkspaceDepth; index++ {
							current = filepath.Join(current, fmt.Sprintf("level-%02d", index))
							if err := os.Mkdir(current, 0o700); err != nil {
								return err
							}
						}
						return nil
					},
					check: func(err error) bool {
						return err == nil
					},
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					root := t.TempDir()
					if err := tc.build(root); err != nil {
						t.Fatalf("build overflowing workspace: %v", err)
					}
					if err := func() error {
						_, err := walker.walk(root)
						return err
					}(); !tc.check(err) {
						t.Fatalf("former workspace limit rejected: %v", err)
					}
				})
			}
		})
	}
}

func collectTrackedFiles(path string) ([]TrackedFile, error) {
	var files []TrackedFile
	err := VisitTrackedFiles(context.Background(), path, func(file TrackedFile) error { files = append(files, file); return nil })
	sort.Slice(files, func(i, j int) bool { return files[i].RelativePath < files[j].RelativePath })
	return files, err
}
func collectTrackedFilesRoot(root *securefs.Root) ([]TrackedFile, error) {
	var files []TrackedFile
	err := VisitTrackedFilesRoot(context.Background(), root, func(file TrackedFile) error { files = append(files, file); return nil })
	sort.Slice(files, func(i, j int) bool { return files[i].RelativePath < files[j].RelativePath })
	return files, err
}
