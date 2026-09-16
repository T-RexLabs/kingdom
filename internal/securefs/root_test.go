//go:build darwin || linux

package securefs

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestRootRemainsPinnedWhenRootPathIsReplaced(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	rootPath := filepath.Join(parent, "root")
	movedPath := filepath.Join(parent, "moved")
	outside := t.TempDir()
	if err := os.Mkdir(rootPath, 0o755); err != nil {
		t.Fatalf("create root: %v", err)
	}
	root, err := OpenRoot(rootPath)
	if err != nil {
		t.Fatalf("open secure root: %v", err)
	}
	defer root.Close()
	if err := os.Rename(rootPath, movedPath); err != nil {
		t.Fatalf("move root path: %v", err)
	}
	if err := os.Symlink(outside, rootPath); err != nil {
		t.Fatalf("replace root path with symlink: %v", err)
	}
	modifiedAt := time.Unix(1_700_000_000, 0)
	if err := root.ReplaceRegular("nested/file.txt", []byte("restored"), 0o640, 0o755, modifiedAt); err != nil {
		t.Fatalf("replace through pinned root: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(movedPath, "nested", "file.txt"))
	if err != nil {
		t.Fatalf("read pinned-root output: %v", err)
	}
	if string(raw) != "restored" {
		t.Fatalf("pinned-root output = %q", raw)
	}
	if _, err := os.Stat(filepath.Join(outside, "nested", "file.txt")); !os.IsNotExist(err) {
		t.Fatalf("replacement symlink target was modified: %v", err)
	}
}

func TestReplaceRegularReplacesSymlinkParentWithoutFollowingIt(t *testing.T) {
	t.Parallel()

	rootPath := t.TempDir()
	outside := t.TempDir()
	outsidePath := filepath.Join(outside, "file.txt")
	if err := os.WriteFile(outsidePath, []byte("outside"), 0o644); err != nil {
		t.Fatalf("write outside guard: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(rootPath, "nested")); err != nil {
		t.Fatalf("create parent symlink: %v", err)
	}
	root, err := OpenRoot(rootPath)
	if err != nil {
		t.Fatalf("open secure root: %v", err)
	}
	defer root.Close()

	modifiedAt := time.Unix(1_700_000_000, 0)
	for range 2 {
		if err := root.ReplaceRegular("nested/file.txt", []byte("restored"), 0o640, 0o755, modifiedAt); err != nil {
			t.Fatalf("replace through symlink parent: %v", err)
		}
	}
	assertFileContents(t, filepath.Join(rootPath, "nested", "file.txt"), "restored")
	assertFileContents(t, outsidePath, "outside")
}

func TestReplaceRegularReplacesFinalSymlinkWithoutFollowingIt(t *testing.T) {
	t.Parallel()

	rootPath := t.TempDir()
	outsidePath := filepath.Join(t.TempDir(), "outside.txt")
	if err := os.WriteFile(outsidePath, []byte("outside"), 0o644); err != nil {
		t.Fatalf("write outside guard: %v", err)
	}
	if err := os.Symlink(outsidePath, filepath.Join(rootPath, "file.txt")); err != nil {
		t.Fatalf("create final symlink: %v", err)
	}
	root, err := OpenRoot(rootPath)
	if err != nil {
		t.Fatalf("open secure root: %v", err)
	}
	defer root.Close()

	modifiedAt := time.Unix(1_700_000_000, 0)
	for range 2 {
		if err := root.ReplaceRegular("file.txt", []byte("restored"), 0o640, 0o755, modifiedAt); err != nil {
			t.Fatalf("replace final symlink: %v", err)
		}
	}
	assertFileContents(t, filepath.Join(rootPath, "file.txt"), "restored")
	assertFileContents(t, outsidePath, "outside")
}

func TestReplaceRegularUsesPinnedParentWhenPathIsReplaced(t *testing.T) {
	t.Parallel()

	rootPath := t.TempDir()
	parentPath := filepath.Join(rootPath, "nested")
	movedPath := filepath.Join(rootPath, "moved")
	outside := t.TempDir()
	if err := os.Mkdir(parentPath, 0o755); err != nil {
		t.Fatalf("create parent directory: %v", err)
	}
	root, err := OpenRoot(rootPath)
	if err != nil {
		t.Fatalf("open secure root: %v", err)
	}
	defer root.Close()
	parentFD, leaf, err := openParent(root, "nested/file.txt", true, 0o755)
	if err != nil {
		t.Fatalf("pin destination parent: %v", err)
	}
	defer unix.Close(parentFD)
	if err := os.Rename(parentPath, movedPath); err != nil {
		t.Fatalf("move pinned parent: %v", err)
	}
	if err := os.Symlink(outside, parentPath); err != nil {
		t.Fatalf("replace parent path with symlink: %v", err)
	}

	if err := replaceRegularAt(parentFD, leaf, "nested/file.txt", []byte("restored"), 0o640, time.Unix(1_700_000_000, 0)); err != nil {
		t.Fatalf("replace through pinned parent: %v", err)
	}
	assertFileContents(t, filepath.Join(movedPath, "file.txt"), "restored")
	if _, err := os.Stat(filepath.Join(outside, "file.txt")); !os.IsNotExist(err) {
		t.Fatalf("replacement symlink target was modified: %v", err)
	}
}

func TestReplaceRegularDoesNotRecursivelyReplaceDirectory(t *testing.T) {
	t.Parallel()

	rootPath := t.TempDir()
	if err := os.Mkdir(filepath.Join(rootPath, "file.txt"), 0o755); err != nil {
		t.Fatalf("create conflicting directory: %v", err)
	}
	guardPath := filepath.Join(rootPath, "file.txt", "guard.txt")
	if err := os.WriteFile(guardPath, []byte("guard"), 0o644); err != nil {
		t.Fatalf("write directory guard: %v", err)
	}
	root, err := OpenRoot(rootPath)
	if err != nil {
		t.Fatalf("open secure root: %v", err)
	}
	defer root.Close()

	err = root.ReplaceRegular("file.txt", []byte("restored"), 0o640, 0o755, time.Unix(1_700_000_000, 0))
	if err == nil {
		t.Fatal("expected real directory shape conflict")
	}
	assertFileContents(t, guardPath, "guard")
}

func TestRemoveEntryUnlinksFinalSymlinkWithoutFollowingIt(t *testing.T) {
	t.Parallel()

	rootPath := t.TempDir()
	outsidePath := filepath.Join(t.TempDir(), "outside.txt")
	if err := os.WriteFile(outsidePath, []byte("outside"), 0o644); err != nil {
		t.Fatalf("write outside guard: %v", err)
	}
	if err := os.Symlink(outsidePath, filepath.Join(rootPath, "deleted.txt")); err != nil {
		t.Fatalf("create deletion symlink: %v", err)
	}
	root, err := OpenRoot(rootPath)
	if err != nil {
		t.Fatalf("open secure root: %v", err)
	}
	defer root.Close()

	if err := root.RemoveEntry("deleted.txt"); err != nil {
		t.Fatalf("remove final symlink: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(rootPath, "deleted.txt")); !os.IsNotExist(err) {
		t.Fatalf("deletion symlink remains: %v", err)
	}
	assertFileContents(t, outsidePath, "outside")
}

func TestRemoveTreeUnlinksNestedSymlinksWithoutFollowingThem(t *testing.T) {
	t.Parallel()

	rootPath := t.TempDir()
	targetPath := filepath.Join(rootPath, "target")
	outsidePath := filepath.Join(t.TempDir(), "outside.txt")
	if err := os.MkdirAll(filepath.Join(targetPath, "nested"), 0o755); err != nil {
		t.Fatalf("create target tree: %v", err)
	}
	if err := os.WriteFile(outsidePath, []byte("outside"), 0o644); err != nil {
		t.Fatalf("write outside guard: %v", err)
	}
	if err := os.Symlink(outsidePath, filepath.Join(targetPath, "nested", "outside-link")); err != nil {
		t.Fatalf("create nested symlink: %v", err)
	}

	root, err := OpenRoot(rootPath)
	if err != nil {
		t.Fatalf("open secure root: %v", err)
	}
	defer root.Close()
	if err := root.RemoveTree("target"); err != nil {
		t.Fatalf("remove target tree: %v", err)
	}
	if _, err := os.Lstat(targetPath); !os.IsNotExist(err) {
		t.Fatalf("target tree remains: %v", err)
	}
	assertFileContents(t, outsidePath, "outside")
}

func assertFileContents(t *testing.T, path, want string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if string(raw) != want {
		t.Fatalf("%s = %q, want %q", path, raw, want)
	}
}
