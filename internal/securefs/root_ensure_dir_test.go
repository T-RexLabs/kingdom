//go:build darwin || linux || windows

package securefs

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// Windows reports a missing symlink privilege as ERROR_PRIVILEGE_NOT_HELD.
const windowsPrivilegeNotHeld syscall.Errno = 1314

func TestEnsureDirRejectsRegularFileParentWithoutChangingIt(t *testing.T) {
	t.Parallel()

	rootPath := t.TempDir()
	parentPath := filepath.Join(rootPath, "parent")
	wantContents := []byte("keep this file")
	if err := os.WriteFile(parentPath, wantContents, 0o600); err != nil {
		t.Fatalf("write regular-file parent: %v", err)
	}
	before, err := os.Lstat(parentPath)
	if err != nil {
		t.Fatalf("stat regular-file parent before EnsureDir: %v", err)
	}
	root, err := OpenRoot(rootPath)
	if err != nil {
		t.Fatalf("open secure root: %v", err)
	}
	defer root.Close()

	if err := root.EnsureDir(filepath.Join("parent", "child"), 0o755); err == nil {
		t.Fatal("expected regular-file parent rejection")
	}
	after, err := os.Lstat(parentPath)
	if err != nil {
		t.Fatalf("stat regular-file parent after EnsureDir: %v", err)
	}
	if !after.Mode().IsRegular() || !os.SameFile(before, after) {
		t.Fatalf("regular-file parent changed: before=%v after=%v", before.Mode(), after.Mode())
	}
	gotContents, err := os.ReadFile(parentPath)
	if err != nil {
		t.Fatalf("read regular-file parent after EnsureDir: %v", err)
	}
	if string(gotContents) != string(wantContents) {
		t.Fatalf("regular-file parent contents = %q, want %q", gotContents, wantContents)
	}
}

func TestEnsureDirRejectsSymlinkParentWithoutChangingIt(t *testing.T) {
	t.Parallel()

	rootPath := t.TempDir()
	outside := t.TempDir()
	parentPath := filepath.Join(rootPath, "parent")
	if err := os.Symlink(outside, parentPath); err != nil {
		if errors.Is(err, os.ErrPermission) || errors.Is(err, windowsPrivilegeNotHeld) {
			t.Skipf("symlink creation is unavailable: %v", err)
		}
		t.Fatalf("create symlink parent: %v", err)
	}
	before, err := os.Lstat(parentPath)
	if err != nil {
		t.Fatalf("stat symlink parent before EnsureDir: %v", err)
	}
	root, err := OpenRoot(rootPath)
	if err != nil {
		t.Fatalf("open secure root: %v", err)
	}
	defer root.Close()

	if err := root.EnsureDir(filepath.Join("parent", "child"), 0o755); err == nil {
		t.Fatal("expected symlink parent rejection")
	}
	after, err := os.Lstat(parentPath)
	if err != nil {
		t.Fatalf("stat symlink parent after EnsureDir: %v", err)
	}
	if after.Mode()&os.ModeSymlink == 0 || !os.SameFile(before, after) {
		t.Fatalf("symlink parent changed: before=%v after=%v", before.Mode(), after.Mode())
	}
	gotTarget, err := os.Readlink(parentPath)
	if err != nil {
		t.Fatalf("read symlink parent after EnsureDir: %v", err)
	}
	if gotTarget != outside {
		t.Fatalf("symlink parent target = %q, want %q", gotTarget, outside)
	}
	if _, err := os.Lstat(filepath.Join(outside, "child")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("symlink target was modified: %v", err)
	}
}

func TestReplaceRegularReplacesNonDirectoryParents(t *testing.T) {
	t.Parallel()

	rootPath := t.TempDir()
	parentPath := filepath.Join(rootPath, "parent")
	if err := os.WriteFile(parentPath, []byte("replace this file"), 0o600); err != nil {
		t.Fatalf("write regular-file parent: %v", err)
	}
	root, err := OpenRoot(rootPath)
	if err != nil {
		t.Fatalf("open secure root: %v", err)
	}
	defer root.Close()

	if err := root.ReplaceRegular("parent/child.txt", []byte("restored"), 0o600, 0o755, time.Time{}); err != nil {
		t.Fatalf("ReplaceRegular did not preserve replaceable-parent behavior: %v", err)
	}
	if info, err := os.Stat(parentPath); err != nil || !info.IsDir() {
		t.Fatalf("replaceable parent is not a directory: info=%v err=%v", info, err)
	}
	contents, err := os.ReadFile(filepath.Join(parentPath, "child.txt"))
	if err != nil || string(contents) != "restored" {
		t.Fatalf("restored child contents=%q err=%v, want restored", contents, err)
	}
}
