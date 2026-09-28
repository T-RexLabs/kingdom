//go:build windows

package securefs

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

func TestWindowsRemoveEntryTreatsReparseParentAsAbsentWithoutFollowing(t *testing.T) {
	rootPath := t.TempDir()
	outside := t.TempDir()
	guard := filepath.Join(outside, "deleted.txt")
	if err := os.WriteFile(guard, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(rootPath, "nested")
	if err := os.Symlink(outside, link); err != nil {
		if errors.Is(err, windows.ERROR_PRIVILEGE_NOT_HELD) || errors.Is(err, windows.ERROR_ACCESS_DENIED) {
			t.Skipf("directory symlink creation is unavailable: %v", err)
		}
		t.Fatalf("create reparse-point parent: %v", err)
	}

	root, err := OpenRoot(rootPath)
	if err != nil {
		t.Fatalf("open secure root: %v", err)
	}
	defer root.Close()
	if err := root.RemoveEntry("nested/deleted.txt"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("remove through reparse-point parent error=%v, want not-exist", err)
	}
	if raw, err := os.ReadFile(guard); err != nil || string(raw) != "outside" {
		t.Fatalf("outside target changed: bytes=%q err=%v", raw, err)
	}
	if _, err := os.Lstat(link); err != nil {
		t.Fatalf("remove of child path also removed its reparse-point parent: %v", err)
	}
}
