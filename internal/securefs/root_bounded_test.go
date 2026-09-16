//go:build darwin || linux || windows

package securefs

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestReadDirBoundedDetectsEntryOverflow(t *testing.T) {
	t.Parallel()

	rootPath := t.TempDir()
	for index := range 17 {
		name := filepath.Join(rootPath, "entry-"+string(rune('a'+index)))
		if err := os.WriteFile(name, nil, 0o600); err != nil {
			t.Fatalf("write entry %d: %v", index, err)
		}
	}
	root, err := OpenRoot(rootPath)
	if err != nil {
		t.Fatalf("open root: %v", err)
	}
	defer root.Close()

	entries, err := root.ReadDirBounded(".", 0)
	if !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("zero-limit enumeration error = %v, want ErrLimitExceeded", err)
	}
	if len(entries) != 0 {
		t.Fatalf("zero-limit entries = %d, want 0", len(entries))
	}
	entries, err = root.ReadDirBounded(".", 16)
	if !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("bounded enumeration error = %v, want ErrLimitExceeded", err)
	}
	if len(entries) != 16 {
		t.Fatalf("bounded entries = %d, want 16", len(entries))
	}
	if err := os.Remove(filepath.Join(rootPath, "entry-q")); err != nil {
		t.Fatalf("remove overflow entry: %v", err)
	}
	entries, err = root.ReadDirBounded(".", 16)
	if err != nil {
		t.Fatalf("enumerate exact entry limit: %v", err)
	}
	if len(entries) != 16 {
		t.Fatalf("exact entries = %d, want 16", len(entries))
	}
}

func TestReadDirBoundedClassifiesSymlinkWithoutFollowingIt(t *testing.T) {
	rootPath := t.TempDir()
	target := filepath.Join(t.TempDir(), "target.txt")
	if err := os.WriteFile(target, []byte("outside root"), 0o600); err != nil {
		t.Fatalf("write symlink target: %v", err)
	}
	if err := os.Symlink(target, filepath.Join(rootPath, "receipt.json")); err != nil {
		if errors.Is(err, os.ErrPermission) {
			t.Skip("symlink creation is unavailable")
		}
		t.Fatalf("create symlink: %v", err)
	}
	root, err := OpenRoot(rootPath)
	if err != nil {
		t.Fatalf("open root: %v", err)
	}
	defer root.Close()

	entries, err := root.ReadDirBounded(".", 1)
	if err != nil {
		t.Fatalf("list symlink entry: %v", err)
	}
	if len(entries) != 1 || entries[0].Name != "receipt.json" || entries[0].Type != EntryTypeSymlink {
		t.Fatalf("symlink entry=%#v, want no-follow symlink metadata", entries)
	}
}

func TestReadDirBoundedEntryIdentitySupportsConditionalRemoval(t *testing.T) {
	rootPath := t.TempDir()
	if err := os.WriteFile(filepath.Join(rootPath, "receipt.json"), []byte("receipt"), 0o600); err != nil {
		t.Fatalf("write receipt: %v", err)
	}
	root, err := OpenRoot(rootPath)
	if err != nil {
		t.Fatalf("open root: %v", err)
	}
	defer root.Close()

	entries, err := root.ReadDirBounded(".", 1)
	if err != nil {
		t.Fatalf("list receipt entry: %v", err)
	}
	if len(entries) != 1 || entries[0].Type != EntryTypeRegular || entries[0].Identity == (FileIdentity{}) {
		t.Fatalf("regular entry lacks stable identity: %#v", entries)
	}
	if err := root.RemoveEntryIfIdentity("receipt.json", entries[0].Type, entries[0].Identity); err != nil {
		t.Fatalf("remove listed receipt by identity: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(rootPath, "receipt.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("listed receipt was not removed: %v", err)
	}
}
