//go:build windows

package securefs

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestWindowsEnsureDirCreatesAndReplaysParentChain(t *testing.T) {
	t.Parallel()

	rootPath := t.TempDir()
	root, err := OpenRoot(rootPath)
	if err != nil {
		t.Fatalf("open secure root: %v", err)
	}
	defer root.Close()

	const path = "provider-completion/handoffs"
	if err := root.EnsureDir(path, 0o700); err != nil {
		t.Fatalf("create secure parent chain: %v", err)
	}
	if err := root.EnsureDir(path, 0o700); err != nil {
		t.Fatalf("replay secure parent chain: %v", err)
	}
	for _, directory := range []string{"provider-completion", path} {
		entry, err := root.ReadEntry(directory)
		if err != nil {
			t.Fatalf("inspect created directory %q: %v", directory, err)
		}
		if entry.Type != EntryTypeDirectory {
			t.Fatalf("created entry %q type = %q, want directory", directory, entry.Type)
		}
	}
	entries, err := root.ReadDirBounded(path, 8)
	if err != nil {
		t.Fatalf("list published directory: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("published directory has %d temporary entries, want none", len(entries))
	}
}

func TestWindowsCreateRegularExactPublishesWithoutReplaceAndReplaysExactly(t *testing.T) {
	t.Parallel()

	rootPath := t.TempDir()
	root, err := OpenRoot(rootPath)
	if err != nil {
		t.Fatalf("open secure root: %v", err)
	}
	defer root.Close()

	const path = "provider-completion/handoffs/terminal.json"
	want := []byte(`{"version":"v1","terminal":true}`)
	first, err := root.CreateRegularExact(path, want, 0o600, 0o700)
	if err != nil {
		t.Fatalf("publish exact regular file: %v", err)
	}
	got, err := root.ReadRegular(path, int64(len(want)))
	if err != nil || string(got) != string(want) {
		t.Fatalf("read published file = %q, err=%v", got, err)
	}

	replay, err := root.CreateRegularExact(path, want, 0o600, 0o700)
	if err != nil {
		t.Fatalf("replay exact regular file: %v", err)
	}
	if replay != first {
		t.Fatalf("replay identity = %#v, want original %#v", replay, first)
	}
	if _, err := root.CreateRegularExact(path, []byte(`{"version":"changed"}`), 0o600, 0o700); !errors.Is(err, ErrContentConflict) {
		t.Fatalf("changed replay error = %v, want ErrContentConflict", err)
	}
	got, err = root.ReadRegular(path, int64(len(want)))
	if err != nil || string(got) != string(want) {
		t.Fatalf("changed replay replaced original: contents=%q err=%v", got, err)
	}

	entries, err := root.ReadDirBounded(filepath.Dir(path), 8)
	if err != nil {
		t.Fatalf("list publication parent: %v", err)
	}
	if len(entries) != 1 || entries[0].Name != filepath.Base(path) {
		t.Fatalf("publication parent entries = %#v, want only %q", entries, filepath.Base(path))
	}
	if _, err := os.Stat(filepath.Join(rootPath, filepath.FromSlash(path))); err != nil {
		t.Fatalf("stat published file: %v", err)
	}
}
