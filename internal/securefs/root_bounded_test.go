//go:build darwin || linux || windows

package securefs

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestSplitRelativePathPreservesWhitespaceAndRejectsInvalidPaths(t *testing.T) {
	absolutePath, err := filepath.Abs("outside")
	if err != nil {
		t.Fatalf("resolve absolute test path: %v", err)
	}
	tests := []struct {
		name    string
		path    string
		want    []string
		wantErr bool
	}{
		{name: "leading component whitespace", path: " leading", want: []string{" leading"}},
		{name: "trailing component whitespace", path: "trailing ", want: []string{"trailing "}},
		{name: "whitespace-only name", path: " ", want: []string{" "}},
		{name: "whitespace in multiple components", path: " leading / trailing ", want: []string{" leading ", " trailing "}},
		{name: "empty path", path: "", wantErr: true},
		{name: "absolute path", path: absolutePath, wantErr: true},
		{name: "traversal path", path: "../outside", wantErr: true},
		{name: "root path", path: ".", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := splitRelativePath(test.path)
			if test.wantErr {
				if err == nil {
					t.Fatalf("splitRelativePath(%q) = %q, want an error", test.path, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("splitRelativePath(%q): %v", test.path, err)
			}
			if len(got) != len(test.want) {
				t.Fatalf("splitRelativePath(%q) = %#v, want %#v", test.path, got, test.want)
			}
			for index := range test.want {
				if got[index] != test.want[index] {
					t.Fatalf("splitRelativePath(%q) = %#v, want %#v", test.path, got, test.want)
				}
			}
		})
	}
}

func TestReadDirDoesNotTreatWhitespaceOnlyPathAsPinnedRoot(t *testing.T) {
	root, err := OpenRoot(t.TempDir())
	if err != nil {
		t.Fatalf("open root: %v", err)
	}
	defer root.Close()

	if _, err := root.ReadDirBounded(" ", 1); err == nil {
		t.Fatal("whitespace-only path resolved to the pinned root")
	}
}

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
	if len(entries) != 1 || entries[0].Name != "receipt.json" || entries[0].Type != EntryTypeSymlink || entries[0].Identity == (FileIdentity{}) {
		t.Fatalf("symlink entry=%#v, want no-follow symlink metadata", entries)
	}
}

func TestRemoveEntryIfIdentityProtectsReplacementSymlink(t *testing.T) {
	rootPath := t.TempDir()
	firstTarget := filepath.Join(t.TempDir(), "first.txt")
	secondTarget := filepath.Join(t.TempDir(), "second.txt")
	if err := os.WriteFile(firstTarget, []byte("first target"), 0o600); err != nil {
		t.Fatalf("write first target: %v", err)
	}
	if err := os.WriteFile(secondTarget, []byte("second target"), 0o600); err != nil {
		t.Fatalf("write second target: %v", err)
	}
	linkPath := filepath.Join(rootPath, "link")
	if err := os.Symlink(firstTarget, linkPath); err != nil {
		if errors.Is(err, os.ErrPermission) {
			t.Skip("symlink creation is unavailable")
		}
		t.Fatalf("create initial symlink: %v", err)
	}
	root, err := OpenRoot(rootPath)
	if err != nil {
		t.Fatalf("open root: %v", err)
	}
	defer root.Close()

	entries, err := root.ReadDirBounded(".", 1)
	if err != nil {
		t.Fatalf("read symlink identity: %v", err)
	}
	if len(entries) != 1 || entries[0].Type != EntryTypeSymlink || entries[0].Identity == (FileIdentity{}) {
		t.Fatalf("symlink identity entry=%#v", entries)
	}
	if err := os.Remove(linkPath); err != nil {
		t.Fatalf("remove initial symlink: %v", err)
	}
	if err := os.Symlink(secondTarget, linkPath); err != nil {
		t.Fatalf("create replacement symlink: %v", err)
	}
	if err := root.RemoveEntryIfIdentity("link", EntryTypeSymlink, entries[0].Identity); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("conditional removal error=%v, want ErrIdentityMismatch", err)
	}
	if info, err := os.Lstat(linkPath); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("replacement symlink was changed: info=%#v err=%v", info, err)
	}
	if raw, err := os.ReadFile(firstTarget); err != nil || string(raw) != "first target" {
		t.Fatalf("first symlink target changed: bytes=%q err=%v", raw, err)
	}
	if raw, err := os.ReadFile(secondTarget); err != nil || string(raw) != "second target" {
		t.Fatalf("replacement symlink target changed: bytes=%q err=%v", raw, err)
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

func TestRemoveTreeBatchIsBoundedAndResumesAfterRootReopen(t *testing.T) {
	rootPath := t.TempDir()
	treePath := filepath.Join(rootPath, "tree")
	for directory := range 7 {
		for file := range 3 {
			name := filepath.Join(treePath, filepath.Join("dir-"+string(rune('a'+directory)), "file-"+string(rune('a'+file))))
			if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
				t.Fatalf("create directory %d: %v", directory, err)
			}
			if err := os.WriteFile(name, []byte("provider content"), 0o600); err != nil {
				t.Fatalf("create file %d/%d: %v", directory, file, err)
			}
		}
	}
	root, err := OpenRoot(rootPath)
	if err != nil {
		t.Fatalf("open root: %v", err)
	}
	defer func() { _ = root.Close() }()

	cursor := ""
	complete := false
	calls := 0
	for !complete {
		progress, removeErr := root.RemoveTreeBatch("tree", cursor, 3)
		if removeErr != nil {
			t.Fatalf("remove tree batch: %v", removeErr)
		}
		if progress.EntriesProcessed > 3 || progress.EntriesRemoved > 3 {
			t.Fatalf("batch exceeded its bound: %+v", progress)
		}
		if !progress.Complete && progress.EntriesProcessed == 0 {
			t.Fatal("incomplete batch made no progress")
		}
		calls++
		cursor = progress.Cursor
		complete = progress.Complete
		if !complete {
			if err := root.Close(); err != nil {
				t.Fatalf("close root between batches: %v", err)
			}
			root, err = OpenRoot(rootPath)
			if err != nil {
				t.Fatalf("reopen root between batches: %v", err)
			}
		}
	}
	if calls < 2 {
		t.Fatalf("tree removal calls=%d, want multiple batches", calls)
	}
	if _, err := os.Lstat(treePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("tree remains after complete batch: %v", err)
	}
}

func TestRemoveTreeBatchRemovesLiteralTrailingSpaceFilename(t *testing.T) {
	rootPath := t.TempDir()
	targetPath := filepath.Join(rootPath, "target")
	if err := os.Mkdir(targetPath, 0o755); err != nil {
		t.Fatalf("create target directory: %v", err)
	}
	const name = "literal trailing "
	literalPath, err := literalFilesystemPath(filepath.Join(targetPath, name))
	if err != nil {
		t.Fatalf("resolve literal filename path: %v", err)
	}
	if err := os.WriteFile(literalPath, []byte("trailing-space file"), 0o600); err != nil {
		t.Fatalf("write literal trailing-space file: %v", err)
	}

	root, err := OpenRoot(rootPath)
	if err != nil {
		t.Fatalf("open root: %v", err)
	}
	defer root.Close()

	entries, err := root.ReadDirBounded("target", 1)
	if err != nil {
		t.Fatalf("enumerate target: %v", err)
	}
	if len(entries) != 1 || entries[0].Name != name {
		t.Fatalf("enumerated entries = %#v, want the exact filename %q", entries, name)
	}

	cursor := ""
	complete := false
	calls := 0
	for !complete {
		if calls >= 4 {
			t.Fatalf("tree removal did not complete after %d batches", calls)
		}
		progress, removeErr := root.RemoveTreeBatch("target", cursor, 1)
		if removeErr != nil {
			t.Fatalf("remove target batch: %v", removeErr)
		}
		if progress.EntriesProcessed > 1 {
			t.Fatalf("batch exceeded its one-entry bound: %+v", progress)
		}
		if !progress.Complete && progress.EntriesProcessed == 0 {
			t.Fatal("incomplete tree removal made no progress")
		}
		cursor = progress.Cursor
		complete = progress.Complete
		calls++
	}
	if calls < 2 {
		t.Fatalf("tree removal calls=%d, want the file and target directory removal", calls)
	}
	if _, err := os.Lstat(literalPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("literal trailing-space file remains after removal: %v", err)
	}
}

func TestRemoveTreeBatchPreservesExternalSymlinkTarget(t *testing.T) {
	rootPath := t.TempDir()
	treePath := filepath.Join(rootPath, "tree")
	if err := os.MkdirAll(treePath, 0o755); err != nil {
		t.Fatalf("create tree: %v", err)
	}
	outsideRoot := t.TempDir()
	guardPath := filepath.Join(outsideRoot, "guard.txt")
	if err := os.WriteFile(guardPath, []byte("outside guard"), 0o600); err != nil {
		t.Fatalf("write external guard: %v", err)
	}
	if err := os.Symlink(outsideRoot, filepath.Join(treePath, "outside-link")); err != nil {
		if errors.Is(err, os.ErrPermission) {
			t.Skip("symlink creation is unavailable")
		}
		t.Fatalf("create external symlink: %v", err)
	}
	root, err := OpenRoot(rootPath)
	if err != nil {
		t.Fatalf("open root: %v", err)
	}
	defer root.Close()

	progress, err := root.RemoveTreeBatch("tree", "", 2)
	if err != nil || !progress.Complete {
		t.Fatalf("remove symlink tree progress=%+v err=%v, want complete", progress, err)
	}
	if raw, err := os.ReadFile(guardPath); err != nil || string(raw) != "outside guard" {
		t.Fatalf("external symlink target changed: bytes=%q err=%v", raw, err)
	}
	if _, err := os.Lstat(treePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("tree remains after removal: %v", err)
	}
}

func literalFilesystemPath(path string) (string, error) {
	if runtime.GOOS != "windows" {
		return path, nil
	}
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	return `\\?\` + absolutePath, nil
}

func TestRemoveTreeBatchDoesNotTreatSymlinkParentAsComplete(t *testing.T) {
	rootPath := t.TempDir()
	outsideRoot := t.TempDir()
	guardPath := filepath.Join(outsideRoot, "guard.txt")
	if err := os.WriteFile(guardPath, []byte("outside guard"), 0o600); err != nil {
		t.Fatalf("write external guard: %v", err)
	}
	if err := os.Symlink(outsideRoot, filepath.Join(rootPath, "parent-link")); err != nil {
		if errors.Is(err, os.ErrPermission) {
			t.Skip("symlink creation is unavailable")
		}
		t.Fatalf("create parent symlink: %v", err)
	}
	root, err := OpenRoot(rootPath)
	if err != nil {
		t.Fatalf("open root: %v", err)
	}
	defer root.Close()

	progress, err := root.RemoveTreeBatch("parent-link/target", "", 4)
	if err == nil || progress.Complete {
		t.Fatalf("removal progress=%+v err=%v, want an inaccessible-path error", progress, err)
	}
	if raw, err := os.ReadFile(guardPath); err != nil || string(raw) != "outside guard" {
		t.Fatalf("symlink parent target changed: bytes=%q err=%v", raw, err)
	}
}
