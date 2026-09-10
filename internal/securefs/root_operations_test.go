//go:build darwin || linux

package securefs

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestReadRegularEnforcesExactByteLimitAndRejectsSpecialFiles(t *testing.T) {
	t.Parallel()

	rootPath := t.TempDir()
	if err := os.WriteFile(filepath.Join(rootPath, "exact.json"), []byte("1234"), 0o600); err != nil {
		t.Fatalf("write exact file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(rootPath, "overflow.json"), []byte("12345"), 0o600); err != nil {
		t.Fatalf("write overflow file: %v", err)
	}
	outside := filepath.Join(t.TempDir(), "outside.json")
	if err := os.WriteFile(outside, []byte("outside"), 0o600); err != nil {
		t.Fatalf("write symlink target: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(rootPath, "symlink.json")); err != nil {
		t.Fatalf("create symlink: %v", err)
	}
	if err := unix.Mkfifo(filepath.Join(rootPath, "fifo.json"), 0o600); err != nil {
		t.Fatalf("create fifo: %v", err)
	}
	root, err := OpenRoot(rootPath)
	if err != nil {
		t.Fatalf("open root: %v", err)
	}
	defer root.Close()

	raw, exactIdentity, err := root.ReadRegularIdentified("exact.json", 4)
	if err != nil {
		t.Fatalf("read exact limit: %v", err)
	}
	if string(raw) != "1234" {
		t.Fatalf("exact bytes = %q", raw)
	}
	_, overflowIdentity, err := root.ReadRegularIdentified("overflow.json", 4)
	if !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("overflow error = %v, want ErrLimitExceeded", err)
	}
	if exactIdentity == (FileIdentity{}) || overflowIdentity == (FileIdentity{}) {
		t.Fatal("exact and overflow reads must return stable identities")
	}
	if exactIdentity.SizeBytes != 4 || overflowIdentity.SizeBytes != 5 {
		t.Fatalf("identity sizes = (%d, %d), want (4, 5)", exactIdentity.SizeBytes, overflowIdentity.SizeBytes)
	}
	if _, err := root.ReadRegular("symlink.json", 64); err == nil {
		t.Fatal("expected symlink read rejection")
	}

	started := time.Now()
	if _, err := root.ReadRegular("fifo.json", 64); err == nil {
		t.Fatal("expected fifo read rejection")
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("fifo rejection blocked for %s", elapsed)
	}
}

func TestReadBoundedRegularRejectsMutationDuringRead(t *testing.T) {
	t.Parallel()

	before := FileIdentity{Device: 1, Inode: 2, SizeBytes: 4, ModTimeSeconds: 3, ChangeTimeSeconds: 4}
	after := before
	after.ChangeTimeNanoseconds = 1
	mutated := false
	reader := &mutationReader{
		reader: bytes.NewBufferString("1234"),
		mutate: func() { mutated = true },
	}

	raw, identity, err := readBoundedRegular(reader, 4, before, func() (FileIdentity, error) {
		if mutated {
			return after, nil
		}
		return before, nil
	})
	if !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("mutation error = %v, want ErrIdentityMismatch", err)
	}
	if raw != nil {
		t.Fatalf("mutated read returned bytes %q", raw)
	}
	if identity != after {
		t.Fatalf("mutation identity = %#v, want stable post-read identity %#v", identity, after)
	}
}

type mutationReader struct {
	reader *bytes.Buffer
	mutate func()
	done   bool
}

func (r *mutationReader) Read(p []byte) (int, error) {
	if !r.done {
		r.done = true
		r.mutate()
	}
	return r.reader.Read(p)
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

	entries, err := root.ReadDirBounded(".", 16)
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

func TestEnsureDirRejectsSymlinkParent(t *testing.T) {
	t.Parallel()

	rootPath := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(rootPath, "parent")); err != nil {
		t.Fatalf("create parent symlink: %v", err)
	}
	root, err := OpenRoot(rootPath)
	if err != nil {
		t.Fatalf("open root: %v", err)
	}
	defer root.Close()
	if err := root.EnsureDir("parent/child", 0o755); err == nil {
		t.Fatal("expected symlink parent rejection")
	}
	if _, err := os.Stat(filepath.Join(outside, "child")); !os.IsNotExist(err) {
		t.Fatalf("symlink target was modified: %v", err)
	}
}

func TestRemoveEmptyDirNeverRecursivelyDeletes(t *testing.T) {
	t.Parallel()

	rootPath := t.TempDir()
	if err := os.Mkdir(filepath.Join(rootPath, "empty"), 0o755); err != nil {
		t.Fatalf("create empty directory: %v", err)
	}
	if err := os.Mkdir(filepath.Join(rootPath, "nonempty"), 0o755); err != nil {
		t.Fatalf("create nonempty directory: %v", err)
	}
	guard := filepath.Join(rootPath, "nonempty", "guard")
	if err := os.WriteFile(guard, []byte("guard"), 0o600); err != nil {
		t.Fatalf("write guard: %v", err)
	}
	root, err := OpenRoot(rootPath)
	if err != nil {
		t.Fatalf("open root: %v", err)
	}
	defer root.Close()

	if err := root.RemoveEmptyDir("nonempty"); err == nil {
		t.Fatal("expected nonempty directory rejection")
	}
	assertFileContents(t, guard, "guard")
	if err := root.RemoveEmptyDir("empty"); err != nil {
		t.Fatalf("remove empty directory: %v", err)
	}
	if _, err := os.Stat(filepath.Join(rootPath, "empty")); !os.IsNotExist(err) {
		t.Fatalf("empty directory remains: %v", err)
	}
}

func TestRemoveEntryUsesPinnedParentAfterReplacement(t *testing.T) {
	t.Parallel()

	rootPath := t.TempDir()
	parentPath := filepath.Join(rootPath, "parent")
	movedPath := filepath.Join(rootPath, "moved")
	outside := t.TempDir()
	if err := os.Mkdir(parentPath, 0o755); err != nil {
		t.Fatalf("create parent: %v", err)
	}
	if err := os.WriteFile(filepath.Join(parentPath, "receipt.json"), []byte("receipt"), 0o600); err != nil {
		t.Fatalf("write receipt: %v", err)
	}
	outsideGuard := filepath.Join(outside, "receipt.json")
	if err := os.WriteFile(outsideGuard, []byte("outside"), 0o600); err != nil {
		t.Fatalf("write outside guard: %v", err)
	}
	root, err := OpenRoot(rootPath)
	if err != nil {
		t.Fatalf("open root: %v", err)
	}
	defer root.Close()
	parentFD, leaf, err := openParent(root, "parent/receipt.json", false, 0)
	if err != nil {
		t.Fatalf("pin parent: %v", err)
	}
	defer unix.Close(parentFD)
	if err := os.Rename(parentPath, movedPath); err != nil {
		t.Fatalf("move pinned parent: %v", err)
	}
	if err := os.Symlink(outside, parentPath); err != nil {
		t.Fatalf("replace parent with symlink: %v", err)
	}

	if err := removeEntryAt(parentFD, leaf, "parent/receipt.json"); err != nil {
		t.Fatalf("remove through pinned parent: %v", err)
	}
	if _, err := os.Stat(filepath.Join(movedPath, "receipt.json")); !os.IsNotExist(err) {
		t.Fatalf("pinned entry remains: %v", err)
	}
	assertFileContents(t, outsideGuard, "outside")
}

func TestRemoveRegularIfIdentityRefusesReplacement(t *testing.T) {
	t.Parallel()

	rootPath := t.TempDir()
	path := filepath.Join(rootPath, "receipt.json")
	if err := os.WriteFile(path, []byte("first"), 0o600); err != nil {
		t.Fatalf("write first receipt: %v", err)
	}
	root, err := OpenRoot(rootPath)
	if err != nil {
		t.Fatalf("open root: %v", err)
	}
	defer root.Close()
	_, identity, err := root.ReadRegularIdentified("receipt.json", 64)
	if err != nil {
		t.Fatalf("read identified receipt: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove first receipt: %v", err)
	}
	if err := os.WriteFile(path, []byte("replacement"), 0o600); err != nil {
		t.Fatalf("write replacement receipt: %v", err)
	}
	if err := root.RemoveRegularIfIdentity("receipt.json", identity); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("conditional removal error = %v, want ErrIdentityMismatch", err)
	}
	assertFileContents(t, path, "replacement")
}

func TestRemoveRegularIfIdentityRefusesInPlaceMutation(t *testing.T) {
	t.Parallel()

	rootPath := t.TempDir()
	path := filepath.Join(rootPath, "receipt.json")
	if err := os.WriteFile(path, []byte("first"), 0o600); err != nil {
		t.Fatalf("write receipt: %v", err)
	}
	root, err := OpenRoot(rootPath)
	if err != nil {
		t.Fatalf("open root: %v", err)
	}
	defer root.Close()
	_, identity, err := root.ReadRegularIdentified("receipt.json", 64)
	if err != nil {
		t.Fatalf("read identified receipt: %v", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open receipt for mutation: %v", err)
	}
	if _, err := file.WriteAt([]byte("other"), 0); err != nil {
		file.Close()
		t.Fatalf("mutate receipt in place: %v", err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		t.Fatalf("sync mutated receipt: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close mutated receipt: %v", err)
	}
	future := time.Now().Add(24 * time.Hour)
	if err := os.Chtimes(path, future, future); err != nil {
		t.Fatalf("set deterministic mutation timestamp: %v", err)
	}

	if err := root.RemoveRegularIfIdentity("receipt.json", identity); !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("conditional removal error = %v, want ErrIdentityMismatch", err)
	}
	assertFileContents(t, path, "other")
}

func TestCreateRegularExactCreatesAndReplaysWithoutReplacement(t *testing.T) {
	t.Parallel()
	rootPath := t.TempDir()
	root, err := OpenRoot(rootPath)
	if err != nil {
		t.Fatalf("open root: %v", err)
	}
	defer root.Close()

	first, err := root.CreateRegularExact("trusted/handoff.json", []byte(`{"value":1}`), 0o600, 0o700)
	if err != nil {
		t.Fatalf("create exact regular file: %v", err)
	}
	replay, err := root.CreateRegularExact("trusted/handoff.json", []byte(`{"value":1}`), 0o600, 0o700)
	if err != nil {
		t.Fatalf("replay exact regular file: %v", err)
	}
	if replay != first || first.ContentSHA256 == "" {
		t.Fatalf("replay identity = %#v, want %#v", replay, first)
	}
	info, err := os.Stat(filepath.Join(rootPath, "trusted", "handoff.json"))
	if err != nil {
		t.Fatalf("stat exact regular file: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("exact regular mode = %o, want 600", info.Mode().Perm())
	}
	if _, err := root.CreateRegularExact("trusted/handoff.json", []byte(`{"value":2}`), 0o600, 0o700); !errors.Is(err, ErrContentConflict) {
		t.Fatalf("changed replay error = %v, want ErrContentConflict", err)
	}
	assertFileContents(t, filepath.Join(rootPath, "trusted", "handoff.json"), `{"value":1}`)
}
