package securefs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type streamTestWriter func([]byte) (int, error)

func (w streamTestWriter) Write(p []byte) (int, error) { return w(p) }

func TestStreamingReplaceRejectsCancellationAndProducerFailureAtomically(t *testing.T) {
	path := t.TempDir()
	root, err := OpenRoot(path)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := os.WriteFile(filepath.Join(path, "file"), []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, cancelDuringWrite := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		failure := errors.New("digest mismatch")
		err := root.ReplaceRegularFrom(ctx, "file", func(w io.Writer) error {
			if _, err := io.WriteString(w, "unverified bytes"); err != nil {
				return err
			}
			if cancelDuringWrite {
				cancel()
				return nil
			}
			return failure
		}, 0600, 0700, time.Now())
		cancel()
		want := failure
		if cancelDuringWrite {
			want = context.Canceled
		}
		if !errors.Is(err, want) {
			t.Fatalf("replace=%v, want %v", err, want)
		}
		raw, err := os.ReadFile(filepath.Join(path, "file"))
		if err != nil || string(raw) != "original" {
			t.Fatalf("destination=%q error=%v", raw, err)
		}
		entries, err := os.ReadDir(path)
		if err != nil || len(entries) != 1 {
			t.Fatalf("staging leak: %v %v", entries, err)
		}
	}
}

func TestStreamingReplacePreservesAncestorsUntilProducerSucceeds(t *testing.T) {
	for _, failure := range []string{"integrity", "write", "cancel"} {
		for _, ancestor := range []string{"regular", "symlink", "missing"} {
			t.Run(failure+"/"+ancestor, func(t *testing.T) {
				path := t.TempDir()
				outside := t.TempDir()
				parent := filepath.Join(path, "parent")
				if err := os.WriteFile(filepath.Join(outside, "sentinel"), []byte("outside"), 0600); err != nil {
					t.Fatal(err)
				}
				switch ancestor {
				case "regular":
					if err := os.WriteFile(parent, []byte("sentinel"), 0600); err != nil {
						t.Fatal(err)
					}
				case "symlink":
					if err := os.Symlink(outside, parent); err != nil {
						t.Skipf("symlink unavailable: %v", err)
					}
				}
				root, err := OpenRoot(path)
				if err != nil {
					t.Fatal(err)
				}
				defer root.Close()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				want := errors.New("canonical digest mismatch")
				if failure == "write" {
					want = io.ErrShortWrite
				}
				if failure == "cancel" {
					want = context.Canceled
				}
				err = root.ReplaceRegularFrom(ctx, "parent/child", func(w io.Writer) error {
					if _, err := io.WriteString(w, "unverified partial content"); err != nil {
						return err
					}
					if failure == "cancel" {
						cancel()
						return nil
					}
					if failure == "write" {
						_, err := io.Copy(streamTestWriter(func(p []byte) (int, error) { n, err := w.Write(p[:1]); return n, err }), strings.NewReader("more bytes"))
						return err
					}
					return want
				}, 0600, 0700, time.Now())
				if !errors.Is(err, want) {
					t.Fatalf("replace=%v want=%v", err, want)
				}
				switch ancestor {
				case "regular":
					if raw, err := os.ReadFile(parent); err != nil || string(raw) != "sentinel" {
						t.Fatalf("failed stream destroyed ancestor: %q %v", raw, err)
					}
				case "symlink":
					if target, err := os.Readlink(parent); err != nil || target != outside {
						t.Fatalf("failed stream replaced ancestor link: %q %v", target, err)
					}
				case "missing":
					if _, err := os.Lstat(parent); !os.IsNotExist(err) {
						t.Fatalf("failed stream created ancestors: %v", err)
					}
				}
				entries, err := os.ReadDir(path)
				wantEntries := 1
				if ancestor == "missing" {
					wantEntries = 0
				}
				if err != nil || len(entries) != wantEntries {
					t.Fatalf("staging leaked: %v %v", entries, err)
				}
				if entries, err := os.ReadDir(outside); err != nil || len(entries) != 1 || entries[0].Name() != "sentinel" {
					t.Fatalf("outside modified: %v %v", entries, err)
				}
			})
		}
	}
}

func TestStreamingReplaceRepairsAncestorsOnlyAfterVerifiedStaging(t *testing.T) {
	path := t.TempDir()
	parent := filepath.Join(path, "parent")
	if err := os.WriteFile(parent, []byte("sentinel"), 0600); err != nil {
		t.Fatal(err)
	}
	root, err := OpenRoot(path)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	err = root.ReplaceRegularFrom(context.Background(), "parent/child", func(w io.Writer) error {
		// The input can itself depend on the conflicting ancestor. Its bytes
		// must remain available throughout the producer's validation phase.
		raw, err := os.ReadFile(parent)
		if err != nil {
			return err
		}
		if string(raw) != "sentinel" {
			return errors.New("ancestor changed before verification")
		}
		_, err = w.Write(raw)
		return err
	}, 0600, 0700, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if raw, err := os.ReadFile(filepath.Join(parent, "child")); err != nil || string(raw) != "sentinel" {
		t.Fatalf("publication=%q error=%v", raw, err)
	}
	if entries, err := os.ReadDir(path); err != nil || len(entries) != 1 {
		t.Fatalf("successful staging leaked: %v %v", entries, err)
	}
}

func TestStreamingReplaceKeepsStagingParentPinnedDuringProduction(t *testing.T) {
	path, outside := t.TempDir(), t.TempDir()
	probe := filepath.Join(path, "symlink-probe")
	if err := os.Symlink(outside, probe); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if err := os.Remove(probe); err != nil {
		t.Fatal(err)
	}
	parent, moved := filepath.Join(path, "parent"), filepath.Join(path, "moved")
	if err := os.Mkdir(parent, 0700); err != nil {
		t.Fatal(err)
	}
	root, err := OpenRoot(path)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	err = root.ReplaceRegularFrom(context.Background(), "parent/child", func(w io.Writer) error {
		if err := os.Rename(parent, moved); err != nil {
			return err
		}
		if err := os.Symlink(outside, parent); err != nil {
			return err
		}
		_, err := io.WriteString(w, "verified")
		return err
	}, 0600, 0700, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if raw, err := os.ReadFile(filepath.Join(moved, "child")); err != nil || string(raw) != "verified" {
		t.Fatalf("pinned publication=%q error=%v", raw, err)
	}
	if entries, err := os.ReadDir(outside); err != nil || len(entries) != 0 {
		t.Fatalf("followed redirected parent: %v %v", entries, err)
	}
	if entries, err := os.ReadDir(moved); err != nil || len(entries) != 1 {
		t.Fatalf("pinned staging leaked: %v %v", entries, err)
	}
}

func TestCopyRegularStreamsLargeFileAndDetectsConcurrentMutation(t *testing.T) {
	path := t.TempDir()
	filePath := filepath.Join(path, "large")
	file, err := os.Create(filePath)
	if err != nil {
		t.Fatal(err)
	}
	const size = 32*1024*1024 + 1
	if err := file.Truncate(size); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	root, err := OpenRoot(path)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	hash := sha256.New()
	identity, info, err := root.CopyRegular(context.Background(), "large", hash)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != size || identity.SizeBytes != size || identity.ContentSHA256 != hex.EncodeToString(hash.Sum(nil)) {
		t.Fatalf("incorrect streamed identity: %+v", identity)
	}
	mutated := false
	_, _, err = root.CopyRegular(context.Background(), "large", streamTestWriter(func(p []byte) (int, error) {
		if !mutated {
			mutated = true
			f, err := os.OpenFile(filePath, os.O_WRONLY, 0)
			if err != nil {
				return 0, err
			}
			_, writeErr := f.WriteAt([]byte("changed"), 0)
			closeErr := f.Close()
			if err := errors.Join(writeErr, closeErr); err != nil {
				return 0, err
			}
			if err := os.Chtimes(filePath, info.ModTime(), info.ModTime()); err != nil {
				return 0, err
			}
		}
		return len(p), nil
	}))
	if !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("mutation with restored mtime: %v", err)
	}
}

func TestStreamingCopyCancellationAndNoFollow(t *testing.T) {
	path := t.TempDir()
	if err := os.WriteFile(filepath.Join(path, "file"), []byte(strings.Repeat("x", 256*1024)), 0600); err != nil {
		t.Fatal(err)
	}
	root, err := OpenRoot(path)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	copied := 0
	_, _, err = root.CopyRegular(ctx, "file", streamTestWriter(func(p []byte) (int, error) { copied += len(p); cancel(); return len(p), nil }))
	if !errors.Is(err, context.Canceled) || copied >= 256*1024 {
		t.Fatalf("copy bytes=%d err=%v", copied, err)
	}
	if err := os.Symlink(filepath.Join(path, "file"), filepath.Join(path, "link")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if _, _, err := root.CopyRegular(context.Background(), "link", io.Discard); err == nil {
		t.Fatal("followed symlink")
	}
}

func TestIterateDirCancellationStopsBeforeNextEntry(t *testing.T) {
	path := t.TempDir()
	for _, name := range []string{"a", "b", "c"} {
		if err := os.WriteFile(filepath.Join(path, name), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	root, err := OpenRoot(path)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	visited := 0
	err = root.IterateDir(ctx, ".", func(Entry) error { visited++; cancel(); return nil })
	if !errors.Is(err, context.Canceled) || visited != 1 {
		t.Fatalf("visited=%d error=%v", visited, err)
	}
}
