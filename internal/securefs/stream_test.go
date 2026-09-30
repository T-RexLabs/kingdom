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
