package securefs

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"kingdom/internal/filemode"
)

func TestMaterializeRegularFromVerifiesBeforeMutation(t *testing.T) {
	for _, shape := range []string{"file", "ancestor", "symlink", "missing"} {
		for _, failure := range []string{"integrity", "cancellation"} {
			t.Run(shape+"/"+failure, func(t *testing.T) {
				path, outside := t.TempDir(), t.TempDir()
				destination := "parent/child"
				if shape == "file" {
					destination = "parent"
				}
				if shape == "file" || shape == "ancestor" {
					if err := os.WriteFile(filepath.Join(path, "parent"), []byte("original"), 0o644); err != nil {
						t.Fatal(err)
					}
				} else if shape == "symlink" {
					if err := os.Symlink(outside, filepath.Join(path, "parent")); err != nil {
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
				want := errors.New("unverified content")
				if failure == "cancellation" {
					want = context.Canceled
				}
				err = root.MaterializeRegularFrom(ctx, destination, func(w io.Writer) error {
					if _, err := io.WriteString(w, "partial untrusted bytes"); err != nil {
						return err
					}
					if failure == "cancellation" {
						cancel()
						return nil
					}
					return want
				}, 0o755, 0o755, time.Now())
				if !errors.Is(err, want) {
					t.Fatalf("materialize error=%v, want %v", err, want)
				}
				expectedEntries := 1
				if shape == "missing" {
					expectedEntries = 0
				}
				if entries, err := os.ReadDir(path); err != nil || len(entries) != expectedEntries {
					t.Fatalf("failed staging changed workspace: %v %v", entries, err)
				}
				if shape == "file" || shape == "ancestor" {
					if raw, err := os.ReadFile(filepath.Join(path, "parent")); err != nil || string(raw) != "original" {
						t.Fatalf("original file changed: %q %v", raw, err)
					}
				} else if shape == "symlink" {
					if link, err := os.Readlink(filepath.Join(path, "parent")); err != nil || link != outside {
						t.Fatalf("ancestor link changed: %q %v", link, err)
					}
				}
				if entries, err := os.ReadDir(outside); err != nil || len(entries) != 0 {
					t.Fatalf("outside changed: %v %v", entries, err)
				}
			})
		}
	}
}

func TestMaterializeRegularFromKeepsPinnedParentAndMetadata(t *testing.T) {
	path, outside := t.TempDir(), t.TempDir()
	parent, moved := filepath.Join(path, "parent"), filepath.Join(path, "moved")
	if err := os.Mkdir(parent, 0o755); err != nil {
		t.Fatal(err)
	}
	root, err := OpenRoot(path)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	modified := time.Unix(1700000000, 0)
	if err := root.MaterializeRegularFrom(context.Background(), "parent/file", func(w io.Writer) error {
		if err := os.Rename(parent, moved); err != nil {
			return err
		}
		if err := os.Symlink(outside, parent); err != nil {
			t.Skipf("symlink unavailable: %v", err)
		}
		_, err := io.WriteString(w, "verified")
		return err
	}, 0o751, 0o755, modified); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(moved, "file")
	if raw, err := os.ReadFile(file); err != nil || string(raw) != "verified" {
		t.Fatalf("pinned file=%q error=%v", raw, err)
	}
	info, err := os.Stat(file)
	if err != nil || !filemode.Equal(int64(info.Mode().Perm()), 0o751) || !info.ModTime().Equal(modified) {
		t.Fatalf("materialized metadata=%v error=%v", info, err)
	}
	if entries, err := os.ReadDir(outside); err != nil || len(entries) != 0 {
		t.Fatalf("publication escaped pinned parent: %v %v", entries, err)
	}
}

func TestMaterializeRegularFromPreservesDirectoryConflicts(t *testing.T) {
	path := t.TempDir()
	if err := os.Mkdir(filepath.Join(path, "file"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "file", "keep"), []byte("keep"), 0o644); err != nil {
		t.Fatal(err)
	}
	root, err := OpenRoot(path)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	err = root.MaterializeRegularFrom(context.Background(), "file", func(w io.Writer) error { _, err := io.WriteString(w, "new"); return err }, 0o644, 0o755, time.Now())
	if err == nil {
		t.Fatal("expected real-directory conflict")
	}
	if raw, err := os.ReadFile(filepath.Join(path, "file", "keep")); err != nil || string(raw) != "keep" {
		t.Fatalf("directory conflict was changed: %q %v", raw, err)
	}
}
