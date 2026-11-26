package workspacearchive

import (
	"bytes"
	"context"
	"errors"
	"kingdom/internal/securefs"
	"os"
	"path/filepath"
	"testing"
)

func TestExportDeterministicRoundTripAndProtectedExclusions(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"src", "empty", ".kingdom", "node_modules", "nested/.git"} {
		if err := os.MkdirAll(filepath.Join(dir, name), 0755); err != nil {
			t.Fatal(err)
		}
	}
	for name, data := range map[string]string{"src/main.go": "package main", "run": "run", ".kingdom/secret": "private", "node_modules/generated": "generated", "nested/.git/config": "private"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(data), 0755); err != nil {
			t.Fatal(err)
		}
	}
	root, err := securefs.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	var first, second bytes.Buffer
	a, err := ExportZIP(context.Background(), root, &first, importLimits())
	if err != nil {
		t.Fatal(err)
	}
	b, err := ExportZIP(context.Background(), root, &second, importLimits())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Bytes(), second.Bytes()) || a.Digest != b.Digest || a.ArchiveBytes != int64(first.Len()) {
		t.Fatal("export is not deterministic")
	}
	staged, err := ImportZIP(context.Background(), t.TempDir(), bytes.NewReader(first.Bytes()), int64(first.Len()), importLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer staged.Close()
	if staged.Digest != a.Digest {
		t.Fatalf("export/import manifest mismatch: %s %s", a.Digest, staged.Digest)
	}
	for _, entry := range a.Manifest.Entries {
		if entry.Path == ".kingdom" || entry.Path == "node_modules" || entry.Path == "nested/.git" {
			t.Fatal("protected or generated entry exported")
		}
	}
}
func TestExportRejectsLinksAndLimits(t *testing.T) {
	for _, test := range []string{"symlink", "archive", "expanded", "file", "entries", "depth", "path"} {
		t.Run(test, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.Mkdir(filepath.Join(dir, "src"), 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "src/file"), []byte("content"), 0644); err != nil {
				t.Fatal(err)
			}
			limits := importLimits()
			switch test {
			case "symlink":
				if err := os.Symlink("src/file", filepath.Join(dir, "link")); err != nil {
					t.Fatal(err)
				}
			case "archive":
				limits.ArchiveBytes = 10
			case "expanded":
				limits.ExpandedBytes = 2
			case "file":
				limits.FileBytes = 2
			case "entries":
				limits.Entries = 1
			case "depth":
				limits.Depth = 1
			case "path":
				limits.PathBytes = 4
			}
			root, err := securefs.OpenRoot(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			var output bytes.Buffer
			if _, err := ExportZIP(context.Background(), root, &output, limits); err == nil {
				t.Fatal("invalid export accepted")
			}
			if int64(output.Len()) > limits.ArchiveBytes {
				t.Fatal("export exceeded output bound")
			}
		})
	}
}

type mutatingWriter struct {
	bytes.Buffer
	mutate func()
}

func (w *mutatingWriter) Write(data []byte) (int, error) {
	if w.mutate != nil {
		f := w.mutate
		w.mutate = nil
		f()
	}
	return w.Buffer.Write(data)
}
func TestExportDetectsTreeMutationAndCancellation(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "file"), []byte("original"), 0644)
	root, err := securefs.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	writer := &mutatingWriter{mutate: func() {
		if err := os.WriteFile(filepath.Join(dir, "added"), []byte("new"), 0644); err != nil {
			t.Fatal(err)
		}
	}}
	if _, err := ExportZIP(context.Background(), root, writer, importLimits()); !errors.Is(err, securefs.ErrIdentityMismatch) {
		t.Fatalf("tree mutation accepted: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ExportZIP(ctx, root, &bytes.Buffer{}, importLimits()); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
