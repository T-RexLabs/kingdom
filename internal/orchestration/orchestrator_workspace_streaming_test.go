package orchestration

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

	"kingdom/internal/securefs"
)

func TestPrepareExecutionWorkspaceStreamsLargeDeepSeedWithoutFollowingDestinationLinks(t *testing.T) {
	for _, kind := range []executionWorkspaceKind{workspaceCanonical, workspacePrivateSource} {
		t.Run(string(kind), func(t *testing.T) {
			seed, destination, outside := t.TempDir(), t.TempDir(), t.TempDir()
			deep := strings.Repeat("d/", 65) + "large.bin"
			if err := os.MkdirAll(filepath.Dir(filepath.Join(seed, deep)), 0755); err != nil {
				t.Fatal(err)
			}
			f, err := os.Create(filepath.Join(seed, deep))
			if err != nil {
				t.Fatal(err)
			}
			const size = 32*1024*1024 + 1
			if err := f.Truncate(size); err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(outside, "keep"), []byte("untouched"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, filepath.Join(destination, "d")); err != nil {
				t.Skipf("symlink unavailable: %v", err)
			}
			if err := os.Symlink(outside, filepath.Join(seed, "skip")); err != nil {
				t.Fatal(err)
			}
			loop := &orchestrationLoop{}
			if err := loop.prepareExecutionWorkspaceContext(context.Background(), destination, executionWorkspaceContext{SeedRoot: seed, WorkspaceKind: kind}); err != nil {
				t.Fatal(err)
			}
			root, err := securefs.OpenRoot(destination)
			if err != nil {
				t.Fatal(err)
			}
			defer root.Close()
			identity, _, err := root.CopyRegular(context.Background(), deep, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			file, err := os.Open(filepath.Join(seed, deep))
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			hash := sha256.New()
			if _, err := io.Copy(hash, file); err != nil {
				t.Fatal(err)
			}
			if identity.SizeBytes != size || identity.ContentSHA256 != hex.EncodeToString(hash.Sum(nil)) {
				t.Fatalf("seed copy identity=%+v", identity)
			}
			if _, err := os.Lstat(filepath.Join(destination, "skip")); !os.IsNotExist(err) {
				t.Fatalf("source link was copied: %v", err)
			}
			if entries, err := os.ReadDir(outside); err != nil || len(entries) != 1 || entries[0].Name() != "keep" {
				t.Fatalf("outside modified: %v %v", entries, err)
			}
		})
	}
}

func TestPrepareExecutionWorkspaceHonorsCancelledSeedCopy(t *testing.T) {
	seed, destination := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(seed, "file"), []byte("seed"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	loop := &orchestrationLoop{}
	if err := loop.prepareExecutionWorkspaceContext(ctx, destination, executionWorkspaceContext{SeedRoot: seed}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled preparation=%v", err)
	}
	if entries, err := os.ReadDir(destination); err != nil || len(entries) != 0 {
		t.Fatalf("cancelled preparation wrote entries: %v %v", entries, err)
	}
}

func TestWorkspaceArtifactVisitorCancelsWithoutCollectingTree(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"a", "b", "c"} {
		if err := os.WriteFile(filepath.Join(root, name), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	visited := 0
	err := visitWorkspaceArtifacts(ctx, root, func(executionArtifact) error { visited++; cancel(); return nil })
	if !errors.Is(err, context.Canceled) || visited != 1 {
		t.Fatalf("visited=%d error=%v", visited, err)
	}
}
