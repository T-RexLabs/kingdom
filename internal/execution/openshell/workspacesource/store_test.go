package workspacesource

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/securefs"
	"kingdom/internal/workspacearchive"
	"kingdom/internal/workspacetransfer"
)

type resolver string

func (r resolver) Open(context.Context, providerrequest.Preparation) (*securefs.Root, error) {
	return securefs.OpenRoot(string(r))
}
func preparation() providerrequest.Preparation {
	return providerrequest.Preparation{ResourceID: "attempt", Owner: providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: "invocation"}, Lease: providerrequest.Lease{Owner: "worker", Version: 1}}
}
func fixture(t *testing.T) (*Store, string, string) {
	t.Helper()
	input := t.TempDir()
	private := t.TempDir()
	if e := os.Chmod(private, 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(input, "file.txt"), []byte("original"), 0600); e != nil {
		t.Fatal(e)
	}
	s, e := New(private, resolver(input), workspacetransfer.DefaultLimits())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	return s, input, private
}
func TestRetainedSourceSurvivesWorkspaceChangesAndRestart(t *testing.T) {
	s, input, private := fixture(t)
	ctx := context.Background()
	ref, e := s.Capture(ctx, preparation())
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(input, "file.txt"), []byte("changed"), 0600); e != nil {
		t.Fatal(e)
	}
	restarted, e := New(private, resolver(input), workspacetransfer.DefaultLimits())
	if e != nil {
		t.Fatal(e)
	}
	defer restarted.Close()
	a, e := restarted.Open(ctx, ref)
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	staged, e := workspacearchive.ImportZIP(ctx, t.TempDir(), a, a.Size(), workspacetransfer.DefaultLimits())
	if e != nil {
		t.Fatal(e)
	}
	defer staged.Close()
	data, e := os.ReadFile(filepath.Join(staged.Path, "file.txt"))
	if e != nil || string(data) != "original" {
		t.Fatalf("changed input adopted: %q %v", data, e)
	}
}
func TestSourceRejectsChangedArchiveAndReferenceSubstitution(t *testing.T) {
	s, _, private := fixture(t)
	ctx := context.Background()
	ref, e := s.Capture(ctx, preparation())
	if e != nil {
		t.Fatal(e)
	}
	bad := ref
	bad.ID = "../outside"
	if a, e := s.Open(ctx, bad); e == nil {
		a.Close()
		t.Fatal("path accepted")
	}
	bad = ref
	bad.Bytes++
	if a, e := s.Open(ctx, bad); e == nil {
		a.Close()
		t.Fatal("size accepted")
	}
	filename := filepath.Join(private, ref.ID+".zip")
	if e = os.Chmod(filename, 0600); e != nil {
		t.Fatal(e)
	}
	file, e := os.OpenFile(filename, os.O_WRONLY, 0)
	if e != nil {
		t.Fatal(e)
	}
	_, e = file.WriteAt([]byte("bad"), 0)
	file.Close()
	if e != nil {
		t.Fatal(e)
	}
	os.Chmod(filename, 0400)
	if a, e := s.Open(ctx, ref); e == nil {
		a.Close()
		t.Fatal("changed content accepted")
	}
}
func TestSourceRejectsWorkspaceAndArchiveSymlinks(t *testing.T) {
	s, input, private := fixture(t)
	ctx := context.Background()
	ref, e := s.Capture(ctx, preparation())
	if e != nil {
		t.Fatal(e)
	}
	name := filepath.Join(private, ref.ID+".zip")
	target := filepath.Join(private, "retained.zip")
	if e = os.Rename(name, target); e != nil {
		t.Fatal(e)
	}
	if e = os.Symlink(target, name); e != nil {
		t.Fatal(e)
	}
	if a, e := s.Open(ctx, ref); e == nil {
		a.Close()
		t.Fatal("archive symlink accepted")
	}
	if e = os.Symlink(target, filepath.Join(input, "escape")); e != nil {
		t.Fatal(e)
	}
	before, _ := os.ReadDir(private)
	if _, e = s.Capture(ctx, preparation()); e == nil {
		t.Fatal("input symlink accepted")
	}
	after, _ := os.ReadDir(private)
	if len(before) != len(after) {
		t.Fatal("failed capture retained archive")
	}
}
func TestSourceHonorsCancellationAndIndependentDescriptors(t *testing.T) {
	s, _, _ := fixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := s.Capture(ctx, preparation()); e == nil {
		t.Fatal("cancelled capture accepted")
	}
	ref, e := s.Capture(context.Background(), preparation())
	if e != nil {
		t.Fatal(e)
	}
	a, e := s.Open(context.Background(), ref)
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	b, e := s.Open(context.Background(), ref)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	first := make([]byte, 4)
	second := make([]byte, 4)
	if _, e = a.ReadAt(first, 0); e != nil && e != io.EOF {
		t.Fatal(e)
	}
	if _, e = b.ReadAt(second, 0); e != nil && e != io.EOF {
		t.Fatal(e)
	}
	if string(first) != string(second) {
		t.Fatal("shared cursor")
	}
}
