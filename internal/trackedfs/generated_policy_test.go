package trackedfs

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"kingdom/internal/securefs"
)

func TestGeneratedDirectoriesAreExcludedWithoutReservingFileNames(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	generated := []string{"node_modules", ".venv", "__pycache__", ".pytest_cache", ".ruff_cache", ".mypy_cache", ".tox", ".nox", ".terraform"}
	want := map[string]bool{}
	write := func(path string) {
		t.Helper()
		absolute := filepath.Join(root, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(absolute), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(absolute, []byte(path), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range generated {
		write(name + "/excluded.txt")
		write("repos/service/tools/" + name + "/deep/excluded.txt")
		file := "fixtures/" + name
		write(file)
		want[file] = true
		if !ShouldSkipDirectory("repos/service/" + name) {
			t.Errorf("generated directory %q should be skipped", name)
		}
		if IsProtectedRelativePath("repos/service/" + name + "/fixture.txt") {
			t.Errorf("generated directory %q must not become hard-protected", name)
		}
	}
	for _, path := range []string{".gitignore", "config/local.yaml", ".terraform.lock.hcl", "infra/main.tf", "dist/report.json", "src/venv/api.py", "vendor/library.go", "fixtures/.venv-example/file.txt"} {
		write(path)
		want[path] = true
	}
	// Git ignore rules are project content, not workspace eligibility rules.
	if err := os.WriteFile(filepath.Join(root, ".gitignore"), []byte("*\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	files, err := collectTrackedFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, file := range files {
		got[file.RelativePath] = true
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("eligible files = %v, want %v", got, want)
	}
}

func TestGeneratedDirectoryNamesDoNotHideSymlinksFromEntryWalker(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	outside := t.TempDir()
	for _, name := range []string{".venv", "node_modules"} {
		if err := os.Symlink(outside, filepath.Join(root, name)); err != nil {
			if runtime.GOOS == "windows" {
				t.Skipf("symlink creation unavailable: %v", err)
			}
			t.Fatal(err)
		}
	}
	pinned, err := securefs.OpenRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	defer pinned.Close()
	got := map[string]securefs.EntryType{}
	if err := VisitTrackedEntriesRoot(context.Background(), pinned, func(entry TrackedEntry) error {
		got[entry.RelativePath] = entry.Type
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := map[string]securefs.EntryType{".venv": securefs.EntryTypeSymlink, "node_modules": securefs.EntryTypeSymlink}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("cleanup entries = %v, want %v", got, want)
	}
	files, err := collectTrackedFilesRoot(pinned)
	if err != nil || len(files) != 0 {
		t.Fatalf("symlinks must not be captured: files=%v err=%v", files, err)
	}
}
