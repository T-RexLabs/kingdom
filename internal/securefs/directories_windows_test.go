//go:build windows

package securefs

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSyncDirectoryUsesPinnedRootAfterWorkingDirectoryChange(t *testing.T) {
	originalWorkingDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(originalWorkingDirectory); err != nil {
			t.Errorf("restore working directory: %v", err)
		}
	})

	firstRoot := t.TempDir()
	secondRoot := t.TempDir()
	for _, root := range []string{firstRoot, secondRoot} {
		if err := os.Mkdir(filepath.Join(root, "target"), 0o700); err != nil {
			t.Fatalf("create target directory under %q: %v", root, err)
		}
	}
	if err := os.Chdir(firstRoot); err != nil {
		t.Fatalf("change into first root: %v", err)
	}
	root, err := OpenDirChain(".", Directory{Name: "target"})
	if err != nil {
		t.Fatalf("open relative secure directory chain: %v", err)
	}
	defer root.Close()
	if !filepath.IsAbs(root.Name()) {
		t.Fatalf("secure directory name is not absolute: %q", root.Name())
	}
	if err := os.Chdir(secondRoot); err != nil {
		t.Fatalf("change into second root: %v", err)
	}
	if err := SyncDirectory(root); err != nil {
		t.Fatalf("sync originally pinned directory after working-directory change: %v", err)
	}
}
