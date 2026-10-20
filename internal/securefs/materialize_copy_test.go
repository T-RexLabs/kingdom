package securefs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kingdom/internal/filemode"
)

func TestMaterializeRegularCopyPreservesMetadataAndIndependentContent(t *testing.T) {
	sourcePath, destinationPath := t.TempDir(), t.TempDir()
	filePath := filepath.Join(sourcePath, "file")
	if err := os.WriteFile(filePath, []byte("source content"), 0o751); err != nil {
		t.Fatal(err)
	}
	modified := time.Unix(1700000000, 0)
	if err := os.Chtimes(filePath, modified, modified); err != nil {
		t.Fatal(err)
	}
	source, err := OpenRoot(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	destination, err := OpenRoot(destinationPath)
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	entry, err := source.ReadEntry("file")
	if err != nil {
		t.Fatal(err)
	}
	if err := destination.MaterializeRegularCopy(context.Background(), "nested/copy", source, "file", entry.Identity, 0o755); err != nil {
		t.Fatal(err)
	}
	copyPath := filepath.Join(destinationPath, "nested", "copy")
	info, err := os.Stat(copyPath)
	if err != nil || !filemode.Equal(int64(info.Mode().Perm()), 0o751) || !info.ModTime().Equal(modified) {
		t.Fatalf("copy metadata=%v err=%v", info, err)
	}
	if err := os.WriteFile(copyPath, []byte("private mutation"), 0o600); err != nil {
		t.Fatal(err)
	}
	if raw, err := os.ReadFile(filePath); err != nil || string(raw) != "source content" {
		t.Fatalf("copy write changed source: %q %v", raw, err)
	}
	if err := os.WriteFile(filePath, []byte("source mutation"), 0o600); err != nil {
		t.Fatal(err)
	}
	if raw, err := os.ReadFile(copyPath); err != nil || string(raw) != "private mutation" {
		t.Fatalf("source write changed copy: %q %v", raw, err)
	}
}

func TestMaterializeRegularCopyRejectsChangedSourceBeforePublishing(t *testing.T) {
	sourcePath, destinationPath := t.TempDir(), t.TempDir()
	filePath := filepath.Join(sourcePath, "file")
	if err := os.WriteFile(filePath, []byte("original"), 0o644); err != nil {
		t.Fatal(err)
	}
	source, err := OpenRoot(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	entry, err := source.ReadEntry("file")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filePath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filePath, []byte("replaced"), 0o644); err != nil {
		t.Fatal(err)
	}
	destination, err := OpenRoot(destinationPath)
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	err = destination.MaterializeRegularCopy(context.Background(), "nested/file", source, "file", entry.Identity, 0o755)
	if !errors.Is(err, ErrIdentityMismatch) {
		t.Fatalf("replaced source accepted: %v", err)
	}
	if entries, err := os.ReadDir(destinationPath); err != nil || len(entries) != 0 {
		t.Fatalf("replaced source left destination changes: %v %v", entries, err)
	}
}

func TestRegularCopyDoesNotFollowSourceLinks(t *testing.T) {
	sourcePath, outsidePath, destinationPath := t.TempDir(), t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(outsidePath, "file"), []byte("outside"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outsidePath, filepath.Join(sourcePath, "parent")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if err := os.Symlink(filepath.Join(outsidePath, "file"), filepath.Join(sourcePath, "link")); err != nil {
		t.Fatal(err)
	}
	source, err := OpenRoot(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	destination, err := OpenRoot(destinationPath)
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	outside, err := OpenRoot(outsidePath)
	if err != nil {
		t.Fatal(err)
	}
	defer outside.Close()
	target, err := outside.ReadEntry("file")
	if err != nil {
		t.Fatal(err)
	}
	// The target's own identity matches, so only refusing the link can fail.
	for _, path := range []string{"parent/file", "link"} {
		for name, copyFile := range map[string]func(context.Context, string, *Root, string, FileIdentity, os.FileMode) error{
			"materialize": destination.MaterializeRegularCopy,
			"replace":     destination.ReplaceRegularCopy,
		} {
			err := copyFile(context.Background(), "copy", source, path, target.Identity, 0o755)
			if err == nil || errors.Is(err, ErrIdentityMismatch) {
				t.Fatalf("%s followed source link %q: %v", name, path, err)
			}
		}
	}
	if entries, err := os.ReadDir(destinationPath); err != nil || len(entries) != 0 {
		t.Fatalf("linked source left destination changes: %v %v", entries, err)
	}
}

// Context checks provide a deterministic point after the first staged chunk,
// without depending on goroutine timing to interrupt an active filesystem copy.
type materializeCopyContext struct {
	context.Context
	check func()
}

func (c materializeCopyContext) Err() error {
	c.check()
	return c.Context.Err()
}

func TestMaterializeRegularCopyRejectsInterruptedSourceBeforeMutation(t *testing.T) {
	for _, shape := range []string{"file", "ancestor", "symlink", "missing"} {
		for _, failure := range []string{"mutation", "cancellation"} {
			t.Run(shape+"/"+failure, func(t *testing.T) {
				sourcePath, destinationPath, outsidePath := t.TempDir(), t.TempDir(), t.TempDir()
				sourceFile := filepath.Join(sourcePath, "file")
				if err := os.WriteFile(sourceFile, []byte(strings.Repeat("x", 512*1024)), 0o644); err != nil {
					t.Fatal(err)
				}
				source, err := OpenRoot(sourcePath)
				if err != nil {
					t.Fatal(err)
				}
				defer source.Close()
				entry, err := source.ReadEntry("file")
				if err != nil {
					t.Fatal(err)
				}
				sourceInfo, err := os.Stat(sourceFile)
				if err != nil {
					t.Fatal(err)
				}
				path := "parent/child"
				if shape == "file" {
					path = "parent"
				}
				if shape == "file" || shape == "ancestor" {
					if err := os.WriteFile(filepath.Join(destinationPath, "parent"), []byte("original"), 0o644); err != nil {
						t.Fatal(err)
					}
				} else if shape == "symlink" {
					if err := os.Symlink(outsidePath, filepath.Join(destinationPath, "parent")); err != nil {
						t.Skipf("symlink unavailable: %v", err)
					}
				}
				destination, err := OpenRoot(destinationPath)
				if err != nil {
					t.Fatal(err)
				}
				defer destination.Close()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				interrupted := false
				checked := materializeCopyContext{Context: ctx, check: func() {
					if interrupted {
						return
					}
					entries, err := os.ReadDir(destinationPath)
					if err != nil {
						t.Fatal(err)
					}
					for _, item := range entries {
						if item.Name() == "parent" {
							continue
						}
						info, err := item.Info()
						if err != nil {
							t.Fatal(err)
						}
						if !info.Mode().IsRegular() || info.Size() == 0 {
							continue
						}
						interrupted = true
						if failure == "cancellation" {
							cancel()
						} else {
							file, err := os.OpenFile(sourceFile, os.O_WRONLY, 0)
							if err != nil {
								t.Fatal(err)
							}
							_, writeErr := file.WriteAt([]byte("changed"), 0)
							if err := errors.Join(writeErr, file.Close()); err != nil {
								t.Fatal(err)
							}
							// Content can change without a size or mtime change. The
							// opened identity must still detect that unstable version.
							modified := sourceInfo.ModTime()
							if err := os.Chtimes(sourceFile, modified, modified); err != nil {
								t.Fatal(err)
							}
						}
						return
					}
				}}
				err = destination.MaterializeRegularCopy(checked, path, source, "file", entry.Identity, 0o755)
				want := ErrIdentityMismatch
				if failure == "cancellation" {
					want = context.Canceled
				}
				if !interrupted || !errors.Is(err, want) {
					t.Fatalf("interrupted=%v err=%v, want %v", interrupted, err, want)
				}
				expectedEntries := 1
				if shape == "missing" {
					expectedEntries = 0
				}
				if entries, err := os.ReadDir(destinationPath); err != nil || len(entries) != expectedEntries {
					t.Fatalf("interruption changed destination entries: %v %v", entries, err)
				}
				if shape == "file" || shape == "ancestor" {
					if raw, err := os.ReadFile(filepath.Join(destinationPath, "parent")); err != nil || string(raw) != "original" {
						t.Fatalf("interruption changed original file: %q %v", raw, err)
					}
				} else if shape == "symlink" {
					if link, err := os.Readlink(filepath.Join(destinationPath, "parent")); err != nil || link != outsidePath {
						t.Fatalf("interruption changed ancestor link: %q %v", link, err)
					}
				}
				if entries, err := os.ReadDir(outsidePath); err != nil || len(entries) != 0 {
					t.Fatalf("interruption changed outside target: %v %v", entries, err)
				}
			})
		}
	}
}

func TestRegularCopyRefusesADigestItCannotVerify(t *testing.T) {
	sourcePath, destinationPath := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(sourcePath, "file"), []byte("source content"), 0o644); err != nil {
		t.Fatal(err)
	}
	source, err := OpenRoot(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	destination, err := OpenRoot(destinationPath)
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	entry, err := source.ReadEntry("file")
	if err != nil {
		t.Fatal(err)
	}
	entry.Identity.ContentSHA256 = strings.Repeat("0", 64)
	if err := destination.ReplaceRegularCopy(context.Background(), "copy", source, "file", entry.Identity, 0o755); err == nil {
		t.Fatal("a copy accepted a content digest it does not check")
	}
	if entries, err := os.ReadDir(destinationPath); err != nil || len(entries) != 0 {
		t.Fatalf("refused copy left destination changes: %v %v", entries, err)
	}
}
