//go:build darwin || linux

package securefs

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestRegularReplacementRetainsDurableSyncBoundaries(t *testing.T) {
	for _, failFileSync := range []bool{false, true} {
		path := t.TempDir()
		root, err := OpenRoot(path)
		if err != nil {
			t.Fatal(err)
		}
		defer root.Close()
		fileSyncs, directorySyncs := 0, 0
		failure := errors.New("sync failure")
		err = replaceRegularFromAtWithSync(context.Background(), root.fd, "nested/file", "nested/file", func(w io.Writer) error {
			_, err := io.WriteString(w, "verified")
			return err
		}, 0o644, 0o755, time.Now(), func(file *os.File) error {
			fileSyncs++
			if failFileSync {
				return failure
			}
			return file.Sync()
		}, func(fd int) error {
			directorySyncs++
			return unix.Fsync(fd)
		})
		if fileSyncs != 1 {
			t.Fatalf("file syncs=%d, want one", fileSyncs)
		}
		if failFileSync {
			if !errors.Is(err, failure) || directorySyncs != 0 {
				t.Fatalf("failed file sync err=%v directory syncs=%d", err, directorySyncs)
			}
			if _, err := os.Stat(filepath.Join(path, "nested")); !os.IsNotExist(err) {
				t.Fatalf("failed file sync created ancestors: %v", err)
			}
		} else if err != nil || directorySyncs != 3 {
			t.Fatalf("durable replacement err=%v directory syncs=%d, want ancestor, parent and staging sync", err, directorySyncs)
		}
	}
}
