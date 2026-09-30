//go:build windows

package securefs

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/windows"
)

func openedFileIdentity(file *os.File) (FileIdentity, error) { return windowsFileIdentity(file) }
func openDirectoryFile(root *Root, path string) (*os.File, error) {
	return openRootDirectory(root, path)
}
func directoryEntry(directory *os.File, name string) (Entry, error) {
	return readRootEntry(&Root{rootFile: directory}, name)
}

func replaceRootRegularFrom(ctx context.Context, root *Root, path string, produce func(io.Writer) error, mode, dirMode fs.FileMode, modified time.Time, durable bool) error {
	staging, relativePath, err := openWindowsStagingParent(root, path)
	if err != nil {
		return err
	}
	defer staging.Close()
	temp, err := secureWindowsTempFromAtWithDurability(windows.Handle(staging.Fd()), produce, mode, durable)
	if err != nil {
		return err
	}
	published := false
	defer func() {
		if !published {
			_ = deleteWindowsEntryByHandle(temp)
		}
		_ = temp.Close()
	}()
	stamp := windows.NsecToFiletime(modified.UnixNano())
	if err := windows.SetFileTime(windows.Handle(temp.Fd()), nil, &stamp, &stamp); err != nil {
		return err
	}
	if durable {
		if err := temp.Sync(); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// The producer, metadata writes and any required flush succeeded. Only now may
	// conflicting ancestors be claimed and repaired under the pinned directory.
	stagingRoot := &Root{rootFile: staging}
	dir := filepath.Dir(relativePath)
	if dir != "." {
		if err := ensureRootDirWithPolicy(stagingRoot, dir, dirMode, true, durable); err != nil {
			return err
		}
	}
	parent, err := openRootDirectory(stagingRoot, dir)
	if err != nil {
		return err
	}
	defer parent.Close()
	// Handle-relative rename replaces the link itself, never its target, and
	// refuses to replace a real directory with a file.
	if err := renameWindowsEntryAt(temp, parent, filepath.Base(relativePath), true); err != nil {
		return err
	}
	published = true
	return nil
}

func openWindowsStagingParent(root *Root, path string) (*os.File, string, error) {
	parts, err := windowsRelativeComponents(path)
	if err != nil {
		return nil, "", err
	}
	directory, err := openRootDirectory(root, ".")
	if err != nil {
		return nil, "", err
	}
	for index, part := range parts[:len(parts)-1] {
		pinned := &Root{rootFile: directory}
		entry, err := readRootEntry(pinned, part)
		if errors.Is(err, os.ErrNotExist) || (err == nil && entry.Type != EntryTypeDirectory) {
			return directory, strings.Join(parts[index:], string(filepath.Separator)), nil
		}
		if err != nil {
			_ = directory.Close()
			return nil, "", err
		}
		child, err := openRootDirectory(pinned, part)
		_ = directory.Close()
		if err != nil {
			return nil, "", err
		}
		directory = child
	}
	return directory, parts[len(parts)-1], nil
}
