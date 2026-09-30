//go:build windows

package securefs

import (
	"context"
	"golang.org/x/sys/windows"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

func openedFileIdentity(file *os.File) (FileIdentity, error) { return windowsFileIdentity(file) }
func openDirectoryFile(root *Root, path string) (*os.File, error) {
	return openRootDirectory(root, path)
}
func directoryEntry(directory *os.File, name string) (Entry, error) {
	return readRootEntry(&Root{rootFile: directory}, name)
}

func replaceRootRegularFrom(ctx context.Context, root *Root, path string, produce func(io.Writer) error, mode, dirMode fs.FileMode, modified time.Time) error {
	parts, err := windowsRelativeComponents(path)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if dir != "." {
		if err := ensureRootDirReplacingNonDirectories(root, dir, dirMode); err != nil {
			return err
		}
	}
	parent, err := openRootDirectory(root, dir)
	if err != nil {
		return err
	}
	defer parent.Close()
	temp, err := secureWindowsTempFromAt(windows.Handle(parent.Fd()), produce, mode)
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
	if err := temp.Sync(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Handle-relative rename replaces the link itself, never its target, and
	// refuses to replace a real directory with a file.
	if err := renameWindowsEntryWriteThrough(temp, parent, parts[len(parts)-1], true); err != nil {
		return err
	}
	published = true
	return nil
}
