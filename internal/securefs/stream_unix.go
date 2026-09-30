//go:build darwin || linux

package securefs

import (
	"golang.org/x/sys/unix"
	"os"
)

func openedFileIdentity(file *os.File) (FileIdentity, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		return FileIdentity{}, err
	}
	return fileIdentity(stat), nil
}

func openDirectoryFile(root *Root, path string) (*os.File, error) {
	fd, err := openRootDir(root, path)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), path), nil
}

func directoryEntry(directory *os.File, name string) (Entry, error) {
	var stat unix.Stat_t
	if err := unix.Fstatat(int(directory.Fd()), name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return Entry{}, err
	}
	return Entry{Name: name, Type: entryType(uint32(stat.Mode)), SizeBytes: stat.Size, Identity: fileIdentity(stat)}, nil
}
