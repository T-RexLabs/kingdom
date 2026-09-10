//go:build darwin || linux

package securefs

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"sort"
	"time"

	"golang.org/x/sys/unix"
)

func openRoot(path string) (*Root, error) {
	if path == "" {
		return nil, fmt.Errorf("secure root path is required")
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open secure root %q: %w", path, err)
	}
	return &Root{fd: fd, name: path}, nil
}

func closeRoot(root *Root) error {
	if root == nil || root.fd < 0 {
		return nil
	}
	err := unix.Close(root.fd)
	root.fd = -1
	return err
}

func openRootRegular(root *Root, path string) (*os.File, error) {
	parentFD, leaf, err := openParent(root, path, false, 0)
	if err != nil {
		return nil, err
	}
	defer unix.Close(parentFD)
	var entry unix.Stat_t
	if err := unix.Fstatat(parentFD, leaf, &entry, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return nil, fmt.Errorf("inspect secure regular file %q: %w", path, err)
	}
	if uint32(entry.Mode)&unix.S_IFMT != unix.S_IFREG {
		return nil, fmt.Errorf("secure path %q must be a regular non-symlink file", path)
	}
	fd, err := unix.Openat(parentFD, leaf, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open secure regular file %q: %w", path, err)
	}
	var opened unix.Stat_t
	if err := unix.Fstat(fd, &opened); err != nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("stat secure regular file %q: %w", path, err)
	}
	if uint32(opened.Mode)&unix.S_IFMT != unix.S_IFREG {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("secure path %q must be a regular non-symlink file", path)
	}
	if fileIdentity(entry) != fileIdentity(opened) {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("%w: secure regular file %q changed while opening", ErrIdentityMismatch, path)
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("open secure regular file %q: invalid descriptor", path)
	}
	return file, nil
}

func readRootRegular(root *Root, path string, maxBytes int64) ([]byte, FileIdentity, error) {
	if maxBytes < 0 || maxBytes == math.MaxInt64 {
		return nil, FileIdentity{}, fmt.Errorf("maximum read size must be between 0 and %d", int64(math.MaxInt64-1))
	}
	file, err := openRootRegular(root, path)
	if err != nil {
		return nil, FileIdentity{}, err
	}
	defer file.Close()
	var opened unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &opened); err != nil {
		return nil, FileIdentity{}, fmt.Errorf("stat secure regular file %q: %w", path, err)
	}
	identity := fileIdentity(opened)
	if opened.Size > maxBytes {
		return nil, identity, fmt.Errorf("%w: secure regular file %q exceeds %d bytes", ErrLimitExceeded, path, maxBytes)
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil {
		return nil, identity, fmt.Errorf("read secure regular file %q: %w", path, err)
	}
	if int64(len(raw)) > maxBytes {
		return nil, identity, fmt.Errorf("%w: secure regular file %q exceeds %d bytes", ErrLimitExceeded, path, maxBytes)
	}
	return raw, identity, nil
}

func readRootDir(root *Root, path string) ([]os.DirEntry, error) {
	fd, err := openRootDir(root, path)
	if err != nil {
		return nil, err
	}
	directory := os.NewFile(uintptr(fd), path)
	defer directory.Close()
	entries, err := directory.ReadDir(-1)
	if err != nil {
		return nil, fmt.Errorf("read secure directory %q: %w", path, err)
	}
	return entries, nil
}

func readRootDirBounded(root *Root, path string, maxEntries int) ([]Entry, error) {
	if maxEntries < 0 || maxEntries == math.MaxInt {
		return nil, fmt.Errorf("maximum directory entries must be between 0 and %d", math.MaxInt-1)
	}
	fd, err := openRootDir(root, path)
	if err != nil {
		return nil, err
	}
	directory := os.NewFile(uintptr(fd), path)
	defer directory.Close()
	dirEntries, err := directory.ReadDir(maxEntries + 1)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("read secure directory %q: %w", path, err)
	}
	overflow := len(dirEntries) > maxEntries
	if overflow {
		dirEntries = dirEntries[:maxEntries]
	}
	entries := make([]Entry, 0, len(dirEntries))
	for _, dirEntry := range dirEntries {
		var stat unix.Stat_t
		if err := unix.Fstatat(fd, dirEntry.Name(), &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			return nil, fmt.Errorf("inspect secure directory entry %q: %w", dirEntry.Name(), err)
		}
		entries = append(entries, Entry{Name: dirEntry.Name(), Type: entryType(uint32(stat.Mode)), SizeBytes: stat.Size, Identity: fileIdentity(stat)})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	if overflow {
		return entries, fmt.Errorf("%w: secure directory %q contains more than %d entries", ErrLimitExceeded, path, maxEntries)
	}
	return entries, nil
}

func ensureRootDir(root *Root, path string, mode fs.FileMode) error {
	if root == nil || root.fd < 0 {
		return fmt.Errorf("secure root is closed")
	}
	components, err := splitRelativePath(path)
	if err != nil {
		return err
	}
	fd, err := unix.Dup(root.fd)
	if err != nil {
		return fmt.Errorf("duplicate secure root %q: %w", root.name, err)
	}
	defer func() { _ = unix.Close(fd) }()
	for _, component := range components {
		childFD, err := openOrCreateDirectoryAt(fd, component, mode)
		if err != nil {
			return fmt.Errorf("ensure secure directory component %q: %w", component, err)
		}
		unix.Close(fd)
		fd = childFD
	}
	return nil
}

func syncRootDir(root *Root, path string) error {
	fd, err := openRootDir(root, path)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	if err := unix.Fsync(fd); err != nil {
		return fmt.Errorf("sync secure directory %q: %w", path, err)
	}
	return nil
}

func replaceRootRegular(root *Root, path string, data []byte, mode, dirMode fs.FileMode, modifiedAt time.Time) error {
	parentFD, leaf, err := openParent(root, path, true, dirMode)
	if err != nil {
		return err
	}
	defer unix.Close(parentFD)
	return replaceRegularAt(parentFD, leaf, path, data, mode, modifiedAt)
}

func replaceRegularAt(parentFD int, leaf, path string, data []byte, mode fs.FileMode, modifiedAt time.Time) error {
	tempName, tempFile, err := createTempAt(parentFD)
	if err != nil {
		return fmt.Errorf("create secure temporary file for %q: %w", path, err)
	}
	committed := false
	defer func() {
		_ = tempFile.Close()
		if !committed {
			_ = unix.Unlinkat(parentFD, tempName, 0)
		}
	}()
	if _, err := tempFile.Write(data); err != nil {
		return fmt.Errorf("write secure temporary file for %q: %w", path, err)
	}
	if err := tempFile.Chmod(mode.Perm()); err != nil {
		return fmt.Errorf("set restored mode for %q: %w", path, err)
	}
	tv := []unix.Timeval{unix.NsecToTimeval(modifiedAt.UnixNano()), unix.NsecToTimeval(modifiedAt.UnixNano())}
	if err := unix.Futimes(int(tempFile.Fd()), tv); err != nil {
		return fmt.Errorf("set restored time for %q: %w", path, err)
	}
	if err := tempFile.Sync(); err != nil {
		return fmt.Errorf("sync restored file %q: %w", path, err)
	}
	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("close restored file %q: %w", path, err)
	}

	var destination unix.Stat_t
	statErr := unix.Fstatat(parentFD, leaf, &destination, unix.AT_SYMLINK_NOFOLLOW)
	if statErr == nil {
		switch destination.Mode & unix.S_IFMT {
		case unix.S_IFDIR:
			return fmt.Errorf("secure destination %q is a directory; refusing recursive replacement", path)
		case unix.S_IFREG:
		default:
			if err := unix.Unlinkat(parentFD, leaf, 0); err != nil {
				return fmt.Errorf("remove replaceable secure destination %q: %w", path, err)
			}
		}
	} else if !errors.Is(statErr, unix.ENOENT) {
		return fmt.Errorf("inspect secure destination %q: %w", path, statErr)
	}
	if err := unix.Renameat(parentFD, tempName, parentFD, leaf); err != nil {
		return fmt.Errorf("commit restored file %q: %w", path, err)
	}
	committed = true
	if err := unix.Fsync(parentFD); err != nil {
		return fmt.Errorf("sync secure parent after publishing %q: %w", path, err)
	}
	return nil
}

func removeRootEntry(root *Root, path string) error {
	parentFD, leaf, err := openParent(root, path, false, 0)
	if err != nil {
		if errors.Is(err, unix.ENOTDIR) || errors.Is(err, unix.ELOOP) {
			return fmt.Errorf("secure removal %q: %w", path, fs.ErrNotExist)
		}
		return err
	}
	defer unix.Close(parentFD)
	var info unix.Stat_t
	if err := unix.Fstatat(parentFD, leaf, &info, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return fmt.Errorf("inspect secure removal %q: %w", path, err)
	}
	if info.Mode&unix.S_IFMT == unix.S_IFDIR {
		return fmt.Errorf("secure removal %q is a directory; refusing recursive removal", path)
	}
	return removeEntryAt(parentFD, leaf, path)
}

func removeRootEntryIfIdentity(root *Root, path string, expectedType EntryType, identity FileIdentity) error {
	parentFD, leaf, err := openParent(root, path, false, 0)
	if err != nil {
		return err
	}
	defer unix.Close(parentFD)
	var info unix.Stat_t
	if err := unix.Fstatat(parentFD, leaf, &info, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return fmt.Errorf("inspect secure conditional removal %q: %w", path, err)
	}
	if entryType(uint32(info.Mode)) != expectedType || fileIdentity(info) != identity {
		return fmt.Errorf("%w: secure entry %q", ErrIdentityMismatch, path)
	}
	if expectedType == EntryTypeDirectory {
		return fmt.Errorf("secure conditional removal %q is a directory; refusing recursive removal", path)
	}
	return removeEntryAt(parentFD, leaf, path)
}

func removeEntryAt(parentFD int, leaf, path string) error {
	if err := unix.Unlinkat(parentFD, leaf, 0); err != nil {
		return fmt.Errorf("remove secure entry %q: %w", path, err)
	}
	if err := unix.Fsync(parentFD); err != nil {
		return fmt.Errorf("sync secure parent after removing %q: %w", path, err)
	}
	return nil
}

func removeRootEmptyDir(root *Root, path string) error {
	parentFD, leaf, err := openParent(root, path, false, 0)
	if err != nil {
		return err
	}
	defer unix.Close(parentFD)
	if err := unix.Unlinkat(parentFD, leaf, unix.AT_REMOVEDIR); err != nil {
		return fmt.Errorf("remove empty secure directory %q: %w", path, err)
	}
	if err := unix.Fsync(parentFD); err != nil {
		return fmt.Errorf("sync secure parent after removing directory %q: %w", path, err)
	}
	return nil
}

func openRootDir(root *Root, path string) (int, error) {
	if root == nil || root.fd < 0 {
		return -1, fmt.Errorf("secure root is closed")
	}
	fd, err := unix.Openat(root.fd, ".", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, fmt.Errorf("open secure root directory %q: %w", root.name, err)
	}
	trimmed := path
	if trimmed == "" || trimmed == "." {
		return fd, nil
	}
	components, err := splitRelativePath(trimmed)
	if err != nil {
		unix.Close(fd)
		return -1, err
	}
	for _, component := range components {
		childFD, openErr := unix.Openat(fd, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		unix.Close(fd)
		if openErr != nil {
			return -1, fmt.Errorf("open secure directory component %q: %w", component, openErr)
		}
		fd = childFD
	}
	return fd, nil
}

func openOrCreateDirectoryAt(parentFD int, component string, mode fs.FileMode) (int, error) {
	childFD, err := unix.Openat(parentFD, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err == nil {
		return childFD, nil
	}
	if !errors.Is(err, unix.ENOENT) {
		return -1, err
	}
	if err := unix.Mkdirat(parentFD, component, uint32(mode.Perm())); err != nil && !errors.Is(err, unix.EEXIST) {
		return -1, err
	}
	if err := unix.Fsync(parentFD); err != nil {
		return -1, err
	}
	return unix.Openat(parentFD, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
}

func entryType(mode uint32) EntryType {
	switch mode & unix.S_IFMT {
	case unix.S_IFREG:
		return EntryTypeRegular
	case unix.S_IFDIR:
		return EntryTypeDirectory
	case unix.S_IFLNK:
		return EntryTypeSymlink
	case unix.S_IFIFO:
		return EntryTypeFIFO
	case unix.S_IFSOCK:
		return EntryTypeSocket
	case unix.S_IFBLK, unix.S_IFCHR:
		return EntryTypeDevice
	default:
		return EntryTypeOther
	}
}

func fileIdentity(stat unix.Stat_t) FileIdentity {
	return FileIdentity{Device: uint64(stat.Dev), Inode: uint64(stat.Ino)}
}

func openParent(root *Root, path string, create bool, mode fs.FileMode) (int, string, error) {
	if root == nil || root.fd < 0 {
		return -1, "", fmt.Errorf("secure root is closed")
	}
	components, err := splitRelativePath(path)
	if err != nil {
		return -1, "", err
	}
	parentFD, err := unix.Dup(root.fd)
	if err != nil {
		return -1, "", fmt.Errorf("duplicate secure root %q: %w", root.name, err)
	}
	for _, component := range components[:len(components)-1] {
		childFD, openErr := openDirectoryAt(parentFD, component, create, mode)
		if openErr != nil {
			unix.Close(parentFD)
			return -1, "", fmt.Errorf("open secure directory component %q: %w", component, openErr)
		}
		unix.Close(parentFD)
		parentFD = childFD
	}
	return parentFD, components[len(components)-1], nil
}

func openDirectoryAt(parentFD int, component string, create bool, mode fs.FileMode) (int, error) {
	for range 16 {
		childFD, err := unix.Openat(parentFD, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err == nil || !create {
			return childFD, err
		}
		var info unix.Stat_t
		statErr := unix.Fstatat(parentFD, component, &info, unix.AT_SYMLINK_NOFOLLOW)
		switch {
		case errors.Is(statErr, unix.ENOENT):
		case statErr != nil:
			return -1, statErr
		case info.Mode&unix.S_IFMT == unix.S_IFDIR:
			continue
		default:
			if unlinkErr := unix.Unlinkat(parentFD, component, 0); unlinkErr != nil {
				if errors.Is(unlinkErr, unix.ENOENT) || errors.Is(unlinkErr, unix.EISDIR) || errors.Is(unlinkErr, unix.EPERM) {
					continue
				}
				return -1, unlinkErr
			}
		}
		if mkdirErr := unix.Mkdirat(parentFD, component, uint32(mode.Perm())); mkdirErr != nil && !errors.Is(mkdirErr, unix.EEXIST) {
			return -1, mkdirErr
		}
	}
	return -1, fmt.Errorf("secure directory component %q kept changing", component)
}

func createTempAt(parentFD int) (string, *os.File, error) {
	for range 32 {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return "", nil, err
		}
		name := ".kingdom-restore-" + hex.EncodeToString(random[:])
		fd, err := unix.Openat(parentFD, name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0o600)
		if errors.Is(err, unix.EEXIST) {
			continue
		}
		if err != nil {
			return "", nil, err
		}
		return name, os.NewFile(uintptr(fd), name), nil
	}
	return "", nil, fmt.Errorf("could not allocate a unique secure temporary file")
}
