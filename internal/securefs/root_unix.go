//go:build darwin || linux

package securefs

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

func openRoot(path string) (*Root, error) {
	if path == "" {
		return nil, fmt.Errorf("secure root path is required")
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
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
	fd, err := unix.Openat(parentFD, leaf, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open secure regular file %q: %w", path, err)
	}
	file := os.NewFile(uintptr(fd), path)
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("stat secure regular file %q: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, fmt.Errorf("secure path %q must be a regular non-symlink file", path)
	}
	return file, nil
}

func readRootDir(root *Root, path string) ([]os.DirEntry, error) {
	if root == nil || root.fd < 0 {
		return nil, fmt.Errorf("secure root is closed")
	}
	fd, err := unix.Dup(root.fd)
	if err != nil {
		return nil, fmt.Errorf("duplicate secure root %q: %w", root.name, err)
	}
	trimmed := path
	if trimmed != "" && trimmed != "." {
		components, splitErr := splitRelativePath(trimmed)
		if splitErr != nil {
			unix.Close(fd)
			return nil, splitErr
		}
		for _, component := range components {
			childFD, openErr := unix.Openat(fd, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
			unix.Close(fd)
			if openErr != nil {
				return nil, fmt.Errorf("open secure directory component %q: %w", component, openErr)
			}
			fd = childFD
		}
	}
	directory := os.NewFile(uintptr(fd), path)
	defer directory.Close()
	entries, err := directory.ReadDir(-1)
	if err != nil {
		return nil, fmt.Errorf("read secure directory %q: %w", path, err)
	}
	return entries, nil
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
	if err := unix.Unlinkat(parentFD, leaf, 0); err != nil {
		return fmt.Errorf("remove secure entry %q: %w", path, err)
	}
	return nil
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
		childFD, err := unix.Openat(parentFD, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
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
