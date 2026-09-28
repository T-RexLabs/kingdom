//go:build darwin || linux

package securefs

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
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
	raw, identity, err := readBoundedRegular(file, maxBytes, fileIdentity(opened), func() (FileIdentity, error) {
		var postRead unix.Stat_t
		if err := unix.Fstat(int(file.Fd()), &postRead); err != nil {
			return FileIdentity{}, fmt.Errorf("stat secure regular file %q after read: %w", path, err)
		}
		return fileIdentity(postRead), nil
	})
	if err != nil {
		return nil, identity, err
	}
	digest := sha256.Sum256(raw)
	identity.ContentSHA256 = hex.EncodeToString(digest[:])
	return raw, identity, nil
}

func readBoundedRegular(reader io.Reader, maxBytes int64, before FileIdentity, postReadIdentity func() (FileIdentity, error)) ([]byte, FileIdentity, error) {
	raw, readErr := io.ReadAll(io.LimitReader(reader, maxBytes+1))
	after, statErr := postReadIdentity()
	if statErr != nil {
		return nil, FileIdentity{}, statErr
	}
	if after != before {
		return nil, after, fmt.Errorf("%w: secure regular file changed while reading", ErrIdentityMismatch)
	}
	if readErr != nil {
		return nil, after, fmt.Errorf("read secure regular file: %w", readErr)
	}
	if int64(len(raw)) > maxBytes {
		return nil, after, fmt.Errorf("%w: secure regular file exceeds %d bytes", ErrLimitExceeded, maxBytes)
	}
	return raw, after, nil
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

func readRootEntry(root *Root, path string) (Entry, error) {
	parentFD, leaf, err := openParent(root, path, false, 0)
	if err != nil {
		return Entry{}, err
	}
	defer unix.Close(parentFD)
	var info unix.Stat_t
	if err := unix.Fstatat(parentFD, leaf, &info, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return Entry{}, fmt.Errorf("inspect secure entry %q: %w", path, err)
	}
	return Entry{Name: leaf, Type: entryType(uint32(info.Mode)), SizeBytes: info.Size, Identity: fileIdentity(info)}, nil
}

func rootTreeCursorUnavailable(err error) bool {
	return errors.Is(err, unix.ENOENT) || errors.Is(err, unix.ENOTDIR) || errors.Is(err, unix.ELOOP)
}

func rootDirectoryNotEmpty(err error) bool {
	return errors.Is(err, unix.ENOTEMPTY) || errors.Is(err, unix.EEXIST)
}

func createOSRootDirectory(root *os.Root, name string, mode fs.FileMode) (bool, error) {
	if err := root.Mkdir(name, mode.Perm()); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func syncOSRootDirectory(root *os.Root) error {
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	return errors.Join(syncErr, closeErr)
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
	relativePath := ""
	for _, component := range components {
		childFD, err := openOrCreateDirectoryAt(fd, component, mode)
		if err != nil {
			return fmt.Errorf("ensure secure directory component %q: %w", component, err)
		}
		if relativePath == "" {
			relativePath = component
		} else {
			relativePath += "/" + component
		}
		if err := root.syncDirectoryEntry(fd, childFD, relativePath, unix.Fsync); err != nil {
			_ = unix.Close(childFD)
			return fmt.Errorf("sync secure directory component %q: %w", component, err)
		}
		unix.Close(fd)
		fd = childFD
	}
	return nil
}

func (r *Root) syncDirectoryEntry(parentFD, childFD int, relativePath string, syncParent func(int) error) error {
	if r == nil || parentFD < 0 || childFD < 0 || relativePath == "" || syncParent == nil {
		return fmt.Errorf("secure directory entry identity and sync operation are required")
	}
	var parentStat, childStat unix.Stat_t
	if err := unix.Fstat(parentFD, &parentStat); err != nil {
		return fmt.Errorf("identify secure directory parent: %w", err)
	}
	if err := unix.Fstat(childFD, &childStat); err != nil {
		return fmt.Errorf("identify secure directory child: %w", err)
	}
	parentIdentity := fileIdentity(parentStat)
	childIdentity := fileIdentity(childStat)
	key := directorySyncCacheKey{
		path:                relativePath,
		parentDevice:        parentIdentity.Device,
		parentInode:         parentIdentity.Inode,
		parentChangeSeconds: parentIdentity.ChangeTimeSeconds,
		parentChangeNanos:   parentIdentity.ChangeTimeNanoseconds,
		childDevice:         childIdentity.Device,
		childInode:          childIdentity.Inode,
	}
	r.syncMu.Lock()
	if _, ok := r.syncedDirs[key]; ok {
		r.syncMu.Unlock()
		return nil
	}
	r.syncMu.Unlock()
	if err := syncParent(parentFD); err != nil {
		return fmt.Errorf("sync secure parent for %q: %w", relativePath, err)
	}
	r.syncMu.Lock()
	if r.syncedDirs == nil {
		r.syncedDirs = make(map[directorySyncCacheKey]struct{})
	}
	if len(r.syncedDirs) >= 8192 {
		for existing := range r.syncedDirs {
			delete(r.syncedDirs, existing)
		}
	}
	r.syncedDirs[key] = struct{}{}
	r.syncMu.Unlock()
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

func createRootRegularExact(root *Root, path string, data []byte, mode, dirMode fs.FileMode) (FileIdentity, error) {
	parentFD, leaf, err := openParent(root, path, true, dirMode)
	if err != nil {
		return FileIdentity{}, err
	}
	defer unix.Close(parentFD)

	tempName, tempFile, err := createTempAt(parentFD)
	if err != nil {
		return FileIdentity{}, fmt.Errorf("create secure temporary file for %q: %w", path, err)
	}
	published := false
	defer func() {
		_ = tempFile.Close()
		if !published {
			_ = unix.Unlinkat(parentFD, tempName, 0)
		}
	}()
	if _, err := tempFile.Write(data); err != nil {
		return FileIdentity{}, fmt.Errorf("write secure temporary file for %q: %w", path, err)
	}
	if err := tempFile.Chmod(mode.Perm()); err != nil {
		return FileIdentity{}, fmt.Errorf("set secure file mode for %q: %w", path, err)
	}
	if err := tempFile.Sync(); err != nil {
		return FileIdentity{}, fmt.Errorf("sync secure file %q: %w", path, err)
	}
	if err := tempFile.Close(); err != nil {
		return FileIdentity{}, fmt.Errorf("close secure file %q: %w", path, err)
	}

	if err := renameAtNoReplace(parentFD, tempName, parentFD, leaf); err != nil {
		if !errors.Is(err, unix.EEXIST) {
			return FileIdentity{}, fmt.Errorf("publish secure file %q: %w", path, err)
		}
		identity, replayErr := readExactRegularAt(parentFD, leaf, path, data, mode)
		if replayErr != nil {
			return FileIdentity{}, replayErr
		}
		return identity, nil
	}
	published = true
	if err := unix.Fsync(parentFD); err != nil {
		return FileIdentity{}, fmt.Errorf("sync secure parent after publishing %q: %w", path, err)
	}
	return readExactRegularAt(parentFD, leaf, path, data, mode)
}

func readExactRegularAt(parentFD int, leaf, path string, expected []byte, mode fs.FileMode) (FileIdentity, error) {
	fd, err := unix.Openat(parentFD, leaf, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return FileIdentity{}, fmt.Errorf("%w: open existing secure file %q: %v", ErrContentConflict, path, err)
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return FileIdentity{}, fmt.Errorf("open existing secure file %q: invalid descriptor", path)
	}
	defer file.Close()
	var before unix.Stat_t
	if err := unix.Fstat(fd, &before); err != nil {
		return FileIdentity{}, fmt.Errorf("inspect existing secure file %q: %w", path, err)
	}
	if before.Mode&unix.S_IFMT != unix.S_IFREG || fs.FileMode(before.Mode).Perm() != mode.Perm() {
		return FileIdentity{}, fmt.Errorf("%w: existing secure file %q has different type or mode", ErrContentConflict, path)
	}
	raw, identity, err := readBoundedRegular(file, int64(len(expected)), fileIdentity(before), func() (FileIdentity, error) {
		var after unix.Stat_t
		if err := unix.Fstat(fd, &after); err != nil {
			return FileIdentity{}, err
		}
		return fileIdentity(after), nil
	})
	if err != nil {
		return FileIdentity{}, fmt.Errorf("%w: existing secure file %q cannot be replayed: %v", ErrContentConflict, path, err)
	}
	if !bytes.Equal(raw, expected) {
		return FileIdentity{}, fmt.Errorf("%w: existing secure file %q has different content", ErrContentConflict, path)
	}
	digest := sha256.Sum256(raw)
	identity.ContentSHA256 = hex.EncodeToString(digest[:])
	return identity, nil
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
	if entryType(uint32(info.Mode)) != expectedType || !sameFileIdentityMetadata(fileIdentity(info), identity) {
		return fmt.Errorf("%w: secure entry %q", ErrIdentityMismatch, path)
	}
	if expectedType == EntryTypeDirectory {
		return fmt.Errorf("secure conditional removal %q is a directory; refusing recursive removal", path)
	}
	claimName, err := claimEntryAt(parentFD, leaf)
	if err != nil {
		return fmt.Errorf("claim secure entry %q for removal: %w", path, err)
	}
	var claimed unix.Stat_t
	if err := unix.Fstatat(parentFD, claimName, &claimed, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return fmt.Errorf("inspect claimed secure entry %q: %w", path, err)
	}
	if entryType(uint32(claimed.Mode)) != expectedType || !sameClaimedFileIdentity(fileIdentity(claimed), identity) {
		if restoreErr := renameAtNoReplace(parentFD, claimName, parentFD, leaf); restoreErr != nil {
			return fmt.Errorf("%w: secure entry %q changed while claiming; restore failed: %v", ErrIdentityMismatch, path, restoreErr)
		}
		return fmt.Errorf("%w: secure entry %q changed while claiming", ErrIdentityMismatch, path)
	}
	if identity.ContentSHA256 != "" {
		if err := verifyClaimedRegularContent(parentFD, claimName, identity); err != nil {
			if restoreErr := renameAtNoReplace(parentFD, claimName, parentFD, leaf); restoreErr != nil {
				return fmt.Errorf("%w: secure entry %q content changed while claiming; restore failed: %v", ErrIdentityMismatch, path, restoreErr)
			}
			return fmt.Errorf("%w: secure entry %q content changed while claiming: %v", ErrIdentityMismatch, path, err)
		}
	}
	return removeEntryAt(parentFD, claimName, path)
}

func sameFileIdentityMetadata(actual, expected FileIdentity) bool {
	actual.ContentSHA256 = ""
	expected.ContentSHA256 = ""
	return actual == expected
}

func sameClaimedFileIdentity(actual, expected FileIdentity) bool {
	return actual.Device == expected.Device &&
		actual.Inode == expected.Inode &&
		actual.SizeBytes == expected.SizeBytes &&
		actual.ModTimeSeconds == expected.ModTimeSeconds &&
		actual.ModTimeNanoseconds == expected.ModTimeNanoseconds
}

func verifyClaimedRegularContent(parentFD int, claimName string, expected FileIdentity) error {
	fd, err := unix.Openat(parentFD, claimName, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), claimName)
	if file == nil {
		_ = unix.Close(fd)
		return fmt.Errorf("invalid claimed file descriptor")
	}
	defer file.Close()
	var before unix.Stat_t
	if err := unix.Fstat(fd, &before); err != nil {
		return err
	}
	raw, err := io.ReadAll(io.LimitReader(file, expected.SizeBytes+1))
	if err != nil {
		return err
	}
	var after unix.Stat_t
	if err := unix.Fstat(fd, &after); err != nil {
		return err
	}
	if fileIdentity(before) != fileIdentity(after) || int64(len(raw)) != expected.SizeBytes {
		return ErrIdentityMismatch
	}
	digest := sha256.Sum256(raw)
	if hex.EncodeToString(digest[:]) != expected.ContentSHA256 {
		return ErrIdentityMismatch
	}
	return nil
}

func claimEntryAt(parentFD int, leaf string) (string, error) {
	for attempt := 0; attempt < 8; attempt++ {
		raw := make([]byte, 16)
		if _, err := rand.Read(raw); err != nil {
			return "", err
		}
		claimName := ".receipt-cleanup-" + hex.EncodeToString(raw) + ".json"
		if err := renameAtNoReplace(parentFD, leaf, parentFD, claimName); err != nil {
			if errors.Is(err, unix.EEXIST) {
				continue
			}
			return "", err
		}
		return claimName, nil
	}
	return "", fmt.Errorf("could not allocate unique cleanup claim")
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

func openParent(root *Root, path string, create bool, mode fs.FileMode) (int, string, error) {
	return openParentWithSync(root, path, create, mode, unix.Fsync)
}

func openParentWithSync(root *Root, path string, create bool, mode fs.FileMode, syncParent func(int) error) (int, string, error) {
	if root == nil || root.fd < 0 {
		return -1, "", fmt.Errorf("secure root is closed")
	}
	if create && syncParent == nil {
		return -1, "", fmt.Errorf("secure directory sync operation is required")
	}
	components, err := splitRelativePath(path)
	if err != nil {
		return -1, "", err
	}
	parentFD, err := unix.Dup(root.fd)
	if err != nil {
		return -1, "", fmt.Errorf("duplicate secure root %q: %w", root.name, err)
	}
	relativePath := ""
	for _, component := range components[:len(components)-1] {
		childFD, openErr := openDirectoryAt(parentFD, component, create, mode)
		if openErr != nil {
			unix.Close(parentFD)
			return -1, "", fmt.Errorf("open secure directory component %q: %w", component, openErr)
		}
		if relativePath == "" {
			relativePath = component
		} else {
			relativePath += "/" + component
		}
		if create {
			if syncErr := root.syncDirectoryEntry(parentFD, childFD, relativePath, syncParent); syncErr != nil {
				_ = unix.Close(childFD)
				_ = unix.Close(parentFD)
				return -1, "", fmt.Errorf("sync secure parent for %q: %w", relativePath, syncErr)
			}
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
