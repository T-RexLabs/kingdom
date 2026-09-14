//go:build windows

package securefs

import (
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/windows"
)

func openRoot(path string) (*Root, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("secure root must be a non-symlink directory")
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	opened, err := root.Stat(".")
	if err != nil || !os.SameFile(info, opened) {
		root.Close()
		return nil, ErrIdentityMismatch
	}
	return &Root{name: path, platformRoot: root}, nil
}
func closeRoot(root *Root) error {
	if root == nil || root.platformRoot == nil {
		return nil
	}
	return root.platformRoot.Close()
}

func openRootRegular(root *Root, path string) (*os.File, error) {
	if _, err := splitRelativePath(path); err != nil {
		return nil, err
	}
	info, err := root.platformRoot.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("secure entry %q must be regular", path)
	}
	file, err := root.platformRoot.Open(path)
	if err != nil {
		return nil, err
	}
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		file.Close()
		return nil, ErrIdentityMismatch
	}
	return file, nil
}

func readRootRegular(root *Root, path string, maxBytes int64) ([]byte, FileIdentity, error) {
	file, err := openRootRegular(root, path)
	if err != nil {
		return nil, FileIdentity{}, err
	}
	defer file.Close()
	before, err := windowsFileIdentity(file)
	if err != nil {
		return nil, FileIdentity{}, err
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil {
		return nil, FileIdentity{}, err
	}
	after, err := windowsFileIdentity(file)
	if err != nil {
		return nil, FileIdentity{}, err
	}
	if before != after {
		return nil, after, ErrIdentityMismatch
	}
	if int64(len(raw)) > maxBytes {
		return nil, after, ErrLimitExceeded
	}
	sum := sha256.Sum256(raw)
	after.ContentSHA256 = fmt.Sprintf("%x", sum[:])
	return raw, after, nil
}

func readRootDir(root *Root, path string) ([]os.DirEntry, error) {
	file, err := root.platformRoot.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return file.ReadDir(-1)
}
func readRootDirBounded(root *Root, path string, max int) ([]Entry, error) {
	entries, err := readRootDir(root, path)
	if err != nil {
		return nil, err
	}
	overflow := len(entries) > max
	if overflow {
		entries = entries[:max]
	}
	result := make([]Entry, 0, len(entries))
	for _, item := range entries {
		info, err := item.Info()
		if err != nil {
			return nil, err
		}
		result = append(result, Entry{Name: item.Name(), Type: windowsEntryType(info.Mode()), SizeBytes: info.Size()})
	}
	if overflow {
		return result, ErrLimitExceeded
	}
	return result, nil
}
func ensureRootDir(root *Root, path string, mode fs.FileMode) error {
	components, err := splitRelativePath(path)
	if err != nil {
		return err
	}
	current := ""
	for _, part := range components {
		current = filepath.Join(current, part)
		info, err := root.platformRoot.Lstat(current)
		if errors.Is(err, os.ErrNotExist) {
			if err := root.platformRoot.Mkdir(current, mode); err != nil {
				return err
			}
			continue
		}
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("secure directory component is invalid")
		}
	}
	return nil
}
func replaceRootRegular(root *Root, path string, data []byte, mode, dirMode fs.FileMode, modified time.Time) error {
	if err := ensureRootDir(root, filepath.Dir(path), dirMode); err != nil && filepath.Dir(path) != "." {
		return err
	}
	temp, err := secureWindowsTemp(root, filepath.Dir(path), data, mode)
	if err != nil {
		return err
	}
	defer root.platformRoot.Remove(temp)
	if err := root.platformRoot.Rename(temp, path); err != nil {
		return err
	}
	return root.platformRoot.Chtimes(path, modified, modified)
}
func createRootRegularExact(root *Root, path string, data []byte, mode, dirMode fs.FileMode) (FileIdentity, error) {
	if dir := filepath.Dir(path); dir != "." {
		if err := ensureRootDir(root, dir, dirMode); err != nil {
			return FileIdentity{}, err
		}
	}
	temp, err := secureWindowsTemp(root, filepath.Dir(path), data, mode)
	if err != nil {
		return FileIdentity{}, err
	}
	defer root.platformRoot.Remove(temp)
	if err := root.platformRoot.Link(temp, path); err != nil {
		if existing, openErr := openRootRegular(root, path); openErr == nil {
			defer existing.Close()
			raw, _, _ := readRootRegular(root, path, int64(len(data)))
			if string(raw) == string(data) {
				return windowsFileIdentity(existing)
			}
		}
		return FileIdentity{}, ErrContentConflict
	}
	file, err := openRootRegular(root, path)
	if err != nil {
		return FileIdentity{}, err
	}
	defer file.Close()
	return windowsFileIdentity(file)
}
func removeRootEntry(root *Root, path string) error {
	info, err := root.platformRoot.Lstat(path)
	if err != nil {
		return err
	}
	if info.IsDir() {
		return fmt.Errorf("refusing to remove directory")
	}
	return root.platformRoot.Remove(path)
}
func removeRootEntryIfIdentity(root *Root, path string, expectedType EntryType, identity FileIdentity) error {
	if expectedType != EntryTypeRegular {
		return ErrIdentityMismatch
	}
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return err
	}
	claim := filepath.Join(filepath.Dir(path), fmt.Sprintf(".claimed-%x.tmp", token[:]))
	if err := root.platformRoot.Rename(path, claim); err != nil {
		return err
	}
	raw, actual, err := readRootRegular(root, claim, identity.SizeBytes)
	if err != nil || actual.Device != identity.Device || actual.Inode != identity.Inode || actual.SizeBytes != identity.SizeBytes || actual.ContentSHA256 != identity.ContentSHA256 {
		_ = root.platformRoot.Rename(claim, path)
		return ErrIdentityMismatch
	}
	_ = raw
	return root.platformRoot.Remove(claim)
}
func removeRootEmptyDir(root *Root, path string) error { return root.platformRoot.Remove(path) }
func syncRootDir(root *Root, path string) error {
	absolute := filepath.Join(root.name, path)
	name, err := windows.UTF16PtrFromString(absolute)
	if err != nil {
		return err
	}
	handle, err := windows.CreateFile(name, windows.FILE_GENERIC_READ|windows.FILE_GENERIC_WRITE, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(handle), absolute)
	defer file.Close()
	expected, err := root.platformRoot.Stat(path)
	if err != nil {
		return err
	}
	opened, err := file.Stat()
	if err != nil || !os.SameFile(expected, opened) {
		return ErrIdentityMismatch
	}
	return windows.FlushFileBuffers(handle)
}

func secureWindowsTemp(root *Root, dir string, data []byte, mode fs.FileMode) (string, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", err
	}
	name := filepath.Join(dir, fmt.Sprintf(".secure-%x.tmp", token[:]))
	file, err := root.platformRoot.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return "", err
	}
	if _, err := file.Write(data); err != nil {
		file.Close()
		return "", err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return "", err
	}
	return name, file.Close()
}
func windowsFileIdentity(file *os.File) (FileIdentity, error) {
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(file.Fd()), &info); err != nil {
		return FileIdentity{}, err
	}
	inode := uint64(info.FileIndexHigh)<<32 | uint64(info.FileIndexLow)
	size := int64(uint64(info.FileSizeHigh)<<32 | uint64(info.FileSizeLow))
	return FileIdentity{Device: uint64(info.VolumeSerialNumber), Inode: inode, SizeBytes: size, ModTimeSeconds: int64(info.LastWriteTime.HighDateTime), ModTimeNanoseconds: int64(info.LastWriteTime.LowDateTime)}, nil
}
func windowsEntryType(mode fs.FileMode) EntryType {
	if mode&os.ModeSymlink != 0 {
		return EntryTypeSymlink
	}
	if mode.IsRegular() {
		return EntryTypeRegular
	}
	if mode.IsDir() {
		return EntryTypeDirectory
	}
	return EntryTypeOther
}
