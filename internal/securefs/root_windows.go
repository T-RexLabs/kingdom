//go:build windows

package securefs

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

func openRoot(path string) (*Root, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(absolute)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("secure root must be a non-symlink directory")
	}
	root, err := os.OpenRoot(absolute)
	if err != nil {
		return nil, err
	}
	opened, err := root.Stat(".")
	if err != nil || !os.SameFile(info, opened) {
		root.Close()
		return nil, ErrIdentityMismatch
	}
	handle, err := windows.CreateFile(
		windows.StringToUTF16Ptr(absolute),
		windows.FILE_GENERIC_READ|windows.SYNCHRONIZE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT,
		0,
	)
	if err != nil {
		root.Close()
		return nil, err
	}
	rootFile := os.NewFile(uintptr(handle), absolute)
	if rootFile == nil {
		_ = windows.CloseHandle(handle)
		root.Close()
		return nil, fmt.Errorf("open secure root: invalid directory handle")
	}
	nativeInfo, err := rootFile.Stat()
	if err != nil || !os.SameFile(opened, nativeInfo) {
		_ = rootFile.Close()
		_ = root.Close()
		return nil, ErrIdentityMismatch
	}
	return &Root{name: absolute, platformRoot: root, rootFile: rootFile}, nil
}
func closeRoot(root *Root) error {
	if root == nil {
		return nil
	}
	var closeErrs []error
	if root.rootFile != nil {
		if err := root.rootFile.Close(); err != nil {
			closeErrs = append(closeErrs, err)
		}
		root.rootFile = nil
	}
	if root.platformRoot != nil {
		if err := root.platformRoot.Close(); err != nil {
			closeErrs = append(closeErrs, err)
		}
		root.platformRoot = nil
	}
	return errors.Join(closeErrs...)
}

func openRootRegular(root *Root, path string) (*os.File, error) {
	file, err := openRootRelative(root, path, EntryTypeRegular, false, windows.FILE_GENERIC_READ)
	if err != nil {
		return nil, fmt.Errorf("open secure regular file %q: %w", path, err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		_ = file.Close()
		return nil, fmt.Errorf("secure entry %q must be regular", path)
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
	return readWindowsRegularFile(file, maxBytes)
}

func readWindowsRegularFile(file *os.File, maxBytes int64) ([]byte, FileIdentity, error) {
	if maxBytes < 0 || maxBytes == math.MaxInt64 {
		return nil, FileIdentity{}, fmt.Errorf("maximum read size must be between 0 and %d", int64(math.MaxInt64-1))
	}
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
	file, err := openRootDirectory(root, path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return file.ReadDir(-1)
}
func readRootDirBounded(root *Root, path string, max int) ([]Entry, error) {
	if max < 0 || max == math.MaxInt {
		return nil, fmt.Errorf("maximum directory entries must be between 0 and %d", math.MaxInt-1)
	}
	file, err := openRootDirectory(root, path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	entries, err := file.ReadDir(max + 1)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	overflow := len(entries) > max
	if overflow {
		entries = entries[:max]
	}
	result := make([]Entry, 0, len(entries))
	for _, item := range entries {
		entryPath := filepath.Join(path, item.Name())
		entry, err := readRootEntry(root, entryPath)
		if err != nil {
			return nil, err
		}
		entry.Name = item.Name()
		result = append(result, entry)
	}
	if overflow {
		return result, ErrLimitExceeded
	}
	return result, nil
}

func readRootEntry(root *Root, path string) (Entry, error) {
	components, err := windowsRelativeComponents(path)
	if err != nil {
		return Entry{}, err
	}
	file, err := openRootRelative(root, path, EntryTypeOther, true, windows.FILE_READ_ATTRIBUTES)
	if err != nil {
		return Entry{}, fmt.Errorf("inspect secure entry %q: %w", path, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return Entry{}, fmt.Errorf("stat secure entry %q: %w", path, err)
	}
	identity, err := windowsFileIdentity(file)
	if err != nil {
		return Entry{}, fmt.Errorf("identify secure entry %q: %w", path, err)
	}
	entryType, err := windowsEntryTypeFromHandle(file, info)
	if err != nil {
		return Entry{}, fmt.Errorf("classify secure entry %q: %w", path, err)
	}
	return Entry{Name: components[len(components)-1], Type: entryType, SizeBytes: info.Size(), Identity: identity}, nil
}

func rootTreeCursorUnavailable(err error) bool {
	return errors.Is(err, fs.ErrNotExist) || errors.Is(err, windows.ERROR_FILE_NOT_FOUND) || errors.Is(err, windows.ERROR_PATH_NOT_FOUND) || errors.Is(err, windows.ERROR_DIRECTORY) || errors.Is(err, windows.ERROR_CANT_ACCESS_FILE)
}

func rootDirectoryNotEmpty(err error) bool {
	return errors.Is(err, windows.ERROR_DIR_NOT_EMPTY)
}

func openRootDirectory(root *Root, path string) (*os.File, error) {
	if path == "" || path == "." {
		return duplicateWindowsRootFile(root)
	}
	return openRootRelative(root, path, EntryTypeDirectory, false, windows.FILE_GENERIC_READ)
}

func duplicateWindowsRootFile(root *Root) (*os.File, error) {
	if root == nil || root.rootFile == nil {
		return nil, fmt.Errorf("secure root is closed")
	}
	var duplicate windows.Handle
	if err := windows.DuplicateHandle(windows.CurrentProcess(), windows.Handle(root.rootFile.Fd()), windows.CurrentProcess(), &duplicate, 0, false, windows.DUPLICATE_SAME_ACCESS); err != nil {
		return nil, fmt.Errorf("duplicate secure root handle: %w", err)
	}
	file := os.NewFile(uintptr(duplicate), root.name)
	if file == nil {
		_ = windows.CloseHandle(duplicate)
		return nil, fmt.Errorf("duplicate secure root handle: invalid handle")
	}
	return file, nil
}

func openRootRelative(root *Root, path string, expectedType EntryType, allowFinalReparse bool, access uint32) (*os.File, error) {
	if root == nil || root.rootFile == nil {
		return nil, fmt.Errorf("secure root is closed")
	}
	components, err := windowsRelativeComponents(path)
	if err != nil {
		return nil, err
	}
	parent := windows.Handle(root.rootFile.Fd())
	var ownedParent *os.File
	for index, component := range components {
		final := index == len(components)-1
		attributes := uint32(windows.OBJ_CASE_INSENSITIVE)
		options := uint32(windows.FILE_OPEN_FOR_BACKUP_INTENT | windows.FILE_SYNCHRONOUS_IO_NONALERT)
		if !final || !allowFinalReparse {
			attributes |= windows.OBJ_DONT_REPARSE
		} else {
			options |= windows.FILE_OPEN_REPARSE_POINT
		}
		if !final || (final && expectedType == EntryTypeDirectory) {
			options |= windows.FILE_DIRECTORY_FILE
		}
		if final && expectedType == EntryTypeRegular {
			options |= windows.FILE_NON_DIRECTORY_FILE
		}
		handle, openErr := windowsOpenRelative(parent, component, access|windows.SYNCHRONIZE, attributes, options)
		if openErr != nil {
			if ownedParent != nil {
				_ = ownedParent.Close()
			}
			return nil, fmt.Errorf("open secure path component %q: %w", component, openErr)
		}
		file := os.NewFile(uintptr(handle), path)
		if file == nil {
			_ = windows.CloseHandle(handle)
			if ownedParent != nil {
				_ = ownedParent.Close()
			}
			return nil, fmt.Errorf("open secure path component %q: invalid handle", component)
		}
		info, statErr := file.Stat()
		if statErr != nil {
			_ = file.Close()
			if ownedParent != nil {
				_ = ownedParent.Close()
			}
			return nil, statErr
		}
		if !final {
			attributes, attributesErr := windowsFileAttributes(file)
			if attributesErr != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || attributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
				_ = file.Close()
				if ownedParent != nil {
					_ = ownedParent.Close()
				}
				if attributesErr != nil {
					return nil, attributesErr
				}
				return nil, fmt.Errorf("secure path component %q is not a non-reparse directory", component)
			}
			if ownedParent != nil {
				_ = ownedParent.Close()
			}
			ownedParent = file
			parent = windows.Handle(file.Fd())
			continue
		}
		attributes, attributesErr := windowsFileAttributes(file)
		if attributesErr != nil {
			_ = file.Close()
			if ownedParent != nil {
				_ = ownedParent.Close()
			}
			return nil, attributesErr
		}
		if expectedType == EntryTypeDirectory && (!info.IsDir() || info.Mode()&os.ModeSymlink != 0 || attributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0) {
			_ = file.Close()
			if ownedParent != nil {
				_ = ownedParent.Close()
			}
			return nil, fmt.Errorf("secure entry %q is not a non-reparse directory", path)
		}
		if expectedType == EntryTypeRegular && (!info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || attributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0) {
			_ = file.Close()
			if ownedParent != nil {
				_ = ownedParent.Close()
			}
			return nil, fmt.Errorf("secure entry %q is not a regular file", path)
		}
		if ownedParent != nil {
			_ = ownedParent.Close()
		}
		return file, nil
	}
	return nil, fmt.Errorf("secure relative path is empty")
}

func windowsRelativeComponents(path string) ([]string, error) {
	components, err := splitRelativePath(path)
	if err != nil {
		return nil, err
	}
	for _, component := range components {
		if !filepath.IsLocal(component) {
			return nil, fmt.Errorf("invalid secure path component %q", component)
		}
	}
	return components, nil
}

func windowsOpenRelative(parent windows.Handle, component string, access, attributes, options uint32) (windows.Handle, error) {
	return windowsOpenRelativeWithDisposition(parent, component, access, attributes, options, windows.FILE_OPEN)
}

func windowsCreateRelative(parent windows.Handle, component string, access, options uint32) (windows.Handle, error) {
	return windowsOpenRelativeWithDisposition(
		parent,
		component,
		access,
		uint32(windows.OBJ_CASE_INSENSITIVE|windows.OBJ_DONT_REPARSE),
		options,
		windows.FILE_CREATE,
	)
}

func windowsOpenRelativeWithDisposition(parent windows.Handle, component string, access, attributes, options, disposition uint32) (windows.Handle, error) {
	name, err := windows.NewNTUnicodeString(strings.ReplaceAll(component, "/", `\`))
	if err != nil {
		return windows.InvalidHandle, err
	}
	objectAttributes := &windows.OBJECT_ATTRIBUTES{
		Length:        uint32(unsafe.Sizeof(windows.OBJECT_ATTRIBUTES{})),
		RootDirectory: parent,
		ObjectName:    name,
		Attributes:    attributes,
	}
	var handle windows.Handle
	if err := windows.NtCreateFile(
		&handle,
		access,
		objectAttributes,
		&windows.IO_STATUS_BLOCK{},
		nil,
		windows.FILE_ATTRIBUTE_NORMAL,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		disposition,
		options,
		0,
		0,
	); err != nil {
		if status, ok := err.(windows.NTStatus); ok {
			return windows.InvalidHandle, status.Errno()
		}
		return windows.InvalidHandle, err
	}
	return handle, nil
}

func windowsNameCollision(err error) bool {
	return errors.Is(err, windows.ERROR_FILE_EXISTS) || errors.Is(err, windows.ERROR_ALREADY_EXISTS)
}

func ensureRootDir(root *Root, path string, mode fs.FileMode) error {
	return ensureRootDirWithPolicy(root, path, mode, false)
}

// ensureRootDirReplacingNonDirectories preserves the parent replacement
// semantics used by ReplaceRegular and CreateRegularExact.
func ensureRootDirReplacingNonDirectories(root *Root, path string, mode fs.FileMode) error {
	return ensureRootDirWithPolicy(root, path, mode, true)
}

func ensureRootDirWithPolicy(root *Root, path string, mode fs.FileMode, replaceNonDirectories bool) error {
	components, err := windowsRelativeComponents(path)
	if err != nil {
		return err
	}
	current := ""
	for _, part := range components {
		current = filepath.Join(current, part)
		entry, err := readRootEntry(root, current)
		if errors.Is(err, os.ErrNotExist) {
			if err := createRootDirectoryComponent(root, current, mode); err != nil {
				return fmt.Errorf("create secure directory component %q: %w", current, err)
			}
			continue
		}
		if err != nil {
			return err
		}
		if entry.Type == EntryTypeDirectory {
			continue
		}
		if !replaceNonDirectories {
			return fmt.Errorf("secure directory component %q is not a directory", current)
		}
		if err := removeRootEntryIfIdentity(root, current, entry.Type, entry.Identity); err != nil {
			return fmt.Errorf("remove replaceable secure directory component %q: %w", current, err)
		}
		if err := createRootDirectoryComponent(root, current, mode); err != nil {
			return fmt.Errorf("create secure directory component %q: %w", current, err)
		}
	}
	return nil
}

func createRootDirectoryComponent(root *Root, path string, mode fs.FileMode) error {
	components, err := windowsRelativeComponents(path)
	if err != nil {
		return err
	}
	parent, err := openRootDirectory(root, filepath.Dir(path))
	if err != nil {
		return fmt.Errorf("open secure parent for %q: %w", path, err)
	}
	defer parent.Close()
	if err := createWindowsDirectoryAt(parent, components[len(components)-1], mode); err != nil {
		if !windowsNameCollision(err) {
			return err
		}
		entry, inspectErr := readRootEntry(root, path)
		if inspectErr == nil && entry.Type == EntryTypeDirectory {
			return nil
		}
		return err
	}
	return nil
}

func createOSRootDirectory(root *os.Root, name string, mode fs.FileMode) (bool, error) {
	if root == nil {
		return false, fmt.Errorf("secure directory root is required")
	}
	rootName := root.Name()
	if !filepath.IsAbs(rootName) {
		absolute, err := filepath.Abs(rootName)
		if err != nil {
			return false, err
		}
		rootName = absolute
	}
	path, err := windows.UTF16PtrFromString(rootName)
	if err != nil {
		return false, err
	}
	handle, err := windows.CreateFile(path, windows.FILE_GENERIC_READ|windows.SYNCHRONIZE, windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE, nil, windows.OPEN_EXISTING, windows.FILE_FLAG_BACKUP_SEMANTICS|windows.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if err != nil {
		return false, fmt.Errorf("open secure directory parent for creation: %w", err)
	}
	parent := os.NewFile(uintptr(handle), rootName)
	if parent == nil {
		_ = windows.CloseHandle(handle)
		return false, fmt.Errorf("open secure directory parent for creation: invalid handle")
	}
	defer parent.Close()
	expected, err := root.Stat(".")
	if err != nil {
		return false, err
	}
	opened, err := parent.Stat()
	if err != nil || !os.SameFile(expected, opened) {
		return false, ErrIdentityMismatch
	}
	if err := createWindowsDirectoryAt(parent, name, mode); err != nil {
		if !windowsNameCollision(err) {
			return false, err
		}
		info, statErr := root.Lstat(name)
		if statErr == nil && info.IsDir() && info.Mode()&os.ModeSymlink == 0 {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func createWindowsDirectoryAt(parent *os.File, name string, mode fs.FileMode) error {
	// Windows directories inherit their ACL from the parent, as with os.Root.Mkdir.
	parentHandle := windows.Handle(parent.Fd())
	options := uint32(windows.FILE_DIRECTORY_FILE | windows.FILE_OPEN_FOR_BACKUP_INTENT | windows.FILE_SYNCHRONOUS_IO_NONALERT | windows.FILE_WRITE_THROUGH)
	for range 32 {
		var token [16]byte
		if _, err := rand.Read(token[:]); err != nil {
			return fmt.Errorf("allocate secure temporary directory name: %w", err)
		}
		tempName := fmt.Sprintf(".secure-dir-%x.tmp", token[:])
		handle, err := windowsCreateRelative(parentHandle, tempName, windows.FILE_READ_ATTRIBUTES|windows.DELETE, options)
		if windowsNameCollision(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("create secure temporary directory: %w", err)
		}
		directory := os.NewFile(uintptr(handle), tempName)
		if directory == nil {
			_ = windows.CloseHandle(handle)
			return fmt.Errorf("create secure temporary directory: invalid handle")
		}
		if err := renameWindowsEntryNoReplaceWriteThrough(directory, parent, name); err != nil {
			cleanupErr := deleteWindowsEntryByHandle(directory)
			closeErr := directory.Close()
			return errors.Join(fmt.Errorf("publish secure directory %q: %w", name, err), cleanupErr, closeErr)
		}
		if err := directory.Close(); err != nil {
			return fmt.Errorf("close published secure directory %q: %w", name, err)
		}
		return nil
	}
	return fmt.Errorf("could not allocate a unique secure temporary directory")
}

func replaceRootRegular(root *Root, path string, data []byte, mode, dirMode fs.FileMode, modified time.Time) error {
	return replaceRootRegularFrom(context.Background(), root, path, func(w io.Writer) error { _, err := w.Write(data); return err }, mode, dirMode, modified)
}
func createRootRegularExact(root *Root, path string, data []byte, mode, dirMode fs.FileMode) (FileIdentity, error) {
	components, err := windowsRelativeComponents(path)
	if err != nil {
		return FileIdentity{}, err
	}
	if dir := filepath.Dir(path); dir != "." {
		if err := ensureRootDirReplacingNonDirectories(root, dir, dirMode); err != nil {
			return FileIdentity{}, err
		}
	}
	parent, err := openRootDirectory(root, filepath.Dir(path))
	if err != nil {
		return FileIdentity{}, fmt.Errorf("open secure parent for %q: %w", path, err)
	}
	defer parent.Close()
	tempFile, err := secureWindowsTempAt(windows.Handle(parent.Fd()), data, mode)
	if err != nil {
		return FileIdentity{}, fmt.Errorf("create secure temporary file for %q: %w", path, err)
	}
	published := false
	defer func() {
		if tempFile == nil {
			return
		}
		if !published {
			_ = deleteWindowsEntryByHandle(tempFile)
		}
		_ = tempFile.Close()
	}()
	leaf := components[len(components)-1]
	if err := renameWindowsEntryNoReplaceWriteThrough(tempFile, parent, leaf); err != nil {
		existingHandle, openErr := windowsOpenRelative(
			windows.Handle(parent.Fd()),
			leaf,
			windows.FILE_GENERIC_READ,
			uint32(windows.OBJ_CASE_INSENSITIVE|windows.OBJ_DONT_REPARSE),
			uint32(windows.FILE_NON_DIRECTORY_FILE|windows.FILE_OPEN_FOR_BACKUP_INTENT|windows.FILE_SYNCHRONOUS_IO_NONALERT),
		)
		if openErr != nil {
			if errors.Is(openErr, fs.ErrNotExist) {
				return FileIdentity{}, fmt.Errorf("publish secure file %q: %w", path, err)
			}
			return FileIdentity{}, fmt.Errorf("%w: existing secure file %q cannot be replayed: %v", ErrContentConflict, path, openErr)
		}
		existing := os.NewFile(uintptr(existingHandle), path)
		if existing == nil {
			_ = windows.CloseHandle(existingHandle)
			return FileIdentity{}, fmt.Errorf("open existing secure file %q: invalid handle", path)
		}
		defer existing.Close()
		raw, _, readErr := readWindowsRegularFile(existing, int64(len(data)))
		if readErr != nil || string(raw) != string(data) {
			return FileIdentity{}, fmt.Errorf("%w: existing secure file %q has different content", ErrContentConflict, path)
		}
		return windowsFileIdentity(existing)
	}
	published = true
	identity, err := windowsFileIdentity(tempFile)
	if err != nil {
		return FileIdentity{}, err
	}
	if err := tempFile.Close(); err != nil {
		tempFile = nil
		return FileIdentity{}, fmt.Errorf("close published secure file %q: %w", path, err)
	}
	tempFile = nil
	return identity, nil
}
func removeRootEntry(root *Root, path string) error {
	entry, err := readRootEntry(root, path)
	if err != nil {
		blocked, parentErr := windowsPathHasUntraversableParent(root, path)
		if parentErr == nil && blocked {
			// A child below a reparse point is not part of the secure root's
			// namespace. Treat it as absent so snapshot restoration can later
			// remove the reparse-point parent itself without following it.
			return fs.ErrNotExist
		}
		return err
	}
	if entry.Type == EntryTypeDirectory {
		return fmt.Errorf("refusing to remove directory")
	}
	return root.platformRoot.Remove(path)
}

func windowsPathHasUntraversableParent(root *Root, path string) (bool, error) {
	components, err := windowsRelativeComponents(path)
	if err != nil {
		return false, err
	}
	for length := 1; length < len(components); length++ {
		parentPath := strings.Join(components[:length], string(filepath.Separator))
		parent, err := openRootRelative(root, parentPath, EntryTypeOther, true, windows.FILE_READ_ATTRIBUTES)
		if err != nil {
			return false, err
		}
		attributes, attributesErr := windowsFileAttributes(parent)
		info, statErr := parent.Stat()
		closeErr := parent.Close()
		if err := errors.Join(attributesErr, statErr, closeErr); err != nil {
			return false, err
		}
		if attributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 || !info.IsDir() {
			return true, nil
		}
	}
	return false, nil
}

func removeRootEntryIfIdentity(root *Root, path string, expectedType EntryType, identity FileIdentity) error {
	if expectedType == EntryTypeDirectory {
		return ErrIdentityMismatch
	}
	observed, err := readRootEntry(root, path)
	if err != nil {
		return err
	}
	if observed.Type != expectedType || !sameWindowsEntryIdentity(observed.Identity, identity) {
		return fmt.Errorf("%w: secure entry %q", ErrIdentityMismatch, path)
	}
	if identity.ContentSHA256 != "" {
		if expectedType != EntryTypeRegular {
			return fmt.Errorf("%w: non-regular secure entry %q has content identity", ErrIdentityMismatch, path)
		}
		if err := verifyWindowsRegularClaim(root, path, identity); err != nil {
			return fmt.Errorf("%w: secure entry %q content changed: %v", ErrIdentityMismatch, path, err)
		}
	}
	claim, err := newWindowsClaimPath(path)
	if err != nil {
		return err
	}
	if err := renameRootEntryNoReplace(root, path, claim); err != nil {
		return fmt.Errorf("claim secure entry %q for removal: %w", path, err)
	}
	file, err := openRootRelative(root, claim, EntryTypeOther, true, windows.FILE_READ_ATTRIBUTES|windows.DELETE)
	if err != nil {
		return restoreWindowsClaim(root, claim, path, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return restoreWindowsClaim(root, claim, path, err)
	}
	actual, err := windowsFileIdentity(file)
	if err != nil {
		return restoreWindowsClaim(root, claim, path, err)
	}
	actualType, err := windowsEntryTypeFromHandle(file, info)
	// Claiming renames the object and legitimately advances its change time.
	// Keep all other observed metadata, and verify content against the original
	// digest using the claimed generation's change time.
	claimedIdentity := identity
	claimedIdentity.ChangeTimeSeconds = actual.ChangeTimeSeconds
	claimedIdentity.ChangeTimeNanoseconds = actual.ChangeTimeNanoseconds
	if err != nil || actualType != expectedType || !sameWindowsEntryIdentity(actual, claimedIdentity) {
		return restoreWindowsClaim(root, claim, path, ErrIdentityMismatch)
	}
	if identity.ContentSHA256 != "" {
		if err := verifyWindowsRegularClaim(root, claim, claimedIdentity); err != nil {
			return restoreWindowsClaim(root, claim, path, err)
		}
	}
	if err := deleteWindowsEntryByHandle(file); err != nil {
		return fmt.Errorf("remove claimed secure entry %q: %w", path, err)
	}
	return nil
}
func removeRootEmptyDir(root *Root, path string) error { return root.platformRoot.Remove(path) }

func newWindowsClaimPath(path string) (string, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", err
	}
	return filepath.Join(filepath.Dir(path), fmt.Sprintf(".claimed-%x.tmp", token[:])), nil
}

// renameWindowsEntryNoReplaceWriteThrough publishes through a pinned parent
// handle. The source must be opened with FILE_WRITE_THROUGH: Microsoft's
// CreateFile documentation states that this also flushes NTFS metadata changes
// such as rename operations. FileRenameInfo's false ReplaceIfExists preserves
// CreateRegularExact's no-replace behavior without resolving a path from the
// process working directory or following parent reparse points.
func renameWindowsEntryNoReplaceWriteThrough(source, parent *os.File, name string) error {
	return renameWindowsEntryWriteThrough(source, parent, name, false)
}

func renameWindowsEntryWriteThrough(source, parent *os.File, name string, replace bool) error {
	utf16Name, err := windows.UTF16FromString(name)
	if err != nil {
		return err
	}
	nameLength := (len(utf16Name) - 1) * 2
	var renameInfoLayout struct {
		ReplaceIfExists uint32
		RootDirectory   windows.Handle
		FileNameLength  uint32
		FileName        [1]uint16
	}
	nameOffset := int(unsafe.Offsetof(renameInfoLayout.FileName))
	buffer := make([]byte, nameOffset+nameLength)
	info := (*struct {
		ReplaceIfExists uint32
		RootDirectory   windows.Handle
		FileNameLength  uint32
		FileName        [1]uint16
	})(unsafe.Pointer(&buffer[0]))
	info.RootDirectory = windows.Handle(parent.Fd())
	if replace {
		info.ReplaceIfExists = 1
	}
	info.FileNameLength = uint32(nameLength)
	copy(unsafe.Slice(&info.FileName[0], len(utf16Name)-1), utf16Name[:len(utf16Name)-1])
	return windows.SetFileInformationByHandle(
		windows.Handle(source.Fd()),
		windows.FileRenameInfo,
		(*byte)(unsafe.Pointer(&buffer[0])),
		uint32(len(buffer)),
	)
}

func verifyWindowsRegularClaim(root *Root, path string, expected FileIdentity) error {
	actual, _, err := root.CopyRegular(context.Background(), path, io.Discard)
	if err != nil {
		return err
	}
	if !sameWindowsEntryIdentity(actual, expected) || actual.ContentSHA256 != expected.ContentSHA256 {
		return ErrIdentityMismatch
	}
	return nil
}

func restoreWindowsClaim(root *Root, claim, original string, cause error) error {
	if err := renameRootEntryNoReplace(root, claim, original); err != nil {
		return fmt.Errorf("%w: claim rollback failed after %v: %v", ErrIdentityMismatch, cause, err)
	}
	return fmt.Errorf("%w: claimed secure entry changed: %v", ErrIdentityMismatch, cause)
}

func renameRootEntryNoReplace(root *Root, oldPath, newPath string) error {
	file, err := openRootRelative(root, oldPath, EntryTypeOther, true, windows.DELETE|windows.FILE_READ_ATTRIBUTES)
	if err != nil {
		return err
	}
	defer file.Close()
	components, err := windowsRelativeComponents(newPath)
	if err != nil {
		return err
	}
	name := strings.Join(components, `\`)
	utf16Name, err := windows.UTF16FromString(name)
	if err != nil {
		return err
	}
	nameLength := (len(utf16Name) - 1) * 2
	var renameExLayout struct {
		Flags          uint32
		RootDirectory  windows.Handle
		FileNameLength uint32
		FileName       [1]uint16
	}
	exOffset := int(unsafe.Offsetof(renameExLayout.FileName))
	exBuffer := make([]byte, exOffset+nameLength)
	renameEx := (*struct {
		Flags          uint32
		RootDirectory  windows.Handle
		FileNameLength uint32
		FileName       [1]uint16
	})(unsafe.Pointer(&exBuffer[0]))
	renameEx.RootDirectory = windows.Handle(root.rootFile.Fd())
	renameEx.FileNameLength = uint32(nameLength)
	copy(unsafe.Slice(&renameEx.FileName[0], len(utf16Name)-1), utf16Name[:len(utf16Name)-1])
	const fileRenameInformationEx = 65
	err = windows.NtSetInformationFile(windows.Handle(file.Fd()), &windows.IO_STATUS_BLOCK{}, (*byte)(unsafe.Pointer(&exBuffer[0])), uint32(len(exBuffer)), fileRenameInformationEx)
	if err == nil {
		return nil
	}

	var renameLayout struct {
		ReplaceIfExists byte
		RootDirectory   windows.Handle
		FileNameLength  uint32
		FileName        [1]uint16
	}
	legacyOffset := int(unsafe.Offsetof(renameLayout.FileName))
	legacyBuffer := make([]byte, legacyOffset+nameLength)
	legacy := (*struct {
		ReplaceIfExists byte
		RootDirectory   windows.Handle
		FileNameLength  uint32
		FileName        [1]uint16
	})(unsafe.Pointer(&legacyBuffer[0]))
	legacy.RootDirectory = windows.Handle(root.rootFile.Fd())
	legacy.FileNameLength = uint32(nameLength)
	copy(unsafe.Slice(&legacy.FileName[0], len(utf16Name)-1), utf16Name[:len(utf16Name)-1])
	err = windows.NtSetInformationFile(windows.Handle(file.Fd()), &windows.IO_STATUS_BLOCK{}, (*byte)(unsafe.Pointer(&legacyBuffer[0])), uint32(len(legacyBuffer)), windows.FileRenameInformation)
	if status, ok := err.(windows.NTStatus); ok {
		return status.Errno()
	}
	return err
}

func deleteWindowsEntryByHandle(file *os.File) error {
	disposition := struct{ Flags uint32 }{
		Flags: windows.FILE_DISPOSITION_DELETE | windows.FILE_DISPOSITION_POSIX_SEMANTICS | windows.FILE_DISPOSITION_IGNORE_READONLY_ATTRIBUTE,
	}
	err := windows.NtSetInformationFile(windows.Handle(file.Fd()), &windows.IO_STATUS_BLOCK{}, (*byte)(unsafe.Pointer(&disposition)), uint32(unsafe.Sizeof(disposition)), windows.FileDispositionInformationEx)
	if err == nil {
		return nil
	}
	legacy := struct{ DeleteFile byte }{DeleteFile: 1}
	err = windows.NtSetInformationFile(windows.Handle(file.Fd()), &windows.IO_STATUS_BLOCK{}, (*byte)(unsafe.Pointer(&legacy)), uint32(unsafe.Sizeof(legacy)), windows.FileDispositionInformation)
	if status, ok := err.(windows.NTStatus); ok {
		return status.Errno()
	}
	return err
}

func sameWindowsEntryIdentity(actual, expected FileIdentity) bool {
	return actual.Device == expected.Device &&
		actual.Inode == expected.Inode &&
		actual.SizeBytes == expected.SizeBytes &&
		actual.ModTimeSeconds == expected.ModTimeSeconds &&
		actual.ModTimeNanoseconds == expected.ModTimeNanoseconds &&
		actual.ChangeTimeSeconds == expected.ChangeTimeSeconds &&
		actual.ChangeTimeNanoseconds == expected.ChangeTimeNanoseconds
}

func windowsFileAttributes(file *os.File) (uint32, error) {
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(file.Fd()), &info); err != nil {
		return 0, err
	}
	return info.FileAttributes, nil
}

func windowsEntryTypeFromHandle(file *os.File, info fs.FileInfo) (EntryType, error) {
	attributes, err := windowsFileAttributes(file)
	if err != nil {
		return EntryTypeOther, err
	}
	entryType := windowsEntryType(info.Mode())
	if attributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 && entryType != EntryTypeSymlink {
		return EntryTypeOther, nil
	}
	return entryType, nil
}

// syncRootDir retains the Windows directory-handle flush operation for
// existing callers. FlushFileBuffers documentation does not establish it as a
// crash-durable namespace publication primitive.
func syncRootDir(root *Root, path string) error {
	if root == nil || root.platformRoot == nil {
		return fmt.Errorf("secure root is closed")
	}
	return syncOSRootDir(root.platformRoot, path)
}

func syncOSRootDirectory(root *os.Root) error {
	return syncOSRootDir(root, ".")
}

func syncOSRootDir(root *os.Root, path string) error {
	if root == nil {
		return fmt.Errorf("secure directory root is required")
	}
	rootName := root.Name()
	if !filepath.IsAbs(rootName) {
		return fmt.Errorf("secure directory root must have an absolute name")
	}
	absolute := filepath.Join(rootName, path)
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
	expected, err := root.Stat(path)
	if err != nil {
		return err
	}
	opened, err := file.Stat()
	if err != nil || !os.SameFile(expected, opened) {
		return ErrIdentityMismatch
	}
	return windows.FlushFileBuffers(handle)
}

func secureWindowsTempAt(parent windows.Handle, data []byte, mode fs.FileMode) (*os.File, error) {
	return secureWindowsTempFromAt(parent, func(w io.Writer) error { _, err := w.Write(data); return err }, mode)
}

func secureWindowsTempFromAt(parent windows.Handle, produce func(io.Writer) error, mode fs.FileMode) (*os.File, error) {
	options := uint32(windows.FILE_NON_DIRECTORY_FILE | windows.FILE_OPEN_FOR_BACKUP_INTENT | windows.FILE_SYNCHRONOUS_IO_NONALERT | windows.FILE_WRITE_THROUGH)
	access := uint32(windows.FILE_GENERIC_READ | windows.FILE_GENERIC_WRITE | windows.DELETE)
	for range 32 {
		var token [16]byte
		if _, err := rand.Read(token[:]); err != nil {
			return nil, err
		}
		name := fmt.Sprintf(".secure-%x.tmp", token[:])
		handle, err := windowsCreateRelative(parent, name, access, options)
		if windowsNameCollision(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		file := os.NewFile(uintptr(handle), name)
		if file == nil {
			_ = windows.CloseHandle(handle)
			return nil, fmt.Errorf("create secure temporary file: invalid handle")
		}
		cleanup := func(cause error) error {
			deleteErr := deleteWindowsEntryByHandle(file)
			closeErr := file.Close()
			return errors.Join(cause, deleteErr, closeErr)
		}
		if err := produce(file); err != nil {
			return nil, cleanup(fmt.Errorf("write secure temporary file: %w", err))
		}
		if err := file.Chmod(mode.Perm()); err != nil {
			return nil, cleanup(fmt.Errorf("set secure temporary file mode: %w", err))
		}
		if err := file.Sync(); err != nil {
			return nil, cleanup(fmt.Errorf("sync secure temporary file: %w", err))
		}
		return file, nil
	}
	return nil, fmt.Errorf("could not allocate a unique secure temporary file")
}

func windowsFileIdentity(file *os.File) (FileIdentity, error) {
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(file.Fd()), &info); err != nil {
		return FileIdentity{}, err
	}
	var basic struct {
		CreationTime, LastAccessTime, LastWriteTime, ChangeTime int64
		FileAttributes                                          uint32
	}
	if err := windows.GetFileInformationByHandleEx(windows.Handle(file.Fd()), windows.FileBasicInfo, (*byte)(unsafe.Pointer(&basic)), uint32(unsafe.Sizeof(basic))); err != nil {
		return FileIdentity{}, err
	}
	inode := uint64(info.FileIndexHigh)<<32 | uint64(info.FileIndexLow)
	size := int64(uint64(info.FileSizeHigh)<<32 | uint64(info.FileSizeLow))
	return FileIdentity{
		Device:                uint64(info.VolumeSerialNumber),
		Inode:                 inode,
		SizeBytes:             size,
		ModTimeSeconds:        int64(info.LastWriteTime.HighDateTime),
		ModTimeNanoseconds:    int64(info.LastWriteTime.LowDateTime),
		ChangeTimeSeconds:     basic.ChangeTime / 10000000,
		ChangeTimeNanoseconds: (basic.ChangeTime % 10000000) * 100,
	}, nil
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
