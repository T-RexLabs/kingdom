package securefs

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Root pins a trusted directory for descriptor-relative descendant access.
type Root struct {
	fd           int
	name         string
	platformRoot *os.Root
}

// ErrLimitExceeded reports that a bounded filesystem operation reached its
// caller-supplied limit.
var ErrLimitExceeded = errors.New("secure filesystem limit exceeded")

// ErrIdentityMismatch reports that an entry changed after it was read.
var ErrIdentityMismatch = errors.New("secure filesystem entry identity changed")

// ErrContentConflict reports that a create-or-replay destination already
// contains different bytes or metadata.
var ErrContentConflict = errors.New("secure filesystem content conflict")

// FileIdentity identifies one stable version of an opened filesystem object
// without exposing paths.
type FileIdentity struct {
	Device                uint64
	Inode                 uint64
	SizeBytes             int64
	ModTimeSeconds        int64
	ModTimeNanoseconds    int64
	ChangeTimeSeconds     int64
	ChangeTimeNanoseconds int64
	ContentSHA256         string
}

// EntryType describes an entry without following it.
type EntryType string

const (
	EntryTypeRegular   EntryType = "regular"
	EntryTypeDirectory EntryType = "directory"
	EntryTypeSymlink   EntryType = "symlink"
	EntryTypeFIFO      EntryType = "fifo"
	EntryTypeSocket    EntryType = "socket"
	EntryTypeDevice    EntryType = "device"
	EntryTypeOther     EntryType = "other"
)

// Entry is descriptor-relative metadata for one directory entry.
type Entry struct {
	Name      string
	Type      EntryType
	SizeBytes int64
	Identity  FileIdentity
}

func splitRelativePath(path string) ([]string, error) {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" || filepath.IsAbs(trimmed) {
		return nil, fmt.Errorf("relative path is required")
	}
	cleaned := filepath.Clean(filepath.FromSlash(trimmed))
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("path escapes secure root: %q", path)
	}
	components := strings.Split(cleaned, string(filepath.Separator))
	for _, component := range components {
		if component == "" || component == "." || component == ".." {
			return nil, fmt.Errorf("invalid secure path component in %q", path)
		}
	}
	return components, nil
}

// OpenRoot pins path without following a symlink at the root itself.
func OpenRoot(path string) (*Root, error) {
	return openRoot(path)
}

// Close releases the pinned root descriptor.
func (r *Root) Close() error {
	return closeRoot(r)
}

// OpenRegular opens a regular descendant without following any symlink.
func (r *Root) OpenRegular(path string) (*os.File, error) {
	return openRootRegular(r, path)
}

// ReadRegular reads at most maxBytes from a regular descendant without
// following any symlink. A file of exactly maxBytes is accepted.
func (r *Root) ReadRegular(path string, maxBytes int64) ([]byte, error) {
	raw, _, err := readRootRegular(r, path, maxBytes)
	return raw, err
}

// ReadRegularIdentified reads a bounded regular file and returns the identity
// required to avoid deleting a later replacement by name.
func (r *Root) ReadRegularIdentified(path string, maxBytes int64) ([]byte, FileIdentity, error) {
	return readRootRegular(r, path, maxBytes)
}

// ReadDir reads one descendant directory without following any symlink.
// An empty path or "." reads the pinned root itself.
func (r *Root) ReadDir(path string) ([]os.DirEntry, error) {
	return readRootDir(r, path)
}

// ReadDirBounded enumerates at most maxEntries from one descendant directory
// without following any symlink. It returns the bounded prefix together with
// ErrLimitExceeded when another entry exists.
func (r *Root) ReadDirBounded(path string, maxEntries int) ([]Entry, error) {
	return readRootDirBounded(r, path, maxEntries)
}

// EnsureDir creates a descendant directory chain without following or
// replacing any existing entry.
func (r *Root) EnsureDir(path string, mode fs.FileMode) error {
	return ensureRootDir(r, path, mode)
}

// ReplaceRegular atomically replaces a descendant with a regular file without
// opening the destination for writing. Replaceable non-directory parents are
// removed without following them, and missing parents are created with dirMode.
func (r *Root) ReplaceRegular(path string, data []byte, mode, dirMode fs.FileMode, modifiedAt time.Time) error {
	return replaceRootRegular(r, path, data, mode, dirMode, modifiedAt)
}

// CreateRegularExact atomically creates a regular descendant or returns the
// identity of an exact existing replay. It never replaces an existing entry.
func (r *Root) CreateRegularExact(path string, data []byte, mode, dirMode fs.FileMode) (FileIdentity, error) {
	return createRootRegularExact(r, path, data, mode, dirMode)
}

// RemoveEntry removes a non-directory descendant without following it.
func (r *Root) RemoveEntry(path string) error {
	return removeRootEntry(r, path)
}

// RemoveRegularIfIdentity removes a regular file only when it still identifies
// the object returned by ReadRegularIdentified.
func (r *Root) RemoveRegularIfIdentity(path string, identity FileIdentity) error {
	return removeRootEntryIfIdentity(r, path, EntryTypeRegular, identity)

}

// RemoveEntryIfIdentity removes a non-directory entry only while both its type
// and identity still match descriptor-relative enumeration.
func (r *Root) RemoveEntryIfIdentity(path string, entryType EntryType, identity FileIdentity) error {
	return removeRootEntryIfIdentity(r, path, entryType, identity)
}

// RemoveEmptyDir removes only an empty descendant directory without following
// symlinks or recursively deleting contents.
func (r *Root) RemoveEmptyDir(path string) error {
	return removeRootEmptyDir(r, path)
}

// RemoveTree removes a descendant directory tree without following symlinks.
// A final non-directory entry is unlinked rather than traversed.
func (r *Root) RemoveTree(path string) error {
	return removeRootTree(r, path)
}

// SyncDir fsyncs one descendant directory. An empty path or "." syncs the
// pinned root itself.
func (r *Root) SyncDir(path string) error {
	return syncRootDir(r, path)
}
