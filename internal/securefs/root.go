package securefs

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Root pins a trusted directory for descriptor-relative descendant access.
type Root struct {
	fd           int
	name         string
	platformRoot *os.Root
	rootFile     *os.File
	syncMu       sync.Mutex
	syncedDirs   map[directorySyncCacheKey]struct{}
}

type directorySyncCacheKey struct {
	path                                   string
	parentDevice, parentInode              uint64
	parentChangeSeconds, parentChangeNanos int64
	childDevice, childInode                uint64
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

// TreeRemovalProgress reports one bounded batch of a resumable tree removal.
// Cursor identifies the directory currently being processed, relative to the
// path passed to RemoveTreeBatch and expressed with slash separators.
type TreeRemovalProgress struct {
	Cursor           string
	EntriesProcessed int
	EntriesRemoved   int
	Complete         bool
}

func splitRelativePath(path string) ([]string, error) {
	if path == "" || filepath.IsAbs(path) {
		return nil, fmt.Errorf("relative path is required")
	}
	cleaned := filepath.Clean(filepath.FromSlash(path))
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

// ReadEntry returns no-follow metadata and identity for one descendant entry.
func (r *Root) ReadEntry(path string) (Entry, error) {
	return readRootEntry(r, path)
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

// RemoveTreeBatch removes at most maxEntries tree entries or traversal actions
// from a descendant tree without following symlinks. The returned cursor can
// be persisted and passed to a later call, including after process restart.
// Complete is true only when the tree's root entry no longer exists.
func (r *Root) RemoveTreeBatch(path, cursor string, maxEntries int) (TreeRemovalProgress, error) {
	if maxEntries <= 0 {
		return TreeRemovalProgress{}, fmt.Errorf("tree removal batch size must be positive")
	}
	if _, err := splitRelativePath(path); err != nil {
		return TreeRemovalProgress{}, err
	}
	cursorParts := []string(nil)
	if cursor != "" {
		var err error
		cursorParts, err = splitRelativePath(cursor)
		if err != nil {
			return TreeRemovalProgress{}, fmt.Errorf("invalid tree removal cursor: %w", err)
		}
	}
	progress := TreeRemovalProgress{Cursor: strings.Join(cursorParts, "/")}
	for progress.EntriesProcessed < maxEntries {
		currentPath := path
		if len(cursorParts) > 0 {
			currentPath = filepath.Join(path, filepath.FromSlash(strings.Join(cursorParts, "/")))
		}
		currentEntry, inspectErr := r.ReadEntry(currentPath)
		if inspectErr != nil {
			if rootTreeCursorUnavailable(inspectErr) {
				if len(cursorParts) == 0 {
					if errors.Is(inspectErr, fs.ErrNotExist) {
						progress.Cursor = ""
						progress.Complete = true
						return progress, nil
					}
					return progress, fmt.Errorf("secure tree root %q is not reachable without following links: %w", path, inspectErr)
				}
				cursorParts = cursorParts[:len(cursorParts)-1]
				progress.EntriesProcessed++
				progress.Cursor = strings.Join(cursorParts, "/")
				continue
			}
			return progress, fmt.Errorf("inspect secure tree entry %q: %w", currentPath, inspectErr)
		}
		if currentEntry.Type != EntryTypeDirectory {
			if len(cursorParts) == 0 {
				if err := r.RemoveEntryIfIdentity(path, currentEntry.Type, currentEntry.Identity); err != nil && !errors.Is(err, fs.ErrNotExist) {
					return progress, fmt.Errorf("remove secure tree root entry %q: %w", path, err)
				}
				progress.EntriesProcessed++
				progress.EntriesRemoved++
				progress.Cursor = ""
				progress.Complete = true
				return progress, nil
			}
			cursorParts = cursorParts[:len(cursorParts)-1]
			progress.EntriesProcessed++
			progress.Cursor = strings.Join(cursorParts, "/")
			continue
		}
		entries, err := r.ReadDirBounded(currentPath, 1)
		if len(entries) > 0 {
			if err != nil && !errors.Is(err, ErrLimitExceeded) {
				return progress, fmt.Errorf("read secure directory %q for removal: %w", currentPath, err)
			}
			entry := entries[0]
			progress.EntriesProcessed++
			if entry.Type == EntryTypeDirectory {
				cursorParts = append(cursorParts, entry.Name)
				progress.Cursor = strings.Join(cursorParts, "/")
				continue
			}
			entryPath := filepath.Join(currentPath, entry.Name)
			if err := r.RemoveEntryIfIdentity(entryPath, entry.Type, entry.Identity); err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					progress.EntriesRemoved++
					continue
				}
				return progress, fmt.Errorf("remove secure tree entry %q: %w", entryPath, err)
			}
			progress.EntriesRemoved++
			continue
		}
		if err != nil {
			if !rootTreeCursorUnavailable(err) {
				return progress, fmt.Errorf("read secure directory %q for removal: %w", currentPath, err)
			}
			if len(cursorParts) > 0 {
				cursorParts = cursorParts[:len(cursorParts)-1]
			}
			progress.EntriesProcessed++
			progress.Cursor = strings.Join(cursorParts, "/")
			continue
		}
		if len(cursorParts) == 0 {
			if err := r.RemoveEmptyDir(path); err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					progress.Cursor = ""
					progress.Complete = true
					return progress, nil
				}
				if rootDirectoryNotEmpty(err) {
					progress.EntriesProcessed++
					continue
				}
				return progress, fmt.Errorf("remove empty secure tree root %q: %w", path, err)
			}
			progress.EntriesProcessed++
			progress.EntriesRemoved++
			progress.Cursor = ""
			progress.Complete = true
			return progress, nil
		}
		if err := r.RemoveEmptyDir(currentPath); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				cursorParts = cursorParts[:len(cursorParts)-1]
				progress.EntriesProcessed++
				progress.Cursor = strings.Join(cursorParts, "/")
				continue
			}
			if rootDirectoryNotEmpty(err) {
				progress.EntriesProcessed++
				continue
			}
			return progress, fmt.Errorf("remove empty secure directory %q: %w", currentPath, err)
		}
		progress.EntriesProcessed++
		progress.EntriesRemoved++
		cursorParts = cursorParts[:len(cursorParts)-1]
		progress.Cursor = strings.Join(cursorParts, "/")
	}
	return progress, nil
}

// SyncDir requests the platform's directory-sync operation for one descendant.
// An empty path or "." targets the pinned root. This is not a portable guarantee
// that namespace entries are crash-durable; Windows callers needing that
// property must use a documented write-through publication operation.
func (r *Root) SyncDir(path string) error {
	return syncRootDir(r, path)
}
