package securefs

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Root pins a trusted directory for descriptor-relative descendant access.
type Root struct {
	fd   int
	name string
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

// ReadDir reads one descendant directory without following any symlink.
// An empty path or "." reads the pinned root itself.
func (r *Root) ReadDir(path string) ([]os.DirEntry, error) {
	return readRootDir(r, path)
}

// ReplaceRegular atomically replaces a descendant with a regular file without
// opening the destination for writing. Replaceable non-directory parents are
// removed without following them, and missing parents are created with dirMode.
func (r *Root) ReplaceRegular(path string, data []byte, mode, dirMode fs.FileMode, modifiedAt time.Time) error {
	return replaceRootRegular(r, path, data, mode, dirMode, modifiedAt)
}

// RemoveEntry removes a non-directory descendant without following it.
func (r *Root) RemoveEntry(path string) error {
	return removeRootEntry(r, path)
}
