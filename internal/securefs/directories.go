// Package securefs provides descriptor-relative, no-follow access to
// security-sensitive directory trees beneath a caller-selected trusted anchor.
package securefs

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

type Directory struct {
	Name      string
	Mode      fs.FileMode
	Create    bool
	ExactMode bool
}

// OpenDirChain opens a descendant directory while pinning every path component
// to a directory descriptor. The anchor itself is the caller's trust boundary.
func OpenDirChain(anchor string, chain ...Directory) (*os.Root, error) {
	if anchor == "" {
		return nil, fmt.Errorf("secure directory anchor is required")
	}
	absoluteAnchor, err := filepath.Abs(anchor)
	if err != nil {
		return nil, fmt.Errorf("resolve secure directory anchor: %w", err)
	}
	root, err := os.OpenRoot(absoluteAnchor)
	if err != nil {
		return nil, err
	}
	for _, component := range chain {
		child, err := OpenDir(root, component)
		if err != nil {
			_ = root.Close()
			return nil, err
		}
		_ = root.Close()
		root = child
	}
	return root, nil
}

// OpenDir opens one directory below an already pinned root. The caller retains
// ownership of parent and must close both roots.
func OpenDir(parent *os.Root, component Directory) (*os.Root, error) {
	return openDirWithSync(parent, component, SyncDirectory)
}

func openDirWithSync(parent *os.Root, component Directory, syncDirectory func(*os.Root) error) (*os.Root, error) {
	if component.Name == "" || component.Name == "." || component.Name == ".." {
		return nil, fmt.Errorf("invalid secure directory component")
	}
	if component.Create && syncDirectory == nil {
		return nil, fmt.Errorf("secure directory sync operation is required when creating directories")
	}
	info, statErr := parent.Lstat(component.Name)
	created := false
	needsSync := component.Create
	if errors.Is(statErr, fs.ErrNotExist) && component.Create {
		var mkdirErr error
		created, mkdirErr = createOSRootDirectory(parent, component.Name, component.Mode.Perm())
		if mkdirErr != nil {
			return nil, mkdirErr
		}
		info, statErr = parent.Lstat(component.Name)
	}
	if statErr != nil {
		return nil, statErr
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return nil, fmt.Errorf("secure path component %q must be a non-symlink directory", component.Name)
	}
	if component.ExactMode && EnforcesPOSIXMode() && info.Mode().Perm() != component.Mode.Perm() {
		return nil, fmt.Errorf("secure directory %q must have mode %04o", component.Name, component.Mode.Perm())
	}
	child, err := parent.OpenRoot(component.Name)
	if err != nil {
		return nil, err
	}
	openedInfo, err := child.Stat(".")
	if err != nil || !os.SameFile(info, openedInfo) {
		_ = child.Close()
		return nil, fmt.Errorf("secure path component %q changed while opening", component.Name)
	}
	if component.ExactMode && created {
		if err := ProtectCurrentUserOnlyPath(child.Name()); err != nil {
			_ = child.Close()
			return nil, fmt.Errorf("protect secure directory %q: %w", component.Name, err)
		}
	}
	directory, err := child.Open(".")
	if err != nil {
		_ = child.Close()
		return nil, err
	}
	validateErr := ValidateCurrentUserOwnership(directory)
	if validateErr == nil && component.ExactMode {
		validateErr = ValidateCurrentUserOnly(directory)
	}
	closeErr := directory.Close()
	if validateErr != nil {
		_ = child.Close()
		return nil, fmt.Errorf("secure directory %q: %w", component.Name, validateErr)
	}
	if closeErr != nil {
		_ = child.Close()
		return nil, closeErr
	}
	if needsSync {
		// Create=true callers may be retrying a path created by another process
		// or by an earlier attempt whose sync failed. Sync both parent and child
		// before returning an open directory that can receive published data.
		if err := syncDirectory(parent); err != nil {
			_ = child.Close()
			return nil, fmt.Errorf("sync secure parent for directory %q: %w", component.Name, err)
		}
		if err := syncDirectory(child); err != nil {
			_ = child.Close()
			return nil, fmt.Errorf("sync newly created secure directory %q: %w", component.Name, err)
		}
	}
	return child, nil
}

// SyncDirectory requests the platform's directory-sync operation for an
// already pinned os.Root. On Windows it flushes a write-capable directory
// handle, which does not promise crash-durable namespace publication.
func SyncDirectory(root *os.Root) error {
	if root == nil {
		return fmt.Errorf("secure directory root is required")
	}
	return syncOSRootDirectory(root)
}

// OpenRegularFile opens a leaf without following a symlink and verifies that
// the descriptor still identifies the entry inspected through the pinned root.
func OpenRegularFile(root *os.Root, name string, flags int, mode fs.FileMode) (*os.File, error) {
	info, err := root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s must be a regular non-symlink file", name)
	}
	file, err := root.OpenFile(name, flags, mode.Perm())
	if err != nil {
		return nil, err
	}
	openedInfo, err := file.Stat()
	if err != nil || !os.SameFile(info, openedInfo) {
		_ = file.Close()
		return nil, fmt.Errorf("%s changed while opening", name)
	}
	return file, nil
}
