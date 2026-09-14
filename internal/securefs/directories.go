// Package securefs provides descriptor-relative, no-follow access to
// security-sensitive directory trees beneath a caller-selected trusted anchor.
package securefs

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
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
	root, err := os.OpenRoot(anchor)
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
	if component.Name == "" || component.Name == "." || component.Name == ".." {
		return nil, fmt.Errorf("invalid secure directory component")
	}
	info, statErr := parent.Lstat(component.Name)
	if errors.Is(statErr, fs.ErrNotExist) && component.Create {
		if err := parent.Mkdir(component.Name, component.Mode.Perm()); err != nil && !errors.Is(err, fs.ErrExist) {
			return nil, err
		}
		info, statErr = parent.Lstat(component.Name)
	}
	if statErr != nil {
		return nil, statErr
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return nil, fmt.Errorf("secure path component %q must be a non-symlink directory", component.Name)
	}
	if component.ExactMode && info.Mode().Perm() != component.Mode.Perm() {
		return nil, fmt.Errorf("secure directory %q must have mode %04o", component.Name, component.Mode.Perm())
	}
	if err := CurrentUserOwns(info); err != nil {
		return nil, fmt.Errorf("secure directory %q: %w", component.Name, err)
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
	return child, nil
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
