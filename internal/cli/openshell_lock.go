package cli

import (
	"errors"
	"fmt"
	"kingdom/internal/securefs"
	"os"
	"path/filepath"
)

// The actual foreground runtime holds this lock; a detached parent never does.
// Setup holds the same stable rendezvous file before reading or publishing YAML.
func lockProjectConfiguration(configPath string) (func(), error) {
	root, err := securefs.OpenRoot(filepath.Dir(configPath))
	if err != nil {
		return nil, err
	}
	defer root.Close()
	if _, err = root.CreateRegularExact(".configuration.lock", nil, 0600, 0700); err != nil {
		return nil, err
	}
	f, err := root.OpenRegular(".configuration.lock")
	if err != nil {
		return nil, err
	}
	if err = securefs.ValidateCurrentUserOnly(f); err != nil {
		f.Close()
		return nil, err
	}
	if err = tryLockProjectFile(f); err != nil {
		f.Close()
		return nil, errors.New("stop the project runtime before changing its configuration")
	}
	return func() { _ = unlockSummonFile(f); _ = f.Close() }, nil
}
func setupDir(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode().Perm() != 0700 {
		return fmt.Errorf("managed state must be a private directory")
	}
	root, err := securefs.OpenRoot(path)
	if err != nil {
		return err
	}
	defer root.Close()
	return nil
}

func lockSummonConfiguration(path string, detachedParent bool) (func(), error) {
	if detachedParent {
		return func() {}, nil
	}
	return lockProjectConfiguration(path)
}
