//go:build !darwin && !linux && !windows

package securefs

import (
	"fmt"
	"io/fs"
	"os"
	"time"
)

func unsupportedRoot() error {
	return fmt.Errorf("descriptor-relative no-follow filesystem access is unsupported on this platform")
}

func openRoot(string) (*Root, error) { return nil, unsupportedRoot() }
func closeRoot(*Root) error          { return nil }
func openRootRegular(*Root, string) (*os.File, error) {
	return nil, unsupportedRoot()
}
func readRootRegular(*Root, string, int64) ([]byte, FileIdentity, error) {
	return nil, FileIdentity{}, unsupportedRoot()
}
func readRootDir(*Root, string) ([]os.DirEntry, error) { return nil, unsupportedRoot() }
func readRootDirBounded(*Root, string, int) ([]Entry, error) {
	return nil, unsupportedRoot()
}
func ensureRootDir(*Root, string, fs.FileMode) error { return unsupportedRoot() }
func replaceRootRegular(*Root, string, []byte, fs.FileMode, fs.FileMode, time.Time) error {
	return unsupportedRoot()
}
func createRootRegularExact(*Root, string, []byte, fs.FileMode, fs.FileMode) (FileIdentity, error) {
	return FileIdentity{}, unsupportedRoot()
}
func removeRootEntry(*Root, string) error { return unsupportedRoot() }
func removeRootEntryIfIdentity(*Root, string, EntryType, FileIdentity) error {
	return unsupportedRoot()
}
func removeRootEmptyDir(*Root, string) error { return unsupportedRoot() }
func syncRootDir(*Root, string) error        { return unsupportedRoot() }
