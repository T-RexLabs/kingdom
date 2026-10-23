//go:build !darwin && !linux && !windows

package securefs

import (
	"context"
	"fmt"
	"io"
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
func readRootEntry(*Root, string) (Entry, error) { return Entry{}, unsupportedRoot() }
func rootTreeCursorUnavailable(error) bool       { return false }
func rootDirectoryNotEmpty(error) bool           { return false }
func createOSRootDirectory(*os.Root, string, fs.FileMode) (bool, error) {
	return false, unsupportedRoot()
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
func removeRootEmptyDir(*Root, string) error            { return unsupportedRoot() }
func syncRootDir(*Root, string) error                   { return unsupportedRoot() }
func syncOSRootDirectory(*os.Root) error                { return unsupportedRoot() }
func openedFileIdentity(*os.File) (FileIdentity, error) { return FileIdentity{}, unsupportedRoot() }
func openDirectoryFile(*Root, string) (*os.File, error) { return nil, unsupportedRoot() }
func directoryEntry(*os.File, string) (Entry, error)    { return Entry{}, unsupportedRoot() }
func replaceRootRegularFrom(context.Context, *Root, string, func(io.Writer) error, fs.FileMode, fs.FileMode, time.Time, bool) error {
	return unsupportedRoot()
}

func rootDirectoryIdentity(*Root) (FileIdentity, error) { return FileIdentity{}, unsupportedRoot() }
