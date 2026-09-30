//go:build !darwin && !linux

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
func readRootDir(*Root, string) ([]os.DirEntry, error) { return nil, unsupportedRoot() }
func replaceRootRegular(*Root, string, []byte, fs.FileMode, fs.FileMode, time.Time) error {
	return unsupportedRoot()
}
func removeRootEntry(*Root, string) error { return unsupportedRoot() }
