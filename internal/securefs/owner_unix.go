//go:build darwin || linux

package securefs

import (
	"fmt"
	"io/fs"
	"os"
	"syscall"
)

func CurrentUserOwns(info fs.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() {
		return fmt.Errorf("must be owned by the current user")
	}
	return nil
}

func ValidateCurrentUserOwnership(file *os.File) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	return CurrentUserOwns(info)
}

func ValidateCurrentUserOnly(file *os.File) error {
	return ValidateCurrentUserOwnership(file)
}

func ProtectCurrentUserOnlyPath(string) error { return nil }

// EnforcesPOSIXMode reports whether FileMode permission bits are authoritative.
func EnforcesPOSIXMode() bool { return true }
