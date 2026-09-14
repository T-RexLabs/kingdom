//go:build darwin || linux

package github

import (
	"fmt"
	"io/fs"
	"os"
	"syscall"
)

func lockFile(file *os.File) error   { return syscall.Flock(int(file.Fd()), syscall.LOCK_EX) }
func unlockFile(file *os.File) error { return syscall.Flock(int(file.Fd()), syscall.LOCK_UN) }
func validateFileOwner(info fs.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() {
		return fmt.Errorf("github key path must be owned by current user")
	}
	return nil
}
func validateFileSingleLink(info fs.FileInfo, name string) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 {
		return fmt.Errorf("%s must have exactly one link", name)
	}
	return nil
}
