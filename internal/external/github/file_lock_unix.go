//go:build darwin || linux

package github

import (
	"fmt"
	"os"
	"syscall"
)

func lockFile(file *os.File) error   { return syscall.Flock(int(file.Fd()), syscall.LOCK_EX) }
func unlockFile(file *os.File) error { return syscall.Flock(int(file.Fd()), syscall.LOCK_UN) }
func validateFileSingleLink(file *os.File, name string) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 {
		return fmt.Errorf("%s must have exactly one link", name)
	}
	return nil
}
