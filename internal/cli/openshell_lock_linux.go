//go:build linux

package cli

import (
	"golang.org/x/sys/unix"
	"os"
)

func tryLockProjectFile(f *os.File) error { return unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB) }
func publishSetupDirectory(from, to string) error {
	return unix.Renameat2(unix.AT_FDCWD, from, unix.AT_FDCWD, to, unix.RENAME_NOREPLACE)
}
