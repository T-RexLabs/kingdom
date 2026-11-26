//go:build windows

package cli

import (
	"errors"
	"golang.org/x/sys/windows"
	"os"
)

func tryLockProjectFile(f *os.File) error {
	var o windows.Overlapped
	return windows.LockFileEx(windows.Handle(f.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &o)
}
func publishSetupDirectory(from, to string) error {
	return errors.New("local OpenShell setup requires a supported POSIX host")
}
