//go:build windows

package github

import (
	"io/fs"
	"os"

	"golang.org/x/sys/windows"
)

func lockFile(file *os.File) error {
	var overlap windows.Overlapped
	return windows.LockFileEx(windows.Handle(file.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK, 0, 1, 0, &overlap)
}
func unlockFile(file *os.File) error {
	var overlap windows.Overlapped
	return windows.UnlockFileEx(windows.Handle(file.Fd()), 0, 1, 0, &overlap)
}
func validateFileOwner(fs.FileInfo) error              { return nil }
func validateFileSingleLink(fs.FileInfo, string) error { return nil }
