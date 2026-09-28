//go:build windows

package cli

import (
	"os"
	"os/exec"

	"golang.org/x/sys/windows"
)

func configureDetachedCommand(cmd *exec.Cmd) {
	cmd.SysProcAttr = &windows.SysProcAttr{CreationFlags: windows.CREATE_NEW_PROCESS_GROUP | windows.DETACHED_PROCESS}
}
func lockSummonFile(file *os.File) error {
	var overlap windows.Overlapped
	return windows.LockFileEx(windows.Handle(file.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK, 0, 1, 0, &overlap)
}
func unlockSummonFile(file *os.File) error {
	var overlap windows.Overlapped
	return windows.UnlockFileEx(windows.Handle(file.Fd()), 0, 1, 0, &overlap)
}
func summonSignals() []os.Signal { return []os.Signal{os.Interrupt} }
