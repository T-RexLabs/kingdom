//go:build darwin || linux

package cli

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

func configureDetachedCommand(cmd *exec.Cmd) { cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} }
func isProcessRunning(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}
func lockSummonFile(file *os.File) error            { return syscall.Flock(int(file.Fd()), syscall.LOCK_EX) }
func unlockSummonFile(file *os.File) error          { return syscall.Flock(int(file.Fd()), syscall.LOCK_UN) }
func summonSignals() []os.Signal                    { return []os.Signal{syscall.SIGINT, syscall.SIGTERM} }
func signalSummonProcess(process *os.Process) error { return process.Signal(syscall.SIGTERM) }
