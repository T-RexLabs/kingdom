//go:build darwin || linux

package cli

import (
	"os"
	"os/exec"
	"syscall"
)

func configureDetachedCommand(cmd *exec.Cmd) { cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} }
func lockSummonFile(file *os.File) error     { return syscall.Flock(int(file.Fd()), syscall.LOCK_EX) }
func unlockSummonFile(file *os.File) error   { return syscall.Flock(int(file.Fd()), syscall.LOCK_UN) }
func summonSignals() []os.Signal             { return []os.Signal{syscall.SIGINT, syscall.SIGTERM} }
