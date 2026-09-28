//go:build darwin

package processguardian

import (
	"errors"
	"fmt"
	"os/exec"

	"golang.org/x/sys/unix"
)

func processIdentityToken(pid int) (string, error) {
	process, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return "", fmt.Errorf("read process identity: %w", err)
	}
	if process.Proc.P_pid != int32(pid) {
		return "", fmt.Errorf("process identity pid mismatch")
	}
	start := process.Proc.P_starttime
	if start.Sec == 0 && start.Usec == 0 {
		return "", fmt.Errorf("process identity start time is missing")
	}
	return fmt.Sprintf("%d:%d", start.Sec, start.Usec), nil
}

func processIdentityNotFound(err error) bool {
	return errors.Is(err, unix.ESRCH) || errors.Is(err, unix.ENOENT) || errors.Is(err, unix.EIO)
}

func configureCommandIdentity(*Command) error   { return nil }
func configureGuardianIdentity(*exec.Cmd) error { return nil }
