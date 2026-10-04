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

func platformProcessGroupQuiescent(pgid int) (bool, error) {
	err := unix.Kill(-pgid, 0)
	if errors.Is(err, unix.ESRCH) {
		return true, nil
	}
	return false, err
}

func platformAcknowledgedTreeAbsent(ownership Ownership) (bool, error) {
	current, err := processIdentityToken(ownership.TreeID)
	if err == nil {
		return current != ownership.TreeIdentityToken, nil
	}
	if !processIdentityNotFound(err) {
		if groupAbsent(ownership.TreeID) {
			return true, nil
		}
		return false, fmt.Errorf("verify unix process-group generation: %w", err)
	}
	return platformProcessGroupQuiescent(ownership.TreeID)
}
