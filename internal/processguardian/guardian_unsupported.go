//go:build !darwin && !linux && !windows

package processguardian

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"
)

func platformSupported() bool { return false }
func configureGuardianProcess(*exec.Cmd) error {
	return fmt.Errorf("process guardian is unsupported on this platform")
}
func runGuardian(*os.File, *os.File, *os.File, *os.File, *os.File) error {
	return fmt.Errorf("process guardian is unsupported on this platform")
}
func runGate(io.Reader, *os.File) error {
	return fmt.Errorf("process guardian is unsupported on this platform")
}
func terminateAcknowledgedTree(Ownership, time.Duration, time.Duration) error {
	return fmt.Errorf("process guardian is unsupported on this platform")
}
func configureGuardianIO(*exec.Cmd, *os.File, *os.File, *os.File, *os.File, *os.File) {}
func guardianFiles() (*os.File, *os.File, *os.File, *os.File, *os.File, error) {
	return nil, nil, nil, nil, nil, fmt.Errorf("process guardian is unsupported on this platform")
}
func ManagedProcessAbsent(int, int, string, string) bool { return false }
