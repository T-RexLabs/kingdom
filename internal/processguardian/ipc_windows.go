//go:build windows

package processguardian

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"syscall"

	"golang.org/x/sys/windows"
)

const (
	controlHandleEnv = "KINGDOM_GUARD_CONTROL_HANDLE"
	stdoutHandleEnv  = "KINGDOM_GUARD_STDOUT_HANDLE"
	stderrHandleEnv  = "KINGDOM_GUARD_STDERR_HANDLE"
	stdinHandleEnv   = "KINGDOM_GUARD_STDIN_HANDLE"
)

func configureGuardianIO(cmd *exec.Cmd, spec, control, status, stdout, stderr, stdin *os.File) {
	cmd.Stdin = spec
	cmd.Stdout = status
	cmd.Stderr = os.Stderr
	cmd.Env = append(os.Environ(), controlHandleEnv+"="+strconv.FormatUint(uint64(control.Fd()), 10), stdoutHandleEnv+"="+strconv.FormatUint(uint64(stdout.Fd()), 10), stderrHandleEnv+"="+strconv.FormatUint(uint64(stderr.Fd()), 10), stdinHandleEnv+"="+strconv.FormatUint(uint64(stdin.Fd()), 10))
	cmd.SysProcAttr = &windows.SysProcAttr{AdditionalInheritedHandles: []syscall.Handle{syscall.Handle(control.Fd()), syscall.Handle(stdout.Fd()), syscall.Handle(stderr.Fd()), syscall.Handle(stdin.Fd())}}
}

func guardianFiles() (*os.File, *os.File, *os.File, *os.File, *os.File, *os.File, error) {
	control, err := inheritedFile(controlHandleEnv, "guardian-control")
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}
	stdout, err := inheritedFile(stdoutHandleEnv, "guardian-stdout")
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}
	stderr, err := inheritedFile(stderrHandleEnv, "guardian-stderr")
	if err != nil {
		return nil, nil, nil, nil, nil, nil, err
	}
	stdin, err := inheritedFile(stdinHandleEnv, "guardian-stdin")
	if err != nil {
		closeFiles(control, stdout, stderr)
		return nil, nil, nil, nil, nil, nil, err
	}
	return os.Stdin, control, os.Stdout, stdout, stderr, stdin, nil
}

func inheritedFile(name, label string) (*os.File, error) {
	raw, err := strconv.ParseUint(os.Getenv(name), 10, 64)
	if err != nil || raw == 0 {
		return nil, fmt.Errorf("invalid inherited %s", label)
	}
	return os.NewFile(uintptr(raw), label), nil
}
