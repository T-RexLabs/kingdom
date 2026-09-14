//go:build darwin || linux

package api

import (
	"errors"
	"syscall"
)

func providerTestProcessAbsent(pid int) bool { return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) }
