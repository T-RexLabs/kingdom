//go:build darwin || linux

package processguardian

import (
	"os"
	"os/exec"
)

func configureGuardianIO(cmd *exec.Cmd, spec, control, status, stdout, stderr, stdin *os.File) {
	cmd.ExtraFiles = []*os.File{spec, control, status, stdout, stderr, stdin}
}

func guardianFiles() (*os.File, *os.File, *os.File, *os.File, *os.File, *os.File, error) {
	return os.NewFile(3, "guardian-spec"), os.NewFile(4, "guardian-control"), os.NewFile(5, "guardian-status"), os.NewFile(6, "guardian-stdout"), os.NewFile(7, "guardian-stderr"), os.NewFile(8, "guardian-stdin"), nil
}
