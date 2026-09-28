package processguardian

import (
	"fmt"
	"os"
)

// DispatchHiddenMode must run before ordinary CLI flag processing.
func DispatchHiddenMode(args []string) (int, bool) {
	if len(args) < 2 {
		return 0, false
	}
	switch args[1] {
	case HiddenGuardianMode:
		spec, control, status, stdout, stderr, stdin, err := guardianFiles()
		if err != nil {
			fmt.Fprintln(os.Stderr, "process guardian:", err)
			return 1, true
		}
		if err := runGuardian(spec, control, status, stdout, stderr, stdin); err != nil {
			fmt.Fprintln(os.Stderr, "process guardian:", err)
			return 1, true
		}
		return 0, true
	case HiddenGateMode:
		if err := runGate(os.Stdin, os.NewFile(3, "gate-release"), os.NewFile(5, "provider-stdin")); err != nil {
			fmt.Fprintln(os.Stderr, "process gate:", err)
			return 1, true
		}
		return 0, true
	default:
		return 0, false
	}
}
