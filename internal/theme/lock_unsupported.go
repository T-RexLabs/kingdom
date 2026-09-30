//go:build !darwin && !linux && !windows

package theme

import (
	"fmt"
	"os"
)

func lockFile(*os.File) error {
	return fmt.Errorf("theme mutation locking is unsupported on this platform")
}
func unlockFile(*os.File) error { return nil }
