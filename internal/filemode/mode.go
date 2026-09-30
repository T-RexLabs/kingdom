// Package filemode defines the basic permission metadata retained with files.
package filemode

import (
	"fmt"
	"runtime"
)

// Validate accepts only basic permissions, never type, set-ID or sticky bits.
func Validate(bits int64) error {
	if bits < 0 || bits > 0o777 {
		return fmt.Errorf("file mode must contain only basic permission bits: %d", bits)
	}
	return nil
}

// Equal compares permissions that the host filesystem API can represent.
// Windows Chmod controls only owner-write through the read-only attribute.
// Canonical metadata and archives still retain all the requested basic bits.
func Equal(left, right int64) bool {
	return equalForOS(left, right, runtime.GOOS)
}

func equalForOS(left, right int64, goos string) bool {
	mask := int64(0o777)
	if goos == "windows" {
		mask = 0o200
	}
	return left&mask == right&mask
}
