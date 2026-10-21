//go:build !linux && !darwin

package capability

import "os"

func lockDatabase(string) (*os.File, string, error) { return nil, "", ErrInvalid }
