//go:build linux || darwin

package workspacetransfer

import (
	"golang.org/x/sys/unix"
	"path/filepath"
)

func publishDirectory(source, destination string) error {
	from, err := unix.Open(filepath.Dir(source), unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(from)
	to, err := unix.Open(filepath.Dir(destination), unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(to)
	return renameExclusive(from, filepath.Base(source), to, filepath.Base(destination))
}
