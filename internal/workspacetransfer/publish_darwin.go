//go:build darwin

package workspacetransfer

import "golang.org/x/sys/unix"

func renameExclusive(from int, source string, to int, destination string) error {
	return unix.RenameatxNp(from, source, to, destination, unix.RENAME_EXCL|unix.RENAME_NOFOLLOW_ANY)
}
