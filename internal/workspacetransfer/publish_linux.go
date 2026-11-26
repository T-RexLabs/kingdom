//go:build linux

package workspacetransfer

import "golang.org/x/sys/unix"

func renameExclusive(from int, source string, to int, destination string) error {
	return unix.Renameat2(from, source, to, destination, unix.RENAME_NOREPLACE)
}
