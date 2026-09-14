//go:build darwin || linux

package securefs

import (
	"fmt"
	"io/fs"
	"os"
	"syscall"
)

func CurrentUserOwns(info fs.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() {
		return fmt.Errorf("must be owned by the current user")
	}
	return nil
}
