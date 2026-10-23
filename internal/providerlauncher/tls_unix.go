//go:build linux || darwin

package providerlauncher

import (
	"errors"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
)

// The native boundary installs these files before command admission. No job or
// inherited environment can select another trust root.
func checkProviderTrust() error {
	for _, path := range []string{trustedCA, trustedBundle} {
		for parent := filepath.Dir(path); parent != "/"; parent = filepath.Dir(parent) {
			info, err := os.Lstat(parent)
			if err != nil || !info.IsDir() || info.Mode().Perm()&0022 != 0 {
				return errors.New("unsafe provider trust directory")
			}
			var st unix.Stat_t
			if err = unix.Lstat(parent, &st); err != nil || st.Uid != 0 {
				return errors.New("untrusted provider trust directory")
			}
		}
		if err := checkTrustFile(path, 0); err != nil {
			return err
		}
	}
	return nil
}

func checkTrustFile(path string, owner uint32) error {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if err != nil {
		return errors.New("provider trust file unavailable")
	}
	defer unix.Close(fd)
	var st unix.Stat_t
	if err = unix.Fstat(fd, &st); err != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Mode&0222 != 0 || st.Uid != owner || st.Size <= 0 || st.Size > 4<<20 {
		return errors.New("unsafe provider trust file")
	}
	return nil
}
