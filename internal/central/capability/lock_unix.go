//go:build linux || darwin

package capability

import (
	"os"
	"path/filepath"
	"syscall"
)

// Canonical parent paths converge on one sidecar. Database symlinks and hard
// links are rejected, preventing alternate names from bypassing this owner.
// Do not flock SQLite's inode: Darwin shares flock and record-lock exclusion.
func lockDatabase(path string) (*os.File, string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, "", ErrInvalid
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(abs))
	if err != nil {
		return nil, "", ErrInvalid
	}
	path = filepath.Join(parent, filepath.Base(abs))
	openRegular := func(name string) (*os.File, error) {
		fd, e := syscall.Open(name, syscall.O_RDWR|syscall.O_CREAT|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0600)
		if e != nil {
			return nil, ErrInvalid
		}
		f := os.NewFile(uintptr(fd), name)
		st, e := f.Stat()
		if e != nil || !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
			f.Close()
			return nil, ErrInvalid
		}
		stat, ok := st.Sys().(*syscall.Stat_t)
		if !ok || stat.Nlink != 1 {
			f.Close()
			return nil, ErrInvalid
		}
		return f, nil
	}
	lock, e := openRegular(path + ".owner")
	if e != nil {
		return nil, "", e
	}
	fail := func() (*os.File, string, error) { lock.Close(); return nil, "", ErrInvalid }
	if syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) != nil {
		return fail()
	}
	db, e := openRegular(path)
	if e != nil {
		return fail()
	}
	st, e := db.Stat()
	db.Close()
	if e != nil {
		return fail()
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		info, e := os.Lstat(path + suffix)
		if e == nil && (!info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0) {
			return fail()
		}
		if e != nil && !os.IsNotExist(e) {
			return fail()
		}
	}
	actual, e := os.Lstat(path)
	if e != nil || !os.SameFile(st, actual) {
		return fail()
	}
	lockInfo, e := lock.Stat()
	actualLock, e2 := os.Lstat(path + ".owner")
	if e != nil || e2 != nil || !os.SameFile(lockInfo, actualLock) {
		return fail()
	}
	return lock, path, nil
}
