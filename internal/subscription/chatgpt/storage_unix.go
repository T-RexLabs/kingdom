//go:build linux || darwin

package chatgpt

import (
	"context"
	"database/sql"
	"errors"
	"github.com/google/uuid"
	"kingdom/internal/securefs"
	_ "modernc.org/sqlite"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// FileVault owns protected local storage. Each token operation takes a separate
// sidecar lock, so independent project runtimes serialize rotating refreshes.
// The durable exchange marker still detects a crash after remote delivery.
type FileVault struct {
	*Vault
	db *sql.DB
}

func (f *FileVault) Close() error { return f.db.Close() }
func OpenFileVault(ctx context.Context, root string, key []byte) (*FileVault, error) {
	if !filepath.IsAbs(root) {
		return nil, ErrInvalid
	}
	if e := os.MkdirAll(root, 0700); e != nil {
		return nil, ErrInvalid
	}
	dir, e := securefs.OpenRoot(root)
	if e != nil {
		return nil, ErrInvalid
	}
	defer dir.Close()
	info, e := os.Lstat(root)
	if e != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		return nil, ErrDenied
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || st.Uid != uint32(os.Getuid()) {
		return nil, ErrDenied
	}
	existing, existingErr := dir.OpenRegular("chatgpt.db")
	if existingErr == nil {
		existing.Close()
	} else if errors.Is(existingErr, os.ErrNotExist) {
		if _, e = dir.CreateRegularExact("chatgpt.db", nil, 0600, 0700); e != nil {
			return nil, ErrDenied
		}
	} else {
		return nil, ErrDenied
	}

	f, e := dir.OpenRegular("chatgpt.db")
	if e != nil {
		return nil, ErrDenied
	}
	info, e = f.Stat()
	f.Close()
	if e != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return nil, ErrDenied
	}
	st, ok = info.Sys().(*syscall.Stat_t)
	if !ok || st.Uid != uint32(os.Getuid()) || st.Nlink != 1 {
		return nil, ErrDenied
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		p, e := os.Lstat(filepath.Join(root, "chatgpt.db"+suffix))
		if e == nil && (!p.Mode().IsRegular() || p.Mode().Perm()&0077 != 0) {
			return nil, ErrDenied
		}
		if e != nil && !os.IsNotExist(e) {
			return nil, ErrDenied
		}
	}
	db, e := sql.Open("sqlite", filepath.Join(root, "chatgpt.db"))
	if e != nil {
		return nil, ErrUnavailable
	}
	db.SetMaxOpenConns(1)
	if _, e = db.ExecContext(ctx, `PRAGMA busy_timeout=10000`); e != nil {
		db.Close()
		return nil, e
	}
	v, e := NewVault(ctx, db, key)
	if e != nil {
		db.Close()
		return nil, e
	}
	v.operationLock = func(ctx context.Context, id string) (func(), error) {
		parsed, e := uuid.Parse(id)
		if e != nil || parsed.String() != id {
			return nil, ErrDenied
		}
		path := filepath.Join(root, "chatgpt-"+id+".lock")
		fd, e := syscall.Open(path, syscall.O_RDWR|syscall.O_CREAT|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0600)
		if e != nil {
			return nil, ErrDenied
		}
		file := os.NewFile(uintptr(fd), path)
		stat, e := file.Stat()
		if e != nil {
			file.Close()
			return nil, ErrDenied
		}
		system, ok := stat.Sys().(*syscall.Stat_t)
		if !ok || !stat.Mode().IsRegular() || stat.Mode().Perm() != 0600 || system.Nlink != 1 || system.Uid != uint32(os.Getuid()) {
			file.Close()
			return nil, ErrDenied
		}
		for {
			e = syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB)
			if e == nil {
				return func() { _ = syscall.Flock(fd, syscall.LOCK_UN); _ = file.Close() }, nil
			}
			if e != syscall.EWOULDBLOCK && e != syscall.EAGAIN {
				file.Close()
				return nil, ErrDenied
			}
			select {
			case <-ctx.Done():
				file.Close()
				return nil, ctx.Err()
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
	return &FileVault{v, db}, nil
}
