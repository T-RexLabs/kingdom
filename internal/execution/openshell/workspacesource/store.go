// Package workspacesource retains immutable, bounded input archives outside
// workload trees. Only trusted workspace resolution can supply a source root.
package workspacesource

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"strings"

	"github.com/google/uuid"
	"kingdom/internal/execution/openshell/worker"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
	"kingdom/internal/securefs"
	"kingdom/internal/workspacearchive"
	"kingdom/internal/workspacetransfer"
)

var ErrUnavailable = errors.New("immutable workspace source unavailable")

// WorkspaceResolver opens the exact trusted workspace for a canonical
// preparation. The returned descriptor belongs to Capture. No public API may
// implement this by accepting a caller-supplied path.
type WorkspaceResolver interface {
	Open(context.Context, providerrequest.Preparation) (*securefs.Root, error)
}
type Store struct {
	root     *os.Root
	resolver WorkspaceResolver
	limits   workspacearchive.Limits
}

func New(directory string, resolver WorkspaceResolver, limits workspacearchive.Limits) (*Store, error) {
	if resolver == nil {
		return nil, ErrUnavailable
	}
	max := workspacetransfer.DefaultLimits()
	if limits.ArchiveBytes < 1 || limits.ArchiveBytes > max.ArchiveBytes || limits.ExpandedBytes < 1 || limits.ExpandedBytes > max.ExpandedBytes || limits.FileBytes < 1 || limits.FileBytes > limits.ExpandedBytes || limits.FileBytes > max.FileBytes || limits.Entries < 1 || limits.Entries > max.Entries || limits.Depth < 1 || limits.Depth > max.Depth || limits.PathBytes < 1 || limits.PathBytes > max.PathBytes {
		return nil, ErrUnavailable
	}
	before, e := os.Lstat(directory)
	if e != nil || !before.IsDir() || before.Mode().Perm() != 0700 {
		return nil, ErrUnavailable
	}
	root, e := os.OpenRoot(directory)
	if e != nil {
		return nil, ErrUnavailable
	}
	after, e := root.Stat(".")
	if e != nil || !os.SameFile(before, after) {
		root.Close()
		return nil, ErrUnavailable
	}
	return &Store{root: root, resolver: resolver, limits: limits}, nil
}
func (s *Store) Close() error { return s.root.Close() }

// Capture runs outside the canonical SQL transaction. The adapter must retain
// the returned reference in that transaction before it can create a sandbox.
// A crash before retention can leave an unreferenced private archive, never an
// authorized replacement input.
func (s *Store) Capture(ctx context.Context, p providerrequest.Preparation) (ref remote.SourceArchive, err error) {
	if p.ResourceID == "" || p.Owner.InvocationID == "" || p.Lease.Owner == "" || p.Lease.Version < 1 {
		return ref, ErrUnavailable
	}
	if err = ctx.Err(); err != nil {
		return ref, err
	}
	source, err := s.resolver.Open(ctx, p)
	if err != nil {
		return ref, ErrUnavailable
	}
	if source == nil {
		return ref, ErrUnavailable
	}
	defer source.Close()
	id := uuid.NewString()
	name := id + ".zip"
	file, err := s.root.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return ref, ErrUnavailable
	}
	defer func() {
		err = errors.Join(err, file.Close())
		if err != nil {
			ref = remote.SourceArchive{}
			err = errors.Join(err, s.root.Remove(name))
		}
	}()
	exported, err := workspacearchive.ExportZIP(ctx, source, file, s.limits)
	if err != nil {
		return ref, err
	}
	if err = ctx.Err(); err != nil {
		return ref, err
	}
	if err = file.Chmod(0400); err != nil {
		return ref, err
	}
	if err = file.Sync(); err != nil {
		return ref, err
	}
	if err = securefs.SyncDirectory(s.root); err != nil {
		return ref, err
	}
	return remote.SourceArchive{ID: id, SHA256: exported.Manifest.ArchiveSHA256, Bytes: exported.ArchiveBytes}, nil
}

type archive struct {
	*os.File
	size int64
}

func (a *archive) Size() int64 { return a.size }
func (s *Store) Open(ctx context.Context, ref remote.SourceArchive) (worker.Archive, error) {
	id, e := uuid.Parse(ref.ID)
	hash, e2 := hex.DecodeString(ref.SHA256)
	if e != nil || id == uuid.Nil || id.String() != ref.ID || e2 != nil || len(hash) != 32 || strings.ToLower(ref.SHA256) != ref.SHA256 || ref.Bytes < 1 || ref.Bytes > s.limits.ArchiveBytes {
		return nil, ErrUnavailable
	}
	file, e := securefs.OpenRegularFile(s.root, ref.ID+".zip", os.O_RDONLY, 0)
	if e != nil {
		return nil, ErrUnavailable
	}
	accepted := false
	defer func() {
		if !accepted {
			file.Close()
		}
	}()
	info, e := file.Stat()
	if e != nil || info.Size() != ref.Bytes || info.Mode().Perm() != 0400 {
		return nil, ErrUnavailable
	}
	digest := sha256.New()
	n, e := securefs.Copy(ctx, digest, io.LimitReader(file, ref.Bytes+1))
	if e != nil || n != ref.Bytes || hex.EncodeToString(digest.Sum(nil)) != ref.SHA256 {
		return nil, ErrUnavailable
	}
	if _, e = file.Seek(0, io.SeekStart); e != nil {
		return nil, ErrUnavailable
	}
	accepted = true
	return &archive{File: file, size: ref.Bytes}, nil
}
