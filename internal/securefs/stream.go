package securefs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"sync"
	"time"
)

// Copy copies incrementally, checking cancellation between reads and writes.
// Wrapping the reader deliberately avoids io.Copy's optional uninterruptible
// WriterTo/ReaderFrom fast paths.
var copyBuffers = sync.Pool{New: func() any { return new([128 * 1024]byte) }}

func Copy(ctx context.Context, dst io.Writer, src io.Reader) (int64, error) {
	buffer := copyBuffers.Get().(*[128 * 1024]byte)
	defer copyBuffers.Put(buffer)
	return io.CopyBuffer(contextWriter{ctx, dst}, contextReader{ctx, src}, buffer[:])
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

type contextWriter struct {
	ctx    context.Context
	writer io.Writer
}

func (w contextWriter) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	return w.writer.Write(p)
}

// CopyRegular streams a no-follow file and verifies its opened identity, size,
// and metadata remained stable. Callers must discard staged output on error.
func (r *Root) CopyRegular(ctx context.Context, path string, dst io.Writer) (FileIdentity, fs.FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return FileIdentity{}, nil, err
	}
	file, err := r.OpenRegular(path)
	if err != nil {
		return FileIdentity{}, nil, err
	}
	defer file.Close()
	return copyRegularFile(ctx, file, dst)
}

func copyRegularFile(ctx context.Context, file *os.File, dst io.Writer) (FileIdentity, fs.FileInfo, error) {
	before, err := openedFileIdentity(file)
	if err != nil {
		return FileIdentity{}, nil, err
	}
	info, err := file.Stat()
	if err != nil {
		return FileIdentity{}, nil, err
	}
	hash := sha256.New()
	after, err := copyPinned(ctx, file, io.MultiWriter(dst, hash), before)
	if err != nil {
		return FileIdentity{}, nil, err
	}
	// The bytes returned here always belong to this stable pinned object.
	after.ContentSHA256 = hex.EncodeToString(hash.Sum(nil))
	return after, info, ctx.Err()
}

// copyPinned streams the pinned file to dst and fails if it is no longer the
// object identified by before once every byte has been read.
func copyPinned(ctx context.Context, file *os.File, dst io.Writer, before FileIdentity) (FileIdentity, error) {
	n, err := Copy(ctx, dst, file)
	if err != nil {
		return FileIdentity{}, err
	}
	after, err := openedFileIdentity(file)
	if err != nil {
		return FileIdentity{}, err
	}
	if before != after || n != after.SizeBytes {
		return FileIdentity{}, ErrIdentityMismatch
	}
	return after, nil
}

// ReplaceRegularFrom stages streamed output and publishes it atomically only
// after produce succeeds. Digest/identity validation belongs inside produce.
// Staging uses a pinned existing directory: failed/cancelled production or
// staging I/O leaves destination ancestors untouched too. Ancestor repair begins
// only after successful staging and the final pre-publication cancellation check.
func (r *Root) ReplaceRegularFrom(ctx context.Context, path string, produce func(io.Writer) error, mode, dirMode fs.FileMode, modified time.Time) error {
	return r.writeRegularFrom(ctx, path, produce, mode, dirMode, modified, true)
}

// MaterializeRegularFrom creates an atomic, verified private working copy without
// durability syncs. It must only be used for rebuildable files whose canonical
// content is stored elsewhere. It retains ReplaceRegularFrom's no-follow access,
// metadata preservation, and verification-before-replacement guarantees.
func (r *Root) MaterializeRegularFrom(ctx context.Context, path string, produce func(io.Writer) error, mode, dirMode fs.FileMode, modified time.Time) error {
	return r.writeRegularFrom(ctx, path, produce, mode, dirMode, modified, false)
}

func (r *Root) writeRegularFrom(ctx context.Context, path string, produce func(io.Writer) error, mode, dirMode fs.FileMode, modified time.Time, durable bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return replaceRootRegularFrom(ctx, r, path, func(w io.Writer) error {
		if err := produce(contextWriter{ctx, w}); err != nil {
			return err
		}
		return ctx.Err()
	}, mode, dirMode, modified, durable)
}

// IterateDir visits a directory incrementally in filesystem order through a
// pinned no-follow handle. No whole-directory allocation or entry cap is used.
func (r *Root) IterateDir(ctx context.Context, path string, visit func(Entry) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	directory, err := openDirectoryFile(r, path)
	if err != nil {
		return err
	}
	defer directory.Close()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		names, readErr := directory.Readdirnames(128)
		for _, name := range names {
			if err := ctx.Err(); err != nil {
				return err
			}
			entry, err := directoryEntry(directory, name)
			if err != nil {
				return err
			}
			if err := visit(entry); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

// RemoveTree runs independent bounded, resumable batches until completion.
// A cancelled operation can safely restart at the root; removed entries are
// its durable progress. Lifecycle owners retain their cleanup debt until done.
func (r *Root) RemoveTree(ctx context.Context, path string, batchSize int) error {
	cursor := ""
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		progress, err := r.RemoveTreeBatch(path, cursor, batchSize)
		if err != nil {
			return err
		}
		if progress.Complete {
			return nil
		}
		cursor = progress.Cursor
	}
}
