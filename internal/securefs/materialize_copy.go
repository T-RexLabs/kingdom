package securefs

import (
	"context"
	"fmt"
	"io"
	"io/fs"
)

// MaterializeRegularCopy makes an independent private copy of the regular file
// observed by no-follow enumeration. One pinned source handle supplies both the
// bytes and metadata; a changed source is rejected before staged bytes publish.
// Unlike CopyRegular, it does not compute a content digest. Use CopyRegular when
// the caller needs to verify canonical content rather than materialize a copy.
func (r *Root) MaterializeRegularCopy(ctx context.Context, path string, source *Root, sourcePath string, expected FileIdentity, dirMode fs.FileMode) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	file, err := source.OpenRegular(sourcePath)
	if err != nil {
		return err
	}
	defer file.Close()
	before, err := openedFileIdentity(file)
	if err != nil {
		return err
	}
	expected.ContentSHA256 = ""
	if before != expected {
		return fmt.Errorf("source %q: %w", sourcePath, ErrIdentityMismatch)
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	return r.MaterializeRegularFrom(ctx, path, func(dst io.Writer) error {
		n, err := Copy(ctx, dst, file)
		if err != nil {
			return err
		}
		after, err := openedFileIdentity(file)
		if err != nil {
			return err
		}
		if before != after || n != after.SizeBytes {
			return fmt.Errorf("source %q: %w", sourcePath, ErrIdentityMismatch)
		}
		return ctx.Err()
	}, info.Mode().Perm(), dirMode, info.ModTime())
}
