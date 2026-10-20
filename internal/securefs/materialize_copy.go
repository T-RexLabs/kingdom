package securefs

import (
	"context"
	"fmt"
	"io"
	"io/fs"
)

// ReplaceRegularCopy durably copies the regular file observed by no-follow
// enumeration. One pinned source handle supplies both the bytes and metadata;
// a source that differs from expected or changes while it is read is rejected
// before staged bytes publish. It computes no content digest: use CopyRegular
// when the caller must verify canonical content.
func (r *Root) ReplaceRegularCopy(ctx context.Context, path string, source *Root, sourcePath string, expected FileIdentity, dirMode fs.FileMode) error {
	return r.copyRegular(ctx, path, source, sourcePath, expected, dirMode, true)
}

// MaterializeRegularCopy is ReplaceRegularCopy without durability syncs, for an
// independent private copy whose canonical content is stored elsewhere.
func (r *Root) MaterializeRegularCopy(ctx context.Context, path string, source *Root, sourcePath string, expected FileIdentity, dirMode fs.FileMode) error {
	return r.copyRegular(ctx, path, source, sourcePath, expected, dirMode, false)
}

func (r *Root) copyRegular(ctx context.Context, path string, source *Root, sourcePath string, expected FileIdentity, dirMode fs.FileMode, durable bool) error {
	if expected.ContentSHA256 != "" {
		return fmt.Errorf("source %q: a copy cannot verify a content digest", sourcePath)
	}
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
	if before != expected {
		return fmt.Errorf("source %q: %w", sourcePath, ErrIdentityMismatch)
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	return r.writeRegularFrom(ctx, path, func(dst io.Writer) error {
		if _, err := copyPinned(ctx, file, dst, before); err != nil {
			return fmt.Errorf("source %q: %w", sourcePath, err)
		}
		return nil
	}, info.Mode().Perm(), dirMode, info.ModTime(), durable)
}
