package runtimefs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"

	"kingdom/internal/securefs"
)

// This is a performance cache, never an eligibility limit. Reuse requires the
// complete no-follow identity (including change time) to still match. Eviction
// simply causes another streaming digest verification.
type blobVerifier struct {
	store    *Store
	root     *securefs.Root
	verified map[string]securefs.FileIdentity
}

func (s *Store) newBlobVerifier() (*blobVerifier, error) {
	root, err := securefs.OpenRoot(s.blobRoot)
	if err != nil {
		return nil, err
	}
	return &blobVerifier{store: s, root: root, verified: make(map[string]securefs.FileIdentity)}, nil
}

func (v *blobVerifier) verify(ctx context.Context, blobID string, size int64, durable bool) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if _, err := v.store.blobPath(blobID); err != nil {
		return false, err
	}
	path := filepath.Join(blobID[:2], blobID[2:4], blobID)
	entry, err := v.root.ReadEntry(path)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if entry.Type != securefs.EntryTypeRegular || entry.SizeBytes != size {
		return false, fmt.Errorf("blob %q: type or size mismatch", blobID)
	}
	if identity, ok := v.verified[blobID]; ok && identity == entry.Identity {
		return true, nil
	}
	identity, _, err := v.root.CopyRegular(ctx, path, io.Discard)
	if err != nil {
		return false, err
	}
	if identity.ContentSHA256 != blobID {
		return false, fmt.Errorf("blob %q: digest mismatch", blobID)
	}
	if identity.SizeBytes != size {
		return false, fmt.Errorf("blob %q: size mismatch", blobID)
	}
	if durable {
		if err := v.store.syncBlobDirectory(blobID); err != nil {
			return false, err
		}
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	identity.ContentSHA256 = ""
	if len(v.verified) >= 128 {
		clear(v.verified)
	}
	v.verified[blobID] = identity
	return true, nil
}
