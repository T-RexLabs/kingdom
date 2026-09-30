package runtimefs

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"kingdom/internal/securefs"
)

// MaterializeSnapshot copies canonical bytes into an existing empty private
// workspace. Files are verified and atomically installed without durability
// syncs: the snapshot remains the authority. The caller exclusively owns this
// target and must discard/reset a partial workspace before retrying or launch.
// Restoring an existing execution/publication target requires RestoreSnapshot.
func (s *Store) MaterializeSnapshot(ctx context.Context, snapshotID, rootPath string) error {
	if s == nil || s.db == nil || strings.TrimSpace(snapshotID) == "" {
		return fmt.Errorf("runtime fs store and snapshot are required for materialization")
	}
	root, err := securefs.OpenRoot(rootPath)
	if err != nil {
		return fmt.Errorf("open private workspace: %w", err)
	}
	defer root.Close()
	if err := root.IterateDir(ctx, "", func(securefs.Entry) error {
		return fmt.Errorf("snapshot materialization requires an empty private workspace")
	}); err != nil {
		return err
	}
	return s.VisitSnapshotManifest(ctx, snapshotID, func(entry SnapshotEntry) error {
		if err := root.MaterializeRegularFrom(ctx, entry.Path, func(w io.Writer) error {
			return s.copySnapshotBlob(ctx, entry, w)
		}, os.FileMode(entry.ModeBits), 0o755, time.Unix(0, entry.ModifiedNS)); err != nil {
			return fmt.Errorf("materialize private file %q: %w", entry.Path, err)
		}
		return nil
	})
}
