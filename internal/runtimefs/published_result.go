package runtimefs

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"io"
	"io/fs"
	"strings"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/publishedresult"
	"kingdom/internal/trackedfs"
)

// publishedSnapshot accepts only a completed run whose trusted result has
// reached its publication target. A partial or dirty attempt grants no access.
func (s *Store) publishedSnapshot(ctx context.Context, run string) (string, error) {
	if s == nil || s.db == nil || run == "" || strings.TrimSpace(run) != run {
		return "", publishedresult.ErrUnavailable
	}
	var snapshot string
	err := s.db.QueryRowContext(ctx, `SELECT t.published_snapshot_id FROM workflow_runs r
 JOIN run_execution_targets t ON t.run_id=r.run_id
 JOIN filesystem_snapshots f ON f.snapshot_id=t.published_snapshot_id AND f.run_id=r.run_id
 WHERE r.run_id=? AND r.run_status='succeeded' AND t.state='trusted'
 AND t.published_snapshot_id!='' AND t.published_snapshot_id=t.trusted_snapshot_id`, run).Scan(&snapshot)
	if err != nil {
		return "", publishedresult.ErrUnavailable
	}
	return snapshot, nil
}
func (s *Store) Resolve(ctx context.Context, run string) (publishedresult.ResultSnapshot, error) {
	snapshot, err := s.publishedSnapshot(ctx, run)
	if err != nil {
		return publishedresult.ResultSnapshot{}, err
	}
	result := publishedresult.ResultSnapshot{RunID: run, SnapshotID: snapshot, Files: []publishedresult.ResultFile{}}
	var total int64
	err = s.VisitSnapshotManifest(ctx, snapshot, func(e SnapshotEntry) error {
		if len(result.Files) >= publishedresult.MaxFiles || !validPublishedEntry(e) || e.SizeBytes > publishedresult.MaxTotalBytes-total {
			return publishedresult.ErrUnavailable
		}
		total += e.SizeBytes
		mode := uint32(0644)
		if e.ModeBits&0111 != 0 {
			mode = 0755
		}
		result.Files = append(result.Files, publishedresult.ResultFile{Path: e.Path, Mode: mode, Size: e.SizeBytes, SHA256: e.BlobID})
		return nil
	})
	if err != nil {
		return publishedresult.ResultSnapshot{}, publishedresult.ErrUnavailable
	}
	current, err := s.publishedSnapshot(ctx, run)
	if err != nil || current != snapshot {
		return publishedresult.ResultSnapshot{}, publishedresult.ErrUnavailable
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return publishedresult.ResultSnapshot{}, err
	}
	sum := sha256.Sum256(raw)
	result.ManifestSHA256 = hex.EncodeToString(sum[:])
	return result, nil
}
func (s *Store) Open(ctx context.Context, run, snapshot, path string) (io.ReadCloser, error) {
	current, err := s.publishedSnapshot(ctx, run)
	if err != nil || current != snapshot || !fs.ValidPath(path) || path == "." {
		return nil, publishedresult.ErrUnavailable
	}
	lineage, err := s.snapshotLineage(ctx, snapshot)
	if err != nil {
		return nil, publishedresult.ErrUnavailable
	}
	var entry SnapshotEntry
	found := false
	// Resolve one path along the bounded lineage. Rebuilding the full manifest
	// for each file would make repository publication quadratic in file count.
	for _, id := range lineage {
		candidate, e := scanSnapshotEntry(s.db.QueryRowContext(ctx, `SELECT `+entryColumns+` FROM filesystem_snapshot_entries WHERE snapshot_id=? AND path=?`, id, path))
		if e == sql.ErrNoRows {
			continue
		}
		if e != nil {
			return nil, publishedresult.ErrUnavailable
		}
		entry, found = candidate, true
		break
	}
	if !found || !validPublishedEntry(entry) {
		return nil, publishedresult.ErrUnavailable
	}
	current, err = s.publishedSnapshot(ctx, run)
	if err != nil || current != snapshot {
		return nil, publishedresult.ErrUnavailable
	}
	return providerexecution.OpenContentCopy(ctx, func(ctx context.Context, w io.Writer) error {
		n, err := s.CopyBlob(ctx, entry.BlobID, w)
		if err != nil {
			return err
		}
		if n != entry.SizeBytes {
			return publishedresult.ErrUnavailable
		}
		return nil
	}), nil
}

func validPublishedEntry(e SnapshotEntry) bool {
	return e.EntryType == "file" && fs.ValidPath(e.Path) && e.Path != "." && len(e.Path) <= 4096 && !trackedfs.IsProtectedRelativePath(strings.ToLower(e.Path)) && e.SizeBytes >= 0 && e.SizeBytes <= publishedresult.MaxFileBytes && e.ModeBits >= 0 && e.ModeBits <= 0777 && validateSnapshotEntry(e) == nil
}
