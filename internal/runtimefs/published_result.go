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

type publicationSnapshots struct{ source, published string }

// publicationSelection accepts only a completed run whose trusted result has
// reached its publication target. Partial or dirty attempts grant no access.
func (s *Store) publicationSelection(ctx context.Context, run string) (publicationSnapshots, error) {
	if s == nil || s.db == nil || run == "" || strings.TrimSpace(run) != run {
		return publicationSnapshots{}, publishedresult.ErrUnavailable
	}
	var selection publicationSnapshots
	err := s.db.QueryRowContext(ctx, `SELECT t.source_snapshot_id,t.published_snapshot_id FROM workflow_runs r
 JOIN run_execution_targets t ON t.run_id=r.run_id
 JOIN filesystem_snapshots f ON f.snapshot_id=t.published_snapshot_id AND f.run_id=r.run_id
 JOIN filesystem_snapshots b ON b.snapshot_id=t.source_snapshot_id AND b.run_id=r.run_id
 WHERE r.run_id=? AND r.run_status='succeeded' AND t.state='trusted'
 AND t.published_snapshot_id!='' AND t.published_snapshot_id=t.trusted_snapshot_id`, run).Scan(&selection.source, &selection.published)
	if err != nil {
		return publicationSnapshots{}, publishedresult.ErrUnavailable
	}
	return selection, nil
}
func (s *Store) Resolve(ctx context.Context, run string) (publishedresult.ResultSnapshot, error) {
	selection, err := s.publicationSelection(ctx, run)
	if err != nil {
		return publishedresult.ResultSnapshot{}, err
	}
	if _, err = s.publicationLineage(ctx, run, selection.source); err != nil {
		return publishedresult.ResultSnapshot{}, err
	}
	if _, err = s.publicationLineage(ctx, run, selection.published); err != nil {
		return publishedresult.ResultSnapshot{}, err
	}
	before, err := s.OpenSnapshotManifest(ctx, selection.source)
	if err != nil {
		return publishedresult.ResultSnapshot{}, publishedresult.ErrUnavailable
	}
	defer before.Close()
	after, err := s.OpenSnapshotManifest(ctx, selection.published)
	if err != nil {
		return publishedresult.ResultSnapshot{}, publishedresult.ErrUnavailable
	}
	defer after.Close()
	result := publishedresult.ResultSnapshot{RunID: run, SnapshotID: selection.published, SourceSnapshotID: selection.source, Files: []publishedresult.ResultFile{}, Changes: []publishedresult.ResultChange{}}
	validate := func(manifest *SnapshotManifest, collect bool) error {
		var total int64
		count := 0
		return manifest.Visit(ctx, func(e SnapshotEntry) error {
			count++
			if count > publishedresult.MaxFiles || !validPublishedEntry(e) || e.SizeBytes > publishedresult.MaxTotalBytes-total {
				return publishedresult.ErrUnavailable
			}
			total += e.SizeBytes
			if collect {
				result.Files = append(result.Files, publishedFile(e))
			}
			return nil
		})
	}
	if err = validate(before, false); err != nil {
		return publishedresult.ResultSnapshot{}, publishedresult.ErrUnavailable
	}
	if err = validate(after, true); err != nil {
		return publishedresult.ResultSnapshot{}, publishedresult.ErrUnavailable
	}
	// The canonical diff includes timestamp changes. Git tracks content and the
	// executable bit, so omit changes to timestamps or other permission bits.
	err = s.VisitSnapshotDiff(ctx, selection.source, selection.published, func(diff SnapshotDiffEntry) error {
		old, oldFound, e := before.Lookup(ctx, diff.Path)
		if e != nil {
			return e
		}
		next, nextFound, e := after.Lookup(ctx, diff.Path)
		if e != nil {
			return e
		}
		if oldFound && nextFound && old.BlobID == next.BlobID && old.SizeBytes == next.SizeBytes && (old.ModeBits&0111 != 0) == (next.ModeBits&0111 != 0) {
			return nil
		}
		change := publishedresult.ResultChange{Path: diff.Path}
		if oldFound {
			value := publishedFile(old)
			change.Before = &value
		}
		if nextFound {
			value := publishedFile(next)
			change.After = &value
		}
		if change.Before == nil && change.After == nil {
			return publishedresult.ErrUnavailable
		}
		if len(result.Changes) >= 2*publishedresult.MaxFiles {
			return publishedresult.ErrUnavailable
		}
		result.Changes = append(result.Changes, change)
		return nil
	})
	if err != nil {
		return publishedresult.ResultSnapshot{}, publishedresult.ErrUnavailable
	}
	current, err := s.publicationSelection(ctx, run)
	if err != nil || current != selection {
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
	selection, err := s.publicationSelection(ctx, run)
	if err != nil || (snapshot != selection.published && snapshot != selection.source) || !fs.ValidPath(path) || path == "." {
		return nil, publishedresult.ErrUnavailable
	}
	lineage, err := s.publicationLineage(ctx, run, snapshot)
	if err != nil {
		return nil, publishedresult.ErrUnavailable
	}
	var entry SnapshotEntry
	found := false
	// Resolve one path along bounded lineage instead of rebuilding the entire
	// manifest for each file in a repository publication.
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
	current, err := s.publicationSelection(ctx, run)
	if err != nil || current != selection {
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

func (s *Store) publicationLineage(ctx context.Context, run, snapshot string) ([]string, error) {
	lineage, err := s.snapshotLineage(ctx, snapshot)
	if err != nil {
		return nil, publishedresult.ErrUnavailable
	}
	for _, id := range lineage {
		var owner string
		if err := s.db.QueryRowContext(ctx, `SELECT run_id FROM filesystem_snapshots WHERE snapshot_id=?`, id).Scan(&owner); err != nil || owner != run {
			return nil, publishedresult.ErrUnavailable
		}
	}
	return lineage, nil
}
func publishedFile(e SnapshotEntry) publishedresult.ResultFile {
	mode := uint32(0644)
	if e.ModeBits&0111 != 0 {
		mode = 0755
	}
	return publishedresult.ResultFile{Path: e.Path, Mode: mode, Size: e.SizeBytes, SHA256: e.BlobID}
}
func validPublishedEntry(e SnapshotEntry) bool {
	return e.EntryType == "file" && fs.ValidPath(e.Path) && e.Path != "." && len(e.Path) <= 4096 && !trackedfs.IsProtectedRelativePath(strings.ToLower(e.Path)) && e.SizeBytes >= 0 && e.SizeBytes <= publishedresult.MaxFileBytes && e.ModeBits >= 0 && e.ModeBits <= 0777 && validateSnapshotEntry(e) == nil
}
