package runtimefs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"

	"kingdom/internal/trackedfs"
	_ "modernc.org/sqlite"
)

const metadataPageSize = 128
const entryColumns = "path, entry_type, blob_id, size_bytes, mode_bits, mtime_ns"

// workspaceIndex is disposable, private disk-backed working state, never a
// publication authority. Its fixed SQLite cache and file-backed sorter keep
// memory independent of workspace size. One scratch transaction avoids fsync
// per entry; the authoritative database still publishes in one durable commit.
type workspaceIndex struct {
	db                           *sql.DB
	tx                           *sql.Tx
	path                         string
	putStmt, getStmt, deleteStmt *sql.Stmt
}

func (s *Store) newWorkspaceIndex(ctx context.Context) (_ *workspaceIndex, err error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("runtime fs store is required")
	}
	file, err := os.CreateTemp(s.tempRoot, "workspace-*.sqlite")
	if err != nil {
		return nil, err
	}
	index := &workspaceIndex{path: file.Name()}
	defer func() {
		if err != nil {
			index.Close()
		}
	}()
	if err = file.Close(); err != nil {
		return nil, err
	}
	index.db, err = sql.Open("sqlite", (&url.URL{Scheme: "file", Path: index.path}).String())
	if err != nil {
		return nil, err
	}
	index.db.SetMaxOpenConns(1)
	_, err = index.db.ExecContext(ctx, `PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA mmap_size=0; PRAGMA cache_size=-1024; PRAGMA temp_store=FILE;
CREATE TABLE entries (space INTEGER NOT NULL, path TEXT NOT NULL, entry_type TEXT NOT NULL, blob_id TEXT NOT NULL, size_bytes INTEGER NOT NULL, mode_bits INTEGER NOT NULL, mtime_ns INTEGER NOT NULL, PRIMARY KEY(space, path)) WITHOUT ROWID;
CREATE INDEX entries_live_paths ON entries(space, path) WHERE entry_type = 'file'`)
	if err != nil {
		return nil, err
	}
	index.tx, err = index.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	index.putStmt, err = index.tx.PrepareContext(ctx, `INSERT OR REPLACE INTO entries VALUES (?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return nil, err
	}
	index.getStmt, err = index.tx.PrepareContext(ctx, `SELECT `+entryColumns+` FROM entries WHERE space = ? AND path = ?`)
	if err != nil {
		return nil, err
	}
	index.deleteStmt, err = index.tx.PrepareContext(ctx, `DELETE FROM entries WHERE space = ? AND path = ?`)
	if err != nil {
		return nil, err
	}
	return index, nil
}

func (i *workspaceIndex) Close() {
	for _, stmt := range []*sql.Stmt{i.putStmt, i.getStmt, i.deleteStmt} {
		if stmt != nil {
			_ = stmt.Close()
		}
	}
	if i.tx != nil {
		_ = i.tx.Rollback()
	}
	if i.db != nil {
		_ = i.db.Close()
	}
	_ = os.Remove(i.path)
}

type entryScanner interface{ Scan(...any) error }

func scanSnapshotEntry(row entryScanner) (SnapshotEntry, error) {
	var e SnapshotEntry
	err := row.Scan(&e.Path, &e.EntryType, &e.BlobID, &e.SizeBytes, &e.ModeBits, &e.ModifiedNS)
	return e, err
}

func (i *workspaceIndex) put(ctx context.Context, space int, e SnapshotEntry) error {
	_, err := i.putStmt.ExecContext(ctx, space, e.Path, e.EntryType, e.BlobID, e.SizeBytes, e.ModeBits, e.ModifiedNS)
	return err
}
func (i *workspaceIndex) get(ctx context.Context, space int, path string) (SnapshotEntry, bool, error) {
	e, err := scanSnapshotEntry(i.getStmt.QueryRowContext(ctx, space, path))
	if errors.Is(err, sql.ErrNoRows) {
		return SnapshotEntry{}, false, nil
	}
	return e, err == nil, err
}
func (i *workspaceIndex) remove(ctx context.Context, space int, path string) error {
	_, err := i.deleteStmt.ExecContext(ctx, space, path)
	return err
}

// hasLiveDescendant distinguishes an obsolete file tombstone from a required
// parent directory. The range uses '/' and its next byte, '0', so literal path
// names (including SQL wildcard characters) need no escaping or full-tree scan.
func (i *workspaceIndex) hasLiveDescendant(ctx context.Context, space int, path string) (bool, error) {
	var found bool
	err := i.tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM entries
WHERE space = ? AND entry_type = 'file' AND path >= ? AND path < ?)`, space, path+"/", path+"0").Scan(&found)
	return found, err
}

// visit releases each cursor before invoking callers, which may perform indexed
// lookups or writes on the same single-connection staging database.
func (i *workspaceIndex) visit(ctx context.Context, space int, visit func(SnapshotEntry) error) error {
	after := ""
	for {
		rows, err := i.tx.QueryContext(ctx, `SELECT `+entryColumns+` FROM entries WHERE space = ? AND path > ? ORDER BY path LIMIT ?`, space, after, metadataPageSize)
		if err != nil {
			return err
		}
		page, err := readEntryPage(rows)
		if err != nil {
			return err
		}
		if len(page) == 0 {
			return nil
		}
		for _, e := range page {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := visit(e); err != nil {
				return err
			}
		}
		after = page[len(page)-1].Path
	}
}

func readEntryPage(rows *sql.Rows) ([]SnapshotEntry, error) {
	defer rows.Close()
	page := make([]SnapshotEntry, 0, metadataPageSize)
	for rows.Next() {
		e, err := scanSnapshotEntry(rows)
		if err != nil {
			return nil, err
		}
		page = append(page, e)
	}
	return page, rows.Err()
}

func (s *Store) loadSnapshotIndex(ctx context.Context, index *workspaceIndex, space int, snapshotID string) (int, error) {
	lineage, err := s.snapshotLineage(ctx, snapshotID)
	if err != nil {
		return 0, err
	}
	for n := len(lineage) - 1; n >= 0; n-- {
		after := ""
		comparison := ">=" // Include malformed empty paths on the first page so validation rejects them.
		for {
			rows, err := s.db.QueryContext(ctx, `SELECT `+entryColumns+` FROM filesystem_snapshot_entries WHERE snapshot_id = ? AND path `+comparison+` ? ORDER BY path LIMIT ?`, lineage[n], after, metadataPageSize)
			if err != nil {
				return 0, fmt.Errorf("load snapshot entries: %w", err)
			}
			page, err := readEntryPage(rows)
			if err != nil {
				return 0, err
			}
			if len(page) == 0 {
				break
			}
			for _, e := range page {
				if trackedfs.IsProtectedRelativePath(e.Path) {
					continue
				}
				if err := validateSnapshotEntry(e); err != nil {
					return 0, err
				}
				if err := index.put(ctx, space, e); err != nil {
					return 0, err
				}
			}
			after = page[len(page)-1].Path
			comparison = ">"
		}
	}
	return len(lineage), nil
}

// VisitSnapshotManifest visits resolved regular-file metadata in path order.
// Pages and callbacks never retain the whole manifest or a main-DB connection.
func (s *Store) VisitSnapshotManifest(ctx context.Context, snapshotID string, visit func(SnapshotEntry) error) error {
	manifest, err := s.OpenSnapshotManifest(ctx, snapshotID)
	if err != nil {
		return err
	}
	defer manifest.Close()
	return manifest.Visit(ctx, visit)
}

// SnapshotManifest owns an operation-local indexed disk view. Close it when done.
type SnapshotManifest struct{ index *workspaceIndex }

func (s *Store) OpenSnapshotManifest(ctx context.Context, snapshotID string) (*SnapshotManifest, error) {
	index, err := s.newWorkspaceIndex(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := s.loadSnapshotIndex(ctx, index, 0, snapshotID); err != nil {
		index.Close()
		return nil, err
	}
	return &SnapshotManifest{index: index}, nil
}
func (m *SnapshotManifest) Close() { m.index.Close() }
func (m *SnapshotManifest) Lookup(ctx context.Context, path string) (SnapshotEntry, bool, error) {
	e, found, err := m.index.get(ctx, 0, path)
	return e, found && e.EntryType == "file", err
}

// HasLiveDescendant reports whether the resolved snapshot requires path to be
// a directory. It performs an indexed literal-prefix lookup, not a tree scan.
func (m *SnapshotManifest) HasLiveDescendant(ctx context.Context, path string) (bool, error) {
	return m.index.hasLiveDescendant(ctx, 0, path)
}

func (m *SnapshotManifest) Visit(ctx context.Context, visit func(SnapshotEntry) error) error {
	return m.index.visit(ctx, 0, func(e SnapshotEntry) error {
		if e.EntryType == "delete" {
			return nil
		}
		return visit(e)
	})
}

// VisitSnapshotDiff streams differences in path order. Indexed joins and
// private disk staging replace both whole-manifest maps and the diff slice.
func (s *Store) VisitSnapshotDiff(ctx context.Context, left, right string, visit func(SnapshotDiffEntry) error) error {
	index, err := s.newWorkspaceIndex(ctx)
	if err != nil {
		return err
	}
	defer index.Close()
	if _, err := s.loadSnapshotIndex(ctx, index, 0, left); err != nil {
		return err
	}
	if _, err := s.loadSnapshotIndex(ctx, index, 1, right); err != nil {
		return err
	}
	_, err = index.tx.ExecContext(ctx, `INSERT INTO entries
SELECT 2, r.path, r.entry_type, r.blob_id, r.size_bytes, r.mode_bits, r.mtime_ns
FROM entries r LEFT JOIN entries l ON l.space = 0 AND l.path = r.path
WHERE r.space = 1 AND r.entry_type = 'file' AND
(l.path IS NULL OR l.entry_type = 'delete' OR l.blob_id <> r.blob_id OR l.size_bytes <> r.size_bytes OR l.mode_bits <> r.mode_bits OR l.mtime_ns <> r.mtime_ns);
INSERT INTO entries
SELECT 2, l.path, 'delete', '', 0, 0, 0
FROM entries l LEFT JOIN entries r ON r.space = 1 AND r.path = l.path
WHERE l.space = 0 AND l.entry_type = 'file' AND (r.path IS NULL OR r.entry_type = 'delete')`)
	if err != nil {
		return err
	}
	return index.visit(ctx, 2, func(e SnapshotEntry) error { return visit(SnapshotDiffEntry(e)) })
}
