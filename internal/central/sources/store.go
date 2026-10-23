// Package sources owns immutable initial project input and the permanent first-start fence.
package sources

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"

	"kingdom/internal/central/catalog"
	"kingdom/internal/central/mutation"
	"kingdom/internal/securefs"
	"kingdom/internal/workspacearchive"
)

var ErrUnavailable = errors.New("project input unavailable")
var ErrConflict = errors.New("initial project input is already fixed")
var ErrInvalid = errors.New("invalid project input")

type Metadata struct {
	ID     string `json:"id"`
	Kind   string `json:"kind"`
	Files  int    `json:"files"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}
type Status struct {
	CanUpload bool      `json:"can_upload"`
	State     string    `json:"state"`
	Source    *Metadata `json:"source,omitempty"`
}
type Service struct {
	db      *sql.DB
	catalog *catalog.Store
	path    string
	root    *securefs.Root
	prepare sync.Mutex
	slots   chan struct{}
}

func New(ctx context.Context, db *sql.DB, authority *catalog.Store, path string) (*Service, error) {
	if db == nil || authority == nil || !filepath.IsAbs(path) {
		return nil, ErrInvalid
	}
	if err := os.MkdirAll(path, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		return nil, ErrInvalid
	}
	root, err := securefs.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	s := &Service{db: db, catalog: authority, path: path, root: root, slots: make(chan struct{}, 4)}
	tx, err := s.write(ctx)
	if err != nil {
		root.Close()
		return nil, err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS central_source_schema(slot INTEGER PRIMARY KEY CHECK(slot=1),version INTEGER NOT NULL);
 INSERT INTO central_source_schema VALUES(1,1) ON CONFLICT DO NOTHING;
 CREATE TABLE IF NOT EXISTS central_project_sources(project TEXT PRIMARY KEY REFERENCES central_projects(id),metadata BLOB NOT NULL,stage TEXT NOT NULL,manifest BLOB NOT NULL);
 CREATE TRIGGER IF NOT EXISTS central_source_immutable BEFORE UPDATE ON central_project_sources BEGIN SELECT RAISE(ABORT,'source is immutable'); END;
 CREATE TRIGGER IF NOT EXISTS central_source_retained BEFORE DELETE ON central_project_sources BEGIN SELECT RAISE(ABORT,'source is retained'); END;
 CREATE TABLE IF NOT EXISTS central_project_starts(project TEXT PRIMARY KEY REFERENCES central_projects(id),device INTEGER NOT NULL,inode INTEGER NOT NULL,ready INTEGER NOT NULL CHECK(ready IN(0,1)));
 CREATE TRIGGER IF NOT EXISTS central_start_identity BEFORE UPDATE ON central_project_starts WHEN NEW.project!=OLD.project OR NEW.device!=OLD.device OR NEW.inode!=OLD.inode OR NEW.ready<OLD.ready BEGIN SELECT RAISE(ABORT,'startup identity is immutable'); END;
 CREATE TRIGGER IF NOT EXISTS central_start_retained BEFORE DELETE ON central_project_starts BEGIN SELECT RAISE(ABORT,'startup fence is retained'); END;`)
	if err == nil {
		var version int
		err = tx.QueryRowContext(ctx, `SELECT version FROM central_source_schema WHERE slot=1`).Scan(&version)
		if err == nil && version != 1 {
			err = ErrInvalid
		}
	}
	if err == nil {
		err = mutation.Initialize(ctx, tx)
	}
	if err == nil {
		err = tx.Commit()
	}
	if err != nil {
		root.Close()
		return nil, err
	}
	return s, nil
}
func (s *Service) Close() error { return s.root.Close() }
func (s *Service) write(ctx context.Context) (*sql.Tx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE central_bootstrap SET slot=slot WHERE slot=1`); err != nil {
		tx.Rollback()
		return nil, err
	}
	return tx, nil
}
func (s *Service) Status(ctx context.Context, actor, project string) (Status, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Status{}, err
	}
	defer tx.Rollback()
	if err = catalog.AuthorizeTx(ctx, tx, actor, project, catalog.UseProject); err != nil {
		return Status{}, err
	}
	result := Status{State: "empty"}
	var raw []byte
	err = tx.QueryRowContext(ctx, `SELECT metadata FROM central_project_sources WHERE project=?`, project).Scan(&raw)
	if err == nil {
		var m Metadata
		if json.Unmarshal(raw, &m) != nil {
			return Status{}, ErrUnavailable
		}
		result.Source = &m
		result.State = "imported"
	} else if !errors.Is(err, sql.ErrNoRows) {
		return Status{}, err
	}
	var count int
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM central_project_starts WHERE project=?`, project).Scan(&count); err != nil {
		return Status{}, err
	}
	if count > 0 {
		result.State = "started"
	}
	result.CanUpload = result.State == "empty" && catalog.AuthorizeTx(ctx, tx, actor, project, catalog.ManageProject) == nil
	return result, nil
}
func readSource(ctx context.Context, tx *sql.Tx, project string) (string, workspacearchive.Manifest, error) {
	var stage string
	var raw []byte
	err := tx.QueryRowContext(ctx, `SELECT stage,manifest FROM central_project_sources WHERE project=?`, project).Scan(&stage, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return "", workspacearchive.Manifest{}, nil
	}
	if err != nil {
		return "", workspacearchive.Manifest{}, err
	}
	var manifest workspacearchive.Manifest
	if json.Unmarshal(raw, &manifest) != nil {
		return "", manifest, ErrUnavailable
	}
	return stage, manifest, nil
}
