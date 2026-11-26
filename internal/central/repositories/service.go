package repositories

import (
	"context"
	"database/sql"
	"encoding/hex"
	"io"
	"os"

	"github.com/google/uuid"
	"kingdom/internal/central/catalog"
	"kingdom/internal/central/mutation"
	"kingdom/internal/central/sources"
)

type SourceImporter interface {
	ReplayRepository(context.Context, string, string, string, int64, string) (sources.Metadata, bool, error)
	ImportRepository(context.Context, string, string, string, sources.RepositoryOrigin, io.Reader) (sources.Metadata, error)
}
type Service struct {
	db      *sql.DB
	catalog *catalog.Store
	client  *Client
	sources SourceImporter
	slots   chan struct{}
	results ResultSource
}
type RepositoryStatus struct {
	Available    bool  `json:"available"`
	RepositoryID int64 `json:"repository_id,omitempty"`
	CanPublish   bool  `json:"can_publish"`
}

func New(ctx context.Context, db *sql.DB, authority *catalog.Store, client *Client, source SourceImporter, results ResultSource) (*Service, error) {
	if db == nil || authority == nil || client == nil || source == nil {
		return nil, ErrInvalid
	}
	s := &Service{db: db, catalog: authority, client: client, sources: source, results: results, slots: make(chan struct{}, 4)}
	tx, err := s.write(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS central_repository_schema(slot INTEGER PRIMARY KEY CHECK(slot=1),version INTEGER NOT NULL);
 INSERT INTO central_repository_schema VALUES(1,1) ON CONFLICT DO NOTHING;
 CREATE TABLE IF NOT EXISTS central_repository_imports(actor TEXT NOT NULL,request_key TEXT NOT NULL,id TEXT NOT NULL UNIQUE,project TEXT NOT NULL REFERENCES central_projects(id),fingerprint TEXT NOT NULL,commit_sha TEXT NOT NULL DEFAULT '',PRIMARY KEY(actor,request_key));
 CREATE TRIGGER IF NOT EXISTS central_repository_import_immutable BEFORE UPDATE ON central_repository_imports WHEN NEW.actor!=OLD.actor OR NEW.request_key!=OLD.request_key OR NEW.id!=OLD.id OR NEW.project!=OLD.project OR NEW.fingerprint!=OLD.fingerprint OR (OLD.commit_sha!='' AND NEW.commit_sha!=OLD.commit_sha) BEGIN SELECT RAISE(ABORT,'repository import is immutable'); END;
 CREATE TRIGGER IF NOT EXISTS central_repository_import_retained BEFORE DELETE ON central_repository_imports BEGIN SELECT RAISE(ABORT,'repository import is retained'); END;`)
	if err != nil {
		return nil, err
	}
	var version int
	if err = tx.QueryRowContext(ctx, `SELECT version FROM central_repository_schema WHERE slot=1`).Scan(&version); err != nil || (version != 1 && version != 2) {
		return nil, ErrInvalid
	}
	if err = publicationSchema(ctx, tx); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE central_repository_schema SET version=2 WHERE slot=1 AND version=1`); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return s, nil
}
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
func (s *Service) Status(ctx context.Context, actor, project string) (RepositoryStatus, error) {
	if err := s.catalog.Authorize(ctx, actor, project, catalog.UseProject); err != nil {
		return RepositoryStatus{}, err
	}
	g, ok := s.client.Grant(project)
	return RepositoryStatus{ok, g.RepositoryID, g.Publish && s.catalog.Authorize(ctx, actor, project, catalog.ManageProject) == nil}, nil
}
func (s *Service) importOperation(ctx context.Context, actor, project, key, ref, commit string) (id, pinned string, err error) {
	g, ok := s.client.Grant(project)
	if !ok {
		return "", "", ErrDenied
	}
	fingerprint, err := mutation.Fingerprint(struct {
		Grant Grant
		Ref   string
	}{g, ref})
	if err != nil {
		return "", "", err
	}
	tx, err := s.write(ctx)
	if err != nil {
		return "", "", err
	}
	defer tx.Rollback()
	if err = catalog.AuthorizeTx(ctx, tx, actor, project, catalog.ManageProject); err != nil {
		return "", "", err
	}
	var retained string
	err = tx.QueryRowContext(ctx, `SELECT id,commit_sha,fingerprint FROM central_repository_imports WHERE actor=? AND request_key=?`, actor, key).Scan(&id, &pinned, &retained)
	if err == sql.ErrNoRows {
		id = uuid.NewString()
		_, err = tx.ExecContext(ctx, `INSERT INTO central_repository_imports(actor,request_key,id,project,fingerprint)VALUES(?,?,?,?,?)`, actor, key, id, project, hex.EncodeToString(fingerprint[:]))
	} else if err == nil && retained != hex.EncodeToString(fingerprint[:]) {
		return "", "", mutation.ErrConflict
	}
	if err != nil {
		return "", "", err
	}
	if pinned == "" && commit != "" {
		if !gitSHA.MatchString(commit) {
			return "", "", ErrInvalid
		}
		_, err = tx.ExecContext(ctx, `UPDATE central_repository_imports SET commit_sha=? WHERE id=? AND commit_sha=''`, commit, id)
		if err != nil {
			return "", "", err
		}
		pinned = commit
	}
	if err = tx.Commit(); err != nil {
		return "", "", ErrUnavailable
	}
	return id, pinned, nil
}
func (s *Service) Import(ctx context.Context, actor, project, key, ref string) (sources.Metadata, error) {
	if !mutation.ValidKey(key) || !validRef(ref) {
		return sources.Metadata{}, ErrInvalid
	}
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	case <-ctx.Done():
		return sources.Metadata{}, ctx.Err()
	}
	id, commit, err := s.importOperation(ctx, actor, project, key, ref, "")
	if err != nil {
		return sources.Metadata{}, err
	}
	if commit == "" {
		resolved, err := s.client.resolveRef(ctx, project, ref)
		if err != nil {
			return sources.Metadata{}, err
		}
		id, commit, err = s.importOperation(ctx, actor, project, key, ref, resolved)
		if err != nil {
			return sources.Metadata{}, err
		}
	}
	grant, _ := s.client.Grant(project)
	if result, found, err := s.sources.ReplayRepository(ctx, actor, project, id, grant.RepositoryID, commit); err != nil || found {
		return result, err
	}
	file, err := os.CreateTemp("", "kingdom-repository-import-")
	if err != nil {
		return sources.Metadata{}, ErrUnavailable
	}
	defer os.Remove(file.Name())
	defer file.Close()
	bounded := &archiveWriter{dst: file, remaining: sources.MaxUploadBytes}
	proof, err := s.client.ReadArchive(ctx, project, commit, bounded)
	if err != nil {
		return sources.Metadata{}, err
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		return sources.Metadata{}, ErrUnavailable
	}
	// SourceImporter repeats current project-owner authorization in its writer
	// transaction and atomically excludes the permanent first-start marker.
	return s.sources.ImportRepository(ctx, actor, project, id, sources.RepositoryOrigin{RepositoryID: proof.RepositoryID, CommitSHA: proof.CommitSHA, TreeSHA: proof.TreeSHA}, file)
}

type archiveWriter struct {
	dst       io.Writer
	remaining int64
}

func (w *archiveWriter) Write(data []byte) (int, error) {
	if int64(len(data)) > w.remaining {
		return 0, ErrInvalid
	}
	n, err := w.dst.Write(data)
	w.remaining -= int64(n)
	return n, err
}
func (c *Client) resolveRef(ctx context.Context, project, ref string) (string, error) {
	if !validRef(ref) {
		return "", ErrInvalid
	}
	a, err := c.authorize(ctx, project, false)
	if err != nil {
		return "", err
	}
	var result struct {
		SHA string `json:"sha"`
	}
	if err = c.request(ctx, "GET", a.path("/commits/"+ref), a.token, nil, maxMetadataBytes, &result); err != nil {
		return "", err
	}
	if !gitSHA.MatchString(result.SHA) || (gitSHA.MatchString(ref) && ref != result.SHA) {
		return "", ErrUnavailable
	}
	return result.SHA, nil
}
