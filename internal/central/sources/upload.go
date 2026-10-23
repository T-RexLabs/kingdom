package sources

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"regexp"

	"github.com/google/uuid"
	"kingdom/internal/central/catalog"
	"kingdom/internal/central/mutation"
	"kingdom/internal/securefs"
	"kingdom/internal/workspacearchive"
	"kingdom/internal/workspacetransfer"
)

const MaxUploadBytes int64 = 128 << 20

func (s *Service) Upload(ctx context.Context, actor, project, key, kind, name string, input io.Reader) (Metadata, error) {
	return s.upload(ctx, actor, project, key, kind, name, nil, input)
}

// ImportRepository accepts only trusted repository-client provenance. HTTP upload
// routes cannot supply this metadata.
func (s *Service) ImportRepository(ctx context.Context, actor, project, key string, origin RepositoryOrigin, input io.Reader) (Metadata, error) {
	if origin.RepositoryID <= 0 || !regexp.MustCompile(`^[a-f0-9]{40}$`).MatchString(origin.CommitSHA) || !regexp.MustCompile(`^[a-f0-9]{40}$`).MatchString(origin.TreeSHA) {
		return Metadata{}, ErrInvalid
	}
	return s.upload(ctx, actor, project, key, "github", "", &origin, input)
}

func (s *Service) upload(ctx context.Context, actor, project, key, kind, name string, origin *RepositoryOrigin, input io.Reader) (Metadata, error) {
	if !mutation.ValidKey(key) || (kind != "zip" && kind != "file" && (kind != "github" || origin == nil)) || input == nil {
		return Metadata{}, ErrInvalid
	}
	if err := s.catalog.Authorize(ctx, actor, project, catalog.ManageProject); err != nil {
		return Metadata{}, err
	}
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	case <-ctx.Done():
		return Metadata{}, ctx.Err()
	}
	file, err := os.CreateTemp(s.path, "upload-")
	if err != nil {
		return Metadata{}, err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	hash := sha256.New()
	var count int64
	if kind == "file" {
		if name == "" {
			return Metadata{}, ErrInvalid
		}
		writer := zip.NewWriter(file)
		header := &zip.FileHeader{Name: name, Method: zip.Store}
		header.SetMode(0644)
		target, e := writer.CreateHeader(header)
		if e != nil {
			return Metadata{}, ErrInvalid
		}
		count, err = securefs.Copy(ctx, io.MultiWriter(target, hash), io.LimitReader(input, (64<<20)+1))
		if err != nil || count > 64<<20 {
			return Metadata{}, ErrInvalid
		}
		if err = writer.Close(); err != nil {
			return Metadata{}, err
		}
	} else {
		count, err = securefs.Copy(ctx, io.MultiWriter(file, hash), io.LimitReader(input, MaxUploadBytes+1))
		if err != nil || count > MaxUploadBytes {
			return Metadata{}, ErrInvalid
		}
	}
	info, err := file.Stat()
	if err != nil {
		return Metadata{}, err
	}
	stage, err := workspacearchive.ImportZIP(ctx, s.path, file, info.Size(), workspacetransfer.DefaultLimits())
	if err != nil {
		return Metadata{}, ErrInvalid
	}
	retain := false
	defer func() {
		if !retain {
			stage.Close()
		}
	}()
	relative, err := filepath.Rel(s.path, stage.Path)
	if err != nil {
		return Metadata{}, err
	}
	// Sync all verified input bytes before publishing their reference in SQLite.
	for _, entry := range stage.Manifest.Entries {
		if entry.Kind != "file" {
			continue
		}
		content, e := s.root.OpenRegular(filepath.Join(relative, entry.Path))
		if e != nil {
			return Metadata{}, e
		}
		e = content.Sync()
		content.Close()
		if e != nil {
			return Metadata{}, e
		}
	}
	if err = s.root.SyncDir(relative); err != nil {
		return Metadata{}, err
	}
	if err = s.root.SyncDir(filepath.Dir(relative)); err != nil {
		return Metadata{}, err
	}
	if err = s.root.SyncDir("."); err != nil {
		return Metadata{}, err
	}
	fingerprint, err := mutation.Fingerprint(struct{ Kind, Name, Digest string }{kind, name, hex.EncodeToString(hash.Sum(nil))})
	if origin != nil {
		fingerprint, err = mutation.Fingerprint(struct {
			Kind, Name, Digest string
			Origin             *RepositoryOrigin
		}{kind, name, hex.EncodeToString(hash.Sum(nil)), origin})
	}

	if err != nil {
		return Metadata{}, err
	}
	tx, err := s.write(ctx)
	if err != nil {
		return Metadata{}, err
	}
	defer tx.Rollback()
	if err = catalog.AuthorizeTx(ctx, tx, actor, project, catalog.ManageProject); err != nil {
		return Metadata{}, err
	}
	if result, found, e := mutation.Replay[Metadata](ctx, tx, actor, key, "project.source", project, fingerprint); e != nil || found {
		return result, e
	}
	var fixed int
	if err = tx.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM central_project_starts WHERE project=?)+(SELECT count(*) FROM central_project_sources WHERE project=?)`, project, project).Scan(&fixed); err != nil {
		return Metadata{}, err
	}
	if fixed != 0 {
		return Metadata{}, ErrConflict
	}
	result := Metadata{Repository: origin, ID: uuid.NewString(), Kind: kind, Bytes: stage.Manifest.ExpandedBytes, SHA256: stage.Digest}
	for _, entry := range stage.Manifest.Entries {
		if entry.Kind == "file" {
			result.Files++
		}
	}
	raw, _ := json.Marshal(result)
	manifest, _ := json.Marshal(stage.Manifest)
	if _, err = tx.ExecContext(ctx, `INSERT INTO central_project_sources VALUES(?,?,?,?)`, project, raw, relative, manifest); err != nil {
		return Metadata{}, err
	}
	if err = mutation.Record(ctx, tx, actor, key, "project.source", project, fingerprint, result); err != nil {
		return Metadata{}, err
	}
	// A failed Commit may have persisted. Retain immutable bytes for reconciliation.
	retain = true
	if err = tx.Commit(); err != nil {
		return Metadata{}, ErrUnavailable
	}
	return result, nil
}

// ReplayRepository resolves only the exact trusted import operation's committed
// source. It does not accept an unrelated source or bypass current ownership.
func (s *Service) ReplayRepository(ctx context.Context, actor, project, key string, repositoryID int64, commit string) (Metadata, bool, error) {
	if !mutation.ValidKey(key) || repositoryID <= 0 {
		return Metadata{}, false, ErrInvalid
	}
	tx, err := s.write(ctx)
	if err != nil {
		return Metadata{}, false, err
	}
	defer tx.Rollback()
	if err = catalog.AuthorizeTx(ctx, tx, actor, project, catalog.ManageProject); err != nil {
		return Metadata{}, false, err
	}
	var raw []byte
	err = tx.QueryRowContext(ctx, `SELECT m.response FROM central_mutations m JOIN central_project_sources s ON s.project=m.resource AND s.metadata=m.response WHERE m.actor=? AND m.request_id=? AND m.operation='project.source' AND m.resource=?`, actor, key, project).Scan(&raw)
	if err == sql.ErrNoRows {
		return Metadata{}, false, nil
	}
	if err != nil {
		return Metadata{}, false, err
	}
	var result Metadata
	if json.Unmarshal(raw, &result) != nil || result.Kind != "github" || result.Repository == nil || result.Repository.RepositoryID != repositoryID || result.Repository.CommitSHA != commit {
		return Metadata{}, false, ErrConflict
	}
	return result, true, nil
}
