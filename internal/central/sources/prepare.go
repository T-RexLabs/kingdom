package sources

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"io/fs"
	"kingdom/internal/workspacearchive"
	"path/filepath"

	"kingdom/internal/securefs"
)

// PrepareInitial is called only by the trusted project builder, before consumers
// start. The permanent marker serializes startup with upload publication.
func (s *Service) PrepareInitial(ctx context.Context, project, destination string) error {
	s.prepare.Lock()
	defer s.prepare.Unlock()
	root, err := securefs.OpenRoot(destination)
	if err != nil {
		return err
	}
	defer root.Close()
	identity, err := root.DirectoryIdentity()
	if err != nil {
		return err
	}
	tx, err := s.write(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var device, inode uint64
	var ready int
	err = tx.QueryRowContext(ctx, `SELECT device,inode,ready FROM central_project_starts WHERE project=?`, project).Scan(&device, &inode, &ready)
	if errors.Is(err, sql.ErrNoRows) {
		entries, e := root.ReadDirBounded(".", 1)
		if e != nil || len(entries) != 0 {
			return ErrConflict
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO central_project_starts VALUES(?,?,?,0)`, project, identity.Device, identity.Inode)
	} else if err == nil && (device != identity.Device || inode != identity.Inode) {
		return ErrConflict
	}
	if err != nil {
		return err
	}
	if ready == 1 {
		return tx.Commit()
	}
	stage, manifest, err := readSource(ctx, tx, project)
	if err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if err = validatePartial(root, manifest); err != nil {
		return err
	}
	// Exact-create permits recovery after a partial initial copy, without replacing
	// existing bytes. Once ready, subsequent startups never restore the input.
	for _, entry := range manifest.Entries {
		if err = ctx.Err(); err != nil {
			return err
		}
		if entry.Kind == "directory" {
			if err = root.EnsureDir(entry.Path, 0755); err != nil {
				return err
			}
			continue
		}
		content, e := s.root.ReadRegular(filepath.Join(stage, entry.Path), entry.Size+1)
		if e != nil {
			return e
		}
		sum := sha256.Sum256(content)
		if int64(len(content)) != entry.Size || hex.EncodeToString(sum[:]) != entry.SHA256 {
			return ErrUnavailable
		}
		mode := fs.FileMode(0644)
		if entry.Executable {
			mode = 0755
		}
		if _, err = root.CreateRegularExact(entry.Path, content, mode, 0755); err != nil {
			return err
		}
	}
	if err = root.SyncDir("."); err != nil {
		return err
	}
	current, err := securefs.OpenRoot(destination)
	if err != nil {
		return err
	}
	currentIdentity, err := current.DirectoryIdentity()
	current.Close()
	if err != nil || currentIdentity.Device != identity.Device || currentIdentity.Inode != identity.Inode {
		return ErrConflict
	}
	tx, err = s.write(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE central_project_starts SET ready=1 WHERE project=? AND device=? AND inode=? AND ready=0`, project, identity.Device, identity.Inode)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil || n != 1 {
		return ErrConflict
	}
	return tx.Commit()
}

func validatePartial(root *securefs.Root, manifest workspacearchive.Manifest) error {
	allowed := map[string]string{}
	for _, entry := range manifest.Entries {
		allowed[entry.Path] = entry.Kind
	}
	pending := []string{"."}
	count := 0
	for len(pending) > 0 {
		parent := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		entries, err := root.ReadDirBounded(parent, 32768-count+1)
		if err != nil {
			return ErrConflict
		}
		for _, entry := range entries {
			count++
			if count > 32768 {
				return ErrConflict
			}
			name := filepath.ToSlash(filepath.Join(parent, entry.Name))
			kind, ok := allowed[name]
			if !ok {
				return ErrConflict
			}
			if kind == "directory" && entry.Type == securefs.EntryTypeDirectory {
				pending = append(pending, name)
			} else if kind != "file" || entry.Type != securefs.EntryTypeRegular {
				return ErrConflict
			}
		}
	}
	return nil
}
