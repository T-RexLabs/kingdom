package connections

import (
	"context"
	"database/sql"
	"strings"

	"kingdom/internal/central/catalog"
)

// PinForProject returns an exact visible revision without decrypting it. The
// API uses this metadata to capture an immutable execution selection. This is
// not a lasting grant: the worker must check access again before dispatch.
func (s *Store) PinForProject(ctx context.Context, actor, project, id string, revision int64) (Metadata, error) {
	if strings.TrimSpace(project) == "" || revision <= 0 {
		return Metadata{}, ErrInvalid
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Metadata{}, err
	}
	defer tx.Rollback()
	if err := catalog.AuthorizeTx(ctx, tx, actor, project, catalog.UseProject); err != nil {
		return Metadata{}, ErrDenied
	}
	u, err := user(ctx, tx, actor)
	if err != nil {
		return Metadata{}, err
	}
	m, _, err := read(ctx, tx, id, revision)
	if err != nil {
		return Metadata{}, err
	}
	if m.Disabled || !allowed(u, m, false) {
		return Metadata{}, ErrDenied
	}
	return m, tx.Commit()
}

// ResolveForProject is the worker's credential boundary. It checks both current
// project membership and credential visibility in one catalog transaction.
// Arguments must come from the canonical invocation snapshot, not HTTP headers.
// The result does not grant future authority: the worker must fence active work
// after revocation and must never replace this exact revision with the latest.
func (s *Store) ResolveForProject(ctx context.Context, actor, project, id string, revision int64) (Metadata, Secret, error) {
	if strings.TrimSpace(project) == "" {
		return Metadata{}, Secret{}, ErrInvalid
	}
	return s.resolve(ctx, actor, project, id, revision)
}
