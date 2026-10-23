package workspacesource

import (
	"context"
	"database/sql"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/securefs"
)

// CanonicalResolver accepts only a current preparation and its retained root.
// It does not accept host paths from the worker or public request.
type CanonicalResolver struct {
	DB        *sql.DB
	Authority *providerrequest.Authority
}

func (r CanonicalResolver) Open(ctx context.Context, p providerrequest.Preparation) (*securefs.Root, error) {
	if r.DB == nil || r.Authority == nil {
		return nil, ErrUnavailable
	}
	current, err := r.Authority.Preparation(ctx, p.Owner.InvocationID)
	if err != nil || current != p {
		return nil, ErrUnavailable
	}
	root, err := providerrequest.OpenPreparationWorkspace(ctx, r.DB, p)
	if err != nil {
		return nil, ErrUnavailable
	}
	current, err = r.Authority.Preparation(ctx, p.Owner.InvocationID)
	if err != nil || current != p {
		root.Close()
		return nil, ErrUnavailable
	}
	return root, nil
}
