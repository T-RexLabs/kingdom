// Package selections checks public model choices against current central access.
// It never decrypts a credential or performs provider discovery.
package selections

import (
	"context"
	"errors"
	"time"

	"kingdom/internal/central/connections"
	"kingdom/internal/execution/access"
)

// CachedModels validates a model and thinking effort for this exact immutable
// connection revision. Implementations must use in-memory data only and fail
// closed when the revision has no usable catalog. Discovery is a separate step.
type CachedModels interface {
	Validate(context.Context, connections.Metadata, string, string) error
}
type Resolver struct {
	connections *connections.Store
	models      CachedModels
}

func New(store *connections.Store, models CachedModels) (*Resolver, error) {
	if store == nil || models == nil {
		return nil, errors.New("connection store and cached model validator are required")
	}
	return &Resolver{connections: store, models: models}, nil
}
func (r *Resolver) Resolve(ctx context.Context, actor, project string, requested access.RequestedSelection) (access.Selection, error) {
	if requested.Connection == nil {
		return access.Selection{}, access.ErrInvalid
	}
	bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	metadata, err := r.connections.PinForProject(bounded, actor, project, requested.Connection.ID, requested.Connection.Revision)
	if err != nil {
		return access.Selection{}, err
	}
	selection := access.Selection{Model: requested.Model, ThinkingEffort: requested.ThinkingEffort, Connection: access.Connection{ID: metadata.ID, Revision: metadata.Revision, OwnerID: metadata.OwnerID, Scope: string(metadata.Scope), Provider: metadata.Definition.Provider}}
	snapshot := access.Snapshot{Version: 1, ActorID: actor, ProjectID: project, Selection: selection}
	if err = snapshot.Validate(); err != nil {
		return access.Selection{}, err
	}
	if err = r.models.Validate(bounded, metadata, requested.Model, requested.ThinkingEffort); err != nil {
		return access.Selection{}, err
	}
	if err = bounded.Err(); err != nil {
		return access.Selection{}, err
	}
	return selection, nil
}
