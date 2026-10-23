package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/access"
	"kingdom/internal/execution/providerrequest"
	"time"
)

// LocalSelectionResolver reads only explicit local credential metadata. It must
// validate the model and thinking effort against the configured catalog without
// network calls or project database reads inside the enqueue transaction.
type LocalSelectionResolver interface {
	ResolveLocal(context.Context, string, string) (access.Selection, error)
}
type LocalSelectionConfig struct {
	OwnerID, ProjectID string
	Resolver           LocalSelectionResolver
}

var errLocalSelection = errors.New("local model credential selection is unavailable")

func validateLocalSelectionConfig(ctx context.Context, db *sql.DB, cfg ServerConfig) error {
	domain, owner, err := access.ProjectDomain(ctx, db)
	if err != nil {
		return err
	}
	if cfg.LocalSelection == nil {
		if domain == access.DomainLocalOwner {
			backend, ok := cfg.ProviderService.(interface {
				ExecutionBackend() providerexecution.Backend
			})
			if !ok || backend.ExecutionBackend() != providerexecution.BackendLocal || cfg.Authenticator != nil {
				return errLocalSelection
			}
			return providerrequest.RequireHostLocalMode(ctx, db)
		}
		return nil
	}
	c := cfg.LocalSelection
	project, err := access.Project(ctx, db)
	if err != nil {
		return err
	}
	if domain != access.DomainLocalOwner || owner != c.OwnerID || project != c.ProjectID || c.Resolver == nil || cfg.HostedSelection != nil || cfg.Authenticator != nil || cfg.ProviderModelCatalog == nil {
		return errLocalSelection
	}
	return nil
}
func resolveLocalSelection(ctx context.Context, c *LocalSelectionConfig, raw string) (access.Snapshot, error) {
	if c == nil || c.Resolver == nil {
		return access.Snapshot{}, errLocalSelection
	}
	var requested access.RequestedSelection
	if json.Unmarshal([]byte(raw), &requested) != nil || requested.Connection != nil || requested.Model == "" {
		return access.Snapshot{}, errLocalSelection
	}
	bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	selection, err := c.Resolver.ResolveLocal(bounded, requested.Model, requested.ThinkingEffort)
	if err != nil || bounded.Err() != nil {
		return access.Snapshot{}, errLocalSelection
	}
	snapshot := access.Snapshot{Version: 2, Domain: access.DomainLocalOwner, ActorID: c.OwnerID, ProjectID: c.ProjectID, Selection: selection}
	if snapshot.Validate() != nil || access.MatchesSnapshotSelection(raw, snapshot) != nil {
		return access.Snapshot{}, errLocalSelection
	}
	return snapshot, nil
}
