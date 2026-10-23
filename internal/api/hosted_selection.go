package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	gen "kingdom/internal/apigen"
	"kingdom/internal/execution/access"
	"kingdom/internal/state"
)

// SelectionResolver checks current project membership, exact credential revision
// visibility (PinForProject), and cached connection-scoped model capabilities.
// It returns metadata only. It must honor context deadlines and must not perform
// network calls or access the project database: planning invokes it inside its
// enqueue transaction. External catalog refresh belongs to a separate preflight.
type SelectionResolver interface {
	Resolve(context.Context, string, string, access.RequestedSelection) (access.Selection, error)
}
type HostedSelectionConfig struct {
	ProjectID string
	Resolver  SelectionResolver
}

var errHostedSelection = errors.New("model connection selection is unavailable for this user and project")

func validateHostedSelectionConfig(ctx context.Context, db *sql.DB, cfg ServerConfig) error {
	project, err := access.Project(ctx, db)
	if err != nil {
		return err
	}
	if cfg.HostedSelection == nil {
		if project != "" && cfg.LocalSelection == nil {
			return errors.New("hosted project requires a selection resolver")
		}
		return nil
	}
	domain, _, domainErr := access.ProjectDomain(ctx, db)
	if domainErr != nil {
		return domainErr
	}
	if domain != access.DomainHosted || cfg.LocalSelection != nil || project == "" || project != cfg.HostedSelection.ProjectID || cfg.HostedSelection.Resolver == nil || cfg.Authenticator == nil || cfg.ProviderModelCatalog == nil {
		return errors.New("hosted selection requires matching project, authenticator, resolver and explicit catalog")
	}
	return nil
}
func resolveHostedSelection(ctx context.Context, cfg *HostedSelectionConfig, raw string) (access.Snapshot, error) {
	actor := authenticatedActorID(ctx)
	if cfg == nil || actor == "" || actor == operatorActorID {
		return access.Snapshot{}, errHostedSelection
	}
	var requested access.RequestedSelection
	if err := json.Unmarshal([]byte(raw), &requested); err != nil || requested.Connection == nil {
		return access.Snapshot{}, errHostedSelection
	}
	bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	selection, err := cfg.Resolver.Resolve(bounded, actor, cfg.ProjectID, requested)
	if err != nil || bounded.Err() != nil {
		return access.Snapshot{}, errHostedSelection
	}
	snapshot := access.Snapshot{Version: 1, ActorID: actor, ProjectID: cfg.ProjectID, Selection: selection}
	if snapshot.Validate() != nil || access.MatchesSelection(raw, selection) != nil {
		return access.Snapshot{}, errHostedSelection
	}
	return snapshot, nil
}
func (s *Server) planningSelection(ctx context.Context, selection *gen.PlanningAgentModelSelection) error {
	if s.localSelection != nil {
		raw, err := json.Marshal(selection)
		if err != nil {
			return err
		}
		_, err = resolveLocalSelection(ctx, s.localSelection, string(raw))
		return err
	}
	if s.hostedSelection == nil {
		if selection != nil && selection.Connection != nil {
			return errors.New("model connections are not accepted in local mode")
		}
		return nil
	}
	raw, err := json.Marshal(selection)
	if err != nil {
		return err
	}
	_, err = resolveHostedSelection(ctx, s.hostedSelection, string(raw))
	return err
}
func (s *Server) runAccessSnapshot(ctx context.Context, selection *gen.RunModelSelection) (*access.RunSnapshot, error) {
	if s.hostedSelection == nil && s.localSelection == nil {
		return nil, nil
	}
	if selection == nil {
		return nil, errHostedSelection
	}
	resolve := func(entry gen.RunModelSelectionEntry) (access.Snapshot, error) {
		raw, err := json.Marshal(entry)
		if err != nil {
			return access.Snapshot{}, err
		}
		if s.localSelection != nil {
			return resolveLocalSelection(ctx, s.localSelection, string(raw))
		}
		return resolveHostedSelection(ctx, s.hostedSelection, string(raw))
	}
	global, err := resolve(selection.Global)
	if err != nil {
		return nil, err
	}
	result := &access.RunSnapshot{Version: global.Version, Domain: global.Domain, ActorID: global.ActorID, ProjectID: global.ProjectID, Global: global.Selection, Roles: map[string]access.Selection{}}
	if selection.RoleOverrides != nil {
		for role, entry := range *selection.RoleOverrides {
			snapshot, err := resolve(entry)
			if err != nil {
				return nil, err
			}
			result.Roles[role] = snapshot.Selection
		}
	}
	return result, result.Validate()
}
func normalizeHostedRunSelection(selection *gen.RunModelSelection) (*gen.RunModelSelection, string, error) {
	if selection == nil {
		return nil, "", errHostedSelection
	}
	roles := map[string]bool{}
	for _, role := range state.CanonicalRoleIDs() {
		roles[role] = true
	}
	if selection.RoleOverrides != nil {
		for role := range *selection.RoleOverrides {
			if !roles[role] {
				return nil, "", errors.New("model selection role is not recognized")
			}
		}
	}
	raw, err := json.Marshal(selection)
	if err != nil {
		return nil, "", err
	}
	// Exact public tuples are preserved. Resolver validation happens before the
	// creation transaction, without defaulting or substituting another revision.
	var copied gen.RunModelSelection
	if err = json.Unmarshal(raw, &copied); err != nil {
		return nil, "", err
	}
	return &copied, string(raw), nil
}
