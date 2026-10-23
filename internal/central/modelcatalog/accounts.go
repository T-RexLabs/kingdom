package modelcatalog

import (
	"context"
	"sort"
	"strings"

	"kingdom/internal/central/connections"
	"kingdom/internal/central/modelproxy"
)

// AccountModel carries a private trusted route separately from public model
// metadata. Neither an endpoint nor an account credential reaches the portal.
type AccountModel struct {
	Model Model
	Route modelproxy.Route
}

type AccountDiscovery interface {
	Discover(context.Context, string, connections.Definition, connections.AccountReference) ([]AccountModel, error)
}

// AccountRoute reads only a previously discovered exact connection revision.
// It must not refresh an account while a project writer holds its transaction.
func (s *Service) AccountRoute(m connections.Metadata, model string) (modelproxy.Route, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.cache[key(m)]
	if s.closed || !ok || e.metadata != m || m.Disabled || !s.now().Before(e.snapshot.ExpiresAt) {
		return modelproxy.Route{}, ErrUnavailable
	}
	r, ok := e.routes[strings.TrimPrefix(model, m.Definition.Provider+"/")]
	if !ok || !strings.HasPrefix(model, m.Definition.Provider+"/") {
		return modelproxy.Route{}, ErrUnavailable
	}
	return r, nil
}

func (s *Service) refreshAccount(ctx context.Context, actor, project string, m connections.Metadata, fail func() (Snapshot, error)) (Snapshot, error) {
	if s.accounts == nil || m.Scope != connections.Personal || m.OwnerID != actor {
		return fail()
	}
	resolved, ref, err := s.store.ResolveAccountForProject(ctx, actor, project, m.ID, m.Revision)
	if err != nil || resolved != m {
		return fail()
	}
	items, err := s.accounts.Discover(ctx, actor, m.Definition, ref)
	if err != nil || len(items) > 10000 {
		return fail()
	}
	models := make([]Model, 0, len(items))
	routes := make(map[string]modelproxy.Route, len(items))
	for _, item := range items {
		model, route := item.Model, item.Route
		if !safeText(model.ID, 1024) || !safeText(model.Name, 256) || route.Model != model.ID || !route.ValidProtocol() || route.Endpoint == "" {
			return fail()
		}
		if _, exists := routes[model.ID]; exists {
			return fail()
		}
		seen := map[string]bool{}
		for _, effort := range model.Efforts {
			if !safeText(effort, 128) || seen[effort] {
				return fail()
			}
			seen[effort] = true
		}
		model.Efforts = append([]string(nil), model.Efforts...)
		models = append(models, model)
		routes[model.ID] = route
	}
	sort.Slice(models, func(i, j int) bool { return models[i].ID < models[j].ID })
	current, err := s.store.PinForProject(ctx, actor, project, m.ID, m.Revision)
	if err != nil || current != m || ctx.Err() != nil {
		return fail()
	}
	now := s.now()
	v := Snapshot{ConnectionID: m.ID, Revision: m.Revision, Models: models, DiscoveredAt: now, ExpiresAt: now.Add(s.ttl)}
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, item := range s.cache {
		if !now.Before(item.snapshot.ExpiresAt) {
			delete(s.cache, k)
		}
	}
	if s.closed || ctx.Err() != nil || len(s.cache) >= 10000 && s.cache[key(m)].metadata.ID == "" {
		return Snapshot{}, ErrUnavailable
	}
	s.cache[key(m)] = entry{metadata: m, snapshot: clone(v), routes: routes}
	return clone(v), nil
}
