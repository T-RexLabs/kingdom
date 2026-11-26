package modelcatalog

import (
	"context"
	"sort"
	"strings"

	"kingdom/internal/api"
	gen "kingdom/internal/apigen"
)

type projectCatalog struct {
	service *Service
	project string
}

func (s *Service) ForProject(project string) api.ProviderModelCatalog {
	return &projectCatalog{s, project}
}
func (p *projectCatalog) Snapshot(ctx context.Context, refresh bool) (api.ProviderModelCatalogSnapshot, error) {
	unavailable := api.ProviderModelCatalogSnapshot{Items: []gen.ProviderModel{}, Status: api.ProviderModelCatalogStatusUnavailable, FailureReason: api.ProviderModelCatalogFailureReasonServiceUnavailable, FailureMessage: "Refresh an authorized connection model catalog first."}
	actor := api.AuthenticatedActorID(ctx)
	if actor == "" {
		return unavailable, nil
	}
	metadata, err := p.service.store.List(ctx, actor)
	if err != nil {
		return unavailable, ErrUnavailable
	}
	seen := map[string]gen.ProviderModel{}
	for _, m := range metadata {
		// Refresh is explicit on the outer, session-protected connection endpoint.
		// A project model read never decrypts a credential or starts discovery.
		snapshot, err := p.service.List(ctx, actor, p.project, m.ID, m.Revision)
		if err != nil {
			continue
		}
		for _, model := range snapshot.Models {
			id := m.Definition.Provider + "/" + model.ID
			item := gen.ProviderModel{Id: id, ModelName: model.Name, ProviderId: m.Definition.Provider, ProviderName: m.Definition.Provider, ThinkingEffortOptions: []gen.ThinkingEffortOption{}}
			for _, effort := range model.Efforts {
				label := strings.ToUpper(effort[:1]) + effort[1:]
				item.ThinkingEffortOptions = append(item.ThinkingEffortOptions, gen.ThinkingEffortOption{Label: label, Value: effort})
			}
			// The legacy project catalog cannot represent credential references. For
			// ambiguous profiles retain only efforts supported by every visible revision.
			if prior, ok := seen[id]; ok {
				allowed := map[string]bool{}
				for _, e := range item.ThinkingEffortOptions {
					allowed[e.Value] = true
				}
				item.ThinkingEffortOptions = nil
				for _, e := range prior.ThinkingEffortOptions {
					if allowed[e.Value] {
						item.ThinkingEffortOptions = append(item.ThinkingEffortOptions, e)
					}
				}
			}
			seen[id] = item
		}
	}
	if len(seen) == 0 {
		return unavailable, nil
	}
	items := make([]gen.ProviderModel, 0, len(seen))
	for _, item := range seen {
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Id < items[j].Id })
	return api.ProviderModelCatalogSnapshot{Items: items, Status: api.ProviderModelCatalogStatusFresh}, nil
}
func (p *projectCatalog) List(ctx context.Context, refresh bool) ([]gen.ProviderModel, error) {
	v, err := p.Snapshot(ctx, refresh)
	return v.Items, err
}
