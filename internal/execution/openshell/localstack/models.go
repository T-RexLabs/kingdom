package localstack

import (
	"context"
	"database/sql"
	"kingdom/internal/api"
	gen "kingdom/internal/apigen"
	"kingdom/internal/central/modelproxy"
	"kingdom/internal/execution/access"
	"kingdom/internal/execution/openshell/localowner"
	"strings"
)

// Credentials is the isolated local owner contract. An explicit subscription
// owner can implement it without copying host OpenCode account state.
type Credentials interface {
	modelproxy.Resolver
	RequireLocalRevision(context.Context, access.Snapshot) error
	ResolveLocal(context.Context, string, string) (access.Selection, error)
	api.ProviderModelCatalog
}
type apiKeyModels struct {
	store      *localowner.Store
	id         string
	definition localowner.Definition
}

func (m *apiKeyModels) Pin(ctx context.Context, b modelproxy.Binding) (modelproxy.Route, error) {
	return m.store.Pin(ctx, b)
}
func (m *apiKeyModels) Resolve(ctx context.Context, b modelproxy.Binding, r modelproxy.Route) (modelproxy.Credential, error) {
	return m.store.Resolve(ctx, b, r)
}
func (m *apiKeyModels) RequireLocalRevision(ctx context.Context, s access.Snapshot) error {
	return m.store.RequireLocalRevision(ctx, s)
}
func (m *apiKeyModels) ResolveLocal(ctx context.Context, model, effort string) (access.Selection, error) {
	return m.store.Selection(ctx, m.id, model, effort)
}
func (m *apiKeyModels) List(ctx context.Context, _ bool) ([]gen.ProviderModel, error) {
	rows := make([]gen.ProviderModel, 0, len(m.definition.Models))
	for _, model := range m.definition.Models {
		id := m.definition.Provider + "/" + model.ID
		if _, e := m.ResolveLocal(ctx, id, ""); e != nil {
			return nil, e
		}
		row := gen.ProviderModel{Id: id, ModelName: modelLabel(model), ProviderId: m.definition.Provider, ProviderName: providerLabel(m.definition), ThinkingEffortOptions: []gen.ThinkingEffortOption{}}
		for _, effort := range model.Efforts {
			row.ThinkingEffortOptions = append(row.ThinkingEffortOptions, gen.ThinkingEffortOption{Label: strings.ToUpper(effort[:1]) + effort[1:], Value: effort})
		}
		rows = append(rows, row)
	}
	return rows, nil
}
func (m *apiKeyModels) Snapshot(ctx context.Context, refresh bool) (api.ProviderModelCatalogSnapshot, error) {
	rows, e := m.List(ctx, refresh)
	if e != nil {
		return api.ProviderModelCatalogSnapshot{Status: api.ProviderModelCatalogStatusUnavailable}, e
	}
	return api.ProviderModelCatalogSnapshot{Items: rows, Status: api.ProviderModelCatalogStatusFresh}, nil
}

func providerLabel(d localowner.Definition) string {
	if d.Name != "" {
		return d.Name
	}
	if d.Provider == "openai" {
		return "OpenAI"
	}
	return d.Provider
}
func modelLabel(m localowner.Model) string {
	if m.Name != "" {
		return m.Name
	}
	if m.ID == "gpt-6-luna" {
		return "GPT-6 Luna"
	}
	return m.ID
}

func APIKeyCredentials(ctx context.Context, db *sql.DB, key []byte, owner, project string, c ModelConfig) (Credentials, error) {
	if c.AuthKind != "api_key" {
		return nil, ErrConfig
	}
	vault, e := localowner.New(ctx, db, key, owner, project)
	if e != nil {
		return nil, e
	}
	secret, e := privateFile(c.APIKeyFile, 16384)
	if e != nil {
		return nil, e
	}
	defer clear(secret)
	if _, e = vault.Put(ctx, c.ConnectionID, c.Definition, bytesTrim(secret), false); e != nil {
		return nil, e
	}
	return &apiKeyModels{vault, c.ConnectionID, c.Definition}, nil
}
