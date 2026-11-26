package localowner

import (
	"context"
	"kingdom/internal/api"
	gen "kingdom/internal/apigen"
	"kingdom/internal/central/modelproxy"
	"kingdom/internal/execution/access"
	"kingdom/internal/subscription/chatgpt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// ChatGPT binds one Kingdom-owned registration to one local owner and project.
// Mutable OAuth credentials never enter execution snapshots or model revisions.
type chatGPTService interface {
	Registration(context.Context, string, string) (chatgpt.Registration, error)
	Models(context.Context, string, string, http.RoundTripper) ([]chatgpt.Model, chatgpt.Registration, error)
	Access(context.Context, string, string) (chatgpt.Token, chatgpt.Registration, error)
}
type ChatGPT struct {
	service            chatGPTService
	owner, project, id string
	transport          http.RoundTripper
	mu                 sync.Mutex
	models             []chatgpt.Model
	revision           int64
	expires            time.Time
}

func NewChatGPT(service chatGPTService, owner, project, registration string, transport http.RoundTripper) (*ChatGPT, error) {
	if service == nil || !validID(owner) || !validID(project) || !validID(registration) {
		return nil, modelproxy.ErrInvalid
	}
	return &ChatGPT{service: service, owner: owner, project: project, id: registration, transport: transport}, nil
}
func (m *ChatGPT) catalog(ctx context.Context, refresh bool) ([]chatgpt.Model, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	reg, e := m.service.Registration(ctx, m.owner, m.id)
	if e != nil || reg.State != "ready" {
		return nil, 0, modelproxy.ErrDenied
	}
	if !refresh && reg.Revision == m.revision && time.Now().Before(m.expires) {
		return m.models, m.revision, nil
	}
	rows, reg, e := m.service.Models(ctx, m.owner, m.id, m.transport)
	if e != nil {
		return nil, 0, e
	}
	m.models, m.revision, m.expires = rows, reg.Revision, time.Now().Add(5*time.Minute)
	return rows, reg.Revision, nil
}
func (m *ChatGPT) ResolveLocal(ctx context.Context, model, effort string) (access.Selection, error) {
	rows, revision, e := m.catalog(ctx, false)
	if e != nil {
		return access.Selection{}, e
	}
	for _, row := range rows {
		if model != "openai/"+row.Slug {
			continue
		}
		allowed := effort == ""
		for _, level := range row.Reasoning {
			allowed = allowed || level.Effort == effort
		}
		if allowed {
			return access.Selection{Model: model, ThinkingEffort: effort, Connection: access.Connection{ID: m.id, Revision: revision, OwnerID: m.owner, Scope: "local_owner", Provider: "openai"}}, nil
		}
	}
	return access.Selection{}, modelproxy.ErrDenied
}
func (m *ChatGPT) route(ctx context.Context, s access.Snapshot) (modelproxy.Route, error) {
	if s.Validate() != nil || s.AuthorityDomain() != access.DomainLocalOwner || s.ActorID != m.owner || s.ProjectID != m.project {
		return modelproxy.Route{}, modelproxy.ErrDenied
	}
	expected, e := m.ResolveLocal(ctx, s.Selection.Model, s.Selection.ThinkingEffort)
	if e != nil || expected != s.Selection {
		return modelproxy.Route{}, modelproxy.ErrDenied
	}
	return modelproxy.Route{Protocol: "openai_chatgpt", Endpoint: "https://api.openai.com/v1", Model: strings.TrimPrefix(expected.Model, "openai/")}, nil
}
func (m *ChatGPT) RequireLocalRevision(ctx context.Context, s access.Snapshot) error {
	_, e := m.route(ctx, s)
	return e
}
func (m *ChatGPT) Pin(ctx context.Context, b modelproxy.Binding) (modelproxy.Route, error) {
	return m.route(ctx, b.Access)
}
func (m *ChatGPT) Resolve(ctx context.Context, b modelproxy.Binding, r modelproxy.Route) (modelproxy.Credential, error) {
	expected, e := m.route(ctx, b.Access)
	if e != nil || r != expected {
		return modelproxy.Credential{}, modelproxy.ErrDenied
	}
	token, reg, e := m.service.Access(ctx, m.owner, m.id)
	if e != nil || reg.Revision != b.Access.Selection.Connection.Revision {
		return modelproxy.Credential{}, modelproxy.ErrDenied
	}
	if _, e = m.route(ctx, b.Access); e != nil {
		return modelproxy.Credential{}, e
	}
	return modelproxy.NewBearerCredential(token.Bearer())
}
func (m *ChatGPT) List(ctx context.Context, refresh bool) ([]gen.ProviderModel, error) {
	rows, _, e := m.catalog(ctx, refresh)
	if e != nil {
		return nil, e
	}
	out := make([]gen.ProviderModel, 0, len(rows))
	for _, row := range rows {
		model := gen.ProviderModel{Id: "openai/" + row.Slug, ModelName: row.Name, ProviderId: "openai", ProviderName: "ChatGPT", ThinkingEffortOptions: []gen.ThinkingEffortOption{}}
		for _, level := range row.Reasoning {
			model.ThinkingEffortOptions = append(model.ThinkingEffortOptions, gen.ThinkingEffortOption{Label: strings.ToUpper(level.Effort[:1]) + level.Effort[1:], Value: level.Effort})
		}
		out = append(out, model)
	}
	return out, nil
}
func (m *ChatGPT) Snapshot(ctx context.Context, refresh bool) (api.ProviderModelCatalogSnapshot, error) {
	rows, e := m.List(ctx, refresh)
	if e != nil {
		return api.ProviderModelCatalogSnapshot{Status: api.ProviderModelCatalogStatusUnavailable}, e
	}
	return api.ProviderModelCatalogSnapshot{Status: api.ProviderModelCatalogStatusFresh, Items: rows}, nil
}
