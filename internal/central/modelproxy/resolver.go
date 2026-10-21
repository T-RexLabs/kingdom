package modelproxy

import (
	"context"
	"kingdom/internal/central/connections"
	"strings"
)

// APIKeyResolver has no OAuth refresh or host credential discovery fallback.
// Providers explicitly maps configured connection providers to the supported
// OpenAI-compatible protocol. A future trusted OAuth owner implements Resolver.
type APIKeyResolver struct {
	Store     *connections.Store
	Providers map[string]string
}

func (r *APIKeyResolver) route(binding Binding, metadata connections.Metadata) (Route, error) {
	selected := binding.Access.Selection
	c := selected.Connection
	if r == nil || r.Store == nil || binding.Access.Validate() != nil || metadata.ID != c.ID || metadata.Revision != c.Revision || metadata.OwnerID != c.OwnerID || string(metadata.Scope) != c.Scope || metadata.Definition.Provider != c.Provider || metadata.Disabled || metadata.Definition.AuthKind != "api_key" || r.Providers[c.Provider] != "openai" {
		return Route{}, ErrDenied
	}
	endpoint := metadata.Definition.Endpoint
	if endpoint == "" {
		if c.Provider != "openai" {
			return Route{}, ErrDenied
		}
		endpoint = "https://api.openai.com/v1"
	}
	return Route{Protocol: "openai", Endpoint: endpoint, Model: strings.TrimPrefix(selected.Model, c.Provider+"/")}, nil
}
func (r *APIKeyResolver) Pin(ctx context.Context, b Binding) (Route, error) {
	if r == nil || r.Store == nil {
		return Route{}, ErrDenied
	}
	a := b.Access
	c := a.Selection.Connection
	m, e := r.Store.PinForProject(ctx, a.ActorID, a.ProjectID, c.ID, c.Revision)
	if e != nil {
		return Route{}, ErrDenied
	}
	return r.route(b, m)
}
func (r *APIKeyResolver) Resolve(ctx context.Context, b Binding, expected Route) (Credential, error) {
	if r == nil || r.Store == nil {
		return Credential{}, ErrDenied
	}
	a := b.Access
	c := a.Selection.Connection
	m, secret, e := r.Store.ResolveForProject(ctx, a.ActorID, a.ProjectID, c.ID, c.Revision)
	if e != nil {
		return Credential{}, ErrDenied
	}
	route, e := r.route(b, m)
	if e != nil || route != expected {
		return Credential{}, ErrDenied
	}
	return NewBearerCredential(string(secret.Bytes()))
}
