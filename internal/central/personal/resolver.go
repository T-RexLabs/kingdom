package personal

import (
	"context"
	"strings"

	"kingdom/internal/central/connections"
	"kingdom/internal/central/modelproxy"
	"kingdom/internal/execution/access"
)

type Routes interface {
	PinAccountRoute(context.Context, string, string, connections.Metadata, string, string) (modelproxy.Route, error)
}

// Resolver uses only trusted connection metadata to choose an authentication
// owner. A failed account never falls back to another account or an API key.
type Resolver struct {
	Connections *connections.Store
	Accounts    *Service
	Routes      Routes
	APIKeys     modelproxy.Resolver
}

func (r *Resolver) account(ctx context.Context, b modelproxy.Binding) (connections.Metadata, connections.AccountReference, error) {
	if r == nil || r.Connections == nil || r.Accounts == nil || b.Access.Validate() != nil || b.Access.AuthorityDomain() != access.DomainHosted {
		return connections.Metadata{}, connections.AccountReference{}, ErrDenied
	}
	ctx, done, err := r.Accounts.enter(ctx)
	if err != nil {
		return connections.Metadata{}, connections.AccountReference{}, ErrDenied
	}
	defer done()
	a := b.Access
	c := a.Selection.Connection
	m, ref, err := r.Connections.ResolveAccountForProject(ctx, a.ActorID, a.ProjectID, c.ID, c.Revision)
	if err != nil || m.OwnerID != a.ActorID || m.OwnerID != c.OwnerID || string(m.Scope) != c.Scope || m.Definition.Provider != c.Provider {
		return m, ref, ErrDenied
	}
	if m.Definition.AuthKind == "copilot_oauth" && m.Definition.Provider == "github-copilot" {
		if _, e := r.Accounts.read(ctx, a.ActorID, ref); e != nil {
			return m, ref, ErrDenied
		}
	} else if m.Definition.AuthKind == "openai_oauth" && m.Definition.Provider == "openai" && r.Accounts.chatgpt != nil {
		reg, e := r.Accounts.chatgpt.Registration(ctx, a.ActorID, ref.ID)
		if e != nil || reg.Revision != ref.Revision || reg.State != "ready" {
			return m, ref, ErrDenied
		}
	} else {
		return m, ref, ErrDenied
	}
	return m, ref, nil
}

// validateAccountRoute checks the registry's trusted immutable route against
// current account metadata. It does not consult model discovery or choose a
// replacement protocol, model, endpoint, account, or connection revision.
func (r *Resolver) validateAccountRoute(ctx context.Context, b modelproxy.Binding, m connections.Metadata, ref connections.AccountReference, route modelproxy.Route) error {
	if route.Model != strings.TrimPrefix(b.Access.Selection.Model, m.Definition.Provider+"/") {
		return ErrDenied
	}
	switch m.Definition.AuthKind {
	case "openai_oauth":
		if route.Protocol != modelproxy.ProtocolOpenAIChatGPT || route.Endpoint != "https://api.openai.com/v1" {
			return ErrDenied
		}
	case "copilot_oauth":
		credential, err := r.Accounts.read(ctx, b.Access.ActorID, ref)
		if err != nil {
			return ErrDenied
		}
		base, err := copilotEndpoint(credential)
		if err != nil {
			return ErrDenied
		}
		switch route.Protocol {
		case modelproxy.ProtocolOpenAIChat, modelproxy.ProtocolOpenAIResponses:
			if route.Endpoint != base {
				return ErrDenied
			}
		case modelproxy.ProtocolAnthropic:
			if route.Endpoint != base+"/v1" {
				return ErrDenied
			}
		default:
			return ErrDenied
		}
	default:
		return ErrDenied
	}
	return nil
}
func (r *Resolver) metadata(ctx context.Context, b modelproxy.Binding) (connections.Metadata, error) {
	if r == nil || r.Connections == nil || b.Access.Validate() != nil || b.Access.AuthorityDomain() != access.DomainHosted {
		return connections.Metadata{}, ErrDenied
	}
	a := b.Access
	c := a.Selection.Connection
	return r.Connections.PinForProject(ctx, a.ActorID, a.ProjectID, c.ID, c.Revision)
}
func (r *Resolver) Pin(ctx context.Context, b modelproxy.Binding) (modelproxy.Route, error) {
	m, err := r.metadata(ctx, b)
	if err != nil {
		return modelproxy.Route{}, ErrDenied
	}
	if m.Definition.AuthKind == "api_key" {
		if r.APIKeys == nil {
			return modelproxy.Route{}, ErrDenied
		}
		return r.APIKeys.Pin(ctx, b)
	}
	if r.Routes == nil || r.Accounts == nil {
		return modelproxy.Route{}, ErrDenied
	}
	ctx, done, err := r.Accounts.enter(ctx)
	if err != nil {
		return modelproxy.Route{}, ErrDenied
	}
	defer done()
	m, ref, err := r.account(ctx, b)
	if err != nil {
		return modelproxy.Route{}, ErrDenied
	}
	route, err := r.Routes.PinAccountRoute(ctx, b.Access.ActorID, b.Access.ProjectID, m, b.Access.Selection.Model, b.Access.Selection.ThinkingEffort)
	if err != nil || r.validateAccountRoute(ctx, b, m, ref, route) != nil {
		return modelproxy.Route{}, ErrDenied
	}
	current, currentRef, err := r.account(ctx, b)
	if err != nil || current != m || currentRef != ref || ctx.Err() != nil {
		return modelproxy.Route{}, ErrDenied
	}
	return route, nil
}
func (r *Resolver) Resolve(ctx context.Context, b modelproxy.Binding, expected modelproxy.Route) (modelproxy.Credential, error) {
	m, err := r.metadata(ctx, b)
	if err != nil {
		return modelproxy.Credential{}, ErrDenied
	}
	if m.Definition.AuthKind == "api_key" {
		if r.APIKeys == nil {
			return modelproxy.Credential{}, ErrDenied
		}
		return r.APIKeys.Resolve(ctx, b, expected)
	}
	if r.Accounts == nil {
		return modelproxy.Credential{}, ErrDenied
	}
	ctx, done, err := r.Accounts.enter(ctx)
	if err != nil {
		return modelproxy.Credential{}, ErrDenied
	}
	defer done()
	current, ref, err := r.account(ctx, b)
	if err != nil || current != m || r.validateAccountRoute(ctx, b, m, ref, expected) != nil {
		return modelproxy.Credential{}, ErrDenied
	}
	if m.Definition.AuthKind == "copilot_oauth" {
		credential, err := r.Accounts.read(ctx, b.Access.ActorID, ref)
		if err != nil {
			return modelproxy.Credential{}, ErrDenied
		}
		return modelproxy.NewCopilotCredential(credential.Access)
	}
	token, reg, err := r.Accounts.chatgpt.Access(ctx, b.Access.ActorID, ref.ID)
	if err != nil || reg.Revision != ref.Revision {
		return modelproxy.Credential{}, ErrDenied
	}
	return modelproxy.NewBearerCredential(token.Bearer())
}

// Disconnect is called only after the connection has been durably disabled and
// its model capabilities have been revoked and joined by the central server.
// A false result means upstream revocation is not confirmed.
func (s *Service) Disconnect(ctx context.Context, owner string, d connections.Definition, ref connections.AccountReference) (bool, error) {
	ctx, done, err := s.enter(ctx)
	if err != nil {
		return false, err
	}
	defer done()
	switch d.AuthKind {
	case "copilot_oauth":
		if d.Provider != "github-copilot" {
			return false, ErrDenied
		}
		return false, s.fence(ctx, owner, ref)
	case "openai_oauth":
		if d.Provider != "openai" || s.chatgpt == nil {
			return false, ErrDenied
		}
		reg, err := s.chatgpt.Registration(ctx, owner, ref.ID)
		if err != nil || reg.Revision != ref.Revision {
			return false, ErrDenied
		}
		return s.chatgpt.Disconnect(ctx, owner, ref.ID)
	default:
		return false, ErrDenied
	}
}
