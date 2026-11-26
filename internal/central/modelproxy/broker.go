package modelproxy

import (
	"context"
	"kingdom/internal/central/capability"
	"kingdom/internal/core/providerexecution"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type Broker struct {
	mu        sync.Mutex
	cfg       Config
	closing   bool
	ids       map[string]struct{}
	transport *http.Transport
}

func New(cfg Config) (*Broker, error) {
	if cfg.Registry == nil || cfg.Authorizer == nil || cfg.Resolver == nil {
		return nil, ErrInvalid
	}
	public, e := url.Parse(cfg.PublicBaseURL)
	if e != nil || public.Scheme != "https" || public.Host == "" || public.User != nil || public.RawQuery != "" || public.Fragment != "" || !strings.HasSuffix(public.Path, "/v1") {
		return nil, ErrInvalid
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 5 * time.Minute
	}
	if cfg.MaxRequestBytes == 0 {
		cfg.MaxRequestBytes = 2 << 20
	}
	if cfg.MaxResponseBytes == 0 {
		cfg.MaxResponseBytes = 32 << 20
	}
	if cfg.RequestTimeout <= 0 || cfg.RequestTimeout > time.Hour || cfg.MaxRequestBytes <= 0 || cfg.MaxRequestBytes > 8<<20 || cfg.MaxResponseBytes <= 0 || cfg.MaxResponseBytes > 128<<20 {
		return nil, ErrInvalid
	}
	transport, e := newTransport(cfg)
	if e != nil {
		return nil, e
	}
	return &Broker{cfg: cfg, transport: transport, ids: map[string]struct{}{}}, nil
}
func (b *Broker) Issue(ctx context.Context, binding Binding) (Lease, error) {
	if binding.Request != nil || b.cfg.Authorizer.Check(ctx, binding, StagePrepare) != nil {
		return Lease{}, ErrDenied
	}
	route, err := b.cfg.Resolver.Pin(ctx, binding)
	if err != nil || b.validateRoute(route) != nil || route.Model != strings.TrimPrefix(binding.Access.Selection.Model, binding.Access.Selection.Connection.Provider+"/") {
		return Lease{}, ErrDenied
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closing {
		return Lease{}, ErrDenied
	}
	lease, err := b.cfg.Registry.Issue(ctx, capability.Pins{Purpose: capability.Model, Binding: binding, Route: route})
	if err != nil {
		return Lease{}, ErrDenied
	}
	b.ids[lease.ID] = struct{}{}
	return Lease{Lease: lease, BaseURL: b.cfg.PublicBaseURL, Protocol: route.Protocol}, nil
}
func (b *Broker) Activate(ctx context.Context, id string, identity providerexecution.RequestIdentity) error {
	pins, _, err := b.cfg.Registry.Lookup(ctx, id)
	if err != nil || pins.Purpose != capability.Model {
		return ErrDenied
	}
	binding := pins.Binding
	binding.Request = &identity
	if b.cfg.Authorizer.Check(ctx, binding, StageActivate) != nil {
		return ErrDenied
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closing {
		return ErrDenied
	}
	return b.cfg.Registry.Activate(ctx, id, identity)
}
func (b *Broker) Revoke(ctx context.Context, id string) error {
	pins, _, err := b.cfg.Registry.Lookup(ctx, id)
	if err != nil || pins.Purpose != capability.Model {
		return ErrDenied
	}
	return b.cfg.Registry.Revoke(ctx, id)
}
func (b *Broker) RequireClosed(ctx context.Context, binding Binding) error {
	return b.cfg.Registry.RequireBindingClosed(ctx, capability.Model, binding)
}
func (b *Broker) Close(ctx context.Context) error {
	b.mu.Lock()
	b.closing = true
	ids := make([]string, 0, len(b.ids))
	for id := range b.ids {
		ids = append(ids, id)
	}
	b.mu.Unlock()
	for _, id := range ids {
		if err := b.Revoke(ctx, id); err != nil {
			return err
		}
	}
	b.transport.CloseIdleConnections()
	return nil
}

func (b *Broker) RevokeAccess(ctx context.Context, actor, project string) error {
	return b.cfg.Registry.RevokeAccess(ctx, actor, project)
}
func (b *Broker) RevokeUser(ctx context.Context, actor string) error {
	return b.cfg.Registry.RevokeUser(ctx, actor)
}
func (b *Broker) RevokeBinding(ctx context.Context, binding Binding) error {
	return b.cfg.Registry.RevokeBinding(ctx, capability.Model, binding)
}
