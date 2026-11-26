// Package pluginauthority binds plugin callbacks to one canonical invocation.
package pluginauthority

import (
	"context"
	"errors"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"

	"kingdom/internal/central/capability"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/providers/opencode"
)

type Stage string

const (
	Prepare  Stage = "prepare"
	Activate Stage = "activate"
	Active   Stage = "active"
	Receipt  Stage = "receipt"
)

// Check must validate current canonical authority for Prepare/Active. Receipt
// checks the exact historical owner; it grants no new admission after fencing.
type Authorizer interface {
	Check(context.Context, capability.Binding, Stage) error
}
type Lease struct {
	ID, BaseURL string
	ExpiresAt   time.Time
	token       string
}

func (l Lease) Token() string              { return l.token }
func (l Lease) String() string             { return "[plugin capability]" }
func (l Lease) GoString() string           { return "[plugin capability]" }
func (Lease) MarshalJSON() ([]byte, error) { return nil, capability.ErrDenied }

type Broker struct {
	registry   *capability.Registry
	authorizer Authorizer
	base       string
}

func New(registry *capability.Registry, authorizer Authorizer, base string) (*Broker, error) {
	u, e := url.Parse(base)
	if registry == nil || authorizer == nil || (reflect.ValueOf(authorizer).Kind() == reflect.Pointer && reflect.ValueOf(authorizer).IsNil()) || e != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" || u.RawPath != "" {
		return nil, errors.New("invalid plugin authority configuration")
	}
	return &Broker{registry: registry, authorizer: authorizer, base: strings.TrimRight(base, "/")}, nil
}
func (b *Broker) Issue(ctx context.Context, binding capability.Binding) (Lease, error) {
	if binding.Request != nil {
		return Lease{}, capability.ErrDenied
	}
	if e := b.authorizer.Check(ctx, binding, Prepare); e != nil {
		return Lease{}, capability.ErrDenied
	}
	l, e := b.registry.Issue(ctx, capability.Pins{Purpose: capability.Plugin, Binding: binding})
	if e != nil {
		return Lease{}, e
	}
	return Lease{ID: l.ID, BaseURL: b.base, ExpiresAt: l.ExpiresAt, token: l.Token()}, nil
}
func (b *Broker) owned(ctx context.Context, id string) (capability.Pins, error) {
	p, _, e := b.registry.Lookup(ctx, id)
	if e != nil || p.Purpose != capability.Plugin {
		return capability.Pins{}, capability.ErrDenied
	}
	return p, nil
}
func (b *Broker) Activate(ctx context.Context, id string, identity providerexecution.RequestIdentity) error {
	p, e := b.owned(ctx, id)
	if e != nil {
		return e
	}
	p.Binding.Request = &identity
	if e = b.authorizer.Check(ctx, p.Binding, Activate); e != nil {
		return capability.ErrDenied
	}
	return b.registry.Activate(ctx, id, identity)
}
func (b *Broker) FenceAdmissions(ctx context.Context, id string) error {
	if _, e := b.owned(ctx, id); e != nil {
		return e
	}
	return b.registry.FenceAdmissions(ctx, id)
}
func (b *Broker) Revoke(ctx context.Context, id string) error {
	if _, e := b.owned(ctx, id); e != nil {
		return e
	}
	return b.registry.Revoke(ctx, id)
}
func (b *Broker) RequireClosed(ctx context.Context, binding capability.Binding) error {
	binding.Request = nil
	return b.registry.RequireClosed(ctx, capability.Pins{Purpose: capability.Plugin, Binding: binding})
}
func (b *Broker) FenceBinding(ctx context.Context, binding capability.Binding) error {
	return b.registry.FenceBinding(ctx, capability.Plugin, binding)
}
func (b *Broker) RevokeBinding(ctx context.Context, binding capability.Binding) error {
	return b.registry.RevokeBinding(ctx, capability.Plugin, binding)
}
func (b *Broker) Begin(ctx context.Context, token string, receipt bool) (opencode.ManagedCallbackScope, error) {
	mode, stage := capability.ModeNew, Active
	if receipt {
		mode, stage = capability.ModeReceipt, Receipt
	}
	admission, e := b.registry.Begin(ctx, token, capability.Plugin, mode)
	if e != nil {
		return opencode.ManagedCallbackScope{}, capability.ErrDenied
	}
	binding := admission.Pins.Binding
	identity := admission.Request
	binding.Request = &identity
	if e = b.authorizer.Check(admission.Context, binding, stage); e != nil {
		admission.Done()
		return opencode.ManagedCallbackScope{}, capability.ErrDenied
	}
	if receipt {
		return opencode.ManagedCallbackScope{Context: admission.Context, Identity: identity, Release: admission.Done}, nil
	}
	work, cancel := context.WithCancel(admission.Context)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-work.Done():
				return
			case <-ticker.C:
				if b.authorizer.Check(work, binding, Active) != nil {
					cancel()
					return
				}
			}
		}
	}()
	var once sync.Once
	release := func() { once.Do(func() { cancel(); <-done; admission.Done() }) }
	return opencode.ManagedCallbackScope{Context: work, Identity: identity, Release: release}, nil
}
