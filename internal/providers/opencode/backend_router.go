package opencode

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/access"
)

// BackendRouter owns explicitly configured providers. Routing is always derived
// from canonical source pins, including cleanup after a configuration change.
// An unavailable historical backend is an error, never a fallback.
type BackendRouter struct {
	db             *sql.DB
	selected       providerexecution.Backend
	local, managed Backend
}

func NewBackendRouter(db *sql.DB, selected providerexecution.Backend, local, managed Backend) (*BackendRouter, error) {
	if db == nil || (selected != providerexecution.BackendLocal && selected != providerexecution.BackendOpenShell) {
		return nil, fmt.Errorf("explicit backend routing configuration required")
	}
	if local != nil && nilManagedDependency(local) || managed != nil && nilManagedDependency(managed) {
		return nil, fmt.Errorf("backend must not be typed nil")
	}
	if local != nil && local.ExecutionBackend() != providerexecution.BackendLocal {
		return nil, fmt.Errorf("local backend mismatch")
	}
	if managed != nil && managed.ExecutionBackend() != providerexecution.BackendOpenShell {
		return nil, fmt.Errorf("managed backend mismatch")
	}
	r := &BackendRouter{db, selected, local, managed}
	if _, err := r.backend(selected); err != nil {
		return nil, err
	}
	return r, nil
}
func (r *BackendRouter) ExecutionBackend() providerexecution.Backend { return r.selected }
func (r *BackendRouter) SupportsRemoteCleanup() bool                 { return r.managed != nil }
func (r *BackendRouter) InvocationBackend(ctx context.Context, id string) (providerexecution.Backend, error) {
	return access.InvocationBackend(ctx, r.db, id)
}
func (r *BackendRouter) backend(kind providerexecution.Backend) (Backend, error) {
	var b Backend
	switch kind {
	case providerexecution.BackendLocal:
		b = r.local
	case providerexecution.BackendOpenShell:
		b = r.managed
	}
	if b == nil {
		return nil, fmt.Errorf("retained execution backend is not configured")
	}
	return b, nil
}
func (r *BackendRouter) invocation(ctx context.Context, id string) (Backend, error) {
	kind, err := r.InvocationBackend(ctx, id)
	if err != nil {
		return nil, err
	}
	return r.backend(kind)
}
func (r *BackendRouter) Prepare(ctx context.Context, req ExecuteRequest) (Prepared, error) {
	b, e := r.invocation(ctx, req.InvocationID)
	if e != nil {
		return Prepared{}, e
	}
	return b.Prepare(ctx, req)
}
func (r *BackendRouter) Execute(ctx context.Context, req ExecuteRequest) (ExecuteResponse, error) {
	b, e := r.invocation(ctx, req.InvocationID)
	if e != nil {
		return ExecuteResponse{}, e
	}
	return b.Execute(ctx, req)
}
func (r *BackendRouter) Drain(ctx context.Context, id string) error {
	b, e := r.invocation(ctx, id)
	if e != nil {
		return e
	}
	return b.Drain(ctx, id)
}
func (r *BackendRouter) Discard(ctx context.Context, id string) error {
	b, e := r.invocation(ctx, id)
	if e != nil {
		return e
	}
	return b.Discard(ctx, id)
}
func (r *BackendRouter) Cancel(ctx context.Context, id string) error {
	kind, e := access.AttemptBackend(ctx, r.db, id)
	if e != nil {
		return e
	}
	b, e := r.backend(kind)
	if e != nil {
		return e
	}
	return b.Cancel(ctx, id)
}
func (r *BackendRouter) CloseContext(ctx context.Context) error {
	var failures []error
	for _, b := range []Backend{r.local, r.managed} {
		if b != nil {
			failures = append(failures, b.CloseContext(ctx))
		}
	}
	return errors.Join(failures...)
}
func (r *BackendRouter) ForceClose() error {
	var failures []error
	for _, b := range []Backend{r.local, r.managed} {
		if b != nil {
			failures = append(failures, b.ForceClose())
		}
	}
	return errors.Join(failures...)
}

// ValidateManagedConfiguration is used only by the explicit local assembly gate.
func (r *BackendRouter) ValidateManagedConfiguration() error {
	managed, ok := r.managed.(*ManagedService)
	if !ok || managed == nil || r.selected != providerexecution.BackendOpenShell {
		return fmt.Errorf("managed provider is not initialized")
	}
	return managed.ValidateConfiguration()
}
func (r *BackendRouter) SourceBackend(ctx context.Context, kind, id string) (providerexecution.Backend, error) {
	backend, e := access.SourceBackend(ctx, r.db, kind, id)
	if e != nil {
		return "", e
	}
	if _, e = r.backend(backend); e != nil {
		return "", e
	}
	return backend, nil
}
