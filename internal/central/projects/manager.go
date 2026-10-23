// Package projects owns one shared runtime for each central project. Actor
// identity remains request-scoped in the supplied authenticator.
package projects

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"sync"
	"time"

	"github.com/google/uuid"
	"kingdom/internal/api"
)

var ErrUnavailable = errors.New("project runtime unavailable")

// Runtime has completed startup. Close must join consumers and provider work
// before releasing its database. A failed Close retains ownership for retry.
type Runtime interface {
	http.Handler
	Close(context.Context) error
}

// Builder owns partial startup, including cleanup on failure. Its context is
// owned by Manager, so an HTTP client disconnect cannot orphan a runtime.
type Builder func(context.Context, string, api.Authenticator) (Runtime, error)

type Config struct {
	Build          Builder
	StartupTimeout time.Duration
	MaxProjects    int
}

type entry struct {
	ready      chan struct{}
	runtime    Runtime
	err        error
	closeToken chan struct{}
	closed     bool
}

type Manager struct {
	cfg      Config
	lifetime context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	closed   bool
	entries  map[string]*entry
}

func New(cfg Config) (*Manager, error) {
	if cfg.Build == nil || cfg.StartupTimeout < 0 || cfg.MaxProjects < 0 {
		return nil, ErrUnavailable
	}
	if cfg.StartupTimeout == 0 {
		cfg.StartupTimeout = 60 * time.Second
	}
	if cfg.MaxProjects == 0 {
		cfg.MaxProjects = 128
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{cfg: cfg, lifetime: ctx, cancel: cancel, entries: map[string]*entry{}}, nil
}

// Factory matches the central server's ProjectFactory. The server must perform
// current project authorization before calling it. Cached runtimes retain the
// project authenticator, never a session or the first requesting actor.
func (m *Manager) Factory(ctx context.Context, id string, authenticator api.Authenticator) (http.Handler, error) {
	parsed, err := uuid.Parse(id)
	if err != nil || parsed == uuid.Nil || parsed.String() != id || nilInterface(authenticator) {
		return nil, ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, ErrUnavailable
	}
	e := m.entries[id]
	if e == nil {
		if len(m.entries) >= m.cfg.MaxProjects {
			m.mu.Unlock()
			return nil, ErrUnavailable
		}
		e = &entry{ready: make(chan struct{}), closeToken: make(chan struct{}, 1)}
		e.closeToken <- struct{}{}
		m.entries[id] = e
		go m.build(id, authenticator, e)
	}
	m.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-e.ready:
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || e.err != nil || nilInterface(e.runtime) {
		return nil, ErrUnavailable
	}
	return e.runtime, nil
}

func (m *Manager) build(id string, authenticator api.Authenticator, e *entry) {
	ctx, cancel := context.WithTimeout(m.lifetime, m.cfg.StartupTimeout)
	defer cancel()
	runtime, err := m.cfg.Build(ctx, id, authenticator)
	if nilInterface(runtime) {
		runtime = nil
		if err == nil {
			err = ErrUnavailable
		}
	}
	// A cancelled startup cannot publish a runtime. A builder that returned one
	// still transfers its ownership, so Close can drain it before releasing state.
	if err == nil {
		err = ctx.Err()
	}
	m.mu.Lock()
	e.runtime, e.err = runtime, err
	close(e.ready)
	// Failed startup without remaining ownership may be retried. A builder must
	// not return a partially owned runtime as nil.
	if err != nil && nilInterface(runtime) && m.entries[id] == e {
		delete(m.entries, id)
	}
	m.mu.Unlock()
}

// Close permanently stops admission and waits for in-flight construction.
// Timeout or cleanup failure preserves entries, so a later Close can finish.
func (m *Manager) Close(ctx context.Context) error {
	m.mu.Lock()
	m.closed = true
	m.cancel()
	entries := make([]*entry, 0, len(m.entries))
	for _, e := range m.entries {
		entries = append(entries, e)
	}
	m.mu.Unlock()
	errorsOut := make(chan error, len(entries))
	var joined sync.WaitGroup
	for _, e := range entries {
		joined.Add(1)
		go func() { defer joined.Done(); errorsOut <- closeEntry(ctx, e) }()
	}
	joined.Wait()
	close(errorsOut)
	var failures []error
	for err := range errorsOut {
		if err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}
func closeEntry(ctx context.Context, e *entry) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-e.ready:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-e.closeToken:
	}
	defer func() { e.closeToken <- struct{}{} }()
	if e.closed {
		return nil
	}
	if e.runtime != nil {
		if err := e.runtime.Close(ctx); err != nil {
			return err
		}
	}
	e.closed = true
	return nil
}

func nilInterface(v any) bool {
	if v == nil {
		return true
	}
	r := reflect.ValueOf(v)
	switch r.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return r.IsNil()
	}
	return false
}
