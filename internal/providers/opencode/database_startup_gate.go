package opencode

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
)

// OpenCode configures WAL before its own busy timeout. Serializing only the
// connection startup window prevents concurrent processes sharing OPENCODE_DB
// from racing on that pragma while preserving concurrent model execution.
type databaseStartupGateRegistry struct {
	mu    sync.Mutex
	gates map[string]chan struct{}
}

var opencodeDatabaseStartupGates = databaseStartupGateRegistry{gates: map[string]chan struct{}{}}

func (r *databaseStartupGateRegistry) acquire(ctx context.Context, databasePath string) (func(), error) {
	key := canonicalDatabasePath(databasePath)
	if key == "" {
		return func() {}, nil
	}
	r.mu.Lock()
	gate := r.gates[key]
	if gate == nil {
		gate = make(chan struct{}, 1)
		r.gates[key] = gate
	}
	r.mu.Unlock()
	select {
	case gate <- struct{}{}:
		var once sync.Once
		return func() { once.Do(func() { <-gate }) }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func canonicalDatabasePath(path string) string {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return ""
	}
	abs, err := filepath.Abs(trimmed)
	if err != nil {
		return filepath.Clean(trimmed)
	}
	return filepath.Clean(abs)
}
