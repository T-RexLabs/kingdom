// Package shutdown coordinates concurrent termination of an HTTP
// server and the planning service so neither blocks the other during
// process exit.
//
// The motivating bug: when the kingdomd SIGINT path runs
// http.Server.Shutdown sequentially before the planning service's
// Close, a long-lived SSE/planner request can consume the entire HTTP
// grace window before the planner is ever cancelled. Concurrent
// teardown lets the planner's cancelled-event terminal write reach
// the SSE handler, which then drains in the same window the HTTP
// server is shutting down.
//
// The Coordinator is sync.Once-guarded so repeated calls are real
// no-ops, not silent re-runs that would each pay the wg-drain wall
// clock. This matters because Service.Close is idempotent in semantics
// but not in latency — a second call still blocks until either the
// runner WaitGroup drains or the supplied ctx fires.
package shutdown

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Closer is the contract for the planning-service-side teardown:
// cancel in-flight work, wait for goroutines to drain (bounded by
// ctx). planning.Service.Close in internal/planning satisfies this.
type Closer interface {
	Close(ctx context.Context) error
}

// CloseFunc adapts a function into a shutdown.Closer.
type CloseFunc func(ctx context.Context) error

func (f CloseFunc) Close(ctx context.Context) error {
	if f == nil {
		return nil
	}
	return f(ctx)
}

// CompositeCloser runs multiple closers concurrently under the same
// caller-owned context and joins their errors.
type CompositeCloser struct {
	Closers []Closer
}

func (c CompositeCloser) Close(ctx context.Context) error {
	active := make([]Closer, 0, len(c.Closers))
	for _, closer := range c.Closers {
		if closer != nil {
			active = append(active, closer)
		}
	}
	if len(active) == 0 {
		return nil
	}

	errCh := make(chan error, len(active))
	var wg sync.WaitGroup
	for _, closer := range active {
		wg.Add(1)
		go func(closer Closer) {
			defer wg.Done()
			errCh <- closer.Close(ctx)
		}(closer)
	}
	wg.Wait()
	close(errCh)

	var joined error
	for err := range errCh {
		if err != nil {
			joined = errors.Join(joined, err)
		}
	}
	return joined
}

// Shutdowner is the contract for the HTTP-server-side teardown:
// stop accepting new connections, wait for in-flight requests to
// finish (bounded by ctx). *http.Server satisfies this.
type Shutdowner interface {
	Shutdown(ctx context.Context) error
}

// Logger receives diagnostic messages from the teardown goroutines.
// log.Printf satisfies this; pass a no-op for tests that don't care
// about the noise.
type Logger func(format string, args ...any)

// CloseResult carries the per-subsystem teardown errors so callers
// can decide what to do with each (e.g. exit non-zero on planner
// drain failure even when HTTP shutdown was clean). errors.Join is
// the natural shape when callers just want a single error chain.
type CloseResult struct {
	Closer     error
	Shutdowner error
}

// Coordinator runs Close and Shutdown concurrently exactly once. The
// zero value is ready to use; Run can be called repeatedly and will
// return the cached result without re-executing the underlying
// teardowns.
type Coordinator struct {
	once   sync.Once
	result CloseResult
}

// Run cancels both subsystems in parallel goroutines, each bounded by
// its own copy of `window`. Returns the per-subsystem teardown errors
// so callers can surface them (e.g. exit non-zero on planner drain
// failure). Subsequent calls return the cached result without
// re-executing the underlying teardowns — the once-guard makes Run
// safe to invoke from a deferred safety-net path AND from an
// explicit signal handler without paying the wg-drain wall clock
// twice or double-running Close/Shutdown.
func (c *Coordinator) Run(window time.Duration, closer Closer, shutdowner Shutdowner, log Logger) CloseResult {
	c.once.Do(func() {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), window)
			defer cancel()
			if err := closer.Close(ctx); err != nil {
				c.result.Closer = err
				if log != nil {
					log("api server close error: %v", err)
				}
			}
		}()
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), window)
			defer cancel()
			if err := shutdowner.Shutdown(ctx); err != nil {
				c.result.Shutdowner = err
				if log != nil {
					log("server shutdown error: %v", err)
				}
			}
		}()
		wg.Wait()
	})
	return c.result
}
