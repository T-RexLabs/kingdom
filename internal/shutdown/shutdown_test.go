package shutdown

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeCloser captures invocation count so the once-guard contract is
// observable. The supplied behaviour fn lets each test inject delay
// or errors without writing a new struct per case.
type fakeCloser struct {
	calls    atomic.Int32
	behavior func(ctx context.Context) error
}

func (f *fakeCloser) Close(ctx context.Context) error {
	f.calls.Add(1)
	if f.behavior == nil {
		return nil
	}
	return f.behavior(ctx)
}

type fakeShutdowner struct {
	calls    atomic.Int32
	behavior func(ctx context.Context) error
}

func (f *fakeShutdowner) Shutdown(ctx context.Context) error {
	f.calls.Add(1)
	if f.behavior == nil {
		return nil
	}
	return f.behavior(ctx)
}

// TestCoordinatorRunsBothConcurrently pins the headline contract: the
// two teardowns run in parallel, not serially. Both block for ~50ms;
// total wall clock should be ~50ms, not ~100ms. Wide tolerance because
// CI is noisy.
func TestCoordinatorRunsBothConcurrently(t *testing.T) {
	t.Parallel()

	closer := &fakeCloser{behavior: func(ctx context.Context) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	}}
	shutdowner := &fakeShutdowner{behavior: func(ctx context.Context) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	}}

	var c Coordinator
	start := time.Now()
	c.Run(5*time.Second, closer, shutdowner, nil)
	elapsed := time.Since(start)

	if elapsed > 200*time.Millisecond {
		t.Fatalf("expected concurrent execution (~50ms), got %v — likely serialised", elapsed)
	}
	if got := closer.calls.Load(); got != 1 {
		t.Fatalf("expected closer called exactly once, got %d", got)
	}
	if got := shutdowner.calls.Load(); got != 1 {
		t.Fatalf("expected shutdowner called exactly once, got %d", got)
	}
}

// TestCoordinatorBoundsHangingTeardown pins the deadline contract: a
// hung teardown does not block the whole shutdown forever — each
// goroutine has its own ctx with the supplied window.
func TestCoordinatorBoundsHangingTeardown(t *testing.T) {
	t.Parallel()

	releaseCloser := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-releaseCloser:
		default:
			close(releaseCloser)
		}
	})

	closer := &fakeCloser{behavior: func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-releaseCloser:
			return nil
		}
	}}
	shutdowner := &fakeShutdowner{behavior: func(ctx context.Context) error { return nil }}

	var c Coordinator
	start := time.Now()
	c.Run(50*time.Millisecond, closer, shutdowner, nil)
	elapsed := time.Since(start)

	if elapsed < 50*time.Millisecond {
		t.Fatalf("expected coordinator to wait at least the deadline (50ms), got %v", elapsed)
	}
	if elapsed > 500*time.Millisecond {
		t.Fatalf("expected coordinator to bound at the deadline (50ms + slack), got %v", elapsed)
	}
}

// TestCoordinatorRunIsOnce pins the safety-net contract: a second Run
// on the same Coordinator is a real no-op, not a silent re-execution.
// Without this, callers who chain "explicit signal-handler call" with
// "deferred safety call" would pay the wg-drain wall clock twice.
func TestCoordinatorRunIsOnce(t *testing.T) {
	t.Parallel()

	closer := &fakeCloser{}
	shutdowner := &fakeShutdowner{}

	var c Coordinator
	c.Run(time.Second, closer, shutdowner, nil)

	start := time.Now()
	c.Run(time.Second, closer, shutdowner, nil)
	secondCall := time.Since(start)

	if secondCall > 50*time.Millisecond {
		t.Fatalf("expected second Run to short-circuit, took %v", secondCall)
	}
	if got := closer.calls.Load(); got != 1 {
		t.Fatalf("expected closer called once across two Run invocations, got %d", got)
	}
	if got := shutdowner.calls.Load(); got != 1 {
		t.Fatalf("expected shutdowner called once across two Run invocations, got %d", got)
	}
}

// TestCoordinatorRoutesErrorsToLogger pins the diagnostic surface:
// teardown errors flow through the supplied logger rather than
// silently disappearing — without this, a failed shutdown is invisible
// to operators reading the wire.
func TestCoordinatorRoutesErrorsToLogger(t *testing.T) {
	t.Parallel()

	closer := &fakeCloser{behavior: func(ctx context.Context) error { return errors.New("close went wrong") }}
	shutdowner := &fakeShutdowner{behavior: func(ctx context.Context) error { return errors.New("shutdown went wrong") }}

	var (
		mu       sync.Mutex
		messages []string
	)
	logger := func(format string, args ...any) {
		// Both teardown goroutines may log concurrently; the mutex
		// keeps the slice mutation race-free without contaminating
		// the contract under test.
		mu.Lock()
		defer mu.Unlock()
		messages = append(messages, format)
	}

	var c Coordinator
	c.Run(time.Second, closer, shutdowner, logger)

	mu.Lock()
	defer mu.Unlock()
	if len(messages) != 2 {
		t.Fatalf("expected 2 logger calls (one per teardown), got %d: %v", len(messages), messages)
	}
}

// TestCoordinatorRunReturnsCloseResult pins the new return-value
// contract: callers can read per-subsystem errors and act on them
// (e.g. exit non-zero on planner drain failure).
func TestCoordinatorRunReturnsCloseResult(t *testing.T) {
	t.Parallel()

	closeErr := errors.New("close failed")
	shutdownErr := errors.New("shutdown failed")
	closer := &fakeCloser{behavior: func(ctx context.Context) error { return closeErr }}
	shutdowner := &fakeShutdowner{behavior: func(ctx context.Context) error { return shutdownErr }}

	var c Coordinator
	result := c.Run(time.Second, closer, shutdowner, nil)

	if !errors.Is(result.Closer, closeErr) {
		t.Fatalf("expected result.Closer = %v, got %v", closeErr, result.Closer)
	}
	if !errors.Is(result.Shutdowner, shutdownErr) {
		t.Fatalf("expected result.Shutdowner = %v, got %v", shutdownErr, result.Shutdowner)
	}
}

// TestCoordinatorRepeatedRunReturnsCachedResult pins the once-guard's
// stability contract: subsequent Run invocations return the same
// CloseResult without re-executing the underlying teardowns. Callers
// that defer Run as a safety net while *also* invoking it from a
// signal handler get a consistent error chain across both call sites.
func TestCoordinatorRepeatedRunReturnsCachedResult(t *testing.T) {
	t.Parallel()

	closeErr := errors.New("close failed once")
	closer := &fakeCloser{behavior: func(ctx context.Context) error { return closeErr }}
	shutdowner := &fakeShutdowner{behavior: func(ctx context.Context) error { return nil }}

	var c Coordinator
	first := c.Run(time.Second, closer, shutdowner, nil)
	second := c.Run(time.Second, closer, shutdowner, nil)

	if first.Closer != second.Closer {
		t.Fatalf("expected cached result; first=%v second=%v", first.Closer, second.Closer)
	}
	if got := closer.calls.Load(); got != 1 {
		t.Fatalf("expected closer called exactly once across repeated Run calls, got %d", got)
	}
}
