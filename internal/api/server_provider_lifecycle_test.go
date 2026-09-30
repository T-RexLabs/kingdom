package api

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type contextPlannerCloser struct {
	testPlannerAgent
	started, release   chan struct{}
	once               sync.Once
	calls, legacyCalls atomic.Int32
}

func (c *contextPlannerCloser) Close() error { c.legacyCalls.Add(1); return nil }
func (c *contextPlannerCloser) CloseContext(ctx context.Context) error {
	c.calls.Add(1)
	c.once.Do(func() { close(c.started) })
	select {
	case <-c.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestServerPlannerClosePreservesDeadlineAndSingleOwnership(t *testing.T) {
	planner := &contextPlannerCloser{started: make(chan struct{}), release: make(chan struct{})}
	handler, _ := newAPIServerForTestWithConfig(t, ServerConfig{PlannerAgent: planner})
	server := handler.(*Server)
	server.plannerAdapterCloser = planner
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := server.Close(ctx)
	close(planner.release)
	if !errors.Is(err, context.DeadlineExceeded) || planner.calls.Load() != 1 || planner.legacyCalls.Load() != 0 {
		t.Fatalf("deadline lost: err=%v context=%d legacy=%d", err, planner.calls.Load(), planner.legacyCalls.Load())
	}
	if err := server.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if planner.calls.Load() != 2 {
		t.Fatalf("adapter was closed again outside service ownership: %d", planner.calls.Load())
	}
}

func TestServerForceCloseDoesNotRestartAdapterDrain(t *testing.T) {
	planner := &contextPlannerCloser{started: make(chan struct{}), release: make(chan struct{})}
	handler, _ := newAPIServerForTestWithConfig(t, ServerConfig{PlannerAgent: planner})
	server := handler.(*Server)
	server.plannerAdapterCloser = planner
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- server.Close(ctx) }()
	<-planner.started
	server.ForceClose()
	var err error
	overran := false
	select {
	case err = <-done:
	case <-time.After(250 * time.Millisecond):
		overran = true
	}
	close(planner.release)
	if overran {
		err = <-done
	}
	if err != nil || overran || planner.calls.Load() != 1 {
		t.Fatalf("force restarted adapter drain: err=%v overran=%v calls=%d", err, overran, planner.calls.Load())
	}
}

type countedPlannerCloser struct{ calls int }

func (c *countedPlannerCloser) Close() error {
	c.calls++
	if c.calls == 1 {
		return errors.New("transient scratch cleanup failure")
	}
	return nil
}

func TestServerOwnsPlannerAdapterCleanupAndRetriesFailure(t *testing.T) {
	closer := &countedPlannerCloser{}
	server := &Server{plannerAdapterCloser: closer}
	if err := server.Close(context.Background()); err == nil {
		t.Fatal("planner adapter close failure was lost")
	}
	if err := server.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := server.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if closer.calls != 2 {
		t.Fatalf("close calls=%d", closer.calls)
	}
}
