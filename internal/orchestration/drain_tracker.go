package orchestration

import (
	"context"
	"fmt"
	"sync"
)

type executionDrainTracker struct {
	mu     sync.Mutex
	active int
	idle   chan struct{}
}

func newExecutionDrainTracker() *executionDrainTracker {
	idle := make(chan struct{})
	close(idle)
	return &executionDrainTracker{idle: idle}
}

func (t *executionDrainTracker) Start() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.active == 0 {
		t.idle = make(chan struct{})
	}
	t.active++
}

func (t *executionDrainTracker) Done() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.active == 0 {
		return
	}
	t.active--
	if t.active == 0 {
		close(t.idle)
	}
}

func (t *executionDrainTracker) Wait(ctx context.Context) error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	active := t.active
	idle := t.idle
	t.mu.Unlock()
	if active == 0 {
		return nil
	}
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("wait for runtime execution drain: %w", ctx.Err())
	}
}
