package orchestration

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type executionDrainTracker struct {
	mu     sync.Mutex
	active int
	idle   chan struct{}
	err    error
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

func (t *executionDrainTracker) RecordError(err error) {
	if t == nil || err == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.err = errors.Join(t.err, err)
}

func (t *executionDrainTracker) Wait(ctx context.Context) error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	active := t.active
	idle := t.idle
	err := t.err
	t.mu.Unlock()
	if active == 0 {
		return err
	}
	select {
	case <-idle:
		t.mu.Lock()
		defer t.mu.Unlock()
		return t.err
	case <-ctx.Done():
		return fmt.Errorf("wait for runtime execution drain: %w", ctx.Err())
	}
}
