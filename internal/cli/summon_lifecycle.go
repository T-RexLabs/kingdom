package cli

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"
)

// summonWriter serializes the access log, startup watcher, and shutdown owner
// for one invocation. It does not hold any runtime or authority lock.
type summonWriter struct {
	mu  sync.Mutex
	out io.Writer
}

func (w *summonWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.out == nil {
		return len(p), nil
	}
	return w.out.Write(p)
}

// startSummonWarm never writes caller-owned output from its worker. This keeps
// diagnostics safe even if a noncooperative provider exceeds the bounded join.
func startSummonWarm(ctx context.Context, warm func(context.Context)) (<-chan time.Duration, func() error) {
	warmCtx, cancel := context.WithTimeout(ctx, summonProviderWarmTimeout)
	result := make(chan time.Duration, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer cancel()
		start := time.Now()
		warm(warmCtx)
		result <- time.Since(start)
	}()
	stop := func() error {
		cancel()
		timer := time.NewTimer(time.Second)
		defer timer.Stop()
		select {
		case <-done:
			return nil
		case <-timer.C:
			return errors.New("provider warm did not stop after cancellation")
		}
	}
	return result, stop
}
