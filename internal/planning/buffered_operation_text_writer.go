package planning

import (
	"context"
	"strings"
	"sync"
	"time"

	"kingdom/internal/state"
)

const (
	plannerTextDeltaBatchWindow = 40 * time.Millisecond
	plannerTextDeltaBatchChars  = 192
)

type appendOperationEventFunc func(context.Context, string, *sequenceCursor, string, any) (state.PlanningOperationEvent, error)

// bufferedOperationTextWriter batches adjacent text deltas into small durable
// writes so provider stdout consumption is not coupled 1:1 to SQLite latency.
// Replay semantics stay intact because flushed batches still land as ordered
// `text_delta` events in the canonical planner event log.
type bufferedOperationTextWriter struct {
	mu          sync.Mutex
	ctx         context.Context
	operationID string
	cursor      *sequenceCursor
	appendEvent appendOperationEventFunc
	timer       *time.Timer
	buffer      strings.Builder
	closed      bool
	err         error
}

func newBufferedOperationTextWriter(
	ctx context.Context,
	operationID string,
	cursor *sequenceCursor,
	appendEvent appendOperationEventFunc,
) *bufferedOperationTextWriter {
	return &bufferedOperationTextWriter{
		ctx:         ctx,
		operationID: strings.TrimSpace(operationID),
		cursor:      cursor,
		appendEvent: appendEvent,
	}
}

func (w *bufferedOperationTextWriter) Write(text string) {
	if w == nil || strings.TrimSpace(text) == "" {
		return
	}

	flushNow := false
	w.mu.Lock()
	if w.closed || w.err != nil {
		w.mu.Unlock()
		return
	}
	_, _ = w.buffer.WriteString(text)
	if w.buffer.Len() >= plannerTextDeltaBatchChars {
		flushNow = true
		if w.timer != nil {
			w.timer.Stop()
			w.timer = nil
		}
	} else if w.timer == nil {
		w.timer = time.AfterFunc(plannerTextDeltaBatchWindow, func() {
			_ = w.Flush()
		})
	}
	w.mu.Unlock()

	if flushNow {
		_ = w.Flush()
	}
}

func (w *bufferedOperationTextWriter) Flush() error {
	if w == nil {
		return nil
	}

	w.mu.Lock()
	if w.timer != nil {
		w.timer.Stop()
		w.timer = nil
	}
	if w.err != nil {
		err := w.err
		w.mu.Unlock()
		return err
	}
	text := w.buffer.String()
	w.buffer.Reset()
	ctx := w.ctx
	operationID := w.operationID
	cursor := w.cursor
	appendEvent := w.appendEvent
	w.mu.Unlock()

	if text == "" {
		return nil
	}
	_, err := appendEvent(ctx, operationID, cursor, "text_delta", map[string]any{"text": text})
	if err != nil {
		w.mu.Lock()
		if w.err == nil {
			w.err = err
		}
		w.mu.Unlock()
	}
	return err
}

func (w *bufferedOperationTextWriter) Close() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	return w.Flush()
}
