package planning

import (
	"context"
	"strings"
	"sync"
	"time"

	"kingdom/internal/acp"
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
	payload      map[string]any
	timer       *time.Timer
	buffer      strings.Builder
	closed      bool
	err         error
}

func newBufferedOperationTextWriter(
	ctx context.Context,
	operationID string,
	cursor *sequenceCursor,
	payload map[string]any,
	appendEvent appendOperationEventFunc,
) *bufferedOperationTextWriter {
	return &bufferedOperationTextWriter{
		ctx:         ctx,
		operationID: strings.TrimSpace(operationID),
		cursor:      cursor,
		payload:     cloneOperationEventPayload(payload),
		appendEvent: appendEvent,
	}
}

func (w *bufferedOperationTextWriter) Write(text string) {
	if w == nil || text == "" {
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
	payload := cloneOperationEventPayload(w.payload)
	appendEvent := w.appendEvent
	w.mu.Unlock()

	if text == "" {
		return nil
	}
	payload["text"] = text
	_, err := appendEvent(ctx, operationID, cursor, acp.MessagePartEventType, payload)
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

func cloneOperationEventPayload(payload map[string]any) map[string]any {
	if len(payload) == 0 {
		return map[string]any{}
	}
	cloned := make(map[string]any, len(payload))
	for key, value := range payload {
		cloned[key] = value
	}
	return cloned
}
