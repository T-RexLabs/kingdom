package orchestration

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"
)

const (
	executionStreamEventBatchWindow = 40 * time.Millisecond
	executionStreamEventBatchChars  = 192
)

type bufferedExecutionProgressWriter struct {
	mu         sync.Mutex
	flushMu    sync.Mutex
	reportOnce sync.Once
	ctx        context.Context
	loop       *orchestrationLoop
	result     asyncExecutionResult
	timer      *time.Timer
	buffer     strings.Builder
	closed     bool
	err        error
}

func (o *orchestrationLoop) executeWithDurableProgress(ctx context.Context, result asyncExecutionResult, req opencode.ExecuteRequest) (opencode.ExecuteResponse, error) {
	req, writer := o.attachDurableProgressWriter(ctx, result, req)
	response, err := o.adapter.Execute(ctx, req)
	return response, closeDurableProgressWriter(writer, err)
}

func (o *orchestrationLoop) attachDurableProgressWriter(ctx context.Context, result asyncExecutionResult, req opencode.ExecuteRequest) (opencode.ExecuteRequest, *bufferedExecutionProgressWriter) {
	writer := o.newBufferedExecutionProgressWriter(ctx, result)
	if writer != nil {
		existing := req.OnProgress
		req.OnProgress = func(text string) {
			if existing != nil {
				existing(text)
			}
			writer.Write(text)
		}
	}
	return req, writer
}

func (o *orchestrationLoop) newBufferedExecutionProgressWriter(ctx context.Context, result asyncExecutionResult) *bufferedExecutionProgressWriter {
	if o == nil || o.events == nil || strings.TrimSpace(result.attemptID) == "" {
		return nil
	}
	return &bufferedExecutionProgressWriter{
		ctx:    context.WithoutCancel(firstNonNilContext(ctx)),
		loop:   o,
		result: result,
	}
}

func closeDurableProgressWriter(writer *bufferedExecutionProgressWriter, err error) error {
	if writer == nil {
		return err
	}
	if flushErr := writer.Close(); flushErr != nil {
		writer.reportError(flushErr)
	}
	return err
}

func (w *bufferedExecutionProgressWriter) Write(text string) {
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
	if w.buffer.Len() >= executionStreamEventBatchChars {
		flushNow = true
		if w.timer != nil {
			w.timer.Stop()
			w.timer = nil
		}
	} else if w.timer == nil {
		w.timer = time.AfterFunc(executionStreamEventBatchWindow, func() {
			_ = w.Flush()
		})
	}
	w.mu.Unlock()

	if flushNow {
		_ = w.Flush()
	}
}

func (w *bufferedExecutionProgressWriter) Flush() error {
	if w == nil {
		return nil
	}
	// A timer may have consumed the buffer while its insert is still pending.
	// Serialize the whole flush so Close joins that insert and sees its error.
	w.flushMu.Lock()
	defer w.flushMu.Unlock()

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
	loop := w.loop
	result := w.result
	w.mu.Unlock()

	if text == "" {
		return nil
	}
	if err := loop.appendDurableStreamProgress(ctx, result, text); err != nil {
		w.mu.Lock()
		if w.err == nil {
			w.err = err
		}
		w.mu.Unlock()
		return err
	}
	return nil
}

func (w *bufferedExecutionProgressWriter) Close() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	return w.Flush()
}

func (w *bufferedExecutionProgressWriter) reportError(err error) {
	if w == nil || err == nil {
		return
	}
	w.reportOnce.Do(func() { w.reportErrorOnce(err) })
}

func (w *bufferedExecutionProgressWriter) reportErrorOnce(err error) {
	w.mu.Lock()
	ctx := w.ctx
	loop := w.loop
	result := w.result
	w.mu.Unlock()

	attemptID := strings.TrimSpace(result.attemptID)
	slog.Warn("failed to persist durable execution stream progress", "attempt_id", attemptID, "error", err)
	if loop == nil {
		return
	}
	now := time.Now().UTC().Format(time.RFC3339)
	if loop.now != nil {
		now = loop.now().UTC().Format(time.RFC3339)
	}
	loop.emitCompletionLifecycleObservation(ctx, result, "durable_progress_flush_failed", now, map[string]any{"error": err.Error()})
}

func (o *orchestrationLoop) appendDurableStreamProgress(ctx context.Context, result asyncExecutionResult, text string) error {
	if o == nil || o.events == nil || strings.TrimSpace(result.attemptID) == "" || text == "" {
		return nil
	}

	payloadRaw, err := marshalAsyncExecutionAttemptEventPayload(result, "stream_event", map[string]any{"text": text})
	if err != nil {
		return fmt.Errorf("marshal stream progress payload: %w", err)
	}

	occurredAt := o.now().UTC().Format(time.RFC3339Nano)
	o.eventSeqMu.Lock()
	defer o.eventSeqMu.Unlock()
	seq := o.nextEventSeq(ctx, result.attemptID)
	_, err = o.events.Ingest(ctx, state.AttemptEventIngestionRequest{
		EventID:         fmt.Sprintf("evt_orch_%s_%s_%d_stream_%d", sanitizeFileToken(result.run.RunID), sanitizeFileToken(result.node.NodeID), result.attempt.Attempt, seq),
		RunID:           result.run.RunID,
		NodeRunID:       sql.NullString{String: result.attempt.NodeRunID, Valid: strings.TrimSpace(result.attempt.NodeRunID) != ""},
		AttemptID:       result.attemptID,
		SchemaVersion:   "v1",
		EventType:       opencode.AttemptEventTypeProgress,
		Seq:             seq,
		PayloadJSON:     payloadRaw,
		ActorID:         "runtime_orchestrator",
		RoleID:          result.node.RequiredRole,
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      "stream_event",
		ReasonText:      "stream event ingested",
		CorrelationID:   "corr_orch_" + result.run.RunID,
		CausationID:     fmt.Sprintf("caus_orch_%s_%s_stream_%d", result.run.RunID, result.attempt.NodeRunID, seq),
		CreatedAt:       occurredAt,
	})
	if err != nil {
		return fmt.Errorf("ingest stream progress event for attempt %q: %w", result.attemptID, err)
	}
	return nil
}
