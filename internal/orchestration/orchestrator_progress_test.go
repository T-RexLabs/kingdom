package orchestration

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"testing"

	"kingdom/internal/testutil"
)

func TestDurableProgressCloseJoinsInFlightTimerFailure(t *testing.T) {
	writer, db := newProgressWriterFixture(t)
	mustExec(t, db, `CREATE TRIGGER reject_stream_progress_events BEFORE INSERT ON events WHEN NEW.reason_code = 'stream_event' BEGIN SELECT RAISE(FAIL, 'stream progress persistence unavailable'); END`)
	ctx, cancel := testDeadlineContext(t)
	defer cancel()
	release := blockProgressTimerFlush(t, ctx, writer, "still working")
	defer release()
	closed := startProgressClose(t, func() error { return closeDurableProgressWriter(writer, nil) })
	waitProgressWriterState(t, ctx, writer, func() bool { return writer.closed })
	select {
	case err := <-closed:
		t.Fatalf("provider returned before its pending progress flush completed: %v", err)
	default:
	}
	release()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("best-effort progress failure changed provider success: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("progress close did not complete: %v", ctx.Err())
	}
	writer.mu.Lock()
	flushErr := writer.err
	writer.mu.Unlock()
	if flushErr == nil || !strings.Contains(flushErr.Error(), "stream progress persistence unavailable") {
		t.Fatalf("close did not retain the pending insert failure: %v", flushErr)
	}
	if err := closeDurableProgressWriter(writer, nil); err != nil {
		t.Fatalf("repeated progress close changed provider success: %v", err)
	}
	if count := countTestRows(t, db, `SELECT COUNT(*) FROM events WHERE reason_code = 'runtime_completion_observation' AND json_extract(payload_json, '$.phase') = 'durable_progress_flush_failed'`); count != 1 {
		t.Fatalf("progress failure diagnostics=%d, want 1", count)
	}
}

func TestDurableProgressCloseJoinsSuccessfulTimerFlush(t *testing.T) {
	writer, db := newProgressWriterFixture(t)
	ctx, cancel := testDeadlineContext(t)
	defer cancel()
	release := blockProgressTimerFlush(t, ctx, writer, "first")
	defer release()
	closed := startProgressClose(t, writer.Close)
	waitProgressWriterState(t, ctx, writer, func() bool { return writer.closed })
	select {
	case err := <-closed:
		t.Fatalf("close returned before its pending progress insert completed: %v", err)
	default:
	}
	release()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("close successful progress flush: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("progress close did not complete: %v", ctx.Err())
	}
	if count := countTestRows(t, db, `SELECT COUNT(*) FROM events WHERE reason_code = 'stream_event'`); count != 1 {
		t.Fatalf("durable progress events after close=%d, want 1", count)
	}
}

func TestDurableProgressSerializesBatchesAndIgnoresWritesAfterClose(t *testing.T) {
	writer, db := newProgressWriterFixture(t)
	ctx, cancel := testDeadlineContext(t)
	defer cancel()
	release := blockProgressTimerFlush(t, ctx, writer, "first")
	defer release()
	writer.Write("second")
	closed := startProgressClose(t, writer.Close)
	waitProgressWriterState(t, ctx, writer, func() bool { return writer.closed })
	release()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("close ordered progress batches: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("progress close did not complete: %v", ctx.Err())
	}
	writer.Write("ignored after close")
	if err := writer.Flush(); err != nil {
		t.Fatalf("flush closed writer: %v", err)
	}
	rows, err := db.QueryContext(ctx, `SELECT json_extract(payload_json, '$.text') FROM events WHERE reason_code = 'stream_event' ORDER BY seq`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var texts []string
	for rows.Next() {
		var text string
		if err := rows.Scan(&text); err != nil {
			t.Fatal(err)
		}
		texts = append(texts, text)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(texts) != 2 || texts[0] != "first" || texts[1] != "second" {
		t.Fatalf("durable batch order=%q, want [first second]", texts)
	}
}

func newProgressWriterFixture(t *testing.T) (*bufferedExecutionProgressWriter, *sql.DB) {
	t.Helper()
	db := openMigratedTestDB(t)
	seedMinimumGraph(t, db)
	rt, err := newMemoryRuntime(t, Config{DB: db, Paths: DefaultBootstrapPaths(repoRoot(t))})
	if err != nil {
		t.Fatal(err)
	}
	job := asyncExecutionResult{
		attemptID: "run_1:work_intake:1",
		run:       runSnapshot{RunID: "run_1"},
		node:      nodeDefinition{NodeID: "work_intake", RequiredRole: "governance"},
		attempt:   nodeAttempt{NodeRunID: "nr_1", NodeID: "work_intake", Attempt: 1, Wave: 1},
	}
	writer := rt.orchestrator.newBufferedExecutionProgressWriter(t.Context(), job)
	// This manually constructed writer is not part of runtime async admission.
	// Join its timer before runtime and database cleanup, including on failures.
	t.Cleanup(func() { _ = writer.Close() })
	return writer, db
}

func startProgressClose(t *testing.T, closeWriter func() error) <-chan error {
	t.Helper()
	closed := make(chan error, 1)
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		closed <- closeWriter()
	}()
	// The test defers its barrier release before this join can run.
	t.Cleanup(func() { <-exited })
	return closed
}

// The real timer consumes its batch, then blocks at the event sequence lock.
// Closing the writer must join that insert rather than treating its now-empty
// buffer as proof that all progress work has completed.
func blockProgressTimerFlush(t *testing.T, ctx context.Context, writer *bufferedExecutionProgressWriter, text string) func() {
	t.Helper()
	writer.loop.eventSeqMu.Lock()
	var once sync.Once
	release := func() { once.Do(writer.loop.eventSeqMu.Unlock) }
	t.Cleanup(release)
	writer.Write(text)
	waitProgressWriterState(t, ctx, writer, func() bool { return writer.buffer.Len() == 0 && writer.timer == nil })
	return release
}

func waitProgressWriterState(t *testing.T, ctx context.Context, writer *bufferedExecutionProgressWriter, condition func() bool) {
	t.Helper()
	for {
		writer.mu.Lock()
		ready := condition()
		writer.mu.Unlock()
		if ready {
			return
		}
		if err := sleepWithContext(ctx, nil, testutil.DefaultPollInterval); err != nil {
			t.Fatalf("progress writer did not reach the controlled state: %v", err)
		}
	}
}
