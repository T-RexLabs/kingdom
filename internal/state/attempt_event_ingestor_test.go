package state

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/core/rolecomm"
)

func TestAttemptEventIngestorDeduplicatesByAttemptAndSequence(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	ingestor := NewAttemptEventIngestor(db, loadStateTestSchemas(t))
	ctx := context.Background()

	request := baseAttemptEventIngestionRequest()
	request.EventID = "evt_attempt_dedupe_1"
	request.AttemptID = "att_dedupe"
	request.Seq = 1

	if _, err := ingestor.Ingest(ctx, request); err != nil {
		t.Fatalf("ingest first event: %v", err)
	}

	dup := request
	dup.EventID = "evt_attempt_dedupe_2"

	result, err := ingestor.Ingest(ctx, dup)
	if err != nil {
		t.Fatalf("ingest duplicate event: %v", err)
	}
	if !result.Deduplicated {
		t.Fatal("expected duplicate ingest to return deduplicated=true")
	}

	if got := countAttemptEventsBySeq(t, db, "att_dedupe", 1); got != 1 {
		t.Fatalf("expected one deduped event row for seq=1, got %d", got)
	}
}

func TestAttemptEventDedupeConflictRollbackPreservesRunEventSequence(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	ingestor := NewAttemptEventIngestor(db, loadStateTestSchemas(t))
	ctx := context.Background()

	request := baseAttemptEventIngestionRequest()
	request.EventID = "evt_attempt_dedupe_conflict_1"
	request.AttemptID = "att_dedupe_conflict"
	request.Seq = 1

	if _, err := ingestor.Ingest(ctx, request); err != nil {
		t.Fatalf("ingest first event: %v", err)
	}

	baselineMaxSeq := queryRunEventSequenceMax(t, db, request.RunID)
	baselineStoredSeq := queryStoredRunEventSequence(t, db, request.RunID)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}

	dup := request
	dup.EventID = "evt_attempt_dedupe_conflict_2"
	err = appendAttemptEventTx(ctx, tx, loadStateTestSchemas(t), rolecomm.Policy{}, dup, attemptEventAppendOptions{mirrorToMailbox: true})
	if err == nil {
		_ = tx.Rollback()
		t.Fatal("expected duplicate conflict from appendAttemptEventTx")
	}
	if !isAttemptEventDedupeConstraint(err) {
		_ = tx.Rollback()
		t.Fatalf("expected dedupe constraint error, got %v", err)
	}

	if err := rollbackAttemptEventDedupeConflict(ctx, db, tx, dup.AttemptID, dup.Seq); err != nil {
		t.Fatalf("rollback dedupe conflict: %v", err)
	}

	if got := queryRunEventSequenceMax(t, db, request.RunID); got != baselineMaxSeq {
		t.Fatalf("expected max persisted run_event_seq to remain %d after dedupe rollback, got %d", baselineMaxSeq, got)
	}
	if got := queryStoredRunEventSequence(t, db, request.RunID); got != baselineStoredSeq {
		t.Fatalf("expected stored run_event_sequences.last_seq to remain %d after dedupe rollback, got %d", baselineStoredSeq, got)
	}

	verifier := NewAuditIntegrityVerifier(db)
	if err := verifier.VerifyOrError(ctx); err != nil {
		t.Fatalf("expected audit verifier to pass after dedupe rollback: %v", err)
	}
}

func TestAttemptEventIngestorEmitsSequenceGapEvent(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	ingestor := NewAttemptEventIngestor(db, loadStateTestSchemas(t))
	ctx := context.Background()

	first := baseAttemptEventIngestionRequest()
	first.EventID = "evt_attempt_gap_1"
	first.AttemptID = "att_gap"
	first.Seq = 1

	if _, err := ingestor.Ingest(ctx, first); err != nil {
		t.Fatalf("ingest first event: %v", err)
	}

	gap := baseAttemptEventIngestionRequest()
	gap.EventID = "evt_attempt_gap_3"
	gap.AttemptID = "att_gap"
	gap.Seq = 3

	result, err := ingestor.Ingest(ctx, gap)
	if err == nil {
		t.Fatal("expected sequence gap error")
	}

	var gapErr *EventSequenceGapError
	if !errors.As(err, &gapErr) {
		t.Fatalf("expected EventSequenceGapError, got %T: %v", err, err)
	}
	if gapErr.Code != string(errorcatalog.CodeEventSequenceGap) {
		t.Fatalf("unexpected sequence gap code %q", gapErr.Code)
	}
	if !result.GapDetected {
		t.Fatal("expected gap_detected=true when sequence jumps")
	}

	gapEventType, gapReasonCode := queryAttemptEventTypeAndReason(t, db, "att_gap", 2)
	if gapEventType != AttemptIngestGapEventType {
		t.Fatalf("expected gap event type %q, got %q", AttemptIngestGapEventType, gapEventType)
	}
	if gapReasonCode != string(errorcatalog.CodeEventSequenceGap) {
		t.Fatalf("expected gap reason code %q, got %q", errorcatalog.CodeEventSequenceGap, gapReasonCode)
	}

	if got := countAttemptEventsBySeq(t, db, "att_gap", 3); got != 1 {
		t.Fatalf("expected incoming seq=3 event persisted, got %d", got)
	}
}

func TestAttemptEventIngestorEnforcesTerminalAttemptEvent(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	ingestor := NewAttemptEventIngestor(db, nil)
	ctx := context.Background()

	start := baseAttemptEventIngestionRequest()
	start.EventID = "evt_attempt_terminal_start"
	start.AttemptID = "att_terminal"
	start.EventType = "attempt.started"
	start.Seq = 1

	if _, err := ingestor.Ingest(ctx, start); err != nil {
		t.Fatalf("ingest start event: %v", err)
	}

	progress := start
	progress.EventID = "evt_attempt_terminal_progress"
	progress.EventType = "attempt.progress"
	progress.Seq = 2

	if _, err := ingestor.Ingest(ctx, progress); err != nil {
		t.Fatalf("ingest progress event: %v", err)
	}

	err := ingestor.EnsureTerminalEvent(ctx, "att_terminal")
	if err == nil {
		t.Fatal("expected missing terminal event error")
	}

	var missingErr *MissingTerminalAttemptEventError
	if !errors.As(err, &missingErr) {
		t.Fatalf("expected MissingTerminalAttemptEventError, got %T", err)
	}

	terminal := start
	terminal.EventID = "evt_attempt_terminal_done"
	terminal.EventType = "attempt.succeeded"
	terminal.Seq = 3

	if _, err := ingestor.Ingest(ctx, terminal); err != nil {
		t.Fatalf("ingest terminal event: %v", err)
	}

	if err := ingestor.EnsureTerminalEvent(ctx, "att_terminal"); err != nil {
		t.Fatalf("expected terminal guarantee to pass, got %v", err)
	}

	cancelled := baseAttemptEventIngestionRequest()
	cancelled.EventID = "evt_attempt_terminal_cancelled"
	cancelled.AttemptID = "att_terminal_cancelled"
	cancelled.EventType = "attempt.cancelled"
	cancelled.Seq = 1

	if _, err := ingestor.Ingest(ctx, cancelled); err != nil {
		t.Fatalf("ingest cancelled terminal event: %v", err)
	}

	if err := ingestor.EnsureTerminalEvent(ctx, "att_terminal_cancelled"); err != nil {
		t.Fatalf("expected cancelled terminal event to satisfy guarantee, got %v", err)
	}
}

func TestAttemptEventIngestorProjectsRunActivityButNotCollectionsForRetryEvents(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	ingestor := NewAttemptEventIngestor(db, nil)
	ctx := context.Background()

	retry := baseAttemptEventIngestionRequest()
	retry.EventID = "evt_attempt_retry_projected"
	retry.EventType = "attempt.retrying"
	retry.Seq = 1
	retry.PayloadJSON = `{"node_id":"kickoff","node_run_id":"nr_1","retry_index":2,"retry_max":3,"delay_seconds":10,"reason_code":"provider_empty_output_retry"}`
	retry.CreatedAt = "2026-03-01T12:05:00Z"

	if _, err := ingestor.Ingest(ctx, retry); err != nil {
		t.Fatalf("ingest retry event: %v", err)
	}

	var lastActivityAt string
	if err := db.QueryRow(`SELECT last_activity_at FROM run_activity WHERE run_id = 'run_1'`).Scan(&lastActivityAt); err != nil {
		t.Fatalf("query run activity: %v", err)
	}
	if lastActivityAt != retry.CreatedAt {
		t.Fatalf("expected run last_activity_at=%s, got %s", retry.CreatedAt, lastActivityAt)
	}

	var projectedCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM run_collection_events WHERE event_id = ? AND event_type = ?`, retry.EventID, retry.EventType).Scan(&projectedCount); err != nil {
		t.Fatalf("query projected collection event: %v", err)
	}
	if projectedCount != 0 {
		t.Fatalf("expected retry activity to stay out of collection events, got %d projected rows", projectedCount)
	}
}

func TestAttemptEventIngestorProjectsMeaningfulProgressIntoRunActivityOnly(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	ingestor := NewAttemptEventIngestor(db, loadStateTestSchemas(t))
	ctx := context.Background()

	var baseline string
	if err := db.QueryRow(`SELECT last_activity_at FROM run_activity WHERE run_id = 'run_1'`).Scan(&baseline); err != nil {
		t.Fatalf("query baseline run activity: %v", err)
	}

	progress := baseAttemptEventIngestionRequest()
	progress.EventID = "evt_attempt_progress_activity"
	progress.AttemptID = "att_progress_activity"
	progress.EventType = "attempt.progress"
	progress.Seq = 1
	progress.ReasonCode = "stream_event"
	progress.ReasonText = "stream event ingested"
	progress.PayloadJSON = `{"text":"builder produced new output"}`
	progress.CreatedAt = "2026-03-01T12:06:00Z"

	if _, err := ingestor.Ingest(ctx, progress); err != nil {
		t.Fatalf("ingest meaningful progress event: %v", err)
	}

	var lastActivityAt string
	if err := db.QueryRow(`SELECT last_activity_at FROM run_activity WHERE run_id = 'run_1'`).Scan(&lastActivityAt); err != nil {
		t.Fatalf("query run activity: %v", err)
	}
	if lastActivityAt != progress.CreatedAt {
		t.Fatalf("expected run last_activity_at=%s, got %s (baseline=%s)", progress.CreatedAt, lastActivityAt, baseline)
	}

	var projectedCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM run_collection_events WHERE event_id = ? AND event_type = ?`, progress.EventID, progress.EventType).Scan(&projectedCount); err != nil {
		t.Fatalf("query projected collection event: %v", err)
	}
	if projectedCount != 0 {
		t.Fatalf("expected meaningful progress to stay out of collection events, got %d projected rows", projectedCount)
	}
}

func TestAttemptEventIngestorSuppressesSyntheticProgressFromRunActivityAndCollections(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	ingestor := NewAttemptEventIngestor(db, loadStateTestSchemas(t))
	ctx := context.Background()

	var baseline string
	if err := db.QueryRow(`SELECT last_activity_at FROM run_activity WHERE run_id = 'run_1'`).Scan(&baseline); err != nil {
		t.Fatalf("query baseline run activity: %v", err)
	}

	progress := baseAttemptEventIngestionRequest()
	progress.EventID = "evt_attempt_progress_observation"
	progress.AttemptID = "att_progress_observation"
	progress.EventType = "attempt.progress"
	progress.Seq = 1
	progress.ReasonCode = "runtime_observation"
	progress.ReasonText = "running attempt telemetry snapshot"
	progress.PayloadJSON = `{"execution_diagnostics":{"event_count":1}}`
	progress.CreatedAt = "2026-03-01T12:07:00Z"

	if _, err := ingestor.Ingest(ctx, progress); err != nil {
		t.Fatalf("ingest synthetic progress event: %v", err)
	}

	var lastActivityAt string
	if err := db.QueryRow(`SELECT last_activity_at FROM run_activity WHERE run_id = 'run_1'`).Scan(&lastActivityAt); err != nil {
		t.Fatalf("query run activity: %v", err)
	}
	if lastActivityAt != baseline {
		t.Fatalf("expected synthetic progress to preserve last_activity_at=%s, got %s", baseline, lastActivityAt)
	}

	var projectedCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM run_collection_events WHERE event_id = ?`, progress.EventID).Scan(&projectedCount); err != nil {
		t.Fatalf("query projected collection event: %v", err)
	}
	if projectedCount != 0 {
		t.Fatalf("expected synthetic progress to skip collection projection, got %d events", projectedCount)
	}
}

func TestAttemptEventErrorsAndConstraintHelpers(t *testing.T) {
	t.Parallel()

	gapErr := (&EventSequenceGapError{Code: "event_sequence_gap", AttemptID: "att_1", ExpectedSeq: 2, ObservedSeq: 3}).Error()
	if gapErr != `event_sequence_gap: attempt="att_1" expected_seq=2 observed_seq=3` {
		t.Fatalf("unexpected gap error: %q", gapErr)
	}

	missingNoEvents := (&MissingTerminalAttemptEventError{AttemptID: "att_2"}).Error()
	if missingNoEvents != `missing terminal attempt event for "att_2": attempt has no events` {
		t.Fatalf("unexpected no-events terminal message: %q", missingNoEvents)
	}

	missingWithLast := (&MissingTerminalAttemptEventError{AttemptID: "att_2", LastEventType: "attempt.progress", LastEventSeq: 5}).Error()
	if missingWithLast != `missing terminal attempt event for "att_2": last event "attempt.progress" at seq=5` {
		t.Fatalf("unexpected last-event terminal message: %q", missingWithLast)
	}

	if isAttemptEventDedupeConstraint(errors.New("random error")) {
		t.Fatal("expected dedupe helper to ignore unrelated errors")
	}
}

func TestShouldMirrorAttemptEventToMailboxFiltersRuntimeTelemetry(t *testing.T) {
	t.Parallel()

	if shouldMirrorAttemptEventToMailbox(AttemptEventIngestionRequest{ReasonCode: "runtime_observation"}) {
		t.Fatal("expected runtime_observation events to skip mailbox mirroring")
	}
	if shouldMirrorAttemptEventToMailbox(AttemptEventIngestionRequest{ReasonCode: "runtime_completion_observation"}) {
		t.Fatal("expected runtime_completion_observation events to skip mailbox mirroring")
	}
	if !shouldMirrorAttemptEventToMailbox(AttemptEventIngestionRequest{ReasonCode: "orchestration_progress"}) {
		t.Fatal("expected non-telemetry events to mirror into mailbox")
	}
}

func baseAttemptEventIngestionRequest() AttemptEventIngestionRequest {
	return AttemptEventIngestionRequest{
		EventID:         "evt_attempt_1",
		RunID:           "run_1",
		NodeRunID:       sql.NullString{String: "nr_1", Valid: true},
		AttemptID:       "att_attempt",
		SchemaVersion:   "v1",
		EventType:       "attempt.progress",
		Seq:             1,
		PayloadJSON:     `{"message":"ok"}`,
		ActorID:         "actor_1",
		RoleID:          "king",
		AuthoritySource: "system",
		ReasonCode:      "internal_error",
		ReasonText:      "event ingest test",
		CorrelationID:   "corr_attempt",
		CausationID:     "caus_attempt",
		PrevHash:        "prev_attempt",
		EntryHash:       "entry_attempt",
		CreatedAt:       "2026-03-01T12:00:00Z",
	}
}

func countAttemptEventsBySeq(t *testing.T, db *sql.DB, attemptID string, seq int) int {
	t.Helper()

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE attempt_id = ? AND seq = ?`, attemptID, seq).Scan(&count); err != nil {
		t.Fatalf("count attempt events by seq: %v", err)
	}

	return count
}

func queryAttemptEventTypeAndReason(t *testing.T, db *sql.DB, attemptID string, seq int) (string, string) {
	t.Helper()

	var eventType string
	var reasonCode string
	if err := db.QueryRow(`SELECT event_type, reason_code FROM events WHERE attempt_id = ? AND seq = ?`, attemptID, seq).Scan(&eventType, &reasonCode); err != nil {
		t.Fatalf("query attempt event type and reason: %v", err)
	}

	return eventType, reasonCode
}

func queryRunEventSequenceMax(t *testing.T, db *sql.DB, runID string) int {
	t.Helper()

	var seq int
	if err := db.QueryRow(`SELECT COALESCE(MAX(run_event_seq), 0) FROM events WHERE run_id = ?`, runID).Scan(&seq); err != nil {
		t.Fatalf("query max run_event_seq: %v", err)
	}
	return seq
}

func queryStoredRunEventSequence(t *testing.T, db *sql.DB, runID string) int {
	t.Helper()

	var seq int
	if err := db.QueryRow(`SELECT last_seq FROM run_event_sequences WHERE run_id = ?`, runID).Scan(&seq); err != nil {
		t.Fatalf("query stored run event sequence: %v", err)
	}
	return seq
}
