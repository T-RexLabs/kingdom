package state

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"
)

func TestHasRecentTimeoutActivityTracksProviderActivity(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	insertExecutionActivityEvent(t, db, executionActivitySeed{
		EventID:    "evt_activity_provider",
		EventType:  "attempt.activity",
		ReasonCode: "provider_activity",
		Payload:    `{"kind":"kingdom.tool","label":"Processing"}`,
		CreatedAt:  "2026-03-01T12:00:20Z",
	})

	active, err := HasRecentTimeoutActivity(context.Background(), db, "nr_1", mustParseRFC3339(t, "2026-03-01T12:00:30Z"), 15*time.Second)
	if err != nil {
		t.Fatalf("has recent timeout activity: %v", err)
	}
	if !active {
		t.Fatal("expected provider activity to count as recent execution activity")
	}
}

func TestHasRecentTimeoutActivityIgnoresFutureDatedActivity(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	insertExecutionActivityEvent(t, db, executionActivitySeed{
		EventID:    "evt_activity_provider_future",
		EventType:  "attempt.activity",
		ReasonCode: "provider_activity",
		Payload:    `{"kind":"kingdom.tool","label":"Processing"}`,
		CreatedAt:  "2026-03-01T12:02:30Z",
	})

	active, err := HasRecentTimeoutActivity(context.Background(), db, "nr_1", mustParseRFC3339(t, "2026-03-01T12:00:30Z"), time.Hour)
	if err != nil {
		t.Fatalf("has recent timeout activity with future timestamp: %v", err)
	}
	if active {
		t.Fatal("expected future-dated provider activity to be ignored")
	}
}

func TestHasRecentTimeoutActivityTracksStreamProgress(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	insertExecutionActivityEvent(t, db, executionActivitySeed{
		EventID:    "evt_activity_progress",
		EventType:  "attempt.progress",
		ReasonCode: "stream_event",
		Payload:    `{"text":"still working"}`,
		ActorID:    "runtime",
		Authority:  "runtime.event_bridge",
		CreatedAt:  "2026-03-01T12:00:20Z",
	})

	active, err := HasRecentTimeoutActivity(context.Background(), db, "nr_1", mustParseRFC3339(t, "2026-03-01T12:00:30Z"), 15*time.Second)
	if err != nil {
		t.Fatalf("has recent timeout activity: %v", err)
	}
	if !active {
		t.Fatal("expected non-synthetic progress to count as recent execution activity")
	}
}

func TestHasRecentTimeoutActivityIgnoresRuntimeObservation(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	insertExecutionActivityEvent(t, db, executionActivitySeed{
		EventID:    "evt_activity_runtime_observation",
		EventType:  "attempt.progress",
		ReasonCode: "runtime_observation",
		Payload:    `{"execution_diagnostics":{"event_count":3,"execution_path_diagnostics_snapshot":{"stdout_event_count":7,"step_finish_seen":true,"last_stdout_event_at":"2026-03-01T12:00:20Z"}}}`,
		CreatedAt:  "2026-03-01T12:00:20Z",
	})

	active, err := HasRecentTimeoutActivity(context.Background(), db, "nr_1", mustParseRFC3339(t, "2026-03-01T12:00:30Z"), 15*time.Second)
	if err != nil {
		t.Fatalf("has recent timeout activity: %v", err)
	}
	if active {
		t.Fatal("expected runtime observation alone to be ignored as execution activity")
	}
}

func TestHasRecentTimeoutActivityTracksCompletionObservation(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	insertExecutionActivityEvent(t, db, executionActivitySeed{
		EventID:    "evt_activity_completion_observation",
		EventType:  "attempt.progress",
		ReasonCode: "runtime_completion_observation",
		Payload:    `{"phase":"provider_export_reconciliation_started"}`,
		CreatedAt:  "2026-03-01T12:00:20Z",
	})

	active, err := HasRecentTimeoutActivity(context.Background(), db, "nr_1", mustParseRFC3339(t, "2026-03-01T12:00:30Z"), 15*time.Second)
	if err != nil {
		t.Fatalf("has recent timeout activity: %v", err)
	}
	if !active {
		t.Fatal("expected completion observation to count as recent execution activity")
	}
}

func TestHasRecentTimeoutActivityHandlesFractionalSecondCutoff(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	insertExecutionActivityEvent(t, db, executionActivitySeed{
		EventID:    "evt_activity_fractional_second",
		EventType:  "attempt.activity",
		ReasonCode: "provider_activity",
		Payload:    `{"kind":"kingdom.tool","label":"Processing"}`,
		CreatedAt:  "2026-03-01T12:00:00.500Z",
	})

	active, err := HasRecentTimeoutActivity(context.Background(), db, "nr_1", mustParseRFC3339(t, "2026-03-01T12:00:01Z"), time.Second)
	if err != nil {
		t.Fatalf("has recent fractional-second timeout activity: %v", err)
	}
	if !active {
		t.Fatal("expected fractional-second provider activity after the cutoff to count as recent execution activity")
	}
}

func TestRecentExecutionActivityQueryUsesNodeRunIndex(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	const indexName = "idx_events_node_run_type_created_activity"
	if !indexExists(t, db, indexName) {
		t.Fatalf("expected index %q to exist", indexName)
	}
	assertPlanUsesIndex(t, db,
		`SELECT created_at, event_type, reason_code, payload_json
		 FROM events
		 WHERE node_run_id = ?
		   AND event_type IN ('attempt.activity', 'attempt.progress')
		   AND julianday(created_at) >= julianday(?)
		 ORDER BY julianday(created_at) DESC, run_event_seq DESC, seq DESC, event_id DESC`,
		[]any{"nr_1", "2026-03-01T12:00:00Z"},
		indexName,
	)
}

func TestHasRecentTimeoutActivityScansPastSyntheticObservationOverflow(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	insertExecutionActivityEvent(t, db, executionActivitySeed{
		EventID:    "evt_timeout_activity_provider_base",
		EventType:  "attempt.activity",
		ReasonCode: "provider_activity",
		Payload:    `{"kind":"kingdom.tool","label":"Processing"}`,
		CreatedAt:  "2026-03-01T12:00:00Z",
	})
	baseTime := mustParseRFC3339(t, "2026-03-01T12:00:00Z")
	for i := 0; i < 70; i++ {
		observedAt := baseTime.Add(time.Duration(i+1) * time.Second).Format(time.RFC3339)
		insertExecutionActivityEvent(t, db, executionActivitySeed{
			EventID:    fmt.Sprintf("evt_timeout_activity_observation_%02d", i),
			EventType:  "attempt.progress",
			ReasonCode: "runtime_observation",
			Payload:    `{"execution_diagnostics":{"event_count":3,"execution_path_diagnostics_snapshot":{"stdout_event_count":7}}}`,
			CreatedAt:  observedAt,
		})
	}

	active, err := HasRecentTimeoutActivity(context.Background(), db, "nr_1", mustParseRFC3339(t, "2026-03-01T12:01:11Z"), 2*time.Minute)
	if err != nil {
		t.Fatalf("has recent timeout activity after synthetic overflow: %v", err)
	}
	if !active {
		t.Fatal("expected timeout activity scan to find recent provider activity past synthetic observation overflow")
	}
}

func TestHasRecentExecutionLivenessTreatsRuntimeObservationWithLiveDiagnosticsAsRecent(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	insertExecutionActivityEvent(t, db, executionActivitySeed{
		EventID:    "evt_liveness_runtime_observation",
		EventType:  "attempt.progress",
		ReasonCode: "runtime_observation",
		Payload:    `{"execution_diagnostics":{"attempt_id":"run_1:decree_intake:1","event_count":2,"execution_path_diagnostics_snapshot":{"stdout_event_count":17,"last_stdout_event_at":"2026-03-01T12:00:20Z","step_finish_seen":true}}}`,
		CreatedAt:  "2026-03-01T12:00:20Z",
	})

	active, err := HasRecentExecutionLiveness(context.Background(), db, "nr_1", mustParseRFC3339(t, "2026-03-01T12:00:30Z"), 15*time.Second)
	if err != nil {
		t.Fatalf("has recent execution liveness: %v", err)
	}
	if !active {
		t.Fatal("expected runtime observation with live diagnostics to count as execution liveness")
	}
}

func TestHasRecentExecutionLivenessIgnoresRuntimeObservationWithoutLiveDiagnostics(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	insertExecutionActivityEvent(t, db, executionActivitySeed{
		EventID:    "evt_liveness_runtime_observation_quiet",
		EventType:  "attempt.progress",
		ReasonCode: "runtime_observation",
		Payload:    `{"execution_diagnostics":{"event_count":0,"stream_error":"attempt not found: \"run_1:decree_intake:1\""}}`,
		CreatedAt:  "2026-03-01T12:00:20Z",
	})

	active, err := HasRecentExecutionLiveness(context.Background(), db, "nr_1", mustParseRFC3339(t, "2026-03-01T12:00:30Z"), 15*time.Second)
	if err != nil {
		t.Fatalf("has recent execution liveness: %v", err)
	}
	if active {
		t.Fatal("expected runtime observation without live diagnostics to be ignored for execution liveness")
	}
}

type executionActivitySeed struct {
	EventID    string
	EventType  string
	ReasonCode string
	Payload    string
	ActorID    string
	Authority  string
	CreatedAt  string
}

func insertExecutionActivityEvent(t *testing.T, db *sql.DB, seed executionActivitySeed) {
	t.Helper()

	actorID := seed.ActorID
	if actorID == "" {
		actorID = "runtime_orchestrator"
	}
	authority := seed.Authority
	if authority == "" {
		authority = "runtime.orchestrator"
	}
	mustExec(t, db, fmt.Sprintf(`INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('%s', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_1', 'run_1:decree_intake:1', 'v1', '%s', COALESCE((SELECT MAX(seq) FROM events WHERE attempt_id = 'run_1:decree_intake:1'), 0) + 1, '%s', '%s', 'king', '%s', '%s', 'seed activity', 'corr_%s', 'caus_%s', 'prev_%s', 'entry_%s', '%s')`, seed.EventID, seed.EventType, seed.Payload, actorID, authority, seed.ReasonCode, seed.EventID, seed.EventID, seed.EventID, seed.EventID, seed.CreatedAt))
}
