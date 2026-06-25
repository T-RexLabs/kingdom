package state

import (
	"context"
	"testing"
	"time"
)

func TestHasRecentExecutionActivityTracksProviderActivity(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_activity_provider', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_1', 'run_1:decree_intake:1', 'v1', 'attempt.activity', 2, '{"kind":"kingdom.tool","label":"Processing"}', 'runtime_orchestrator', 'king', 'runtime.orchestrator', 'provider_activity', 'provider activity telemetry', 'corr_activity_provider', 'caus_activity_provider', 'prev_activity_provider', 'entry_activity_provider', '2026-03-01T12:00:20Z')`)

	active, err := HasRecentExecutionActivity(context.Background(), db, "nr_1", mustParseRFC3339(t, "2026-03-01T12:00:30Z"), 15*time.Second)
	if err != nil {
		t.Fatalf("has recent execution activity: %v", err)
	}
	if !active {
		t.Fatal("expected provider activity to count as recent execution activity")
	}
}

func TestHasRecentExecutionActivityTracksStreamProgress(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_activity_progress', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_1', 'run_1:decree_intake:1', 'v1', 'attempt.progress', 2, '{"text":"still working"}', 'runtime', 'king', 'runtime.event_bridge', 'stream_event', 'stream event ingested', 'corr_activity_progress', 'caus_activity_progress', 'prev_activity_progress', 'entry_activity_progress', '2026-03-01T12:00:20Z')`)

	active, err := HasRecentExecutionActivity(context.Background(), db, "nr_1", mustParseRFC3339(t, "2026-03-01T12:00:30Z"), 15*time.Second)
	if err != nil {
		t.Fatalf("has recent execution activity: %v", err)
	}
	if !active {
		t.Fatal("expected non-synthetic progress to count as recent execution activity")
	}
}

func TestHasRecentExecutionActivityIgnoresRuntimeObservation(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_activity_runtime_observation', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_1', 'run_1:decree_intake:1', 'v1', 'attempt.progress', 2, '{"execution_diagnostics":{"event_count":3,"execution_path_diagnostics_snapshot":{"stdout_event_count":7,"step_finish_seen":true,"last_stdout_event_at":"2026-03-01T12:00:20Z"}}}', 'runtime_orchestrator', 'king', 'runtime.orchestrator', 'runtime_observation', 'running attempt telemetry snapshot', 'corr_activity_runtime_observation', 'caus_activity_runtime_observation', 'prev_activity_runtime_observation', 'entry_activity_runtime_observation', '2026-03-01T12:00:20Z')`)

	active, err := HasRecentExecutionActivity(context.Background(), db, "nr_1", mustParseRFC3339(t, "2026-03-01T12:00:30Z"), 15*time.Second)
	if err != nil {
		t.Fatalf("has recent execution activity: %v", err)
	}
	if active {
		t.Fatal("expected runtime observation alone to be ignored as execution activity")
	}
}

func TestHasRecentExecutionActivityTracksCompletionObservation(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_activity_completion_observation', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_1', 'run_1:decree_intake:1', 'v1', 'attempt.progress', 2, '{"phase":"provider_export_reconciliation_started"}', 'runtime_orchestrator', 'king', 'runtime.orchestrator', 'runtime_completion_observation', 'execution completion lifecycle telemetry', 'corr_activity_completion_observation', 'caus_activity_completion_observation', 'prev_activity_completion_observation', 'entry_activity_completion_observation', '2026-03-01T12:00:20Z')`)

	active, err := HasRecentExecutionActivity(context.Background(), db, "nr_1", mustParseRFC3339(t, "2026-03-01T12:00:30Z"), 15*time.Second)
	if err != nil {
		t.Fatalf("has recent execution activity: %v", err)
	}
	if !active {
		t.Fatal("expected completion observation to count as recent execution activity")
	}
}
