package supervisor

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"kingdom/internal/dbmigrations"
	"kingdom/internal/execution/store"

	_ "modernc.org/sqlite"
)

type fakeRunner struct {
	result RunnerResult
	err    error
}

func (f fakeRunner) ExecuteAttempt(_ context.Context, _ store.AttemptExecution) (RunnerResult, error) {
	if f.err != nil {
		return RunnerResult{}, f.err
	}
	return f.result, nil
}

func TestSupervisorRunOnceCommitsSuccess(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	st := store.New(db)
	attemptID := "run_1:decree_intake:1"
	if err := st.EnqueueAttempt(context.Background(), store.EnqueueAttemptRequest{
		AttemptID: attemptID,
		RunID:     "run_1",
		NodeRunID: "nr_1",
		CreatedAt: "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}

	sup, err := New(Config{
		Store:      st,
		Runner:     fakeRunner{result: RunnerResult{TerminalState: "committed_success", Payload: map[string]any{"status": "ok"}}},
		LeaseOwner: "worker-1",
		LeaseTTL:   30 * time.Second,
		Now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 10, 0, time.UTC)
		},
	})
	if err != nil {
		t.Fatalf("new supervisor: %v", err)
	}

	processed, err := sup.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("run once: %v", err)
	}
	if processed != 1 {
		t.Fatalf("expected one processed attempt, got %d", processed)
	}

	attempt, found, err := st.GetAttempt(context.Background(), attemptID)
	if err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if !found {
		t.Fatal("expected attempt row to exist")
	}
	if attempt.State != "committed_success" {
		t.Fatalf("expected committed_success state, got %q", attempt.State)
	}

	var outboxCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_outbox WHERE attempt_id = ? AND status = 'pending'`, attemptID).Scan(&outboxCount); err != nil {
		t.Fatalf("query outbox count: %v", err)
	}
	if outboxCount != 1 {
		t.Fatalf("expected one pending outbox row, got %d", outboxCount)
	}

	var eventCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_event_log WHERE attempt_id = ?`, attemptID).Scan(&eventCount); err != nil {
		t.Fatalf("query attempt event count: %v", err)
	}
	if eventCount != 2 {
		t.Fatalf("expected started and terminal attempt events, got %d", eventCount)
	}
}

func TestSupervisorRunOnceCommitsFailureOnRunnerError(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	st := store.New(db)
	attemptID := "run_1:decree_intake:1"
	if err := st.EnqueueAttempt(context.Background(), store.EnqueueAttemptRequest{
		AttemptID: attemptID,
		RunID:     "run_1",
		NodeRunID: "nr_1",
		CreatedAt: "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}

	sup, err := New(Config{
		Store:      st,
		Runner:     fakeRunner{err: errors.New("runner exploded")},
		LeaseOwner: "worker-1",
		LeaseTTL:   30 * time.Second,
		Now: func() time.Time {
			return time.Date(2026, 3, 1, 12, 0, 10, 0, time.UTC)
		},
	})
	if err != nil {
		t.Fatalf("new supervisor: %v", err)
	}

	processed, err := sup.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("run once: %v", err)
	}
	if processed != 1 {
		t.Fatalf("expected one processed attempt, got %d", processed)
	}

	attempt, found, err := st.GetAttempt(context.Background(), attemptID)
	if err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if !found {
		t.Fatal("expected attempt row to exist")
	}
	if attempt.State != "committed_failure" {
		t.Fatalf("expected committed_failure state, got %q", attempt.State)
	}

	var terminalEventType string
	if err := db.QueryRow(`SELECT event_type FROM attempt_event_log WHERE attempt_id = ? ORDER BY seq DESC LIMIT 1`, attemptID).Scan(&terminalEventType); err != nil {
		t.Fatalf("query terminal event type: %v", err)
	}
	if terminalEventType != "attempt.failed" {
		t.Fatalf("expected terminal attempt.failed event, got %q", terminalEventType)
	}
}

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite test db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("enable foreign keys: %v", err)
	}

	return db
}

func applyMigration(t *testing.T, db *sql.DB, fileName string) {
	t.Helper()
	_ = fileName

	entries, err := dbmigrations.List()
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	for _, name := range entries {
		raw, err := dbmigrations.Read(name)
		if err != nil {
			t.Fatalf("read migration %s: %v", name, err)
		}
		if _, err := db.Exec(string(raw)); err != nil {
			t.Fatalf("apply migration %s: %v", name, err)
		}
	}
}

func seedMinimumGraph(t *testing.T, db *sql.DB) {
	t.Helper()

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_1', 'D1', 'text', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_1', 'royal-decree', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_1', 'wfdef_1', 'decree_intake', 'king', 'decree_intake', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_2', 'wfdef_1', 'royal_closure', 'king', 'royal_closure', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_1', 'wfdef_1', 'decree_intake', 'royal_closure', 'always', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_1', NULL, 'decree_1', 'wfdef_1', 'running', 'draft', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_1', 'run_1', 'decree_intake', 1,1, 'running', 'king', 'agent_king', 1, NULL, NULL)`)
}

func mustExec(t *testing.T, db *sql.DB, statement string) {
	t.Helper()
	if _, err := db.Exec(statement); err != nil {
		t.Fatalf("exec failed for %q: %v", statement, err)
	}
}
