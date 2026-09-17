package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"kingdom/internal/dbmigrations"
)

func TestShouldProjectRunCollectionEventWhitelist(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		eventType string
		want      bool
	}{
		{name: "run state changed", eventType: "run.state_changed", want: true},
		{name: "run created", eventType: "run.created", want: true},
		{name: "run retry reopened", eventType: "run.retry_reopened", want: true},
		{name: "approval recorded", eventType: RunCollectionEventTypeApprovalRecorded, want: true},
		{name: "attempt progress", eventType: "attempt.progress", want: false},
		{name: "attempt retry scheduled", eventType: "attempt.retry_scheduled", want: false},
		{name: "node state changed", eventType: "node.state_changed", want: false},
		{name: "mailbox message", eventType: "mailbox.message_recorded", want: false},
		{name: "escalation", eventType: "escalation.raised", want: false},
	}

	for _, tt := range tests {
		tc := tt
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ShouldProjectRunCollectionEvent(tc.eventType); got != tc.want {
				t.Fatalf("ShouldProjectRunCollectionEvent(%q) = %t, want %t", tc.eventType, got, tc.want)
			}
		})
	}
}

func TestAppendRunCollectionEventProjectsStablePendingApprovalKey(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyAllMigrationsForRunCollectionEventTest(t, db)

	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_1', 'Pending Plan', 'Pending Plan', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_1', 'royal-decree-v1', 'v1', '{"version":"v1","workflow_id":"royal-decree-v1","nodes":[],"edges":[]}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, autonomous_approvals, pending_approval_key, row_version, created_at, updated_at) VALUES ('run_1', NULL, 'decree_1', 'wfdef_1', 'running', 'awaiting_king_decision', 0, 'plan:gate_1', 9, '2026-03-01T12:01:00Z', '2026-03-01T12:02:00Z')`)

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := AppendRunCollectionEventTx(context.Background(), tx, RunCollectionEventAppendRequest{
		EventID:     "rce_1",
		RunID:       "run_1",
		EventType:   RunCollectionEventTypeApprovalRecorded,
		PayloadJSON: `{"event_type":"approval.recorded"}`,
		CreatedAt:   "2026-03-01T12:03:00Z",
	}); err != nil {
		t.Fatalf("append run collection event: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit run collection event tx: %v", err)
	}

	var raw string
	if err := db.QueryRow(`SELECT payload_json FROM run_collection_events WHERE event_id = 'rce_1'`).Scan(&raw); err != nil {
		t.Fatalf("query projected payload: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("decode projected payload: %v", err)
	}
	if payload["pending_approval_key"] != "plan:gate_1" {
		t.Fatalf("expected projected pending_approval_key plan:gate_1, got %#v", payload["pending_approval_key"])
	}
}

func TestAppendRunCollectionEventOmitsStalePendingApprovalKeyForTerminalRun(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyAllMigrationsForRunCollectionEventTest(t, db)

	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_1', 'Cancelled Pending Plan', 'Cancelled Pending Plan', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_1', 'royal-decree-v1', 'v1', '{"version":"v1","workflow_id":"royal-decree-v1","nodes":[],"edges":[]}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, autonomous_approvals, pending_approval_key, row_version, created_at, updated_at) VALUES ('run_1', NULL, 'decree_1', 'wfdef_1', 'cancelled', 'awaiting_king_decision', 0, 'plan:gate_1', 9, '2026-03-01T12:01:00Z', '2026-03-01T12:02:00Z')`)

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	if err := AppendRunCollectionEventTx(context.Background(), tx, RunCollectionEventAppendRequest{
		EventID:     "rce_1",
		RunID:       "run_1",
		EventType:   RunCollectionEventTypeApprovalRecorded,
		PayloadJSON: `{"event_type":"approval.recorded"}`,
		CreatedAt:   "2026-03-01T12:03:00Z",
	}); err != nil {
		t.Fatalf("append run collection event: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit run collection event tx: %v", err)
	}

	var raw string
	if err := db.QueryRow(`SELECT payload_json FROM run_collection_events WHERE event_id = 'rce_1'`).Scan(&raw); err != nil {
		t.Fatalf("query projected payload: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("decode projected payload: %v", err)
	}
	if _, ok := payload["pending_approval_key"]; ok {
		t.Fatalf("expected projected payload to omit stale pending_approval_key, got %#v", payload["pending_approval_key"])
	}
	if payload["display_state"] != "cancelled" {
		t.Fatalf("expected projected display_state cancelled, got %#v", payload["display_state"])
	}
}

func applyAllMigrationsForRunCollectionEventTest(t *testing.T, db *sql.DB) {
	t.Helper()

	entries, err := dbmigrations.List()
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	migrations := make([]SQLMigration, 0, len(entries))
	for _, name := range entries {
		raw, err := dbmigrations.Read(name)
		if err != nil {
			t.Fatalf("read migration %s: %v", name, err)
		}
		migrations = append(migrations, SQLMigration{Name: name, SQL: raw})
	}
	if err := ApplySQLMigrations(context.Background(), db, migrations, time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
}
