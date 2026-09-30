package state

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"kingdom/internal/bootstrapassets"
	"kingdom/internal/dbmigrations"

	_ "modernc.org/sqlite"
)

func TestMigrationBaselineSchema(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	if !tableExists(t, db, "workflow_runs") {
		t.Fatal("expected workflow_runs table to exist after up migration")
	}
	if !tableExists(t, db, "attempt_executions") {
		t.Fatal("expected attempt_executions table to exist after up migration")
	}
	if !tableExists(t, db, "attempt_event_log") {
		t.Fatal("expected attempt_event_log table to exist after up migration")
	}
	if !tableExists(t, db, "attempt_outbox") {
		t.Fatal("expected attempt_outbox table to exist after up migration")
	}
	if !tableExists(t, db, "provider_invocations") {
		t.Fatal("expected provider_invocations table to exist after up migration")
	}
	if !tableExists(t, db, "provider_terminal_handoffs") {
		t.Fatal("expected provider_terminal_handoffs table to exist after up migration")
	}
	if !tableExists(t, db, "provider_process_guards") {
		t.Fatal("expected provider_process_guards table to exist after up migration")
	}
	if !tableExists(t, db, "provider_completion_evidence") {
		t.Fatal("expected provider_completion_evidence table to exist after up migration")
	}
	if !tableExists(t, db, "provider_completion_settlements") {
		t.Fatal("expected provider_completion_settlements table to exist after up migration")
	}
	if !tableExists(t, db, "attempt_cancel_outbox") {
		t.Fatal("expected attempt_cancel_outbox table to exist after up migration")
	}
	if !tableExists(t, db, "planning_sessions") {
		t.Fatal("expected planning_sessions table to exist after up migration")
	}
	if !tableExists(t, db, "planning_messages") {
		t.Fatal("expected planning_messages table to exist after up migration")
	}
	if !tableExists(t, db, "planning_drafts") {
		t.Fatal("expected planning_drafts table to exist after up migration")
	}
	if !tableExists(t, db, "planning_context_snapshots") {
		t.Fatal("expected planning_context_snapshots table to exist after up migration")
	}
	if !tableExists(t, db, "planning_operations") {
		t.Fatal("expected planning_operations table to exist after up migration")
	}
	if !tableExists(t, db, "planning_operation_events") {
		t.Fatal("expected planning_operation_events table to exist after up migration")
	}
	if !tableExists(t, db, "planning_activity_reconciliations") {
		t.Fatal("expected planning_activity_reconciliations table to exist after up migration")
	}
	if !tableExists(t, db, "run_plan_reviews") {
		t.Fatal("expected run_plan_reviews table to exist after up migration")
	}
	if !tableExists(t, db, "run_planning_outputs") {
		t.Fatal("expected run_planning_outputs table to exist after up migration")
	}
	if !tableExists(t, db, "run_worktrees") {
		t.Fatal("expected run_worktrees table to exist after up migration")
	}
	if !tableExists(t, db, "run_execution_targets") {
		t.Fatal("expected run_execution_targets table to exist after up migration")
	}
	if !tableExists(t, db, "filesystem_snapshots") {
		t.Fatal("expected filesystem_snapshots table to exist after up migration")
	}
	if !tableExists(t, db, "filesystem_snapshot_entries") {
		t.Fatal("expected filesystem_snapshot_entries table to exist after up migration")
	}
	if !tableExists(t, db, "filesystem_blobs") {
		t.Fatal("expected filesystem_blobs table to exist after up migration")
	}
	if !tableExists(t, db, "run_publication_batches") {
		t.Fatal("expected run_publication_batches table to exist after up migration")
	}
	if !tableExists(t, db, "run_publication_entries") {
		t.Fatal("expected run_publication_entries table to exist after up migration")
	}
	if !tableExists(t, db, "run_publication_paths") {
		t.Fatal("expected run_publication_paths table to exist after up migration")
	}
	if !tableExists(t, db, "run_search_documents") {
		t.Fatal("expected run_search_documents table to exist after up migration")
	}
	if !tableExists(t, db, "run_search_fts") {
		t.Fatal("expected run_search_fts table to exist after up migration")
	}
	if !columnExists(t, db, "workflow_runs", "publish_mode") {
		t.Fatal("expected workflow_runs.publish_mode column to exist after up migration")
	}
	if !columnExists(t, db, "attempt_executions", "provider_timeout_ms") {
		t.Fatal("expected attempt_executions.provider_timeout_ms column to exist after up migration")
	}
	if !columnExists(t, db, "attempt_executions", "active_provider_invocation_id") {
		t.Fatal("expected attempt_executions.active_provider_invocation_id column to exist after up migration")
	}
	if !columnExists(t, db, "attempt_executions", "canonical_settlement_id") {
		t.Fatal("expected attempt_executions.canonical_settlement_id column to exist after up migration")
	}
	if !columnExists(t, db, "attempt_executions", "base_snapshot_id") {
		t.Fatal("expected attempt_executions.base_snapshot_id column to exist after up migration")
	}
	if !columnExists(t, db, "attempt_executions", "result_snapshot_id") {
		t.Fatal("expected attempt_executions.result_snapshot_id column to exist after up migration")
	}
	for _, column := range []string{"completion_mode", "invalid_reason", "opened_at", "settled_at", "terminal_handoff_pending_at", "terminal_recovery_required_at", "terminal_handoff_file_cleanup_pending_at"} {
		if !columnExists(t, db, "provider_invocations", column) {
			t.Fatalf("expected provider_invocations.%s column to exist after up migration", column)
		}
	}
	for _, column := range []string{"completion_capability_hash", "completion_capability_issued_at", "completion_capability_used_at", "completion_capability_revoked_at", "receipt_seen_count", "first_receipt_seen_at", "last_receipt_seen_at", "last_receipt_disposition", "receipt_persisted_at", "receipt_inbox_cleaned_at"} {
		if columnExists(t, db, "provider_invocations", column) {
			t.Fatalf("expected retired provider_invocations.%s column to be absent", column)
		}
	}
	if columnExists(t, db, "provider_process_guards", "terminal_recovery_required_at") {
		t.Fatal("expected terminal recovery markers to live on provider invocations, not process guards")
	}
	if indexExists(t, db, "idx_provider_invocations_receipt_ingestion") {
		t.Fatal("expected retired receipt ingestion index to be absent after up migration")
	}
	for _, column := range []string{"handoff_id", "provider_invocation_id", "attempt_id", "run_id", "node_run_id", "expected_lease_owner", "expected_lease_version", "stream_observation_json", "payload_json", "created_at"} {
		if !columnExists(t, db, "provider_terminal_handoffs", column) {
			t.Fatalf("expected provider_terminal_handoffs.%s column to exist after up migration", column)
		}
	}
	if !indexExists(t, db, "idx_provider_terminal_handoffs_pending") {
		t.Fatal("expected pending provider terminal handoff index after up migration")
	}
	for _, column := range []string{"guard_id", "owner_kind", "owner_id", "provider_invocation_id", "attempt_id", "run_id", "node_run_id", "command_ordinal", "lifecycle", "platform", "guardian_pid", "guardian_identity_token", "child_pid", "child_identity_token", "tree_id", "tree_identity_token", "quiescence_capability_hash", "lease_owner", "lease_version", "started_at", "acknowledged_at", "termination_requested_at", "exited_at", "quiescent_at", "exit_code", "exit_signal", "termination_forced", "failure_code", "failure_text", "quiescence_evidence_sha256", "quiescence_evidence_json", "quiescence_evidence_lost_at", "created_at", "updated_at"} {
		if !columnExists(t, db, "provider_process_guards", column) {
			t.Fatalf("expected provider_process_guards.%s column to exist after up migration", column)
		}
	}
	for _, indexName := range []string{"idx_provider_process_guards_pending", "idx_provider_process_guards_invocation"} {
		if !indexExists(t, db, indexName) {
			t.Fatalf("expected process guard index %q after up migration", indexName)
		}
	}
	if !columnExists(t, db, "provider_completion_evidence", "evidence_kind") {
		t.Fatal("expected provider_completion_evidence.evidence_kind column to exist after up migration")
	}
	if !columnExists(t, db, "provider_completion_settlements", "primary_evidence_id") {
		t.Fatal("expected provider_completion_settlements.primary_evidence_id column to exist after up migration")
	}
	if !columnExists(t, db, "artifacts", "artifact_action") {
		t.Fatal("expected artifacts.artifact_action column to exist after up migration")
	}
	if !columnExists(t, db, "planning_provider_invocations", "completion_mode") {
		t.Fatal("expected planning_provider_invocations.completion_mode column to exist after up migration")
	}
	if !columnExists(t, db, "workflow_runs", "pending_approval_key") {
		t.Fatal("expected workflow_runs.pending_approval_key column to exist after up migration")
	}
	if tableExists(t, db, "attempt_export_checks") {
		t.Fatal("expected attempt_export_checks table to be absent after up migration")
	}
}

func TestMigrationBaselineOmitsObsoleteRunDetailProjectionTables(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	for _, tableName := range []string{"run_detail_stats", "run_agent_memberships", "run_agent_projection"} {
		if tableExists(t, db, tableName) {
			t.Fatalf("expected %s to be dropped after full migration", tableName)
		}
	}
}

func TestMigrationUpgradeAddsRecoveryLeaseAndResponseWorkspaceCleanupFields(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)

	var existingMigrations []SQLMigration
	for _, name := range []string{
		"0001_initial_schema.up.sql",
		"0002_events_execution_activity_index.up.sql",
		"0003_external_sources_and_idempotency_retention.up.sql",
		"0004_resumable_workspace_tree_cleanup.up.sql",
		"0005_unbounded_planning_external_context.up.sql",
	} {
		raw, err := dbmigrations.Read(name)
		if err != nil {
			t.Fatalf("read existing migration %s: %v", name, err)
		}
		existingMigrations = append(existingMigrations, SQLMigration{Name: name, SQL: raw})
	}
	if err := ApplySQLMigrations(context.Background(), db, existingMigrations, time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("apply existing migration history: %v", err)
	}
	for _, tableColumn := range [][2]string{
		{"provider_invocations", "terminal_handoff_pending_lease_owner"},
		{"provider_invocations", "terminal_handoff_pending_lease_version"},
		{"provider_invocations", "terminal_handoff_pending_process_quiescent"},
		{"provider_invocations", "terminal_recovery_required_lease_owner"},
		{"provider_invocations", "terminal_recovery_required_lease_version"},
		{"provider_invocations", "terminal_recovery_required_process_quiescent"},
		{"attempt_executions", "response_only_workspace_cleanup_pending_at"},
	} {
		if columnExists(t, db, tableColumn[0], tableColumn[1]) {
			t.Fatalf("existing schema unexpectedly has %s.%s before forward migration", tableColumn[0], tableColumn[1])
		}
	}
	raw, err := dbmigrations.Read("0006_execution_recovery_and_response_workspace_cleanup.up.sql")
	if err != nil {
		t.Fatalf("read forward recovery migration: %v", err)
	}
	if err := ApplySQLMigrations(context.Background(), db, []SQLMigration{{Name: "0006_execution_recovery_and_response_workspace_cleanup.up.sql", SQL: raw}}, time.Date(2026, 9, 29, 12, 1, 0, 0, time.UTC)); err != nil {
		t.Fatalf("apply forward recovery migration: %v", err)
	}
	for _, tableColumn := range [][2]string{
		{"provider_invocations", "terminal_handoff_pending_lease_owner"},
		{"provider_invocations", "terminal_handoff_pending_lease_version"},
		{"provider_invocations", "terminal_handoff_pending_process_quiescent"},
		{"provider_invocations", "terminal_recovery_required_lease_owner"},
		{"provider_invocations", "terminal_recovery_required_lease_version"},
		{"provider_invocations", "terminal_recovery_required_process_quiescent"},
		{"attempt_executions", "response_only_workspace_cleanup_pending_at"},
	} {
		if !columnExists(t, db, tableColumn[0], tableColumn[1]) {
			t.Fatalf("forward migration did not add %s.%s", tableColumn[0], tableColumn[1])
		}
	}
}

func TestMigrationRunSearchProjectionMaintainedByTriggers(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	mustExec(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_search', 'Older Historical Run', 'text', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_search', 'work-order-v1', 'v1', '{"version":"v1","workflow_id":"work-order-v1","nodes":[],"edges":[]}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run_search', NULL, 'work_order_search', 'wfdef_search', 'running', 'draft', 0, 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)

	var runID string
	var runIDSearch string
	var workOrderTitleSearch string
	if err := db.QueryRow(`SELECT run_id, run_id_search, work_order_title_search FROM run_search_documents WHERE run_id = 'run_search'`).Scan(&runID, &runIDSearch, &workOrderTitleSearch); err != nil {
		t.Fatalf("select run search projection: %v", err)
	}
	if runID != "run_search" || runIDSearch != "run_search" || workOrderTitleSearch != "older historical run" {
		t.Fatalf("unexpected run search projection row: run_id=%q run_id_search=%q work_order_title_search=%q", runID, runIDSearch, workOrderTitleSearch)
	}

	var matchedRunID string
	if err := db.QueryRow(`SELECT rsd.run_id
	FROM run_search_fts
	JOIN run_search_documents rsd ON rsd.rowid = run_search_fts.rowid
	WHERE run_search_fts MATCH ?`, `work_order_title_search : "histor"*`).Scan(&matchedRunID); err != nil {
		t.Fatalf("query run search fts by title prefix: %v", err)
	}
	if matchedRunID != "run_search" {
		t.Fatalf("expected title prefix search to resolve run_search, got %q", matchedRunID)
	}

	mustExec(t, db, `UPDATE work_orders SET work_order_title = 'Royal Archive Review' WHERE work_order_id = 'work_order_search'`)

	var oldMatchCount int
	if err := db.QueryRow(`SELECT COUNT(1)
	FROM run_search_fts
	WHERE run_search_fts MATCH ?`, `work_order_title_search : "histor"*`).Scan(&oldMatchCount); err != nil {
		t.Fatalf("count old title prefix matches: %v", err)
	}
	if oldMatchCount != 0 {
		t.Fatalf("expected old title prefix to be removed from search index, got %d matches", oldMatchCount)
	}

	if err := db.QueryRow(`SELECT rsd.run_id
	FROM run_search_fts
	JOIN run_search_documents rsd ON rsd.rowid = run_search_fts.rowid
	WHERE run_search_fts MATCH ?`, `work_order_title_search : "arch"*`).Scan(&matchedRunID); err != nil {
		t.Fatalf("query run search fts by updated title prefix: %v", err)
	}
	if matchedRunID != "run_search" {
		t.Fatalf("expected updated title prefix search to resolve run_search, got %q", matchedRunID)
	}

	assertPlanUsesIndex(t, db, `SELECT run_id FROM run_search_documents WHERE run_id_search = ?`, []any{"run_search"}, "idx_run_search_documents_run_id_search")
	assertPlanUsesVirtualTableIndex(t, db, `SELECT rsd.run_id FROM run_search_fts JOIN run_search_documents rsd ON rsd.rowid = run_search_fts.rowid WHERE run_search_fts MATCH ?`, []any{`work_order_title_search : "arch"*`}, "run_search_fts")
}

func TestMigrationConstraints(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	t.Run("foreign_key_enforced", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_missing_role', 'not-a-role', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
		if err == nil {
			t.Fatal("expected FK violation for missing role")
		}
	})

	t.Run("check_constraint_enforced", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_invalid', NULL, 'work_order_1', 'wfdef_1', 'not-a-status', 'draft', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
		if err == nil {
			t.Fatal("expected CHECK constraint failure for run_status")
		}
	})

	t.Run("awaiting_human_confirmation_status_allowed", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_awaiting_human', NULL, 'work_order_1', 'wfdef_1', 'awaiting_human_confirmation', 'approved', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
		if err != nil {
			t.Fatalf("expected awaiting_human_confirmation to be valid run_status: %v", err)
		}
	})

	t.Run("unique_workflow_version", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_dup', 'work-order', 'v1', '{}', '2026-03-01T12:00:00Z')`)
		if err == nil {
			t.Fatal("expected unique constraint failure for workflow name+version")
		}
	})

	t.Run("unique_node_attempt", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_dup', 'run_1', 'work_intake', 1,1, 'running', 'governance', 'agent_governance', 1, NULL, NULL)`)
		if err == nil {
			t.Fatal("expected unique constraint failure for run_id+node_id+attempt")
		}
	})

	t.Run("edge_uniqueness", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_dup', 'wfdef_1', 'work_intake', 'completion', 'always', '2026-03-01T12:00:00Z')`)
		if err == nil {
			t.Fatal("expected unique edge constraint failure")
		}
	})

	t.Run("idempotency_scope_uniqueness", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO idempotency_keys (idempotency_key_id, route, actor_id, client_key, request_hash, response_status, response_headers_json, response_body_json, created_at, expires_at) VALUES ('idem_dup', '/runs', 'actor_1', 'key_1', 'hash_dup', 201, '{}', '{}', '2026-03-01T12:00:00Z', NULL)`)
		if err == nil {
			t.Fatal("expected idempotency scope unique constraint failure")
		}
	})

	t.Run("event_dedupe_uniqueness", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_dup', 'run_1', 2, 'nr_1', 'att_1', 'v1', 'attempt.progress', 1, '{}', 'actor_1', 'governance', 'system', 'internal_error', 'dup', 'corr_2', 'caus_2', 'p', 'e', '2026-03-01T12:00:00Z')`)
		if err == nil {
			t.Fatal("expected event dedupe unique constraint failure")
		}
	})

	t.Run("run_collection_events_rejects_execution_chatter", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO run_collection_events (event_id, run_id, event_type, payload_json, created_at) VALUES ('rce_bad_attempt_progress', 'run_1', 'attempt.progress', '{}', '2026-03-01T12:00:00Z')`)
		if err == nil {
			t.Fatal("expected run_collection_events to reject attempt.progress rows")
		}
	})

	t.Run("run_planning_outputs_lineage_constraint", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:work_intake:1', 'run_1', 'nr_1', 'committed_success', 'provider_backed', 'server_response', '', NULL, 0, '', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z', '', '', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
		if err != nil {
			t.Fatalf("insert attempt execution seed row for lineage constraint: %v", err)
		}

		_, err = db.Exec(`INSERT INTO run_planning_outputs (planning_output_id, run_id, node_id, node_run_id, attempt_id, agent_id, source_message_id, kind, content_markdown, created_at) VALUES ('planrev_bad_lineage', 'run_1', 'work_intake', 'nr_1', 'run_1:work_intake:1', 'agent_governance', '', 'plan_revision', 'constraint', '2026-03-01T12:00:00Z')`)
		if err == nil {
			t.Fatal("expected trigger failure for invalid run_planning_outputs lineage")
		}
	})

	t.Run("run_planning_outputs_attempt_lineage_constraint", func(t *testing.T) {
		mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '2026-03-01T12:00:00Z')`)
		mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council', 'plan-author', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
		mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_plan_constraint', 'wfdef_1', 'plan_drafting', 'plan-author', 'plan_drafting', '2026-03-01T12:00:00Z')`)
		mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_plan_constraint_1', 'run_1', 'plan_drafting', 1,1, 'succeeded', 'plan-author', 'agent_council', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
		mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_plan_constraint_2', 'run_1', 'plan_drafting', 2,2, 'succeeded', 'plan-author', 'agent_council', 1, '2026-03-01T12:01:00Z', '2026-03-01T12:01:00Z')`)
		mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:plan_drafting:2', 'run_1', 'nr_plan_constraint_2', 'committed_success', 'provider_backed', 'server_response', '', NULL, 0, '', '2026-03-01T12:01:00Z', '2026-03-01T12:01:00Z', '2026-03-01T12:01:00Z', '', '', '2026-03-01T12:01:00Z', '2026-03-01T12:01:00Z')`)

		_, err := db.Exec(`INSERT INTO run_planning_outputs (planning_output_id, run_id, node_id, node_run_id, attempt_id, agent_id, source_message_id, kind, content_markdown, created_at) VALUES ('planrev_bad_attempt_lineage', 'run_1', 'plan_drafting', 'nr_plan_constraint_1', 'run_1:plan_drafting:2', 'agent_council', '', 'plan_revision', 'constraint', '2026-03-01T12:00:00Z')`)
		if err == nil {
			t.Fatal("expected trigger failure for mismatched run_planning_outputs attempt lineage")
		}
	})

	t.Run("run_planning_outputs_append_only", func(t *testing.T) {
		mustExec(t, db, `INSERT OR IGNORE INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '2026-03-01T12:00:00Z')`)
		mustExec(t, db, `INSERT OR IGNORE INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council_append_only', 'plan-author', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
		mustExec(t, db, `INSERT OR IGNORE INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_plan_append_only', 'wfdef_1', 'plan_drafting', 'plan-author', 'plan_drafting', '2026-03-01T12:00:00Z')`)
		mustExec(t, db, `INSERT OR IGNORE INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_plan_append_only', 'run_1', 'plan_drafting', 3,3, 'succeeded', 'plan-author', 'agent_council_append_only', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
		mustExec(t, db, `INSERT OR IGNORE INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:plan_drafting:append_only', 'run_1', 'nr_plan_append_only', 'committed_success', 'provider_backed', 'server_response', '', NULL, 0, '', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z', '', '', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
		mustExec(t, db, `INSERT INTO run_planning_outputs (planning_output_id, run_id, node_id, node_run_id, attempt_id, agent_id, source_message_id, kind, content_markdown, created_at) VALUES ('planout_append_only', 'run_1', 'plan_drafting', 'nr_plan_append_only', 'run_1:plan_drafting:append_only', 'agent_council_append_only', '', 'plan_revision', 'append only', '2026-03-01T12:00:00Z')`)

		if _, err := db.Exec(`UPDATE run_planning_outputs SET content_markdown = 'mutated' WHERE planning_output_id = 'planout_append_only'`); err == nil {
			t.Fatal("expected update failure for append-only run_planning_outputs")
		}
		if _, err := db.Exec(`DELETE FROM run_planning_outputs WHERE planning_output_id = 'planout_append_only'`); err == nil {
			t.Fatal("expected delete failure for append-only run_planning_outputs")
		}
	})

	t.Run("run_plan_reviews_decision_constraint", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO run_plan_revisions (plan_revision_id, run_id, content_markdown, components_json, created_at) VALUES ('planrev_constraint', 'run_1', 'constraint', '[{"node_id":"plan_drafting","planning_output_id":"planout_constraint","node_run_id":"nr_1","attempt_id":"run_1:plan_drafting:1","agent_id":"agent_governance","created_at":"2026-03-01T12:00:00Z","content_markdown":"constraint"}]', '2026-03-01T12:00:00Z')`)
		if err != nil {
			t.Fatalf("insert run plan revision seed for run_plan_reviews: %v", err)
		}
		_, err = db.Exec(`INSERT INTO run_plan_reviews (plan_review_id, run_id, reviewed_plan_revision_id, decision, feedback_markdown, actor_id, role_id, queued_advisory_attempt, queued_plan_attempt, created_at) VALUES ('plan_review_bad', 'run_1', 'planrev_constraint', 'bad', 'feedback', 'actor_1', 'governance', NULL, NULL, '2026-03-01T12:00:00Z')`)
		if err == nil {
			t.Fatal("expected CHECK constraint failure for run_plan_reviews.decision")
		}
	})

	t.Run("run_plan_revisions_append_only", func(t *testing.T) {
		mustExec(t, db, `INSERT INTO run_plan_revisions (plan_revision_id, run_id, content_markdown, components_json, created_at) VALUES ('planrev_append_only', 'run_1', 'append only', '[{"node_id":"plan_drafting","planning_output_id":"planout_append_only_projection","node_run_id":"nr_1","attempt_id":"run_1:plan_drafting:append_only_projection","agent_id":"agent_governance","created_at":"2026-03-01T12:00:00Z","content_markdown":"append only"}]', '2026-03-01T12:00:00Z')`)

		if _, err := db.Exec(`UPDATE run_plan_revisions SET content_markdown = 'mutated' WHERE plan_revision_id = 'planrev_append_only'`); err == nil {
			t.Fatal("expected update failure for append-only run_plan_revisions")
		}
		if _, err := db.Exec(`DELETE FROM run_plan_revisions WHERE plan_revision_id = 'planrev_append_only'`); err == nil {
			t.Fatal("expected delete failure for append-only run_plan_revisions")
		}
	})

	t.Run("attempt_execution_state_constraint", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('att_exec_bad_state', 'run_1', 'nr_1', 'unknown', 'provider_backed', 'server_response', '', NULL, 0, '', NULL, NULL, NULL, '', '', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
		if err == nil {
			t.Fatal("expected CHECK constraint failure for attempt_executions.state")
		}
	})

	t.Run("attempt_event_log_sequence_uniqueness", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('att_exec_1', 'run_1', 'nr_1', 'queued', 'provider_backed', 'server_response', '', NULL, 0, '', NULL, NULL, NULL, '', '', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
		if err != nil {
			t.Fatalf("insert attempt execution seed row: %v", err)
		}

		_, err = db.Exec(`INSERT INTO attempt_event_log (attempt_event_id, attempt_id, run_id, node_run_id, seq, event_type, reason_code, payload_json, created_at) VALUES ('ael_1', 'att_exec_1', 'run_1', 'nr_1', 1, 'attempt.started', 'orchestration_progress', '{}', '2026-03-01T12:00:00Z')`)
		if err != nil {
			t.Fatalf("insert attempt event log seed row: %v", err)
		}

		_, err = db.Exec(`INSERT INTO attempt_event_log (attempt_event_id, attempt_id, run_id, node_run_id, seq, event_type, reason_code, payload_json, created_at) VALUES ('ael_2', 'att_exec_1', 'run_1', 'nr_1', 1, 'attempt.progress', 'orchestration_progress', '{}', '2026-03-01T12:00:01Z')`)
		if err == nil {
			t.Fatal("expected unique constraint failure for attempt_event_log(attempt_id, seq)")
		}
	})

	t.Run("attempt_outbox_uniqueness", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO attempt_outbox (outbox_id, attempt_id, run_id, node_run_id, terminal_seq, terminal_state, payload_json, status, publish_attempts, last_error, created_at, updated_at, published_at) VALUES ('outbox_1', 'att_exec_1', 'run_1', 'nr_1', 3, 'committed_success', '{}', 'pending', 0, '', '2026-03-01T12:00:02Z', '2026-03-01T12:00:02Z', NULL)`)
		if err != nil {
			t.Fatalf("insert attempt outbox seed row: %v", err)
		}

		_, err = db.Exec(`INSERT INTO attempt_outbox (outbox_id, attempt_id, run_id, node_run_id, terminal_seq, terminal_state, payload_json, status, publish_attempts, last_error, created_at, updated_at, published_at) VALUES ('outbox_2', 'att_exec_1', 'run_1', 'nr_1', 3, 'committed_success', '{}', 'pending', 0, '', '2026-03-01T12:00:03Z', '2026-03-01T12:00:03Z', NULL)`)
		if err == nil {
			t.Fatal("expected unique constraint failure for attempt_outbox(attempt_id, terminal_seq)")
		}
	})

	t.Run("attempt_cancel_outbox_uniqueness", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO attempt_cancel_outbox (cancel_outbox_id, attempt_id, run_id, node_run_id, session_id, reason_code, reason_text, requested_at, status, publish_attempts, last_error, created_at, updated_at, published_at) VALUES ('cancel_outbox_1', 'att_exec_1', 'run_1', 'nr_1', 'session_att_exec_1', 'run_terminalized', 'run failed', '2026-03-01T12:00:02Z', 'pending', 0, '', '2026-03-01T12:00:02Z', '2026-03-01T12:00:02Z', NULL)`)
		if err != nil {
			t.Fatalf("insert attempt cancel outbox seed row: %v", err)
		}

		_, err = db.Exec(`INSERT INTO attempt_cancel_outbox (cancel_outbox_id, attempt_id, run_id, node_run_id, session_id, reason_code, reason_text, requested_at, status, publish_attempts, last_error, created_at, updated_at, published_at) VALUES ('cancel_outbox_2', 'att_exec_1', 'run_1', 'nr_1', 'session_att_exec_1', 'run_terminalized', 'run failed again', '2026-03-01T12:00:03Z', 'pending', 0, '', '2026-03-01T12:00:03Z', '2026-03-01T12:00:03Z', NULL)`)
		if err == nil {
			t.Fatal("expected unique constraint failure for attempt_cancel_outbox(attempt_id)")
		}
	})

	t.Run("attempt_executions_provider_session_id_column_present", func(t *testing.T) {
		rows, err := db.Query(`PRAGMA table_info(attempt_executions)`)
		if err != nil {
			t.Fatalf("query attempt_executions table info: %v", err)
		}
		defer rows.Close()

		found := false
		for rows.Next() {
			var (
				cid        int
				name       string
				colType    string
				notNull    int
				defaultV   sql.NullString
				primaryKey int
			)
			if err := rows.Scan(&cid, &name, &colType, &notNull, &defaultV, &primaryKey); err != nil {
				t.Fatalf("scan attempt_executions table info: %v", err)
			}
			if name == "provider_session_id" {
				found = true
				if notNull != 1 {
					t.Fatalf("expected provider_session_id to be NOT NULL, got notNull=%d", notNull)
				}
				if !defaultV.Valid || defaultV.String != "''" {
					t.Fatalf("expected provider_session_id default '' , got %#v", defaultV)
				}
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate attempt_executions table info: %v", err)
		}
		if !found {
			t.Fatal("expected attempt_executions.provider_session_id column to exist")
		}
	})

	t.Run("node_runs_wave_column_present", func(t *testing.T) {
		rows, err := db.Query(`PRAGMA table_info(node_runs)`)
		if err != nil {
			t.Fatalf("query node_runs table info: %v", err)
		}
		defer rows.Close()

		found := false
		for rows.Next() {
			var (
				cid        int
				name       string
				colType    string
				notNull    int
				defaultV   sql.NullString
				primaryKey int
			)
			if err := rows.Scan(&cid, &name, &colType, &notNull, &defaultV, &primaryKey); err != nil {
				t.Fatalf("scan node_runs table info: %v", err)
			}
			if name == "wave" {
				found = true
				if notNull != 1 {
					t.Fatalf("expected node_runs.wave to be NOT NULL, got notNull=%d", notNull)
				}
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate node_runs table info: %v", err)
		}
		if !found {
			t.Fatal("expected node_runs.wave column to exist")
		}
		if !indexExists(t, db, "idx_node_runs_run_node_wave") {
			t.Fatal("expected idx_node_runs_run_node_wave to exist")
		}
	})

	t.Run("node_attempt_requires_responsible_agent", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_missing_agent', 'run_1', 'completion', 1,1, 'pending', 'governance', NULL, 1, NULL, NULL)`)
		if err == nil {
			t.Fatal("expected NOT NULL failure for node_runs.agent_id")
		}
	})

	t.Run("hash_chain_non_null", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO transitions (transition_id, run_id, node_run_id, entity_type, from_state, to_state, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('tr_bad', 'run_1', 'nr_1', 'node', 'ready', 'running', 'actor_1', 'governance', 'system', 'internal_error', 'missing hashes', 'corr_3', 'caus_3', NULL, NULL, '2026-03-01T12:00:00Z')`)
		if err == nil {
			t.Fatal("expected NOT NULL failure for prev_hash/entry_hash")
		}
	})

	t.Run("transitions_append_only", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO transitions (transition_id, run_id, node_run_id, entity_type, from_state, to_state, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('tr_append_1', 'run_1', 'nr_1', 'node', 'running', 'succeeded', 'actor_1', 'governance', 'system', 'internal_error', 'seed', 'corr_append', 'caus_append', 'prev', 'entry', '2026-03-01T12:00:00Z')`)
		if err != nil {
			t.Fatalf("insert transition seed row: %v", err)
		}

		_, err = db.Exec(`UPDATE transitions SET reason_text = 'mutated' WHERE transition_id = 'tr_append_1'`)
		if err == nil {
			t.Fatal("expected append-only update failure for transitions")
		}

		_, err = db.Exec(`DELETE FROM transitions WHERE transition_id = 'tr_append_1'`)
		if err == nil {
			t.Fatal("expected append-only delete failure for transitions")
		}
	})

	t.Run("events_append_only", func(t *testing.T) {
		_, err := db.Exec(`UPDATE events SET reason_text = 'mutated' WHERE event_id = 'evt_1'`)
		if err == nil {
			t.Fatal("expected append-only update failure for events")
		}

		_, err = db.Exec(`DELETE FROM events WHERE event_id = 'evt_1'`)
		if err == nil {
			t.Fatal("expected append-only delete failure for events")
		}
	})

	t.Run("mailbox_message_type_constraint", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at) VALUES ('msg_bad_type', 'run_1', 'agent_governance', 'agent_governance', 'thread_1', 'mutation_request', 'normal', '{}', '2026-03-01T12:00:00Z', NULL)`)
		if err == nil {
			t.Fatal("expected CHECK constraint failure for mailbox message_type")
		}
	})

	t.Run("mailbox_priority_constraint", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at) VALUES ('msg_bad_priority', 'run_1', 'agent_governance', 'agent_governance', 'thread_1', 'status_update', 'urgent', '{}', '2026-03-01T12:00:00Z', NULL)`)
		if err == nil {
			t.Fatal("expected CHECK constraint failure for mailbox priority")
		}
	})

	t.Run("artifact_attempt_provenance_required", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO artifacts (artifact_id, run_id, node_run_id, attempt_id, name, mime, path, sha256, size_bytes, producer_role, created_at) VALUES ('artifact_bad_attempt', 'run_1', 'nr_1', NULL, 'report.md', 'text/markdown', 'artifacts/report.md', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 12, 'governance', '2026-03-01T12:00:00Z')`)
		if err == nil {
			t.Fatal("expected NOT NULL failure for artifacts.attempt_id")
		}
	})

	t.Run("agents_extended_columns_present", func(t *testing.T) {
		columns := map[string]bool{
			"display_name":          false,
			"campaign_id":           false,
			"session_id":            false,
			"capability_profile_id": false,
			"last_heartbeat_at":     false,
		}

		rows, err := db.Query(`PRAGMA table_info(agents)`)
		if err != nil {
			t.Fatalf("query agents table info: %v", err)
		}
		defer rows.Close()

		for rows.Next() {
			var (
				cid        int
				name       string
				colType    string
				notNull    int
				defaultV   sql.NullString
				primaryKey int
			)
			if err := rows.Scan(&cid, &name, &colType, &notNull, &defaultV, &primaryKey); err != nil {
				t.Fatalf("scan agents table info: %v", err)
			}
			if _, ok := columns[name]; ok {
				columns[name] = true
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate agents table info: %v", err)
		}

		for name, present := range columns {
			if !present {
				t.Fatalf("expected agents column %q to exist", name)
			}
		}
	})

	t.Run("planning_session_conversation_state_constraint", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO planning_sessions (planning_session_id, title, title_state, lifecycle_state, conversation_state, active_leaf_message_id, current_operation_id, current_draft_id, context_snapshot_id, project_root, model_selection_json, created_at, updated_at) VALUES ('ps_bad', 'bad', 'custom', 'active', 'done', NULL, NULL, NULL, NULL, '/tmp/project', '{}', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
		if err == nil {
			t.Fatal("expected CHECK constraint failure for planning_sessions.conversation_state")
		}
	})

	t.Run("planning_message_tree_parent_fk_and_siblings", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO planning_sessions (planning_session_id, title, title_state, lifecycle_state, conversation_state, active_leaf_message_id, current_operation_id, current_draft_id, context_snapshot_id, project_root, model_selection_json, created_at, updated_at) VALUES ('ps_1', 'session', 'custom', 'active', 'ready', NULL, NULL, NULL, NULL, '/tmp/project', '{}', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
		if err != nil {
			t.Fatalf("insert planning session seed row: %v", err)
		}

		// A root message (NULL parent) is allowed.
		if _, err := db.Exec(`INSERT INTO planning_messages (planning_message_id, planning_session_id, parent_message_id, role, content_markdown, question_payload_json, answer_payload_json, created_at) VALUES ('pm_root', 'ps_1', NULL, 'assistant', 'hello', '{}', '{}', '2026-03-01T12:01:00Z')`); err != nil {
			t.Fatalf("insert root planning message: %v", err)
		}

		// A non-existent parent must be rejected by the FK.
		if _, err := db.Exec(`INSERT INTO planning_messages (planning_message_id, planning_session_id, parent_message_id, role, content_markdown, question_payload_json, answer_payload_json, created_at) VALUES ('pm_orphan', 'ps_1', 'pm_missing', 'user', 'x', '{}', '{}', '2026-03-01T12:01:30Z')`); err == nil {
			t.Fatal("expected FOREIGN KEY failure for a non-existent parent_message_id")
		}

		// Two siblings under the same parent are explicitly allowed — branching
		// is the core of the tree model.
		if _, err := db.Exec(`INSERT INTO planning_messages (planning_message_id, planning_session_id, parent_message_id, role, content_markdown, question_payload_json, answer_payload_json, created_at) VALUES ('pm_child_a', 'ps_1', 'pm_root', 'user', 'a', '{}', '{}', '2026-03-01T12:02:00Z')`); err != nil {
			t.Fatalf("insert first sibling: %v", err)
		}
		if _, err := db.Exec(`INSERT INTO planning_messages (planning_message_id, planning_session_id, parent_message_id, role, content_markdown, question_payload_json, answer_payload_json, created_at) VALUES ('pm_child_b', 'ps_1', 'pm_root', 'user', 'b', '{}', '{}', '2026-03-01T12:03:00Z')`); err != nil {
			t.Fatalf("expected sibling branches to be allowed, got: %v", err)
		}
	})

	t.Run("planning_operations_scope_active_slots_by_operation_type", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO planning_sessions (planning_session_id, title, title_state, lifecycle_state, conversation_state, active_leaf_message_id, current_operation_id, current_draft_id, context_snapshot_id, project_root, model_selection_json, created_at, updated_at) VALUES ('ps_bootstrap', 'session', 'custom', 'active', 'processing', NULL, 'po_reply_active', NULL, NULL, '/tmp/project', '{}', '2026-03-01T12:10:00Z', '2026-03-01T12:10:00Z')`)
		if err != nil {
			t.Fatalf("insert planning session seed row: %v", err)
		}

		_, err = db.Exec(`INSERT INTO planning_operations (operation_id, acp_run_id, planning_session_id, type, status, request_payload_json, lease_owner, lease_expires_at, lease_version, last_heartbeat_at, error, result_ref, created_at, started_at, finished_at) VALUES ('po_reply_active', 'po_reply_active', 'ps_bootstrap', 'reply', 'queued', '{}', '', NULL, 0, NULL, NULL, NULL, '2026-03-01T12:10:01Z', NULL, NULL)`)
		if err != nil {
			t.Fatalf("insert interactive operation seed row: %v", err)
		}

		_, err = db.Exec(`INSERT INTO planning_operations (operation_id, acp_run_id, planning_session_id, type, status, request_payload_json, lease_owner, lease_expires_at, lease_version, last_heartbeat_at, error, result_ref, created_at, started_at, finished_at) VALUES ('po_unknown_type', 'po_unknown_type', 'ps_bootstrap', 'bootstrap_context', 'queued', '{}', '', NULL, 0, NULL, NULL, NULL, '2026-03-01T12:10:02Z', NULL, NULL)`)
		if err == nil {
			t.Fatal("expected an operation type outside the supported set to be rejected")
		}

		_, err = db.Exec(`INSERT INTO planning_operations (operation_id, acp_run_id, planning_session_id, type, status, request_payload_json, lease_owner, lease_expires_at, lease_version, last_heartbeat_at, error, result_ref, created_at, started_at, finished_at) VALUES ('po_formalize_blocked', 'po_formalize_blocked', 'ps_bootstrap', 'formalize', 'queued', '{}', '', NULL, 0, NULL, NULL, NULL, '2026-03-01T12:10:03Z', NULL, NULL)`)
		if err == nil {
			t.Fatal("expected second interactive planning operation to violate the single-active-interactive index")
		}

		_, err = db.Exec(`INSERT INTO planning_operations (operation_id, acp_run_id, planning_session_id, type, status, request_payload_json, lease_owner, lease_expires_at, lease_version, last_heartbeat_at, error, result_ref, created_at, started_at, finished_at) VALUES ('po_title_active', 'po_title_active', 'ps_bootstrap', 'generate_title', 'queued', '{}', '', NULL, 0, NULL, NULL, NULL, '2026-03-01T12:10:04Z', NULL, NULL)`)
		if err != nil {
			t.Fatalf("expected generate_title operation to coexist with the active interactive slot, got %v", err)
		}

		_, err = db.Exec(`INSERT INTO planning_operations (operation_id, acp_run_id, planning_session_id, type, status, request_payload_json, lease_owner, lease_expires_at, lease_version, last_heartbeat_at, error, result_ref, created_at, started_at, finished_at) VALUES ('po_title_blocked', 'po_title_blocked', 'ps_bootstrap', 'generate_title', 'queued', '{}', '', NULL, 0, NULL, NULL, NULL, '2026-03-01T12:10:05Z', NULL, NULL)`)
		if err == nil {
			t.Fatal("expected second generate_title planning operation to violate the single-active-title index")
		}
	})

	t.Run("planning_draft_ids_remain_unique", func(t *testing.T) {
		_, err := db.Exec(`INSERT INTO planning_drafts (planning_draft_id, planning_session_id, work_order_title, work_order_text, summary_json, created_at) VALUES ('pd_1', 'ps_1', 'title', 'text', '{}', '2026-03-01T12:03:00Z')`)
		if err != nil {
			t.Fatalf("insert planning draft seed row: %v", err)
		}

		_, err = db.Exec(`INSERT INTO planning_drafts (planning_draft_id, planning_session_id, work_order_title, work_order_text, summary_json, created_at) VALUES ('pd_1', 'ps_1', 'title 2', 'text 2', '{}', '2026-03-01T12:04:00Z')`)
		if err == nil {
			t.Fatal("expected unique constraint failure for planning_drafts.planning_draft_id")
		}
	})
}

func TestMigrationStatusQueryIndexes(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `UPDATE agents SET status = 'busy' WHERE agent_id = 'agent_governance'`)
	mustExec(t, db, `UPDATE node_runs SET started_at = '2026-03-01T11:30:00Z' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_3', 'wfdef_1', 'delegated_execution_1', 'governance', 'delegated_execution', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_4', 'wfdef_1', 'execution_review_1', 'governance', 'execution_review', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_5', 'wfdef_1', 'validation_gates_1', 'governance', 'validation_gates', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_pending', NULL, 'work_order_1', 'wfdef_1', 'pending', 'draft', 1, '2026-03-01T12:01:00Z', '2026-03-01T12:01:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_success', NULL, 'work_order_1', 'wfdef_1', 'succeeded', 'approved', 1, '2026-03-01T12:02:00Z', '2026-03-01T12:05:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_failed', NULL, 'work_order_1', 'wfdef_1', 'failed', 'approved', 1, '2026-03-01T12:03:00Z', '2026-03-01T12:06:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_cancelled', NULL, 'work_order_1', 'wfdef_1', 'cancelled', 'approved', 1, '2026-03-01T12:04:00Z', '2026-03-01T12:07:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, autonomous_approvals, row_version, created_at, updated_at) VALUES ('run_manual_plan', NULL, 'work_order_1', 'wfdef_1', 'running', 'awaiting_plan_decision', 0, 1, '2026-03-01T12:08:00Z', '2026-03-01T12:10:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, autonomous_approvals, row_version, created_at, updated_at) VALUES ('run_manual_final', NULL, 'work_order_1', 'wfdef_1', 'awaiting_human_confirmation', 'approved', 0, 1, '2026-03-01T12:09:00Z', '2026-03-01T12:11:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_2', 'run_1', 'delegated_execution_1', 1,1, 'ready', 'governance', 'agent_governance', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_3', 'run_1', 'execution_review_1', 1,1, 'failed', 'governance', 'agent_governance', 1, '2026-03-01T11:40:00Z', '2026-03-01T11:45:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_4', 'run_1', 'validation_gates_1', 2,2, 'succeeded', 'governance', 'agent_governance', 1, '2026-03-01T11:46:00Z', '2026-03-01T11:50:00Z')`)

	for _, indexName := range []string{
		"idx_workflow_runs_active_created",
		"idx_workflow_runs_terminal_created_updated",
		"idx_workflow_runs_manual_decision_updated",
		"idx_workflow_runs_created_at",
		"idx_node_runs_running_started",
		"idx_node_runs_run_status_node_attempt",
	} {
		if !indexExists(t, db, indexName) {
			t.Fatalf("expected index %q to exist", indexName)
		}
	}

	assertPlanUsesIndex(t, db,
		`SELECT run_id, run_status, plan_state, COALESCE(autonomous_approvals, 1), COALESCE(model_selection_json, '{}'), workflow_definition_id
		 FROM workflow_runs
		 WHERE run_status IN ('pending', 'running')
		 ORDER BY created_at ASC, run_id ASC`,
		nil,
		"idx_workflow_runs_active_created",
	)
	assertPlanUsesIndex(t, db,
		`SELECT wr.run_id
		 FROM workflow_runs wr
		 WHERE wr.created_at >= ? AND wr.run_status IN ('succeeded', 'failed', 'cancelled')
		 ORDER BY wr.updated_at DESC
		 LIMIT 20`,
		[]any{"2026-03-01T12:00:00Z"},
		"idx_workflow_runs_terminal_created_updated",
	)
	assertPlanUsesIndex(t, db,
		`SELECT n.node_run_id
		 FROM node_runs n
		 JOIN agents a ON a.agent_id = n.agent_id
		 WHERE n.node_status = 'running'
		   AND a.status = 'busy'
		   AND n.started_at IS NOT NULL
		   AND n.started_at <= ?
		 ORDER BY n.run_id ASC, n.node_id ASC, n.attempt ASC`,
		[]any{"2026-03-01T11:45:00Z"},
		"idx_node_runs_running_started",
	)
	assertPlanUsesIndex(t, db,
		`SELECT wr.run_id
		 FROM workflow_runs wr
		 WHERE wr.autonomous_approvals = 0
		   AND ((wr.run_status = 'running' AND wr.plan_state = 'awaiting_plan_decision')
		    OR wr.run_status = 'awaiting_human_confirmation')
		 ORDER BY wr.updated_at DESC, wr.run_id DESC
		 LIMIT 20`,
		nil,
		"idx_workflow_runs_manual_decision_updated",
	)
	assertPlanUsesIndex(t, db,
		`SELECT CASE
		          WHEN unixepoch(created_at) = unixepoch(?) THEN 11
		          ELSE CAST((unixepoch(created_at) - unixepoch(?)) / 7200 AS INTEGER)
		        END AS bucket_index,
		        COUNT(1)
		 FROM workflow_runs
		 WHERE created_at >= ?
		   AND created_at <= ?
		 GROUP BY bucket_index
		 ORDER BY bucket_index ASC`,
		[]any{"2026-03-02T12:00:00Z", "2026-03-01T12:00:00Z", "2026-03-01T12:00:00Z", "2026-03-02T12:00:00Z"},
		"idx_workflow_runs_created_at",
	)
	assertPlanUsesIndex(t, db,
		`SELECT COUNT(DISTINCT nr.attempt)
		 FROM node_runs nr
		 JOIN workflow_runs wr ON wr.run_id = nr.run_id
		 JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id AND wn.node_id = nr.node_id
		 WHERE nr.run_id = ?
		   AND wn.node_class = 'execution_review'
		   AND nr.node_status = 'failed'`,
		[]any{"run_1"},
		"idx_node_runs_run_status_node_attempt",
	)
}

func TestBootstrapMigrationScaffoldMatchesCanonicalMigrations(t *testing.T) {
	t.Parallel()

	projectRoot := t.TempDir()
	kingdomHome := filepath.Join(projectRoot, ".kingdom")
	if err := bootstrapassets.WriteScaffold(kingdomHome, projectRoot, false); err != nil {
		t.Fatalf("write scaffold: %v", err)
	}

	canonicalNames, err := dbmigrations.List()
	if err != nil {
		t.Fatalf("list canonical migrations: %v", err)
	}
	scaffoldNames := migrationDirFileNames(t, filepath.Join(kingdomHome, "migrations"))
	if strings.Join(canonicalNames, ",") != strings.Join(scaffoldNames, ",") {
		t.Fatalf("canonical/scaffold migration names differ: canonical=%v scaffold=%v", canonicalNames, scaffoldNames)
	}
	expectedNames := []string{
		"0001_initial_schema.up.sql",
		"0002_events_execution_activity_index.up.sql",
		"0003_external_sources_and_idempotency_retention.up.sql",
		"0004_resumable_workspace_tree_cleanup.up.sql",
		"0005_unbounded_planning_external_context.up.sql",
		"0006_execution_recovery_and_response_workspace_cleanup.up.sql",
		"0008_provider_content.up.sql",
		"0009_planning_content.up.sql",
		"0010_artifact_content.up.sql",
		"0011_complete_external_comments.up.sql",
		"0012_provider_stream_content.up.sql",
		"0013_request_content.up.sql",
		"0014_planning_text.up.sql",
		"0015_execution_input_content.up.sql",
	}
	if strings.Join(canonicalNames, ",") != strings.Join(expectedNames, ",") {
		t.Fatalf("expected canonical migration sequence %v, got %v", expectedNames, canonicalNames)
	}

	for _, name := range canonicalNames {
		if !strings.HasSuffix(name, ".up.sql") {
			t.Fatalf("unexpected non-up migration file %q; migration assets are intentionally forward-only", name)
		}
		canonicalMigration, err := dbmigrations.Read(name)
		if err != nil {
			t.Fatalf("read canonical migration %s: %v", name, err)
		}
		scaffoldMigration, err := os.ReadFile(filepath.Join(kingdomHome, "migrations", name))
		if err != nil {
			t.Fatalf("read scaffold migration %s: %v", name, err)
		}
		if string(canonicalMigration) != string(scaffoldMigration) {
			t.Fatalf("scaffold migration %s must match canonical migration", name)
		}
		if name == "0003_external_sources_and_idempotency_retention.up.sql" {
			upper := strings.ToUpper(string(canonicalMigration))
			if strings.Contains(upper, "ALTER TABLE") || strings.Contains(upper, "_LEGACY") {
				t.Fatal("consolidated external migration must be create-only without legacy rebuilds")
			}
		}
	}
}

func TestFreshDatabaseGetsFinalExternalSchema(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	names, err := dbmigrations.List()
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	migrations := make([]SQLMigration, 0, len(names))
	for _, name := range names {
		raw, err := dbmigrations.Read(name)
		if err != nil {
			t.Fatalf("read migration %s: %v", name, err)
		}
		migrations = append(migrations, SQLMigration{Name: name, SQL: raw})
	}
	if err := ApplySQLMigrations(context.Background(), db, migrations, time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("apply consolidated migrations: %v", err)
	}

	for _, column := range []string{"credential_strategy", "app_slug", "credential_generation", "validated_generation"} {
		if !columnExists(t, db, "external_connections", column) {
			t.Fatalf("external_connections.%s is missing", column)
		}
	}
	if _, err := db.Exec(`INSERT INTO external_connections (connection_id,provider,credential_strategy,display_name,app_id,client_id,app_slug,credential_handle,lifecycle,health_class,credential_generation,validated_generation,created_at,updated_at) VALUES ('invalid-healthy','github','app_private_key','Invalid','1','Iv1','app','github-app:invalid-healthy','enabled','healthy',0,0,'2026-08-15T12:00:00Z','2026-08-15T12:00:00Z')`); err == nil {
		t.Fatal("schema accepted healthy connection with generation zero")
	}
	// Live installation claims are separate from the durable account identity.
	for _, column := range []string{"installation_id", "connection_id"} {
		if !columnExists(t, db, "external_connection_installations", column) {
			t.Fatalf("external_connection_installations.%s is missing", column)
		}
	}
	for _, column := range []string{"installation_id", "account_id", "account_login"} {
		if !columnExists(t, db, "external_installations", column) {
			t.Fatalf("external_installations.%s is missing", column)
		}
	}
	for _, column := range []string{"account_id", "account_login"} {
		if columnExists(t, db, "external_connection_installations", column) {
			t.Fatalf("external_connection_installations.%s must not duplicate durable identity", column)
		}
	}
	for _, column := range []string{"installation_id", "installation_account_id", "installation_account_login"} {
		if columnExists(t, db, "external_connections", column) {
			t.Fatalf("external_connections.%s survived the move to the installations table", column)
		}
	}
	for _, column := range []string{"comments_etag", "fetched_comment_pages", "omitted_comment_count", "omitted_comment_pages", "created_at_unix_nano"} {
		if !columnExists(t, db, "external_source_snapshots", column) {
			t.Fatalf("external_source_snapshots.%s is missing", column)
		}
	}
	if !columnExists(t, db, "external_source_observations", "retry_at") {
		t.Fatal("external_source_observations.retry_at is missing")
	}
	if columnExists(t, db, "external_source_observations", "change_summary_json") {
		t.Fatal("unused external_source_observations.change_summary_json survived consolidation")
	}
	var linkHeadsSQL string
	if err := db.QueryRow(`SELECT sql FROM sqlite_master WHERE type='table' AND name='external_source_link_heads'`).Scan(&linkHeadsSQL); err != nil {
		t.Fatalf("read external_source_link_heads schema: %v", err)
	}
	for _, status := range []string{"revoked", "permission_mismatch", "timeout", "malformed_response", "oversized_response"} {
		if !strings.Contains(linkHeadsSQL, "'"+status+"'") {
			t.Fatalf("external_source_link_heads status enum is missing %q", status)
		}
	}
	for _, column := range []string{"event_sequence", "hash_version"} {
		if !columnExists(t, db, "external_integration_audit_events", column) {
			t.Fatalf("external_integration_audit_events.%s is missing", column)
		}
	}

	var sequence int
	var hash string
	if err := db.QueryRow(`SELECT last_sequence,last_hash FROM external_integration_audit_head WHERE head_id=1`).Scan(&sequence, &hash); err != nil {
		t.Fatalf("read initial external audit head: %v", err)
	}
	if sequence != 0 || hash != AuditGenesisPrevHash {
		t.Fatalf("initial external audit head=(%d,%q)", sequence, hash)
	}

	assertNoForeignKeyTo := func(table string, forbidden ...string) {
		t.Helper()
		rows, err := db.Query(`PRAGMA foreign_key_list(` + table + `)`)
		if err != nil {
			t.Fatalf("foreign keys for %s: %v", table, err)
		}
		defer rows.Close()
		for rows.Next() {
			var id, seq int
			var target, from, to, onUpdate, onDelete, match string
			if err := rows.Scan(&id, &seq, &target, &from, &to, &onUpdate, &onDelete, &match); err != nil {
				t.Fatalf("scan foreign key for %s: %v", table, err)
			}
			for _, forbiddenTarget := range forbidden {
				if target == forbiddenTarget {
					t.Fatalf("%s unexpectedly references detached target %s", table, target)
				}
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate foreign keys for %s: %v", table, err)
		}
	}
	assertNoForeignKeyTo("external_source_links", "planning_sessions", "work_orders")
	assertNoForeignKeyTo("planning_external_source_adoptions", "planning_sessions")
	assertNoForeignKeyTo("planning_operation_external_snapshots", "planning_operations")
}

func TestFreshDatabaseGetsFencedResumableManagedWorktreeCleanupSchema(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	names, err := dbmigrations.List()
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	migrations := make([]SQLMigration, 0, len(names))
	for _, name := range names {
		raw, err := dbmigrations.Read(name)
		if err != nil {
			t.Fatalf("read migration %s: %v", name, err)
		}
		migrations = append(migrations, SQLMigration{Name: name, SQL: raw})
	}
	if err := ApplySQLMigrations(context.Background(), db, migrations, time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("apply canonical migrations: %v", err)
	}

	for _, column := range []string{"cleanup_cursor", "cleanup_owner", "cleanup_owner_pid", "cleanup_owner_identity_token", "cleanup_fence", "cleanup_started"} {
		if !columnExists(t, db, "run_worktrees", column) {
			t.Fatalf("run_worktrees.%s is missing", column)
		}
	}
	if !columnExists(t, db, "run_execution_targets", "cleanup_cursor") {
		t.Fatal("run_execution_targets.cleanup_cursor is missing")
	}
	seedMinimumGraph(t, db)
	mustExec(t, db, `INSERT INTO run_worktrees (run_id, repo_common_dir, main_worktree_root, project_relpath, worktree_name, seed_commit_oid, state, created_at, last_used_at) VALUES ('run_1', '/repo/.git', '/repo', '.', 'managed-run', '1111111111111111111111111111111111111111', 'active', '2026-08-15T12:00:00Z', '2026-08-15T12:00:00Z')`)

	var cursor, owner, ownerIdentity string
	var ownerPID, fence, started int
	if err := db.QueryRow(`SELECT cleanup_cursor, cleanup_owner, cleanup_owner_pid, cleanup_owner_identity_token, cleanup_fence, cleanup_started FROM run_worktrees WHERE run_id = 'run_1'`).Scan(&cursor, &owner, &ownerPID, &ownerIdentity, &fence, &started); err != nil {
		t.Fatalf("read default run worktree cleanup fence: %v", err)
	}
	if cursor != "" || owner != "" || ownerPID != 0 || ownerIdentity != "" || fence != 0 || started != 0 {
		t.Fatalf("unexpected initial cleanup state: cursor=%q owner=%q owner_pid=%d owner_identity=%q fence=%d started=%d", cursor, owner, ownerPID, ownerIdentity, fence, started)
	}
	if _, err := db.Exec(`UPDATE run_worktrees SET cleanup_fence = -1 WHERE run_id = 'run_1'`); err == nil {
		t.Fatal("schema accepted a negative worktree cleanup fence")
	}
	if _, err := db.Exec(`UPDATE run_worktrees SET cleanup_started = 2 WHERE run_id = 'run_1'`); err == nil {
		t.Fatal("schema accepted an invalid worktree cleanup-started flag")
	}
}

func TestApplySQLMigrationsBaselineOnly(t *testing.T) {
	t.Parallel()

	load := func(t *testing.T, names ...string) []SQLMigration {
		t.Helper()
		migrations := make([]SQLMigration, 0, len(names))
		for _, name := range names {
			raw, err := dbmigrations.Read(name)
			if err != nil {
				t.Fatalf("read migration %s: %v", name, err)
			}
			migrations = append(migrations, SQLMigration{Name: name, SQL: raw})
		}
		return migrations
	}

	t.Run("fresh_database_applies_single_baseline", func(t *testing.T) {
		db := openTestDB(t)
		if err := ApplySQLMigrations(context.Background(), db, load(t,
			"0001_initial_schema.up.sql",
		), time.Date(2026, 4, 21, 12, 0, 0, 0, time.UTC)); err != nil {
			t.Fatalf("apply migrations: %v", err)
		}

		if !tableExists(t, db, "schema_migrations") {
			t.Fatal("expected schema_migrations table to exist")
		}
		if !tableExists(t, db, "planning_sessions") {
			t.Fatal("expected planning_sessions table to exist")
		}

		var names []string
		rows, err := db.Query(`SELECT name FROM schema_migrations ORDER BY name ASC`)
		if err != nil {
			t.Fatalf("query schema_migrations: %v", err)
		}
		defer rows.Close()
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatalf("scan schema migration name: %v", err)
			}
			names = append(names, name)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate schema migration names: %v", err)
		}
		if len(names) != 1 || names[0] != "0001_initial_schema.up.sql" {
			t.Fatalf("expected only baseline migration recorded, got %v", names)
		}
	})

	t.Run("existing_database_does_not_duplicate_baseline", func(t *testing.T) {
		db := openTestDB(t)
		if err := ApplySQLMigrations(context.Background(), db, load(t,
			"0001_initial_schema.up.sql",
		), time.Date(2026, 4, 21, 12, 4, 0, 0, time.UTC)); err != nil {
			t.Fatalf("apply baseline migration: %v", err)
		}

		if err := ApplySQLMigrations(context.Background(), db, load(t,
			"0001_initial_schema.up.sql",
		), time.Date(2026, 4, 21, 12, 5, 0, 0, time.UTC)); err != nil {
			t.Fatalf("apply migrations to existing db: %v", err)
		}

		if !tableExists(t, db, "planning_sessions") {
			t.Fatal("expected planning_sessions table to exist after re-applying baseline")
		}

		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&count); err != nil {
			t.Fatalf("count schema_migrations rows: %v", err)
		}
		if count != 1 {
			t.Fatalf("expected 1 applied migration recorded, got %d", count)
		}
	})

	t.Run("existing_database_applies_later_forward_migration", func(t *testing.T) {
		db := openTestDB(t)
		if err := ApplySQLMigrations(context.Background(), db, load(t,
			"0001_initial_schema.up.sql",
		), time.Date(2026, 4, 21, 12, 0, 0, 0, time.UTC)); err != nil {
			t.Fatalf("apply baseline migration: %v", err)
		}

		migrations := append(load(t, "0001_initial_schema.up.sql"),
			SQLMigration{Name: "0002_future_feature.up.sql", SQL: []byte(`CREATE TABLE future_feature_flags (feature_id TEXT PRIMARY KEY)`)},
		)
		if err := ApplySQLMigrations(context.Background(), db, migrations, time.Date(2026, 4, 22, 12, 0, 0, 0, time.UTC)); err != nil {
			t.Fatalf("apply forward migration set: %v", err)
		}

		if !tableExists(t, db, "future_feature_flags") {
			t.Fatal("expected later forward migration to create future_feature_flags")
		}

		var names []string
		rows, err := db.Query(`SELECT name FROM schema_migrations ORDER BY name ASC`)
		if err != nil {
			t.Fatalf("query schema_migrations: %v", err)
		}
		defer rows.Close()
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatalf("scan schema migration name: %v", err)
			}
			names = append(names, name)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate schema migration names: %v", err)
		}
		if got := strings.Join(names, ","); got != "0001_initial_schema.up.sql,0002_future_feature.up.sql" {
			t.Fatalf("expected baseline and forward migration recorded, got %v", names)
		}
	})

	t.Run("baseline_contains_final_runtime_execution_schema", func(t *testing.T) {
		db := openTestDB(t)
		if err := ApplySQLMigrations(context.Background(), db, load(t,
			"0001_initial_schema.up.sql",
		), time.Date(2026, 4, 21, 12, 0, 0, 0, time.UTC)); err != nil {
			t.Fatalf("apply baseline migration: %v", err)
		}

		seedMinimumGraph(t, db)
		if !columnExists(t, db, "node_runs", "superseded_at") {
			t.Fatal("expected node_runs.superseded_at in baseline schema")
		}
		if !columnExists(t, db, "attempt_executions", "launch_started_at") {
			t.Fatal("expected attempt_executions.launch_started_at in baseline schema")
		}
		if !columnExists(t, db, "attempt_executions", "progress_event_type") {
			t.Fatal("expected attempt_executions.progress_event_type in baseline schema")
		}
		if !columnExists(t, db, "attempt_executions", "progress_reason_code") {
			t.Fatal("expected attempt_executions.progress_reason_code in baseline schema")
		}
		if !columnExists(t, db, "attempt_executions", "progress_at") {
			t.Fatal("expected attempt_executions.progress_at in baseline schema")
		}
		if !columnExists(t, db, "attempt_executions", "base_snapshot_id") {
			t.Fatal("expected attempt_executions.base_snapshot_id in baseline schema")
		}
		if !columnExists(t, db, "attempt_executions", "result_snapshot_id") {
			t.Fatal("expected attempt_executions.result_snapshot_id in baseline schema")
		}
		if !indexExists(t, db, "idx_attempt_executions_launch_started") {
			t.Fatal("expected launch_started_at index in baseline schema")
		}
		if !columnExists(t, db, "idempotency_keys", "status") {
			t.Fatal("expected idempotency_keys.status in baseline schema")
		}
		if !columnExists(t, db, "idempotency_keys", "updated_at") {
			t.Fatal("expected idempotency_keys.updated_at in baseline schema")
		}
		if !columnExists(t, db, "artifacts", "artifact_action") {
			t.Fatal("expected artifacts.artifact_action in baseline schema")
		}
		for _, tableName := range []string{"run_execution_targets", "filesystem_snapshots", "filesystem_snapshot_entries", "filesystem_blobs", "run_publication_batches", "run_publication_entries", "run_publication_paths"} {
			if !tableExists(t, db, tableName) {
				t.Fatalf("expected %s table in baseline schema", tableName)
			}
		}

		rows, err := db.Query(`PRAGMA foreign_key_check`)
		if err != nil {
			t.Fatalf("run foreign_key_check after baseline apply: %v", err)
		}
		defer rows.Close()
		if rows.Next() {
			var tableName string
			var rowID int64
			var parentTable string
			var fkIndex int64
			if err := rows.Scan(&tableName, &rowID, &parentTable, &fkIndex); err != nil {
				t.Fatalf("scan foreign_key_check row: %v", err)
			}
			t.Fatalf("expected no foreign key violations after upgrade, got table=%s rowid=%d parent=%s fk=%d", tableName, rowID, parentTable, fkIndex)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate foreign_key_check rows: %v", err)
		}
	})

	t.Run("rejects foreign-key-off migration with broken references", func(t *testing.T) {
		db := openTestDB(t)
		if err := ApplySQLMigrations(context.Background(), db, load(t, "0001_initial_schema.up.sql"), time.Date(2026, 4, 21, 12, 0, 0, 0, time.UTC)); err != nil {
			t.Fatalf("apply baseline migration: %v", err)
		}
		seedMinimumGraph(t, db)
		mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:work_intake:1', 'run_1', 'nr_1', 'running', 'provider_backed', 'server_response', '', NULL, 0, '', '2026-03-01T12:00:00Z', NULL, NULL, '', '', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)

		migrations := append(load(t, "0001_initial_schema.up.sql"), SQLMigration{
			Name: "0002_broken_fk_rebuild.up.sql",
			SQL: []byte(`-- kingdom: foreign_keys=off
DELETE FROM node_runs;`),
		})

		err := ApplySQLMigrations(context.Background(), db, migrations, time.Date(2026, 4, 21, 12, 7, 0, 0, time.UTC))
		if err == nil {
			t.Fatal("expected broken foreign-key-off migration to fail")
		}
		if !strings.Contains(err.Error(), "validate foreign keys") {
			t.Fatalf("expected foreign key validation failure, got %v", err)
		}
		if tableExists(t, db, "fk_bad_child") {
			t.Fatal("expected broken migration table to roll back")
		}

		var count int
		if err := db.QueryRow(`SELECT COUNT(1) FROM schema_migrations WHERE name = '0002_broken_fk_rebuild.up.sql'`).Scan(&count); err != nil {
			t.Fatalf("query broken migration recording: %v", err)
		}
		if count != 0 {
			t.Fatalf("expected broken migration to remain unrecorded, got %d rows", count)
		}
	})

	t.Run("ignores_non_up_migrations", func(t *testing.T) {
		db := openTestDB(t)
		migrations := append(load(t, "0001_initial_schema.up.sql"),
			SQLMigration{Name: "0002_placeholder.down.sql", SQL: []byte(`CREATE TABLE should_not_exist_down (id INTEGER PRIMARY KEY)`)},
			SQLMigration{Name: "notes.sql", SQL: []byte(`CREATE TABLE should_not_exist_notes (id INTEGER PRIMARY KEY)`)},
			SQLMigration{Name: "   ", SQL: []byte(`CREATE TABLE should_not_exist_blank (id INTEGER PRIMARY KEY)`)},
		)

		if err := ApplySQLMigrations(context.Background(), db, migrations, time.Date(2026, 4, 21, 12, 6, 0, 0, time.UTC)); err != nil {
			t.Fatalf("apply forward-only migrations: %v", err)
		}

		if tableExists(t, db, "should_not_exist_down") {
			t.Fatal("expected .down.sql migration to be ignored")
		}
		if tableExists(t, db, "should_not_exist_notes") {
			t.Fatal("expected non-up .sql migration to be ignored")
		}

		var names []string
		rows, err := db.Query(`SELECT name FROM schema_migrations ORDER BY name ASC`)
		if err != nil {
			t.Fatalf("query schema_migrations after ignored migrations: %v", err)
		}
		defer rows.Close()
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatalf("scan schema migration name: %v", err)
			}
			names = append(names, name)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iterate schema migration names: %v", err)
		}
		if len(names) != 1 || names[0] != "0001_initial_schema.up.sql" {
			t.Fatalf("expected only forward migration recorded, got %v", names)
		}
	})

}

func migrationDirFileNames(t *testing.T, dir string) []string {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read migration dir %s: %v", dir, err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	return names
}

func seedMinimumGraph(t *testing.T, db *sql.DB) {
	t.Helper()

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance', 'governance', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_1', 'D1', 'text', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_1', 'work-order', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_1', 'wfdef_1', 'work_intake', 'governance', 'work_intake', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_2', 'wfdef_1', 'completion', 'governance', 'completion', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_1', 'wfdef_1', 'work_intake', 'completion', 'always', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_1', NULL, 'work_order_1', 'wfdef_1', 'running', 'draft', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_1', 'run_1', 'work_intake', 1,1, 'running', 'governance', 'agent_governance', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO run_participants (run_id, agent_id, role_id, display_name, parent_agent_id, spawn_depth, first_seen_at) VALUES ('run_1', 'agent_governance', 'governance', 'Governance', NULL, 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO idempotency_keys (idempotency_key_id, route, actor_id, client_key, request_hash, response_status, response_headers_json, response_body_json, created_at, expires_at) VALUES ('idem_1', '/runs', 'actor_1', 'key_1', 'hash_1', 201, '{}', '{}', '2026-03-01T12:00:00Z', NULL)`)

	seedEvent := eventAuditRecord{
		EventID:         "evt_1",
		RunID:           "run_1",
		RunEventSeq:     1,
		NodeRunID:       sql.NullString{String: "nr_1", Valid: true},
		AttemptID:       "att_1",
		SchemaVersion:   "v1",
		EventType:       "attempt.started",
		Seq:             1,
		PayloadJSON:     "{}",
		ActorID:         "actor_1",
		RoleID:          "governance",
		AuthoritySource: "system",
		ReasonCode:      "internal_error",
		ReasonText:      "seed",
		CorrelationID:   "corr_1",
		CausationID:     "caus_1",
		CreatedAt:       "2026-03-01T12:00:00Z",
	}

	canonical, err := canonicalEventJSON(seedEvent)
	if err != nil {
		t.Fatalf("canonicalize seed event: %v", err)
	}
	prevHash := AuditGenesisPrevHash
	entryHash := entryHashFromCanonical(prevHash, canonical)

	mustExec(t, db, fmt.Sprintf(`INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_1', 'run_1', 1, 'nr_1', 'att_1', 'v1', 'attempt.started', 1, '{}', 'actor_1', 'governance', 'system', 'internal_error', 'seed', 'corr_1', 'caus_1', '%s', '%s', '2026-03-01T12:00:00Z')`, prevHash, entryHash))
}

// applyMigration applies the named migration and every later canonical
// migration in sorted order.
func applyMigration(t *testing.T, db *sql.DB, fileName string) {
	t.Helper()

	entries, err := dbmigrations.List()
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, n := range entries {
		if !strings.HasSuffix(n, ".up.sql") {
			continue
		}
		if n < fileName {
			continue
		}
		names = append(names, n)
	}
	sort.Strings(names)

	migrations := make([]SQLMigration, 0, len(names))
	for _, name := range names {
		raw, err := dbmigrations.Read(name)
		if err != nil {
			t.Fatalf("read migration %s: %v", name, err)
		}
		migrations = append(migrations, SQLMigration{Name: name, SQL: raw})
	}
	if err := ApplySQLMigrations(context.Background(), db, migrations, time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("apply migrations from %s: %v", fileName, err)
	}
}

func bindTestPlanningContext(t *testing.T, db *sql.DB, sessionID string) string {
	t.Helper()
	snapshotID := "context_" + sessionID
	mustExecArgs(t, db, `INSERT OR IGNORE INTO planning_context_snapshots (planning_context_snapshot_id,planning_session_id,summary_markdown,included_files_json,excluded_files_json,created_at) VALUES (?,?,'test context','[]','[]','2026-04-21T16:00:00Z')`, snapshotID, sessionID)
	mustExecArgs(t, db, `UPDATE planning_sessions SET context_snapshot_id=? WHERE planning_session_id=?`, snapshotID, sessionID)
	return snapshotID
}

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })

	mustExec(t, db, "PRAGMA foreign_keys = ON")
	return db
}

func tableExists(t *testing.T, db *sql.DB, tableName string) bool {
	t.Helper()

	var found string
	err := db.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name = ?`, tableName).Scan(&found)
	if err == sql.ErrNoRows {
		return false
	}
	if err != nil {
		t.Fatalf("query sqlite_master: %v", err)
	}
	return strings.EqualFold(found, tableName)
}

func indexExists(t *testing.T, db *sql.DB, indexName string) bool {
	t.Helper()

	var found string
	err := db.QueryRow(`SELECT name FROM sqlite_master WHERE type='index' AND name = ?`, indexName).Scan(&found)
	if err == sql.ErrNoRows {
		return false
	}
	if err != nil {
		t.Fatalf("query sqlite_master indexes: %v", err)
	}
	return strings.EqualFold(found, indexName)
}

func columnExists(t *testing.T, db *sql.DB, tableName string, columnName string) bool {
	t.Helper()

	rows, err := db.Query(`PRAGMA table_info(` + tableName + `)`)
	if err != nil {
		t.Fatalf("query table info for %s: %v", tableName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cid        int
			name       string
			columnType string
			notNull    int
			defaultV   sql.NullString
			primaryKey int
		)
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultV, &primaryKey); err != nil {
			t.Fatalf("scan table info for %s: %v", tableName, err)
		}
		if strings.EqualFold(name, columnName) {
			return true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate table info for %s: %v", tableName, err)
	}
	return false
}

func assertPlanUsesIndex(t *testing.T, db *sql.DB, query string, args []any, indexName string) {
	t.Helper()

	details := queryPlanDetails(t, db, query, args...)
	for _, detail := range details {
		if strings.Contains(detail, indexName) {
			return
		}
	}
	t.Fatalf("expected query plan for %q to use index %q, got %v", query, indexName, details)
}

func assertPlanUsesVirtualTableIndex(t *testing.T, db *sql.DB, query string, args []any, tableName string) {
	t.Helper()

	details := queryPlanDetails(t, db, query, args...)
	for _, detail := range details {
		if strings.Contains(detail, tableName) && strings.Contains(detail, "VIRTUAL TABLE INDEX") {
			return
		}
	}
	t.Fatalf("expected query plan for %q to use virtual table index on %q, got %v", query, tableName, details)
}

func queryPlanDetails(t *testing.T, db *sql.DB, query string, args ...any) []string {
	t.Helper()

	rows, err := db.Query(`EXPLAIN QUERY PLAN `+query, args...)
	if err != nil {
		t.Fatalf("explain query plan: %v", err)
	}
	defer rows.Close()

	var details []string
	for rows.Next() {
		var id int
		var parent int
		var notUsed int
		var detail string
		if err := rows.Scan(&id, &parent, &notUsed, &detail); err != nil {
			t.Fatalf("scan query plan: %v", err)
		}
		details = append(details, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate query plan: %v", err)
	}
	if len(details) == 0 {
		t.Fatal("expected explain query plan output")
	}
	return details
}

func mustExec(t *testing.T, db *sql.DB, query string) {
	t.Helper()

	if _, err := db.Exec(query); err != nil {
		t.Fatalf("exec failed: %v\nquery: %s", err, query)
	}
}
