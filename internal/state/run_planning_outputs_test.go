package state

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

func TestValidateRunPlanningOutputLineageTxRejectsAttemptFromDifferentNodeRun(t *testing.T) {
	t.Parallel()

	db := openMigratedTestDB(t)
	seedMinimumGraph(t, db)

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council', 'plan-author', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_plan_validation', 'wfdef_1', 'plan_drafting', 'plan-author', 'plan_drafting', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_plan_validation_1', 'run_1', 'plan_drafting', 1,1, 'succeeded', 'plan-author', 'agent_council', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_plan_validation_2', 'run_1', 'plan_drafting', 2,2, 'succeeded', 'plan-author', 'agent_council', 1, '2026-03-01T12:01:00Z', '2026-03-01T12:01:00Z')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:plan_drafting:2', 'run_1', 'nr_plan_validation_2', 'committed_success', 'provider_backed', 'server_response', '', NULL, 0, '', '2026-03-01T12:01:00Z', '2026-03-01T12:01:00Z', '2026-03-01T12:01:00Z', '', '', '2026-03-01T12:01:00Z', '2026-03-01T12:01:00Z')`)

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	err = validateRunPlanningOutputLineageTx(context.Background(), tx, CreateRunPlanningOutputRequest{
		PlanningOutputID: "planout_attempt_mismatch",
		RunID:            "run_1",
		NodeID:           "plan_drafting",
		NodeRunID:        "nr_plan_validation_1",
		AttemptID:        "run_1:plan_drafting:2",
		AgentID:          "agent_council",
		Kind:             RunPlanningOutputKindPlanRevision,
		ContentMarkdown:  "Current council plan revision",
		CreatedAt:        "2026-03-01T12:01:00Z",
	})
	if err == nil {
		t.Fatal("expected attempt lineage mismatch to be rejected")
	}
	if !strings.Contains(err.Error(), "attempt id must match node run lineage") {
		t.Fatalf("expected attempt lineage error, got %v", err)
	}
}

func TestPersistRunPlanningOutputTxIsIdempotentAndRefreshesPlanRevision(t *testing.T) {
	t.Parallel()

	db := openMigratedTestDB(t)
	seedRunPlanningOutputProjectionGraph(t, db)

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin first tx: %v", err)
	}
	if err := persistRunPlanningOutputTx(context.Background(), tx, db, testPlanRevisionOutputRequest()); err != nil {
		_ = tx.Rollback()
		t.Fatalf("persist initial planning output: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit initial planning output: %v", err)
	}

	tx, err = db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin duplicate tx: %v", err)
	}
	if err := persistRunPlanningOutputTx(context.Background(), tx, db, testPlanRevisionOutputRequest()); err != nil {
		_ = tx.Rollback()
		t.Fatalf("persist duplicate planning output: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit duplicate planning output: %v", err)
	}

	var planningOutputCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM run_planning_outputs WHERE planning_output_id = 'planout_projection_retry'`).Scan(&planningOutputCount); err != nil {
		t.Fatalf("count run planning outputs: %v", err)
	}
	if planningOutputCount != 1 {
		t.Fatalf("expected one persisted planning output, got %d", planningOutputCount)
	}

	store := NewRunPlanRevisionStore(db)
	revision, ok, err := store.LatestRunPlanRevision(context.Background(), "run_projection_retry")
	if err != nil {
		t.Fatalf("load latest plan revision: %v", err)
	}
	if !ok {
		t.Fatal("expected latest plan revision to exist")
	}
	if revision.ContentMarkdown != "Council revision from durable projection." {
		t.Fatalf("expected refreshed plan revision content, got %q", revision.ContentMarkdown)
	}
}

func TestProjectNodeTerminalFromDurableAttemptPersistsPlanningOutputWhenNodeAlreadyProjected(t *testing.T) {
	t.Parallel()

	db := openMigratedTestDB(t)
	seedRunPlanningOutputProjectionGraph(t, db)

	svc := NewTransitionService(db, nil)
	projected, err := svc.ProjectNodeTerminalFromDurableAttempt(context.Background(), DurableAttemptNodeProjectionRequest{
		RunID:      "run_projection_retry",
		NodeRunID:  "nr_projection_retry",
		AttemptID:  "run_projection_retry:plan_drafting:1",
		ActorID:    "runtime_orchestrator",
		OccurredAt: "2026-03-01T12:05:00Z",
		PlanningOutput: &CreateRunPlanningOutputRequest{
			PlanningOutputID: "planout_projection_retry",
			RunID:            "run_projection_retry",
			NodeID:           "plan_drafting",
			NodeRunID:        "nr_projection_retry",
			AttemptID:        "run_projection_retry:plan_drafting:1",
			AgentID:          "agent_projection_retry",
			Kind:             RunPlanningOutputKindPlanRevision,
			ContentMarkdown:  "Council revision from durable projection.",
			CreatedAt:        "2026-03-01T12:05:00Z",
		},
	})
	if err != nil {
		t.Fatalf("project durable terminal node: %v", err)
	}
	if projected {
		t.Fatal("expected already-terminal node projection to remain a transition no-op")
	}

	var planningOutputCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM run_planning_outputs WHERE attempt_id = 'run_projection_retry:plan_drafting:1'`).Scan(&planningOutputCount); err != nil {
		t.Fatalf("count projected planning outputs: %v", err)
	}
	if planningOutputCount != 1 {
		t.Fatalf("expected durable projection retry to persist one planning output, got %d", planningOutputCount)
	}
}

func seedRunPlanningOutputProjectionGraph(t *testing.T, db *sql.DB) {
	t.Helper()

	now := "2026-03-01T12:00:00Z"
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '`+now+`')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_projection_retry', 'plan-author', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_projection_retry', 'Projection Retry', 'text', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_projection_retry', 'work-order', 'v1', '{}', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_projection_retry', 'wfdef_projection_retry', 'plan_drafting', 'plan-author', 'plan_drafting', '`+now+`')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_projection_retry', NULL, 'work_order_projection_retry', 'wfdef_projection_retry', 'running', 'draft', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_projection_retry', 'run_projection_retry', 'plan_drafting', 1, 1, 'succeeded', 'plan-author', 'agent_projection_retry', 1, '`+now+`', '`+now+`')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_projection_retry:plan_drafting:1', 'run_projection_retry', 'nr_projection_retry', 'committed_success', 'runtime_owned', 'not_applicable', '', NULL, 0, '', '`+now+`', '`+now+`', '`+now+`', '`+now+`', '', '', '`+now+`', '`+now+`')`)
	canonicalEvent, err := canonicalEventJSON(eventAuditRecord{
		EventID:         "evt_projection_retry_started",
		RunID:           "run_projection_retry",
		RunEventSeq:     1,
		NodeRunID:       sql.NullString{String: "nr_projection_retry", Valid: true},
		AttemptID:       "run_projection_retry:plan_drafting:1",
		SchemaVersion:   "v1",
		EventType:       "attempt.started",
		Seq:             1,
		PayloadJSON:     `{}`,
		ActorID:         "runtime_orchestrator",
		RoleID:          "plan-author",
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      "orchestration_progress",
		ReasonText:      "seed",
		CorrelationID:   "corr_projection_retry",
		CausationID:     "caus_projection_retry",
		CreatedAt:       now,
	})
	if err != nil {
		t.Fatalf("canonicalize projection retry seed event: %v", err)
	}
	entryHash := entryHashFromCanonical(AuditGenesisPrevHash, canonicalEvent)
	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_projection_retry_started', 'run_projection_retry', 1, 'nr_projection_retry', 'run_projection_retry:plan_drafting:1', 'v1', 'attempt.started', 1, '{}', 'runtime_orchestrator', 'plan-author', 'runtime.orchestrator', 'orchestration_progress', 'seed', 'corr_projection_retry', 'caus_projection_retry', '`+AuditGenesisPrevHash+`', '`+entryHash+`', '`+now+`')`)
}

func testPlanRevisionOutputRequest() CreateRunPlanningOutputRequest {
	return CreateRunPlanningOutputRequest{
		PlanningOutputID: "planout_projection_retry",
		RunID:            "run_projection_retry",
		NodeID:           "plan_drafting",
		NodeRunID:        "nr_projection_retry",
		AttemptID:        "run_projection_retry:plan_drafting:1",
		AgentID:          "agent_projection_retry",
		Kind:             RunPlanningOutputKindPlanRevision,
		ContentMarkdown:  "Council revision from durable projection.",
		CreatedAt:        "2026-03-01T12:05:00Z",
	}
}
