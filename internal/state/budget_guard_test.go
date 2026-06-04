package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestBudgetGuardAllowsBoundaryValues(t *testing.T) {
	t.Parallel()

	db := openBudgetTestDB(t)
	applyBudgetMigration(t, db)
	seedBudgetRun(t, db, seedBudgetRunRequest{
		RunID:        "run_budget_boundary",
		RunStatus:    "running",
		CreatedAt:    "2026-03-01T10:00:00Z",
		AttemptCount: 60,
	})
	seedTokenUsage(t, db, "run_budget_boundary", 1_500_000)

	guard := NewBudgetGuard(db, nil)
	guard.now = func() time.Time {
		return mustParseTime(t, "2026-03-01T12:00:00Z")
	}

	result, err := guard.EnforceRunBudget(context.Background(), EnforceRunBudgetRequest{
		RunID: "run_budget_boundary",
	})
	if err != nil {
		t.Fatalf("enforce run budget: %v", err)
	}
	if result.Exceeded {
		t.Fatalf("expected boundary usage to be allowed, got %+v", result)
	}

	assertRunStatus(t, db, "run_budget_boundary", "running")
	assertEventCount(t, db, "run_budget_boundary", 0)
}

func TestBudgetGuardFailsRunWhenAttemptCapExceeded(t *testing.T) {
	t.Parallel()

	db := openBudgetTestDB(t)
	applyBudgetMigration(t, db)
	seedBudgetRun(t, db, seedBudgetRunRequest{
		RunID:        "run_budget_attempt_breach",
		RunStatus:    "running",
		CreatedAt:    "2026-03-01T11:30:00Z",
		AttemptCount: 61,
	})

	guard := NewBudgetGuard(db, nil)
	guard.now = func() time.Time {
		return mustParseTime(t, "2026-03-01T12:00:00Z")
	}

	result, err := guard.EnforceRunBudget(context.Background(), EnforceRunBudgetRequest{
		RunID:   "run_budget_attempt_breach",
		ActorID: "reconciler",
		RoleID:  "king",
	})
	if err == nil {
		t.Fatal("expected budget breach error")
	}

	var budgetErr *BudgetExceededError
	if !errors.As(err, &budgetErr) {
		t.Fatalf("expected BudgetExceededError, got %T", err)
	}
	if budgetErr.ExceededCap != "attempts" {
		t.Fatalf("expected attempts cap breach, got %q", budgetErr.ExceededCap)
	}
	if !result.Exceeded || result.ExceededCap != "attempts" {
		t.Fatalf("expected attempts breach result, got %+v", result)
	}

	assertRunStatus(t, db, "run_budget_attempt_breach", "failed")
	assertBudgetEscalationEventContains(t, db, "run_budget_attempt_breach", "\"exceeded_cap\":\"attempts\"")
	assertBudgetEscalationEventContains(t, db, "run_budget_attempt_breach", "\"limit\":60")
	assertBudgetEscalationEventContains(t, db, "run_budget_attempt_breach", "\"observed\":61")
}

func TestBudgetGuardFailsRunWhenTokenCapExceeded(t *testing.T) {
	t.Parallel()

	db := openBudgetTestDB(t)
	applyBudgetMigration(t, db)
	seedBudgetRun(t, db, seedBudgetRunRequest{
		RunID:        "run_budget_token_breach",
		RunStatus:    "running",
		CreatedAt:    "2026-03-01T11:30:00Z",
		AttemptCount: 3,
	})
	seedTokenUsage(t, db, "run_budget_token_breach", 1_500_001)

	guard := NewBudgetGuard(db, nil)
	guard.now = func() time.Time {
		return mustParseTime(t, "2026-03-01T12:00:00Z")
	}

	result, err := guard.EnforceRunBudget(context.Background(), EnforceRunBudgetRequest{
		RunID: "run_budget_token_breach",
	})
	if err == nil {
		t.Fatal("expected budget breach error")
	}

	var budgetErr *BudgetExceededError
	if !errors.As(err, &budgetErr) {
		t.Fatalf("expected BudgetExceededError, got %T", err)
	}
	if budgetErr.ExceededCap != "tokens" {
		t.Fatalf("expected tokens cap breach, got %q", budgetErr.ExceededCap)
	}
	if !result.Exceeded || result.ExceededCap != "tokens" {
		t.Fatalf("expected tokens breach result, got %+v", result)
	}

	assertRunStatus(t, db, "run_budget_token_breach", "failed")
	assertBudgetEscalationEventContains(t, db, "run_budget_token_breach", "\"exceeded_cap\":\"tokens\"")
	assertBudgetEscalationEventContains(t, db, "run_budget_token_breach", "\"limit\":1500000")
	assertBudgetEscalationEventContains(t, db, "run_budget_token_breach", "\"observed\":1500001")
}

func TestBudgetGuardFailsRunWhenWallTimeCapExceeded(t *testing.T) {
	t.Parallel()

	db := openBudgetTestDB(t)
	applyBudgetMigration(t, db)
	seedBudgetRun(t, db, seedBudgetRunRequest{
		RunID:        "run_budget_wall_breach",
		RunStatus:    "running",
		CreatedAt:    "2026-03-01T09:59:59Z",
		AttemptCount: 3,
	})
	seedTokenUsage(t, db, "run_budget_wall_breach", 500)

	guard := NewBudgetGuard(db, nil)
	guard.now = func() time.Time {
		return mustParseTime(t, "2026-03-01T12:00:00Z")
	}

	result, err := guard.EnforceRunBudget(context.Background(), EnforceRunBudgetRequest{
		RunID: "run_budget_wall_breach",
	})
	if err == nil {
		t.Fatal("expected budget breach error")
	}

	var budgetErr *BudgetExceededError
	if !errors.As(err, &budgetErr) {
		t.Fatalf("expected BudgetExceededError, got %T", err)
	}
	if budgetErr.ExceededCap != "wall_time" {
		t.Fatalf("expected wall_time cap breach, got %q", budgetErr.ExceededCap)
	}
	if !result.Exceeded || result.ExceededCap != "wall_time" {
		t.Fatalf("expected wall_time breach result, got %+v", result)
	}

	assertRunStatus(t, db, "run_budget_wall_breach", "failed")
	assertBudgetEscalationEventContains(t, db, "run_budget_wall_breach", "\"exceeded_cap\":\"wall_time\"")
	assertBudgetEscalationEventContains(t, db, "run_budget_wall_breach", "\"limit\":7200")
	assertBudgetEscalationEventContains(t, db, "run_budget_wall_breach", "\"observed\":7201")
}

func TestBudgetGuardDoesNotMutateNonRunningRun(t *testing.T) {
	t.Parallel()

	db := openBudgetTestDB(t)
	applyBudgetMigration(t, db)
	seedBudgetRun(t, db, seedBudgetRunRequest{
		RunID:        "run_budget_not_running",
		RunStatus:    "failed",
		CreatedAt:    "2026-03-01T09:00:00Z",
		AttemptCount: 70,
	})
	seedTokenUsage(t, db, "run_budget_not_running", 2_000_000)

	guard := NewBudgetGuard(db, nil)
	guard.now = func() time.Time {
		return mustParseTime(t, "2026-03-01T12:00:00Z")
	}

	result, err := guard.EnforceRunBudget(context.Background(), EnforceRunBudgetRequest{
		RunID: "run_budget_not_running",
	})
	if err != nil {
		t.Fatalf("expected non-running run to skip mutation, got %v", err)
	}
	if result.Exceeded {
		t.Fatalf("expected non-running run to skip enforcement, got %+v", result)
	}

	assertRunStatus(t, db, "run_budget_not_running", "failed")
	assertEventCount(t, db, "run_budget_not_running", 0)
}

func TestBudgetGuardBudgetFailureQuiescesLiveRunWork(t *testing.T) {
	t.Parallel()

	db := openBudgetTestDB(t)
	applyBudgetMigration(t, db)
	seedBudgetRun(t, db, seedBudgetRunRequest{
		RunID:        "run_budget_quiesce",
		RunStatus:    "running",
		CreatedAt:    "2026-03-01T11:30:00Z",
		AttemptCount: 0,
	})

	now := "2026-03-01T12:00:00Z"
	mustBudgetExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Builder', 0, '`+now+`')`)
	mustBudgetExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '`+now+`')`)
	mustBudgetExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'QA', 0, '`+now+`')`)
	mustBudgetExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder', 'builder-berserker', 'busy', NULL, 0, '`+now+`', '`+now+`')`)
	mustBudgetExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer', 'reviewer-shieldmaiden', 'idle', NULL, 0, '`+now+`', '`+now+`')`)
	mustBudgetExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_qa', 'intelligence-qa-scout', 'blocked', NULL, 0, '`+now+`', '`+now+`')`)
	mustBudgetExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_budget_exec', 'wfdef_1', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '`+now+`')`)
	mustBudgetExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_budget_review', 'wfdef_1', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '`+now+`')`)
	mustBudgetExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_budget_validation', 'wfdef_1', 'validation_gates_qa_scout', 'intelligence-qa-scout', 'validation_gates', '`+now+`')`)
	mustBudgetExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_budget_exec', 'run_budget_quiesce', 'delegated_execution_berserker', 1,1, 'running', 'builder-berserker', 'agent_builder', 1, '`+now+`', NULL)`)
	mustBudgetExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_budget_review', 'run_budget_quiesce', 'execution_review_shieldmaiden', 1,1, 'ready', 'reviewer-shieldmaiden', 'agent_reviewer', 1, NULL, NULL)`)
	mustBudgetExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_budget_validation', 'run_budget_quiesce', 'validation_gates_qa_scout', 1,1, 'blocked', 'intelligence-qa-scout', 'agent_qa', 1, '`+now+`', '`+now+`')`)
	mustBudgetExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_budget_quiesce:delegated_execution_berserker:1', 'run_budget_quiesce', 'nr_budget_exec', 'running', 'worker-1', '`+now+`', 1, 'session_budget_exec', '`+now+`', NULL, NULL, '', '', '`+now+`', '`+now+`')`)
	mustBudgetExec(t, db, `INSERT INTO session_leases (session_id, agent_id, status, lease_expires_at, last_heartbeat_at, created_at, updated_at) VALUES ('session_budget_exec', 'agent_builder', 'active', '`+now+`', '`+now+`', '`+now+`', '`+now+`')`)
	store := NewTokenUsageStore(db)
	if _, err := store.Insert(context.Background(), InsertTokenUsageRequest{
		RunID:        "run_budget_quiesce",
		NodeRunID:    "nr_budget_exec",
		AttemptID:    "run_budget_quiesce:delegated_execution_berserker:1",
		RequiredRole: "builder-berserker",
		Model:        "budget-test-model",
		TotalTokens:  1_500_001,
		CreatedAt:    now,
	}); err != nil {
		t.Fatalf("seed token usage: %v", err)
	}

	guard := NewBudgetGuard(db, nil)
	guard.now = func() time.Time {
		return mustParseTime(t, now)
	}

	result, err := guard.EnforceRunBudget(context.Background(), EnforceRunBudgetRequest{RunID: "run_budget_quiesce"})
	if err == nil {
		t.Fatal("expected budget breach error")
	}
	var budgetErr *BudgetExceededError
	if !errors.As(err, &budgetErr) {
		t.Fatalf("expected BudgetExceededError, got %T", err)
	}
	if !result.Exceeded || result.ExceededCap != "tokens" {
		t.Fatalf("expected tokens breach result, got %+v", result)
	}

	assertRunStatus(t, db, "run_budget_quiesce", "failed")
	assertNodeStatus(t, db, "nr_budget_exec", "cancelled")
	assertNodeStatus(t, db, "nr_budget_review", "cancelled")
	assertNodeStatus(t, db, "nr_budget_validation", "cancelled")
	assertAttemptExecutionState(t, db, "run_budget_quiesce:delegated_execution_berserker:1", "committed_cancelled")
	assertSessionLeaseStatus(t, db, "session_budget_exec", "closed")
	assertAgentStatus(t, db, "agent_builder", "idle")
	assertAgentStatus(t, db, "agent_qa", "idle")

	var cancelOutboxStatus string
	if err := db.QueryRow(`SELECT status FROM attempt_cancel_outbox WHERE attempt_id = 'run_budget_quiesce:delegated_execution_berserker:1'`).Scan(&cancelOutboxStatus); err != nil {
		t.Fatalf("query budget cancel outbox status: %v", err)
	}
	if cancelOutboxStatus != "pending" {
		t.Fatalf("expected pending budget cancel outbox status, got %q", cancelOutboxStatus)
	}
}

func openBudgetTestDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })

	mustBudgetExec(t, db, "PRAGMA foreign_keys = ON")
	return db
}

func applyBudgetMigration(t *testing.T, db *sql.DB) {
	t.Helper()
	applyMigration(t, db, "0001_initial_schema.up.sql")
}

type seedBudgetRunRequest struct {
	RunID        string
	RunStatus    string
	CreatedAt    string
	AttemptCount int
}

func seedBudgetRun(t *testing.T, db *sql.DB, req seedBudgetRunRequest) {
	t.Helper()

	if req.RunStatus == "" {
		req.RunStatus = "running"
	}
	if req.CreatedAt == "" {
		req.CreatedAt = "2026-03-01T12:00:00Z"
	}

	mustBudgetExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '2026-03-01T12:00:00Z')`)
	mustBudgetExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustBudgetExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_1', 'D1', 'text', '{}', '2026-03-01T12:00:00Z')`)
	mustBudgetExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_1', 'royal-decree', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustBudgetExec(t, db, fmt.Sprintf(`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES (%q, NULL, 'decree_1', 'wfdef_1', %q, 'draft', 1, %q, %q)`, req.RunID, req.RunStatus, req.CreatedAt, req.CreatedAt))

	for i := 1; i <= req.AttemptCount; i++ {
		nodeRunID := fmt.Sprintf("%s_nr_%d", req.RunID, i)
		nodeID := fmt.Sprintf("node_%d", i)
		mustBudgetExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES (%q, %q, %q, 1,1, 'running', 'king', 'agent_king', 1, %q, NULL)`, nodeRunID, req.RunID, nodeID, req.CreatedAt))
	}
}

func assertRunStatus(t *testing.T, db *sql.DB, runID string, expected string) {
	t.Helper()

	var runStatus string
	if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id = ?`, runID).Scan(&runStatus); err != nil {
		t.Fatalf("query run status: %v", err)
	}
	if runStatus != expected {
		t.Fatalf("expected run status %q, got %q", expected, runStatus)
	}
}

func assertEventCount(t *testing.T, db *sql.DB, runID string, expected int) {
	t.Helper()

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE run_id = ?`, runID).Scan(&count); err != nil {
		t.Fatalf("query events count: %v", err)
	}
	if count != expected {
		t.Fatalf("expected %d events for run %q, got %d", expected, runID, count)
	}
}

func assertBudgetEscalationEventContains(t *testing.T, db *sql.DB, runID string, expectedSubstring string) {
	t.Helper()

	var payload string
	err := db.QueryRow(`SELECT payload_json FROM events WHERE run_id = ? AND event_type = 'escalation.raised' AND reason_code = 'budget_exceeded' ORDER BY created_at DESC, seq DESC LIMIT 1`, runID).Scan(&payload)
	if err != nil {
		t.Fatalf("query budget escalation event: %v", err)
	}
	if !strings.Contains(payload, expectedSubstring) {
		t.Fatalf("expected payload to include %q, got %s", expectedSubstring, payload)
	}
}

func assertNodeStatus(t *testing.T, db *sql.DB, nodeRunID string, expected string) {
	t.Helper()

	var status string
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE node_run_id = ?`, nodeRunID).Scan(&status); err != nil {
		t.Fatalf("query node status: %v", err)
	}
	if status != expected {
		t.Fatalf("expected node %q status %q, got %q", nodeRunID, expected, status)
	}
}

func assertAttemptExecutionState(t *testing.T, db *sql.DB, attemptID string, expected string) {
	t.Helper()

	var state string
	if err := db.QueryRow(`SELECT state FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&state); err != nil {
		t.Fatalf("query attempt execution state: %v", err)
	}
	if state != expected {
		t.Fatalf("expected attempt %q state %q, got %q", attemptID, expected, state)
	}
}

func assertSessionLeaseStatus(t *testing.T, db *sql.DB, sessionID string, expected string) {
	t.Helper()

	var status string
	if err := db.QueryRow(`SELECT status FROM session_leases WHERE session_id = ?`, sessionID).Scan(&status); err != nil {
		t.Fatalf("query session lease status: %v", err)
	}
	if status != expected {
		t.Fatalf("expected session lease %q status %q, got %q", sessionID, expected, status)
	}
}

func assertAgentStatus(t *testing.T, db *sql.DB, agentID string, expected string) {
	t.Helper()

	var status string
	if err := db.QueryRow(`SELECT status FROM agents WHERE agent_id = ?`, agentID).Scan(&status); err != nil {
		t.Fatalf("query agent status: %v", err)
	}
	if status != expected {
		t.Fatalf("expected agent %q status %q, got %q", agentID, expected, status)
	}
}

func mustBudgetExec(t *testing.T, db *sql.DB, query string) {
	t.Helper()

	if _, err := db.Exec(query); err != nil {
		t.Fatalf("exec failed: %v\nquery: %s", err, query)
	}
}

func mustParseTime(t *testing.T, raw string) time.Time {
	t.Helper()

	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatalf("parse time %q: %v", raw, err)
	}
	return parsed
}

func seedTokenUsage(t *testing.T, db *sql.DB, runID string, totalTokens int64) {
	t.Helper()

	store := NewTokenUsageStore(db)
	_, err := store.Insert(context.Background(), InsertTokenUsageRequest{
		RunID:        runID,
		NodeRunID:    runID + "_nr_1",
		AttemptID:    "attempt_budget_seed",
		RequiredRole: "king",
		Model:        "budget-test-model",
		TotalTokens:  totalTokens,
		CreatedAt:    "2026-03-01T11:59:00Z",
	})
	if err != nil {
		t.Fatalf("seed token usage for run %q: %v", runID, err)
	}
}
