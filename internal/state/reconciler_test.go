package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"kingdom/internal/core/errorcatalog"
	"kingdom/internal/core/retryengine"
	executionstore "kingdom/internal/execution/store"
)

type alwaysRetryPlanner struct{}

func (alwaysRetryPlanner) Plan(errorcatalog.Code, int) (retryengine.PlanResult, error) {
	return retryengine.PlanResult{ShouldRetry: true, MaxRetries: 1}, nil
}

type fixedDelayRetryPlanner struct {
	delay time.Duration
}

func (p fixedDelayRetryPlanner) Plan(errorcatalog.Code, int) (retryengine.PlanResult, error) {
	return retryengine.PlanResult{
		ShouldRetry:      true,
		RetryClass:       "conditional_limited",
		EscalationTarget: "chancellor",
		MaxRetries:       1,
		NextDelay:        p.delay,
	}, nil
}

func TestReconcilerStartupRecoversAndReassignsStaleAttempt(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:05:30Z"
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T11:59:00Z' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `UPDATE node_runs SET started_at = '2026-03-01T11:58:00Z', finished_at = NULL WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king_backup', 'king', 'idle', NULL, 0, '2026-03-01T11:58:00Z', '2026-03-01T11:58:00Z')`)
	mustExec(t, db, `INSERT INTO session_leases (session_id, agent_id, status, lease_expires_at, last_heartbeat_at, created_at, updated_at) VALUES ('session_busy', 'agent_king', 'active', '2026-03-01T11:58:30Z', '2026-03-01T11:58:00Z', '2026-03-01T11:58:00Z', '2026-03-01T11:58:00Z')`)
	mustExec(t, db, `INSERT INTO session_leases (session_id, agent_id, status, lease_expires_at, last_heartbeat_at, created_at, updated_at) VALUES ('session_idle', 'agent_king_backup', 'active', '2026-03-01T11:58:30Z', '2026-03-01T11:58:00Z', '2026-03-01T11:58:00Z', '2026-03-01T11:58:00Z')`)

	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), fixedDelayRetryPlanner{delay: 2 * time.Second}, nil)
	if reconciler.db != db {
		t.Fatal("expected reconciler db pointer to match seeded db")
	}

	result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now})
	if err != nil {
		t.Fatalf("reconcile startup: %v", err)
	}

	if result.StaleAttemptsFound != 1 {
		t.Fatalf("expected one stale attempt, got %d", result.StaleAttemptsFound)
	}
	if result.AttemptsReassigned != 1 {
		t.Fatalf("expected one reassigned attempt, got %d", result.AttemptsReassigned)
	}
	if result.SessionsClosed != 2 {
		t.Fatalf("expected two closed sessions, got %d", result.SessionsClosed)
	}
	if result.SessionsRenewed != 0 {
		t.Fatalf("expected no renewed sessions once stale leases lose active attempts, got %d", result.SessionsRenewed)
	}

	if got := queryNodeStatus(t, db, "nr_1"); got != "blocked" {
		t.Fatalf("expected stale node to become blocked, got %q", got)
	}

	retryNodeRunID := "nr_1_retry_2"
	if got := queryNodeStatus(t, db, retryNodeRunID); got != "pending" {
		t.Fatalf("expected delayed reassigned retry node to remain pending, got %q", got)
	}
	if startedAt := queryNodeStartedAt(t, db, retryNodeRunID); startedAt != "" {
		t.Fatalf("expected retry node started_at to remain empty before dispatch, got %q", startedAt)
	}

	if got := queryNodeAssignedAgent(t, db, retryNodeRunID); got != "agent_king_backup" {
		t.Fatalf("unexpected retry assignment agent: got %q", got)
	}
	var dueAt, payloadJSON string
	if err := db.QueryRow(`SELECT due_at FROM retry_decisions WHERE run_id = 'run_1' AND node_run_id = 'nr_1'`).Scan(&dueAt); err != nil {
		t.Fatalf("load stale retry decision due time: %v", err)
	}
	if dueAt != "2026-03-01T12:05:32Z" {
		t.Fatalf("expected fixed stale retry delay to persist due_at, got %q", dueAt)
	}
	if err := db.QueryRow(`SELECT payload_json FROM attempt_event_log WHERE run_id = 'run_1' AND node_run_id = 'nr_1' AND event_type = 'attempt.retry_scheduled'`).Scan(&payloadJSON); err != nil {
		t.Fatalf("load stale retry scheduled event: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode stale retry scheduled event: %v", err)
	}
	if payload["scheduled_at"] != dueAt {
		t.Fatalf("stale retry scheduled_at=%#v, want persisted due_at=%q", payload["scheduled_at"], dueAt)
	}

	if got := querySessionStatus(t, db, "session_busy"); got != "closed" {
		t.Fatalf("expected stale busy session to close, got %q", got)
	}

	idleStatus, idleHeartbeat, idleExpiry := querySessionLeaseState(t, db, "session_idle")
	if idleStatus != "closed" {
		t.Fatalf("expected stale idle session without an active attempt to close, got %q", idleStatus)
	}
	if idleHeartbeat != "2026-03-01T11:58:00Z" {
		t.Fatalf("expected idle session heartbeat to remain unchanged on close, got %q", idleHeartbeat)
	}
	if idleExpiry != "2026-03-01T11:58:30Z" {
		t.Fatalf("expected idle session expiry to remain unchanged on close, got %q", idleExpiry)
	}

	if got := queryTransitionCount(t, db, "run_1"); got != 1 {
		t.Fatalf("expected blocked recovery transition before due release, got %d", got)
	}
	if got := queryEventTypeCount(t, db, "run_1", "attempt.restarted"); got != 0 {
		t.Fatalf("expected reconciler to avoid direct attempt.restarted events, got %d", got)
	}

	verifier := NewAuditIntegrityVerifier(db)
	if err := verifier.VerifyOrError(context.Background()); err != nil {
		t.Fatalf("expected reconciler recovery audit chain to verify: %v", err)
	}
}

func TestReconcilerStartupRecoversAndReassignsStaleAttemptWithDriftedReplacementStatus(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:05:30Z"
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T11:59:00Z' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `UPDATE node_runs SET started_at = '2026-03-01T11:58:00Z', finished_at = NULL WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king_backup', 'king', 'blocked', NULL, 0, '2026-03-01T11:58:00Z', '2026-03-01T11:58:00Z')`)
	mustExec(t, db, `INSERT INTO session_leases (session_id, agent_id, status, lease_expires_at, last_heartbeat_at, created_at, updated_at) VALUES ('session_busy', 'agent_king', 'active', '2026-03-01T11:58:30Z', '2026-03-01T11:58:00Z', '2026-03-01T11:58:00Z', '2026-03-01T11:58:00Z')`)

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)

	result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now})
	if err != nil {
		t.Fatalf("reconcile startup with drifted replacement status: %v", err)
	}
	if result.AttemptsReassigned != 1 {
		t.Fatalf("expected one reassigned attempt, got %d", result.AttemptsReassigned)
	}
	if got := queryNodeAssignedAgent(t, db, "nr_1_retry_2"); got != "agent_king_backup" {
		t.Fatalf("expected drifted replacement agent to be claimed for retry, got %q", got)
	}
	if got := queryReconcilerAgentStatus(t, db, "agent_king_backup"); got != "busy" {
		t.Fatalf("expected drifted replacement agent status to converge to busy after claim, got %q", got)
	}
}

func TestReconcilerSkipsAttemptsWithDurableTerminalExecutionState(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:05:30Z"
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T11:59:00Z' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `UPDATE node_runs SET started_at = '2026-03-01T11:58:00Z', finished_at = NULL WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:decree_intake:1', 'run_1', 'nr_1', 'committed_success', 'provider_backed', 'server_response', '', NULL, 1, 'session_run_1_decree_intake_1', '2026-03-01T11:58:00Z', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z', '', '', '2026-03-01T11:58:00Z', '2026-03-01T12:00:00Z')`)

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)
	result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now})
	if err != nil {
		t.Fatalf("reconcile with durable terminal execution: %v", err)
	}
	if result.StaleAttemptsFound != 0 {
		t.Fatalf("expected no stale attempts once durable execution is terminal, got %d", result.StaleAttemptsFound)
	}
	if got := queryNodeStatus(t, db, "nr_1"); got != "succeeded" {
		t.Fatalf("expected reconciler to project node status from durable execution, got %q", got)
	}
}

func TestReconcilerDoesNotRetryStartingAttemptWithActiveLaunchFence(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	now := "2026-03-01T12:09:30Z"
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('noble-delegate', 'Noble Delegate', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_noble', 'noble-delegate', 'busy', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_starting', 'starting', 'starting', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_starting', 'starting-flow', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_starting', 'wfdef_starting', 'advisor_consultation_noble', 'noble-delegate', 'advisor_consultation', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_starting', NULL, 'decree_starting', 'wfdef_starting', 'running', 'approved', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_starting_1', 'run_starting', 'advisor_consultation_noble', 1,1, 'starting', 'noble-delegate', 'agent_noble', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_starting:advisor_consultation_noble:1', 'run_starting', 'nr_starting_1', 'starting', 'provider_backed', 'server_response', '', NULL, 0, 'session_starting_1', '', '2026-03-01T12:08:00Z', NULL, NULL, NULL, '', '', '2026-03-01T12:08:00Z', '2026-03-01T12:08:00Z')`)
	mustExec(t, db, `INSERT INTO session_leases (session_id, agent_id, status, lease_expires_at, last_heartbeat_at, created_at, updated_at) VALUES ('session_starting_1', 'agent_noble', 'active', '2026-03-01T12:11:00Z', '2026-03-01T12:09:00Z', '2026-03-01T12:08:00Z', '2026-03-01T12:09:00Z')`)

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)
	result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now})
	if err != nil {
		t.Fatalf("reconcile starting launch fence: %v", err)
	}
	if result.StaleAttemptsFound != 0 {
		t.Fatalf("expected active starting launch fence to avoid stale recovery, got %d stale attempts", result.StaleAttemptsFound)
	}
	if got := queryNodeStatus(t, db, "nr_starting_1"); got != "starting" {
		t.Fatalf("expected starting attempt to remain starting while launch fence is active, got %q", got)
	}
	if count := queryEventTypeCount(t, db, "run_starting", "attempt.retry_queued"); count != 0 {
		t.Fatalf("expected no retry queueing while launch fence is active, got %d events", count)
	}
}

func TestFindStaleAttemptsKeepsRunningProviderAttemptWithLiveExecutionLease(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	now := time.Date(2026, 3, 1, 12, 13, 0, 0, time.UTC)
	mustExec(t, db, `UPDATE agents SET status = 'busy' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `UPDATE node_runs SET started_at = '2026-03-01T12:00:00Z' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, created_at, updated_at) VALUES ('run_1:decree_intake:1', 'run_1', 'nr_1', 'running', 'provider_backed', 'server_response', 'worker_1', '2026-03-01T12:20:00Z', 1, 'session_1', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)

	stale, err := findStaleAttemptsWithQuerier(context.Background(), db, now, time.Minute)
	if err != nil {
		t.Fatalf("find stale attempts: %v", err)
	}
	if len(stale) != 0 {
		t.Fatalf("expected a live provider execution lease to prevent stale fencing, got %#v", stale)
	}
}

func TestFindStaleAttemptsKeepsStartingProviderAttemptWithLiveExecutionLease(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	const nowText = "2026-09-28T12:13:00Z"
	mustExec(t, db, `UPDATE agents SET status = 'busy' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'starting', started_at = NULL WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, created_at, updated_at) VALUES ('run_1:decree_intake:1', 'run_1', 'nr_1', 'starting', 'provider_backed', 'server_response', 'worker_1', '2099-03-01T12:20:00Z', 1, 'session_1', '', '2026-03-01T12:00:00Z', NULL, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)

	now, err := time.Parse(time.RFC3339Nano, nowText)
	if err != nil {
		t.Fatalf("parse current time: %v", err)
	}
	stale, err := findStaleAttemptsWithQuerier(context.Background(), db, now, time.Minute)
	if err != nil {
		t.Fatalf("find stale attempts: %v", err)
	}
	if len(stale) != 0 {
		t.Fatalf("expected a live provider execution lease to prevent stale fencing while starting, got %#v", stale)
	}
}

func TestFindStaleAttemptsKeepsLeaseThatExpiresAfterVariablePrecisionNow(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	const nowText = "2026-09-25T10:00:00.1Z"
	mustExec(t, db, `UPDATE agents SET status = 'busy' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'running', started_at = '2026-09-25T09:00:00Z' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, created_at, updated_at) VALUES ('run_1:decree_intake:1', 'run_1', 'nr_1', 'running', 'provider_backed', 'server_response', 'worker_1', '2026-09-25T10:00:00.11Z', 1, 'session_1', '2026-09-25T09:00:00Z', '2026-09-25T09:00:00Z')`)

	now, err := time.Parse(time.RFC3339Nano, nowText)
	if err != nil {
		t.Fatalf("parse current time: %v", err)
	}
	stale, err := findStaleAttemptsWithQuerier(context.Background(), db, now, time.Minute)
	if err != nil {
		t.Fatalf("find stale attempts: %v", err)
	}
	if len(stale) != 0 {
		t.Fatalf("lease expiring at 2026-09-25T10:00:00.11Z was stale at %s: %#v", nowText, stale)
	}
}

func TestFindStaleAttemptsRespectsFractionalSecondCutoff(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	mustExec(t, db, `UPDATE agents SET status = 'busy' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `UPDATE node_runs SET started_at = '2026-03-01T12:00:00.100000001Z' WHERE node_run_id = 'nr_1'`)

	now := time.Date(2026, 3, 1, 12, 1, 0, 100000000, time.UTC)
	stale, err := findStaleAttemptsWithQuerier(context.Background(), db, now, time.Minute)
	if err != nil {
		t.Fatalf("find stale attempts: %v", err)
	}
	if len(stale) != 0 {
		t.Fatalf("attempt after fractional cutoff was classified stale: %#v", stale)
	}
}

func TestReconcilerSchedulesRetryForStartingProviderFailure(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	const attemptID = "run_1:decree_intake:1"
	mustExec(t, db, `UPDATE node_runs SET node_status = 'starting', started_at = NULL WHERE node_run_id = 'nr_1'`)
	mustExecArgs(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, committed_at) VALUES (?, 'run_1', 'nr_1', 'committed_failure', 'provider_backed', 'server_response', 'llm_rate_limited', 'provider rate limit', '2026-03-01T12:00:00.123456789Z', '2026-03-01T12:00:01.123456789Z', '2026-03-01T12:00:01.123456789Z')`, attemptID)

	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), loadRetryEngine(t), nil)
	if _, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: "2026-03-01T12:01:00.123456789Z"}); err != nil {
		t.Fatalf("reconcile starting provider failure: %v", err)
	}
	var decisions, successors int
	if err := db.QueryRow(`SELECT COUNT(1) FROM retry_decisions WHERE source_attempt_id = ? AND decision = 'retry'`, attemptID).Scan(&decisions); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM node_runs WHERE run_id = 'run_1' AND node_id = 'decree_intake' AND attempt = 2`).Scan(&successors); err != nil {
		t.Fatal(err)
	}
	if decisions != 1 || successors != 1 {
		t.Fatalf("recovery created decisions=%d successors=%d, want one each", decisions, successors)
	}
}

func TestReconcilerKeepsStartingLeaseOpenWhileLaunchFenceIsStillValid(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	now := "2026-03-01T12:09:50.1Z"
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('noble-delegate', 'Noble Delegate', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_noble', 'noble-delegate', 'busy', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_starting_lease', 'starting lease', 'starting lease', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_starting_lease', 'starting-lease-flow', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_starting_lease', 'wfdef_starting_lease', 'advisor_consultation_noble', 'noble-delegate', 'advisor_consultation', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_starting_lease', NULL, 'decree_starting_lease', 'wfdef_starting_lease', 'running', 'approved', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_starting_lease_1', 'run_starting_lease', 'advisor_consultation_noble', 1,1, 'starting', 'noble-delegate', 'agent_noble', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_starting_lease:advisor_consultation_noble:1', 'run_starting_lease', 'nr_starting_lease_1', 'starting', 'provider_backed', 'server_response', '', NULL, 0, 'session_starting_lease_1', '', '2026-03-01T12:09:00Z', NULL, NULL, NULL, '', '', '2026-03-01T12:09:00Z', '2026-03-01T12:09:00Z')`)
	mustExec(t, db, `INSERT INTO session_leases (session_id, agent_id, status, lease_expires_at, last_heartbeat_at, created_at, updated_at) VALUES ('session_starting_lease_1', 'agent_noble', 'active', '2026-03-01T12:11:00Z', '2026-03-01T12:09:00Z', '2026-03-01T12:09:00Z', '2026-03-01T12:09:00Z')`)

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)
	result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now})
	if err != nil {
		t.Fatalf("reconcile starting lease retention: %v", err)
	}
	if result.StaleAttemptsFound != 0 {
		t.Fatalf("expected active launch fence to avoid stale attempt recovery, got %d stale attempts", result.StaleAttemptsFound)
	}
	if result.SessionsClosed != 0 {
		t.Fatalf("expected starting lease to remain open while launch fence is valid, got %d closed sessions", result.SessionsClosed)
	}
	if result.SessionsRenewed != 1 {
		t.Fatalf("expected starting lease to renew while launch fence is still active, got %d renewals", result.SessionsRenewed)
	}
	status, heartbeat, expiry := querySessionLeaseState(t, db, "session_starting_lease_1")
	if status != "active" {
		t.Fatalf("expected starting lease to remain active, got %q", status)
	}
	if heartbeat != now {
		t.Fatalf("expected starting lease heartbeat to renew to %q, got %q", now, heartbeat)
	}
	if expiry != "2026-03-01T12:10:25.1Z" {
		t.Fatalf("expected starting lease expiry to extend with the renewal, got %q", expiry)
	}
}

func TestReconcilerRetriesStartingAttemptAfterLaunchFenceHeartbeatGoesStale(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	now := "2026-03-01T12:13:30Z"
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('noble-delegate', 'Noble Delegate', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_noble', 'noble-delegate', 'busy', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_noble_backup', 'noble-delegate', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_starting_stale', 'starting stale', 'starting stale', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_starting_stale', 'starting-stale-flow', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_starting_stale', 'wfdef_starting_stale', 'advisor_consultation_noble', 'noble-delegate', 'advisor_consultation', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_starting_stale', NULL, 'decree_starting_stale', 'wfdef_starting_stale', 'running', 'approved', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_starting_stale_1', 'run_starting_stale', 'advisor_consultation_noble', 1,1, 'starting', 'noble-delegate', 'agent_noble', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_starting_stale:advisor_consultation_noble:1', 'run_starting_stale', 'nr_starting_stale_1', 'starting', 'provider_backed', 'server_response', '', NULL, 0, 'session_starting_stale_1', '', '2026-03-01T12:08:00Z', NULL, NULL, NULL, '', '', '2026-03-01T12:08:00Z', '2026-03-01T12:08:00Z')`)
	mustExec(t, db, `INSERT INTO session_leases (session_id, agent_id, status, lease_expires_at, last_heartbeat_at, created_at, updated_at) VALUES ('session_starting_stale_1', 'agent_noble', 'active', '2026-03-01T12:12:00Z', '2026-03-01T12:09:00Z', '2026-03-01T12:08:00Z', '2026-03-01T12:09:00Z')`)

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)
	result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now})
	if err != nil {
		t.Fatalf("reconcile stale starting attempt: %v", err)
	}
	if result.StaleAttemptsFound != 1 {
		t.Fatalf("expected one stale starting attempt, got %d", result.StaleAttemptsFound)
	}
	if result.AttemptsReassigned != 1 {
		t.Fatalf("expected one reassigned starting attempt, got %d", result.AttemptsReassigned)
	}
	if got := queryNodeStatus(t, db, "nr_starting_stale_1"); got != "blocked" {
		t.Fatalf("expected stale starting attempt to become blocked, got %q", got)
	}
	if got := queryNodeStatus(t, db, "nr_starting_stale_1_retry_2"); got != "pending" {
		t.Fatalf("expected retry attempt to remain pending after stale starting recovery, got %q", got)
	}
	if got := queryNodeAssignedAgent(t, db, "nr_starting_stale_1_retry_2"); got != "agent_noble_backup" {
		t.Fatalf("expected stale starting retry to move to backup agent, got %q", got)
	}
}

func TestReconcilerRetriesStartingAttemptAfterLaunchFenceLeaseExpires(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	now := "2026-03-01T12:13:30Z"
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('noble-delegate', 'Noble Delegate', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_noble', 'noble-delegate', 'busy', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_noble_backup', 'noble-delegate', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_starting_expired', 'starting expired', 'starting expired', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_starting_expired', 'starting-expired-flow', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_starting_expired', 'wfdef_starting_expired', 'advisor_consultation_noble', 'noble-delegate', 'advisor_consultation', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_starting_expired', NULL, 'decree_starting_expired', 'wfdef_starting_expired', 'running', 'approved', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_starting_expired_1', 'run_starting_expired', 'advisor_consultation_noble', 1,1, 'starting', 'noble-delegate', 'agent_noble', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_starting_expired:advisor_consultation_noble:1', 'run_starting_expired', 'nr_starting_expired_1', 'starting', 'provider_backed', 'server_response', '', NULL, 0, 'session_starting_expired_1', '', '2026-03-01T12:08:00Z', NULL, NULL, NULL, '', '', '2026-03-01T12:08:00Z', '2026-03-01T12:08:00Z')`)
	mustExec(t, db, `INSERT INTO session_leases (session_id, agent_id, status, lease_expires_at, last_heartbeat_at, created_at, updated_at) VALUES ('session_starting_expired_1', 'agent_noble', 'active', '2026-03-01T12:12:00Z', '2026-03-01T12:13:00Z', '2026-03-01T12:08:00Z', '2026-03-01T12:13:00Z')`)

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)
	result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now})
	if err != nil {
		t.Fatalf("reconcile expired starting attempt: %v", err)
	}
	if result.StaleAttemptsFound != 1 {
		t.Fatalf("expected expired launch-fence lease to mark one stale attempt, got %d", result.StaleAttemptsFound)
	}
	if result.AttemptsReassigned != 1 {
		t.Fatalf("expected expired launch-fence lease to reassign one attempt, got %d", result.AttemptsReassigned)
	}
	if got := queryNodeStatus(t, db, "nr_starting_expired_1"); got != "blocked" {
		t.Fatalf("expected expired starting attempt to become blocked, got %q", got)
	}
	if got := queryNodeStatus(t, db, "nr_starting_expired_1_retry_2"); got != "pending" {
		t.Fatalf("expected retry attempt to remain pending after lease-expiry recovery, got %q", got)
	}
}

func TestReconcilerDoesNotUseNodeAttemptOrdinalAsRetryBudget(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:13:00Z"
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T12:08:00Z' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king_backup', 'king', 'idle', NULL, 0, '2026-03-01T12:08:00Z', '2026-03-01T12:08:00Z')`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'succeeded', finished_at = '2026-03-01T12:07:00Z' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_2', 'run_1', 'decree_intake', 2,2, 'running', 'king', 'agent_king', 1, '2026-03-01T12:08:00Z', NULL)`)
	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_nr_2_started', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_2', 'att_2', 'v1', 'attempt.started', 1, '{}', 'actor_1', 'king', 'system', 'internal_error', 'seed', 'corr_nr2', 'caus_nr2', 'prev_nr2', 'entry_nr2', '2026-03-01T12:08:00Z')`)

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)

	result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now})
	if err != nil {
		t.Fatalf("reconcile stale exhausted attempt: %v", err)
	}

	if result.AttemptsReassigned != 1 {
		t.Fatalf("expected one reassignment, got %d", result.AttemptsReassigned)
	}
	if result.AttemptsFailed != 0 {
		t.Fatalf("expected no failed attempt recovery, got %d", result.AttemptsFailed)
	}

	if got := queryNodeStatus(t, db, "nr_2"); got != "blocked" {
		t.Fatalf("expected stale attempt to be fenced, got %q", got)
	}

	if got := queryNodeStatus(t, db, "nr_2_retry_3"); got != "pending" {
		t.Fatalf("expected decision-counted retry successor to remain pending, got %q", got)
	}
}

func TestReconcilerStaleRetryCreatesSixtyFirstRunAttempt(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	now := "2026-03-01T12:05:30Z"
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T11:58:00Z' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `UPDATE node_runs SET started_at = '2026-03-01T11:58:00Z', finished_at = NULL WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 1, '2026-03-01T11:58:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king_backup', 'king', 'idle', NULL, 0, '2026-03-01T11:58:00Z', '2026-03-01T11:58:00Z')`)
	for i := 0; i < 59; i++ {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-budget-stale-%d', 'run_1', 'budget-stale-node-%d', 1, 1, 'failed', 'king', 'agent_king', 1, '2026-03-01T11:58:00Z', '2026-03-01T11:59:00Z')`, i, i))
	}

	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), loadRetryEngine(t), nil)
	result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now})
	if err != nil {
		t.Fatalf("reconcile stale budget boundary: %v", err)
	}
	if result.AttemptsReassigned != 1 || result.AttemptsFailed != 0 {
		t.Fatalf("stale budget recovery result=%+v", result)
	}
	var attempts, pendingRetries int
	var decision, reason, runStatus string
	if err := db.QueryRow(`SELECT COUNT(1) FROM node_runs WHERE run_id = 'run_1'`).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT decision, error_code FROM retry_decisions WHERE node_run_id = 'nr_1'`).Scan(&decision, &reason); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT run_status FROM workflow_runs WHERE run_id = 'run_1'`).Scan(&runStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM retry_decisions WHERE decision = 'retry' AND dispatch_state = 'pending'`).Scan(&pendingRetries); err != nil {
		t.Fatal(err)
	}
	if attempts != 61 || decision != string(RetryDecisionRetry) || reason != "agent_unresponsive" || runStatus != "running" || pendingRetries != 1 {
		t.Fatalf("stale budget outcome attempts=%d decision=%q reason=%q run=%q pending=%d", attempts, decision, reason, runStatus, pendingRetries)
	}
}

func TestReconcilerStaleRetryExhaustionPersistsDecisionAndDeliversEscalationOnce(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T11:58:00Z' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `UPDATE node_runs SET started_at = '2026-03-01T11:58:00Z', finished_at = NULL WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king_backup', 'king', 'idle', NULL, 0, '2026-03-01T11:58:00Z', '2026-03-01T11:58:00Z')`)

	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), loadRetryEngine(t), nil)
	firstNow := "2026-03-01T12:05:30Z"
	if result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: firstNow}); err != nil || result.AttemptsReassigned != 1 {
		t.Fatalf("schedule stale successor result=%+v err=%v", result, err)
	}
	var successor, dueAt string
	if err := db.QueryRow(`SELECT target_node_run_id, due_at FROM retry_decisions WHERE node_run_id = 'nr_1'`).Scan(&successor, &dueAt); err != nil {
		t.Fatal(err)
	}
	if _, err := NewTransitionService(db, nil).ReleaseDueDecreeRetries(context.Background(), "run_1", dueAt); err != nil {
		t.Fatalf("release stale successor: %v", err)
	}
	exhaustedAttemptID := composeAttemptID("run_1", "decree_intake", 2)
	store := executionstore.New(db)
	if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{AttemptID: exhaustedAttemptID, RunID: "run_1", NodeRunID: successor, ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeServerResponse, SessionRef: "session_stale_successor", CreatedAt: "2026-03-01T12:06:00Z"}); err != nil {
		t.Fatalf("enqueue stale successor: %v", err)
	}
	if _, err := store.CreateProviderInvocation(context.Background(), executionstore.CreateProviderInvocationRequest{ProviderInvocationID: "invocation_stale_successor", AttemptID: exhaustedAttemptID, RunID: "run_1", NodeRunID: successor, LogicalSessionID: "session_stale_successor", CompletionMode: executionstore.ProviderCompletionModeServerResponse, IssuedAt: "2026-03-01T12:06:00Z", CreatedAt: "2026-03-01T12:06:00Z"}); err != nil {
		t.Fatalf("create stale successor invocation: %v", err)
	}
	if _, err := store.MarkProviderInvocationOpen(context.Background(), executionstore.MarkProviderInvocationOpenRequest{ProviderInvocationID: "invocation_stale_successor", AttemptID: exhaustedAttemptID, ProviderSessionID: "provider_stale_successor", OpenedAt: "2026-03-01T12:06:01Z"}); err != nil {
		t.Fatalf("open stale successor invocation: %v", err)
	}
	mustExec(t, db, `UPDATE node_runs SET node_status = 'running', started_at = '2026-03-01T12:06:00Z', finished_at = NULL WHERE node_run_id = '`+successor+`'`)

	secondNow := "2026-03-01T12:10:00Z"
	staleSuccessor := staleAttempt{NodeRunID: successor, RunID: "run_1", NodeID: "decree_intake", Attempt: 2, Wave: 1, RequiredRole: "king", AgentID: "agent_king_backup", CurrentStatus: "running", StartedAt: "2026-03-01T12:06:00Z"}
	if result, err := reconciler.recoverAttempt(context.Background(), staleSuccessor, mustParseRFC3339(t, secondNow)); err != nil || !result.failed {
		t.Fatalf("exhaust stale successor result=%+v err=%v", result, err)
	}
	if result, err := reconciler.recoverAttempt(context.Background(), staleSuccessor, mustParseRFC3339(t, secondNow)); err != nil || result.failed || result.reassigned {
		t.Fatalf("repeat exhausted stale recovery: %v", err)
	}

	var decision, resolution, target, invocationState, cancelState string
	var decisions, escalations, deliveries, cancellations int
	if err := db.QueryRow(`SELECT decision, resolution, escalation_target, source_attempt_id FROM retry_decisions WHERE node_run_id = ?`, successor).Scan(&decision, &resolution, &target, &exhaustedAttemptID); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM retry_decisions WHERE node_run_id = ?`, successor).Scan(&decisions); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_event_log WHERE attempt_id = ? AND event_type = 'escalation.raised'`, exhaustedAttemptID).Scan(&escalations); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM mailbox_messages WHERE run_id = 'run_1' AND message_type = 'escalation' AND to_agent_id = 'agent_chancellor'`).Scan(&deliveries); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT invocation_state FROM provider_invocations WHERE provider_invocation_id = 'invocation_stale_successor'`).Scan(&invocationState); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(1), COALESCE(MAX(status), '') FROM attempt_cancel_outbox WHERE attempt_id = ?`, exhaustedAttemptID).Scan(&cancellations, &cancelState); err != nil {
		t.Fatal(err)
	}
	if decision != string(RetryDecisionExhausted) || resolution != string(RetryDecisionResolutionEscalated) || target != "chancellor" || decisions != 1 || escalations != 1 || deliveries != 1 || invocationState != "superseded" || cancellations != 1 || cancelState != "pending" {
		t.Fatalf("stale exhaustion decision=%q resolution=%q target=%q decisions=%d events=%d deliveries=%d invocation=%q cancellations=%d cancel_state=%q", decision, resolution, target, decisions, escalations, deliveries, invocationState, cancellations, cancelState)
	}
}

func TestReconcilerFencesStaleProviderInvocationBeforeSchedulingRetry(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:13:00Z"
	attemptID := composeAttemptID("run_1", "decree_intake", 2)
	store := executionstore.New(db)

	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T12:08:00Z' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king_backup', 'king', 'idle', NULL, 0, '2026-03-01T12:08:00Z', '2026-03-01T12:08:00Z')`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'succeeded', finished_at = '2026-03-01T12:07:00Z' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_2', 'run_1', 'decree_intake', 2,2, 'running', 'king', 'agent_king', 1, '2026-03-01T12:08:00Z', NULL)`)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_nr_2_started_stale_settlement', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_2', '%s', 'v1', 'attempt.started', 1, '{}', 'actor_1', 'king', 'system', 'internal_error', 'seed', 'corr_nr2_stale_settlement', 'caus_nr2_stale_settlement', 'prev_nr2_stale_settlement', 'entry_nr2_stale_settlement', '2026-03-01T12:08:00Z')`, attemptID))

	if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_2",
		ExecutionMode:  executionstore.ExecutionModeProviderBacked,
		CompletionMode: executionstore.ProviderCompletionModeServerResponse,
		SessionRef:     "session_reconcile_stale_failure",
		CreatedAt:      "2026-03-01T12:08:00Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	mustExec(t, db, fmt.Sprintf(`UPDATE attempt_executions SET state = 'running', started_at = '2026-03-01T12:08:00Z', updated_at = '2026-03-01T12:08:00Z' WHERE attempt_id = '%s'`, attemptID))
	if _, err := store.CreateProviderInvocation(context.Background(), executionstore.CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_stale_failure",
		AttemptID:            attemptID,
		RunID:                "run_1",
		CompletionMode:       executionstore.ProviderCompletionModeServerResponse,
		NodeRunID:            "nr_2",
		LogicalSessionID:     "session_reconcile_stale_failure",
		IssuedAt:             "2026-03-01T12:08:01Z",
		CreatedAt:            "2026-03-01T12:08:01Z",
	}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := store.MarkProviderInvocationOpen(context.Background(), executionstore.MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "invocation_stale_failure",
		AttemptID:            attemptID,
		ProviderSessionID:    "ses_stale_failure",
		OpenedAt:             "2026-03-01T12:08:02Z",
	}); err != nil {
		t.Fatalf("mark provider invocation open: %v", err)
	}

	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), alwaysRetryPlanner{}, nil)
	result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now})
	if err != nil {
		t.Fatalf("reconcile stale provider-backed attempt: %v", err)
	}
	if result.StaleAttemptsFound != 1 {
		t.Fatalf("expected one stale provider-backed attempt, got %d", result.StaleAttemptsFound)
	}
	if result.AttemptsFailed != 0 || result.AttemptsReassigned != 1 {
		t.Fatalf("expected provider-backed attempt to schedule a fenced retry, got %#v", result)
	}
	if got := queryNodeStatus(t, db, "nr_2"); got != "blocked" {
		t.Fatalf("expected stale provider-backed node to become blocked, got %q", got)
	}

	attempt, found, err := store.GetAttempt(context.Background(), attemptID)
	if err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if !found {
		t.Fatal("expected durable attempt execution to exist")
	}
	if attempt.State != "committed_cancelled" || attempt.FailureCode != "agent_unresponsive" {
		t.Fatalf("expected stale provider-backed attempt to be durably cancelled, got %#v", attempt)
	}
	if strings.TrimSpace(attempt.CanonicalSettlementID) != "" {
		t.Fatalf("expected stale failure to remain without a canonical settlement, got %#v", attempt)
	}

	invocation, found, err := store.GetProviderInvocation(context.Background(), "invocation_stale_failure")
	if err != nil {
		t.Fatalf("get provider invocation: %v", err)
	}
	if !found {
		t.Fatal("expected provider invocation to exist")
	}
	if invocation.InvocationState != executionstore.ProviderInvocationStateSuperseded {
		t.Fatalf("expected stale attempt recovery to revoke provider authority, got %#v", invocation)
	}

	settlement, found, err := store.GetProviderCompletionSettlementByInvocation(context.Background(), "invocation_stale_failure")
	if err != nil {
		t.Fatalf("get provider completion settlement by invocation: %v", err)
	}
	if found {
		t.Fatalf("expected stale attempt failure not to create a provider settlement, got %#v", settlement)
	}
	var retryStatus string
	if err := db.QueryRow(`SELECT node_status FROM node_runs WHERE run_id = 'run_1' AND node_id = 'decree_intake' AND attempt = 3`).Scan(&retryStatus); err != nil {
		t.Fatalf("load fenced retry node: %v", err)
	}
	if retryStatus != "pending" {
		t.Fatalf("expected replacement to wait for cancellation handoff, got %q", retryStatus)
	}
	var cancelStatus string
	if err := db.QueryRow(`SELECT status FROM attempt_cancel_outbox WHERE attempt_id = ?`, attemptID).Scan(&cancelStatus); err != nil {
		t.Fatalf("load stale cancellation handoff: %v", err)
	}
	if cancelStatus != "pending" {
		t.Fatalf("expected stale cancellation handoff to remain pending, got %q", cancelStatus)
	}
}

func TestReconcilerPrecheckPreservesProviderInvocationWithTerminalHandoff(t *testing.T) {
	t.Parallel()

	fixture := newStaleProviderHandoffFenceFixture(t)
	ctx := context.Background()
	mustExecArgs(t, fixture.db, `INSERT INTO provider_terminal_handoffs (
		handoff_id, provider_invocation_id, attempt_id, run_id, node_run_id,
		expected_lease_owner, expected_lease_version, stream_observation_json, payload_json, created_at
	) VALUES ('terminal_handoff_precheck', ?, ?, 'run_1', 'nr_2', ?, ?, '{}', '{}', '2026-03-01T12:08:03Z')`, fixture.invocationID, fixture.attemptID, fixture.leaseOwner, fixture.leaseVersion)

	reconciler := NewReconciler(fixture.db, NewSessionManager(fixture.db, NewSessionLifecycleMetrics()), alwaysRetryPlanner{}, nil)
	stale := staleAttempt{NodeRunID: "nr_2", RunID: "run_1", NodeID: "decree_intake", Attempt: 2, Wave: 2, RequiredRole: "king", AgentID: "agent_king", CurrentStatus: "running", StartedAt: "2026-03-01T12:08:00Z"}
	skip, err := reconciler.shouldSkipStaleRecovery(ctx, stale, fixture.attemptID)
	if err != nil || !skip {
		t.Fatalf("precheck skipped=%t err=%v, want pending terminal handoff to fence stale recovery", skip, err)
	}

	result, err := reconciler.Reconcile(ctx, ReconcileRequest{Now: "2026-03-01T12:13:30Z"})
	if err != nil {
		t.Fatalf("reconcile stale attempt with terminal handoff: %v", err)
	}
	if result.StaleAttemptsFound != 1 || result.AttemptsFailed != 0 || result.AttemptsReassigned != 0 {
		t.Fatalf("terminal handoff stale recovery result=%+v, want one observed candidate and no recovery", result)
	}
	if got := queryNodeStatus(t, fixture.db, "nr_2"); got != "running" {
		t.Fatalf("terminal handoff node status=%q, want running", got)
	}
	attempt, found, err := fixture.store.GetAttempt(ctx, fixture.attemptID)
	if err != nil || !found || attempt.State != "running" {
		t.Fatalf("active attempt was changed by precheck: found=%t attempt=%#v err=%v", found, attempt, err)
	}
	invocation, found, err := fixture.store.GetProviderInvocation(ctx, fixture.invocationID)
	if err != nil || !found || invocation.InvocationState != executionstore.ProviderInvocationStateOpen {
		t.Fatalf("active invocation was superseded despite its terminal handoff: found=%t invocation=%#v err=%v", found, invocation, err)
	}
	if count := countRowsForReconcilerTest(t, fixture.db, `SELECT COUNT(1) FROM node_runs WHERE run_id = 'run_1' AND node_id = 'decree_intake' AND attempt > 2`); count != 0 {
		t.Fatalf("terminal handoff precheck created %d retry attempts", count)
	}
}

func TestReconcilerPrecheckPreservesProviderInvocationWithFileOnlyRecoveryMarker(t *testing.T) {
	t.Parallel()

	fixture := newStaleProviderHandoffFenceFixture(t)

	reconciler := NewReconciler(fixture.db, NewSessionManager(fixture.db, NewSessionLifecycleMetrics()), alwaysRetryPlanner{}, nil)
	stale := staleAttempt{
		NodeRunID: "nr_2", RunID: "run_1", NodeID: "decree_intake", Attempt: 2, Wave: 2,
		RequiredRole: "king", AgentID: "agent_king", CurrentStatus: "running", StartedAt: "2026-03-01T12:08:00Z",
	}
	mustExecArgs(t, fixture.db, `UPDATE provider_invocations SET terminal_handoff_pending_at = ? WHERE provider_invocation_id = ?`, "2026-03-01T12:08:20Z", fixture.invocationID)
	skip, err := reconciler.shouldSkipStaleRecovery(context.Background(), stale, fixture.attemptID)
	if err != nil || !skip {
		t.Fatalf("precheck skipped=%t err=%v, want a file-only recovery marker to fence stale recovery", skip, err)
	}

	result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: "2026-03-01T12:13:30Z"})
	if err != nil {
		t.Fatalf("reconcile stale attempt with file-only recovery marker: %v", err)
	}
	if result.AttemptsFailed != 0 || result.AttemptsReassigned != 0 {
		t.Fatalf("file-only recovery marker allowed stale recovery: %+v", result)
	}
	assertStaleProviderInvocationRemainsActive(t, fixture)
}

func TestReconcilerTransactionFencePreservesInvocationWhenRecoveryMarkerAppearsAfterPrecheck(t *testing.T) {
	t.Parallel()

	fixture := newStaleProviderHandoffFenceFixture(t)
	mustExec(t, fixture.db, `CREATE TRIGGER inject_provider_terminal_recovery_marker_before_stale_fence
		AFTER UPDATE OF status ON agents
		WHEN NEW.agent_id = 'agent_king' AND NEW.status = 'blocked'
		BEGIN
			UPDATE provider_invocations
			SET terminal_handoff_pending_at = '2026-03-01T12:08:03Z'
			WHERE provider_invocation_id = 'invocation_stale_handoff_fence';
		END`)

	reconciler := NewReconciler(fixture.db, NewSessionManager(fixture.db, NewSessionLifecycleMetrics()), alwaysRetryPlanner{}, nil)
	result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: "2026-03-01T12:13:30Z"})
	if err != nil {
		t.Fatalf("reconcile while file-only recovery marker appears: %v", err)
	}
	if result.AttemptsFailed != 0 || result.AttemptsReassigned != 0 {
		t.Fatalf("transaction fence allowed stale recovery after marker appeared: %+v", result)
	}

	if got := queryNodeStatus(t, fixture.db, "nr_2"); got != "running" {
		t.Fatalf("stale transaction changed node status to %q after handoff appeared", got)
	}
	if got := queryReconcilerAgentStatus(t, fixture.db, "agent_king"); got != "busy" {
		t.Fatalf("stale transaction changed agent status to %q after handoff appeared", got)
	}
	attempt, found, err := fixture.store.GetAttempt(context.Background(), fixture.attemptID)
	if err != nil || !found || attempt.State != "running" {
		t.Fatalf("stale transaction changed active attempt: found=%t attempt=%#v err=%v", found, attempt, err)
	}
	invocation, found, err := fixture.store.GetProviderInvocation(context.Background(), fixture.invocationID)
	if err != nil || !found || invocation.InvocationState != executionstore.ProviderInvocationStateOpen {
		t.Fatalf("stale transaction superseded active invocation: found=%t invocation=%#v err=%v", found, invocation, err)
	}
	if count := countRowsForReconcilerTest(t, fixture.db, `SELECT COUNT(1) FROM provider_invocations WHERE provider_invocation_id = ? AND terminal_handoff_pending_at IS NOT NULL`, fixture.invocationID); count != 0 {
		t.Fatalf("rolled-back stale transaction retained %d injected recovery markers", count)
	}
	if count := countRowsForReconcilerTest(t, fixture.db, `SELECT COUNT(1) FROM attempt_cancel_outbox WHERE attempt_id = ?`, fixture.attemptID); count != 0 {
		t.Fatalf("stale transaction queued %d cancellation handoffs after losing authority", count)
	}
	if count := countRowsForReconcilerTest(t, fixture.db, `SELECT COUNT(1) FROM node_runs WHERE run_id = 'run_1' AND node_id = 'decree_intake' AND attempt > 2`); count != 0 {
		t.Fatalf("stale transaction created %d retry attempts after losing authority", count)
	}
}

func TestReconcilerFailsStaleAttemptWithMeaningfulDurableEvidenceWithoutSuccessor(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	ctx := context.Background()
	const attemptID = "run_1:decree_intake:1"
	const invocationID = "invocation_stale_meaningful_progress"
	const now = "2026-03-01T12:13:00Z"
	mustExec(t, db, `UPDATE agents SET status = 'busy' WHERE agent_id = 'agent_king'`)

	store := executionstore.New(db)
	if err := store.EnqueueAttempt(ctx, executionstore.EnqueueAttemptRequest{AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeServerResponse, SessionRef: "session_stale_meaningful_progress", CreatedAt: "2026-03-01T12:00:00Z"}); err != nil {
		t.Fatalf("enqueue stale attempt: %v", err)
	}
	mustExec(t, db, `UPDATE attempt_executions SET state = 'running', started_at = '2026-03-01T12:00:00Z', base_snapshot_id = 'snap_stale_base', result_snapshot_id = 'snap_stale_result' WHERE attempt_id = '`+attemptID+`'`)
	if _, err := store.CreateProviderInvocation(ctx, executionstore.CreateProviderInvocationRequest{ProviderInvocationID: invocationID, AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", LogicalSessionID: "session_stale_meaningful_progress", CompletionMode: executionstore.ProviderCompletionModeServerResponse, IssuedAt: "2026-03-01T12:00:01Z", CreatedAt: "2026-03-01T12:00:01Z"}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := store.MarkProviderInvocationOpen(ctx, executionstore.MarkProviderInvocationOpenRequest{ProviderInvocationID: invocationID, AttemptID: attemptID, ProviderSessionID: "ses_stale_meaningful_progress", OpenedAt: "2026-03-01T12:00:02Z"}); err != nil {
		t.Fatalf("open provider invocation: %v", err)
	}
	mustExec(t, db, `INSERT INTO session_leases (session_id, agent_id, status, lease_expires_at, last_heartbeat_at, created_at, updated_at) VALUES ('session_stale_meaningful_progress', 'agent_king', 'active', '2026-03-01T12:10:00Z', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_stale_meaningful_text', 'run_1', 2, 'nr_1', '`+attemptID+`', 'v1', 'attempt.progress', 1, '{"text":"assistant produced a real response"}', 'runtime', 'king', 'runtime', 'stream_event', 'streamed assistant text', 'corr_stale_meaningful', 'caus_stale_meaningful_text', 'prev', 'entry', '2026-03-01T12:01:00Z')`)
	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_stale_meaningful_reasoning', 'run_1', 3, 'nr_1', '`+attemptID+`', 'v1', 'attempt.activity', 2, '{"kind":"kingdom.reasoning","summary":"reasoning"}', 'runtime', 'king', 'runtime', 'provider_activity', 'provider reasoning', 'corr_stale_meaningful', 'caus_stale_meaningful_reasoning', 'prev', 'entry', '2026-03-01T12:01:01Z')`)
	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_stale_meaningful_tool', 'run_1', 4, 'nr_1', '`+attemptID+`', 'v1', 'attempt.activity', 3, '{"kind":"kingdom.tool","tool_name":"shell"}', 'runtime', 'king', 'runtime', 'provider_activity', 'provider tool', 'corr_stale_meaningful', 'caus_stale_meaningful_tool', 'prev', 'entry', '2026-03-01T12:01:02Z')`)
	mustExec(t, db, `INSERT INTO token_usage (usage_id, run_id, node_run_id, attempt_id, provider_invocation_id, required_role, model, variant, input_tokens, output_tokens, total_tokens, reasoning_tokens, cache_read_tokens, cache_write_tokens, created_at) VALUES ('usage_stale_meaningful', 'run_1', 'nr_1', '`+attemptID+`', '`+invocationID+`', 'king', 'model', '', 5, 3, 8, 2, 0, 0, '2026-03-01T12:01:03Z')`)
	mustExec(t, db, `INSERT INTO filesystem_snapshots (snapshot_id, run_id, parent_snapshot_id, snapshot_kind, created_at) VALUES ('snap_stale_base', 'run_1', NULL, 'baseline', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO filesystem_snapshots (snapshot_id, run_id, parent_snapshot_id, snapshot_kind, created_at) VALUES ('snap_stale_result', 'run_1', 'snap_stale_base', 'attempt_result', '2026-03-01T12:01:04Z')`)
	mustExec(t, db, `INSERT INTO filesystem_snapshot_entries (snapshot_id, path, entry_type, blob_id, size_bytes, mode_bits, mtime_ns) VALUES ('snap_stale_result', 'changed.txt', 'file', 'blob', 1, 420, 0)`)

	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), loadRetryEngine(t), nil)
	meaningful, err := reconciler.hasMeaningfulStaleAttemptProgress(ctx, attemptID)
	if err != nil || !meaningful {
		t.Fatalf("meaningful durable stale evidence=%t err=%v", meaningful, err)
	}
	result, err := reconciler.recoverAttempt(ctx, staleAttempt{NodeRunID: "nr_1", RunID: "run_1", NodeID: "decree_intake", Attempt: 1, Wave: 1, RequiredRole: "king", AgentID: "agent_king", CurrentStatus: "running", StartedAt: "2026-03-01T12:00:00Z"}, mustParseRFC3339(t, now))
	if err != nil || !result.failed || result.reassigned {
		t.Fatalf("recover meaningful stale attempt result=%+v err=%v", result, err)
	}
	if status := queryNodeStatus(t, db, "nr_1"); status != "failed" {
		t.Fatalf("meaningful stale attempt status=%q, want failed", status)
	}
	var successors int
	if err := db.QueryRow(`SELECT COUNT(1) FROM node_runs WHERE run_id = 'run_1' AND node_id = 'decree_intake' AND attempt > 1`).Scan(&successors); err != nil {
		t.Fatal(err)
	}
	if successors != 0 {
		t.Fatalf("meaningful stale attempt created %d retry successors", successors)
	}
	var decision, dispatch, resolution string
	var recordedMeaningful int
	if err := db.QueryRow(`SELECT decision, dispatch_state, resolution, meaningful_progress FROM retry_decisions WHERE source_attempt_id = ?`, attemptID).Scan(&decision, &dispatch, &resolution, &recordedMeaningful); err != nil {
		t.Fatal(err)
	}
	if decision != string(RetryDecisionFailFast) || dispatch != string(RetryDispatchStateNotApplicable) || resolution != string(RetryDecisionResolutionNotApplicable) || recordedMeaningful != 1 {
		t.Fatalf("meaningful stale retry decision=%q dispatch=%q resolution=%q progress=%d", decision, dispatch, resolution, recordedMeaningful)
	}
	invocation, found, err := store.GetProviderInvocation(ctx, invocationID)
	if err != nil || !found || invocation.InvocationState != executionstore.ProviderInvocationStateSuperseded {
		t.Fatalf("stale provider invocation=%#v found=%t err=%v", invocation, found, err)
	}
	var cancelStatus string
	if err := db.QueryRow(`SELECT status FROM attempt_cancel_outbox WHERE attempt_id = ?`, attemptID).Scan(&cancelStatus); err != nil {
		t.Fatal(err)
	}
	if cancelStatus != "pending" {
		t.Fatalf("stale meaningful attempt cancellation status=%q, want pending", cancelStatus)
	}
}

func TestReconcilerRecognizesEachMeaningfulStaleEvidenceSource(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		seed func(*testing.T, *sql.DB)
	}{
		{
			name: "assistant text",
			seed: func(t *testing.T, db *sql.DB) {
				mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_stale_text_source', 'run_1', 2, 'nr_1', 'stale_evidence_attempt', 'v1', 'attempt.progress', 1, '{"text":"assistant response"}', 'runtime', 'king', 'runtime', 'stream_event', 'streamed assistant text', 'corr', 'caus_text', 'prev', 'entry', '2026-03-01T12:01:00Z')`)
			},
		},
		{
			name: "reasoning activity",
			seed: func(t *testing.T, db *sql.DB) {
				mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_stale_reasoning_source', 'run_1', 2, 'nr_1', 'stale_evidence_attempt', 'v1', 'attempt.activity', 1, '{"kind":"kingdom.reasoning","summary":"thinking"}', 'runtime', 'king', 'runtime', 'provider_activity', 'provider reasoning', 'corr', 'caus_reasoning', 'prev', 'entry', '2026-03-01T12:01:00Z')`)
			},
		},
		{
			name: "tool activity",
			seed: func(t *testing.T, db *sql.DB) {
				mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_stale_tool_source', 'run_1', 2, 'nr_1', 'stale_evidence_attempt', 'v1', 'attempt.activity', 1, '{"kind":"kingdom.tool","tool_name":"shell"}', 'runtime', 'king', 'runtime', 'provider_activity', 'provider tool', 'corr', 'caus_tool', 'prev', 'entry', '2026-03-01T12:01:00Z')`)
			},
		},
		{
			name: "token usage",
			seed: func(t *testing.T, db *sql.DB) {
				mustExec(t, db, `INSERT INTO token_usage (usage_id, run_id, node_run_id, attempt_id, required_role, model, variant, input_tokens, output_tokens, total_tokens, reasoning_tokens, cache_read_tokens, cache_write_tokens, created_at) VALUES ('usage_stale_evidence', 'run_1', 'nr_1', 'stale_evidence_attempt', 'king', 'model', '', 1, 0, 1, 0, 0, 0, '2026-03-01T12:01:00Z')`)
			},
		},
		{
			name: "workspace delta",
			seed: func(t *testing.T, db *sql.DB) {
				mustExec(t, db, `UPDATE attempt_executions SET base_snapshot_id = 'snap_evidence_base', result_snapshot_id = 'snap_evidence_result' WHERE attempt_id = 'stale_evidence_attempt'`)
				mustExec(t, db, `INSERT INTO filesystem_snapshots (snapshot_id, run_id, parent_snapshot_id, snapshot_kind, created_at) VALUES ('snap_evidence_base', 'run_1', NULL, 'baseline', '2026-03-01T12:00:00Z')`)
				mustExec(t, db, `INSERT INTO filesystem_snapshots (snapshot_id, run_id, parent_snapshot_id, snapshot_kind, created_at) VALUES ('snap_evidence_result', 'run_1', 'snap_evidence_base', 'attempt_result', '2026-03-01T12:01:00Z')`)
				mustExec(t, db, `INSERT INTO filesystem_snapshot_entries (snapshot_id, path, entry_type, blob_id, size_bytes, mode_bits, mtime_ns) VALUES ('snap_evidence_result', 'changed.txt', 'file', 'blob', 1, 420, 0)`)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := openTestDB(t)
			applyMigration(t, db, "0001_initial_schema.up.sql")
			seedMinimumGraph(t, db)
			mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, created_at, updated_at) VALUES ('stale_evidence_attempt', 'run_1', 'nr_1', 'running', 'provider_backed', 'server_response', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
			tc.seed(t, db)

			meaningful, err := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), loadRetryEngine(t), nil).hasMeaningfulStaleAttemptProgress(context.Background(), "stale_evidence_attempt")
			if err != nil || !meaningful {
				t.Fatalf("meaningful stale evidence=%t err=%v", meaningful, err)
			}
		})
	}
}

func TestReconcilerSchedulesSameAgentExecutionRetryOnlyAfterHandoff(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	const now = "2026-03-01T12:13:00Z"
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Builder', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder', 'builder-berserker', 'busy', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_execution_retry', 'execution retry', 'execution retry', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wf_execution_retry', 'execution-retry', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_execution_retry', 'wf_execution_retry', 'delegated_execution_berserker', 'builder-berserker', 'delegated_execution', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_execution_retry', NULL, 'decree_execution_retry', 'wf_execution_retry', 'running', 'approved', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_execution_retry_1', 'run_execution_retry', 'delegated_execution_berserker', 1, 1, 'running', 'builder-berserker', 'agent_builder', 1, '2026-03-01T12:00:00Z', NULL)`)

	const attemptID = "run_execution_retry:delegated_execution_berserker:1"
	store := executionstore.New(db)
	if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{AttemptID: attemptID, RunID: "run_execution_retry", NodeRunID: "nr_execution_retry_1", ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeServerResponse, SessionRef: "session_execution_retry", CreatedAt: "2026-03-01T12:00:00Z"}); err != nil {
		t.Fatalf("enqueue stale execution attempt: %v", err)
	}
	if _, err := store.CreateProviderInvocation(context.Background(), executionstore.CreateProviderInvocationRequest{ProviderInvocationID: "inv_execution_retry", AttemptID: attemptID, RunID: "run_execution_retry", NodeRunID: "nr_execution_retry_1", LogicalSessionID: "session_execution_retry", CompletionMode: executionstore.ProviderCompletionModeServerResponse, IssuedAt: "2026-03-01T12:00:01Z", CreatedAt: "2026-03-01T12:00:01Z"}); err != nil {
		t.Fatalf("create stale execution invocation: %v", err)
	}
	if _, err := store.MarkProviderInvocationOpen(context.Background(), executionstore.MarkProviderInvocationOpenRequest{ProviderInvocationID: "inv_execution_retry", AttemptID: attemptID, ProviderSessionID: "ses_execution_retry", OpenedAt: "2026-03-01T12:00:02Z"}); err != nil {
		t.Fatalf("open stale execution invocation: %v", err)
	}
	mustExec(t, db, `UPDATE attempt_executions SET state = 'running', started_at = '2026-03-01T12:00:00Z', updated_at = '2026-03-01T12:00:00Z' WHERE attempt_id = '`+attemptID+`'`)

	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), loadRetryEngine(t), nil)
	result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now})
	if err != nil {
		t.Fatalf("reconcile same-agent stale execution: %v", err)
	}
	if result.AttemptsReassigned != 1 || result.AttemptsFailed != 0 {
		t.Fatalf("same-agent stale recovery result=%+v", result)
	}

	const retryNodeRunID = "nr_execution_retry_1_retry_2"
	if got := queryNodeStatus(t, db, retryNodeRunID); got != "pending" {
		t.Fatalf("same-agent retry launched before cancellation handoff: %q", got)
	}
	if got := queryNodeAssignedAgent(t, db, retryNodeRunID); got != "agent_builder" {
		t.Fatalf("same-agent retry assigned %q", got)
	}
	var decision, dispatchState, resolution, dueAt string
	if err := db.QueryRow(`SELECT decision, dispatch_state, resolution, due_at FROM retry_decisions WHERE source_attempt_id = ?`, attemptID).Scan(&decision, &dispatchState, &resolution, &dueAt); err != nil {
		t.Fatalf("load same-agent retry decision: %v", err)
	}
	if decision != string(RetryDecisionRetry) || dispatchState != string(RetryDispatchStatePending) || resolution != string(RetryDecisionResolutionPending) {
		t.Fatalf("same-agent retry decision=%q dispatch=%q resolution=%q", decision, dispatchState, resolution)
	}
	if released, err := NewTransitionService(db, nil).ReleaseDueDecreeRetries(context.Background(), "run_execution_retry", dueAt); err != nil || len(released) != 0 {
		t.Fatalf("same-agent retry bypassed cancellation handoff: released=%#v err=%v", released, err)
	}
	mustExec(t, db, `UPDATE attempt_cancel_outbox SET status = 'published', handoff_state = 'released', published_at = '`+now+`' WHERE attempt_id = '`+attemptID+`'`)
	if released, err := NewTransitionService(db, nil).ReleaseDueDecreeRetries(context.Background(), "run_execution_retry", dueAt); err != nil || len(released) != 1 || released[0].NodeRunID != retryNodeRunID {
		t.Fatalf("same-agent retry release after handoff: released=%#v err=%v", released, err)
	}
}

func TestReconcilerLeavesProviderReplacementPendingUntilCancellationHandoff(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	mustExec(t, db, `UPDATE agents SET status = 'busy' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `UPDATE node_runs SET started_at = '2026-03-01T11:58:00Z' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king_backup', 'king', 'idle', NULL, 0, '2026-03-01T11:58:00Z', '2026-03-01T11:58:00Z')`)
	store := executionstore.New(db)
	if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:      "att_1",
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  executionstore.ExecutionModeProviderBacked,
		CompletionMode: executionstore.ProviderCompletionModeServerResponse,
		CreatedAt:      "2026-03-01T11:58:00Z",
	}); err != nil {
		t.Fatalf("enqueue stale provider attempt: %v", err)
	}
	mustExec(t, db, `UPDATE attempt_executions SET state = 'running', started_at = '2026-03-01T11:58:00Z' WHERE attempt_id = 'att_1'`)
	if _, err := store.CreateProviderInvocation(context.Background(), executionstore.CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_stale_reassign_guard",
		AttemptID:            "att_1",
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		CompletionMode:       executionstore.ProviderCompletionModeServerResponse,
		CreatedAt:            "2026-03-01T11:58:01Z",
	}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := store.MarkProviderInvocationOpen(context.Background(), executionstore.MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "invocation_stale_reassign_guard",
		AttemptID:            "att_1",
		ProviderSessionID:    "ses_stale_reassign_guard",
		OpenedAt:             "2026-03-01T11:58:02Z",
	}); err != nil {
		t.Fatalf("open provider invocation: %v", err)
	}

	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), alwaysRetryPlanner{}, nil)
	result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: "2026-03-01T12:05:30Z"})
	if err != nil {
		t.Fatalf("reconcile stale provider attempt with replacement: %v", err)
	}
	if result.StaleAttemptsFound != 1 || result.AttemptsReassigned != 1 || result.AttemptsFailed != 0 {
		t.Fatalf("expected stale provider attempt to schedule a fenced retry, got %#v", result)
	}
	if got := queryNodeStatus(t, db, "nr_1"); got != "blocked" {
		t.Fatalf("expected stale provider attempt to remain visibly blocked, got %q", got)
	}
	var retries int
	if err := db.QueryRow(`SELECT COUNT(1) FROM node_runs WHERE run_id = 'run_1' AND node_id = 'decree_intake' AND attempt > 1`).Scan(&retries); err != nil {
		t.Fatalf("count provider retry attempts: %v", err)
	}
	if retries != 1 {
		t.Fatalf("expected one pending fenced provider retry, got %d", retries)
	}
	if got := queryNodeStatus(t, db, "nr_1_retry_2"); got != "pending" {
		t.Fatalf("expected provider retry to wait for cancellation handoff, got %q", got)
	}
}

func TestReconcilerKeepsExistingProviderSettlementImmutable(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:13:00Z"
	attemptID := composeAttemptID("run_1", "execution_review_shieldmaiden", 1)
	store := executionstore.New(db)

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '2026-03-01T12:08:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer_existing_settlement', 'reviewer-shieldmaiden', 'busy', NULL, 0, '2026-03-01T12:08:00Z', '2026-03-01T12:08:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review_existing_settlement', 'wfdef_1', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '2026-03-01T12:08:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_existing_settlement', 'run_1', 'execution_review_shieldmaiden', 1, 1, 'running', 'reviewer-shieldmaiden', 'agent_reviewer_existing_settlement', 1, '2026-03-01T12:08:00Z', NULL)`)
	mustExec(t, db, `INSERT INTO run_participants (run_id, agent_id, role_id, display_name, parent_agent_id, spawn_depth, first_seen_at) VALUES ('run_1', 'agent_reviewer_existing_settlement', 'reviewer-shieldmaiden', 'Reviewer', NULL, 0, '2026-03-01T12:08:00Z')`)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_review_existing_settlement_started', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_review_existing_settlement', '%s', 'v1', 'attempt.started', 1, '{}', 'actor_1', 'reviewer-shieldmaiden', 'system', 'internal_error', 'seed', 'corr_review_existing_settlement', 'caus_review_existing_settlement', 'prev_review_existing_settlement', 'entry_review_existing_settlement', '2026-03-01T12:08:00Z')`, attemptID))

	if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_review_existing_settlement",
		ExecutionMode:  executionstore.ExecutionModeProviderBacked,
		CompletionMode: executionstore.ProviderCompletionModeServerResponse,
		SessionRef:     "session_review_existing_settlement",
		CreatedAt:      "2026-03-01T12:08:00Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	mustExec(t, db, fmt.Sprintf(`UPDATE attempt_executions SET state = 'running', started_at = '2026-03-01T12:08:00Z', updated_at = '2026-03-01T12:08:00Z' WHERE attempt_id = '%s'`, attemptID))
	if _, err := store.CreateProviderInvocation(context.Background(), executionstore.CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_review_existing_settlement",
		AttemptID:            attemptID,
		RunID:                "run_1",
		CompletionMode:       executionstore.ProviderCompletionModeServerResponse,
		NodeRunID:            "nr_review_existing_settlement",
		LogicalSessionID:     "session_review_existing_settlement",
		IssuedAt:             "2026-03-01T12:08:01Z",
		CreatedAt:            "2026-03-01T12:08:01Z",
	}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := store.MarkProviderInvocationOpen(context.Background(), executionstore.MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "invocation_review_existing_settlement",
		AttemptID:            attemptID,
		ProviderSessionID:    "ses_review_existing_settlement",
		OpenedAt:             "2026-03-01T12:08:02Z",
	}); err != nil {
		t.Fatalf("mark provider invocation open: %v", err)
	}
	const reviewResponse = "VERDICT: REWORK\n\nReviewer requested changes."
	accepted, err := store.AppendProviderCompletionEvidence(context.Background(), executionstore.AppendProviderCompletionEvidenceRequest{
		EvidenceID:           "evidence_review_existing_settlement",
		ProviderInvocationID: "invocation_review_existing_settlement",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_review_existing_settlement",
		EvidenceKind:         executionstore.ProviderCompletionEvidenceKindStreamObservation,
		PayloadJSON:          `{"observation":{"canonical_terminal_text":"VERDICT: REWORK\n\nReviewer requested changes."}}`,
		CreatedAt:            "2026-03-01T12:08:03Z",
	})
	if err != nil {
		t.Fatalf("append server-response evidence: %v", err)
	}
	seedHistoricalProviderSettlementWithoutTerminal(t, db, executionstore.FinalizeProviderCompletionSettlementRequest{
		SettlementID:          executionstore.ProviderCompletionSettlementID("invocation_review_existing_settlement"),
		ProviderInvocationID:  "invocation_review_existing_settlement",
		AttemptID:             attemptID,
		RunID:                 "run_1",
		NodeRunID:             "nr_review_existing_settlement",
		Outcome:               executionstore.ProviderCompletionOutcomeSuccess,
		PrimaryEvidenceID:     accepted.EvidenceID,
		FinalText:             reviewResponse,
		StructuredPayloadJSON: `{}`,
		SettledAt:             "2026-03-01T12:08:04Z",
	})

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)
	result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now})
	if err != nil {
		t.Fatalf("reconcile stored review success settlement: %v", err)
	}
	if result.AttemptsFailed != 0 {
		t.Fatalf("expected no stale failure recovery while converging stored settlement, got %d", result.AttemptsFailed)
	}
	if got := queryNodeStatus(t, db, "nr_review_existing_settlement"); got != "succeeded" {
		t.Fatalf("expected immutable success settlement to drive node success, got %q", got)
	}

	attempt, found, err := store.GetAttempt(context.Background(), attemptID)
	if err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if !found {
		t.Fatal("expected durable attempt execution to exist")
	}
	if attempt.State != "committed_success" || attempt.FailureCode != "" {
		t.Fatalf("expected existing settlement to drive committed_success, got %#v", attempt)
	}
	settlement, found, err := store.GetProviderCompletionSettlementByInvocation(context.Background(), "invocation_review_existing_settlement")
	if err != nil {
		t.Fatalf("get provider completion settlement: %v", err)
	}
	if !found {
		t.Fatal("expected provider completion settlement to exist")
	}
	if settlement.Outcome != executionstore.ProviderCompletionOutcomeSuccess || settlement.FailureReasonCode != "" {
		t.Fatalf("expected existing success settlement to remain unchanged, got %#v", settlement)
	}
	if settlement.FinalText != reviewResponse || settlement.SettledAt != "2026-03-01T12:08:04Z" {
		t.Fatalf("expected canonical settlement payload and timestamp to remain immutable, got %#v", settlement)
	}
}

func TestReconcilerImmediatelyConvergesRecentSettledProviderAttemptFailure(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:13:00Z"
	attemptID := composeAttemptID("run_1", "decree_intake", 2)
	store := executionstore.New(db)

	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T12:12:15Z' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'succeeded', finished_at = '2026-03-01T12:07:00Z' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_2_recent_failure', 'run_1', 'decree_intake', 2,2, 'running', 'king', 'agent_king', 1, '2026-03-01T12:12:20Z', NULL)`)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_nr_2_started_recent_failure', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_2_recent_failure', '%s', 'v1', 'attempt.started', 1, '{}', 'actor_1', 'king', 'system', 'internal_error', 'seed', 'corr_nr2_recent_failure', 'caus_nr2_recent_failure', 'prev_nr2_recent_failure', 'entry_nr2_recent_failure', '2026-03-01T12:12:20Z')`, attemptID))

	if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_2_recent_failure",
		ExecutionMode:  executionstore.ExecutionModeProviderBacked,
		CompletionMode: executionstore.ProviderCompletionModeServerResponse,
		SessionRef:     "session_recent_settled_failure",
		CreatedAt:      "2026-03-01T12:12:20Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	mustExec(t, db, fmt.Sprintf(`UPDATE attempt_executions SET state = 'running', started_at = '2026-03-01T12:12:20Z', updated_at = '2026-03-01T12:12:20Z' WHERE attempt_id = '%s'`, attemptID))
	if _, err := store.CreateProviderInvocation(context.Background(), executionstore.CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_recent_settled_failure",
		AttemptID:            attemptID,
		RunID:                "run_1",
		CompletionMode:       executionstore.ProviderCompletionModeServerResponse,
		NodeRunID:            "nr_2_recent_failure",
		LogicalSessionID:     "session_recent_settled_failure",
		IssuedAt:             "2026-03-01T12:12:21Z",
		CreatedAt:            "2026-03-01T12:12:21Z",
	}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := store.MarkProviderInvocationOpen(context.Background(), executionstore.MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "invocation_recent_settled_failure",
		AttemptID:            attemptID,
		ProviderSessionID:    "ses_recent_settled_failure",
		OpenedAt:             "2026-03-01T12:12:22Z",
	}); err != nil {
		t.Fatalf("mark provider invocation open: %v", err)
	}
	evidence, err := store.EnsureTerminalProviderCompletionEvidence(context.Background(), executionstore.EnsureTerminalProviderCompletionEvidenceRequest{
		ProviderInvocationID: "invocation_recent_settled_failure",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_2_recent_failure",
		State:                "failed",
		ReasonCode:           "provider_incomplete_turn",
		FailureText:          "provider exited before completion could be settled",
		OutputText:           "Partial provider output",
		EvidenceSource:       "stream",
		CreatedAt:            "2026-03-01T12:12:23Z",
	})
	if err != nil {
		t.Fatalf("ensure terminal provider evidence: %v", err)
	}
	seedHistoricalProviderSettlementWithoutTerminal(t, db, executionstore.FinalizeProviderCompletionSettlementRequest{
		SettlementID:          executionstore.ProviderCompletionSettlementID("invocation_recent_settled_failure"),
		ProviderInvocationID:  "invocation_recent_settled_failure",
		AttemptID:             attemptID,
		RunID:                 "run_1",
		NodeRunID:             "nr_2_recent_failure",
		Outcome:               executionstore.ProviderCompletionOutcomeFailed,
		PrimaryEvidenceID:     evidence.EvidenceID,
		FailureReasonCode:     "provider_incomplete_turn",
		FailureReasonText:     "provider exited before completion could be settled",
		StructuredPayloadJSON: `{}`,
		SettledAt:             "2026-03-01T12:12:24Z",
	})

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)
	result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now})
	if err != nil {
		t.Fatalf("reconcile recent settled provider-backed failure: %v", err)
	}
	if result.StaleAttemptsFound != 0 {
		t.Fatalf("expected no stale attempts for recent settled failure recovery, got %d", result.StaleAttemptsFound)
	}
	if result.AttemptsFailed != 0 {
		t.Fatalf("expected no stale failure recovery for recent settled failure recovery, got %d", result.AttemptsFailed)
	}
	if got := queryNodeStatus(t, db, "nr_2_recent_failure"); got != "failed" {
		t.Fatalf("expected recent settled failure node to fail, got %q", got)
	}

	attempt, found, err := store.GetAttempt(context.Background(), attemptID)
	if err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if !found {
		t.Fatal("expected durable attempt execution to exist")
	}
	if attempt.State != "committed_failure" {
		t.Fatalf("expected committed_failure durable state, got %#v", attempt)
	}
	if attempt.FailureCode != "provider_incomplete_turn" {
		t.Fatalf("expected provider_incomplete_turn durable failure, got %#v", attempt)
	}
	if strings.TrimSpace(attempt.CanonicalSettlementID) == "" {
		t.Fatalf("expected canonical settlement id to be linked, got %#v", attempt)
	}
	settlement, found, err := store.GetProviderCompletionSettlementByInvocation(context.Background(), "invocation_recent_settled_failure")
	if err != nil {
		t.Fatalf("get provider completion settlement by invocation: %v", err)
	}
	if !found {
		t.Fatal("expected provider completion settlement to exist")
	}
	if settlement.Outcome != executionstore.ProviderCompletionOutcomeFailed {
		t.Fatalf("expected failed provider settlement outcome, got %#v", settlement)
	}
	if settlement.PrimaryEvidenceID != evidence.EvidenceID {
		t.Fatalf("expected failed provider settlement to retain terminal evidence %q, got %#v", evidence.EvidenceID, settlement)
	}
}

func TestReconcilerRecoveryHonorsPersistedProviderRetryHint(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	ctx := context.Background()
	store := executionstore.New(db)
	attemptID := composeAttemptID("run_1", "decree_intake", 1)
	if err := store.EnqueueAttempt(ctx, executionstore.EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  executionstore.ExecutionModeProviderBacked,
		CompletionMode: executionstore.ProviderCompletionModeServerResponse,
		SessionRef:     "session_provider_hint_recovery",
		CreatedAt:      "2026-03-01T12:00:00Z",
	}); err != nil {
		t.Fatalf("enqueue provider-backed attempt: %v", err)
	}
	if _, err := store.CreateProviderInvocation(ctx, executionstore.CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_provider_hint_recovery",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		LogicalSessionID:     "session_provider_hint_recovery",
		CompletionMode:       executionstore.ProviderCompletionModeServerResponse,
		IssuedAt:             "2026-03-01T12:00:01Z",
		CreatedAt:            "2026-03-01T12:00:01Z",
	}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := store.MarkProviderInvocationOpen(ctx, executionstore.MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "invocation_provider_hint_recovery",
		AttemptID:            attemptID,
		ProviderSessionID:    "ses_provider_hint_recovery",
		OpenedAt:             "2026-03-01T12:00:02Z",
	}); err != nil {
		t.Fatalf("mark provider invocation open: %v", err)
	}
	evidence, err := store.EnsureTerminalProviderCompletionEvidence(ctx, executionstore.EnsureTerminalProviderCompletionEvidenceRequest{
		ProviderInvocationID: "invocation_provider_hint_recovery",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_1",
		State:                "failed",
		ReasonCode:           "timeout",
		FailureText:          "provider asked Kingdom to retry later",
		EvidenceSource:       "stream",
		CreatedAt:            "2026-03-01T12:00:03Z",
	})
	if err != nil {
		t.Fatalf("ensure terminal provider evidence: %v", err)
	}
	seedHistoricalProviderSettlementWithoutTerminal(t, db, executionstore.FinalizeProviderCompletionSettlementRequest{
		SettlementID:            executionstore.ProviderCompletionSettlementID("invocation_provider_hint_recovery"),
		ProviderInvocationID:    "invocation_provider_hint_recovery",
		AttemptID:               attemptID,
		RunID:                   "run_1",
		NodeRunID:               "nr_1",
		Outcome:                 executionstore.ProviderCompletionOutcomeFailed,
		PrimaryEvidenceID:       evidence.EvidenceID,
		ProviderDiagnosticsJSON: `{"retry_at":"2026-03-01T12:10:00Z"}`,
		FailureReasonCode:       "timeout",
		FailureReasonText:       "provider asked Kingdom to retry later",
		StructuredPayloadJSON:   `{}`,
		SettledAt:               "2026-03-01T12:00:04Z",
	})
	mustExec(t, db, `UPDATE attempt_executions
		SET state = 'committed_failure', terminal_observed_at = '2026-03-01T12:00:03Z',
		    committed_at = '2026-03-01T12:00:04Z', failure_code = 'timeout',
		    failure_text = 'provider asked Kingdom to retry later', updated_at = '2026-03-01T12:00:04Z'
		WHERE attempt_id = 'run_1:decree_intake:1'`)

	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), loadRetryEngine(t), nil)
	if _, err := reconciler.Reconcile(ctx, ReconcileRequest{Now: "2026-03-01T12:01:00Z"}); err != nil {
		t.Fatalf("recover committed provider failure: %v", err)
	}

	var providerRetryAt, dueAt string
	if err := db.QueryRow(`SELECT provider_retry_at, due_at FROM retry_decisions WHERE source_attempt_id = ?`, attemptID).Scan(&providerRetryAt, &dueAt); err != nil {
		t.Fatalf("load recovered retry decision: %v", err)
	}
	if providerRetryAt != "2026-03-01T12:10:00Z" || dueAt != "2026-03-01T12:10:00Z" {
		t.Fatalf("expected persisted provider hint to set retry deadline, got provider_retry_at=%q due_at=%q", providerRetryAt, dueAt)
	}
	restarted := NewTransitionService(db, nil)
	released, err := restarted.ReleaseDueDecreeRetries(ctx, "run_1", "2026-03-01T12:09:59Z")
	if err != nil {
		t.Fatalf("release retries before provider hint: %v", err)
	}
	if len(released) != 0 {
		t.Fatalf("expected restart recovery to retain retry until provider hint, released %#v", released)
	}
}

func TestReconcilerMeaningfulProgressDoesNotEscalateExhaustedRetry(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	ctx := context.Background()
	store := executionstore.New(db)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at) VALUES ('nr_prior_retry', 'run_1', 'decree_intake', 2, 2, 'ready', 'king', 'agent_king', 1, NULL)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, created_at, updated_at) VALUES ('prior_attempt', 'run_1', 'nr_1', 'committed_failure', 'provider_backed', 'server_response', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO retry_decisions (retry_decision_id, subject_kind, run_id, node_id, node_run_id, source_attempt_id, target_node_run_id, policy_id, retry_class, error_code, operator_text, meaningful_progress, decision, retry_index, max_retries, policy_delay_ns, due_at, dispatch_state, escalation_target, resolution, created_at) VALUES ('retry_prior_attempt', 'decree_attempt', 'run_1', 'decree_intake', 'nr_1', 'prior_attempt', 'nr_prior_retry', 'retry-policy-mvp-v1', 'internal_runtime', 'internal_error', 'retry', 0, 'retry', 1, 1, 0, '2026-03-01T12:00:00Z', 'pending', 'chancellor', 'pending', '2026-03-01T12:00:00Z')`)

	attemptID := composeAttemptID("run_1", "decree_intake", 3)
	if err := store.EnqueueAttempt(ctx, executionstore.EnqueueAttemptRequest{AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeServerResponse, SessionRef: "session_meaningful_exhaustion", CreatedAt: "2026-03-01T12:00:00Z"}); err != nil {
		t.Fatalf("enqueue provider-backed attempt: %v", err)
	}
	if _, err := store.CreateProviderInvocation(ctx, executionstore.CreateProviderInvocationRequest{ProviderInvocationID: "invocation_meaningful_exhaustion", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", LogicalSessionID: "session_meaningful_exhaustion", CompletionMode: executionstore.ProviderCompletionModeServerResponse, IssuedAt: "2026-03-01T12:00:01Z", CreatedAt: "2026-03-01T12:00:01Z"}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := store.MarkProviderInvocationOpen(ctx, executionstore.MarkProviderInvocationOpenRequest{ProviderInvocationID: "invocation_meaningful_exhaustion", AttemptID: attemptID, ProviderSessionID: "ses_meaningful_exhaustion", OpenedAt: "2026-03-01T12:00:02Z"}); err != nil {
		t.Fatalf("mark provider invocation open: %v", err)
	}
	evidence, err := store.EnsureTerminalProviderCompletionEvidence(ctx, executionstore.EnsureTerminalProviderCompletionEvidenceRequest{ProviderInvocationID: "invocation_meaningful_exhaustion", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", State: "failed", ReasonCode: "internal_error", FailureText: "provider made progress before failure", EvidenceSource: "stream", CreatedAt: "2026-03-01T12:00:03Z"})
	if err != nil {
		t.Fatalf("ensure terminal provider evidence: %v", err)
	}
	mustExec(t, db, `UPDATE provider_completion_evidence SET payload_json = '{"observation":{"meaningful_progress":true}}' WHERE evidence_id = 'evidence_terminal_invocation_meaningful_exhaustion'`)
	seedHistoricalProviderSettlementWithoutTerminal(t, db, executionstore.FinalizeProviderCompletionSettlementRequest{SettlementID: executionstore.ProviderCompletionSettlementID("invocation_meaningful_exhaustion"), ProviderInvocationID: "invocation_meaningful_exhaustion", AttemptID: attemptID, RunID: "run_1", NodeRunID: "nr_1", Outcome: executionstore.ProviderCompletionOutcomeFailed, PrimaryEvidenceID: evidence.EvidenceID, ProviderDiagnosticsJSON: `{}`, FailureReasonCode: "internal_error", FailureReasonText: "provider made progress before failure", StructuredPayloadJSON: `{}`, SettledAt: "2026-03-01T12:00:04Z"})
	mustExec(t, db, `UPDATE attempt_executions SET state = 'committed_failure', terminal_observed_at = '2026-03-01T12:00:03Z', committed_at = '2026-03-01T12:00:04Z', failure_code = 'internal_error', failure_text = 'provider made progress before failure', updated_at = '2026-03-01T12:00:04Z' WHERE attempt_id = 'run_1:decree_intake:3'`)

	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), loadRetryEngine(t), nil)
	if _, err := reconciler.Reconcile(ctx, ReconcileRequest{Now: "2026-03-01T12:05:00Z"}); err != nil {
		t.Fatalf("reconcile meaningful exhausted failure: %v", err)
	}
	var decision, resolution string
	if err := db.QueryRow(`SELECT decision, resolution FROM retry_decisions WHERE source_attempt_id = ?`, attemptID).Scan(&decision, &resolution); err != nil {
		t.Fatalf("load meaningful exhausted retry decision: %v", err)
	}
	if decision != string(RetryDecisionFailFast) || resolution != string(RetryDecisionResolutionNotApplicable) {
		t.Fatalf("meaningful exhausted retry decision=%q resolution=%q, want fail_fast/not_applicable", decision, resolution)
	}
}

func TestReconcilerDoesNotCreateSettlementFromTerminalAttemptState(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:13:00Z"
	attemptID := composeAttemptID("run_1", "decree_intake", 2)
	store := executionstore.New(db)

	mustExec(t, db, `UPDATE node_runs SET node_status = 'succeeded', finished_at = '2026-03-01T12:07:00Z' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T12:08:00Z' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_2', 'run_1', 'decree_intake', 2,2, 'running', 'king', 'agent_king', 1, '2026-03-01T12:08:00Z', NULL)`)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_nr_2_started_committed_success_backfill', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_2', '%s', 'v1', 'attempt.started', 1, '{}', 'actor_1', 'king', 'system', 'internal_error', 'seed', 'corr_nr2_committed_success_backfill', 'caus_nr2_committed_success_backfill', 'prev_nr2_committed_success_backfill', 'entry_nr2_committed_success_backfill', '2026-03-01T12:08:00Z')`, attemptID))

	if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_2",
		ExecutionMode:  executionstore.ExecutionModeProviderBacked,
		CompletionMode: executionstore.ProviderCompletionModeServerResponse,
		SessionRef:     "session_reconcile_committed_success",
		CreatedAt:      "2026-03-01T12:08:00Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if _, err := store.CreateProviderInvocation(context.Background(), executionstore.CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_committed_success_backfill",
		AttemptID:            attemptID,
		RunID:                "run_1",
		CompletionMode:       executionstore.ProviderCompletionModeServerResponse,
		NodeRunID:            "nr_2",
		LogicalSessionID:     "session_reconcile_committed_success",
		IssuedAt:             "2026-03-01T12:08:01Z",
		CreatedAt:            "2026-03-01T12:08:01Z",
	}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := store.MarkProviderInvocationOpen(context.Background(), executionstore.MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "invocation_committed_success_backfill",
		AttemptID:            attemptID,
		ProviderSessionID:    "ses_committed_success_backfill",
		OpenedAt:             "2026-03-01T12:08:02Z",
	}); err != nil {
		t.Fatalf("mark provider invocation open: %v", err)
	}
	mustExec(t, db, fmt.Sprintf(`UPDATE attempt_executions
		SET state = 'committed_success',
		    started_at = '2026-03-01T12:08:00Z',
		    terminal_observed_at = '2026-03-01T12:08:04Z',
		    committed_at = '2026-03-01T12:08:04Z',
		    canonical_settlement_id = NULL,
		    updated_at = '2026-03-01T12:08:04Z'
		WHERE attempt_id = '%s'`, attemptID))

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)
	if _, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now}); err != nil {
		t.Fatalf("reconcile committed success backfill: %v", err)
	}

	attempt, found, err := store.GetAttempt(context.Background(), attemptID)
	if err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if !found {
		t.Fatal("expected durable attempt execution to exist")
	}
	if attempt.State != "committed_success" {
		t.Fatalf("expected committed_success durable state, got %#v", attempt)
	}
	if strings.TrimSpace(attempt.CanonicalSettlementID) != "" {
		t.Fatalf("expected committed attempt not to authorize a canonical settlement, got %#v", attempt)
	}
	if got := queryNodeStatus(t, db, "nr_2"); got != "succeeded" {
		t.Fatalf("expected committed success node to project as succeeded, got %q", got)
	}

	settlement, found, err := store.GetProviderCompletionSettlementByInvocation(context.Background(), "invocation_committed_success_backfill")
	if err != nil {
		t.Fatalf("get provider completion settlement by invocation: %v", err)
	}
	if found {
		t.Fatalf("expected committed attempt not to create a provider settlement, got %#v", settlement)
	}
}

func TestReconcilerDoesNotClassifyWorkflowReworkAsAutomaticRetry(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		nodeID   string
		nodeRun  string
		roleID   string
		agentID  string
		nodeType string
		code     string
	}{
		{name: "execution review", nodeID: "execution_review_shieldmaiden", nodeRun: "nr_rework_review", roleID: "reviewer-shieldmaiden", agentID: "agent_rework_review", nodeType: "execution_review", code: "execution_review_rework"},
		{name: "validation gate", nodeID: "validation_gates_qa_scout", nodeRun: "nr_rework_validation", roleID: "intelligence-qa-scout", agentID: "agent_rework_validation", nodeType: "validation_gates", code: "validation_gate_rework"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := openTestDB(t)
			applyMigration(t, db, "0001_initial_schema.up.sql")
			seedMinimumGraph(t, db)
			mustExec(t, db, `UPDATE node_runs SET node_status = 'succeeded', finished_at = '2026-03-01T12:00:00Z' WHERE node_run_id = 'nr_1'`)
			mustExec(t, db, fmt.Sprintf(`INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES (%q, %q, 0, '2026-03-01T12:00:00Z')`, tc.roleID, tc.roleID))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES (%q, %q, 'busy', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`, tc.agentID, tc.roleID))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES (%q, 'wfdef_1', %q, %q, %q, '2026-03-01T12:00:00Z')`, "wn_"+tc.nodeRun, tc.nodeID, tc.roleID, tc.nodeType))
			mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES (%q, 'run_1', %q, 1, 1, 'running', %q, %q, 1, '2026-03-01T12:00:00Z', NULL)`, tc.nodeRun, tc.nodeID, tc.roleID, tc.agentID))
			attemptID := composeAttemptID("run_1", tc.nodeID, 1)
			mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, failure_code, failure_text, created_at, updated_at, terminal_observed_at, committed_at) VALUES (%q, 'run_1', %q, 'committed_failure', 'provider_backed', 'server_response', %q, 'workflow requested rework', '2026-03-01T12:00:00Z', '2026-03-01T12:00:01Z', '2026-03-01T12:00:01Z', '2026-03-01T12:00:01Z')`, attemptID, tc.nodeRun, tc.code))

			reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), loadRetryEngine(t), nil)
			if _, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: "2026-03-01T12:05:00Z"}); err != nil {
				t.Fatalf("reconcile rework attempt: %v", err)
			}
			var decisions int
			if err := db.QueryRow(`SELECT COUNT(1) FROM retry_decisions WHERE source_attempt_id = ?`, attemptID).Scan(&decisions); err != nil {
				t.Fatal(err)
			}
			if decisions != 0 {
				t.Fatalf("workflow rework created %d automatic retry decisions", decisions)
			}
			if status := queryNodeStatus(t, db, tc.nodeRun); status != "failed" {
				t.Fatalf("workflow rework node status=%q, want failed projection", status)
			}
		})
	}
}

func TestReconcilerDoesNotCreateSettlementFromTerminalAttemptEvidence(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:13:00Z"
	attemptID := composeAttemptID("run_1", "decree_intake", 2)
	store := executionstore.New(db)

	mustExec(t, db, `UPDATE node_runs SET node_status = 'succeeded', finished_at = '2026-03-01T12:07:00Z' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T12:08:00Z' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_2', 'run_1', 'decree_intake', 2,2, 'running', 'king', 'agent_king', 1, '2026-03-01T12:08:00Z', NULL)`)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_nr_2_started_existing_evidence_backfill', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_2', '%s', 'v1', 'attempt.started', 1, '{}', 'actor_1', 'king', 'system', 'internal_error', 'seed', 'corr_nr2_existing_evidence_backfill', 'caus_nr2_existing_evidence_backfill', 'prev_nr2_existing_evidence_backfill', 'entry_nr2_existing_evidence_backfill', '2026-03-01T12:08:00Z')`, attemptID))

	if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_2",
		ExecutionMode:  executionstore.ExecutionModeProviderBacked,
		CompletionMode: executionstore.ProviderCompletionModeServerResponse,
		SessionRef:     "session_reconcile_existing_evidence",
		CreatedAt:      "2026-03-01T12:08:00Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if _, err := store.CreateProviderInvocation(context.Background(), executionstore.CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_existing_evidence_backfill",
		AttemptID:            attemptID,
		RunID:                "run_1",
		CompletionMode:       executionstore.ProviderCompletionModeServerResponse,
		NodeRunID:            "nr_2",
		LogicalSessionID:     "session_reconcile_existing_evidence",
		IssuedAt:             "2026-03-01T12:08:01Z",
		CreatedAt:            "2026-03-01T12:08:01Z",
	}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := store.MarkProviderInvocationOpen(context.Background(), executionstore.MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "invocation_existing_evidence_backfill",
		AttemptID:            attemptID,
		ProviderSessionID:    "ses_existing_evidence_backfill",
		OpenedAt:             "2026-03-01T12:08:02Z",
	}); err != nil {
		t.Fatalf("mark provider invocation open: %v", err)
	}

	payloadRaw, err := json.Marshal(map[string]any{
		"state":           "failed",
		"reason_code":     "provider_incomplete_turn",
		"failure_text":    "provider exited without a validated terminal response",
		"output_text":     "Recovered provider transcript excerpt.",
		"evidence_source": "export",
	})
	if err != nil {
		t.Fatalf("marshal existing evidence payload: %v", err)
	}
	if _, err := store.AppendProviderCompletionEvidence(context.Background(), executionstore.AppendProviderCompletionEvidenceRequest{
		EvidenceID:           "evidence_terminal_invocation_existing_evidence_backfill",
		ProviderInvocationID: "invocation_existing_evidence_backfill",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_2",
		EvidenceKind:         executionstore.ProviderCompletionEvidenceKindStreamObservation,
		PayloadJSON:          string(payloadRaw),
		CreatedAt:            "2026-03-01T12:08:04Z",
	}); err != nil {
		t.Fatalf("append existing provider evidence: %v", err)
	}

	mustExec(t, db, fmt.Sprintf(`UPDATE attempt_executions
		SET state = 'committed_failure',
		    started_at = '2026-03-01T12:08:00Z',
		    terminal_observed_at = '2026-03-01T12:08:04Z',
		    committed_at = '2026-03-01T12:08:04Z',
		    failure_code = 'provider_incomplete_turn',
		    failure_text = 'provider exited without a validated terminal response',
		    canonical_settlement_id = NULL,
		    updated_at = '2026-03-01T12:08:04Z'
		WHERE attempt_id = '%s'`, attemptID))

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)
	if _, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now}); err != nil {
		t.Fatalf("reconcile terminal attempt evidence: %v", err)
	}

	attempt, found, err := store.GetAttempt(context.Background(), attemptID)
	if err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if !found {
		t.Fatal("expected durable attempt execution to exist")
	}
	if strings.TrimSpace(attempt.CanonicalSettlementID) != "" {
		t.Fatalf("expected contradictory evidence to remain unsettled, got %#v", attempt)
	}
	settlement, found, err := store.GetProviderCompletionSettlementByInvocation(context.Background(), "invocation_existing_evidence_backfill")
	if err != nil {
		t.Fatalf("get provider completion settlement by invocation: %v", err)
	}
	if found {
		t.Fatalf("expected no provider settlement repair, got %#v", settlement)
	}
}

func TestReconcilerDoesNotRewriteSettlementFromTerminalAttemptTruth(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:13:00Z"
	attemptID := composeAttemptID("run_1", "decree_intake", 2)
	store := executionstore.New(db)

	mustExec(t, db, `UPDATE node_runs SET node_status = 'succeeded', finished_at = '2026-03-01T12:07:00Z' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T12:08:00Z' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_2', 'run_1', 'decree_intake', 2,2, 'running', 'king', 'agent_king', 1, '2026-03-01T12:08:00Z', NULL)`)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_nr_2_started_settlement_reconcile', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_2', '%s', 'v1', 'attempt.started', 1, '{}', 'actor_1', 'king', 'system', 'internal_error', 'seed', 'corr_nr2_settlement_reconcile', 'caus_nr2_settlement_reconcile', 'prev_nr2_settlement_reconcile', 'entry_nr2_settlement_reconcile', '2026-03-01T12:08:00Z')`, attemptID))

	if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_2",
		ExecutionMode:  executionstore.ExecutionModeProviderBacked,
		CompletionMode: executionstore.ProviderCompletionModeServerResponse,
		SessionRef:     "session_reconcile_settlement_mismatch",
		CreatedAt:      "2026-03-01T12:08:00Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if _, err := store.CreateProviderInvocation(context.Background(), executionstore.CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_settlement_mismatch",
		AttemptID:            attemptID,
		RunID:                "run_1",
		CompletionMode:       executionstore.ProviderCompletionModeServerResponse,
		NodeRunID:            "nr_2",
		LogicalSessionID:     "session_reconcile_settlement_mismatch",
		IssuedAt:             "2026-03-01T12:08:01Z",
		CreatedAt:            "2026-03-01T12:08:01Z",
	}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := store.MarkProviderInvocationOpen(context.Background(), executionstore.MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "invocation_settlement_mismatch",
		AttemptID:            attemptID,
		ProviderSessionID:    "ses_settlement_mismatch",
		OpenedAt:             "2026-03-01T12:08:02Z",
	}); err != nil {
		t.Fatalf("mark provider invocation open: %v", err)
	}
	const settlementText = "Server-observed success that should be cleared."
	accepted, err := store.AppendProviderCompletionEvidence(context.Background(), executionstore.AppendProviderCompletionEvidenceRequest{
		EvidenceID:           "evidence_settlement_mismatch",
		ProviderInvocationID: "invocation_settlement_mismatch",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_2",
		EvidenceKind:         executionstore.ProviderCompletionEvidenceKindStreamObservation,
		PayloadJSON:          `{"observation":{"canonical_terminal_text":"Server-observed success that should be cleared."}}`,
		CreatedAt:            "2026-03-01T12:08:03Z",
	})
	if err != nil {
		t.Fatalf("append stream observation evidence: %v", err)
	}
	seedHistoricalProviderSettlementWithoutTerminal(t, db, executionstore.FinalizeProviderCompletionSettlementRequest{
		SettlementID:          "settlement_invocation_settlement_mismatch",
		ProviderInvocationID:  "invocation_settlement_mismatch",
		AttemptID:             attemptID,
		RunID:                 "run_1",
		NodeRunID:             "nr_2",
		Outcome:               executionstore.ProviderCompletionOutcomeSuccess,
		PrimaryEvidenceID:     accepted.EvidenceID,
		FinalText:             settlementText,
		StructuredPayloadJSON: `{}`,
		SettledAt:             "2026-03-01T12:08:04Z",
	})

	mustExec(t, db, fmt.Sprintf(`UPDATE attempt_executions
		SET state = 'committed_cancelled',
		    terminal_observed_at = '2026-03-01T12:08:05Z',
		    committed_at = '2026-03-01T12:08:05Z',
		    failure_code = 'run_terminalized',
		    failure_text = 'run terminalized before projection',
		    canonical_settlement_id = NULL,
		    updated_at = '2026-03-01T12:08:05Z'
		WHERE attempt_id = '%s'`, attemptID))

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)
	if _, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now}); err != nil {
		t.Fatalf("reconcile provider settlement mismatch: %v", err)
	}

	attempt, found, err := store.GetAttempt(context.Background(), attemptID)
	if err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if !found {
		t.Fatal("expected durable attempt execution to exist")
	}
	if strings.TrimSpace(attempt.CanonicalSettlementID) != "" {
		t.Fatalf("expected terminal attempt not to rewrite its canonical settlement link, got %#v", attempt)
	}
	if got := queryNodeStatus(t, db, "nr_2"); got != "cancelled" {
		t.Fatalf("expected unrelated terminal node projection to remain intact, got %q", got)
	}
	settlement, found, err := store.GetProviderCompletionSettlementByInvocation(context.Background(), "invocation_settlement_mismatch")
	if err != nil {
		t.Fatalf("get provider completion settlement by invocation: %v", err)
	}
	if !found {
		t.Fatal("expected provider completion settlement to exist")
	}
	if settlement.Outcome != executionstore.ProviderCompletionOutcomeSuccess {
		t.Fatalf("expected existing provider settlement outcome to remain immutable, got %#v", settlement)
	}
	if settlement.FailureReasonCode != "" {
		t.Fatalf("expected existing provider settlement failure reason to remain unchanged, got %#v", settlement)
	}
	if settlement.FinalText != settlementText || settlement.SettledAt != "2026-03-01T12:08:04Z" {
		t.Fatalf("expected existing provider settlement payload and timestamp to remain immutable, got %#v", settlement)
	}
}

func TestReconcilerDoesNotRewritePinnedCanonicalSettlementFromTerminalAttempt(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:13:00Z"
	attemptID := composeAttemptID("run_1", "decree_intake", 2)
	store := executionstore.New(db)

	mustExec(t, db, `UPDATE node_runs SET node_status = 'succeeded', finished_at = '2026-03-01T12:07:00Z' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T12:08:00Z' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_2_pinned', 'run_1', 'decree_intake', 2,2, 'running', 'king', 'agent_king', 1, '2026-03-01T12:08:00Z', NULL)`)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_nr_2_started_settlement_pinned', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_2_pinned', '%s', 'v1', 'attempt.started', 1, '{}', 'actor_1', 'king', 'system', 'internal_error', 'seed', 'corr_nr2_settlement_pinned', 'caus_nr2_settlement_pinned', 'prev_nr2_settlement_pinned', 'entry_nr2_settlement_pinned', '2026-03-01T12:08:00Z')`, attemptID))

	if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_2_pinned",
		ExecutionMode:  executionstore.ExecutionModeProviderBacked,
		CompletionMode: executionstore.ProviderCompletionModeServerResponse,
		SessionRef:     "session_reconcile_settlement_pinned",
		CreatedAt:      "2026-03-01T12:08:00Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if _, err := store.CreateProviderInvocation(context.Background(), executionstore.CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_settlement_pinned",
		AttemptID:            attemptID,
		RunID:                "run_1",
		CompletionMode:       executionstore.ProviderCompletionModeServerResponse,
		NodeRunID:            "nr_2_pinned",
		LogicalSessionID:     "session_reconcile_settlement_pinned",
		IssuedAt:             "2026-03-01T12:08:01Z",
		CreatedAt:            "2026-03-01T12:08:01Z",
	}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := store.MarkProviderInvocationOpen(context.Background(), executionstore.MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "invocation_settlement_pinned",
		AttemptID:            attemptID,
		ProviderSessionID:    "ses_settlement_pinned",
		OpenedAt:             "2026-03-01T12:08:02Z",
	}); err != nil {
		t.Fatalf("mark provider invocation open: %v", err)
	}
	const pinnedSettlementText = "Server-observed success that should remain immutable."
	accepted, err := store.AppendProviderCompletionEvidence(context.Background(), executionstore.AppendProviderCompletionEvidenceRequest{
		EvidenceID:           "evidence_settlement_pinned",
		ProviderInvocationID: "invocation_settlement_pinned",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_2_pinned",
		EvidenceKind:         executionstore.ProviderCompletionEvidenceKindStreamObservation,
		PayloadJSON:          `{"observation":{"canonical_terminal_text":"Server-observed success that should remain immutable."}}`,
		CreatedAt:            "2026-03-01T12:08:03Z",
	})
	if err != nil {
		t.Fatalf("append stream observation evidence: %v", err)
	}
	seedHistoricalProviderSettlementWithoutTerminal(t, db, executionstore.FinalizeProviderCompletionSettlementRequest{
		SettlementID:          "settlement_invocation_settlement_pinned",
		ProviderInvocationID:  "invocation_settlement_pinned",
		AttemptID:             attemptID,
		RunID:                 "run_1",
		NodeRunID:             "nr_2_pinned",
		Outcome:               executionstore.ProviderCompletionOutcomeSuccess,
		PrimaryEvidenceID:     accepted.EvidenceID,
		FinalText:             pinnedSettlementText,
		StructuredPayloadJSON: `{}`,
		SettledAt:             "2026-03-01T12:08:04Z",
	})

	mustExec(t, db, fmt.Sprintf(`UPDATE attempt_executions
		SET state = 'committed_cancelled',
		    terminal_observed_at = '2026-03-01T12:08:05Z',
		    committed_at = '2026-03-01T12:08:05Z',
		    failure_code = 'run_terminalized',
		    failure_text = 'run terminalized before projection',
		    canonical_settlement_id = 'settlement_invocation_settlement_pinned',
		    updated_at = '2026-03-01T12:08:05Z'
		WHERE attempt_id = '%s'`, attemptID))

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)
	if _, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now}); err != nil {
		t.Fatalf("reconcile pinned provider settlement mismatch: %v", err)
	}

	settlement, found, err := store.GetProviderCompletionSettlementByInvocation(context.Background(), "invocation_settlement_pinned")
	if err != nil {
		t.Fatalf("get provider completion settlement by invocation: %v", err)
	}
	if !found {
		t.Fatal("expected provider completion settlement to exist")
	}
	if settlement.Outcome != executionstore.ProviderCompletionOutcomeSuccess {
		t.Fatalf("expected pinned provider settlement outcome to remain immutable, got %#v", settlement)
	}
	if settlement.FailureReasonCode != "" {
		t.Fatalf("expected pinned provider settlement failure reason to remain unchanged, got %#v", settlement)
	}
	if settlement.FinalText != pinnedSettlementText || settlement.SettledAt != "2026-03-01T12:08:04Z" {
		t.Fatalf("expected pinned provider settlement payload and timestamp to remain immutable, got %#v", settlement)
	}
}

func TestReconcilerRetryHandoffDoesNotCommitBlockedStateOnQueueFailure(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:05:30Z"
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T11:59:00Z' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `UPDATE node_runs SET started_at = '2026-03-01T11:58:00Z', finished_at = NULL WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king_backup', 'king', 'idle', NULL, 0, '2026-03-01T11:58:00Z', '2026-03-01T11:58:00Z')`)
	mustExec(t, db, `INSERT INTO session_leases (session_id, agent_id, status, lease_expires_at, last_heartbeat_at, created_at, updated_at) VALUES ('session_busy', 'agent_king', 'active', '2026-03-01T11:58:30Z', '2026-03-01T11:58:00Z', '2026-03-01T11:58:00Z', '2026-03-01T11:58:00Z')`)
	mustExec(t, db, `INSERT INTO session_leases (session_id, agent_id, status, lease_expires_at, last_heartbeat_at, created_at, updated_at) VALUES ('session_idle', 'agent_king_backup', 'active', '2026-03-01T11:58:30Z', '2026-03-01T11:58:00Z', '2026-03-01T11:58:00Z', '2026-03-01T11:58:00Z')`)
	mustExec(t, db, `CREATE TRIGGER fail_reconcile_retry_insert BEFORE INSERT ON node_runs
		WHEN NEW.run_id = 'run_1' AND NEW.node_id = 'decree_intake' AND NEW.attempt = 2
		BEGIN
			SELECT RAISE(FAIL, 'forced retry insert failure');
		END`)

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)

	if _, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now}); err == nil {
		t.Fatal("expected reconcile to fail when retry handoff insert fails")
	}

	if got := queryNodeStatus(t, db, "nr_1"); got != "running" {
		t.Fatalf("expected original stale node to remain running after failed retry handoff, got %q", got)
	}

	var retryCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM node_runs WHERE run_id = 'run_1' AND node_id = 'decree_intake' AND attempt = 2`).Scan(&retryCount); err != nil {
		t.Fatalf("query retry node count: %v", err)
	}
	if retryCount != 0 {
		t.Fatalf("expected no retry attempt rows after failed retry handoff, got %d", retryCount)
	}
}

func TestReconcilerIncludesDiagnosticsInStaleFailurePayload(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:13:00Z"
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T12:08:00Z' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'succeeded', finished_at = '2026-03-01T12:07:00Z' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_2', 'run_1', 'decree_intake', 2,2, 'running', 'king', 'agent_king', 1, '2026-03-01T12:08:00Z', NULL)`)
	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_nr_2_started', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_2', 'att_2', 'v1', 'attempt.started', 1, '{}', 'actor_1', 'king', 'system', 'internal_error', 'seed', 'corr_nr2', 'caus_nr2', 'prev_nr2', 'entry_nr2', '2026-03-01T12:08:00Z')`)
	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_nr_2_progress', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_2', 'att_2', 'v1', 'attempt.progress', 2, '{"text":"draft done","execution_path_diagnostics_snapshot":{"stdout_event_count":3,"step_finish_seen":false}}', 'actor_1', 'king', 'system', 'orchestration_progress', 'seed', 'corr_nr2', 'caus_nr2', 'prev_nr2b', 'entry_nr2b', '2026-03-01T12:09:00Z')`)

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)
	progressDiag, err := reconciler.latestAttemptProgressDiagnostics(context.Background(), "att_2", "nr_2", mustParseRFC3339(t, now))
	if err != nil {
		t.Fatalf("preflight latest progress diagnostics: %v", err)
	}
	if progressDiag["progress_event_seen"] != true {
		t.Fatalf("expected preflight progress_event_seen=true, got %#v", progressDiag)
	}

	if _, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now}); err != nil {
		t.Fatalf("reconcile stale exhausted attempt: %v", err)
	}

	payload := queryLatestAttemptFailedPayload(t, db, "att_2")
	if payload["reason"] != "stale_attempt" {
		t.Fatalf("expected reason=stale_attempt, got %#v", payload["reason"])
	}
	if payload["node_id"] != "decree_intake" {
		t.Fatalf("expected node_id=decree_intake, got %#v", payload["node_id"])
	}
	if payload["retry_allowed"] != false || payload["meaningful_progress"] != true {
		t.Fatalf("expected meaningful provider text to disable retry, got retry_allowed=%#v meaningful_progress=%#v", payload["retry_allowed"], payload["meaningful_progress"])
	}
	if payload["retries_attempted"] == nil || payload["max_retries"] == nil {
		t.Fatalf("expected retries_attempted/max_retries diagnostics in payload, got %#v", payload)
	}
	if payload["stale_threshold_seconds"] == nil || payload["planning_grace_seconds"] == nil {
		t.Fatalf("expected stale threshold diagnostics in payload, got %#v", payload)
	}
	if strings.TrimSpace(fmt.Sprint(payload["candidate_started_at"])) == "" {
		t.Fatalf("expected candidate_started_at in stale payload, got %#v", payload["candidate_started_at"])
	}
	latestProgress, ok := payload["latest_progress"].(map[string]any)
	if !ok {
		t.Fatalf("expected latest_progress map in stale payload, got %#v", payload["latest_progress"])
	}
	if latestProgress["progress_event_seen"] != true {
		t.Fatalf("expected progress_event_seen=true, got %#v (payload=%#v)", latestProgress["progress_event_seen"], latestProgress)
	}
	if strings.TrimSpace(fmt.Sprint(latestProgress["last_progress_at"])) == "" {
		t.Fatalf("expected last_progress_at in latest progress payload, got %#v", latestProgress)
	}
	if snapshot, ok := latestProgress["execution_path_diagnostics_snapshot"].(map[string]any); !ok || snapshot["stdout_event_count"] == nil {
		t.Fatalf("expected execution_path_diagnostics_snapshot with stdout_event_count, got %#v", latestProgress["execution_path_diagnostics_snapshot"])
	}
}

func queryProgressEventCountForAttemptOrNode(t *testing.T, db *sql.DB, attemptID, nodeRunID string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE event_type = 'attempt.progress' AND (attempt_id = ? OR node_run_id = ?)`, attemptID, nodeRunID).Scan(&count); err != nil {
		t.Fatalf("query progress event count: %v", err)
	}
	return count
}

type staleProviderHandoffFenceFixture struct {
	db           *sql.DB
	store        *executionstore.Store
	attemptID    string
	invocationID string
	leaseOwner   string
	leaseVersion int64
}

func newStaleProviderHandoffFenceFixture(t *testing.T) staleProviderHandoffFenceFixture {
	t.Helper()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	const (
		attemptID    = "run_1:decree_intake:2"
		invocationID = "invocation_stale_handoff_fence"
		leaseOwner   = "stale-handoff-fence-worker"
		startedAt    = "2026-03-01T12:08:00Z"
	)
	mustExecArgs(t, db, `UPDATE agents SET status = 'busy', updated_at = ? WHERE agent_id = 'agent_king'`, startedAt)
	mustExecArgs(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king_backup', 'king', 'idle', NULL, 0, ?, ?)`, startedAt, startedAt)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'succeeded', finished_at = '2026-03-01T12:07:00Z' WHERE node_run_id = 'nr_1'`)
	mustExecArgs(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_2', 'run_1', 'decree_intake', 2, 2, 'running', 'king', 'agent_king', 1, ?, NULL)`, startedAt)
	mustExecArgs(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_nr_2_started_handoff_fence', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_2', ?, 'v1', 'attempt.started', 1, '{}', 'actor_1', 'king', 'system', 'internal_error', 'seed', 'corr_nr2_handoff_fence', 'caus_nr2_handoff_fence', 'prev_nr2_handoff_fence', 'entry_nr2_handoff_fence', ?)`, attemptID, startedAt)

	store := executionstore.New(db)
	if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_2",
		ExecutionMode:  executionstore.ExecutionModeProviderBacked,
		CompletionMode: executionstore.ProviderCompletionModeServerResponse,
		SessionRef:     "session_stale_handoff_fence",
		CreatedAt:      startedAt,
	}); err != nil {
		t.Fatalf("enqueue stale provider attempt: %v", err)
	}
	claim, acquired, err := store.ClaimQueuedProviderAttempt(context.Background(), executionstore.ClaimQueuedProviderAttemptRequest{
		AttemptID:  attemptID,
		LeaseOwner: leaseOwner,
		LeaseUntil: "2026-03-01T12:20:00Z",
		ClaimedAt:  startedAt,
	})
	if err != nil || !acquired {
		t.Fatalf("claim stale provider attempt: claim=%#v acquired=%t err=%v", claim, acquired, err)
	}
	if _, err := store.ActivateProviderInvocation(context.Background(), executionstore.ActivateProviderInvocationRequest{
		ProviderInvocationID: invocationID,
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_2",
		LogicalSessionID:     "session_stale_handoff_fence",
		CompletionMode:       executionstore.ProviderCompletionModeServerResponse,
		IssuedAt:             "2026-03-01T12:08:01Z",
		CreatedAt:            "2026-03-01T12:08:01Z",
		OpenedAt:             "2026-03-01T12:08:02Z",
		ExpectedLeaseOwner:   claim.LeaseOwner,
		ExpectedLeaseVersion: claim.LeaseVersion,
	}); err != nil {
		t.Fatalf("activate stale provider invocation: %v", err)
	}
	mustExecArgs(t, db, `UPDATE attempt_executions SET state = 'running', started_at = ?, lease_expires_at = '2026-03-01T12:08:30Z', updated_at = ? WHERE attempt_id = ?`, startedAt, startedAt, attemptID)
	return staleProviderHandoffFenceFixture{db: db, store: store, attemptID: attemptID, invocationID: invocationID, leaseOwner: claim.LeaseOwner, leaseVersion: claim.LeaseVersion}
}

func assertStaleProviderInvocationRemainsActive(t *testing.T, fixture staleProviderHandoffFenceFixture) {
	t.Helper()
	if got := queryNodeStatus(t, fixture.db, "nr_2"); got != "running" {
		t.Fatalf("provider node status=%q, want running", got)
	}
	attempt, found, err := fixture.store.GetAttempt(context.Background(), fixture.attemptID)
	if err != nil || !found || attempt.State != "running" {
		t.Fatalf("active attempt changed: found=%t attempt=%#v err=%v", found, attempt, err)
	}
	invocation, found, err := fixture.store.GetProviderInvocation(context.Background(), fixture.invocationID)
	if err != nil || !found || invocation.InvocationState != executionstore.ProviderInvocationStateOpen {
		t.Fatalf("active provider invocation changed: found=%t invocation=%#v err=%v", found, invocation, err)
	}
	if count := countRowsForReconcilerTest(t, fixture.db, `SELECT COUNT(1) FROM node_runs WHERE run_id = 'run_1' AND node_id = 'decree_intake' AND attempt > 2`); count != 0 {
		t.Fatalf("stale recovery created %d replacement attempts", count)
	}
}

func countRowsForReconcilerTest(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var count int
	if err := db.QueryRow(query, args...).Scan(&count); err != nil {
		t.Fatalf("count reconciler test rows: %v", err)
	}
	return count
}

func seedHistoricalProviderSettlementWithoutTerminal(t *testing.T, db *sql.DB, req executionstore.FinalizeProviderCompletionSettlementRequest) {
	t.Helper()
	structuredPayloadJSON := strings.TrimSpace(req.StructuredPayloadJSON)
	if structuredPayloadJSON == "" {
		structuredPayloadJSON = `{}`
	}
	providerDiagnosticsJSON := strings.TrimSpace(req.ProviderDiagnosticsJSON)
	if providerDiagnosticsJSON == "" {
		providerDiagnosticsJSON = `{}`
	}
	failureCode, failureText := strings.TrimSpace(req.FailureReasonCode), strings.TrimSpace(req.FailureReasonText)
	invocationState := executionstore.ProviderInvocationStateSettledFailed
	if req.Outcome == executionstore.ProviderCompletionOutcomeSuccess {
		invocationState = executionstore.ProviderInvocationStateSettledSuccess
		failureCode, failureText = "", ""
	} else if req.Outcome == executionstore.ProviderCompletionOutcomeCancelled {
		invocationState = executionstore.ProviderInvocationStateSettledCancelled
	}
	if _, err := db.Exec(`INSERT INTO provider_completion_settlements (
		settlement_id, provider_invocation_id, attempt_id, run_id, node_run_id, outcome,
		primary_evidence_id, final_text, structured_payload_json, provider_diagnostics_json,
		failure_reason_code, failure_reason_text, settled_at, created_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		req.SettlementID, req.ProviderInvocationID, req.AttemptID, req.RunID, req.NodeRunID, string(req.Outcome),
		req.PrimaryEvidenceID, req.FinalText, structuredPayloadJSON, providerDiagnosticsJSON,
		failureCode, failureText, req.SettledAt, req.SettledAt,
	); err != nil {
		t.Fatalf("seed historical provider settlement %q: %v", req.SettlementID, err)
	}
	if _, err := db.Exec(`UPDATE provider_invocations
		SET invocation_state = ?, settled_at = ?,
		    failure_reason_code = ?, failure_reason_text = ?, updated_at = ?
		WHERE provider_invocation_id = ?`,
		string(invocationState), req.SettledAt, failureCode, failureText, req.SettledAt, req.ProviderInvocationID,
	); err != nil {
		t.Fatalf("seed historical provider invocation settlement %q: %v", req.ProviderInvocationID, err)
	}
	if _, err := db.Exec(`UPDATE attempt_executions SET canonical_settlement_id = ? WHERE attempt_id = ?`, req.SettlementID, req.AttemptID); err != nil {
		t.Fatalf("link historical provider settlement %q: %v", req.SettlementID, err)
	}
}

func mustParseRFC3339(t *testing.T, raw string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatalf("parse rfc3339 %q: %v", raw, err)
	}
	return parsed
}

func TestReconcilerAllowsOneSelfRetryForPlanningNodeWhenNoReplacementAgent(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:05:30Z"
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T11:59:00Z' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `UPDATE node_runs SET started_at = '2026-03-01T11:58:00Z', finished_at = NULL WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_nr_1_started', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_1', 'run_1:decree_intake:1', 'v1', 'attempt.started', 1, '{}', 'actor_1', 'king', 'system', 'orchestration_progress', 'seed', 'corr_nr1', 'caus_nr1', 'prev_nr1', 'entry_nr1', '2026-03-01T11:58:00Z')`)

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)

	result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now})
	if err != nil {
		t.Fatalf("reconcile startup: %v", err)
	}

	if result.AttemptsReassigned != 1 {
		t.Fatalf("expected one reassigned attempt via self-retry, got %d", result.AttemptsReassigned)
	}
	retryNodeRunID := "nr_1_retry_2"
	if got := queryNodeStatus(t, db, retryNodeRunID); got != "pending" {
		t.Fatalf("expected delayed self-retry node to remain pending, got %q", got)
	}
	if startedAt := queryNodeStartedAt(t, db, retryNodeRunID); startedAt != "" {
		t.Fatalf("expected self-retry started_at to remain empty before dispatch, got %q", startedAt)
	}
	if got := queryNodeAssignedAgent(t, db, retryNodeRunID); got != "agent_king" {
		t.Fatalf("expected self-retry to keep same agent, got %q", got)
	}
	if got := queryReconcilerAgentStatus(t, db, "agent_king"); got != "busy" {
		t.Fatalf("expected agent_king to remain busy during self-retry, got %q", got)
	}
}

func TestReconcilerDoesNotMarkRecentPlanningAttemptAsStale(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `UPDATE node_runs SET node_status = 'succeeded', finished_at = '2026-03-01T12:00:00Z' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_planner_1', 'king', 'busy', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_plan_1', 'run_1', 'plan_drafting', 1,1, 'running', 'king', 'agent_planner_1', 1, '2026-03-01T12:00:00Z', NULL)`)
	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_plan_started', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_plan_1', 'run_1:plan_drafting:1', 'v1', 'attempt.started', 1, '{}', 'actor_1', 'king', 'system', 'orchestration_progress', 'seed', 'corr_plan', 'caus_plan', 'prev_plan', 'entry_plan', '2026-03-01T12:00:00Z')`)

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)

	result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: "2026-03-01T12:01:10Z"})
	if err != nil {
		t.Fatalf("reconcile recent planning attempt: %v", err)
	}
	if result.StaleAttemptsFound != 0 {
		t.Fatalf("expected no stale attempts for recent planning run, got %d", result.StaleAttemptsFound)
	}
	if got := queryNodeStatus(t, db, "nr_plan_1"); got != "running" {
		t.Fatalf("expected planning node to remain running, got %q", got)
	}
}

func TestReconcilerPlanningNodesUseExtendedStaleGraceWindow(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `UPDATE node_runs SET node_status = 'succeeded', finished_at = '2026-03-01T12:00:00Z' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_planner_2', 'king', 'busy', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_plan_2', 'run_1', 'plan_drafting', 1,1, 'running', 'king', 'agent_planner_2', 1, '2026-03-01T12:00:00Z', NULL)`)
	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_plan_started_2', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_plan_2', 'run_1:plan_drafting:1', 'v1', 'attempt.started', 1, '{}', 'actor_1', 'king', 'system', 'orchestration_progress', 'seed', 'corr_plan_2', 'caus_plan_2', 'prev_plan_2', 'entry_plan_2', '2026-03-01T12:00:00Z')`)

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)

	result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: "2026-03-01T12:02:30Z"})
	if err != nil {
		t.Fatalf("reconcile long-running planning attempt: %v", err)
	}
	if result.StaleAttemptsFound != 0 {
		t.Fatalf("expected planning attempt to stay running under extended grace, got stale=%d", result.StaleAttemptsFound)
	}
	if got := queryNodeStatus(t, db, "nr_plan_2"); got != "running" {
		t.Fatalf("expected planning node to remain running, got %q", got)
	}
}

func TestExecutionKickoffIsNotClassifiedAsPlanningNode(t *testing.T) {
	t.Parallel()

	if isPlanningNodeID("execution_kickoff") {
		t.Fatal("expected execution_kickoff to stay out of planning stale-grace classification")
	}
}

func TestReconcilerSkipsStaleFailureWhenRecentProgressHeartbeatExists(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:13:00Z"
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T12:08:00Z' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'succeeded', finished_at = '2026-03-01T12:07:00Z' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_builder_1', 'run_1', 'decree_intake', 2,2, 'running', 'king', 'agent_king', 1, '2026-03-01T12:08:00Z', NULL)`)
	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_builder_started', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_builder_1', 'run_1:decree_intake:2', 'v1', 'attempt.started', 1, '{}', 'actor_1', 'king', 'system', 'orchestration_progress', 'seed', 'corr_builder', 'caus_builder', 'prev_builder', 'entry_builder', '2026-03-01T12:08:00Z')`)
	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_builder_progress_recent', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_builder_1', 'run_1:decree_intake:2', 'v1', 'attempt.progress', 2, '{"text":"provider produced progress output"}', 'actor_1', 'king', 'system', 'orchestration_progress', 'seed', 'corr_builder', 'caus_builder_progress', 'prev_builder_2', 'entry_builder_2', '2026-03-01T12:12:50Z')`)

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)

	result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now})
	if err != nil {
		t.Fatalf("reconcile with recent progress heartbeat: %v", err)
	}
	if result.StaleAttemptsFound != 0 {
		t.Fatalf("expected no stale attempts when recent progress heartbeat exists, got %d", result.StaleAttemptsFound)
	}
	if got := queryNodeStatus(t, db, "nr_builder_1"); got != "running" {
		t.Fatalf("expected attempt to remain running after recent progress heartbeat, got %q", got)
	}
}

func TestReconcilerIgnoresSyntheticRuntimeObservationHeartbeats(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:13:00Z"
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T12:08:00Z' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'succeeded', finished_at = '2026-03-01T12:07:00Z' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_builder_2', 'run_1', 'decree_intake', 2,2, 'running', 'king', 'agent_king', 1, '2026-03-01T12:08:00Z', NULL)`)
	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_builder_started_2', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_builder_2', 'run_1:decree_intake:2', 'v1', 'attempt.started', 1, '{}', 'actor_1', 'king', 'system', 'orchestration_progress', 'seed', 'corr_builder_2', 'caus_builder_2', 'prev_builder_2', 'entry_builder_2', '2026-03-01T12:08:00Z')`)
	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_builder_observe_recent', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_builder_2', 'run_1:decree_intake:2', 'v1', 'attempt.progress', 2, '{"execution_diagnostics":{"attempt_id":"run_1:decree_intake:2","event_count":0,"stream_error":"attempt not found: \"run_1:decree_intake:2\""}}', 'actor_1', 'king', 'system', 'runtime_observation', 'seed', 'corr_builder_2', 'caus_builder_2b', 'prev_builder_2b', 'entry_builder_2b', '2026-03-01T12:12:50Z')`)

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)

	result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now})
	if err != nil {
		t.Fatalf("reconcile with synthetic runtime observation heartbeat: %v", err)
	}
	if result.StaleAttemptsFound == 0 {
		t.Fatal("expected stale attempt detection to ignore synthetic runtime observation heartbeat")
	}
	if got := queryNodeStatus(t, db, "nr_builder_2"); got != "failed" {
		t.Fatalf("expected attempt to fail after stale detection when only synthetic heartbeat exists, got %q", got)
	}
}

func TestReconcilerTreatsLiveRuntimeObservationAsHeartbeat(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:13:00Z"
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T12:08:00Z' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'succeeded', finished_at = '2026-03-01T12:07:00Z' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_builder_live_observe', 'run_1', 'decree_intake', 2,2, 'running', 'king', 'agent_king', 1, '2026-03-01T12:08:00Z', NULL)`)
	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_builder_live_observe_started', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_builder_live_observe', 'run_1:decree_intake:2', 'v1', 'attempt.started', 1, '{}', 'actor_1', 'king', 'system', 'orchestration_progress', 'seed', 'corr_builder_live_observe', 'caus_builder_live_observe', 'prev_builder_live_observe', 'entry_builder_live_observe', '2026-03-01T12:08:00Z')`)
	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_builder_live_observe_recent', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_builder_live_observe', 'run_1:decree_intake:2', 'v1', 'attempt.progress', 2, '{"execution_diagnostics":{"attempt_id":"run_1:decree_intake:2","event_count":2,"execution_path_diagnostics_snapshot":{"stdout_event_count":17,"last_stdout_event_at":"2026-03-01T12:12:50Z","step_finish_seen":true,"last_step_finish_reason":"tool-calls","completion_gate_passed":false}}}', 'actor_1', 'king', 'system', 'runtime_observation', 'seed', 'corr_builder_live_observe', 'caus_builder_live_observe_recent', 'prev_builder_live_observe_2', 'entry_builder_live_observe_2', '2026-03-01T12:12:50Z')`)

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)

	result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now})
	if err != nil {
		t.Fatalf("reconcile with live runtime observation heartbeat: %v", err)
	}
	if result.StaleAttemptsFound != 0 {
		t.Fatalf("expected no stale attempts when runtime observation shows a live execution, got %d", result.StaleAttemptsFound)
	}
	if got := queryNodeStatus(t, db, "nr_builder_live_observe"); got != "running" {
		t.Fatalf("expected attempt to remain running when runtime observation shows a live execution, got %q", got)
	}
}

func TestReconcilerTreatsRecentProviderActivityAsHeartbeat(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:13:00Z"
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T12:08:00Z' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'succeeded', finished_at = '2026-03-01T12:07:00Z' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_builder_live', 'run_1', 'decree_intake', 2,2, 'running', 'king', 'agent_king', 1, '2026-03-01T12:08:00Z', NULL)`)
	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_builder_live_started', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_builder_live', 'run_1:decree_intake:2', 'v1', 'attempt.started', 1, '{}', 'actor_1', 'king', 'system', 'orchestration_progress', 'seed', 'corr_builder_live', 'caus_builder_live', 'prev_builder_live', 'entry_builder_live', '2026-03-01T12:08:00Z')`)
	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_builder_live_activity_recent', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_builder_live', 'run_1:decree_intake:2', 'v1', 'attempt.activity', 2, '{"kind":"kingdom.tool","label":"Processing"}', 'actor_1', 'king', 'runtime.orchestrator', 'provider_activity', 'provider activity telemetry', 'corr_builder_live', 'caus_builder_live_activity', 'prev_builder_live_2', 'entry_builder_live_2', '2026-03-01T12:12:50Z')`)

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)

	result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now})
	if err != nil {
		t.Fatalf("reconcile with live provider activity heartbeat: %v", err)
	}
	if result.StaleAttemptsFound != 0 {
		t.Fatalf("expected no stale attempts when provider activity is recent, got %d", result.StaleAttemptsFound)
	}
	if got := queryNodeStatus(t, db, "nr_builder_live"); got != "running" {
		t.Fatalf("expected attempt to remain running when provider activity is recent, got %q", got)
	}
}

func TestReconcilerTreatsRecentStreamProgressAsHeartbeat(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:13:00Z"
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T12:08:00Z' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'succeeded', finished_at = '2026-03-01T12:07:00Z' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_builder_quiet', 'run_1', 'decree_intake', 2,2, 'running', 'king', 'agent_king', 1, '2026-03-01T12:08:00Z', NULL)`)
	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_builder_quiet_started', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_builder_quiet', 'run_1:decree_intake:2', 'v1', 'attempt.started', 1, '{}', 'actor_1', 'king', 'system', 'orchestration_progress', 'seed', 'corr_builder_quiet', 'caus_builder_quiet', 'prev_builder_quiet', 'entry_builder_quiet', '2026-03-01T12:08:00Z')`)
	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_builder_quiet_progress_recent', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_builder_quiet', 'run_1:decree_intake:2', 'v1', 'attempt.progress', 2, '{"text":"still working"}', 'actor_1', 'king', 'runtime.event_bridge', 'stream_event', 'stream event ingested', 'corr_builder_quiet', 'caus_builder_quiet_progress', 'prev_builder_quiet_2', 'entry_builder_quiet_2', '2026-03-01T12:12:50Z')`)

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)

	result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now})
	if err != nil {
		t.Fatalf("reconcile with recent stream progress heartbeat: %v", err)
	}
	if result.StaleAttemptsFound != 0 {
		t.Fatalf("expected no stale attempts when stream progress is recent, got %d", result.StaleAttemptsFound)
	}
	if got := queryNodeStatus(t, db, "nr_builder_quiet"); got != "running" {
		t.Fatalf("expected attempt to remain running when stream progress is recent, got %q", got)
	}
}

func TestReconcilerTreatsRecentCompletionObservationAsHeartbeat(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:13:00Z"
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T12:08:00Z' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'succeeded', finished_at = '2026-03-01T12:07:00Z' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_builder_completion', 'run_1', 'decree_intake', 2,2, 'running', 'king', 'agent_king', 1, '2026-03-01T12:08:00Z', NULL)`)
	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_builder_completion_started', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_builder_completion', 'run_1:decree_intake:2', 'v1', 'attempt.started', 1, '{}', 'actor_1', 'king', 'system', 'orchestration_progress', 'seed', 'corr_builder_completion', 'caus_builder_completion_started', 'prev_builder_completion_started', 'entry_builder_completion_started', '2026-03-01T12:08:00Z')`)
	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_builder_completion_progress', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_builder_completion', 'run_1:decree_intake:2', 'v1', 'attempt.progress', 2, '{"execution_diagnostics":{"attempt_id":"run_1:decree_intake:2","event_count":2,"execution_path_diagnostics_snapshot":{"stdout_event_count":17,"last_stdout_event_at":"2026-03-01T12:12:50Z","step_finish_seen":true,"last_step_finish_reason":"tool-calls","completion_gate_passed":false}}}', 'actor_1', 'king', 'system', 'runtime_observation', 'seed', 'corr_builder_completion', 'caus_builder_completion_progress', 'prev_builder_completion_progress', 'entry_builder_completion_progress', '2026-03-01T12:12:50Z')`)
	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_builder_completion_observation', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_builder_completion', 'run_1:decree_intake:2', 'v1', 'attempt.progress', 3, '{"phase":"complete_execution_entered","attempt_id":"run_1:decree_intake:2","node_id":"decree_intake","node_run_id":"nr_builder_completion"}', 'actor_1', 'king', 'system', 'runtime_completion_observation', 'seed', 'corr_builder_completion', 'caus_builder_completion_observation', 'prev_builder_completion_observation', 'entry_builder_completion_observation', '2026-03-01T12:12:55Z')`)

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)

	result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now})
	if err != nil {
		t.Fatalf("reconcile with recent completion observation heartbeat: %v", err)
	}
	if result.StaleAttemptsFound != 0 {
		t.Fatalf("expected no stale attempts when completion observation follows recent execution activity, got %d", result.StaleAttemptsFound)
	}
	if got := queryNodeStatus(t, db, "nr_builder_completion"); got != "running" {
		t.Fatalf("expected attempt to remain running after recent completion observation, got %q", got)
	}
}

func TestReconcilerRecoverAttemptSkipsTerminalObservedExecutionState(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:13:00Z"
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T12:08:00Z' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'succeeded', finished_at = '2026-03-01T12:07:00Z' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_terminal_observed', 'run_1', 'decree_intake', 2,2, 'running', 'king', 'agent_king', 1, '2026-03-01T12:08:00Z', NULL)`)
	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_terminal_observed_started', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_terminal_observed', 'run_1:decree_intake:2', 'v1', 'attempt.started', 1, '{}', 'actor_1', 'king', 'system', 'orchestration_progress', 'seed', 'corr_terminal_observed', 'caus_terminal_observed_started', 'prev_terminal_observed_started', 'entry_terminal_observed_started', '2026-03-01T12:08:00Z')`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:decree_intake:2', 'run_1', 'nr_terminal_observed', 'terminal_observed', 'provider_backed', 'server_response', '', NULL, 1, 'session_run_1_decree_intake_2', '2026-03-01T12:08:00Z', '2026-03-01T12:12:59Z', NULL, '', '', '2026-03-01T12:08:00Z', '2026-03-01T12:12:59Z')`)

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)

	outcome, err := reconciler.recoverAttempt(context.Background(), staleAttempt{
		NodeRunID:     "nr_terminal_observed",
		RunID:         "run_1",
		NodeID:        "decree_intake",
		Attempt:       2,
		Wave:          2,
		RequiredRole:  "king",
		AgentID:       "agent_king",
		CurrentStatus: "running",
		StartedAt:     "2026-03-01T12:08:00Z",
	}, mustParseRFC3339(t, now))
	if err != nil {
		t.Fatalf("recover attempt with terminal_observed durable execution: %v", err)
	}
	if outcome.reassigned || outcome.failed || outcome.agentBlocked {
		t.Fatalf("expected terminal_observed durable execution to skip stale recovery, got %#v", outcome)
	}
	if got := queryNodeStatus(t, db, "nr_terminal_observed"); got != "running" {
		t.Fatalf("expected node status to remain running after terminal_observed skip, got %q", got)
	}
}

func TestReconcilerRunStartupUsesNowClock(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), nil, nil)
	reconciler.now = func() time.Time {
		return time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	}

	result, err := reconciler.RunStartup(context.Background())
	if err != nil {
		t.Fatalf("run startup reconcile: %v", err)
	}
	if result.StaleAttemptsFound != 0 {
		t.Fatalf("expected no stale attempts, got %d", result.StaleAttemptsFound)
	}
}

func TestReconcilerErrorsWhenRetryPlannerMissing(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:05:30Z"
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T11:59:00Z' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `UPDATE node_runs SET started_at = '2026-03-01T11:58:00Z', finished_at = NULL WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king_backup', 'king', 'idle', NULL, 0, '2026-03-01T11:58:00Z', '2026-03-01T11:58:00Z')`)
	mustExec(t, db, `INSERT INTO session_leases (session_id, agent_id, status, lease_expires_at, last_heartbeat_at, created_at, updated_at) VALUES ('session_busy', 'agent_king', 'active', '2026-03-01T11:58:30Z', '2026-03-01T11:58:00Z', '2026-03-01T11:58:00Z', '2026-03-01T11:58:00Z')`)

	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), nil, nil)
	_, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now})
	if err == nil {
		t.Fatal("expected reconcile to fail when a retry path needs an explicit retry planner")
	}
	if !strings.Contains(err.Error(), "retry planner is required") {
		t.Fatalf("expected explicit retry planner error, got %v", err)
	}
}

func TestNormalizeReconcileInputsAppliesDefaultThresholds(t *testing.T) {
	t.Parallel()

	nowTime, reassignThreshold, staleThreshold, err := normalizeReconcileInputs(ReconcileRequest{}, func() time.Time {
		return time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	})
	if err != nil {
		t.Fatalf("normalize reconcile inputs: %v", err)
	}
	if !nowTime.Equal(time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("unexpected normalized now: %s", nowTime)
	}
	if reassignThreshold != DefaultReassignCandidateThreshold {
		t.Fatalf("expected default reassign threshold %s, got %s", DefaultReassignCandidateThreshold, reassignThreshold)
	}
	if staleThreshold != DefaultStaleHeartbeatThreshold {
		t.Fatalf("expected default stale threshold %s, got %s", DefaultStaleHeartbeatThreshold, staleThreshold)
	}
}

func loadRetryEngine(t *testing.T) *retryengine.Engine {
	t.Helper()

	policy, err := retryengine.LoadPolicyFromFile(
		repoRootPath(t, "policies", "retry_policy.yaml"),
		repoRootPath(t, "contracts", "schemas", "retry_policy.schema.json"),
	)
	if err != nil {
		t.Fatalf("load retry policy: %v", err)
	}

	engine, err := retryengine.NewEngine(policy)
	if err != nil {
		t.Fatalf("build retry engine: %v", err)
	}

	return engine
}

func repoRootPath(t *testing.T, parts ...string) string {
	t.Helper()

	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}

	allParts := append([]string{root}, parts...)
	return filepath.Join(allParts...)
}

func queryNodeAssignedAgent(t *testing.T, db *sql.DB, nodeRunID string) string {
	t.Helper()

	var agentID string
	if err := db.QueryRow(`SELECT agent_id FROM node_runs WHERE node_run_id = ?`, nodeRunID).Scan(&agentID); err != nil {
		t.Fatalf("query assigned agent: %v", err)
	}
	return agentID
}

func querySessionStatus(t *testing.T, db *sql.DB, sessionID string) string {
	t.Helper()

	var status string
	if err := db.QueryRow(`SELECT status FROM session_leases WHERE session_id = ?`, sessionID).Scan(&status); err != nil {
		t.Fatalf("query session status: %v", err)
	}
	return status
}

func querySessionLeaseState(t *testing.T, db *sql.DB, sessionID string) (string, string, string) {
	t.Helper()

	var status string
	var heartbeat string
	var expiry string
	if err := db.QueryRow(`SELECT status, last_heartbeat_at, lease_expires_at FROM session_leases WHERE session_id = ?`, sessionID).Scan(&status, &heartbeat, &expiry); err != nil {
		t.Fatalf("query session lease state: %v", err)
	}

	return status, heartbeat, expiry
}

func queryNodeStartedAt(t *testing.T, db *sql.DB, nodeRunID string) string {
	t.Helper()

	var startedAt sql.NullString
	if err := db.QueryRow(`SELECT started_at FROM node_runs WHERE node_run_id = ?`, nodeRunID).Scan(&startedAt); err != nil {
		t.Fatalf("query node started_at: %v", err)
	}
	if !startedAt.Valid {
		return ""
	}
	return startedAt.String
}

func queryEventTypeCount(t *testing.T, db *sql.DB, runID, eventType string) int {
	t.Helper()

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE run_id = ? AND event_type = ?`, runID, eventType).Scan(&count); err != nil {
		t.Fatalf("query event count: %v", err)
	}
	return count
}

func queryLatestAttemptFailedPayload(t *testing.T, db *sql.DB, attemptID string) map[string]any {
	t.Helper()

	var payloadRaw string
	if err := db.QueryRow(`SELECT payload_json FROM events WHERE attempt_id = ? AND event_type = 'attempt.failed' ORDER BY seq DESC LIMIT 1`, attemptID).Scan(&payloadRaw); err != nil {
		t.Fatalf("query attempt.failed payload: %v", err)
	}

	payload := make(map[string]any)
	if err := json.Unmarshal([]byte(payloadRaw), &payload); err != nil {
		t.Fatalf("decode attempt.failed payload: %v", err)
	}
	return payload
}

func queryLatestEventPayloadForType(t *testing.T, db *sql.DB, runID, eventType string) map[string]any {
	t.Helper()

	var payloadRaw string
	if err := db.QueryRow(`SELECT payload_json FROM events WHERE run_id = ? AND event_type = ? ORDER BY run_event_seq DESC LIMIT 1`, runID, eventType).Scan(&payloadRaw); err != nil {
		t.Fatalf("query %s payload: %v", eventType, err)
	}

	payload := make(map[string]any)
	if err := json.Unmarshal([]byte(payloadRaw), &payload); err != nil {
		t.Fatalf("decode %s payload: %v", eventType, err)
	}
	return payload
}

func queryReconcilerAgentStatus(t *testing.T, db *sql.DB, agentID string) string {
	t.Helper()

	var status string
	if err := db.QueryRow(`SELECT status FROM agents WHERE agent_id = ?`, agentID).Scan(&status); err != nil {
		t.Fatalf("query agent status: %v", err)
	}
	return status
}
