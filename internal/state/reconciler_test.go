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

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)
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
	if got := queryNodeStatus(t, db, retryNodeRunID); got != "ready" {
		t.Fatalf("expected reassigned retry node to be ready for orchestrator dispatch, got %q", got)
	}
	if startedAt := queryNodeStartedAt(t, db, retryNodeRunID); startedAt != "" {
		t.Fatalf("expected retry node started_at to remain empty before dispatch, got %q", startedAt)
	}

	if got := queryNodeAssignedAgent(t, db, retryNodeRunID); got != "agent_king_backup" {
		t.Fatalf("unexpected retry assignment agent: got %q", got)
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

	if got := queryTransitionCount(t, db, "run_1"); got != 2 {
		t.Fatalf("expected two recovery transitions, got %d", got)
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
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:decree_intake:1', 'run_1', 'nr_1', 'committed_success', '', NULL, 1, 'session_run_1_decree_intake_1', '2026-03-01T11:58:00Z', '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z', '', '', '2026-03-01T11:58:00Z', '2026-03-01T12:00:00Z')`)

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
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_starting:advisor_consultation_noble:1', 'run_starting', 'nr_starting_1', 'starting', 'provider_backed', '', NULL, 0, 'session_starting_1', '', '2026-03-01T12:08:00Z', NULL, NULL, NULL, '', '', '2026-03-01T12:08:00Z', '2026-03-01T12:08:00Z')`)
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

func TestReconcilerKeepsStartingLeaseOpenWhileLaunchFenceIsStillValid(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	now := "2026-03-01T12:09:50Z"
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('noble-delegate', 'Noble Delegate', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_noble', 'noble-delegate', 'busy', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_starting_lease', 'starting lease', 'starting lease', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_starting_lease', 'starting-lease-flow', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_starting_lease', 'wfdef_starting_lease', 'advisor_consultation_noble', 'noble-delegate', 'advisor_consultation', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_starting_lease', NULL, 'decree_starting_lease', 'wfdef_starting_lease', 'running', 'approved', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_starting_lease_1', 'run_starting_lease', 'advisor_consultation_noble', 1,1, 'starting', 'noble-delegate', 'agent_noble', 1, NULL, NULL)`)
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_starting_lease:advisor_consultation_noble:1', 'run_starting_lease', 'nr_starting_lease_1', 'starting', 'provider_backed', '', NULL, 0, 'session_starting_lease_1', '', '2026-03-01T12:09:00Z', NULL, NULL, NULL, '', '', '2026-03-01T12:09:00Z', '2026-03-01T12:09:00Z')`)
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
	if expiry != "2026-03-01T12:10:25Z" {
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
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_starting_stale:advisor_consultation_noble:1', 'run_starting_stale', 'nr_starting_stale_1', 'starting', 'provider_backed', '', NULL, 0, 'session_starting_stale_1', '', '2026-03-01T12:08:00Z', NULL, NULL, NULL, '', '', '2026-03-01T12:08:00Z', '2026-03-01T12:08:00Z')`)
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
	if got := queryNodeStatus(t, db, "nr_starting_stale_1_retry_2"); got != "ready" {
		t.Fatalf("expected retry attempt to be ready after stale starting recovery, got %q", got)
	}
	if got := queryNodeAssignedAgent(t, db, "nr_starting_stale_1_retry_2"); got != "agent_noble_backup" {
		t.Fatalf("expected stale starting retry to move to backup agent, got %q", got)
	}
	payload := queryLatestEventPayloadForType(t, db, "run_starting_stale", "attempt.retry_queued")
	if got, _ := payload["node_id"].(string); got != "advisor_consultation_noble" {
		t.Fatalf("expected retry payload node_id, got %#v", payload)
	}
	if got, _ := payload["attempt_id"].(string); got != "run_starting_stale:advisor_consultation_noble:2" {
		t.Fatalf("expected retry payload attempt_id, got %#v", payload)
	}
	if got, _ := payload["required_role"].(string); got != "noble-delegate" {
		t.Fatalf("expected retry payload required_role, got %#v", payload)
	}
	if got, _ := payload["execution_mode"].(string); got != "provider_backed" {
		t.Fatalf("expected retry payload execution_mode, got %#v", payload)
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
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, lease_owner, lease_expires_at, lease_version, session_ref, provider_session_id, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_starting_expired:advisor_consultation_noble:1', 'run_starting_expired', 'nr_starting_expired_1', 'starting', 'provider_backed', '', NULL, 0, 'session_starting_expired_1', '', '2026-03-01T12:08:00Z', NULL, NULL, NULL, '', '', '2026-03-01T12:08:00Z', '2026-03-01T12:08:00Z')`)
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
	if got := queryNodeStatus(t, db, "nr_starting_expired_1_retry_2"); got != "ready" {
		t.Fatalf("expected retry attempt to be ready after lease-expiry recovery, got %q", got)
	}
}

func TestReconcilerFailsAndEscalatesWhenRetryBudgetExhausted(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:13:00Z"
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T12:08:00Z' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'succeeded', finished_at = '2026-03-01T12:07:00Z' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_2', 'run_1', 'decree_intake', 2,2, 'running', 'king', 'agent_king', 1, '2026-03-01T12:08:00Z', NULL)`)
	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_nr_2_started', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_2', 'att_2', 'v1', 'attempt.started', 1, '{}', 'actor_1', 'king', 'system', 'internal_error', 'seed', 'corr_nr2', 'caus_nr2', 'prev_nr2', 'entry_nr2', '2026-03-01T12:08:00Z')`)

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)

	result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now})
	if err != nil {
		t.Fatalf("reconcile stale exhausted attempt: %v", err)
	}

	if result.AttemptsReassigned != 0 {
		t.Fatalf("expected no reassignment, got %d", result.AttemptsReassigned)
	}
	if result.AttemptsFailed != 1 {
		t.Fatalf("expected one failed attempt recovery, got %d", result.AttemptsFailed)
	}

	if got := queryNodeStatus(t, db, "nr_2"); got != "failed" {
		t.Fatalf("expected stale exhausted attempt to fail, got %q", got)
	}

	if got := queryEscalationEventCount(t, db, "run_1", "agent_unresponsive"); got != 1 {
		t.Fatalf("expected one dead-letter escalation event, got %d", got)
	}
}

func TestReconcilerStaleFailureCommitsDurableFailureAndProviderSettlement(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:13:00Z"
	attemptID := composeAttemptID("run_1", "decree_intake", 2)
	store := executionstore.New(db)

	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T12:08:00Z' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'succeeded', finished_at = '2026-03-01T12:07:00Z' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_2', 'run_1', 'decree_intake', 2,2, 'running', 'king', 'agent_king', 1, '2026-03-01T12:08:00Z', NULL)`)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_nr_2_started_stale_settlement', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_2', '%s', 'v1', 'attempt.started', 1, '{}', 'actor_1', 'king', 'system', 'internal_error', 'seed', 'corr_nr2_stale_settlement', 'caus_nr2_stale_settlement', 'prev_nr2_stale_settlement', 'entry_nr2_stale_settlement', '2026-03-01T12:08:00Z')`, attemptID))

	if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:     attemptID,
		RunID:         "run_1",
		NodeRunID:     "nr_2",
		ExecutionMode: executionstore.ExecutionModeProviderBacked,
		SessionRef:    "session_reconcile_stale_failure",
		CreatedAt:     "2026-03-01T12:08:00Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	mustExec(t, db, fmt.Sprintf(`UPDATE attempt_executions SET state = 'running', started_at = '2026-03-01T12:08:00Z', updated_at = '2026-03-01T12:08:00Z' WHERE attempt_id = '%s'`, attemptID))
	if _, err := store.CreateProviderInvocation(context.Background(), executionstore.CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_stale_failure",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_2",
		LogicalSessionID:     "session_reconcile_stale_failure",
		CompletionCapability: "capability_stale_failure",
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

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)
	result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now})
	if err != nil {
		t.Fatalf("reconcile stale provider-backed attempt: %v", err)
	}
	if result.AttemptsFailed != 1 {
		t.Fatalf("expected one failed attempt recovery, got %d", result.AttemptsFailed)
	}
	if got := queryNodeStatus(t, db, "nr_2"); got != "failed" {
		t.Fatalf("expected stale attempt node to fail, got %q", got)
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
	if attempt.FailureCode != string(errorcatalog.CodeAgentUnresponsive) {
		t.Fatalf("expected durable failure code %q, got %#v", errorcatalog.CodeAgentUnresponsive, attempt)
	}
	if strings.TrimSpace(attempt.CanonicalSettlementID) == "" {
		t.Fatalf("expected canonical settlement id to be linked, got %#v", attempt)
	}

	invocation, found, err := store.GetProviderInvocation(context.Background(), "invocation_stale_failure")
	if err != nil {
		t.Fatalf("get provider invocation: %v", err)
	}
	if !found {
		t.Fatal("expected provider invocation to exist")
	}
	if invocation.InvocationState != executionstore.ProviderInvocationStateSettledFailed {
		t.Fatalf("expected settled_failed invocation state, got %#v", invocation)
	}

	settlement, found, err := store.GetProviderCompletionSettlementByInvocation(context.Background(), "invocation_stale_failure")
	if err != nil {
		t.Fatalf("get provider completion settlement by invocation: %v", err)
	}
	if !found {
		t.Fatal("expected provider completion settlement to exist")
	}
	if settlement.Outcome != executionstore.ProviderCompletionOutcomeFailed {
		t.Fatalf("expected failed provider settlement outcome, got %#v", settlement)
	}
	if settlement.FailureReasonCode != string(errorcatalog.CodeAgentUnresponsive) {
		t.Fatalf("expected provider settlement reason code %q, got %#v", errorcatalog.CodeAgentUnresponsive, settlement)
	}
}

func TestReconcilerRecoversReceiptPersistedProviderAttemptAsSuccess(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:13:00Z"
	attemptID := composeAttemptID("run_1", "decree_intake", 2)
	store := executionstore.New(db)

	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T12:08:00Z' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'succeeded', finished_at = '2026-03-01T12:07:00Z' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_2', 'run_1', 'decree_intake', 2,2, 'running', 'king', 'agent_king', 1, '2026-03-01T12:08:00Z', NULL)`)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_nr_2_started_receipt_recovery', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_2', '%s', 'v1', 'attempt.started', 1, '{}', 'actor_1', 'king', 'system', 'internal_error', 'seed', 'corr_nr2_receipt_recovery', 'caus_nr2_receipt_recovery', 'prev_nr2_receipt_recovery', 'entry_nr2_receipt_recovery', '2026-03-01T12:08:00Z')`, attemptID))

	if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:     attemptID,
		RunID:         "run_1",
		NodeRunID:     "nr_2",
		ExecutionMode: executionstore.ExecutionModeProviderBacked,
		SessionRef:    "session_reconcile_receipt_success",
		CreatedAt:     "2026-03-01T12:08:00Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	mustExec(t, db, fmt.Sprintf(`UPDATE attempt_executions SET state = 'running', started_at = '2026-03-01T12:08:00Z', updated_at = '2026-03-01T12:08:00Z' WHERE attempt_id = '%s'`, attemptID))
	if _, err := store.CreateProviderInvocation(context.Background(), executionstore.CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_stale_receipt_success",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_2",
		LogicalSessionID:     "session_reconcile_receipt_success",
		CompletionCapability: "capability_stale_receipt_success",
		IssuedAt:             "2026-03-01T12:08:01Z",
		CreatedAt:            "2026-03-01T12:08:01Z",
	}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := store.MarkProviderInvocationOpen(context.Background(), executionstore.MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "invocation_stale_receipt_success",
		AttemptID:            attemptID,
		ProviderSessionID:    "ses_stale_receipt_success",
		OpenedAt:             "2026-03-01T12:08:02Z",
	}); err != nil {
		t.Fatalf("mark provider invocation open: %v", err)
	}
	accepted, err := store.AcceptProviderCompletionReceipt(context.Background(), executionstore.AcceptProviderCompletionReceiptRequest{
		EvidenceID:            "receipt_stale_receipt_success",
		ProviderInvocationID:  "invocation_stale_receipt_success",
		AttemptID:             attemptID,
		RunID:                 "run_1",
		NodeRunID:             "nr_2",
		ProtocolVersion:       "2026-05-01",
		CompletionCapability:  "capability_stale_receipt_success",
		FinalText:             "Recovered provider-backed response.",
		StructuredPayloadJSON: `{}`,
		ReceivedAt:            "2026-03-01T12:08:03Z",
	})
	if err != nil {
		t.Fatalf("accept provider completion receipt: %v", err)
	}

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)
	result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now})
	if err != nil {
		t.Fatalf("reconcile receipt-persisted provider-backed attempt: %v", err)
	}
	if result.AttemptsFailed != 0 {
		t.Fatalf("expected no failed attempt recovery once receipt is persisted, got %d", result.AttemptsFailed)
	}
	if got := queryNodeStatus(t, db, "nr_2"); got != "succeeded" {
		t.Fatalf("expected receipt-backed stale attempt node to succeed, got %q", got)
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
	if strings.TrimSpace(attempt.CanonicalSettlementID) == "" {
		t.Fatalf("expected canonical settlement id to be linked, got %#v", attempt)
	}

	invocation, found, err := store.GetProviderInvocation(context.Background(), "invocation_stale_receipt_success")
	if err != nil {
		t.Fatalf("get provider invocation: %v", err)
	}
	if !found {
		t.Fatal("expected provider invocation to exist")
	}
	if invocation.InvocationState != executionstore.ProviderInvocationStateSettledSuccess {
		t.Fatalf("expected settled_success invocation state, got %#v", invocation)
	}

	settlement, found, err := store.GetProviderCompletionSettlementByInvocation(context.Background(), "invocation_stale_receipt_success")
	if err != nil {
		t.Fatalf("get provider completion settlement by invocation: %v", err)
	}
	if !found {
		t.Fatal("expected provider completion settlement to exist")
	}
	if settlement.Outcome != executionstore.ProviderCompletionOutcomeSuccess {
		t.Fatalf("expected success provider settlement outcome, got %#v", settlement)
	}
	if settlement.PrimaryEvidenceID != accepted.Evidence.EvidenceID {
		t.Fatalf("expected success provider settlement to reuse persisted receipt evidence %q, got %#v", accepted.Evidence.EvidenceID, settlement)
	}
}

func TestReconcilerImmediatelyConvergesRecentReceiptPersistedProviderAttempt(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:13:00Z"
	attemptID := composeAttemptID("run_1", "decree_intake", 2)
	store := executionstore.New(db)

	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T12:12:15Z' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'succeeded', finished_at = '2026-03-01T12:07:00Z' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_2_recent_receipt', 'run_1', 'decree_intake', 2,2, 'running', 'king', 'agent_king', 1, '2026-03-01T12:12:20Z', NULL)`)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_nr_2_started_recent_receipt', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_2_recent_receipt', '%s', 'v1', 'attempt.started', 1, '{}', 'actor_1', 'king', 'system', 'internal_error', 'seed', 'corr_nr2_recent_receipt', 'caus_nr2_recent_receipt', 'prev_nr2_recent_receipt', 'entry_nr2_recent_receipt', '2026-03-01T12:12:20Z')`, attemptID))

	if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:     attemptID,
		RunID:         "run_1",
		NodeRunID:     "nr_2_recent_receipt",
		ExecutionMode: executionstore.ExecutionModeProviderBacked,
		SessionRef:    "session_recent_receipt_success",
		CreatedAt:     "2026-03-01T12:12:20Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	mustExec(t, db, fmt.Sprintf(`UPDATE attempt_executions SET state = 'running', started_at = '2026-03-01T12:12:20Z', updated_at = '2026-03-01T12:12:20Z' WHERE attempt_id = '%s'`, attemptID))
	if _, err := store.CreateProviderInvocation(context.Background(), executionstore.CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_recent_receipt_success",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_2_recent_receipt",
		LogicalSessionID:     "session_recent_receipt_success",
		CompletionCapability: "capability_recent_receipt_success",
		IssuedAt:             "2026-03-01T12:12:21Z",
		CreatedAt:            "2026-03-01T12:12:21Z",
	}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := store.MarkProviderInvocationOpen(context.Background(), executionstore.MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "invocation_recent_receipt_success",
		AttemptID:            attemptID,
		ProviderSessionID:    "ses_recent_receipt_success",
		OpenedAt:             "2026-03-01T12:12:22Z",
	}); err != nil {
		t.Fatalf("mark provider invocation open: %v", err)
	}
	accepted, err := store.AcceptProviderCompletionReceipt(context.Background(), executionstore.AcceptProviderCompletionReceiptRequest{
		EvidenceID:            "receipt_recent_receipt_success",
		ProviderInvocationID:  "invocation_recent_receipt_success",
		AttemptID:             attemptID,
		RunID:                 "run_1",
		NodeRunID:             "nr_2_recent_receipt",
		ProtocolVersion:       "2026-05-01",
		CompletionCapability:  "capability_recent_receipt_success",
		FinalText:             "Recovered recent provider-backed response.",
		StructuredPayloadJSON: `{}`,
		ReceivedAt:            "2026-03-01T12:12:23Z",
	})
	if err != nil {
		t.Fatalf("accept provider completion receipt: %v", err)
	}

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)
	result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now})
	if err != nil {
		t.Fatalf("reconcile recent receipt-persisted provider-backed attempt: %v", err)
	}
	if result.StaleAttemptsFound != 0 {
		t.Fatalf("expected no stale attempts for recent receipt-backed recovery, got %d", result.StaleAttemptsFound)
	}
	if result.AttemptsFailed != 0 {
		t.Fatalf("expected no stale failure recovery for recent receipt-backed recovery, got %d", result.AttemptsFailed)
	}
	if got := queryNodeStatus(t, db, "nr_2_recent_receipt"); got != "succeeded" {
		t.Fatalf("expected recent receipt-backed attempt node to succeed, got %q", got)
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
	if strings.TrimSpace(attempt.CanonicalSettlementID) == "" {
		t.Fatalf("expected canonical settlement id to be linked, got %#v", attempt)
	}
	settlement, found, err := store.GetProviderCompletionSettlementByInvocation(context.Background(), "invocation_recent_receipt_success")
	if err != nil {
		t.Fatalf("get provider completion settlement by invocation: %v", err)
	}
	if !found {
		t.Fatal("expected provider completion settlement to exist")
	}
	if settlement.Outcome != executionstore.ProviderCompletionOutcomeSuccess {
		t.Fatalf("expected success provider settlement outcome, got %#v", settlement)
	}
	if settlement.PrimaryEvidenceID != accepted.Evidence.EvidenceID {
		t.Fatalf("expected success provider settlement to reuse persisted receipt evidence %q, got %#v", accepted.Evidence.EvidenceID, settlement)
	}
}

func TestReconcilerRecoversReceiptPersistedExecutionReviewReworkAsFailedSettlement(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:13:00Z"
	attemptID := composeAttemptID("run_1", "execution_review_shieldmaiden", 1)
	store := executionstore.New(db)

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Reviewer', 0, '2026-03-01T12:08:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reviewer', 'reviewer-shieldmaiden', 'busy', NULL, 0, '2026-03-01T12:08:00Z', '2026-03-01T12:08:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_review', 'wfdef_1', 'execution_review_shieldmaiden', 'reviewer-shieldmaiden', 'execution_review', '2026-03-01T12:08:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_review_receipt_rework', 'run_1', 'execution_review_shieldmaiden', 1, 1, 'running', 'reviewer-shieldmaiden', 'agent_reviewer', 1, '2026-03-01T12:08:00Z', NULL)`)
	mustExec(t, db, `INSERT INTO run_participants (run_id, agent_id, role_id, display_name, parent_agent_id, spawn_depth, first_seen_at) VALUES ('run_1', 'agent_reviewer', 'reviewer-shieldmaiden', 'Reviewer', NULL, 0, '2026-03-01T12:08:00Z')`)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_review_receipt_rework_started', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_review_receipt_rework', '%s', 'v1', 'attempt.started', 1, '{}', 'actor_1', 'reviewer-shieldmaiden', 'system', 'internal_error', 'seed', 'corr_review_receipt_rework', 'caus_review_receipt_rework', 'prev_review_receipt_rework', 'entry_review_receipt_rework', '2026-03-01T12:08:00Z')`, attemptID))

	if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:     attemptID,
		RunID:         "run_1",
		NodeRunID:     "nr_review_receipt_rework",
		ExecutionMode: executionstore.ExecutionModeProviderBacked,
		SessionRef:    "session_review_receipt_rework",
		CreatedAt:     "2026-03-01T12:08:00Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	mustExec(t, db, fmt.Sprintf(`UPDATE attempt_executions SET state = 'running', started_at = '2026-03-01T12:08:00Z', updated_at = '2026-03-01T12:08:00Z' WHERE attempt_id = '%s'`, attemptID))
	if _, err := store.CreateProviderInvocation(context.Background(), executionstore.CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_review_receipt_rework",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_review_receipt_rework",
		LogicalSessionID:     "session_review_receipt_rework",
		CompletionCapability: "capability_review_receipt_rework",
		IssuedAt:             "2026-03-01T12:08:01Z",
		CreatedAt:            "2026-03-01T12:08:01Z",
	}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := store.MarkProviderInvocationOpen(context.Background(), executionstore.MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "invocation_review_receipt_rework",
		AttemptID:            attemptID,
		ProviderSessionID:    "ses_review_receipt_rework",
		OpenedAt:             "2026-03-01T12:08:02Z",
	}); err != nil {
		t.Fatalf("mark provider invocation open: %v", err)
	}
	accepted, err := store.AcceptProviderCompletionReceipt(context.Background(), executionstore.AcceptProviderCompletionReceiptRequest{
		EvidenceID:            "receipt_review_receipt_rework",
		ProviderInvocationID:  "invocation_review_receipt_rework",
		AttemptID:             attemptID,
		RunID:                 "run_1",
		NodeRunID:             "nr_review_receipt_rework",
		ProtocolVersion:       "2026-05-01",
		CompletionCapability:  "capability_review_receipt_rework",
		FinalText:             "VERDICT: REWORK\n\nReviewer requested changes.",
		StructuredPayloadJSON: `{}`,
		ReceivedAt:            "2026-03-01T12:08:03Z",
	})
	if err != nil {
		t.Fatalf("accept provider completion receipt: %v", err)
	}

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)
	result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now})
	if err != nil {
		t.Fatalf("reconcile review receipt-persisted rework attempt: %v", err)
	}
	if result.AttemptsFailed != 0 {
		t.Fatalf("expected no stale failure recovery for review receipt rework convergence, got %d", result.AttemptsFailed)
	}
	if got := queryNodeStatus(t, db, "nr_review_receipt_rework"); got != "failed" {
		t.Fatalf("expected review receipt rework node to fail, got %q", got)
	}

	attempt, found, err := store.GetAttempt(context.Background(), attemptID)
	if err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if !found {
		t.Fatal("expected durable attempt execution to exist")
	}
	if attempt.State != "committed_failure" || attempt.FailureCode != "execution_review_rework" {
		t.Fatalf("expected committed_failure with execution_review_rework, got %#v", attempt)
	}
	settlement, found, err := store.GetProviderCompletionSettlementByInvocation(context.Background(), "invocation_review_receipt_rework")
	if err != nil {
		t.Fatalf("get provider completion settlement: %v", err)
	}
	if !found {
		t.Fatal("expected provider completion settlement to exist")
	}
	if settlement.Outcome != executionstore.ProviderCompletionOutcomeFailed {
		t.Fatalf("expected failed provider settlement outcome, got %#v", settlement)
	}
	if settlement.PrimaryEvidenceID != accepted.Evidence.EvidenceID {
		t.Fatalf("expected review rework to retain receipt evidence %q, got %#v", accepted.Evidence.EvidenceID, settlement)
	}
	if settlement.FailureReasonCode != "execution_review_rework" {
		t.Fatalf("expected execution_review_rework settlement reason, got %#v", settlement)
	}
}

func TestReconcilerCorrectsStoredExecutionReviewSuccessSettlementWithReworkReceipt(t *testing.T) {
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
		AttemptID:     attemptID,
		RunID:         "run_1",
		NodeRunID:     "nr_review_existing_settlement",
		ExecutionMode: executionstore.ExecutionModeProviderBacked,
		SessionRef:    "session_review_existing_settlement",
		CreatedAt:     "2026-03-01T12:08:00Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	mustExec(t, db, fmt.Sprintf(`UPDATE attempt_executions SET state = 'running', started_at = '2026-03-01T12:08:00Z', updated_at = '2026-03-01T12:08:00Z' WHERE attempt_id = '%s'`, attemptID))
	if _, err := store.CreateProviderInvocation(context.Background(), executionstore.CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_review_existing_settlement",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_review_existing_settlement",
		LogicalSessionID:     "session_review_existing_settlement",
		CompletionCapability: "capability_review_existing_settlement",
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
	accepted, err := store.AcceptProviderCompletionReceipt(context.Background(), executionstore.AcceptProviderCompletionReceiptRequest{
		EvidenceID:            "receipt_review_existing_settlement",
		ProviderInvocationID:  "invocation_review_existing_settlement",
		AttemptID:             attemptID,
		RunID:                 "run_1",
		NodeRunID:             "nr_review_existing_settlement",
		ProtocolVersion:       "2026-05-01",
		CompletionCapability:  "capability_review_existing_settlement",
		FinalText:             "VERDICT: REWORK\n\nReviewer requested changes.",
		StructuredPayloadJSON: `{}`,
		ReceivedAt:            "2026-03-01T12:08:03Z",
	})
	if err != nil {
		t.Fatalf("accept provider completion receipt: %v", err)
	}
	if _, err := store.FinalizeProviderCompletionSettlement(context.Background(), executionstore.FinalizeProviderCompletionSettlementRequest{
		SettlementID:          executionstore.ProviderCompletionSettlementID("invocation_review_existing_settlement"),
		ProviderInvocationID:  "invocation_review_existing_settlement",
		AttemptID:             attemptID,
		RunID:                 "run_1",
		NodeRunID:             "nr_review_existing_settlement",
		Outcome:               executionstore.ProviderCompletionOutcomeSuccess,
		PrimaryEvidenceID:     accepted.Evidence.EvidenceID,
		FinalText:             "VERDICT: REWORK\n\nReviewer requested changes.",
		StructuredPayloadJSON: `{}`,
		SettledAt:             "2026-03-01T12:08:04Z",
	}); err != nil {
		t.Fatalf("finalize incorrect success settlement: %v", err)
	}

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)
	result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now})
	if err != nil {
		t.Fatalf("reconcile stored review success settlement: %v", err)
	}
	if result.AttemptsFailed != 0 {
		t.Fatalf("expected no stale failure recovery while correcting stored settlement, got %d", result.AttemptsFailed)
	}
	if got := queryNodeStatus(t, db, "nr_review_existing_settlement"); got != "failed" {
		t.Fatalf("expected corrected review settlement node to fail, got %q", got)
	}

	attempt, found, err := store.GetAttempt(context.Background(), attemptID)
	if err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if !found {
		t.Fatal("expected durable attempt execution to exist")
	}
	if attempt.State != "committed_failure" || attempt.FailureCode != "execution_review_rework" {
		t.Fatalf("expected committed_failure with execution_review_rework, got %#v", attempt)
	}
	settlement, found, err := store.GetProviderCompletionSettlementByInvocation(context.Background(), "invocation_review_existing_settlement")
	if err != nil {
		t.Fatalf("get provider completion settlement: %v", err)
	}
	if !found {
		t.Fatal("expected provider completion settlement to exist")
	}
	if settlement.Outcome != executionstore.ProviderCompletionOutcomeFailed || settlement.FailureReasonCode != "execution_review_rework" {
		t.Fatalf("expected corrected failed review settlement, got %#v", settlement)
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
		AttemptID:     attemptID,
		RunID:         "run_1",
		NodeRunID:     "nr_2_recent_failure",
		ExecutionMode: executionstore.ExecutionModeProviderBacked,
		SessionRef:    "session_recent_settled_failure",
		CreatedAt:     "2026-03-01T12:12:20Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	mustExec(t, db, fmt.Sprintf(`UPDATE attempt_executions SET state = 'running', started_at = '2026-03-01T12:12:20Z', updated_at = '2026-03-01T12:12:20Z' WHERE attempt_id = '%s'`, attemptID))
	if _, err := store.CreateProviderInvocation(context.Background(), executionstore.CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_recent_settled_failure",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_2_recent_failure",
		LogicalSessionID:     "session_recent_settled_failure",
		CompletionCapability: "capability_recent_settled_failure",
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
	if _, err := store.FinalizeProviderCompletionSettlement(context.Background(), executionstore.FinalizeProviderCompletionSettlementRequest{
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
	}); err != nil {
		t.Fatalf("finalize provider completion settlement: %v", err)
	}

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

func TestReconcilerFinalizesCommittedSuccessProviderSettlementWithoutCanonicalLink(t *testing.T) {
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
		AttemptID:     attemptID,
		RunID:         "run_1",
		NodeRunID:     "nr_2",
		ExecutionMode: executionstore.ExecutionModeProviderBacked,
		SessionRef:    "session_reconcile_committed_success",
		CreatedAt:     "2026-03-01T12:08:00Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if _, err := store.CreateProviderInvocation(context.Background(), executionstore.CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_committed_success_backfill",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_2",
		LogicalSessionID:     "session_reconcile_committed_success",
		CompletionCapability: "capability_committed_success_backfill",
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
	accepted, err := store.AcceptProviderCompletionReceipt(context.Background(), executionstore.AcceptProviderCompletionReceiptRequest{
		EvidenceID:            "receipt_committed_success_backfill",
		ProviderInvocationID:  "invocation_committed_success_backfill",
		AttemptID:             attemptID,
		RunID:                 "run_1",
		NodeRunID:             "nr_2",
		ProtocolVersion:       "2026-05-01",
		CompletionCapability:  "capability_committed_success_backfill",
		FinalText:             "Recovered committed success response.",
		StructuredPayloadJSON: `{}`,
		ReceivedAt:            "2026-03-01T12:08:03Z",
	})
	if err != nil {
		t.Fatalf("accept provider completion receipt: %v", err)
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
	if strings.TrimSpace(attempt.CanonicalSettlementID) == "" {
		t.Fatalf("expected canonical settlement id to be linked, got %#v", attempt)
	}
	if got := queryNodeStatus(t, db, "nr_2"); got != "succeeded" {
		t.Fatalf("expected committed success node to project as succeeded, got %q", got)
	}

	settlement, found, err := store.GetProviderCompletionSettlementByInvocation(context.Background(), "invocation_committed_success_backfill")
	if err != nil {
		t.Fatalf("get provider completion settlement by invocation: %v", err)
	}
	if !found {
		t.Fatal("expected provider completion settlement to exist")
	}
	if settlement.Outcome != executionstore.ProviderCompletionOutcomeSuccess {
		t.Fatalf("expected success provider settlement outcome, got %#v", settlement)
	}
	if settlement.PrimaryEvidenceID != accepted.Evidence.EvidenceID {
		t.Fatalf("expected success provider settlement to reuse persisted receipt evidence %q, got %#v", accepted.Evidence.EvidenceID, settlement)
	}
}

func TestReconcilerRecoversInvalidHandshakeConflictAsFailedSettlement(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:13:00Z"
	attemptID := composeAttemptID("run_1", "decree_intake", 2)
	store := executionstore.New(db)

	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T12:08:00Z' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'succeeded', finished_at = '2026-03-01T12:07:00Z' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_2_invalid_handshake_conflict', 'run_1', 'decree_intake', 2,2, 'running', 'king', 'agent_king', 1, '2026-03-01T12:08:00Z', NULL)`)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_nr_2_invalid_handshake_conflict_started', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_2_invalid_handshake_conflict', '%s', 'v1', 'attempt.started', 1, '{}', 'actor_1', 'king', 'system', 'internal_error', 'seed', 'corr_nr2_invalid_handshake_conflict', 'caus_nr2_invalid_handshake_conflict', 'prev_nr2_invalid_handshake_conflict', 'entry_nr2_invalid_handshake_conflict', '2026-03-01T12:08:00Z')`, attemptID))

	if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:     attemptID,
		RunID:         "run_1",
		NodeRunID:     "nr_2_invalid_handshake_conflict",
		ExecutionMode: executionstore.ExecutionModeProviderBacked,
		SessionRef:    "session_invalid_handshake_conflict",
		CreatedAt:     "2026-03-01T12:08:00Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	mustExec(t, db, fmt.Sprintf(`UPDATE attempt_executions SET state = 'running', started_at = '2026-03-01T12:08:00Z', updated_at = '2026-03-01T12:08:00Z' WHERE attempt_id = '%s'`, attemptID))
	if _, err := store.CreateProviderInvocation(context.Background(), executionstore.CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_invalid_handshake_conflict",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_2_invalid_handshake_conflict",
		LogicalSessionID:     "session_invalid_handshake_conflict",
		CompletionCapability: "capability_invalid_handshake_conflict",
		IssuedAt:             "2026-03-01T12:08:01Z",
		CreatedAt:            "2026-03-01T12:08:01Z",
	}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := store.MarkProviderInvocationOpen(context.Background(), executionstore.MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "invocation_invalid_handshake_conflict",
		AttemptID:            attemptID,
		ProviderSessionID:    "ses_invalid_handshake_conflict",
		OpenedAt:             "2026-03-01T12:08:02Z",
	}); err != nil {
		t.Fatalf("mark provider invocation open: %v", err)
	}
	accepted, err := store.AcceptProviderCompletionReceipt(context.Background(), executionstore.AcceptProviderCompletionReceiptRequest{
		EvidenceID:            "receipt_invalid_handshake_conflict_1",
		ProviderInvocationID:  "invocation_invalid_handshake_conflict",
		AttemptID:             attemptID,
		RunID:                 "run_1",
		NodeRunID:             "nr_2_invalid_handshake_conflict",
		ProtocolVersion:       "v1",
		CompletionCapability:  "capability_invalid_handshake_conflict",
		FinalText:             "Recovered provider-backed response.",
		StructuredPayloadJSON: `{}`,
		ReceivedAt:            "2026-03-01T12:08:03Z",
	})
	if err != nil {
		t.Fatalf("accept provider completion receipt: %v", err)
	}
	if _, err := store.AcceptProviderCompletionReceipt(context.Background(), executionstore.AcceptProviderCompletionReceiptRequest{
		EvidenceID:            "receipt_invalid_handshake_conflict_2",
		ProviderInvocationID:  "invocation_invalid_handshake_conflict",
		AttemptID:             attemptID,
		RunID:                 "run_1",
		NodeRunID:             "nr_2_invalid_handshake_conflict",
		ProtocolVersion:       "v1",
		CompletionCapability:  "capability_invalid_handshake_conflict",
		FinalText:             "A conflicting settled answer.",
		StructuredPayloadJSON: `{}`,
		ReceivedAt:            "2026-03-01T12:08:04Z",
	}); err == nil {
		t.Fatal("expected conflicting receipt to invalidate the invocation")
	}

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)
	result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now})
	if err != nil {
		t.Fatalf("reconcile invalid-handshake conflict attempt: %v", err)
	}
	if result.AttemptsFailed != 0 {
		t.Fatalf("expected no stale failure recovery for invalid-handshake conflict convergence, got %d", result.AttemptsFailed)
	}
	if got := queryNodeStatus(t, db, "nr_2_invalid_handshake_conflict"); got != "failed" {
		t.Fatalf("expected invalid-handshake conflict node to fail, got %q", got)
	}

	attempt, found, err := store.GetAttempt(context.Background(), attemptID)
	if err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if !found {
		t.Fatal("expected durable attempt execution to exist")
	}
	if attempt.State != "committed_failure" || attempt.FailureCode != "provider_incomplete_turn" {
		t.Fatalf("expected committed_failure with provider_incomplete_turn, got %#v", attempt)
	}
	settlement, found, err := store.GetProviderCompletionSettlementByInvocation(context.Background(), "invocation_invalid_handshake_conflict")
	if err != nil {
		t.Fatalf("get provider completion settlement by invocation: %v", err)
	}
	if !found {
		t.Fatal("expected provider completion settlement to exist")
	}
	if settlement.Outcome != executionstore.ProviderCompletionOutcomeFailed || settlement.FailureReasonCode != "provider_incomplete_turn" {
		t.Fatalf("expected failed provider settlement with provider_incomplete_turn, got %#v", settlement)
	}
	if settlement.PrimaryEvidenceID == accepted.Evidence.EvidenceID {
		t.Fatalf("expected invalid-handshake settlement to use runtime terminal evidence instead of accepted receipt evidence, got %#v", settlement)
	}
	if !strings.Contains(settlement.FailureReasonText, "conflicting provider completion receipts") {
		t.Fatalf("expected invalid-handshake conflict failure text, got %#v", settlement)
	}
}

func TestReconcilerRecoversInvalidHandshakeCapabilityMismatchAsFailedSettlement(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:13:00Z"
	attemptID := composeAttemptID("run_1", "decree_intake", 2)
	store := executionstore.New(db)

	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T12:08:00Z' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'succeeded', finished_at = '2026-03-01T12:07:00Z' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_2_invalid_handshake_capability', 'run_1', 'decree_intake', 2,2, 'running', 'king', 'agent_king', 1, '2026-03-01T12:08:00Z', NULL)`)
	mustExec(t, db, fmt.Sprintf(`INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_nr_2_invalid_handshake_capability_started', 'run_1', COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = 'run_1'), 0) + 1, 'nr_2_invalid_handshake_capability', '%s', 'v1', 'attempt.started', 1, '{}', 'actor_1', 'king', 'system', 'internal_error', 'seed', 'corr_nr2_invalid_handshake_capability', 'caus_nr2_invalid_handshake_capability', 'prev_nr2_invalid_handshake_capability', 'entry_nr2_invalid_handshake_capability', '2026-03-01T12:08:00Z')`, attemptID))

	if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:     attemptID,
		RunID:         "run_1",
		NodeRunID:     "nr_2_invalid_handshake_capability",
		ExecutionMode: executionstore.ExecutionModeProviderBacked,
		SessionRef:    "session_invalid_handshake_capability",
		CreatedAt:     "2026-03-01T12:08:00Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	mustExec(t, db, fmt.Sprintf(`UPDATE attempt_executions SET state = 'running', started_at = '2026-03-01T12:08:00Z', updated_at = '2026-03-01T12:08:00Z' WHERE attempt_id = '%s'`, attemptID))
	if _, err := store.CreateProviderInvocation(context.Background(), executionstore.CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_invalid_handshake_capability",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_2_invalid_handshake_capability",
		LogicalSessionID:     "session_invalid_handshake_capability",
		CompletionCapability: "capability_invalid_handshake_capability",
		IssuedAt:             "2026-03-01T12:08:01Z",
		CreatedAt:            "2026-03-01T12:08:01Z",
	}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}
	if _, err := store.MarkProviderInvocationOpen(context.Background(), executionstore.MarkProviderInvocationOpenRequest{
		ProviderInvocationID: "invocation_invalid_handshake_capability",
		AttemptID:            attemptID,
		ProviderSessionID:    "ses_invalid_handshake_capability",
		OpenedAt:             "2026-03-01T12:08:02Z",
	}); err != nil {
		t.Fatalf("mark provider invocation open: %v", err)
	}
	if _, err := store.AcceptProviderCompletionReceipt(context.Background(), executionstore.AcceptProviderCompletionReceiptRequest{
		EvidenceID:            "receipt_invalid_handshake_capability",
		ProviderInvocationID:  "invocation_invalid_handshake_capability",
		AttemptID:             attemptID,
		RunID:                 "run_1",
		NodeRunID:             "nr_2_invalid_handshake_capability",
		ProtocolVersion:       "v1",
		CompletionCapability:  "wrong-capability",
		FinalText:             "Recovered provider-backed response.",
		StructuredPayloadJSON: `{}`,
		ReceivedAt:            "2026-03-01T12:08:03Z",
	}); err == nil {
		t.Fatal("expected capability mismatch to invalidate the invocation")
	}

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)
	result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now})
	if err != nil {
		t.Fatalf("reconcile invalid-handshake capability attempt: %v", err)
	}
	if result.AttemptsFailed != 0 {
		t.Fatalf("expected no stale failure recovery for invalid-handshake capability convergence, got %d", result.AttemptsFailed)
	}
	if got := queryNodeStatus(t, db, "nr_2_invalid_handshake_capability"); got != "failed" {
		t.Fatalf("expected invalid-handshake capability node to fail, got %q", got)
	}

	attempt, found, err := store.GetAttempt(context.Background(), attemptID)
	if err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if !found {
		t.Fatal("expected durable attempt execution to exist")
	}
	if attempt.State != "committed_failure" || attempt.FailureCode != "provider_incomplete_turn" {
		t.Fatalf("expected committed_failure with provider_incomplete_turn, got %#v", attempt)
	}
	settlement, found, err := store.GetProviderCompletionSettlementByInvocation(context.Background(), "invocation_invalid_handshake_capability")
	if err != nil {
		t.Fatalf("get provider completion settlement by invocation: %v", err)
	}
	if !found {
		t.Fatal("expected provider completion settlement to exist")
	}
	if settlement.Outcome != executionstore.ProviderCompletionOutcomeFailed || settlement.FailureReasonCode != "provider_incomplete_turn" {
		t.Fatalf("expected failed provider settlement with provider_incomplete_turn, got %#v", settlement)
	}
	if !strings.Contains(settlement.FailureReasonText, "completion capability mismatch") {
		t.Fatalf("expected capability mismatch failure text, got %#v", settlement)
	}
	if _, foundReceipt, err := store.GetProviderCompletionReceiptEvidence(context.Background(), "invocation_invalid_handshake_capability"); err != nil {
		t.Fatalf("get provider completion receipt evidence: %v", err)
	} else if foundReceipt {
		t.Fatal("expected no receipt evidence for capability mismatch invalid handshake")
	}
}

func TestReconcilerReusesExistingTerminalEvidenceWhenSettlementMissing(t *testing.T) {
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
		AttemptID:     attemptID,
		RunID:         "run_1",
		NodeRunID:     "nr_2",
		ExecutionMode: executionstore.ExecutionModeProviderBacked,
		SessionRef:    "session_reconcile_existing_evidence",
		CreatedAt:     "2026-03-01T12:08:00Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if _, err := store.CreateProviderInvocation(context.Background(), executionstore.CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_existing_evidence_backfill",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_2",
		LogicalSessionID:     "session_reconcile_existing_evidence",
		CompletionCapability: "capability_existing_evidence_backfill",
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
		"failure_text":    "provider exited without an accepted kingdom_complete_attempt receipt",
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
		    failure_text = 'provider exited without an accepted kingdom_complete_attempt receipt',
		    canonical_settlement_id = NULL,
		    updated_at = '2026-03-01T12:08:04Z'
		WHERE attempt_id = '%s'`, attemptID))

	engine := loadRetryEngine(t)
	reconciler := NewReconciler(db, NewSessionManager(db, NewSessionLifecycleMetrics()), engine, nil)
	if _, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: now}); err != nil {
		t.Fatalf("reconcile existing evidence backfill: %v", err)
	}

	attempt, found, err := store.GetAttempt(context.Background(), attemptID)
	if err != nil {
		t.Fatalf("get attempt: %v", err)
	}
	if !found {
		t.Fatal("expected durable attempt execution to exist")
	}
	if strings.TrimSpace(attempt.CanonicalSettlementID) == "" {
		t.Fatalf("expected canonical settlement id to be linked, got %#v", attempt)
	}
	settlement, found, err := store.GetProviderCompletionSettlementByInvocation(context.Background(), "invocation_existing_evidence_backfill")
	if err != nil {
		t.Fatalf("get provider completion settlement by invocation: %v", err)
	}
	if !found {
		t.Fatal("expected provider completion settlement to exist")
	}
	if settlement.Outcome != executionstore.ProviderCompletionOutcomeFailed {
		t.Fatalf("expected failed provider settlement outcome, got %#v", settlement)
	}
	if settlement.PrimaryEvidenceID != "evidence_terminal_invocation_existing_evidence_backfill" {
		t.Fatalf("expected reconcile to reuse the existing terminal evidence, got %#v", settlement)
	}
}

func TestReconcilerReconcilesExistingProviderSettlementToDurableAttemptTruth(t *testing.T) {
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
		AttemptID:     attemptID,
		RunID:         "run_1",
		NodeRunID:     "nr_2",
		ExecutionMode: executionstore.ExecutionModeProviderBacked,
		SessionRef:    "session_reconcile_settlement_mismatch",
		CreatedAt:     "2026-03-01T12:08:00Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if _, err := store.CreateProviderInvocation(context.Background(), executionstore.CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_settlement_mismatch",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_2",
		LogicalSessionID:     "session_reconcile_settlement_mismatch",
		CompletionCapability: "capability_settlement_mismatch",
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
	accepted, err := store.AcceptProviderCompletionReceipt(context.Background(), executionstore.AcceptProviderCompletionReceiptRequest{
		EvidenceID:            "receipt_settlement_mismatch",
		ProviderInvocationID:  "invocation_settlement_mismatch",
		AttemptID:             attemptID,
		RunID:                 "run_1",
		NodeRunID:             "nr_2",
		ProtocolVersion:       "2026-05-01",
		CompletionCapability:  "capability_settlement_mismatch",
		FinalText:             "Receipt-backed success that should be cleared.",
		StructuredPayloadJSON: `{}`,
		ReceivedAt:            "2026-03-01T12:08:03Z",
	})
	if err != nil {
		t.Fatalf("accept provider completion receipt: %v", err)
	}
	if _, err := store.FinalizeProviderCompletionSettlement(context.Background(), executionstore.FinalizeProviderCompletionSettlementRequest{
		SettlementID:          "settlement_invocation_settlement_mismatch",
		ProviderInvocationID:  "invocation_settlement_mismatch",
		AttemptID:             attemptID,
		RunID:                 "run_1",
		NodeRunID:             "nr_2",
		Outcome:               executionstore.ProviderCompletionOutcomeSuccess,
		PrimaryEvidenceID:     accepted.Evidence.EvidenceID,
		StructuredPayloadJSON: `{}`,
		SettledAt:             "2026-03-01T12:08:04Z",
	}); err != nil {
		t.Fatalf("finalize initial provider settlement: %v", err)
	}

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
	if strings.TrimSpace(attempt.CanonicalSettlementID) == "" {
		t.Fatalf("expected canonical settlement id to be linked, got %#v", attempt)
	}
	settlement, found, err := store.GetProviderCompletionSettlementByInvocation(context.Background(), "invocation_settlement_mismatch")
	if err != nil {
		t.Fatalf("get provider completion settlement by invocation: %v", err)
	}
	if !found {
		t.Fatal("expected provider completion settlement to exist")
	}
	if settlement.Outcome != executionstore.ProviderCompletionOutcomeCancelled {
		t.Fatalf("expected cancelled provider settlement outcome, got %#v", settlement)
	}
	if settlement.FailureReasonCode != "run_terminalized" {
		t.Fatalf("expected cancelled provider settlement reason code to reconcile, got %#v", settlement)
	}
	if settlement.FinalText != "" {
		t.Fatalf("expected cancelled provider settlement to clear stale success text, got %#v", settlement)
	}
}

func TestReconcilerReconcilesPinnedCanonicalSettlementMismatchToDurableAttemptTruth(t *testing.T) {
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
		AttemptID:     attemptID,
		RunID:         "run_1",
		NodeRunID:     "nr_2_pinned",
		ExecutionMode: executionstore.ExecutionModeProviderBacked,
		SessionRef:    "session_reconcile_settlement_pinned",
		CreatedAt:     "2026-03-01T12:08:00Z",
	}); err != nil {
		t.Fatalf("enqueue attempt: %v", err)
	}
	if _, err := store.CreateProviderInvocation(context.Background(), executionstore.CreateProviderInvocationRequest{
		ProviderInvocationID: "invocation_settlement_pinned",
		AttemptID:            attemptID,
		RunID:                "run_1",
		NodeRunID:            "nr_2_pinned",
		LogicalSessionID:     "session_reconcile_settlement_pinned",
		CompletionCapability: "capability_settlement_pinned",
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
	accepted, err := store.AcceptProviderCompletionReceipt(context.Background(), executionstore.AcceptProviderCompletionReceiptRequest{
		EvidenceID:            "receipt_settlement_pinned",
		ProviderInvocationID:  "invocation_settlement_pinned",
		AttemptID:             attemptID,
		RunID:                 "run_1",
		NodeRunID:             "nr_2_pinned",
		ProtocolVersion:       "2026-05-01",
		CompletionCapability:  "capability_settlement_pinned",
		FinalText:             "Receipt-backed success that should be replaced.",
		StructuredPayloadJSON: `{}`,
		ReceivedAt:            "2026-03-01T12:08:03Z",
	})
	if err != nil {
		t.Fatalf("accept provider completion receipt: %v", err)
	}
	if _, err := store.FinalizeProviderCompletionSettlement(context.Background(), executionstore.FinalizeProviderCompletionSettlementRequest{
		SettlementID:          "settlement_invocation_settlement_pinned",
		ProviderInvocationID:  "invocation_settlement_pinned",
		AttemptID:             attemptID,
		RunID:                 "run_1",
		NodeRunID:             "nr_2_pinned",
		Outcome:               executionstore.ProviderCompletionOutcomeSuccess,
		PrimaryEvidenceID:     accepted.Evidence.EvidenceID,
		StructuredPayloadJSON: `{}`,
		SettledAt:             "2026-03-01T12:08:04Z",
	}); err != nil {
		t.Fatalf("finalize initial provider settlement: %v", err)
	}

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
	if settlement.Outcome != executionstore.ProviderCompletionOutcomeCancelled {
		t.Fatalf("expected cancelled provider settlement outcome, got %#v", settlement)
	}
	if settlement.FailureReasonCode != "run_terminalized" {
		t.Fatalf("expected cancelled provider settlement reason code to reconcile, got %#v", settlement)
	}
	if settlement.FinalText != "" {
		t.Fatalf("expected cancelled provider settlement to clear stale success text, got %#v", settlement)
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
	if payload["retry_allowed"] != false {
		t.Fatalf("expected retry_allowed=false for exhausted retry budget, got %#v", payload["retry_allowed"])
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
	if got := queryNodeStatus(t, db, retryNodeRunID); got != "ready" {
		t.Fatalf("expected self-retry node to be ready for orchestrator dispatch, got %q", got)
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
	mustExec(t, db, `INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, lease_owner, lease_expires_at, lease_version, session_ref, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at) VALUES ('run_1:decree_intake:2', 'run_1', 'nr_terminal_observed', 'terminal_observed', '', NULL, 1, 'session_run_1_decree_intake_2', '2026-03-01T12:08:00Z', '2026-03-01T12:12:59Z', NULL, '', '', '2026-03-01T12:08:00Z', '2026-03-01T12:12:59Z')`)

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
