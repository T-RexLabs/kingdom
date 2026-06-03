package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"kingdom/internal/core/retryengine"
	"kingdom/internal/core/schemas"
	"kingdom/internal/core/workflow"
	"kingdom/internal/dbmigrations"
	kingdom "kingdom/internal/orchestration"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/state"
	"kingdom/internal/testutil"

	_ "modernc.org/sqlite"
)

func TestIntegrationTransitionAtomicRollbackOnEventConflict(t *testing.T) {
	t.Parallel()

	db := openDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	svc := state.NewTransitionService(db, loadEventSchemas(t))
	err := svc.Apply(context.Background(), state.TransitionRequest{
		TransitionID:        "tr_conflict_event",
		EventID:             "evt_1",
		RunID:               "run_1",
		NodeRunID:           sql.NullString{String: "nr_1", Valid: true},
		Entity:              state.EntityTypeNode,
		FromState:           "running",
		ToState:             "failed",
		ActorID:             "reconciler",
		RoleID:              "king",
		AuthoritySource:     "system",
		ReasonCode:          "internal_error",
		ReasonText:          "simulate event conflict",
		CorrelationID:       "corr_h2_atomicity",
		CausationID:         "caus_h2_atomicity",
		OccurredAt:          "2026-03-01T12:30:00Z",
		TransitionPrevHash:  "prev_transition",
		TransitionEntryHash: "entry_transition",
		EventPrevHash:       "prev_event",
		EventEntryHash:      "entry_event",
		AttemptID:           "run_1:decree_intake:1",
		SchemaVersion:       "v1",
		EventType:           "attempt.failed",
		EventSeq:            2,
		PayloadJSON:         `{"reason":"forced_conflict"}`,
	})
	if err == nil {
		t.Fatal("expected unique event conflict")
	}
	if !strings.Contains(err.Error(), "UNIQUE") {
		t.Fatalf("expected UNIQUE constraint failure, got %v", err)
	}

	if got := queryString(t, db, `SELECT node_status FROM node_runs WHERE node_run_id = ?`, "nr_1"); got != "running" {
		t.Fatalf("expected node status rollback to running, got %q", got)
	}
	if got := queryInt(t, db, `SELECT COUNT(1) FROM transitions WHERE transition_id = ?`, "tr_conflict_event"); got != 0 {
		t.Fatalf("expected transition row rollback, got %d", got)
	}
}

func TestIntegrationIdempotencyCreateLookupConflict(t *testing.T) {
	t.Parallel()

	db := openDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	store := state.NewIdempotencyStore(db)
	base := state.IdempotencyRecord{
		ID:              "idem_h2_a",
		Route:           "/api/v1/runs",
		ActorID:         "actor_h2",
		ClientKey:       "client_key_h2",
		RequestHash:     "hash_a",
		ResponseStatus:  202,
		ResponseHeaders: `{"Content-Type":"application/json"}`,
		ResponseBody:    `{"run_id":"run_h2"}`,
		CreatedAt:       "2026-03-01T12:00:00Z",
	}
	if err := store.Create(context.Background(), base); err != nil {
		t.Fatalf("create idempotency record: %v", err)
	}

	got, found, err := store.Lookup(context.Background(), base.Route, base.ActorID, base.ClientKey)
	if err != nil {
		t.Fatalf("lookup idempotency record: %v", err)
	}
	if !found {
		t.Fatal("expected created idempotency record to be found")
	}
	if got.ResponseStatus != base.ResponseStatus || got.ResponseBody != base.ResponseBody {
		t.Fatalf("unexpected replay payload: status=%d body=%s", got.ResponseStatus, got.ResponseBody)
	}

	conflict := base
	conflict.ID = "idem_h2_b"
	conflict.RequestHash = "hash_b"
	err = store.Create(context.Background(), conflict)
	if err == nil {
		t.Fatal("expected idempotency scope conflict")
	}
	var conflictErr *state.IdempotencyConflictError
	if !errors.As(err, &conflictErr) {
		t.Fatalf("expected IdempotencyConflictError, got %T: %v", err, err)
	}
}

func TestIntegrationReconcilerReassignsStaleAttemptAndReconcilesLeases(t *testing.T) {
	t.Parallel()

	db := openDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:05:30Z"
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T11:59:00Z' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `UPDATE node_runs SET started_at = '2026-03-01T11:58:00Z', finished_at = NULL WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king_backup', 'king', 'idle', NULL, 0, '2026-03-01T11:58:00Z', '2026-03-01T11:58:00Z')`)
	mustExec(t, db, `INSERT INTO session_leases (session_id, agent_id, status, lease_expires_at, last_heartbeat_at, created_at, updated_at) VALUES ('session_busy', 'agent_king', 'active', '2026-03-01T11:58:30Z', '2026-03-01T11:58:00Z', '2026-03-01T11:58:00Z', '2026-03-01T11:58:00Z')`)
	mustExec(t, db, `INSERT INTO session_leases (session_id, agent_id, status, lease_expires_at, last_heartbeat_at, created_at, updated_at) VALUES ('session_idle', 'agent_king_backup', 'active', '2026-03-01T11:58:30Z', '2026-03-01T11:58:00Z', '2026-03-01T11:58:00Z', '2026-03-01T11:58:00Z')`)

	engine := loadRetryEngine(t)
	reconciler := state.NewReconciler(db, state.NewSessionManager(db, state.NewSessionLifecycleMetrics()), engine, loadEventSchemas(t))

	result, err := reconciler.Reconcile(context.Background(), state.ReconcileRequest{Now: now})
	if err != nil {
		t.Fatalf("reconcile stale attempt: %v", err)
	}

	if result.StaleAttemptsFound != 1 || result.AttemptsReassigned != 1 || result.SessionsClosed != 2 || result.SessionsRenewed != 0 {
		t.Fatalf("unexpected reconcile result: %+v", result)
	}

	if got := queryString(t, db, `SELECT node_status FROM node_runs WHERE node_run_id = ?`, "nr_1"); got != "blocked" {
		t.Fatalf("expected stale node blocked, got %q", got)
	}
	if got := queryString(t, db, `SELECT node_status FROM node_runs WHERE node_run_id = ?`, "nr_1_retry_2"); got != "ready" {
		t.Fatalf("expected reassigned node ready for orchestrator dispatch, got %q", got)
	}
	if got := queryNullableString(t, db, `SELECT started_at FROM node_runs WHERE node_run_id = ?`, "nr_1_retry_2"); got != "" {
		t.Fatalf("expected reassigned node started_at empty before dispatch, got %q", got)
	}
	if got := queryString(t, db, `SELECT agent_id FROM node_runs WHERE node_run_id = ?`, "nr_1_retry_2"); got != "agent_king_backup" {
		t.Fatalf("expected reassigned node agent_king_backup, got %q", got)
	}
	if got := queryString(t, db, `SELECT status FROM session_leases WHERE session_id = ?`, "session_busy"); got != "closed" {
		t.Fatalf("expected busy session closed, got %q", got)
	}
	if got := queryString(t, db, `SELECT status FROM session_leases WHERE session_id = ?`, "session_idle"); got != "closed" {
		t.Fatalf("expected idle session without an active attempt to close, got %q", got)
	}
}

func TestIntegrationSchedulerDispatchOrderDeterministicAcrossInputPermutations(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	scheduler := workflow.NewScheduler(workflow.SchedulerConfig{QueueAgingBoost: 24 * time.Hour})

	base := []workflow.ReadyNode{
		{RunID: "run-b", NodeID: "node-b", Attempt: 1, RunPriority: 3, CriticalPath: false, ReadyAt: now.Add(-2 * time.Minute)},
		{RunID: "run-a", NodeID: "node-c", Attempt: 1, RunPriority: 3, CriticalPath: true, ReadyAt: now.Add(-90 * time.Second)},
		{RunID: "run-a", NodeID: "node-a", Attempt: 2, RunPriority: 3, CriticalPath: true, ReadyAt: now.Add(-90 * time.Second)},
		{RunID: "run-a", NodeID: "node-a", Attempt: 1, RunPriority: 3, CriticalPath: true, ReadyAt: now.Add(-90 * time.Second)},
		{RunID: "run-z", NodeID: "node-z", Attempt: 1, RunPriority: 4, CriticalPath: false, ReadyAt: now.Add(-30 * time.Second)},
	}

	want := scheduler.DispatchOrder(now, base)
	wantIDs := dispatchIDs(want)

	for i := range base {
		rotated := rotateReadyNodes(base, i)
		got := scheduler.DispatchOrder(now, rotated)
		gotIDs := dispatchIDs(got)
		if !reflect.DeepEqual(gotIDs, wantIDs) {
			t.Fatalf("dispatch order changed for rotation=%d\nwant=%v\ngot=%v", i, wantIDs, gotIDs)
		}
	}
}

func TestIntegrationReconcilerFailurePathPersistsEscalationAuditEvent(t *testing.T) {
	t.Parallel()

	db := openDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	now := "2026-03-01T12:03:00Z"
	mustExec(t, db, `UPDATE agents SET status = 'busy', updated_at = '2026-03-01T11:59:00Z' WHERE agent_id = 'agent_king'`)
	mustExec(t, db, `UPDATE node_runs SET attempt = 2 WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `UPDATE node_runs SET started_at = '2026-03-01T11:58:00Z', finished_at = NULL WHERE node_run_id = 'nr_1'`)

	engine := loadRetryEngine(t)
	reconciler := state.NewReconciler(db, state.NewSessionManager(db, state.NewSessionLifecycleMetrics()), engine, loadEventSchemas(t))

	result, err := reconciler.Reconcile(context.Background(), state.ReconcileRequest{Now: now})
	if err != nil {
		t.Fatalf("reconcile failure path: %v", err)
	}
	if result.AttemptsFailed != 1 {
		t.Fatalf("expected one failed attempt recovery, got %+v", result)
	}

	if got := queryInt(t, db, `SELECT COUNT(1) FROM events WHERE run_id = ? AND event_type = 'escalation.raised' AND reason_code = 'agent_unresponsive'`, "run_1"); got != 1 {
		t.Fatalf("expected one escalation event, got %d", got)
	}
	if got := queryString(t, db, `SELECT authority_source FROM events WHERE run_id = ? AND event_type = 'escalation.raised' ORDER BY created_at DESC, event_id DESC LIMIT 1`, "run_1"); got != "system.reconciler" {
		t.Fatalf("expected reconciler authority source, got %q", got)
	}
}

func TestIntegrationRuntimeDispatchPersistsWorkspaceAndArtifacts(t *testing.T) {
	t.Parallel()

	db := openDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedDispatchGraph(t, db)

	rt, err := kingdom.NewRuntime(kingdom.Config{
		DB:               db,
		Paths:            kingdom.DefaultBootstrapPaths(repoRoot(t)),
		Adapter:          opencode.NewMemoryAdapter(),
		ProviderEngine:   "opencode",
		DispatchInterval: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := rt.Start(ctx); err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer func() {
		if err := rt.Close(); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
	}()

	testutil.Eventually(t, 3*time.Second, 25*time.Millisecond, func() error {
		if got := queryString(t, db, `SELECT run_status FROM workflow_runs WHERE run_id = ?`, "run_dispatch"); got == "succeeded" {
			if artifacts := queryInt(t, db, `SELECT COUNT(1) FROM artifacts WHERE run_id = ?`, "run_dispatch"); artifacts < 2 {
				return fmt.Errorf("expected persisted artifacts for dispatch run, got %d", artifacts)
			}
			if released := queryInt(t, db, `SELECT COUNT(1) FROM workspaces WHERE run_id = ? AND status = 'released'`, "run_dispatch"); released < 1 {
				return fmt.Errorf("expected released workspace records for provider-backed nodes, got %d", released)
			}
			return nil
		}
		return fmt.Errorf("run_dispatch not yet succeeded")
	})
}

func openDB(t *testing.T) *sql.DB {
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

func applyMigration(t *testing.T, db *sql.DB, fileName string) {
	t.Helper()

	names, err := dbmigrations.List()
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	apply := false
	for _, name := range names {
		if name == fileName {
			apply = true
		}
		if !apply {
			continue
		}
		raw, readErr := dbmigrations.Read(name)
		if readErr != nil {
			t.Fatalf("read migration %s: %v", name, readErr)
		}
		if _, execErr := db.Exec(string(raw)); execErr != nil {
			t.Fatalf("apply migration %s: %v", name, execErr)
		}
	}
	if !apply {
		t.Fatalf("migration %s not found", fileName)
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
	mustExec(t, db, `INSERT INTO events (event_id, run_id, run_event_seq, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, prev_hash, entry_hash, created_at) VALUES ('evt_1', 'run_1', 1, 'nr_1', 'run_1:decree_intake:1', 'v1', 'attempt.started', 1, '{}', 'actor_1', 'king', 'system', 'internal_error', 'seed', 'corr_1', 'caus_1', 'p', 'e', '2026-03-01T12:00:00Z')`)
}

func seedDispatchGraph(t *testing.T, db *sql.DB) {
	t.Helper()

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_chancellor', 'chancellor', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_dispatch', 'dispatch', 'dispatch run', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_dispatch', 'dispatch-flow', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_dispatch_1', 'wfdef_dispatch', 'decree_intake', 'king', 'decree_intake', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_dispatch_2', 'wfdef_dispatch', 'plan_drafting', 'council-member', 'plan_drafting', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_edges (workflow_edge_id, workflow_definition_id, from_node_id, to_node_id, on_condition, created_at) VALUES ('edge_dispatch_1', 'wfdef_dispatch', 'decree_intake', 'plan_drafting', 'always', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_dispatch', NULL, 'decree_dispatch', 'wfdef_dispatch', 'pending', 'draft', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
}

func loadRetryEngine(t *testing.T) *retryengine.Engine {
	t.Helper()

	policyPath := filepath.Join(repoRoot(t), "policies", "retry_policy.yaml")
	schemaPath := filepath.Join(repoRoot(t), "contracts", "schemas", "retry_policy.schema.json")
	policy, err := retryengine.LoadPolicyFromFile(policyPath, schemaPath)
	if err != nil {
		t.Fatalf("load retry policy: %v", err)
	}

	engine, err := retryengine.NewEngine(policy)
	if err != nil {
		t.Fatalf("build retry engine: %v", err)
	}
	return engine
}

func loadEventSchemas(t *testing.T) *schemas.Set {
	t.Helper()

	set, err := schemas.Load(filepath.Join(repoRoot(t), "contracts", "schemas"))
	if err != nil {
		t.Fatalf("load event schemas: %v", err)
	}
	return set
}

func repoRoot(t *testing.T) string {
	t.Helper()

	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}
	return root
}

func dispatchIDs(nodes []workflow.ReadyNode) []string {
	ids := make([]string, 0, len(nodes))
	for _, node := range nodes {
		ids = append(ids, node.RunID+"/"+node.NodeID+"/"+strconv.Itoa(node.Attempt))
	}
	return ids
}

func rotateReadyNodes(nodes []workflow.ReadyNode, offset int) []workflow.ReadyNode {
	if len(nodes) == 0 {
		return nil
	}
	offset = offset % len(nodes)
	rotated := make([]workflow.ReadyNode, 0, len(nodes))
	rotated = append(rotated, nodes[offset:]...)
	rotated = append(rotated, nodes[:offset]...)
	return rotated
}

func queryString(t *testing.T, db *sql.DB, query string, args ...any) string {
	t.Helper()

	var value string
	if err := db.QueryRow(query, args...).Scan(&value); err != nil {
		t.Fatalf("query string failed: %v", err)
	}
	return value
}

func queryNullableString(t *testing.T, db *sql.DB, query string, args ...any) string {
	t.Helper()

	var value sql.NullString
	if err := db.QueryRow(query, args...).Scan(&value); err != nil {
		t.Fatalf("query nullable string failed: %v", err)
	}
	if !value.Valid {
		return ""
	}
	return value.String
}

func queryInt(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()

	var value int
	if err := db.QueryRow(query, args...).Scan(&value); err != nil {
		t.Fatalf("query int failed: %v", err)
	}
	return value
}

func mustExec(t *testing.T, db *sql.DB, query string) {
	t.Helper()

	if _, err := db.Exec(query); err != nil {
		t.Fatalf("exec failed: %v\nquery: %s", err, query)
	}
}
