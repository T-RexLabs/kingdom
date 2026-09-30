package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func applyAgentRegistrySchema(t *testing.T, db *sql.DB) {
	t.Helper()
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
}

func TestAgentRegistrySpawnPersistsLineageAndDepth(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyAgentRegistrySchema(t, db)

	registry := NewAgentRegistry(db)
	err := registry.SpawnAgent(context.Background(), SpawnAgentRequest{
		AgentID:       "agent_council_1",
		RoleID:        "king",
		Status:        AgentStatusIdle,
		ParentAgentID: "agent_king",
		MaxSpawnDepth: 3,
		CreatedAt:     "2026-03-01T13:00:00Z",
		UpdatedAt:     "2026-03-01T13:00:00Z",
	})
	if err != nil {
		t.Fatalf("spawn agent: %v", err)
	}

	if got := queryAgentParentID(t, db, "agent_council_1"); got != "agent_king" {
		t.Fatalf("unexpected parent agent id, got %q", got)
	}

	if got := queryAgentDepth(t, db, "agent_council_1"); got != 1 {
		t.Fatalf("unexpected spawn depth, got %d", got)
	}
}

func TestAgentRegistrySpawnRejectsDepthCap(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyAgentRegistrySchema(t, db)

	registry := NewAgentRegistry(db)
	err := registry.SpawnAgent(context.Background(), SpawnAgentRequest{
		AgentID:       "agent_too_deep",
		RoleID:        "king",
		Status:        AgentStatusIdle,
		ParentAgentID: "agent_king",
		MaxSpawnDepth: 0,
		CreatedAt:     "2026-03-01T13:00:00Z",
		UpdatedAt:     "2026-03-01T13:00:00Z",
	})
	if err == nil {
		t.Fatal("expected spawn depth cap error")
	}

	var depthErr *SpawnDepthExceededError
	if !errors.As(err, &depthErr) {
		t.Fatalf("expected SpawnDepthExceededError, got %T: %v", err, err)
	}

	if got := queryAgentCount(t, db, "agent_too_deep"); got != 0 {
		t.Fatalf("expected no inserted child agent, got %d", got)
	}
}

func TestAgentRegistryReassignRecomputesDepthAndPreventsCycles(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyAgentRegistrySchema(t, db)

	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_chancellor', 'king', 'idle', 'agent_king', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council', 'king', 'idle', 'agent_chancellor', 2, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder', 'king', 'idle', 'agent_council', 3, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_peer', 'king', 'idle', 'agent_king', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)

	registry := NewAgentRegistry(db)
	if err := registry.ReassignAgent(context.Background(), ReassignAgentRequest{
		AgentID:          "agent_council",
		NewParentAgentID: "agent_peer",
		MaxSpawnDepth:    4,
		UpdatedAt:        "2026-03-01T13:30:00Z",
	}); err != nil {
		t.Fatalf("reassign agent: %v", err)
	}

	if got := queryAgentParentID(t, db, "agent_council"); got != "agent_peer" {
		t.Fatalf("expected council parent agent_peer, got %q", got)
	}

	if got := queryAgentDepth(t, db, "agent_council"); got != 2 {
		t.Fatalf("expected council depth 2, got %d", got)
	}

	if got := queryAgentDepth(t, db, "agent_builder"); got != 3 {
		t.Fatalf("expected builder depth 3 after subtree recompute, got %d", got)
	}

	err := registry.ReassignAgent(context.Background(), ReassignAgentRequest{
		AgentID:          "agent_king",
		NewParentAgentID: "agent_builder",
		MaxSpawnDepth:    5,
		UpdatedAt:        "2026-03-01T14:00:00Z",
	})
	if err == nil {
		t.Fatal("expected lineage cycle error")
	}

	var cycleErr *LineageCycleError
	if !errors.As(err, &cycleErr) {
		t.Fatalf("expected LineageCycleError, got %T: %v", err, err)
	}

	if got := queryAgentParentID(t, db, "agent_king"); got != "" {
		t.Fatalf("expected king parent to stay root, got %q", got)
	}
}

func TestAgentRegistryReassignRejectsDepthCapOverflow(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyAgentRegistrySchema(t, db)

	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_chancellor', 'king', 'idle', 'agent_king', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council', 'king', 'idle', 'agent_chancellor', 2, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder', 'king', 'idle', 'agent_council', 3, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_peer', 'king', 'idle', 'agent_king', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)

	registry := NewAgentRegistry(db)
	err := registry.ReassignAgent(context.Background(), ReassignAgentRequest{
		AgentID:          "agent_council",
		NewParentAgentID: "agent_peer",
		MaxSpawnDepth:    2,
		UpdatedAt:        "2026-03-01T14:00:00Z",
	})
	if err == nil {
		t.Fatal("expected SpawnDepthExceededError")
	}

	var depthErr *SpawnDepthExceededError
	if !errors.As(err, &depthErr) {
		t.Fatalf("expected SpawnDepthExceededError, got %T: %v", err, err)
	}
}

func TestAgentRegistryReassignRetriesSQLiteWriterContention(t *testing.T) {
	dbPath := filepath.ToSlash(filepath.Join(t.TempDir(), "agent-reassign.sqlite"))
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?_pragma=busy_timeout(1)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)", dbPath))
	if err != nil {
		t.Fatalf("open file-backed sqlite: %v", err)
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	t.Cleanup(func() { _ = db.Close() })
	applyAgentRegistrySchema(t, db)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES
		('agent_parent_contention', 'king', 'idle', 'agent_king', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z'),
		('agent_child_contention', 'king', 'idle', 'agent_king', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)

	blocker, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin SQLite writer blocker: %v", err)
	}
	if _, err := blocker.Exec(`UPDATE agents SET updated_at = '2026-03-01T12:00:01Z' WHERE agent_id = 'agent_king'`); err != nil {
		_ = blocker.Rollback()
		t.Fatalf("hold SQLite writer lock: %v", err)
	}
	reassigned := make(chan error, 1)
	go func() {
		reassigned <- NewAgentRegistry(db).ReassignAgent(context.Background(), ReassignAgentRequest{
			AgentID: "agent_child_contention", NewParentAgentID: "agent_parent_contention",
			MaxSpawnDepth: DefaultMaxSpawnDepth, UpdatedAt: "2026-03-01T12:00:02Z",
		})
	}()
	time.Sleep(75 * time.Millisecond)
	if err := blocker.Commit(); err != nil {
		t.Fatalf("release SQLite writer lock: %v", err)
	}
	select {
	case err := <-reassigned:
		if err != nil {
			t.Fatalf("reassign agent after transient SQLite contention: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reassign agent did not finish after the SQLite writer lock was released")
	}
	if got := queryAgentParentID(t, db, "agent_child_contention"); got != "agent_parent_contention" {
		t.Fatalf("reassigned agent parent=%q, want agent_parent_contention", got)
	}
}

func TestAgentRegistryStatusTransitions(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	registry := NewAgentRegistry(db)
	if err := registry.UpdateAgentStatus(context.Background(), UpdateAgentStatusRequest{
		AgentID:    "agent_king",
		FromStatus: AgentStatusIdle,
		ToStatus:   AgentStatusBusy,
		UpdatedAt:  "2026-03-01T14:00:00Z",
	}); err != nil {
		t.Fatalf("transition status idle->busy: %v", err)
	}

	if got := queryAgentStatus(t, db, "agent_king"); got != string(AgentStatusBusy) {
		t.Fatalf("expected busy status, got %q", got)
	}

	err := registry.UpdateAgentStatus(context.Background(), UpdateAgentStatusRequest{
		AgentID:    "agent_king",
		FromStatus: AgentStatusBusy,
		ToStatus:   AgentStatus("pending"),
		UpdatedAt:  "2026-03-01T14:01:00Z",
	})
	if err == nil {
		t.Fatal("expected illegal status transition")
	}

	var illegalErr *IllegalAgentStatusTransitionError
	if !errors.As(err, &illegalErr) {
		t.Fatalf("expected IllegalAgentStatusTransitionError, got %T: %v", err, err)
	}
}

func TestAgentRegistryCreateNodeAttemptRequiresResponsibleAgent(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	registry := NewAgentRegistry(db)
	err := registry.CreateNodeAttempt(context.Background(), CreateNodeAttemptRequest{
		NodeRunID:    "nr_2",
		RunID:        "run_1",
		NodeID:       "royal_closure",
		Attempt:      1,
		Wave:         1,
		NodeStatus:   "pending",
		RequiredRole: "king",
		AgentID:      "",
		RowVersion:   1,
	})
	if err == nil {
		t.Fatal("expected missing responsible agent error")
	}

	var missingErr *MissingResponsibleAgentError
	if !errors.As(err, &missingErr) {
		t.Fatalf("expected MissingResponsibleAgentError, got %T: %v", err, err)
	}

	if err := registry.CreateNodeAttempt(context.Background(), CreateNodeAttemptRequest{
		NodeRunID:    "nr_2",
		RunID:        "run_1",
		NodeID:       "royal_closure",
		Attempt:      1,
		Wave:         1,
		NodeStatus:   "pending",
		RequiredRole: "king",
		AgentID:      "agent_king",
		RowVersion:   1,
	}); err != nil {
		t.Fatalf("create node attempt with responsible agent: %v", err)
	}

	if got := queryNodeAgentID(t, db, "nr_2"); got != "agent_king" {
		t.Fatalf("expected node attempt agent assignment, got %q", got)
	}
}

func TestAgentRegistryListRunAgentsReturnsOnlyRunParticipantsOrdered(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_agents', 'Agents', 'list participants', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wf_agents', 'royal-decree-v1', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run_agents', NULL, 'decree_agents', 'wf_agents', 'running', 'approved', 0, 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '2026-03-01T12:00:00Z')`)

	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_root', 'king', 'The King', 'busy', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:01:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council', 'council-member', 'Councilor', 'idle', 'agent_root', 1, '2026-03-01T12:02:00Z', '2026-03-01T12:03:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder', 'council-member', 'Builder', 'busy', 'agent_council', 2, '2026-03-01T12:04:00Z', '2026-03-01T12:05:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_external', 'council-member', 'External', 'idle', NULL, 0, '2026-03-01T12:06:00Z', '2026-03-01T12:06:00Z')`)
	mustExec(t, db, `INSERT INTO run_participants (run_id, agent_id, role_id, display_name, parent_agent_id, spawn_depth, first_seen_at) VALUES ('run_agents', 'agent_root', 'king', 'The King', NULL, 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO run_participants (run_id, agent_id, role_id, display_name, parent_agent_id, spawn_depth, first_seen_at) VALUES ('run_agents', 'agent_council', 'council-member', 'Councilor', 'agent_root', 1, '2026-03-01T12:02:00Z')`)
	mustExec(t, db, `INSERT INTO run_participants (run_id, agent_id, role_id, display_name, parent_agent_id, spawn_depth, first_seen_at) VALUES ('run_agents', 'agent_builder', 'council-member', 'Builder', 'agent_council', 2, '2026-03-01T12:04:00Z')`)

	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_agents_1', 'run_agents', 'kickoff', 1,1, 'running', 'king', 'agent_root', 1, '2026-03-01T12:01:00Z', NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_agents_2', 'run_agents', 'implementation', 1,1, 'running', 'council-member', 'agent_builder', 1, '2026-03-01T12:05:00Z', NULL)`)

	mustExec(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at) VALUES ('msg_agents_1', 'run_agents', 'agent_root', 'agent_council', 'thread_agents', 'status_update', 'normal', '{}', '2026-03-01T12:06:00Z', NULL)`)

	registry := NewAgentRegistry(db)
	agents, err := registry.ListRunAgents(context.Background(), "run_agents")
	if err != nil {
		t.Fatalf("list run agents: %v", err)
	}

	if len(agents) != 3 {
		t.Fatalf("expected 3 participants, got %d", len(agents))
	}

	orderedIDs := []string{agents[0].AgentID, agents[1].AgentID, agents[2].AgentID}
	if !reflect.DeepEqual(orderedIDs, []string{"agent_root", "agent_council", "agent_builder"}) {
		t.Fatalf("unexpected run agent ordering: %v", orderedIDs)
	}

	for _, agent := range agents {
		if agent.AgentID == "agent_external" {
			t.Fatalf("unexpected non-participant agent in results")
		}
	}
}

func TestAgentRegistryCountRunAgentsCountsProjectedParticipants(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyAgentRegistrySchema(t, db)

	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_agents_count', 'Agents Count', 'count participants', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wf_agents_count', 'royal-decree-v1', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run_agents_count', NULL, 'decree_agents_count', 'wf_agents_count', 'running', 'approved', 0, 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '2026-03-01T12:00:00Z')`)

	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, spawn_depth, created_at, updated_at) VALUES ('agent_root_count', 'king', 'The King', 'busy', 0, '2026-03-01T12:00:00Z', '2026-03-01T12:01:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_builder_count', 'council-member', 'Builder', 'busy', 'agent_root_count', 1, '2026-03-01T12:02:00Z', '2026-03-01T12:03:00Z')`)
	mustExec(t, db, `INSERT INTO run_participants (run_id, agent_id, role_id, display_name, parent_agent_id, spawn_depth, first_seen_at) VALUES ('run_agents_count', 'agent_root_count', 'king', 'The King', NULL, 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO run_participants (run_id, agent_id, role_id, display_name, parent_agent_id, spawn_depth, first_seen_at) VALUES ('run_agents_count', 'agent_builder_count', 'council-member', 'Builder', 'agent_root_count', 1, '2026-03-01T12:02:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_agents_count', 'run_agents_count', 'implementation', 1,1, 'running', 'council-member', 'agent_builder_count', 1, '2026-03-01T12:05:00Z', NULL)`)
	mustExec(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at) VALUES ('msg_agents_count', 'run_agents_count', 'agent_root_count', 'agent_builder_count', 'thread_agents_count', 'status_update', 'normal', '{}', '2026-03-01T12:06:00Z', NULL)`)

	registry := NewAgentRegistry(db)
	count, err := registry.CountRunAgents(context.Background(), "run_agents_count")
	if err != nil {
		t.Fatalf("count run agents: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 projected participants, got %d", count)
	}
}

func TestAgentRegistryListRunAgentsUsesRunScopedFirstSeenAt(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_agents_first_seen', 'Agents First Seen', 'run chronology', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wf_agents_first_seen', 'royal-decree-v1', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run_agents_first_seen', NULL, 'decree_agents_first_seen', 'wf_agents_first_seen', 'running', 'approved', 0, 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_reused', 'council-member', 'Reused Agent', 'busy', NULL, 0, '2026-03-01T08:00:00Z', '2026-03-01T12:05:00Z')`)
	mustExec(t, db, `INSERT INTO run_participants (run_id, agent_id, role_id, display_name, parent_agent_id, spawn_depth, first_seen_at) VALUES ('run_agents_first_seen', 'agent_reused', 'council-member', 'Reused Agent', NULL, 0, '2026-03-01T12:03:00Z')`)

	registry := NewAgentRegistry(db)
	agents, err := registry.ListRunAgents(context.Background(), "run_agents_first_seen")
	if err != nil {
		t.Fatalf("list run agents: %v", err)
	}
	if len(agents) != 1 {
		t.Fatalf("expected one run participant, got %d", len(agents))
	}
	if agents[0].FirstSeenAt != "2026-03-01T12:03:00Z" {
		t.Fatalf("expected run-scoped first_seen_at, got %#v", agents[0].FirstSeenAt)
	}
}

func TestAgentRegistryListRunAgentsExcludesAllfather(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('allfather', 'Allfather', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, spawn_depth, created_at, updated_at) VALUES ('agent_allfather', 'allfather', 'Allfather', 'idle', 0, '2026-03-01T12:02:00Z', '2026-03-01T12:02:00Z')`)
	mustExec(t, db, `INSERT INTO run_participants (run_id, agent_id, role_id, display_name, parent_agent_id, spawn_depth, first_seen_at) VALUES ('run_1', 'agent_allfather', 'allfather', 'Allfather', NULL, 0, '2026-03-01T12:02:00Z')`)

	registry := NewAgentRegistry(db)
	agents, err := registry.ListRunAgents(context.Background(), "run_1")
	if err != nil {
		t.Fatalf("list run agents excluding allfather: %v", err)
	}

	if len(agents) != 1 {
		t.Fatalf("expected only runtime agents, got %d", len(agents))
	}
	if agents[0].RoleID != "king" {
		t.Fatalf("expected king to remain visible, got %q", agents[0].RoleID)
	}
}

func TestAgentRegistryListRunAgentsPreservesHistoricalExclusionWhenLeavingAllfather(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyAgentRegistrySchema(t, db)

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('allfather', 'Allfather', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_role_transition_add', 'Role Transition Add', 'projection rebuild', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wf_role_transition_add', 'royal-decree-v1', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run_role_transition_add', NULL, 'decree_role_transition_add', 'wf_role_transition_add', 'running', 'approved', 0, 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, spawn_depth, created_at, updated_at) VALUES ('agent_transition_add', 'allfather', 'Transition Agent', 'idle', 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO run_participants (run_id, agent_id, role_id, display_name, parent_agent_id, spawn_depth, first_seen_at) VALUES ('run_role_transition_add', 'agent_transition_add', 'allfather', 'Transition Agent', NULL, 0, '2026-03-01T12:01:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_role_transition_add', 'run_role_transition_add', 'implementation', 1,1, 'running', 'council-member', 'agent_transition_add', 1, '2026-03-01T12:01:00Z', NULL)`)
	mustExec(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at) VALUES ('msg_role_transition_add', 'run_role_transition_add', 'agent_transition_add', 'agent_transition_add', 'thread_role_transition_add', 'status_update', 'normal', '{}', '2026-03-01T12:02:00Z', NULL)`)

	registry := NewAgentRegistry(db)
	if agents, err := registry.ListRunAgents(context.Background(), "run_role_transition_add"); err != nil {
		t.Fatalf("list run agents before role transition: %v", err)
	} else if len(agents) != 0 {
		t.Fatalf("expected no participants before leaving allfather, got %d", len(agents))
	}
	if count, err := registry.CountRunAgents(context.Background(), "run_role_transition_add"); err != nil {
		t.Fatalf("count run agents before role transition: %v", err)
	} else if count != 0 {
		t.Fatalf("expected projected participant count 0 before leaving allfather, got %d", count)
	}

	mustExec(t, db, `UPDATE agents SET role_id = 'council-member', updated_at = '2026-03-01T12:03:00Z' WHERE agent_id = 'agent_transition_add'`)

	agents, err := registry.ListRunAgents(context.Background(), "run_role_transition_add")
	if err != nil {
		t.Fatalf("list run agents after role transition: %v", err)
	}
	if len(agents) != 0 {
		t.Fatalf("expected historical allfather exclusion to remain stable, got %#v", agents)
	}
	if count, err := registry.CountRunAgents(context.Background(), "run_role_transition_add"); err != nil {
		t.Fatalf("count run agents after role transition: %v", err)
	} else if count != 0 {
		t.Fatalf("expected projected participant count 0 after leaving allfather, got %d", count)
	}
}

func TestAgentRegistryListRunAgentsPreservesHistoricalMembershipWhenEnteringAllfather(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyAgentRegistrySchema(t, db)

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('allfather', 'Allfather', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_role_transition_remove', 'Role Transition Remove', 'projection prune', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wf_role_transition_remove', 'royal-decree-v1', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run_role_transition_remove', NULL, 'decree_role_transition_remove', 'wf_role_transition_remove', 'running', 'approved', 0, 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, spawn_depth, created_at, updated_at) VALUES ('agent_transition_remove', 'council-member', 'Transition Agent', 'idle', 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO run_participants (run_id, agent_id, role_id, display_name, parent_agent_id, spawn_depth, first_seen_at) VALUES ('run_role_transition_remove', 'agent_transition_remove', 'council-member', 'Transition Agent', NULL, 0, '2026-03-01T12:01:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_role_transition_remove', 'run_role_transition_remove', 'implementation', 1,1, 'running', 'council-member', 'agent_transition_remove', 1, '2026-03-01T12:01:00Z', NULL)`)
	mustExec(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at) VALUES ('msg_role_transition_remove', 'run_role_transition_remove', 'agent_transition_remove', 'agent_transition_remove', 'thread_role_transition_remove', 'status_update', 'normal', '{}', '2026-03-01T12:02:00Z', NULL)`)

	registry := NewAgentRegistry(db)
	if agents, err := registry.ListRunAgents(context.Background(), "run_role_transition_remove"); err != nil {
		t.Fatalf("list run agents before entering allfather: %v", err)
	} else if len(agents) != 1 || agents[0].AgentID != "agent_transition_remove" {
		t.Fatalf("expected one participant before entering allfather, got %#v", agents)
	}
	if count, err := registry.CountRunAgents(context.Background(), "run_role_transition_remove"); err != nil {
		t.Fatalf("count run agents before entering allfather: %v", err)
	} else if count != 1 {
		t.Fatalf("expected projected participant count 1 before entering allfather, got %d", count)
	}

	mustExec(t, db, `UPDATE agents SET role_id = 'allfather', updated_at = '2026-03-01T12:03:00Z' WHERE agent_id = 'agent_transition_remove'`)

	agents, err := registry.ListRunAgents(context.Background(), "run_role_transition_remove")
	if err != nil {
		t.Fatalf("list run agents after entering allfather: %v", err)
	}
	if len(agents) != 1 || agents[0].AgentID != "agent_transition_remove" {
		t.Fatalf("expected historical participant to remain visible, got %#v", agents)
	}
	if count, err := registry.CountRunAgents(context.Background(), "run_role_transition_remove"); err != nil {
		t.Fatalf("count run agents after entering allfather: %v", err)
	} else if count != 1 {
		t.Fatalf("expected projected participant count 1 after entering allfather, got %d", count)
	}
}

func TestAgentRegistryListRunAgentsDerivesBusyStatusFromRunningNodes(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_agents_status', 'Agents Status', 'status derivation', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wf_agents_status', 'royal-decree-v1', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run_agents_status', NULL, 'decree_agents_status', 'wf_agents_status', 'running', 'approved', 0, 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '2026-03-01T12:00:00Z')`)

	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_root_status', 'king', 'Root', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_qa_status', 'council-member', 'QA Scout', 'blocked', 'agent_root_status', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO run_participants (run_id, agent_id, role_id, display_name, parent_agent_id, spawn_depth, first_seen_at) VALUES ('run_agents_status', 'agent_root_status', 'king', 'Root', NULL, 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO run_participants (run_id, agent_id, role_id, display_name, parent_agent_id, spawn_depth, first_seen_at) VALUES ('run_agents_status', 'agent_qa_status', 'council-member', 'QA Scout', 'agent_root_status', 1, '2026-03-01T12:01:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_agents_status_1', 'run_agents_status', 'validation_gates_qa_scout', 1,1, 'running', 'council-member', 'agent_qa_status', 1, '2026-03-01T12:01:00Z', NULL)`)

	registry := NewAgentRegistry(db)
	agents, err := registry.ListRunAgents(context.Background(), "run_agents_status")
	if err != nil {
		t.Fatalf("list run agents with derived busy status: %v", err)
	}

	if len(agents) != 2 {
		t.Fatalf("expected 2 participants, got %d", len(agents))
	}

	for _, agent := range agents {
		if agent.AgentID == "agent_qa_status" && agent.Status != "busy" {
			t.Fatalf("expected running participant to derive busy status, got %q", agent.Status)
		}
	}
}

func TestAgentRegistryListRunAgentsPreservesCanonicalOfflineAndFailedStatuses(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_agents_unavailable', 'Agents Unavailable', 'status derivation', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wf_agents_unavailable', 'royal-decree-v1', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run_agents_unavailable', NULL, 'decree_agents_unavailable', 'wf_agents_unavailable', 'running', 'approved', 0, 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '2026-03-01T12:00:00Z')`)

	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, spawn_depth, created_at, updated_at) VALUES ('agent_offline_status', 'council-member', 'Offline Scout', 'offline', 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, spawn_depth, created_at, updated_at) VALUES ('agent_failed_status', 'council-member', 'Failed Scout', 'failed', 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO run_participants (run_id, agent_id, role_id, display_name, parent_agent_id, spawn_depth, first_seen_at) VALUES ('run_agents_unavailable', 'agent_offline_status', 'council-member', 'Offline Scout', NULL, 0, '2026-03-01T12:01:00Z')`)
	mustExec(t, db, `INSERT INTO run_participants (run_id, agent_id, role_id, display_name, parent_agent_id, spawn_depth, first_seen_at) VALUES ('run_agents_unavailable', 'agent_failed_status', 'council-member', 'Failed Scout', NULL, 0, '2026-03-01T12:02:00Z')`)

	registry := NewAgentRegistry(db)
	agents, err := registry.ListRunAgents(context.Background(), "run_agents_unavailable")
	if err != nil {
		t.Fatalf("list run agents with unavailable statuses: %v", err)
	}
	if len(agents) != 2 {
		t.Fatalf("expected 2 participants, got %d", len(agents))
	}

	statuses := map[string]AgentStatus{}
	for _, agent := range agents {
		statuses[agent.AgentID] = agent.Status
	}
	if statuses["agent_offline_status"] != AgentStatusOffline {
		t.Fatalf("expected offline participant status, got %q", statuses["agent_offline_status"])
	}
	if statuses["agent_failed_status"] != AgentStatusFailed {
		t.Fatalf("expected failed participant status, got %q", statuses["agent_failed_status"])
	}
}

func TestAgentRegistryListRunAgentsDoesNotLeakGlobalBusyAcrossRuns(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_agents_busy_scope', 'Agents Busy Scope', 'status derivation', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wf_agents_busy_scope', 'royal-decree-v1', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run_agents_busy_scope', NULL, 'decree_agents_busy_scope', 'wf_agents_busy_scope', 'running', 'approved', 0, 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '2026-03-01T12:00:00Z')`)

	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, spawn_depth, created_at, updated_at) VALUES ('agent_busy_elsewhere', 'council-member', 'Busy Elsewhere', 'busy', 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO run_participants (run_id, agent_id, role_id, display_name, parent_agent_id, spawn_depth, first_seen_at) VALUES ('run_agents_busy_scope', 'agent_busy_elsewhere', 'council-member', 'Busy Elsewhere', NULL, 0, '2026-03-01T12:01:00Z')`)

	registry := NewAgentRegistry(db)
	agents, err := registry.ListRunAgents(context.Background(), "run_agents_busy_scope")
	if err != nil {
		t.Fatalf("list run agents with scoped busy status: %v", err)
	}
	if len(agents) != 1 {
		t.Fatalf("expected 1 participant, got %d", len(agents))
	}
	if agents[0].Status != AgentStatusIdle {
		t.Fatalf("expected idle run-scoped status, got %q", agents[0].Status)
	}
}

func TestAgentRegistryListRunAgentsDerivesFailedStatusFromFailedNodes(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_agents_failed_node', 'Agents Failed Node', 'status derivation', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wf_agents_failed_node', 'royal-decree-v1', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run_agents_failed_node', NULL, 'decree_agents_failed_node', 'wf_agents_failed_node', 'running', 'approved', 0, 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '2026-03-01T12:00:00Z')`)

	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, spawn_depth, created_at, updated_at) VALUES ('agent_failed_node', 'council-member', 'Failed Node Agent', 'idle', 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO run_participants (run_id, agent_id, role_id, display_name, parent_agent_id, spawn_depth, first_seen_at) VALUES ('run_agents_failed_node', 'agent_failed_node', 'council-member', 'Failed Node Agent', NULL, 0, '2026-03-01T12:01:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_agents_failed_node_1', 'run_agents_failed_node', 'validation_gates_qa_scout', 1,1, 'failed', 'council-member', 'agent_failed_node', 1, '2026-03-01T12:01:00Z', '2026-03-01T12:02:00Z')`)

	registry := NewAgentRegistry(db)
	agents, err := registry.ListRunAgents(context.Background(), "run_agents_failed_node")
	if err != nil {
		t.Fatalf("list run agents with failed node status: %v", err)
	}
	if len(agents) != 1 {
		t.Fatalf("expected 1 participant, got %d", len(agents))
	}
	if agents[0].Status != AgentStatusFailed {
		t.Fatalf("expected failed participant status, got %q", agents[0].Status)
	}
}

func TestAgentRegistryListAllAgentsReturnsEveryAgent(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	// seedMinimumGraph already creates role 'king' and agent 'agent_king'.
	// Add a second role and a couple more agents.
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_chancellor', 'chancellor', 'The Chancellor', 'idle', NULL, 0, '2026-03-01T12:01:00Z', '2026-03-01T12:01:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king_deputy', 'king', 'King Deputy', 'busy', 'agent_king', 1, '2026-03-01T12:02:00Z', '2026-03-01T12:02:00Z')`)

	registry := NewAgentRegistry(db)
	agents, err := registry.ListAllAgents(context.Background())
	if err != nil {
		t.Fatalf("list all agents: %v", err)
	}

	// Should return all 3 agents regardless of run participation.
	if len(agents) != 3 {
		t.Fatalf("expected 3 agents, got %d", len(agents))
	}

	// Ordered by spawn_depth ASC, then created_at ASC.
	// depth 0: agent_king (12:00), agent_chancellor (12:01)
	// depth 1: agent_king_deputy (12:02)
	if agents[0].AgentID != "agent_king" {
		t.Errorf("agents[0] expected agent_king, got %s", agents[0].AgentID)
	}
	if agents[1].AgentID != "agent_chancellor" {
		t.Errorf("agents[1] expected agent_chancellor, got %s", agents[1].AgentID)
	}
	if agents[2].AgentID != "agent_king_deputy" {
		t.Errorf("agents[2] expected agent_king_deputy, got %s", agents[2].AgentID)
	}

	// Verify fields.
	if agents[2].SpawnDepth != 1 {
		t.Errorf("expected spawn depth 1, got %d", agents[2].SpawnDepth)
	}
	if string(agents[2].Status) != "busy" {
		t.Errorf("expected busy status, got %s", agents[2].Status)
	}
	if !agents[2].ParentAgentID.Valid || agents[2].ParentAgentID.String != "agent_king" {
		t.Errorf("expected parent agent_king, got %#v", agents[2].ParentAgentID)
	}
}

func TestAgentRegistryListAllAgentsExcludesAllfather(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('allfather', 'Allfather', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, spawn_depth, created_at, updated_at) VALUES ('agent_allfather', 'allfather', 'Allfather', 'idle', 0, '2026-03-01T12:01:00Z', '2026-03-01T12:01:00Z')`)

	registry := NewAgentRegistry(db)
	agents, err := registry.ListAllAgents(context.Background())
	if err != nil {
		t.Fatalf("list all agents excluding allfather: %v", err)
	}

	if len(agents) != 1 {
		t.Fatalf("expected only runtime agents, got %d", len(agents))
	}
	if agents[0].RoleID != "king" {
		t.Fatalf("expected king to remain visible, got %q", agents[0].RoleID)
	}
}

func TestAgentRegistryListAllAgentsReturnsEmptySliceWhenNone(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	// Insert a role but no agents.
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '2026-03-01T12:00:00Z')`)

	registry := NewAgentRegistry(db)
	agents, err := registry.ListAllAgents(context.Background())
	if err != nil {
		t.Fatalf("list all agents: %v", err)
	}
	if len(agents) != 0 {
		t.Fatalf("expected 0 agents, got %d", len(agents))
	}
}

func queryAgentParentID(t *testing.T, db *sql.DB, agentID string) string {
	t.Helper()

	var parent sql.NullString
	if err := db.QueryRow(`SELECT parent_agent_id FROM agents WHERE agent_id = ?`, agentID).Scan(&parent); err != nil {
		t.Fatalf("query parent agent id: %v", err)
	}
	return parent.String
}

func queryAgentDepth(t *testing.T, db *sql.DB, agentID string) int {
	t.Helper()

	var depth int
	if err := db.QueryRow(`SELECT spawn_depth FROM agents WHERE agent_id = ?`, agentID).Scan(&depth); err != nil {
		t.Fatalf("query agent depth: %v", err)
	}
	return depth
}

func queryAgentCount(t *testing.T, db *sql.DB, agentID string) int {
	t.Helper()

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM agents WHERE agent_id = ?`, agentID).Scan(&count); err != nil {
		t.Fatalf("query agent count: %v", err)
	}
	return count
}

func queryAgentStatus(t *testing.T, db *sql.DB, agentID string) string {
	t.Helper()

	var status string
	if err := db.QueryRow(`SELECT status FROM agents WHERE agent_id = ?`, agentID).Scan(&status); err != nil {
		t.Fatalf("query agent status: %v", err)
	}
	return status
}

func queryNodeAgentID(t *testing.T, db *sql.DB, nodeRunID string) string {
	t.Helper()

	var agentID string
	if err := db.QueryRow(`SELECT agent_id FROM node_runs WHERE node_run_id = ?`, nodeRunID).Scan(&agentID); err != nil {
		t.Fatalf("query node attempt agent_id: %v", err)
	}
	return agentID
}

// Regression for the King-stuck-busy bug: once an agent has no active node_runs, a
// status reconcile must move it back to idle. The orchestrator now calls this on every
// completion (no longer gated on a committed transition), so runtime-owned agents like
// the King don't linger "busy".
func TestAgentRegistrySyncOperationalStatusReconcilesStaleBusyToIdle(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)

	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_sync', 'Sync', 'sync', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wf_sync', 'royal-decree-v1', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, rework_cycle_count, row_version, created_at, updated_at) VALUES ('run_sync', NULL, 'decree_sync', 'wf_sync', 'running', 'approved', 0, 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king_sync', 'king', 'King', 'busy', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	// King's only node_run has already completed (not active).
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_king_sync', 'run_sync', 'king_plan_decision', 1, 1, 'succeeded', 'king', 'agent_king_sync', 1, '2026-03-01T12:00:30Z', '2026-03-01T12:01:00Z')`)

	registry := NewAgentRegistry(db)
	if err := registry.SyncOperationalStatus(context.Background(), "agent_king_sync", "2026-03-01T12:01:30Z"); err != nil {
		t.Fatalf("sync operational status: %v", err)
	}

	var status string
	if err := db.QueryRow(`SELECT status FROM agents WHERE agent_id = 'agent_king_sync'`).Scan(&status); err != nil {
		t.Fatalf("read agent status: %v", err)
	}
	if status != "idle" {
		t.Fatalf("expected king to reconcile to idle, got %q", status)
	}
}
