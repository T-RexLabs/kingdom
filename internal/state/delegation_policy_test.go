package state

import (
	"context"
	"database/sql"
	"testing"
)

func TestDelegationPolicyEngineAllowsLockedSpawnMatrixWithRoleMatch(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedDelegationPolicyFixture(t, db)

	engine := NewDelegationPolicyEngine(db, NewAgentRegistry(db))
	decision, err := engine.Delegate(context.Background(), DelegationRequest{
		RunID:            "run_1",
		NodeID:           "advisory_synthesis",
		DelegatorAgentID: "agent_king",
		DelegateAgentID:  "agent_chancellor_2",
		DelegateRoleID:   "chancellor",
		MaxSpawnDepth:    3,
		MaxActiveAgents:  12,
		OccurredAt:       "2026-03-01T15:00:00Z",
	})
	if err != nil {
		t.Fatalf("delegate: %v", err)
	}

	if decision.Outcome != DelegationOutcomeSuccess {
		t.Fatalf("expected success, got %+v", decision)
	}

	if got := queryAgentParentID(t, db, "agent_chancellor_2"); got != "agent_king" {
		t.Fatalf("expected delegated parent agent_king, got %q", got)
	}

	if got := queryAgentDepth(t, db, "agent_chancellor_2"); got != 1 {
		t.Fatalf("expected delegated spawn depth 1, got %d", got)
	}
}

func TestDelegationPolicyEngineAllowsAdvisorSpawnOnMatchingRequiredRole(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedDelegationPolicyFixture(t, db)

	engine := NewDelegationPolicyEngine(db, NewAgentRegistry(db))
	decision, err := engine.Delegate(context.Background(), DelegationRequest{
		RunID:            "run_1",
		NodeID:           "advisor_consultation",
		DelegatorAgentID: "agent_king",
		DelegateAgentID:  "agent_noble_delegate",
		DelegateRoleID:   "noble-delegate",
		MaxSpawnDepth:    3,
		MaxActiveAgents:  12,
		OccurredAt:       "2026-03-01T15:05:00Z",
	})
	if err != nil {
		t.Fatalf("delegate advisor: %v", err)
	}

	if decision.Outcome != DelegationOutcomeSuccess {
		t.Fatalf("expected success, got %+v", decision)
	}
}

func TestDelegationPolicyEngineRejectsNonSpawnerAndEscalatesToChancellor(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedDelegationPolicyFixture(t, db)

	engine := NewDelegationPolicyEngine(db, NewAgentRegistry(db))
	decision, err := engine.Delegate(context.Background(), DelegationRequest{
		RunID:            "run_1",
		NodeID:           "plan_drafting",
		DelegatorAgentID: "agent_noble_existing",
		DelegateAgentID:  "agent_council_2",
		DelegateRoleID:   "council-member",
		MaxSpawnDepth:    3,
		MaxActiveAgents:  12,
		OccurredAt:       "2026-03-01T15:10:00Z",
	})
	if err != nil {
		t.Fatalf("delegate with non-spawner: %v", err)
	}

	if decision.Outcome != DelegationOutcomeHardFail || decision.ReasonCode != delegationReasonPolicyViolation {
		t.Fatalf("expected hard fail policy_violation, got %+v", decision)
	}
	if decision.EscalationTarget != "chancellor" {
		t.Fatalf("expected escalation target chancellor, got %q", decision.EscalationTarget)
	}
}

func TestDelegationPolicyEngineRejectsSpawnableRoleViolations(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedDelegationPolicyFixture(t, db)

	engine := NewDelegationPolicyEngine(db, NewAgentRegistry(db))
	decision, err := engine.Delegate(context.Background(), DelegationRequest{
		RunID:            "run_1",
		NodeID:           "delegated_execution",
		DelegatorAgentID: "agent_chancellor",
		DelegateAgentID:  "agent_builder_new",
		DelegateRoleID:   "builder-berserker",
		MaxSpawnDepth:    3,
		MaxActiveAgents:  12,
		OccurredAt:       "2026-03-01T15:15:00Z",
	})
	if err != nil {
		t.Fatalf("delegate with invalid spawnable role: %v", err)
	}

	if decision.Outcome != DelegationOutcomeHardFail || decision.ReasonCode != delegationReasonPolicyViolation {
		t.Fatalf("expected hard fail policy_violation, got %+v", decision)
	}
}

func TestDelegationPolicyEngineRejectsNoSkipGovernanceUntilApproved(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedDelegationPolicyFixture(t, db)

	mustExec(t, db, `UPDATE workflow_runs SET plan_state = 'draft' WHERE run_id = 'run_1'`)

	engine := NewDelegationPolicyEngine(db, NewAgentRegistry(db))
	decision, err := engine.Delegate(context.Background(), DelegationRequest{
		RunID:            "run_1",
		NodeID:           "delegated_execution",
		DelegatorAgentID: "agent_council",
		DelegateAgentID:  "agent_builder_new",
		DelegateRoleID:   "builder-berserker",
		MaxSpawnDepth:    3,
		MaxActiveAgents:  12,
		OccurredAt:       "2026-03-01T15:20:00Z",
	})
	if err != nil {
		t.Fatalf("delegate with unapproved plan: %v", err)
	}

	if decision.Outcome != DelegationOutcomeHardFail || decision.ReasonCode != delegationReasonDependencyBlocked {
		t.Fatalf("expected hard fail dependency_blocked, got %+v", decision)
	}
	if decision.EscalationTarget != "chancellor" {
		t.Fatalf("expected escalation target chancellor, got %q", decision.EscalationTarget)
	}
}

func TestDelegationPolicyEngineReturnsSoftFailOnMaxActiveAgentsCap(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedDelegationPolicyFixture(t, db)

	engine := NewDelegationPolicyEngine(db, NewAgentRegistry(db))
	decision, err := engine.Delegate(context.Background(), DelegationRequest{
		RunID:            "run_1",
		NodeID:           "advisory_synthesis",
		DelegatorAgentID: "agent_king",
		DelegateAgentID:  "agent_chancellor_2",
		DelegateRoleID:   "chancellor",
		MaxSpawnDepth:    3,
		MaxActiveAgents:  1,
		OccurredAt:       "2026-03-01T15:25:00Z",
	})
	if err != nil {
		t.Fatalf("delegate under active-agent cap: %v", err)
	}

	if decision.Outcome != DelegationOutcomeSoftFail || decision.ReasonCode != delegationReasonConcurrencyCapReached {
		t.Fatalf("expected soft fail concurrency cap, got %+v", decision)
	}
	if got := queryAgentCount(t, db, "agent_chancellor_2"); got != 0 {
		t.Fatalf("expected no spawned agent on soft fail, got %d", got)
	}
}

func TestDelegationPolicyEngineReturnsSoftFailOnNightravenRoleCap(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedDelegationPolicyFixture(t, db)

	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_nightraven_1', 'intelligence-nightraven', 'idle', 'agent_council', 3, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_night', 'wfdef_1', 'security_validation', 'intelligence-nightraven', 'validation_gates', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_night_1', 'run_1', 'security_validation', 1,1, 'running', 'intelligence-nightraven', 'agent_nightraven_1', 1, '2026-03-01T12:02:00Z', NULL)`)

	engine := NewDelegationPolicyEngine(db, NewAgentRegistry(db))
	decision, err := engine.Delegate(context.Background(), DelegationRequest{
		RunID:            "run_1",
		NodeID:           "security_validation",
		DelegatorAgentID: "agent_council",
		DelegateAgentID:  "agent_nightraven_2",
		DelegateRoleID:   "intelligence-nightraven",
		MaxSpawnDepth:    5,
		MaxActiveAgents:  12,
		OccurredAt:       "2026-03-01T15:26:00Z",
	})
	if err != nil {
		t.Fatalf("delegate under nightraven role cap: %v", err)
	}

	if decision.Outcome != DelegationOutcomeSoftFail || decision.ReasonCode != delegationReasonConcurrencyCapReached {
		t.Fatalf("expected soft fail concurrency cap for nightraven, got %+v", decision)
	}
	if got := queryAgentCount(t, db, "agent_nightraven_2"); got != 0 {
		t.Fatalf("expected no spawned nightraven agent on cap, got %d", got)
	}
}

func TestDelegationPolicyEnginePreventsLineageCyclesDeterministically(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedDelegationPolicyFixture(t, db)

	mustExec(t, db, `UPDATE agents SET parent_agent_id = 'agent_council', spawn_depth = 2 WHERE agent_id = 'agent_chancellor'`)

	engine := NewDelegationPolicyEngine(db, NewAgentRegistry(db))
	request := DelegationRequest{
		RunID:            "run_1",
		NodeID:           "plan_drafting",
		DelegatorAgentID: "agent_chancellor",
		DelegateAgentID:  "agent_council",
		DelegateRoleID:   "council-member",
		MaxSpawnDepth:    5,
		MaxActiveAgents:  12,
		OccurredAt:       "2026-03-01T15:30:00Z",
	}

	firstDecision, err := engine.Delegate(context.Background(), request)
	if err != nil {
		t.Fatalf("first cycle check: %v", err)
	}
	secondDecision, err := engine.Delegate(context.Background(), request)
	if err != nil {
		t.Fatalf("second cycle check: %v", err)
	}

	if firstDecision.Outcome != DelegationOutcomeHardFail || firstDecision.ReasonCode != delegationReasonLineageCycle {
		t.Fatalf("expected first hard fail lineage_cycle, got %+v", firstDecision)
	}
	if secondDecision.Outcome != DelegationOutcomeHardFail || secondDecision.ReasonCode != delegationReasonLineageCycle {
		t.Fatalf("expected second hard fail lineage_cycle, got %+v", secondDecision)
	}
}

func TestDelegationPolicyEngineRejectsSpawnDepthCap(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedDelegationPolicyFixture(t, db)

	engine := NewDelegationPolicyEngine(db, NewAgentRegistry(db))
	decision, err := engine.Delegate(context.Background(), DelegationRequest{
		RunID:            "run_1",
		NodeID:           "delegated_execution",
		DelegatorAgentID: "agent_council",
		DelegateAgentID:  "agent_builder_new",
		DelegateRoleID:   "builder-berserker",
		MaxSpawnDepth:    1,
		MaxActiveAgents:  12,
		OccurredAt:       "2026-03-01T15:35:00Z",
	})
	if err != nil {
		t.Fatalf("delegate over depth cap: %v", err)
	}

	if decision.Outcome != DelegationOutcomeHardFail || decision.ReasonCode != delegationReasonSpawnDepthExceeded {
		t.Fatalf("expected hard fail spawn_depth_exceeded, got %+v", decision)
	}
}

func TestDelegationPolicyEngineAppliesDefaultSpawnAndActiveCaps(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedDelegationPolicyFixture(t, db)

	engine := NewDelegationPolicyEngine(db, NewAgentRegistry(db))
	decision, err := engine.Delegate(context.Background(), DelegationRequest{
		RunID:            "run_1",
		NodeID:           "advisory_synthesis",
		DelegatorAgentID: "agent_king",
		DelegateAgentID:  "agent_chancellor_defaulted",
		DelegateRoleID:   "chancellor",
		MaxSpawnDepth:    0,
		MaxActiveAgents:  0,
		OccurredAt:       "2026-03-01T15:40:00Z",
	})
	if err != nil {
		t.Fatalf("delegate with default caps: %v", err)
	}
	if decision.Outcome != DelegationOutcomeSuccess {
		t.Fatalf("expected success with defaults, got %+v", decision)
	}
}

func seedDelegationPolicyFixture(t *testing.T, db *sql.DB) {
	t.Helper()

	seedRoleCatalog(t, db)

	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_chancellor', 'chancellor', 'idle', 'agent_king', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council', 'council-member', 'idle', 'agent_chancellor', 2, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_noble_existing', 'noble-delegate', 'idle', 'agent_king', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)

	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_1', 'D2', 'text', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_1', 'royal-decree', 'v1', '{}', '2026-03-01T12:00:00Z')`)

	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_1', 'wfdef_1', 'decree_intake', 'king', 'decree_intake', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_2', 'wfdef_1', 'advisor_consultation', 'noble-delegate', 'advisor_consultation', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_3', 'wfdef_1', 'advisory_synthesis', 'chancellor', 'advisory_synthesis', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_4', 'wfdef_1', 'plan_drafting', 'council-member', 'plan_drafting', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_5', 'wfdef_1', 'king_plan_decision', 'king', 'king_plan_decision', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_6', 'wfdef_1', 'execution_kickoff', 'council-member', 'execution_kickoff', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_7', 'wfdef_1', 'delegated_execution', 'builder-berserker', 'delegated_execution', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_8', 'wfdef_1', 'validation_gates', 'intelligence-qa-scout', 'validation_gates', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_9', 'wfdef_1', 'royal_closure', 'king', 'royal_closure', '2026-03-01T12:00:00Z')`)

	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_1', NULL, 'decree_1', 'wfdef_1', 'running', 'approved', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)

	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_active', 'run_1', 'decree_intake', 1,1, 'running', 'king', 'agent_king', 1, '2026-03-01T12:00:00Z', NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_s1', 'run_1', 'decree_intake', 2,2, 'succeeded', 'king', 'agent_king', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:05:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_s2', 'run_1', 'advisor_consultation', 1,1, 'succeeded', 'noble-delegate', 'agent_noble_existing', 1, '2026-03-01T12:06:00Z', '2026-03-01T12:07:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_s3', 'run_1', 'advisory_synthesis', 1,1, 'succeeded', 'chancellor', 'agent_chancellor', 1, '2026-03-01T12:08:00Z', '2026-03-01T12:09:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_s4', 'run_1', 'plan_drafting', 1,1, 'succeeded', 'council-member', 'agent_council', 1, '2026-03-01T12:12:00Z', '2026-03-01T12:13:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_s5', 'run_1', 'king_plan_decision', 1,1, 'succeeded', 'king', 'agent_king', 1, '2026-03-01T12:14:00Z', '2026-03-01T12:15:00Z')`)
}

func seedRoleCatalog(t *testing.T, db *sql.DB) {
	t.Helper()

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('chancellor', 'Chancellor', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('council-member', 'Council Member', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('noble-delegate', 'Noble Delegate', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('clergy-delegate', 'Clergy Delegate', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('burgher-delegate', 'Burgher Delegate', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('peasant-delegate', 'Peasant Delegate', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder-berserker', 'Berserker', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer-shieldmaiden', 'Shieldmaiden Reviewer', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-qa-scout', 'Scout', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-nightraven', 'Nightraven', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('intelligence-loki-chaos', 'Loki Servant', 0, '2026-03-01T12:00:00Z')`)
}
