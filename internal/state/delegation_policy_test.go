package state

import (
	"context"
	"database/sql"
	"fmt"
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
		DelegatorAgentID: "agent_governance",
		DelegateAgentID:  "agent_coordinator_2",
		DelegateRoleID:   "coordinator",
		MaxSpawnDepth:    3,
		OccurredAt:       "2026-03-01T15:00:00Z",
	})
	if err != nil {
		t.Fatalf("delegate: %v", err)
	}

	if decision.Outcome != DelegationOutcomeSuccess {
		t.Fatalf("expected success, got %+v", decision)
	}

	if got := queryAgentParentID(t, db, "agent_coordinator_2"); got != "agent_governance" {
		t.Fatalf("expected delegated parent agent_governance, got %q", got)
	}

	if got := queryAgentDepth(t, db, "agent_coordinator_2"); got != 1 {
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
		DelegatorAgentID: "agent_governance",
		DelegateAgentID:  "agent_noble_delegate",
		DelegateRoleID:   "architecture-advisor",
		MaxSpawnDepth:    3,
		OccurredAt:       "2026-03-01T15:05:00Z",
	})
	if err != nil {
		t.Fatalf("delegate advisor: %v", err)
	}

	if decision.Outcome != DelegationOutcomeSuccess {
		t.Fatalf("expected success, got %+v", decision)
	}
}

func TestDelegationPolicyEngineRejectsNonSpawnerAndEscalatesToCoordinator(t *testing.T) {
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
		DelegateRoleID:   "plan-author",
		MaxSpawnDepth:    3,
		OccurredAt:       "2026-03-01T15:10:00Z",
	})
	if err != nil {
		t.Fatalf("delegate with non-spawner: %v", err)
	}

	if decision.Outcome != DelegationOutcomeHardFail || decision.ReasonCode != delegationReasonPolicyViolation {
		t.Fatalf("expected hard fail policy_violation, got %+v", decision)
	}
	if decision.EscalationTarget != "coordinator" {
		t.Fatalf("expected escalation target coordinator, got %q", decision.EscalationTarget)
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
		DelegatorAgentID: "agent_coordinator",
		DelegateAgentID:  "agent_builder_new",
		DelegateRoleID:   "builder",
		MaxSpawnDepth:    3,
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
		DelegateRoleID:   "builder",
		MaxSpawnDepth:    3,
		OccurredAt:       "2026-03-01T15:20:00Z",
	})
	if err != nil {
		t.Fatalf("delegate with unapproved plan: %v", err)
	}

	if decision.Outcome != DelegationOutcomeHardFail || decision.ReasonCode != delegationReasonDependencyBlocked {
		t.Fatalf("expected hard fail dependency_blocked, got %+v", decision)
	}
	if decision.EscalationTarget != "coordinator" {
		t.Fatalf("expected escalation target coordinator, got %q", decision.EscalationTarget)
	}
}

func TestDelegationPolicyEngineAllowsMoreThanTwelveActiveAgents(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedDelegationPolicyFixture(t, db)

	for i := 0; i < 13; i++ {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('active_%d', 'coordinator', 'busy', 'agent_governance', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`, i))
		mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version) VALUES ('active_%d', 'run_1', 'active_%d', 1, 1, 'running', 'coordinator', 'active_%d', 1)`, i, i, i))
	}

	engine := NewDelegationPolicyEngine(db, NewAgentRegistry(db))
	decision, err := engine.Delegate(context.Background(), DelegationRequest{
		RunID:            "run_1",
		NodeID:           "advisory_synthesis",
		DelegatorAgentID: "agent_governance",
		DelegateAgentID:  "agent_coordinator_2",
		DelegateRoleID:   "coordinator",
		MaxSpawnDepth:    3,
		OccurredAt:       "2026-03-01T15:25:00Z",
	})
	if err != nil {
		t.Fatalf("delegate beyond twelve active agents: %v", err)
	}

	if decision.Outcome != DelegationOutcomeSuccess {
		t.Fatalf("expected successful delegation beyond twelve active agents, got %+v", decision)
	}
	if got := queryAgentCount(t, db, "agent_coordinator_2"); got != 1 {
		t.Fatalf("expected spawned agent, got %d", got)
	}
}

func TestDelegationPolicyEngineAllowsMultipleActiveSecurityValidators(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedDelegationPolicyFixture(t, db)

	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_nightraven_1', 'security-validator', 'idle', 'agent_council', 3, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_night', 'wfdef_1', 'security_validation', 'security-validator', 'validation_gates', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_night_1', 'run_1', 'security_validation', 1,1, 'running', 'security-validator', 'agent_nightraven_1', 1, '2026-03-01T12:02:00Z', NULL)`)

	engine := NewDelegationPolicyEngine(db, NewAgentRegistry(db))
	decision, err := engine.Delegate(context.Background(), DelegationRequest{
		RunID:            "run_1",
		NodeID:           "security_validation",
		DelegatorAgentID: "agent_council",
		DelegateAgentID:  "agent_nightraven_2",
		DelegateRoleID:   "security-validator",
		MaxSpawnDepth:    3,
		OccurredAt:       "2026-03-01T15:26:00Z",
	})
	if err != nil {
		t.Fatalf("delegate second nightraven: %v", err)
	}

	if decision.Outcome != DelegationOutcomeSuccess {
		t.Fatalf("expected successful second nightraven delegation, got %+v", decision)
	}
	if got := queryAgentCount(t, db, "agent_nightraven_2"); got != 1 {
		t.Fatalf("expected spawned nightraven agent, got %d", got)
	}
}

func TestDelegationPolicyEnginePreventsLineageCyclesDeterministically(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedDelegationPolicyFixture(t, db)

	mustExec(t, db, `UPDATE agents SET parent_agent_id = 'agent_council', spawn_depth = 2 WHERE agent_id = 'agent_coordinator'`)

	engine := NewDelegationPolicyEngine(db, NewAgentRegistry(db))
	request := DelegationRequest{
		RunID:            "run_1",
		NodeID:           "plan_drafting",
		DelegatorAgentID: "agent_coordinator",
		DelegateAgentID:  "agent_council",
		DelegateRoleID:   "plan-author",
		MaxSpawnDepth:    5,
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
		DelegateRoleID:   "builder",
		MaxSpawnDepth:    1,
		OccurredAt:       "2026-03-01T15:35:00Z",
	})
	if err != nil {
		t.Fatalf("delegate over depth cap: %v", err)
	}

	if decision.Outcome != DelegationOutcomeHardFail || decision.ReasonCode != delegationReasonSpawnDepthExceeded {
		t.Fatalf("expected hard fail spawn_depth_exceeded, got %+v", decision)
	}
}

func TestDelegationPolicyEngineAppliesDefaultSpawnDepth(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedDelegationPolicyFixture(t, db)

	engine := NewDelegationPolicyEngine(db, NewAgentRegistry(db))
	decision, err := engine.Delegate(context.Background(), DelegationRequest{
		RunID:            "run_1",
		NodeID:           "advisory_synthesis",
		DelegatorAgentID: "agent_governance",
		DelegateAgentID:  "agent_coordinator_defaulted",
		DelegateRoleID:   "coordinator",
		MaxSpawnDepth:    0,
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

	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance', 'governance', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_coordinator', 'coordinator', 'idle', 'agent_governance', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_council', 'plan-author', 'idle', 'agent_coordinator', 2, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_noble_existing', 'architecture-advisor', 'idle', 'agent_governance', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)

	mustExec(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_1', 'D2', 'text', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_1', 'work-order', 'v1', '{}', '2026-03-01T12:00:00Z')`)

	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_1', 'wfdef_1', 'work_intake', 'governance', 'work_intake', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_2', 'wfdef_1', 'advisor_consultation', 'architecture-advisor', 'advisor_consultation', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_3', 'wfdef_1', 'advisory_synthesis', 'coordinator', 'advisory_synthesis', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_4', 'wfdef_1', 'plan_drafting', 'plan-author', 'plan_drafting', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_5', 'wfdef_1', 'plan_decision', 'governance', 'plan_decision', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_6', 'wfdef_1', 'execution_kickoff', 'plan-author', 'execution_kickoff', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_7', 'wfdef_1', 'delegated_execution', 'builder', 'delegated_execution', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_8', 'wfdef_1', 'validation_gates', 'qa-validator', 'validation_gates', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_9', 'wfdef_1', 'completion', 'governance', 'completion', '2026-03-01T12:00:00Z')`)

	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_1', NULL, 'work_order_1', 'wfdef_1', 'running', 'approved', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)

	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_active', 'run_1', 'work_intake', 1,1, 'running', 'governance', 'agent_governance', 1, '2026-03-01T12:00:00Z', NULL)`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_s1', 'run_1', 'work_intake', 2,2, 'succeeded', 'governance', 'agent_governance', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:05:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_s2', 'run_1', 'advisor_consultation', 1,1, 'succeeded', 'architecture-advisor', 'agent_noble_existing', 1, '2026-03-01T12:06:00Z', '2026-03-01T12:07:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_s3', 'run_1', 'advisory_synthesis', 1,1, 'succeeded', 'coordinator', 'agent_coordinator', 1, '2026-03-01T12:08:00Z', '2026-03-01T12:09:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_s4', 'run_1', 'plan_drafting', 1,1, 'succeeded', 'plan-author', 'agent_council', 1, '2026-03-01T12:12:00Z', '2026-03-01T12:13:00Z')`)
	mustExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr_s5', 'run_1', 'plan_decision', 1,1, 'succeeded', 'governance', 'agent_governance', 1, '2026-03-01T12:14:00Z', '2026-03-01T12:15:00Z')`)
}

func seedRoleCatalog(t *testing.T, db *sql.DB) {
	t.Helper()

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('coordinator', 'Coordinator', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('plan-author', 'Plan Author', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('architecture-advisor', 'Architecture Advisor', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('risk-advisor', 'Risk Advisor', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('systems-advisor', 'Systems Advisor', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('usability-advisor', 'Usability Advisor', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('reviewer', 'Reviewer Reviewer', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('qa-validator', 'Scout', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('security-validator', 'Security Validator', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('resilience-validator', 'Loki Servant', 0, '2026-03-01T12:00:00Z')`)
}
