package state

import (
	"context"
	"database/sql"
	"strconv"
	"testing"
	"time"
)

func TestDelegationUsesPersistedNodeAuthority(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ nodeID, nodeClass, role string }{
		{"implementation", "delegated_execution", "builder"},
		{"quality_validation", "validation_gates", "qa-validator"},
		{"security_validation", "validation_gates", "security-validator"},
		{"resilience_validation", "validation_gates", "resilience-validator"},
		{"custom_build_step", "delegated_execution", "builder"},
		{"architecture_consultation", "delegated_execution", "builder"},
	} {
		t.Run(tc.nodeID, func(t *testing.T) {
			db := openTestDB(t)
			applyMigration(t, db, "0001_initial_schema.up.sql")
			seedDelegationPolicyFixture(t, db)
			mustExecArgs(t, db, `UPDATE workflow_nodes SET node_id = ?, node_class = ?, required_role = ? WHERE workflow_node_id = 'wn_7'`, tc.nodeID, tc.nodeClass, tc.role)
			engine := NewDelegationPolicyEngine(db, NewAgentRegistry(db))
			request := DelegationRequest{RunID: "run_1", NodeID: tc.nodeID, DelegatorAgentID: "agent_council", DelegateAgentID: "authority_delegate", DelegateRoleID: tc.role, MaxSpawnDepth: 3, OccurredAt: "2026-03-01T15:20:00Z"}
			for _, stage := range []struct {
				name, plan, prerequisite string
				allowed                  bool
			}{
				{"draft plan", "draft", "succeeded", false},
				{"incomplete governance", "approved", "pending", false},
				{"approved and complete", "approved", "succeeded", true},
			} {
				mustExecArgs(t, db, `UPDATE workflow_runs SET plan_state = ? WHERE run_id = 'run_1'`, stage.plan)
				mustExecArgs(t, db, `UPDATE node_runs SET node_status = ? WHERE node_run_id = 'nr_s4'`, stage.prerequisite)
				decision, err := engine.Delegate(context.Background(), request)
				if err != nil {
					t.Fatalf("%s: %v", stage.name, err)
				}
				if stage.allowed {
					if decision.Outcome != DelegationOutcomeSuccess {
						t.Fatalf("%s: expected success, got %+v", stage.name, decision)
					}
				} else {
					if decision.Outcome != DelegationOutcomeHardFail || decision.ReasonCode != delegationReasonDependencyBlocked {
						t.Fatalf("%s: expected dependency_blocked, got %+v", stage.name, decision)
					}
					var count int
					if err := db.QueryRow(`SELECT COUNT(*) FROM agents WHERE agent_id = 'authority_delegate'`).Scan(&count); err != nil {
						t.Fatal(err)
					}
					if count != 0 {
						t.Fatalf("%s: blocked delegation created an agent", stage.name)
					}
				}
			}
		})
	}
}

func TestDelegationRejectsMissingOrInvalidNodeContract(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, nodeClass string
		missing         bool
	}{
		{"missing node", "delegated_execution", true},
		{"unknown class", "unknown_class", false},
		{"empty class", "", false},
		{"class role mismatch", "advisor_consultation", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := openTestDB(t)
			applyMigration(t, db, "0001_initial_schema.up.sql")
			seedDelegationPolicyFixture(t, db)
			nodeID := "delegated_execution"
			if tc.missing {
				nodeID = "missing_node"
			} else {
				mustExecArgs(t, db, `UPDATE workflow_nodes SET node_class = ? WHERE workflow_node_id = 'wn_7'`, tc.nodeClass)
			}
			decision, err := NewDelegationPolicyEngine(db, NewAgentRegistry(db)).Delegate(context.Background(), DelegationRequest{RunID: "run_1", NodeID: nodeID, DelegatorAgentID: "agent_council", DelegateAgentID: "invalid_contract_delegate", DelegateRoleID: "builder", MaxSpawnDepth: 3, OccurredAt: "2026-03-01T15:20:00Z"})
			if err == nil {
				t.Fatalf("expected invalid contract error, got decision %+v", decision)
			}
			var count int
			if err := db.QueryRow(`SELECT COUNT(*) FROM agents WHERE agent_id = 'invalid_contract_delegate'`).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != 0 {
				t.Fatal("invalid contract created an agent")
			}
		})
	}
}

func TestReconcilerAdvisorGraceUsesPersistedClass(t *testing.T) {
	t.Parallel()
	for _, nodeID := range []string{"architecture_consultation", "risk_consultation", "systems_consultation", "usability_consultation", "custom_advice", "implementation"} {
		t.Run(nodeID, func(t *testing.T) {
			db := seedNeutralAdvisorAttempt(t, nodeID, 1)
			reconciler := NewReconciler(db, nil, alwaysRetryPlanner{}, nil)
			stale, err := reconciler.findStaleAttempts(context.Background(), mustParseRFC3339(t, "2026-03-01T12:02:30Z"), DefaultReassignCandidateThreshold)
			if err != nil {
				t.Fatal(err)
			}
			if len(stale) != 0 {
				t.Fatalf("advisor should retain planning grace, got %d stale attempts", len(stale))
			}
			stale, err = reconciler.findStaleAttempts(context.Background(), mustParseRFC3339(t, "2026-03-01T12:08:00Z"), DefaultReassignCandidateThreshold)
			if err != nil {
				t.Fatal(err)
			}
			if len(stale) != 1 {
				t.Fatalf("advisor should become stale after grace, got %d", len(stale))
			}
		})
	}
}

func TestReconcilerAdvisorSelfRetryLimit(t *testing.T) {
	t.Parallel()
	for _, nodeID := range []string{"architecture_consultation", "custom_advice", "implementation"} {
		for _, attempt := range []int{1, 2} {
			t.Run(nodeID+"/"+strconv.Itoa(attempt), func(t *testing.T) {
				db := seedNeutralAdvisorAttempt(t, nodeID, attempt)
				reconciler := NewReconciler(db, nil, alwaysRetryPlanner{}, nil)
				result, err := reconciler.Reconcile(context.Background(), ReconcileRequest{Now: "2026-03-01T12:08:00Z"})
				if err != nil {
					t.Fatal(err)
				}
				if attempt == 1 {
					if result.AttemptsReassigned != 1 || result.AttemptsFailed != 0 {
						t.Fatalf("first planning attempt should retain one self-retry: %+v", result)
					}
				} else if result.AttemptsReassigned != 0 || result.AttemptsFailed != 1 {
					t.Fatalf("second planning attempt must not self-retry: %+v", result)
				}
			})
		}
	}
}

func seedNeutralAdvisorAttempt(t *testing.T, nodeID string, attempt int) *sql.DB {
	t.Helper()
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	mustExec(t, db, `UPDATE node_runs SET node_status = 'succeeded', finished_at = '2026-03-01T12:00:00Z' WHERE node_run_id = 'nr_1'`)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('architecture-advisor', 'Architecture Advisor', 0, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('advisor_agent', 'architecture-advisor', 'busy', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExecArgs(t, db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('advisor_node', 'wfdef_1', ?, 'architecture-advisor', 'advisor_consultation', '2026-03-01T12:00:00Z')`, nodeID)
	mustExecArgs(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at) VALUES ('advisor_attempt', 'run_1', ?, ?, 1, 'running', 'architecture-advisor', 'advisor_agent', 1, '2026-03-01T12:00:00Z')`, nodeID, attempt)
	return db
}

func TestCanonicalRoleNamesRemainNeutral(t *testing.T) {
	t.Parallel()
	db := openRoleSeedTestDB(t)
	applyRoleSeedMigration(t, db)
	if err := EnsureCanonicalRoles(context.Background(), db, time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	for roleID, expected := range map[string]string{"reviewer": "Reviewer", "qa-validator": "QA Validator", "security-validator": "Security Validator", "resilience-validator": "Resilience Validator"} {
		var display string
		if err := db.QueryRow(`SELECT display_name FROM roles WHERE role_id = ?`, roleID).Scan(&display); err != nil {
			t.Fatal(err)
		}
		if display != expected {
			t.Errorf("role %s: got %q, want %q", roleID, display, expected)
		}
	}
}
