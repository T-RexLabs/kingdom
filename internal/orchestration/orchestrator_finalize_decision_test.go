package orchestration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"kingdom/internal/state"
)

func TestFinalizeRunStateWaitsForUnresolvedPlanDecisionAfterSourceSuccess(t *testing.T) {
	for _, autonomous := range []bool{true, false} {
		t.Run(fmt.Sprintf("autonomous=%t", autonomous), func(t *testing.T) {
			loop, db, run, nodes, edges := newUnresolvedPlanFinalizationFixture(t, autonomous)
			ctx := t.Context()
			// This is the dispatch ordering where provider completion lands after
			// the last plan projection pass and before finalization reloads nodes.
			if err := loop.finalizeRunState(ctx, run, nodes, edges); err != nil {
				t.Fatalf("finalize while plan decision is unresolved: %v", err)
			}
			status, plan := queryRunState(t, db, run.RunID)
			if status != "running" || plan != "in_plan_review" {
				t.Fatalf("unresolved plan was finalized: status=%s plan=%s", status, plan)
			}
			if count := queryEventCount(t, db, run.RunID, "approval.recorded"); count != 0 {
				t.Fatalf("finalization recorded %d decisions before approval", count)
			}
			if got := queryNodeStatus(t, db, "nr_decision_completion"); got != "pending" {
				t.Fatalf("completion before decision=%s, want pending", got)
			}

			if err := loop.advancePlanProjection(ctx, &run, loop.now().Format(time.RFC3339Nano), nodes); err != nil {
				t.Fatalf("advance the now-complete plan to decision: %v", err)
			}
			if run.PlanState != "awaiting_plan_decision" {
				t.Fatalf("plan state before approval=%s", run.PlanState)
			}
			if err := loop.autoAdvanceGovernanceDecision(ctx, &run, loop.now().Format(time.RFC3339Nano)); err != nil {
				t.Fatalf("apply autonomous governance: %v", err)
			}
			if !autonomous {
				if run.PlanState != "awaiting_plan_decision" {
					t.Fatalf("manual mode bypassed plan approval: %s", run.PlanState)
				}
				approvePlanStateAsOperatorForRuntimeTest(t, db, run.RunID)
				_, run.PlanState = queryRunState(t, db, run.RunID)
			}
			if run.PlanState != "approved" {
				t.Fatalf("canonical decision did not approve plan: %s", run.PlanState)
			}
			// Approval alone must not skip the pending completion node.
			if err := loop.finalizeRunState(ctx, run, nodes, edges); err != nil {
				t.Fatal(err)
			}
			if got := queryRunStatus(t, db, run.RunID); got != "running" {
				t.Fatalf("approved plan skipped pending completion: %s", got)
			}
			completeDecisionFixtureNode(t, ctx, loop, run.RunID)
			if err := loop.finalizeRunState(ctx, run, nodes, edges); err != nil {
				t.Fatalf("finalize after canonical completion: %v", err)
			}
			want := "succeeded"
			if !autonomous {
				want = "awaiting_human_confirmation"
			}
			if got := queryRunStatus(t, db, run.RunID); got != want {
				t.Fatalf("run after approved completion=%s, want %s", got, want)
			}
		})
	}
}

func TestPlanDecisionEdgesRetainPendingAndReplayableNodesUntilResolved(t *testing.T) {
	for _, condition := range []string{"approved", "needs_rework", " APPROVED ", " NEEDS_REWORK "} {
		for _, plan := range []string{"draft", "in_plan_review", "awaiting_plan_decision", "", "invalid", " APPROVED "} {
			t.Run(fmt.Sprintf("%s/%s", condition, plan), func(t *testing.T) {
				assertPlanDecisionReachability(t, condition, plan, "succeeded", true)
			})
		}
	}
	for _, tc := range []struct {
		name, condition, plan, source string
		resolvable                    bool
	}{
		{"approved branch", "approved", "approved", "succeeded", true},
		{"rework branch", "needs_rework", "needs_rework", "succeeded", true},
		{"opposite approved branch", "approved", "needs_rework", "succeeded", false},
		{"opposite rework branch", "needs_rework", "approved", "succeeded", false},
		{"failed source before decision", "approved", "in_plan_review", "failed", false},
		{"failed source after approval", "approved", "approved", "failed", false},
		{"failed source rework branch", "needs_rework", "invalid", "failed", false},
		{"cancelled source before decision", "approved", "in_plan_review", "cancelled", false},
		{"live source", "approved", "in_plan_review", "running", true},
		{"ordinary unmatched condition", "failed", "in_plan_review", "succeeded", false},
		{"ordinary success condition", "success", "in_plan_review", "succeeded", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertPlanDecisionReachability(t, tc.condition, tc.plan, tc.source, tc.resolvable)
		})
	}
}

func TestUnresolvedPlanDecisionRetainsTransitiveCancelledReplay(t *testing.T) {
	run := runSnapshot{PlanState: "in_plan_review"}
	nodes := []nodeDefinition{{NodeID: "plan"}, {NodeID: "implementation"}, {NodeID: "completion"}}
	latest := map[string]nodeAttempt{
		"plan":           {NodeStatus: "succeeded"},
		"implementation": decisionCancelledAttempt(),
		"completion":     decisionCancelledAttempt(),
	}
	edges := []edgeDefinition{
		{FromNodeID: "plan", ToNodeID: "implementation", On: "approved"},
		{FromNodeID: "implementation", ToNodeID: "completion", On: "success"},
	}
	replay := collectReplayNeededCancelledNodes(run, nodes, edges, latest)
	for _, nodeID := range []string{"implementation", "completion"} {
		if _, found := replay[nodeID]; !found {
			t.Fatalf("unresolved approval lost replay lineage for %s: %v", nodeID, replay)
		}
	}
	if summary := summarizeRunClosure(run, nodes, edges, latest); len(summary.ReplayNodeIDs) != 2 || summary.HasFailed {
		t.Fatalf("replayable decision scope was treated as terminal: %#v", summary)
	}
}

func TestRepairFailedRunRetainsCancelledNodeAwaitingPlanDecision(t *testing.T) {
	loop, db, run, _, _ := newUnresolvedPlanFinalizationFixture(t, true)
	ctx := t.Context()
	err := loop.transition.TerminalizeRunAndCancelLiveNodes(ctx, state.RunTerminalizationRequest{Transition: state.TransitionRequest{
		TransitionID: "tr_decision_scope_failed", EventID: "evt_decision_scope_failed", RunID: run.RunID,
		Entity: state.EntityTypeRun, FromState: "running", ToState: "failed", ActorID: "runtime_orchestrator", RoleID: "coordinator",
		AuthoritySource: "runtime.orchestrator", ReasonCode: "dependency_blocked", ReasonText: "source terminal observation required recovery",
		CorrelationID: "corr_decision_scope_failed", CausationID: "caus_decision_scope_failed", OccurredAt: loop.now().Format(time.RFC3339Nano),
		AttemptID: "run:" + run.RunID + ":status", SchemaVersion: "v1", EventType: "run.state_changed", EventSeq: 1,
		PayloadJSON: fmt.Sprintf(`{"run_id":%q,"status":"failed"}`, run.RunID),
	}})
	if err != nil {
		t.Fatalf("seed canonical failed-run cancellation: %v", err)
	}
	if err := loop.repairFailedRunAfterRecoveredExecution(ctx, run.RunID, loop.now().Format(time.RFC3339Nano)); err != nil {
		t.Fatalf("repair run with recovered source and unresolved decision: %v", err)
	}
	status, plan := queryRunState(t, db, run.RunID)
	if status != "running" || plan != "in_plan_review" {
		t.Fatalf("repair finalized before the plan decision: status=%s plan=%s", status, plan)
	}
	var attempt int
	var nodeStatus string
	if err := db.QueryRow(`SELECT attempt, node_status FROM node_runs WHERE run_id = ? AND node_id = 'completion' ORDER BY attempt DESC LIMIT 1`, run.RunID).Scan(&attempt, &nodeStatus); err != nil {
		t.Fatal(err)
	}
	if attempt != 2 || nodeStatus != "pending" {
		t.Fatalf("conditional replay attempt=%d status=%s, want 2/pending", attempt, nodeStatus)
	}
}

func assertPlanDecisionReachability(t *testing.T, condition, plan, source string, resolvable bool) {
	t.Helper()
	run := runSnapshot{PlanState: plan}
	nodes := []nodeDefinition{{NodeID: "source"}, {NodeID: "target"}}
	latest := map[string]nodeAttempt{"source": {NodeStatus: source}, "target": {NodeStatus: "pending"}}
	edges := []edgeDefinition{{FromNodeID: "source", ToNodeID: "target", On: condition}}
	if got := isNodeTerminallyUnreachable(run, "target", latest, edges); got == resolvable {
		t.Fatalf("terminally unreachable=%t, want %t", got, !resolvable)
	}
	if got := nodeCanBecomeReachable(run, "target", latest, edges, nil); got != resolvable {
		t.Fatalf("can become reachable=%t, want %t", got, resolvable)
	}
	summary := summarizeRunClosure(run, nodes, edges, latest)
	if summary.HasInFlight != resolvable || summary.HasFailed != (source == "failed" || source == "cancelled") {
		t.Fatalf("pending closure=%#v, resolvable=%t source=%s", summary, resolvable, source)
	}
	latest["target"] = decisionCancelledAttempt()
	replay := collectReplayNeededCancelledNodes(run, nodes, edges, latest)
	_, found := replay["target"]
	if found != resolvable {
		t.Fatalf("cancelled target replay=%t, want %t", found, resolvable)
	}
}

func decisionCancelledAttempt() nodeAttempt {
	return nodeAttempt{NodeStatus: "cancelled", TransitionReasonCode: "run_terminalized", TransitionAuthority: runTerminalizedTransitionAuthoritySource}
}

func newUnresolvedPlanFinalizationFixture(t *testing.T, autonomous bool) (*orchestrationLoop, *sql.DB, runSnapshot, []nodeDefinition, []edgeDefinition) {
	t.Helper()
	db := openMigratedTestDB(t)
	seedApprovalClosureRunWithoutValidation(t, db)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('operator', 'Operator', 0, '2026-03-01T12:00:00Z')`)
	const runID = "run_closure_no_validation"
	mustExec(t, db, `UPDATE workflow_runs SET run_status = 'running', plan_state = 'in_plan_review' WHERE run_id = 'run_closure_no_validation'`)
	setRunAutonomousApprovalsForTest(t, db, runID, autonomous)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, spawn_depth, created_at, updated_at) VALUES ('agent_plan_author', 'plan-author', 'idle', 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	for _, node := range []struct{ id, runID, status, role, agent string }{
		{"work_intake", "nr_decision_intake", "succeeded", "governance", "agent_governance"},
		{"plan_drafting", "nr_decision_plan", "succeeded", "plan-author", "agent_plan_author"},
		{"completion", "nr_decision_completion", "pending", "governance", "agent_governance"},
	} {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version) VALUES ('%s', '%s', '%s', 1, 1, '%s', '%s', '%s', 1)`, node.runID, runID, node.id, node.status, node.role, node.agent))
	}
	loop := &orchestrationLoop{db: db, transition: state.NewTransitionService(db, nil), now: func() time.Time { return time.Date(2026, 3, 1, 12, 0, 4, 0, time.UTC) }}
	run := runSnapshot{RunID: runID, RunStatus: "running", PlanState: "in_plan_review", WorkflowDefinitionID: "wfdef_closure_no_validation", AutonomousApprovals: autonomous}
	nodes, edges, err := loop.loadWorkflowGraph(t.Context(), run)
	if err != nil {
		t.Fatal(err)
	}
	return loop, db, run, nodes, edges
}

func completeDecisionFixtureNode(t *testing.T, ctx context.Context, loop *orchestrationLoop, runID string) {
	t.Helper()
	from := "pending"
	for i, to := range []string{"ready", "running", "succeeded"} {
		err := loop.transition.Apply(ctx, state.TransitionRequest{
			TransitionID: fmt.Sprintf("tr_decision_completion_%d", i), EventID: fmt.Sprintf("evt_decision_completion_%d", i),
			RunID: runID, NodeRunID: sql.NullString{String: "nr_decision_completion", Valid: true}, Entity: state.EntityTypeNode,
			FromState: from, ToState: to, ActorID: "agent_governance", RoleID: "governance", AuthoritySource: "runtime.orchestrator",
			ReasonCode: "orchestration_progress", ReasonText: "complete approved work", CorrelationID: "corr_decision_completion", CausationID: "caus_decision_completion",
			OccurredAt: loop.now().Format(time.RFC3339Nano), AttemptID: runID + ":completion:1", SchemaVersion: "v1", EventType: "node.state_changed", EventSeq: i + 1,
			PayloadJSON: fmt.Sprintf(`{"state":%q}`, to),
		})
		if err != nil {
			t.Fatalf("completion %s -> %s: %v", from, to, err)
		}
		from = to
	}
}
