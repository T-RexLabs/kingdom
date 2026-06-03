package orchestration

import (
	"context"
	"testing"
)

func TestIsNodeReadyUsesStoredKingPlanDecisionClassForArbitraryNodeID(t *testing.T) {
	t.Parallel()

	loop := orchestrationLoop{}
	nodes := []nodeDefinition{{NodeID: "approval_gate", RequiredRole: "king", NodeClass: "king_plan_decision"}}
	latest := map[string]nodeAttempt{
		"approval_gate": {
			NodeID:     "approval_gate",
			Attempt:    1,
			NodeStatus: "pending",
		},
	}

	ready, err := loop.isNodeReady(
		context.Background(),
		runSnapshot{RunID: "run_custom_king_decision", PlanState: "awaiting_king_decision"},
		nodes[0],
		buildResolvedNodeClassIndex(nodes),
		latest,
		nil,
	)
	if err != nil {
		t.Fatalf("custom king decision readiness: %v", err)
	}
	if ready {
		t.Fatal("expected arbitrary king_plan_decision node to remain blocked while the decision is still pending")
	}
}

func TestIsNodeReadyUsesStoredExecutionKickoffClassForArbitraryNodeID(t *testing.T) {
	t.Parallel()

	loop := orchestrationLoop{}
	nodes := []nodeDefinition{
		{NodeID: "approval_gate", RequiredRole: "king", NodeClass: "king_plan_decision"},
		{NodeID: "council_plan", RequiredRole: "council-member", NodeClass: "plan_drafting"},
		{NodeID: "kickoff", RequiredRole: "council-member", NodeClass: "execution_kickoff"},
	}
	latest := map[string]nodeAttempt{
		"approval_gate": {
			NodeID:     "approval_gate",
			Attempt:    1,
			NodeStatus: "succeeded",
		},
		"council_plan": {
			NodeID:     "council_plan",
			Attempt:    1,
			NodeStatus: "pending",
		},
		"kickoff": {
			NodeID:     "kickoff",
			Attempt:    1,
			NodeStatus: "pending",
		},
	}
	edges := []edgeDefinition{{FromNodeID: "approval_gate", ToNodeID: "kickoff", On: "success"}}

	ready, err := loop.isNodeReady(
		context.Background(),
		runSnapshot{RunID: "run_custom_kickoff", PlanState: "approved"},
		nodes[2],
		buildResolvedNodeClassIndex(nodes),
		latest,
		edges,
	)
	if err != nil {
		t.Fatalf("custom execution kickoff readiness: %v", err)
	}
	if ready {
		t.Fatal("expected arbitrary execution_kickoff node to remain blocked until all governance predecessors for the wave have succeeded")
	}
}

func TestIsNodeReadyUsesStoredRoyalClosureClassForArbitraryNodeID(t *testing.T) {
	t.Parallel()

	loop := orchestrationLoop{}
	nodes := []nodeDefinition{
		{NodeID: "review_gate", RequiredRole: "reviewer-shieldmaiden", NodeClass: "execution_review"},
		{NodeID: "quality_gate", RequiredRole: "intelligence-qa-scout", NodeClass: "validation_gates"},
		{NodeID: "final_seal", RequiredRole: "king", NodeClass: "royal_closure"},
	}
	latest := map[string]nodeAttempt{
		"review_gate": {
			NodeID:     "review_gate",
			Attempt:    1,
			NodeStatus: "succeeded",
		},
		"quality_gate": {
			NodeID:     "quality_gate",
			Attempt:    1,
			NodeStatus: "pending",
		},
		"final_seal": {
			NodeID:     "final_seal",
			Attempt:    1,
			NodeStatus: "pending",
		},
	}
	edges := []edgeDefinition{{FromNodeID: "review_gate", ToNodeID: "final_seal", On: "success"}}

	ready, err := loop.isNodeReady(
		context.Background(),
		runSnapshot{RunID: "run_custom_closure", PlanState: "approved"},
		nodes[2],
		buildResolvedNodeClassIndex(nodes),
		latest,
		edges,
	)
	if err != nil {
		t.Fatalf("custom royal closure readiness: %v", err)
	}
	if ready {
		t.Fatal("expected arbitrary royal_closure node to remain blocked until every validation gate has succeeded")
	}
}
