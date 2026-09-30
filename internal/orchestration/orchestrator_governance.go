package orchestration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"kingdom/internal/nodeattempt"
	"kingdom/internal/state"
)

const governanceModeAutonomous = "autonomous"

func normalizeGovernanceMode(raw string) string {
	mode := strings.ToLower(strings.TrimSpace(raw))
	switch mode {
	case "", governanceModeAutonomous:
		return governanceModeAutonomous
	default:
		return governanceModeAutonomous
	}
}

func (o *orchestrationLoop) autoAdvanceGovernanceDecision(ctx context.Context, run *runSnapshot, occurredAt string) error {
	if run == nil || run.PlanState != "awaiting_plan_decision" {
		return nil
	}
	if normalizeGovernanceMode(o.governanceMode) != governanceModeAutonomous {
		return nil
	}
	if !run.AutonomousApprovals {
		return nil
	}

	attemptID := "run:" + run.RunID + ":plan"
	eventSeq := o.nextEventSeq(ctx, attemptID)
	err := o.transition.Apply(ctx, state.TransitionRequest{
		TransitionID:    fmt.Sprintf("tr_orch_%s_plan_auto_governance_approve_%d", run.RunID, eventSeq),
		EventID:         fmt.Sprintf("evt_orch_%s_plan_auto_governance_approve_%d", run.RunID, eventSeq),
		RunID:           run.RunID,
		Entity:          state.EntityTypePlan,
		FromState:       "awaiting_plan_decision",
		ToState:         "approved",
		ActorID:         "agent_governance",
		RoleID:          "governance",
		AuthoritySource: "runtime.autonomous_governance",
		ReasonCode:      "governance_decision",
		ReasonText:      "autonomous governance approval based on work order contract",
		CorrelationID:   "corr_orch_" + run.RunID,
		CausationID:     fmt.Sprintf("caus_orch_%s_auto_approve", run.RunID),
		OccurredAt:      occurredAt,
		AttemptID:       attemptID,
		SchemaVersion:   "v1",
		EventType:       "approval.recorded",
		EventSeq:        eventSeq,
		PayloadJSON:     fmt.Sprintf(`{"run_id":%q,"decision":"approve","mode":"autonomous"}`, run.RunID),
	})
	if err == nil {
		_ = o.emitCommunicationMessage(ctx, communicationMessageRequest{
			MessageID:         fmt.Sprintf("msg_comm_%s_plan_approval_response", sanitizeFileToken(run.RunID)),
			RunID:             run.RunID,
			FromAgentID:       deterministicAgentIDForRole("governance"),
			ToAgentID:         deterministicAgentIDForRole("coordinator"),
			ThreadID:          fmt.Sprintf("thread_%s_plan_governance", sanitizeFileToken(run.RunID)),
			CommunicationType: "approval_response",
			NodeID:            "plan_decision",
			AttemptID:         attemptID,
			ReasonCode:        "",
			ReasonText:        "autonomous governance approval based on work order contract",
			AuthoritySource:   "runtime.autonomous_governance",
			OccurredAt:        occurredAt,
		})
		run.PlanState = "approved"
		return nil
	}

	var conflictErr *state.StateConflictError
	if errors.As(err, &conflictErr) {
		run.PlanState = conflictErr.ActualState
		return nil
	}

	var illegalErr *state.IllegalTransitionError
	if errors.As(err, &illegalErr) {
		return nil
	}

	return err
}

func (o *orchestrationLoop) advancePlanProjection(ctx context.Context, run *runSnapshot, occurredAt string, nodes []nodeDefinition) error {
	if run == nil {
		return nil
	}

	nodeIDs := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		nodeIDs[node.NodeID] = struct{}{}
	}

	for {
		from := run.PlanState
		to := ""

		switch run.PlanState {
		case "draft":
			to = "in_plan_review"
		case "in_plan_review":
			if o.canAdvanceToGovernanceDecision(ctx, run.RunID, nodes, nodeIDs) {
				to = "awaiting_plan_decision"
			}
		case "needs_rework":
			to = "in_plan_review"
		default:
			return nil
		}

		if to == "" {
			return nil
		}

		if to == "awaiting_plan_decision" {
			_ = o.emitCommunicationMessage(ctx, communicationMessageRequest{
				MessageID:         fmt.Sprintf("msg_comm_%s_plan_approval_request", sanitizeFileToken(run.RunID)),
				RunID:             run.RunID,
				FromAgentID:       deterministicAgentIDForRole("plan-author"),
				ToAgentID:         deterministicAgentIDForRole("governance"),
				ThreadID:          fmt.Sprintf("thread_%s_plan_governance", sanitizeFileToken(run.RunID)),
				CommunicationType: "approval_request",
				NodeID:            "plan_decision",
				AttemptID:         "run:" + run.RunID + ":plan",
				ReasonCode:        "",
				ReasonText:        "plan is ready for governance decision",
				AuthoritySource:   "runtime.orchestrator",
				OccurredAt:        occurredAt,
			})
		}

		attemptID := "run:" + run.RunID + ":plan"
		eventSeq := o.nextEventSeq(ctx, attemptID)
		err := o.transition.Apply(ctx, state.TransitionRequest{
			TransitionID:    fmt.Sprintf("tr_orch_%s_plan_%s_to_%s_%d", run.RunID, from, to, eventSeq),
			EventID:         fmt.Sprintf("evt_orch_%s_plan_%s_to_%s_%d", run.RunID, from, to, eventSeq),
			RunID:           run.RunID,
			Entity:          state.EntityTypePlan,
			FromState:       from,
			ToState:         to,
			ActorID:         "runtime_orchestrator",
			RoleID:          "coordinator",
			AuthoritySource: "runtime.orchestrator",
			ReasonCode:      "orchestration_progress",
			ReasonText:      "advance plan projection",
			CorrelationID:   "corr_orch_" + run.RunID,
			CausationID:     fmt.Sprintf("caus_orch_%s_plan_%s", run.RunID, to),
			OccurredAt:      occurredAt,
			AttemptID:       attemptID,
			SchemaVersion:   "v1",
			EventType:       "run.state_changed",
			EventSeq:        eventSeq,
			PayloadJSON:     fmt.Sprintf(`{"run_id":%q,"plan_state":%q}`, run.RunID, to),
		})
		if err != nil {
			var conflictErr *state.StateConflictError
			if errors.As(err, &conflictErr) {
				run.PlanState = conflictErr.ActualState
				continue
			}

			var illegalErr *state.IllegalTransitionError
			if errors.As(err, &illegalErr) {
				return nil
			}

			return err
		}

		run.PlanState = to
	}
}

func (o *orchestrationLoop) canAdvanceToGovernanceDecision(ctx context.Context, runID string, nodes []nodeDefinition, nodeIDs map[string]struct{}) bool {
	requiredNodeIDs := make([]string, 0)
	for _, node := range nodes {
		switch node.NodeClass {
		case "plan_drafting":
			requiredNodeIDs = append(requiredNodeIDs, node.NodeID)
		}
	}

	if len(requiredNodeIDs) == 0 {
		if _, exists := nodeIDs["plan_drafting"]; exists {
			requiredNodeIDs = append(requiredNodeIDs, "plan_drafting")
		}
	}

	for _, nodeID := range requiredNodeIDs {
		var status sql.NullString
		query := fmt.Sprintf(`SELECT node_status FROM node_runs WHERE run_id = ? AND node_id = ? ORDER BY %s LIMIT 1`, nodeattempt.CanonicalNodeAttemptOrderSQL("node_runs"))
		err := o.db.QueryRowContext(ctx, query, runID, nodeID).Scan(&status)
		if err != nil || !status.Valid || status.String != "succeeded" {
			return false
		}
	}

	return true
}
