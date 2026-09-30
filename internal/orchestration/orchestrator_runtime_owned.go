package orchestration

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	coreworkflow "kingdom/internal/core/workflow"
	"kingdom/internal/state"
)

func (o *orchestrationLoop) runtimeOwnedNodeResponse(ctx context.Context, run runSnapshot, node nodeDefinition, attempt nodeAttempt) (string, error) {
	switch resolvedNodeClass(node.NodeID, node.NodeClass) {
	case coreworkflow.NodeClassWorkIntake:
		return strings.TrimSpace(`Work intake is complete. The work order is ready for planning.

Advisors will assess the request, the coordinator will synthesize their findings, and the plan author will prepare a plan for approval.`), nil
	case coreworkflow.NodeClassPlanDecision:
		return o.runtimeOwnedGovernancePlanDecisionResponse(ctx, run)
	case coreworkflow.NodeClassExecutionKickoff:
		return "Execution kickoff is complete. The approved plan is ready for implementation, review, and validation.", nil
	case coreworkflow.NodeClassCompletion:
		if run.AutonomousApprovals {
			return "Completion is recorded. The work order is complete.", nil
		}
		return "The final record is ready. The work order awaits operator confirmation before it is complete.", nil
	default:
		return "", fmt.Errorf("runtime-owned response is not defined for node class %q", resolvedNodeClass(node.NodeID, node.NodeClass))
	}
}

func (o *orchestrationLoop) runtimeOwnedGovernancePlanDecisionResponse(ctx context.Context, run runSnapshot) (string, error) {
	if run.AutonomousApprovals {
		return "Plan approval is recorded under autonomous governance. Execution may proceed.", nil
	}
	review, ok, err := latestRunPlanReview(ctx, o.db, run.RunID)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("latest run plan review is unavailable for run %q", run.RunID)
	}
	feedback := strings.TrimSpace(review.FeedbackMarkdown)
	switch review.Decision {
	case state.RunPlanReviewDecisionApproved:
		if feedback == "" {
			return "Operator approval is recorded. The plan is approved, and execution may proceed.", nil
		}
		return strings.TrimSpace(fmt.Sprintf(`Operator approval is recorded. The plan is approved.

Guidance preserved with the work order:
%s`, feedback)), nil
	case state.RunPlanReviewDecisionNeedsRework:
		if feedback == "" {
			return "Operator rework is recorded. The plan author must revise the plan before execution may continue.", nil
		}
		return strings.TrimSpace(fmt.Sprintf(`Operator rework is recorded. The plan author must revise the plan before execution may continue.

Required rework guidance:
%s`, feedback)), nil
	default:
		return "", fmt.Errorf("unsupported plan review decision %q for run %q", review.Decision, run.RunID)
	}
}

func (o *orchestrationLoop) ensureDecisionNodeAttempts(ctx context.Context, run runSnapshot, nodes []nodeDefinition, occurredAt string) error {
	if run.PlanState != "awaiting_plan_decision" {
		return nil
	}
	latest, err := o.loadLatestAttempts(ctx, run.RunID)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		behavior := coreworkflow.BehaviorForNode(node.NodeID, node.NodeClass)
		if !behavior.WaitsForRecordedPlanDecision {
			continue
		}
		attempt, ok := latest[node.NodeID]
		if ok {
			switch attempt.NodeStatus {
			case "pending", "ready", "starting", "running", "blocked":
				continue
			}
		}
		if _, err := o.transitionService().QueuePendingNodeWave(ctx, state.QueuePendingNodeWaveRequest{
			RunID:      run.RunID,
			NodeIDs:    []string{node.NodeID},
			OccurredAt: occurredAt,
		}); err != nil {
			return err
		}
	}
	return nil
}

func latestRunPlanReview(ctx context.Context, db *sql.DB, runID string) (state.RunPlanReview, bool, error) {
	if db == nil {
		return state.RunPlanReview{}, false, fmt.Errorf("plan review store is not configured")
	}
	reviews, err := state.NewRunPlanReviewStore(db).ListRunPlanReviews(ctx, runID, 1)
	if err != nil {
		return state.RunPlanReview{}, false, err
	}
	if len(reviews) == 0 {
		return state.RunPlanReview{}, false, nil
	}
	return reviews[0], true, nil
}
