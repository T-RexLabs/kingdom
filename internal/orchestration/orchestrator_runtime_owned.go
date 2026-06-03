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
	case coreworkflow.NodeClassDecreeIntake:
		return strings.TrimSpace(`Royal intake is complete: the decree now stands before the Crown, and the advisory banners are raised.

The realm will proceed through advisor counsel, chancellor synthesis, and council planning before the King is called to grant assent.`), nil
	case coreworkflow.NodeClassKingPlanDecision:
		return o.runtimeOwnedKingPlanDecisionResponse(ctx, run)
	case coreworkflow.NodeClassExecutionKickoff:
		return strings.TrimSpace(`Execution kickoff is complete: the approved plan now stands opened for implementation, and the builder plus review and validation lanes are active.`), nil
	case coreworkflow.NodeClassRoyalClosure:
		if run.AutonomousApprovals {
			return strings.TrimSpace(`Royal closure is complete: the decree is sealed, the final record is entered, and the realm may reckon this matter concluded.`), nil
		}
		return strings.TrimSpace(`Royal closure is complete: the decree's final record is sealed and now awaits the Allfather's final confirmation before the realm may call it done.`), nil
	default:
		return "", fmt.Errorf("runtime-owned response is not defined for node class %q", resolvedNodeClass(node.NodeID, node.NodeClass))
	}
}

func (o *orchestrationLoop) runtimeOwnedKingPlanDecisionResponse(ctx context.Context, run runSnapshot) (string, error) {
	if run.AutonomousApprovals {
		return strings.TrimSpace(`Royal assent is recorded automatically under autonomous governance. The council's plan now stands approved, and execution may proceed.`), nil
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
			return strings.TrimSpace(`Allfather approval is recorded. The council's plan stands confirmed, and execution may proceed.`), nil
		}
		return strings.TrimSpace(fmt.Sprintf(`Allfather approval is recorded. The council's plan stands confirmed.

Guidance preserved with the decree:
%s`, feedback)), nil
	case state.RunPlanReviewDecisionNeedsRework:
		if feedback == "" {
			return strings.TrimSpace(`Allfather rework is recorded. The council must revise the plan before execution may continue.`), nil
		}
		return strings.TrimSpace(fmt.Sprintf(`Allfather rework is recorded. The council must revise the plan before execution may continue.

Required rework guidance:
%s`, feedback)), nil
	default:
		return "", fmt.Errorf("unsupported plan review decision %q for run %q", review.Decision, run.RunID)
	}
}

func (o *orchestrationLoop) ensureDecisionNodeAttempts(ctx context.Context, run runSnapshot, nodes []nodeDefinition, occurredAt string) error {
	if run.PlanState != "awaiting_king_decision" {
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
			case "pending", "ready", "running", "blocked":
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
