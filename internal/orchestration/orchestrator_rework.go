package orchestration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"

	"kingdom/internal/core/workflow"
	"kingdom/internal/state"
)

type executionLoopResumePlan struct {
	nodeIDs             []string
	ignoreActiveNodeIDs []string
	eligible            bool
	capReached          bool
}

func (o *orchestrationLoop) queueExecutionReworkIfEligible(ctx context.Context, run runSnapshot, occurredAt, failedNodeID string) (bool, bool, error) {
	return o.queueExecutionReworkIfEligibleForClass(ctx, run, occurredAt, failedNodeID, "")
}

func (o *orchestrationLoop) queueExecutionReworkIfEligibleForClass(ctx context.Context, run runSnapshot, occurredAt, failedNodeID, failedNodeClass string) (bool, bool, error) {
	if run.RunStatus != "running" {
		return false, false, nil
	}
	plan, err := o.validationReworkResumePlan(ctx, run.RunID, failedNodeID)
	if err != nil {
		return false, false, err
	}
	if !plan.eligible {
		return false, plan.capReached, nil
	}
	queued, err := o.queuePendingNodeWave(ctx, run.RunID, plan.nodeIDs, plan.ignoreActiveNodeIDs, occurredAt)
	if err != nil {
		return false, false, err
	}
	return queued, false, nil
}

func (o *orchestrationLoop) queueExecutionReviewReworkIfEligible(ctx context.Context, run runSnapshot, occurredAt, failedNodeID string) (bool, bool, error) {
	return o.queueExecutionReviewReworkIfEligibleForClass(ctx, run, occurredAt, failedNodeID, "")
}

func (o *orchestrationLoop) queueExecutionReviewReworkIfEligibleForClass(ctx context.Context, run runSnapshot, occurredAt, failedNodeID, failedNodeClass string) (bool, bool, error) {
	if run.RunStatus != "running" {
		return false, false, nil
	}
	plan, err := o.executionReviewReworkResumePlan(ctx, run.RunID, failedNodeID)
	if err != nil {
		return false, false, err
	}
	if !plan.eligible {
		return false, plan.capReached, nil
	}
	queued, err := o.queuePendingNodeWave(ctx, run.RunID, plan.nodeIDs, plan.ignoreActiveNodeIDs, occurredAt)
	if err != nil {
		return false, false, err
	}
	return queued, false, nil
}

func (o *orchestrationLoop) resumeEligibleExecutionLoops(ctx context.Context, run runSnapshot, occurredAt string) (bool, error) {
	return o.resumeEligibleExecutionLoopsForRun(ctx, run, occurredAt, false)
}

func (o *orchestrationLoop) repairEligibleExecutionLoops(ctx context.Context, run runSnapshot, occurredAt string) (bool, error) {
	return o.resumeEligibleExecutionLoopsForRun(ctx, run, occurredAt, true)
}

func (o *orchestrationLoop) resumeEligibleExecutionLoopsForRun(ctx context.Context, run runSnapshot, occurredAt string, reopenFailed bool) (bool, error) {
	for _, planFn := range []func(context.Context, string) (executionLoopResumePlan, error){
		func(ctx context.Context, runID string) (executionLoopResumePlan, error) {
			return o.executionReviewReworkResumePlan(ctx, runID, "")
		},
		func(ctx context.Context, runID string) (executionLoopResumePlan, error) {
			return o.validationReworkResumePlan(ctx, runID, "")
		},
	} {
		plan, err := planFn(ctx, run.RunID)
		if err != nil {
			return false, err
		}
		if !plan.eligible {
			continue
		}
		if run.RunStatus != "running" {
			if !reopenFailed || run.RunStatus != "failed" {
				return false, nil
			}
			if err := o.reopenFailedRunForRecovery(ctx, run.RunID, occurredAt); err != nil {
				return false, err
			}
			run.RunStatus = "running"
		}
		queued, err := o.queuePendingNodeWave(ctx, run.RunID, plan.nodeIDs, plan.ignoreActiveNodeIDs, occurredAt)
		if err != nil {
			return false, err
		}
		return queued, nil
	}
	return false, nil
}

func (o *orchestrationLoop) executionReviewReworkResumePlan(ctx context.Context, runID, failedNodeID string) (executionLoopResumePlan, error) {
	settings, err := o.loadLoopSettingsForRun(ctx, runID)
	if err != nil {
		return executionLoopResumePlan{}, err
	}
	graph, err := o.workflowGraphForRun(ctx, runID)
	if err != nil {
		return executionLoopResumePlan{}, err
	}
	latest, err := o.loadLatestAttempts(ctx, runID)
	if err != nil {
		return executionLoopResumePlan{}, err
	}
	candidateNodeIDs := candidateLoopNodeIDs(graph, latest, workflow.NodeClassExecutionReview, failedNodeID)
	for _, candidateNodeID := range candidateNodeIDs {
		scopePlan, ok, err := workflow.ResolveExecutionLoopScopePlan(graph, candidateNodeID)
		if err != nil {
			return executionLoopResumePlan{}, err
		}
		if !ok || scopePlan.NodeClass != workflow.NodeClassExecutionReview {
			continue
		}
		latestReviewWave, err := o.latestWaveForNodeIDs(ctx, runID, scopePlan.ParticipantNodeIDs)
		if err != nil {
			return executionLoopResumePlan{}, err
		}
		if latestReviewWave <= 0 {
			continue
		}
		participantNodeIDs, err := o.participantNodeIDsForWave(ctx, runID, scopePlan.ParticipantNodeIDs, latestReviewWave, false)
		if err != nil {
			return executionLoopResumePlan{}, err
		}
		if len(participantNodeIDs) == 0 {
			continue
		}
		waveOutcome, err := o.reviewWaveOutcomeForNodeIDs(ctx, runID, participantNodeIDs, latestReviewWave)
		if err != nil {
			return executionLoopResumePlan{}, err
		}
		if !waveOutcome.Settled || !waveOutcome.HasRework || waveOutcome.HasHardFailure {
			continue
		}
		failedReviewCycles, err := o.reviewReworkCycleCountForNodeIDs(ctx, runID, participantNodeIDs)
		if err != nil {
			return executionLoopResumePlan{}, err
		}
		started, capReached := workflow.CanQueueExecutionReviewRework(settings, failedReviewCycles)
		if !started {
			if strings.TrimSpace(failedNodeID) != "" {
				return executionLoopResumePlan{capReached: capReached}, nil
			}
			continue
		}
		replayNodeIDs := replayNodeIDsForLoopParticipants(graph, scopePlan, participantNodeIDs)
		if hasActiveLoopScopeAttempts(latest, replayNodeIDs) {
			continue
		}
		plan := executionLoopResumePlan{eligible: true, nodeIDs: replayNodeIDs}
		if strings.TrimSpace(failedNodeID) != "" {
			plan.ignoreActiveNodeIDs = []string{failedNodeID}
		}
		return plan, nil
	}
	return executionLoopResumePlan{}, nil
}

func (o *orchestrationLoop) validationReworkResumePlan(ctx context.Context, runID, failedNodeID string) (executionLoopResumePlan, error) {
	settings, err := o.loadLoopSettingsForRun(ctx, runID)
	if err != nil {
		return executionLoopResumePlan{}, err
	}
	graph, err := o.workflowGraphForRun(ctx, runID)
	if err != nil {
		return executionLoopResumePlan{}, err
	}
	latest, err := o.loadLatestAttempts(ctx, runID)
	if err != nil {
		return executionLoopResumePlan{}, err
	}
	candidateNodeIDs := candidateLoopNodeIDs(graph, latest, workflow.NodeClassValidationGates, failedNodeID)
	for _, candidateNodeID := range candidateNodeIDs {
		scopePlan, ok, err := workflow.ResolveExecutionLoopScopePlan(graph, candidateNodeID)
		if err != nil {
			return executionLoopResumePlan{}, err
		}
		if !ok || scopePlan.NodeClass != workflow.NodeClassValidationGates {
			continue
		}
		latestValidationWave, err := o.latestWaveForNodeIDs(ctx, runID, scopePlan.ParticipantNodeIDs)
		if err != nil {
			return executionLoopResumePlan{}, err
		}
		if latestValidationWave <= 0 {
			continue
		}
		participantNodeIDs, err := o.participantNodeIDsForWave(ctx, runID, scopePlan.ParticipantNodeIDs, latestValidationWave, false)
		if err != nil {
			return executionLoopResumePlan{}, err
		}
		if len(participantNodeIDs) == 0 {
			continue
		}
		waveOutcome, err := o.validationWaveOutcomeForNodeIDs(ctx, runID, participantNodeIDs, latestValidationWave)
		if err != nil {
			return executionLoopResumePlan{}, err
		}
		if !waveOutcome.Settled || !waveOutcome.HasRework || waveOutcome.HasHardFailure {
			continue
		}
		failedValidationWaves, err := o.validationReworkWaveCountForNodeIDs(ctx, runID, participantNodeIDs)
		if err != nil {
			return executionLoopResumePlan{}, err
		}
		started, capReached := workflow.CanQueueValidationRework(settings, failedValidationWaves)
		if !started {
			if strings.TrimSpace(failedNodeID) != "" {
				return executionLoopResumePlan{capReached: capReached}, nil
			}
			continue
		}
		replayNodeIDs := replayNodeIDsForLoopParticipants(graph, scopePlan, participantNodeIDs)
		if hasActiveLoopScopeAttempts(latest, replayNodeIDs) {
			continue
		}
		return executionLoopResumePlan{eligible: true, nodeIDs: replayNodeIDs}, nil
	}
	return executionLoopResumePlan{}, nil
}

func (o *orchestrationLoop) queuePendingNodeWave(ctx context.Context, runID string, nodeIDs, ignoreActiveNodeIDs []string, occurredAt string) (bool, error) {
	result, err := o.transitionService().QueuePendingNodeWave(ctx, state.QueuePendingNodeWaveRequest{
		RunID:               runID,
		NodeIDs:             nodeIDs,
		IgnoreActiveNodeIDs: ignoreActiveNodeIDs,
		OccurredAt:          occurredAt,
	})
	if err != nil {
		return false, err
	}
	return result.Queued, nil
}

func (o *orchestrationLoop) canStartExecutionReworkCycle(ctx context.Context, runID string) (bool, bool, error) {
	settings, err := o.loadLoopSettingsForRun(ctx, runID)
	if err != nil {
		return false, false, err
	}
	failedValidationWaves, err := o.validationReworkWaveCount(ctx, runID)
	if err != nil {
		return false, false, err
	}
	started, capReached := workflow.CanStartValidationReworkCycle(settings, failedValidationWaves)
	return started, capReached, nil
}

func (o *orchestrationLoop) canStartExecutionReviewCycle(ctx context.Context, runID string) (bool, bool, error) {
	settings, err := o.loadLoopSettingsForRun(ctx, runID)
	if err != nil {
		return false, false, err
	}
	failedReviewCycles, err := o.reviewReworkCycleCount(ctx, runID)
	if err != nil {
		return false, false, err
	}
	started, capReached := workflow.CanStartExecutionReviewCycle(settings, failedReviewCycles)
	return started, capReached, nil
}

func (o *orchestrationLoop) reviewReworkCycleCount(ctx context.Context, runID string) (int, error) {
	if o == nil || o.db == nil {
		return 0, fmt.Errorf("database is not configured")
	}
	return state.CountReviewReworkWaves(ctx, o.db, runID)
}

func (o *orchestrationLoop) reviewReworkCycleCountBeforeWave(ctx context.Context, runID string, wave int) (int, error) {
	if o == nil || o.db == nil {
		return 0, fmt.Errorf("database is not configured")
	}
	return state.CountReviewReworkWavesBeforeWave(ctx, o.db, runID, wave)
}

func (o *orchestrationLoop) validationReworkWaveCount(ctx context.Context, runID string) (int, error) {
	if o == nil || o.db == nil {
		return 0, fmt.Errorf("database is not configured")
	}
	return state.CountValidationReworkWaves(ctx, o.db, runID)
}

func (o *orchestrationLoop) validationReworkWaveCountBeforeWave(ctx context.Context, runID string, wave int) (int, error) {
	if o == nil || o.db == nil {
		return 0, fmt.Errorf("database is not configured")
	}
	return state.CountValidationReworkWavesBeforeWave(ctx, o.db, runID, wave)
}

func (o *orchestrationLoop) reviewReworkCycleCountForNodeIDs(ctx context.Context, runID string, nodeIDs []string) (int, error) {
	if o == nil || o.db == nil {
		return 0, fmt.Errorf("database is not configured")
	}
	return state.CountReviewReworkWavesForNodeIDs(ctx, o.db, runID, nodeIDs)
}

func (o *orchestrationLoop) reviewReworkCycleCountBeforeWaveForNodeIDs(ctx context.Context, runID string, nodeIDs []string, wave int) (int, error) {
	if o == nil || o.db == nil {
		return 0, fmt.Errorf("database is not configured")
	}
	return state.CountReviewReworkWavesBeforeWaveForNodeIDs(ctx, o.db, runID, nodeIDs, wave)
}

func (o *orchestrationLoop) validationReworkWaveCountForNodeIDs(ctx context.Context, runID string, nodeIDs []string) (int, error) {
	if o == nil || o.db == nil {
		return 0, fmt.Errorf("database is not configured")
	}
	return state.CountValidationReworkWavesForNodeIDs(ctx, o.db, runID, nodeIDs)
}

func (o *orchestrationLoop) validationReworkWaveCountBeforeWaveForNodeIDs(ctx context.Context, runID string, nodeIDs []string, wave int) (int, error) {
	if o == nil || o.db == nil {
		return 0, fmt.Errorf("database is not configured")
	}
	return state.CountValidationReworkWavesBeforeWaveForNodeIDs(ctx, o.db, runID, nodeIDs, wave)
}

func (o *orchestrationLoop) reviewWaveOutcomeForWave(ctx context.Context, runID string, wave int) (state.ReviewWaveOutcome, error) {
	if o == nil || o.db == nil {
		return state.ReviewWaveOutcome{}, fmt.Errorf("database is not configured")
	}
	return state.ReviewWaveOutcomeForWave(ctx, o.db, runID, wave)
}

func candidateLoopNodeIDs(graph *workflow.Graph, latest map[string]nodeAttempt, classID, failedNodeID string) []string {
	trimmedFailedNodeID := strings.TrimSpace(failedNodeID)
	if trimmedFailedNodeID != "" {
		return []string{trimmedFailedNodeID}
	}
	type candidate struct {
		nodeID  string
		wave    int
		attempt int
	}
	candidates := make([]candidate, 0)
	for nodeID, attempt := range latest {
		node, ok := graph.Nodes[nodeID]
		if !ok || workflow.ResolveNodeClass(node.NodeID, node.NodeClass) != classID {
			continue
		}
		if attempt.NodeStatus != "failed" {
			continue
		}
		candidates = append(candidates, candidate{nodeID: nodeID, wave: attempt.Wave, attempt: attempt.Attempt})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].wave != candidates[j].wave {
			return candidates[i].wave > candidates[j].wave
		}
		if candidates[i].attempt != candidates[j].attempt {
			return candidates[i].attempt > candidates[j].attempt
		}
		return candidates[i].nodeID < candidates[j].nodeID
	})
	nodeIDs := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		nodeIDs = append(nodeIDs, candidate.nodeID)
	}
	return nodeIDs
}

func replayNodeIDsForLoopParticipants(graph *workflow.Graph, scopePlan workflow.ExecutionLoopScopePlan, participantNodeIDs []string) []string {
	replayNodeIDs := workflow.SliceNodeIDsBetween(graph, scopePlan.ReplayAnchorNodeIDs, participantNodeIDs, true)
	if len(replayNodeIDs) == 0 {
		replayNodeIDs = append([]string(nil), participantNodeIDs...)
	}
	return replayNodeIDs
}

func hasActiveLoopScopeAttempts(latest map[string]nodeAttempt, nodeIDs []string) bool {
	for _, nodeID := range nodeIDs {
		attempt, ok := latest[nodeID]
		if !ok {
			continue
		}
		switch attempt.NodeStatus {
		case "pending", "ready", "running":
			return true
		}
	}
	return false
}

func validationScopePlansForReplayAnchor(graph *workflow.Graph, replayAnchorNodeID string) ([]workflow.ExecutionLoopScopePlan, error) {
	if graph == nil {
		return nil, fmt.Errorf("workflow graph is required")
	}
	trimmedReplayAnchorNodeID := strings.TrimSpace(replayAnchorNodeID)
	if trimmedReplayAnchorNodeID == "" {
		return nil, nil
	}
	plans := make([]workflow.ExecutionLoopScopePlan, 0)
	seen := make(map[string]struct{})
	for _, nodeID := range graph.Topological {
		plan, ok, err := workflow.ResolveExecutionLoopScopePlan(graph, nodeID)
		if err != nil {
			return nil, err
		}
		if !ok || plan.NodeClass != workflow.NodeClassValidationGates {
			continue
		}
		if _, exists := seen[plan.ScopeID]; exists {
			continue
		}
		for _, anchorNodeID := range plan.ReplayAnchorNodeIDs {
			if anchorNodeID != trimmedReplayAnchorNodeID {
				continue
			}
			seen[plan.ScopeID] = struct{}{}
			plans = append(plans, plan)
			break
		}
	}
	return plans, nil
}

func (o *orchestrationLoop) enqueueNodeAttempt(ctx context.Context, runID, nodeID, occurredAt string) (int, error) {
	result, err := o.transitionService().QueuePendingNodeWave(ctx, state.QueuePendingNodeWaveRequest{
		RunID:      runID,
		NodeIDs:    []string{nodeID},
		OccurredAt: occurredAt,
	})
	if err != nil {
		var limitErr *state.NodeAttemptLimitExceededError
		if errors.As(err, &limitErr) {
			return 0, fmt.Errorf("max attempts reached for run=%s node=%s", runID, nodeID)
		}
		return 0, err
	}
	if !result.Queued || len(result.Attempts) != 1 {
		var maxAttempt int
		if err := o.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(attempt), 0) FROM node_runs WHERE run_id = ? AND node_id = ?`, runID, nodeID).Scan(&maxAttempt); err != nil {
			return 0, err
		}
		attemptLimit, err := o.attemptLimitForNodeRun(ctx, runID, nodeID)
		if err != nil {
			return 0, err
		}
		if maxAttempt >= attemptLimit {
			return 0, fmt.Errorf("max attempts reached for run=%s node=%s", runID, nodeID)
		}
		return 0, fmt.Errorf("node attempt queue skipped for run=%s node=%s", runID, nodeID)
	}
	return result.Attempts[0].Attempt, nil
}

func (o *orchestrationLoop) attemptLimitForNodeRun(ctx context.Context, runID, nodeID string) (int, error) {
	classID, err := o.nodeClassForRunNode(ctx, runID, nodeID)
	if err != nil {
		return 0, err
	}
	settings, err := o.loadLoopSettingsForRun(ctx, runID)
	if err != nil {
		return 0, err
	}
	limit := state.NodeAttemptLimitForClass(classID, settings)
	return limit, nil
}

func (o *orchestrationLoop) nodeClassForRunNode(ctx context.Context, runID, nodeID string) (string, error) {
	var nodeClass string
	err := o.db.QueryRowContext(ctx, `
SELECT wn.node_class
FROM workflow_runs wr
JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id
WHERE wr.run_id = ? AND wn.node_id = ?
LIMIT 1`, runID, nodeID).Scan(&nodeClass)
	if errors.Is(err, sql.ErrNoRows) {
		return resolvedNodeClass(nodeID, ""), nil
	}
	if err != nil {
		return "", err
	}
	return resolvedNodeClass(nodeID, nodeClass), nil
}

func (o *orchestrationLoop) loadLoopSettingsForRun(ctx context.Context, runID string) (workflow.LoopSettings, error) {
	var runUntilAllApprove int
	var maxExecutionReviewCycles sql.NullInt64
	var maxValidationReworkCycles sql.NullInt64
	var hasExecutionReview int
	var hasValidationGates int
	err := o.db.QueryRowContext(ctx, `
SELECT COALESCE(wr.run_until_all_approve, 0),
       wr.max_execution_review_cycles,
       wr.max_validation_rework_cycles,
       COALESCE(MAX(CASE WHEN wn.node_class = 'execution_review' THEN 1 ELSE 0 END), 0),
       COALESCE(MAX(CASE WHEN wn.node_class = 'validation_gates' THEN 1 ELSE 0 END), 0)
FROM workflow_runs wr
LEFT JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id
WHERE wr.run_id = ?
GROUP BY wr.run_id, wr.run_until_all_approve, wr.max_execution_review_cycles, wr.max_validation_rework_cycles`, runID).Scan(
		&runUntilAllApprove,
		&maxExecutionReviewCycles,
		&maxValidationReworkCycles,
		&hasExecutionReview,
		&hasValidationGates,
	)
	if err != nil {
		return workflow.LoopSettings{}, err
	}
	return workflow.NormalizeLoopSettings(workflow.LoopSettings{
		RunUntilAllApprove:        runUntilAllApprove != 0,
		HasExecutionReview:        hasExecutionReview != 0,
		HasValidationGates:        hasValidationGates != 0,
		MaxExecutionReviewCycles:  nullableLoopCap(maxExecutionReviewCycles),
		MaxValidationReworkCycles: nullableLoopCap(maxValidationReworkCycles),
	}), nil
}

func nullableLoopCap(value sql.NullInt64) int {
	if !value.Valid {
		return -1
	}
	return int(value.Int64)
}
