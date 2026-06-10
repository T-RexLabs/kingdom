package orchestration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"kingdom/internal/core/workflow"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/state"
)

const runTerminalizedTransitionAuthoritySource = "state.run_terminalization"

func (o *orchestrationLoop) finalizeRunState(ctx context.Context, run runSnapshot, nodes []nodeDefinition, edges []edgeDefinition) error {
	latest, err := o.loadLatestAttempts(ctx, run.RunID)
	if err != nil {
		return err
	}
	graph, err := workflowGraphFromRunShape(run, nodes, edges)
	if err != nil {
		return err
	}
	for _, classID := range []string{workflow.NodeClassExecutionReview, workflow.NodeClassValidationGates} {
		shouldWait, err := o.hasUnsettledFailedLoopScope(ctx, run.RunID, graph, classID)
		if err != nil {
			return err
		}
		if shouldWait {
			return nil
		}
	}

	summary := summarizeRunClosure(run, nodes, edges, latest)

	now := o.now().UTC().Format(time.RFC3339)
	attemptID := "run:" + run.RunID + ":status"

	if summary.HasFailed {
		queued, err := o.resumeEligibleExecutionLoops(ctx, run, now)
		if err != nil {
			return err
		}
		if queued {
			return nil
		}

		refreshedLatest, err := o.loadLatestAttempts(ctx, run.RunID)
		if err != nil {
			return err
		}
		if latestAttemptsChanged(latest, refreshedLatest) {
			return nil
		}

		eventSeq := o.nextEventSeq(ctx, attemptID)
		if err := o.transition.TerminalizeRunAndCancelLiveNodes(ctx, state.RunTerminalizationRequest{
			Transition: state.TransitionRequest{
				TransitionID:    fmt.Sprintf("tr_orch_%s_run_failed_%d", run.RunID, eventSeq),
				EventID:         fmt.Sprintf("evt_orch_%s_run_failed_%d", run.RunID, eventSeq),
				RunID:           run.RunID,
				Entity:          state.EntityTypeRun,
				FromState:       "running",
				ToState:         "failed",
				ActorID:         "runtime_orchestrator",
				RoleID:          "chancellor",
				AuthoritySource: "runtime.orchestrator",
				ReasonCode:      "dependency_blocked",
				ReasonText:      "node failure propagated to run",
				CorrelationID:   "corr_orch_" + run.RunID,
				CausationID:     "caus_orch_" + run.RunID + "_run_failed",
				OccurredAt:      now,
				AttemptID:       attemptID,
				SchemaVersion:   "v1",
				EventType:       "run.state_changed",
				EventSeq:        eventSeq,
				PayloadJSON:     fmt.Sprintf(`{"run_id":%q,"status":%q}`, run.RunID, "failed"),
			},
		}); err != nil {
			return fmt.Errorf("mark run failed after node failure: %w", err)
		}
		return nil
	}
	if len(summary.ReplayNodeIDs) > 0 {
		return nil
	}

	if summary.HasInFlight {
		return nil
	}

	_ = o.closeRunSessionLeases(ctx, run.RunID, now)

	if !run.AutonomousApprovals {
		eventSeq := o.nextEventSeq(ctx, attemptID)
		awaitingPayload, err := json.Marshal(map[string]any{
			"run_id":   run.RunID,
			"status":   "awaiting_human_confirmation",
			"decision": "confirm_or_rework_required",
		})
		if err != nil {
			return fmt.Errorf("marshal awaiting human confirmation payload: %w", err)
		}
		if err := o.transition.Apply(ctx, state.TransitionRequest{
			TransitionID:    fmt.Sprintf("tr_orch_%s_run_awaiting_human_confirmation_%d", run.RunID, eventSeq),
			EventID:         fmt.Sprintf("evt_orch_%s_run_awaiting_human_confirmation_%d", run.RunID, eventSeq),
			RunID:           run.RunID,
			Entity:          state.EntityTypeRun,
			FromState:       "running",
			ToState:         "awaiting_human_confirmation",
			ActorID:         "runtime_orchestrator",
			RoleID:          "king",
			AuthoritySource: "runtime.orchestrator",
			ReasonCode:      "governance_decision",
			ReasonText:      "king recommendation recorded; awaiting allfather confirm/rework",
			CorrelationID:   "corr_orch_" + run.RunID,
			CausationID:     "caus_orch_" + run.RunID + "_awaiting_human_confirmation",
			OccurredAt:      now,
			AttemptID:       attemptID,
			SchemaVersion:   "v1",
			EventType:       "run.state_changed",
			EventSeq:        eventSeq,
			PayloadJSON:     string(awaitingPayload),
		}); err != nil {
			return fmt.Errorf("mark run awaiting human confirmation: %w", err)
		}
		return nil
	}

	successPayload := map[string]any{"run_id": run.RunID, "status": "succeeded"}
	reasonText := "all node attempts completed successfully"
	payloadRaw, err := json.Marshal(successPayload)
	if err != nil {
		return fmt.Errorf("marshal run success payload: %w", err)
	}
	eventSeq := o.nextEventSeq(ctx, attemptID)

	if err := o.transition.Apply(ctx, state.TransitionRequest{
		TransitionID:    fmt.Sprintf("tr_orch_%s_run_succeeded_%d", run.RunID, eventSeq),
		EventID:         fmt.Sprintf("evt_orch_%s_run_succeeded_%d", run.RunID, eventSeq),
		RunID:           run.RunID,
		Entity:          state.EntityTypeRun,
		FromState:       "running",
		ToState:         "succeeded",
		ActorID:         "runtime_orchestrator",
		RoleID:          "king",
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      "execution_completed",
		ReasonText:      reasonText,
		CorrelationID:   "corr_orch_" + run.RunID,
		CausationID:     "caus_orch_" + run.RunID + "_run_succeeded",
		OccurredAt:      now,
		AttemptID:       attemptID,
		SchemaVersion:   "v1",
		EventType:       "run.state_changed",
		EventSeq:        eventSeq,
		PayloadJSON:     string(payloadRaw),
	}); err != nil {
		return fmt.Errorf("mark run succeeded: %w", err)
	}

	return nil
}

type runClosureSummary struct {
	HasFailed     bool
	HasInFlight   bool
	ReplayNodeIDs []string
}

func summarizeRunClosure(run runSnapshot, nodes []nodeDefinition, edges []edgeDefinition, latest map[string]nodeAttempt) runClosureSummary {
	replayNeeded := collectReplayNeededCancelledNodes(run, nodes, edges, latest)
	summary := runClosureSummary{ReplayNodeIDs: make([]string, 0, len(replayNeeded))}
	for _, node := range nodes {
		attempt, ok := latest[node.NodeID]
		if !ok {
			summary.HasInFlight = true
			continue
		}
		effectiveStatus, awaitingDurableSettlement := finalizationNodeStatus(attempt)
		if awaitingDurableSettlement {
			summary.HasInFlight = true
			continue
		}
		switch effectiveStatus {
		case "failed":
			summary.HasFailed = true
		case "cancelled":
			if _, replay := replayNeeded[node.NodeID]; replay {
				summary.ReplayNodeIDs = append(summary.ReplayNodeIDs, node.NodeID)
				continue
			}
			if !isRunTerminalizedCancelledAttempt(attempt) {
				summary.HasFailed = true
			}
		case "pending", "ready", "starting", "running", "blocked":
			if effectiveStatus == "pending" && isNodeTerminallyUnreachableWithReplay(run, node.NodeID, latest, edges, replayNeeded) {
				continue
			}
			summary.HasInFlight = true
		}
	}
	sort.Strings(summary.ReplayNodeIDs)
	return summary
}

func latestAttemptsChanged(previous, current map[string]nodeAttempt) bool {
	if len(previous) != len(current) {
		return true
	}
	for nodeID, before := range previous {
		after, ok := current[nodeID]
		if !ok {
			return true
		}
		if before.NodeRunID != after.NodeRunID ||
			before.Attempt != after.Attempt ||
			before.Wave != after.Wave ||
			before.NodeStatus != after.NodeStatus ||
			before.TransitionReasonCode != after.TransitionReasonCode ||
			before.DurableState != after.DurableState {
			return true
		}
	}
	return false
}

func (o *orchestrationLoop) repairFailedRunAfterRecoveredExecution(ctx context.Context, runID, occurredAt string) error {
	if o == nil || o.db == nil || o.transition == nil {
		return nil
	}
	run, found, err := o.loadRunSnapshotForRepair(ctx, runID)
	if err != nil || !found {
		return err
	}
	if run.RunStatus != "failed" {
		return nil
	}
	return o.reconcileRecoverableFailedRun(ctx, run, occurredAt)
}

func (o *orchestrationLoop) repairRecoverableFailedRuns(ctx context.Context, occurredAt string) error {
	if o == nil || o.db == nil || o.transition == nil {
		return nil
	}
	rows, err := o.db.QueryContext(ctx, `
SELECT run_id
FROM workflow_runs
WHERE run_status = 'failed'
ORDER BY run_id ASC`)
	if err != nil {
		return fmt.Errorf("list failed runs for repair: %w", err)
	}
	defer rows.Close()
	runIDs := make([]string, 0)
	for rows.Next() {
		var runID string
		if err := rows.Scan(&runID); err != nil {
			return fmt.Errorf("scan failed run for repair: %w", err)
		}
		runIDs = append(runIDs, runID)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate failed runs for repair: %w", err)
	}
	for _, runID := range runIDs {
		if err := o.repairFailedRunAfterRecoveredExecution(ctx, runID, occurredAt); err != nil {
			return err
		}
	}
	return nil
}

func (o *orchestrationLoop) reconcileRecoverableFailedRun(ctx context.Context, run runSnapshot, occurredAt string) error {
	nodes, edges, err := o.loadWorkflowGraph(ctx, run)
	if err != nil {
		return err
	}
	graph, err := workflowGraphFromRunShape(run, nodes, edges)
	if err != nil {
		return err
	}
	for _, classID := range []string{workflow.NodeClassExecutionReview, workflow.NodeClassValidationGates} {
		shouldWait, err := o.hasUnsettledFailedLoopScope(ctx, run.RunID, graph, classID)
		if err != nil {
			return err
		}
		if shouldWait {
			return nil
		}
	}
	latest, err := o.loadLatestAttempts(ctx, run.RunID)
	if err != nil {
		return err
	}
	summary := summarizeRunClosure(run, nodes, edges, latest)
	queued, err := o.repairEligibleExecutionLoops(ctx, run, occurredAt)
	if err != nil {
		return err
	}
	if queued {
		return nil
	}
	if summary.HasFailed {
		return nil
	}
	if err := o.reopenFailedRunForRecovery(ctx, run.RunID, occurredAt); err != nil {
		return err
	}
	run.RunStatus = "running"
	if len(summary.ReplayNodeIDs) > 0 {
		_, err := o.queuePendingNodeWave(ctx, run.RunID, summary.ReplayNodeIDs, nil, occurredAt)
		return err
	}
	if summary.HasInFlight {
		return nil
	}
	return o.finalizeRunState(ctx, run, nodes, edges)
}

func workflowGraphFromRunShape(run runSnapshot, nodes []nodeDefinition, edges []edgeDefinition) (*workflow.Graph, error) {
	definition := workflow.Definition{WorkflowID: strings.TrimSpace(run.WorkflowDefinitionID)}
	for _, node := range nodes {
		definition.Nodes = append(definition.Nodes, workflow.Node{
			NodeID:       node.NodeID,
			RequiredRole: node.RequiredRole,
			NodeClass:    node.NodeClass,
		})
	}
	for _, edge := range edges {
		definition.Edges = append(definition.Edges, workflow.Edge{
			From: edge.FromNodeID,
			To:   edge.ToNodeID,
			On:   edge.On,
		})
	}
	graph, err := workflow.NewParser(nil).ParseDefinition(definition)
	if err != nil {
		return nil, fmt.Errorf("build workflow graph for run %q: %w", run.RunID, err)
	}
	return graph, nil
}

func (o *orchestrationLoop) reopenFailedRunForRecovery(ctx context.Context, runID, occurredAt string) error {
	attemptID := "run:" + runID + ":status"
	eventSeq := o.nextEventSeq(ctx, attemptID)
	if err := o.transition.Apply(ctx, state.TransitionRequest{
		TransitionID:    fmt.Sprintf("tr_orch_%s_run_reopened_%d", runID, eventSeq),
		EventID:         fmt.Sprintf("evt_orch_%s_run_reopened_%d", runID, eventSeq),
		RunID:           runID,
		Entity:          state.EntityTypeRun,
		FromState:       "failed",
		ToState:         "running",
		ActorID:         "runtime_orchestrator",
		RoleID:          "chancellor",
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      "durable_completion_recovered",
		ReasonText:      "run reopened after durable execution recovery",
		CorrelationID:   "corr_orch_" + runID,
		CausationID:     "caus_orch_" + runID + "_run_reopened",
		OccurredAt:      occurredAt,
		AttemptID:       attemptID,
		SchemaVersion:   "v1",
		EventType:       "run.state_changed",
		EventSeq:        eventSeq,
		PayloadJSON:     fmt.Sprintf(`{"run_id":%q,"status":%q}`, runID, "running"),
	}); err != nil {
		var stateErr *state.StateConflictError
		var illegalErr *state.IllegalTransitionError
		if errors.As(err, &stateErr) || errors.As(err, &illegalErr) {
			return nil
		}
		return err
	}
	return nil
}

func (o *orchestrationLoop) loadRunSnapshotForRepair(ctx context.Context, runID string) (runSnapshot, bool, error) {
	if o == nil || o.db == nil || strings.TrimSpace(runID) == "" {
		return runSnapshot{}, false, nil
	}
	var run runSnapshot
	var autonomousApprovalsInt int
	var publishMode string
	var modelSelectionRaw string
	err := o.db.QueryRowContext(ctx,
		`SELECT run_id, run_status, plan_state, COALESCE(autonomous_approvals, 1), COALESCE(publish_mode, 'direct'), COALESCE(model_selection_json, '{}'), workflow_definition_id
		 FROM workflow_runs
		 WHERE run_id = ?
		 LIMIT 1`, strings.TrimSpace(runID),
	).Scan(&run.RunID, &run.RunStatus, &run.PlanState, &autonomousApprovalsInt, &publishMode, &modelSelectionRaw, &run.WorkflowDefinitionID)
	if err == sql.ErrNoRows {
		return runSnapshot{}, false, nil
	}
	if err != nil {
		return runSnapshot{}, false, err
	}
	run.AutonomousApprovals = autonomousApprovalsInt != 0
	run.PublishMode = normalizePublishMode(publishMode)
	run.ModelSelection = decodeRunModelSelection(modelSelectionRaw)
	return run, true, nil
}

func finalizationNodeStatus(attempt nodeAttempt) (string, bool) {
	if !attempt.HasExecutionRecord || attempt.ExecutionMode != executionstore.ExecutionModeProviderBacked {
		return attempt.NodeStatus, false
	}
	trimmedStatus := strings.TrimSpace(attempt.NodeStatus)
	if trimmedStatus != "succeeded" && trimmedStatus != "failed" && trimmedStatus != "cancelled" {
		return trimmedStatus, false
	}
	switch strings.TrimSpace(attempt.DurableState) {
	case "committed_success", "committed_failure", "committed_cancelled":
		return trimmedStatus, false
	default:
		return trimmedStatus, true
	}
}

func isRunTerminalizedCancelledAttempt(attempt nodeAttempt) bool {
	return strings.TrimSpace(attempt.NodeStatus) == "cancelled" &&
		strings.TrimSpace(attempt.TransitionReasonCode) == "run_terminalized" &&
		strings.TrimSpace(attempt.TransitionAuthority) == runTerminalizedTransitionAuthoritySource
}

func collectReplayNeededCancelledNodes(run runSnapshot, nodes []nodeDefinition, edges []edgeDefinition, latest map[string]nodeAttempt) map[string]struct{} {
	candidates := make(map[string]struct{})
	for _, node := range nodes {
		attempt, ok := latest[node.NodeID]
		if !ok || !isRunTerminalizedCancelledAttempt(attempt) {
			continue
		}
		candidates[node.NodeID] = struct{}{}
	}
	replayNeeded := make(map[string]struct{}, len(candidates))
	changed := true
	for changed {
		changed = false
		for nodeID := range candidates {
			if _, exists := replayNeeded[nodeID]; exists {
				continue
			}
			if !nodeCanBecomeReachable(run, nodeID, latest, edges, replayNeeded) {
				continue
			}
			replayNeeded[nodeID] = struct{}{}
			changed = true
		}
	}
	return replayNeeded
}

func nodeCanBecomeReachable(run runSnapshot, nodeID string, latest map[string]nodeAttempt, edges []edgeDefinition, replayNeeded map[string]struct{}) bool {
	incoming := make([]edgeDefinition, 0)
	for _, edge := range edges {
		if edge.ToNodeID == nodeID {
			incoming = append(incoming, edge)
		}
	}
	if len(incoming) == 0 {
		return true
	}

	hasMatchedCondition := false
	hasResolvableDependency := false
	for _, edge := range incoming {
		attempt, ok := latest[edge.FromNodeID]
		if !ok {
			return false
		}
		if conditionMatches(edge.On, attempt.NodeStatus, run.PlanState) {
			hasMatchedCondition = true
		}
		if _, replay := replayNeeded[edge.FromNodeID]; replay {
			hasResolvableDependency = true
		}
		switch attempt.NodeStatus {
		case "pending", "ready", "starting", "running", "blocked":
			hasResolvableDependency = true
		}
	}
	return hasMatchedCondition || hasResolvableDependency
}

func isNodeTerminallyUnreachable(run runSnapshot, nodeID string, latest map[string]nodeAttempt, edges []edgeDefinition) bool {
	return isNodeTerminallyUnreachableWithReplay(run, nodeID, latest, edges, nil)
}

func isNodeTerminallyUnreachableWithReplay(run runSnapshot, nodeID string, latest map[string]nodeAttempt, edges []edgeDefinition, replayNeeded map[string]struct{}) bool {
	incoming := make([]edgeDefinition, 0)
	for _, edge := range edges {
		if edge.ToNodeID == nodeID {
			incoming = append(incoming, edge)
		}
	}
	if len(incoming) == 0 {
		return false
	}

	hasMatchedCondition := false
	hasNonTerminalDependency := false
	for _, edge := range incoming {
		attempt, ok := latest[edge.FromNodeID]
		if !ok {
			return false
		}
		if conditionMatches(edge.On, attempt.NodeStatus, run.PlanState) {
			hasMatchedCondition = true
		}
		if _, replay := replayNeeded[edge.FromNodeID]; replay {
			hasNonTerminalDependency = true
		}
		switch attempt.NodeStatus {
		case "pending", "ready", "starting", "running", "blocked":
			hasNonTerminalDependency = true
		}
	}

	return !hasMatchedCondition && !hasNonTerminalDependency
}
