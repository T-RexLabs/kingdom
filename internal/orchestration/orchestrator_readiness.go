package orchestration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	coreworkflow "kingdom/internal/core/workflow"
	"kingdom/internal/state"
)

var guardedExecutionNodeClasses = map[string]struct{}{
	"execution_kickoff":   {},
	"delegated_execution": {},
	"execution_review":    {},
	"validation_gates":    {},
	"royal_closure":       {},
}

var governancePredecessorNodeClasses = []string{
	"decree_intake",
	"advisor_consultation",
	"advisory_synthesis",
	"plan_drafting",
	"king_plan_decision",
}

func (o *orchestrationLoop) promoteReadyNodes(ctx context.Context, run runSnapshot, nodes []nodeDefinition, edges []edgeDefinition, latest map[string]nodeAttempt, now string) error {
	nodeClasses := buildResolvedNodeClassIndex(nodes)
	for _, node := range nodes {
		attempt, ok := latest[node.NodeID]
		if !ok || attempt.NodeStatus != "pending" {
			continue
		}
		ready, readyErr := o.isNodeReady(ctx, run, node, nodeClasses, latest, edges)
		if readyErr != nil {
			return readyErr
		}
		if !ready {
			continue
		}

		attemptID := composeAttemptID(run.RunID, attempt.NodeID, attempt.Attempt)
		if err := o.transition.Apply(ctx, state.TransitionRequest{
			TransitionID:    fmt.Sprintf("tr_orch_%s_%s_%d_ready", run.RunID, attempt.NodeID, attempt.Attempt),
			EventID:         fmt.Sprintf("evt_orch_%s_%s_%d_ready", run.RunID, attempt.NodeID, attempt.Attempt),
			RunID:           run.RunID,
			NodeRunID:       sql.NullString{String: attempt.NodeRunID, Valid: true},
			Entity:          state.EntityTypeNode,
			FromState:       "pending",
			ToState:         "ready",
			ActorID:         "runtime_orchestrator",
			RoleID:          node.RequiredRole,
			AuthoritySource: "runtime.orchestrator",
			ReasonCode:      "orchestration_progress",
			ReasonText:      "dependencies satisfied",
			CorrelationID:   "corr_orch_" + run.RunID,
			CausationID:     fmt.Sprintf("caus_orch_%s_%s_ready", run.RunID, attempt.NodeRunID),
			OccurredAt:      now,
			AttemptID:       attemptID,
			SchemaVersion:   "v1",
			EventType:       "node.state_changed",
			EventSeq:        o.nextEventSeq(ctx, attemptID),
			PayloadJSON:     fmt.Sprintf(`{"node_run_id":%q,"node_id":%q,"state":%q}`, attempt.NodeRunID, node.NodeID, "ready"),
		}); err != nil {
			var illegal *state.IllegalTransitionError
			if errors.As(err, &illegal) {
				continue
			}
			return err
		}
		updated := attempt
		updated.NodeStatus = "ready"
		latest[node.NodeID] = updated
	}

	return nil
}

func (o *orchestrationLoop) isNodeReady(ctx context.Context, run runSnapshot, node nodeDefinition, nodeClasses map[string]string, latest map[string]nodeAttempt, edges []edgeDefinition) (bool, error) {
	currentClass := resolvedNodeClass(node.NodeID, node.NodeClass)
	behavior := coreworkflow.BehaviorForNode(node.NodeID, currentClass)
	if behavior.WaitsForRecordedPlanDecision && run.PlanState == "awaiting_king_decision" {
		return false, nil
	}

	incoming := make([]edgeDefinition, 0)
	for _, edge := range edges {
		if edge.ToNodeID == node.NodeID {
			incoming = append(incoming, edge)
		}
	}
	if len(incoming) == 0 {
		return true, nil
	}

	if _, guarded := guardedExecutionNodeClasses[currentClass]; guarded {
		if run.PlanState != "approved" {
			return false, nil
		}
		for _, predecessorClass := range governancePredecessorNodeClasses {
			requiredCount := 0
			succeededCount := 0
			for existingNodeID, attempt := range latest {
				if resolvedNodeClassForNodeID(existingNodeID, nodeClasses) != predecessorClass {
					continue
				}
				requiredCount++
				if attempt.NodeStatus == "succeeded" {
					succeededCount++
				}
			}
			if requiredCount > 0 && succeededCount < requiredCount {
				return false, nil
			}
		}
	}

	if currentClass == coreworkflow.NodeClassDelegatedExecution {
		if attempt, ok := latest[node.NodeID]; ok && attempt.Attempt > 1 {
			graph, err := o.workflowGraphForRun(ctx, run.RunID)
			if err != nil {
				return false, err
			}
			blocked, err := o.replayAnchorHasUnsettledValidationFailure(ctx, run.RunID, graph, node.NodeID)
			if err != nil {
				return false, err
			}
			if blocked {
				return false, nil
			}
		}
	}

	for _, edge := range incoming {
		attempt, ok := latest[edge.FromNodeID]
		if !ok {
			return false, nil
		}
		if !conditionMatches(edge.On, attempt.NodeStatus, run.PlanState) {
			return false, nil
		}
	}

	if currentClass == coreworkflow.NodeClassRoyalClosure {
		for candidateNodeID, attempt := range latest {
			if resolvedNodeClassForNodeID(candidateNodeID, nodeClasses) == coreworkflow.NodeClassValidationGates && attempt.NodeStatus != "succeeded" {
				return false, nil
			}
		}
	}

	return true, nil
}

func (o *orchestrationLoop) replayAnchorHasUnsettledValidationFailure(ctx context.Context, runID string, graph *coreworkflow.Graph, replayAnchorNodeID string) (bool, error) {
	scopePlans, err := validationScopePlansForReplayAnchor(graph, replayAnchorNodeID)
	if err != nil {
		return false, err
	}
	for _, scopePlan := range scopePlans {
		latestWave, err := o.latestFailedWaveForNodeIDs(ctx, runID, scopePlan.ParticipantNodeIDs)
		if err != nil {
			return false, err
		}
		if latestWave <= 0 {
			continue
		}
		participantNodeIDs, err := o.participantNodeIDsForWave(ctx, runID, scopePlan.ParticipantNodeIDs, latestWave, false)
		if err != nil {
			return false, err
		}
		if len(participantNodeIDs) == 0 {
			continue
		}
		waveOutcome, err := o.validationWaveOutcomeForNodeIDs(ctx, runID, participantNodeIDs, latestWave)
		if err != nil {
			return false, err
		}
		if !waveOutcome.Settled && (waveOutcome.HasRework || waveOutcome.HasHardFailure) {
			return true, nil
		}
	}
	return false, nil
}

func buildResolvedNodeClassIndex(nodes []nodeDefinition) map[string]string {
	if len(nodes) == 0 {
		return nil
	}
	index := make(map[string]string, len(nodes))
	for _, node := range nodes {
		index[node.NodeID] = resolvedNodeClass(node.NodeID, node.NodeClass)
	}
	return index
}

func resolvedNodeClassForNodeID(nodeID string, nodeClasses map[string]string) string {
	if nodeClasses != nil {
		if nodeClass, ok := nodeClasses[nodeID]; ok {
			return coreworkflow.ResolveNodeClass(nodeID, nodeClass)
		}
	}
	return coreworkflow.ResolveNodeClass(nodeID, "")
}

func (o *orchestrationLoop) classWaveSettledForWave(ctx context.Context, runID, classID string, wave int) (bool, error) {
	if wave <= 0 {
		return true, nil
	}

	var total int
	var settled int
	if err := o.db.QueryRowContext(ctx, `
WITH latest_wave_attempts AS (
	SELECT ranked.node_id,
	       ranked.node_status
	FROM (
		SELECT nr.node_id,
		       nr.node_status,
		       ROW_NUMBER() OVER (
		         PARTITION BY nr.node_id, nr.wave
		         ORDER BY CASE
		           WHEN COALESCE(TRIM(nr.superseded_at), '') = '' THEN 0
		           ELSE 1
		         END ASC,
		         nr.attempt DESC,
		         nr.node_run_id DESC
		       ) AS rn
		FROM node_runs nr
		JOIN workflow_runs wr ON wr.run_id = nr.run_id
		JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id AND wn.node_id = nr.node_id
		WHERE nr.run_id = ?
		  AND wn.node_class = ?
		  AND nr.wave = ?
	) ranked
	WHERE ranked.rn = 1
)
SELECT COUNT(1) AS total,
       COALESCE(SUM(CASE WHEN lwa.node_status IN ('succeeded', 'failed', 'cancelled', 'blocked') THEN 1 ELSE 0 END), 0) AS settled
FROM latest_wave_attempts lwa`, runID, classID, wave).Scan(&total, &settled); err != nil {
		return false, err
	}
	if total == 0 {
		return true, nil
	}

	return settled == total, nil
}

func (o *orchestrationLoop) classWaveHasFailureForWave(ctx context.Context, runID, classID string, wave int) (bool, error) {
	if wave <= 0 {
		return false, nil
	}

	var failures int
	if err := o.db.QueryRowContext(ctx, `
WITH latest_wave_attempts AS (
	SELECT ranked.node_id,
	       ranked.node_status
	FROM (
		SELECT nr.node_id,
		       nr.node_status,
		       ROW_NUMBER() OVER (
		         PARTITION BY nr.node_id, nr.wave
		         ORDER BY CASE
		           WHEN COALESCE(TRIM(nr.superseded_at), '') = '' THEN 0
		           ELSE 1
		         END ASC,
		         nr.attempt DESC,
		         nr.node_run_id DESC
		       ) AS rn
		FROM node_runs nr
		JOIN workflow_runs wr ON wr.run_id = nr.run_id
		JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id AND wn.node_id = nr.node_id
		WHERE nr.run_id = ?
		  AND wn.node_class = ?
		  AND nr.wave = ?
	) ranked
	WHERE ranked.rn = 1
)
SELECT COUNT(1)
FROM latest_wave_attempts lwa
WHERE lwa.node_status = 'failed'`, runID, classID, wave).Scan(&failures); err != nil {
		return false, err
	}

	return failures > 0, nil
}

func (o *orchestrationLoop) loopScopeWaveSettledForNodeIDs(ctx context.Context, runID, classID string, nodeIDs []string, wave int) (bool, error) {
	if wave <= 0 {
		return true, nil
	}
	switch strings.TrimSpace(classID) {
	case coreworkflow.NodeClassExecutionReview:
		outcome, err := o.reviewWaveOutcomeForNodeIDs(ctx, runID, nodeIDs, wave)
		if err != nil {
			return false, err
		}
		return outcome.Settled, nil
	case coreworkflow.NodeClassValidationGates:
		outcome, err := o.validationWaveOutcomeForNodeIDs(ctx, runID, nodeIDs, wave)
		if err != nil {
			return false, err
		}
		return outcome.Settled, nil
	default:
		return true, nil
	}
}

func (o *orchestrationLoop) hasUnsettledFailedLoopScope(ctx context.Context, runID string, graph *coreworkflow.Graph, classID string) (bool, error) {
	if graph == nil {
		return false, fmt.Errorf("workflow graph is required")
	}
	trimmedClassID := strings.TrimSpace(classID)
	if trimmedClassID == "" {
		return false, nil
	}
	seenScopes := make(map[string]struct{})
	for _, nodeID := range graph.Topological {
		scopePlan, ok, err := coreworkflow.ResolveExecutionLoopScopePlan(graph, nodeID)
		if err != nil {
			return false, err
		}
		if !ok || scopePlan.NodeClass != trimmedClassID {
			continue
		}
		if _, seen := seenScopes[scopePlan.ScopeID]; seen {
			continue
		}
		seenScopes[scopePlan.ScopeID] = struct{}{}
		latestFailedWave, err := o.latestFailedWaveForNodeIDs(ctx, runID, scopePlan.ParticipantNodeIDs)
		if err != nil {
			return false, err
		}
		if latestFailedWave <= 0 {
			continue
		}
		participantNodeIDs, err := o.participantNodeIDsForWave(ctx, runID, scopePlan.ParticipantNodeIDs, latestFailedWave, true)
		if err != nil {
			return false, err
		}
		if len(participantNodeIDs) == 0 {
			continue
		}
		settled, err := o.loopScopeWaveSettledForNodeIDs(ctx, runID, scopePlan.NodeClass, participantNodeIDs, latestFailedWave)
		if err != nil {
			return false, err
		}
		if !settled {
			return true, nil
		}
	}
	return false, nil
}

func (o *orchestrationLoop) validationWaveOutcomeForWave(ctx context.Context, runID string, wave int) (state.ValidationWaveOutcome, error) {
	if o == nil || o.db == nil {
		return state.ValidationWaveOutcome{}, fmt.Errorf("database is not configured")
	}
	return state.ValidationWaveOutcomeForWave(ctx, o.db, runID, wave)
}

func (o *orchestrationLoop) reviewWaveOutcomeForNodeIDs(ctx context.Context, runID string, nodeIDs []string, wave int) (state.ReviewWaveOutcome, error) {
	if o == nil || o.db == nil {
		return state.ReviewWaveOutcome{}, fmt.Errorf("database is not configured")
	}
	return state.ReviewWaveOutcomeForNodeIDsAndWave(ctx, o.db, runID, nodeIDs, wave)
}

func (o *orchestrationLoop) validationWaveOutcomeForNodeIDs(ctx context.Context, runID string, nodeIDs []string, wave int) (state.ValidationWaveOutcome, error) {
	if o == nil || o.db == nil {
		return state.ValidationWaveOutcome{}, fmt.Errorf("database is not configured")
	}
	return state.ValidationWaveOutcomeForNodeIDsAndWave(ctx, o.db, runID, nodeIDs, wave)
}

func (o *orchestrationLoop) latestWaveForNodeIDs(ctx context.Context, runID string, nodeIDs []string) (int, error) {
	trimmedNodeIDs := normalizeLoopNodeIDs(nodeIDs)
	if len(trimmedNodeIDs) == 0 {
		return 0, nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(trimmedNodeIDs)), ",")
	args := make([]any, 0, len(trimmedNodeIDs)+1)
	args = append(args, strings.TrimSpace(runID))
	for _, nodeID := range trimmedNodeIDs {
		args = append(args, nodeID)
	}
	query := fmt.Sprintf(`SELECT COALESCE(MAX(wave), 0) FROM node_runs WHERE run_id = ? AND node_id IN (%s)`, placeholders)
	var latestWave int
	if err := o.db.QueryRowContext(ctx, query, args...).Scan(&latestWave); err != nil {
		return 0, err
	}
	return latestWave, nil
}

func (o *orchestrationLoop) latestFailedWaveForNodeIDs(ctx context.Context, runID string, nodeIDs []string) (int, error) {
	trimmedNodeIDs := normalizeLoopNodeIDs(nodeIDs)
	if len(trimmedNodeIDs) == 0 {
		return 0, nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(trimmedNodeIDs)), ",")
	args := make([]any, 0, len(trimmedNodeIDs)+1)
	args = append(args, strings.TrimSpace(runID))
	for _, nodeID := range trimmedNodeIDs {
		args = append(args, nodeID)
	}
	query := fmt.Sprintf(`SELECT COALESCE(MAX(wave), 0) FROM node_runs WHERE run_id = ? AND node_id IN (%s) AND node_status = 'failed'`, placeholders)
	var latestWave int
	if err := o.db.QueryRowContext(ctx, query, args...).Scan(&latestWave); err != nil {
		return 0, err
	}
	return latestWave, nil
}

func (o *orchestrationLoop) participantNodeIDsForWave(ctx context.Context, runID string, nodeIDs []string, wave int, includePending bool) ([]string, error) {
	trimmedNodeIDs := normalizeLoopNodeIDs(nodeIDs)
	if len(trimmedNodeIDs) == 0 || wave <= 0 {
		return nil, nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(trimmedNodeIDs)), ",")
	args := make([]any, 0, len(trimmedNodeIDs)+1)
	args = append(args, strings.TrimSpace(runID))
	for _, nodeID := range trimmedNodeIDs {
		args = append(args, nodeID)
	}
	query := fmt.Sprintf(`
WITH latest_attempts AS (
	SELECT ranked.node_id,
	       ranked.wave,
	       ranked.node_status
	FROM (
		SELECT nr.node_id,
		       nr.wave,
		       nr.node_status,
		       ROW_NUMBER() OVER (
		         PARTITION BY nr.node_id, nr.wave
		         ORDER BY CASE
		           WHEN COALESCE(TRIM(nr.superseded_at), '') = '' THEN 0
		           ELSE 1
		         END ASC,
		         nr.attempt DESC,
		         nr.node_run_id DESC
		       ) AS rn
		FROM node_runs nr
		WHERE nr.run_id = ?
		  AND nr.node_id IN (%s)
	) ranked
	WHERE ranked.rn = 1
)
SELECT node_id
FROM latest_attempts
WHERE wave = ?`, placeholders)
	args = append(args, wave)
	if !includePending {
		query += `
  AND node_status != 'pending'`
	}
	query += `
ORDER BY node_id ASC`
	rows, err := o.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	participants := make([]string, 0, len(trimmedNodeIDs))
	for rows.Next() {
		var nodeID string
		if err := rows.Scan(&nodeID); err != nil {
			return nil, err
		}
		participants = append(participants, nodeID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return participants, nil
}

func normalizeLoopNodeIDs(nodeIDs []string) []string {
	if len(nodeIDs) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(nodeIDs))
	trimmed := make([]string, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		candidate := strings.TrimSpace(nodeID)
		if candidate == "" {
			continue
		}
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}
		trimmed = append(trimmed, candidate)
	}
	return trimmed
}

func (o *orchestrationLoop) latestWaveForNodeClass(ctx context.Context, runID, classID string) (int, error) {
	var latestWave int
	if err := o.db.QueryRowContext(ctx, `
SELECT COALESCE(MAX(nr.wave), 0)
FROM node_runs nr
JOIN workflow_runs wr ON wr.run_id = nr.run_id
JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id AND wn.node_id = nr.node_id
WHERE nr.run_id = ?
	  AND wn.node_class = ?`, runID, classID).Scan(&latestWave); err != nil {
		return 0, err
	}
	return latestWave, nil
}

func conditionMatches(on, predecessorStatus, planState string) bool {
	switch strings.ToLower(strings.TrimSpace(on)) {
	case "", "success":
		return predecessorStatus == "succeeded"
	case "failed":
		return predecessorStatus == "failed"
	case "blocked":
		return predecessorStatus == "blocked"
	case "approved":
		return predecessorStatus == "succeeded" && planState == "approved"
	case "needs_rework":
		return predecessorStatus == "succeeded" && planState == "needs_rework"
	default:
		return predecessorStatus == "succeeded"
	}
}

func nodeClassForNodeID(nodeID string) string {
	return coreworkflow.CanonicalNodeClassForNodeID(nodeID)
}

func resolvedNodeClass(nodeID, nodeClass string) string {
	return coreworkflow.ResolveNodeClass(nodeID, nodeClass)
}
