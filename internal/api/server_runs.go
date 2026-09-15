package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"unicode"

	gen "kingdom/internal/apigen"
	"kingdom/internal/core/errorcatalog"
	coreverdict "kingdom/internal/core/verdict"
	coreworkflow "kingdom/internal/core/workflow"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/mailboxview"
	"kingdom/internal/state"
	"kingdom/internal/workspace"
)

const (
	runStateFilterPending               = "pending"
	runStateFilterRunning               = "running"
	runStateFilterAwaitingHumanDecision = "awaiting_human_decision"
	runStateFilterSucceeded             = "succeeded"
	runStateFilterFailed                = "failed"
	runStateFilterCancelled             = "cancelled"
)

func (s *Server) handleGetRun(w http.ResponseWriter, r *http.Request) {
	runID := strings.TrimSpace(r.PathValue("id"))
	if runID == "" {
		s.writeBadRequest(w, "run id is required")
		return
	}

	tx, err := s.db.BeginTx(r.Context(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		s.writeInternalError(w, "fetch run failed")
		return
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var summary gen.RunDetailSummary
	var createdAt string
	var updatedAt string
	var lastActivityAt string
	var autonomousApprovalsInt int
	var runUntilAllApproveInt int
	var maxExecutionReviewCycles int
	var maxValidationReworkCycles int
	var publishMode string
	var modelSelectionJSON string
	var pendingApprovalKey sql.NullString
	err = tx.QueryRowContext(
		r.Context(),
		`SELECT wr.run_id, wd.workflow_name, wr.decree_id, d.decree_title, wr.run_status, wr.plan_state, wr.created_at, wr.updated_at, ra.last_activity_at,
		        COALESCE(wr.autonomous_approvals, 1), COALESCE(wr.run_until_all_approve, 0), COALESCE(wr.max_execution_review_cycles, 2), COALESCE(wr.max_validation_rework_cycles, 2), COALESCE(wr.publish_mode, 'direct'), COALESCE(wr.model_selection_json, '{}'), `+state.PendingApprovalKeySelectExpr("wr")+`
		 FROM workflow_runs wr
		 JOIN run_activity ra ON ra.run_id = wr.run_id
		 JOIN workflow_definitions wd ON wd.workflow_definition_id = wr.workflow_definition_id
		 JOIN decrees d ON d.decree_id = wr.decree_id
		 WHERE wr.run_id = ?`,
		runID,
	).Scan(&summary.RunId, &summary.WorkflowId, &summary.DecreeId, &summary.DecreeTitle, &summary.Status, &summary.PlanState, &createdAt, &updatedAt, &lastActivityAt, &autonomousApprovalsInt, &runUntilAllApproveInt, &maxExecutionReviewCycles, &maxValidationReworkCycles, &publishMode, &modelSelectionJSON, &pendingApprovalKey)
	if errors.Is(err, sql.ErrNoRows) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "run not found", "api.get_run", map[string]any{"run_id": runID})
		return
	}
	if err != nil {
		s.writeInternalError(w, "fetch run failed")
		return
	}

	summary.CreatedAt = mustParseRFC3339(createdAt)
	summary.UpdatedAt = mustParseRFC3339(updatedAt)
	summary.LastActivityAt = mustParseRFC3339(lastActivityAt)
	autonomousApprovals := autonomousApprovalsInt != 0
	summary.AutonomousApprovals = &autonomousApprovals
	summary.DisplayState = gen.RunDisplayState(state.DeriveRunDisplayState(string(summary.Status), string(summary.PlanState)))
	runUntilAllApprove := runUntilAllApproveInt != 0
	summary.RunUntilAllApprove = &runUntilAllApprove
	summary.MaxExecutionReviewCycles = &maxExecutionReviewCycles
	summary.MaxValidationReworkCycles = &maxValidationReworkCycles
	summary.PublishMode = refRunPublishMode(gen.RunPublishMode(workspace.NormalizePublishMode(publishMode)))
	modelSelection, decodeErr := decodeRunModelSelection(modelSelectionJSON)
	if decodeErr != nil {
		s.writeInternalError(w, "decode run model selection failed")
		return
	}
	summary.ModelSelection = modelSelection
	if pendingApprovalKey.Valid {
		trimmedPendingApprovalKey := strings.TrimSpace(pendingApprovalKey.String)
		if trimmedPendingApprovalKey != "" {
			summary.PendingApprovalKey = &trimmedPendingApprovalKey
		}
	}

	reviewCurrentWave, validationCurrentWave, reviewReworkCycles, validationReworkWaves, reviewAutoSucceededCount, validationAutoSucceededCount, metricsErr := s.fetchLoopMetrics(r.Context(), tx, runID)
	if metricsErr != nil {
		s.writeInternalError(w, "fetch run failed")
		return
	}
	summary.ReviewCurrentWave = reviewCurrentWave
	summary.ValidationCurrentWave = validationCurrentWave
	summary.ReviewReworkCycles = reviewReworkCycles
	summary.ValidationReworkWaves = validationReworkWaves
	summary.ReviewAutoSucceededCount = reviewAutoSucceededCount
	summary.ValidationAutoSucceededCount = validationAutoSucceededCount

	runtimeRetries, retryErr := s.fetchActiveRuntimeRetries(r.Context(), tx, runID)
	if retryErr != nil {
		s.writeInternalError(w, "fetch run failed")
		return
	}
	summary.RuntimeRetries = runtimeRetries

	streamCursor, streamCursorErr := s.loadRunStreamCursor(r.Context(), tx, runID)
	if streamCursorErr != nil {
		s.writeInternalError(w, "fetch run failed")
		return
	}
	summary.StreamCursor = streamCursor

	tabCounts, countsErr := s.loadRunTabCounts(r.Context(), tx, runID)
	if countsErr != nil {
		s.writeInternalError(w, "fetch run failed")
		return
	}
	summary.TabCounts = *tabCounts

	failedNodes, failedNodesErr := s.loadFailedRunNodes(r.Context(), tx, runID)
	if failedNodesErr != nil {
		s.writeInternalError(w, "fetch run failed")
		return
	}
	summary.FailedNodes = failedNodes

	if err := tx.Commit(); err != nil {
		s.writeInternalError(w, "fetch run failed")
		return
	}

	writeJSON(w, http.StatusOK, summary)
}

func (s *Server) fetchLoopMetrics(ctx context.Context, q sqlReadQuerier, runID string) (int, int, int, int, int, int, error) {
	var reviewCurrentWave int
	if err := q.QueryRowContext(ctx, `
SELECT COALESCE(MAX(nr.wave), 0)
FROM node_runs nr
JOIN workflow_runs wr ON wr.run_id = nr.run_id
JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id AND wn.node_id = nr.node_id
WHERE nr.run_id = ?
	  AND wn.node_class = 'execution_review'`, runID).Scan(&reviewCurrentWave); err != nil {
		return 0, 0, 0, 0, 0, 0, err
	}

	var validationCurrentWave int
	if err := q.QueryRowContext(ctx, `
SELECT COALESCE(MAX(nr.wave), 0)
FROM node_runs nr
JOIN workflow_runs wr ON wr.run_id = nr.run_id
JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id AND wn.node_id = nr.node_id
WHERE nr.run_id = ?
	  AND wn.node_class = 'validation_gates'`, runID).Scan(&validationCurrentWave); err != nil {
		return 0, 0, 0, 0, 0, 0, err
	}

	var reviewFailedCycles int
	if count, err := state.CountReviewReworkWaves(ctx, q, runID); err != nil {
		return 0, 0, 0, 0, 0, 0, err
	} else {
		reviewFailedCycles = count
	}

	var validationFailedWaves int
	if count, err := state.CountValidationReworkWaves(ctx, q, runID); err != nil {
		return 0, 0, 0, 0, 0, 0, err
	} else {
		validationFailedWaves = count
	}

	var reviewAutoSucceededCount int
	if err := q.QueryRowContext(ctx, `
SELECT COUNT(DISTINCT nr.wave)
FROM events e
JOIN node_runs nr ON nr.node_run_id = e.node_run_id AND nr.run_id = e.run_id
JOIN workflow_runs wr ON wr.run_id = nr.run_id
JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id AND wn.node_id = nr.node_id
WHERE e.run_id = ?
  AND e.event_type = 'attempt.succeeded'
  AND e.reason_code = 'execution_review_cap_auto_succeeded'
  AND wn.node_class = 'execution_review'`, runID).Scan(&reviewAutoSucceededCount); err != nil {
		return 0, 0, 0, 0, 0, 0, err
	}

	var validationAutoSucceededCount int
	if err := q.QueryRowContext(ctx, `
SELECT COUNT(DISTINCT nr.wave)
FROM events e
JOIN node_runs nr ON nr.node_run_id = e.node_run_id AND nr.run_id = e.run_id
JOIN workflow_runs wr ON wr.run_id = nr.run_id
JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id AND wn.node_id = nr.node_id
WHERE e.run_id = ?
  AND e.event_type = 'attempt.succeeded'
  AND e.reason_code = 'validation_gate_cap_auto_succeeded'
  AND wn.node_class = 'validation_gates'`, runID).Scan(&validationAutoSucceededCount); err != nil {
		return 0, 0, 0, 0, 0, 0, err
	}

	return reviewCurrentWave, validationCurrentWave, reviewFailedCycles, validationFailedWaves, reviewAutoSucceededCount, validationAutoSucceededCount, nil
}

func (s *Server) fetchActiveRuntimeRetries(ctx context.Context, q sqlReadQuerier, runID string) ([]gen.RuntimeRetryStatus, error) {
	rows, err := q.QueryContext(ctx, `
WITH latest_attempt_events AS (
	SELECT e.node_run_id,
	       e.run_event_seq,
	       e.event_type,
	       e.reason_code,
	       e.payload_json,
	       e.event_id,
	       ROW_NUMBER() OVER (
	         PARTITION BY e.node_run_id
	         ORDER BY e.run_event_seq DESC
	       ) AS event_rank
	FROM events e
	WHERE e.run_id = ?
	  AND COALESCE(e.node_run_id, '') != ''
	  AND e.event_type IN ('attempt.started', 'attempt.retrying', 'attempt.retry_recovered', 'attempt.failed', 'attempt.succeeded', 'attempt.cancelled')
)
SELECT COALESCE(NULLIF(TRIM(CAST(json_extract(lae.payload_json, '$.node_id') AS TEXT)), ''), nr.node_id),
	   COALESCE(NULLIF(TRIM(CAST(json_extract(lae.payload_json, '$.node_run_id') AS TEXT)), ''), lae.node_run_id),
	   CAST(json_extract(lae.payload_json, '$.retry_index') AS INTEGER),
	   CAST(json_extract(lae.payload_json, '$.retry_max') AS INTEGER),
	   CAST(json_extract(lae.payload_json, '$.delay_seconds') AS INTEGER)
FROM latest_attempt_events lae
LEFT JOIN node_runs nr ON nr.node_run_id = lae.node_run_id
WHERE lae.event_rank = 1
	AND lae.event_type = 'attempt.retrying'
	AND lae.reason_code = 'provider_empty_output_retry'
ORDER BY lae.run_event_seq DESC, COALESCE(lae.node_run_id, '') ASC`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	retries := make([]gen.RuntimeRetryStatus, 0)
	for rows.Next() {
		var nodeID sql.NullString
		var nodeRunID sql.NullString
		var retryIndex sql.NullInt64
		var retryMax sql.NullInt64
		var delaySeconds sql.NullInt64
		if err := rows.Scan(&nodeID, &nodeRunID, &retryIndex, &retryMax, &delaySeconds); err != nil {
			return nil, err
		}

		if !nodeID.Valid || !nodeRunID.Valid || !retryIndex.Valid || !retryMax.Valid || !delaySeconds.Valid {
			continue
		}

		trimmedNodeID := strings.TrimSpace(nodeID.String)
		trimmedNodeRunID := strings.TrimSpace(nodeRunID.String)
		if trimmedNodeID == "" || trimmedNodeRunID == "" || retryIndex.Int64 < 1 || retryMax.Int64 < 1 || delaySeconds.Int64 < 1 {
			continue
		}

		retries = append(retries, gen.RuntimeRetryStatus{
			DelaySeconds: int(delaySeconds.Int64),
			NodeId:       trimmedNodeID,
			NodeRunId:    trimmedNodeRunID,
			RetryIndex:   int(retryIndex.Int64),
			RetryMax:     int(retryMax.Int64),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return retries, nil
}

func (s *Server) loadRunTabCounts(ctx context.Context, q sqlReadQuerier, runID string) (*gen.RunTabCounts, error) {
	dagNodes, err := scalarInt(ctx, q, `
SELECT COUNT(1)
FROM workflow_nodes wn
JOIN workflow_runs wr ON wr.workflow_definition_id = wn.workflow_definition_id
WHERE wr.run_id = ?`, runID)
	if err != nil {
		return nil, fmt.Errorf("count dag nodes for %q: %w", runID, err)
	}
	agents, err := state.CountRunAgentsWithQuerier(ctx, q, runID)
	if err != nil {
		return nil, err
	}
	activityEvents, err := scalarInt(ctx, q, `
SELECT COUNT(1)
FROM events
WHERE run_id = ?`, runID)
	if err != nil {
		return nil, fmt.Errorf("count activity events for %q: %w", runID, err)
	}
	messages, err := scalarInt(ctx, q, fmt.Sprintf(`
SELECT COUNT(1)
FROM mailbox_messages mm
WHERE mm.run_id = ?
	  AND %s`, mailboxview.UserFacingMailboxPredicate("mm")), runID)
	if err != nil {
		return nil, fmt.Errorf("count run messages for %q: %w", runID, err)
	}
	executions, err := scalarInt(ctx, q, `
SELECT COUNT(1)
FROM attempt_executions
WHERE run_id = ?`, runID)
	if err != nil {
		return nil, fmt.Errorf("count executions for %q: %w", runID, err)
	}

	return &gen.RunTabCounts{
		DagNodes:       dagNodes,
		Agents:         agents,
		ActivityEvents: activityEvents,
		Executions:     executions,
		Messages:       messages,
	}, nil
}

func (s *Server) loadRunStreamCursor(ctx context.Context, q sqlReadQuerier, runID string) (*gen.RunStreamCursor, error) {
	var eventID string
	var runEventSeq int
	err := q.QueryRowContext(ctx, `
SELECT event_id, run_event_seq
FROM events
WHERE run_id = ?
ORDER BY run_event_seq DESC
LIMIT 1`, runID).Scan(&eventID, &runEventSeq)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &gen.RunStreamCursor{EventId: eventID, RunEventSeq: runEventSeq}, nil
}

func (s *Server) loadFailedRunNodes(ctx context.Context, q sqlReadQuerier, runID string) ([]gen.RunGraphNode, error) {
	rows, err := q.QueryContext(ctx, `WITH latest_node_runs AS (
	  SELECT nr.node_id,
	         nr.node_run_id,
	         nr.node_status,
	         ROW_NUMBER() OVER (
	           PARTITION BY nr.node_id
	           ORDER BY CASE
	             WHEN COALESCE(TRIM(nr.superseded_at), '') = '' THEN 0
	             ELSE 1
	           END ASC,
	           nr.attempt DESC,
	           nr.node_run_id DESC
	         ) AS node_rank
	  FROM node_runs nr
	  WHERE nr.run_id = ?
	), latest_failure_events AS (
	  SELECT e.node_run_id,
	         NULLIF(TRIM(CAST(json_extract(e.payload_json, '$.error') AS TEXT)), '') AS failure_reason,
	         NULLIF(TRIM(e.reason_text), '') AS failure_reason_text,
	         NULLIF(TRIM(e.reason_code), '') AS failure_reason_code,
	         ROW_NUMBER() OVER (
	           PARTITION BY e.node_run_id
	           ORDER BY e.seq DESC, e.event_id DESC
	         ) AS failure_rank
	  FROM events e
	  WHERE e.run_id = ?
	    AND e.event_type = 'attempt.failed'
	)
	SELECT wn.node_id,
	       wn.required_role,
	       COALESCE(wn.node_class, ''),
	       NULLIF(TRIM(ae.failure_text), '') AS durable_failure_reason,
	       NULLIF(TRIM(ae.failure_code), '') AS durable_failure_code,
	       COALESCE(NULLIF(TRIM(cpi.invalid_reason), ''), NULLIF(TRIM(api.invalid_reason), '')) AS completion_invalid_reason,
	       lfe.failure_reason,
	       lfe.failure_reason_text,
	       lfe.failure_reason_code
	FROM workflow_runs wr
	JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id
	JOIN latest_node_runs lnr
	  ON lnr.node_id = wn.node_id
	 AND lnr.node_rank = 1
	 AND lnr.node_status = 'failed'
	LEFT JOIN attempt_executions ae
	  ON ae.run_id = wr.run_id
	 AND ae.node_run_id = lnr.node_run_id
	LEFT JOIN provider_completion_settlements pcs
	  ON pcs.settlement_id = ae.canonical_settlement_id
	LEFT JOIN provider_invocations api
	  ON api.provider_invocation_id = ae.active_provider_invocation_id
	LEFT JOIN provider_invocations cpi
	  ON cpi.provider_invocation_id = pcs.provider_invocation_id
	LEFT JOIN latest_failure_events lfe
	  ON lfe.node_run_id = lnr.node_run_id
	 AND lfe.failure_rank = 1
	WHERE wr.run_id = ?
	ORDER BY wn.node_id ASC`, runID, runID, runID)
	if err != nil {
		return nil, fmt.Errorf("list failed run nodes for %q: %w", runID, err)
	}
	defer rows.Close()

	items := make([]gen.RunGraphNode, 0)
	for rows.Next() {
		var item gen.RunGraphNode
		var nodeClass string
		var durableFailureReason sql.NullString
		var durableFailureCode sql.NullString
		var completionInvalidReason sql.NullString
		var failureReason sql.NullString
		var failureReasonText sql.NullString
		var failureReasonCode sql.NullString
		if err := rows.Scan(&item.NodeId, &item.RequiredRole, &nodeClass, &durableFailureReason, &durableFailureCode, &completionInvalidReason, &failureReason, &failureReasonText, &failureReasonCode); err != nil {
			return nil, fmt.Errorf("scan failed run node for %q: %w", runID, err)
		}
		// The query already filters to node_status = 'failed'.
		item.State = runGraphNodeState(string(gen.RunGraphNodeStateFailed), durableFailureCode, failureReasonCode)
		item.ExecutionOwnership = runGraphNodeExecutionOwnership(item.NodeId, nodeClass)
		if trimmed, ok := canonicalFailedNodeReason(durableFailureReason, durableFailureCode, completionInvalidReason, failureReason, failureReasonText, failureReasonCode); ok {
			item.FailureReason = &trimmed
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate failed run nodes for %q: %w", runID, err)
	}
	return items, nil
}

// runGraphNodeState projects a durable node status onto the state the graph
// reports.
//
// A reviewer or validator that asks for changes is durably `failed` — the
// workflow engine needs a failure to route the loop back — but drawing it as a
// failed step tells the operator something broke when nothing did. The failure
// code is what separates the two, so the graph resolves it here and reports
// rework as its own state. The durable status is untouched.
func runGraphNodeState(nodeStatus string, failureCodes ...sql.NullString) gen.RunGraphNodeState {
	state := gen.RunGraphNodeState(nodeStatus)
	if state != gen.RunGraphNodeStateFailed {
		return state
	}
	for _, code := range failureCodes {
		if code.Valid && coreverdict.IsReworkReasonCode(code.String) {
			return gen.RunGraphNodeStateRework
		}
	}
	return state
}

func canonicalFailedNodeReason(durableFailureReason, durableFailureCode, completionInvalidReason, failureReason, failureReasonText, failureReasonCode sql.NullString) (string, bool) {
	for _, candidate := range []sql.NullString{durableFailureCode, failureReasonCode} {
		if !candidate.Valid {
			continue
		}
		if trimmed := operatorFailureReasonForCode(candidate.String, completionInvalidReason); trimmed != "" {
			return trimmed, true
		}
	}
	for _, candidate := range []sql.NullString{durableFailureReason, failureReason, failureReasonText} {
		if candidate.Valid {
			trimmed := strings.TrimSpace(candidate.String)
			if trimmed != "" {
				return trimmed, true
			}
		}
	}
	for _, candidate := range []sql.NullString{durableFailureCode, failureReasonCode} {
		if !candidate.Valid {
			continue
		}
		trimmed := strings.TrimSpace(candidate.String)
		if trimmed == "" {
			continue
		}
		return humanizeFailureCode(trimmed), true
	}
	return "", false
}

const (
	completionReceiptConflictFailureText = "Kingdom detected conflicting completion receipts for this step and could not safely accept the result."
	providerIncompleteTurnFailureText    = "Kingdom could not confirm a clean finish for this step."
)

func operatorFailureReasonForCode(code string, completionInvalidReason sql.NullString) string {
	switch strings.ToLower(strings.TrimSpace(code)) {
	case "provider_incomplete_turn":
		if completionInvalidReason.Valid && strings.ToLower(strings.TrimSpace(completionInvalidReason.String)) == "completion_receipt_conflict" {
			return completionReceiptConflictFailureText
		}
		return providerIncompleteTurnFailureText
	default:
		return ""
	}
}

func humanizeFailureCode(code string) string {
	parts := strings.Fields(strings.ReplaceAll(strings.TrimSpace(code), "_", " "))
	for idx, part := range parts {
		runes := []rune(part)
		if len(runes) == 0 {
			continue
		}
		runes[0] = unicode.ToUpper(runes[0])
		for inner := 1; inner < len(runes); inner++ {
			runes[inner] = unicode.ToLower(runes[inner])
		}
		parts[idx] = string(runes)
	}
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, " ") + "."
}

func (s *Server) handleGetRunPlan(w http.ResponseWriter, r *http.Request) {
	runID := strings.TrimSpace(r.PathValue("id"))
	if runID == "" {
		s.writeBadRequest(w, "run id is required")
		return
	}

	snapshot, _, err := s.loadRunPlanSnapshot(r.Context(), runID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "run not found", "api.get_run_plan", map[string]any{"run_id": runID})
			return
		}
		s.writeInternalError(w, "fetch run plan snapshot failed")
		return
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func (s *Server) handleListRunPlanReviews(w http.ResponseWriter, r *http.Request) {
	runID := strings.TrimSpace(r.PathValue("id"))
	if runID == "" {
		s.writeBadRequest(w, "run id is required")
		return
	}
	if !s.runExists(r.Context(), runID) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "run not found", "api.list_run_plan_reviews", map[string]any{"run_id": runID})
		return
	}

	reviews, err := state.NewRunPlanReviewStore(s.db).ListRunPlanReviews(r.Context(), runID, 100)
	if err != nil {
		s.writeInternalError(w, "list run plan reviews failed")
		return
	}

	response := gen.RunPlanReviewsResponse{RunId: runID, Items: make([]gen.RunPlanReview, 0, len(reviews))}
	for _, review := range reviews {
		response.Items = append(response.Items, gen.RunPlanReview{
			PlanReviewId:           review.PlanReviewID,
			RunId:                  review.RunID,
			ReviewedPlanRevisionId: review.ReviewedPlanRevisionID,
			Decision:               gen.RunPlanReviewDecision(review.Decision),
			FeedbackMarkdown:       review.FeedbackMarkdown,
			ActorId:                review.ActorID,
			RoleId:                 review.RoleID,
			QueuedAdvisoryAttempt:  review.QueuedAdvisoryAttempt,
			QueuedPlanAttempt:      review.QueuedPlanAttempt,
			CreatedAt:              mustParseRFC3339(review.CreatedAt),
		})
	}
	writeJSON(w, http.StatusOK, response)
}

func (s *Server) loadRunPlanSnapshot(ctx context.Context, runID string) (gen.RunPlanSnapshot, bool, error) {
	trimmedRunID := strings.TrimSpace(runID)
	var planState string
	var runUpdatedAt string
	if err := s.db.QueryRowContext(ctx, `SELECT plan_state, updated_at FROM workflow_runs WHERE run_id = ?`, trimmedRunID).Scan(&planState, &runUpdatedAt); err != nil {
		return gen.RunPlanSnapshot{}, false, err
	}
	revisions := state.NewRunPlanRevisionStore(s.db)
	revision, ok, err := revisions.CurrentRunPlanRevision(ctx, trimmedRunID)
	if err != nil {
		return gen.RunPlanSnapshot{}, false, err
	}
	if !ok {
		return gen.RunPlanSnapshot{
			RunId:          trimmedRunID,
			PlanState:      gen.RunPlanSnapshotPlanState(planState),
			RevisionStatus: gen.RunPlanSnapshotRevisionStatusPending,
			Components:     []gen.RunPlanSnapshotComponent{},
			CreatedAt:      mustParseRFC3339(runUpdatedAt),
			Format:         gen.Markdown,
			Content:        "",
		}, false, nil
	}

	var snapshot gen.RunPlanSnapshot
	snapshot.RunId = trimmedRunID
	snapshot.PlanState = gen.RunPlanSnapshotPlanState(planState)
	snapshot.RevisionStatus = gen.RunPlanSnapshotRevisionStatusReady
	snapshot.Format = gen.Markdown
	snapshot.Content = revision.ContentMarkdown
	snapshot.Components = make([]gen.RunPlanSnapshotComponent, 0, len(revision.Components))
	for _, component := range revision.Components {
		snapshot.Components = append(snapshot.Components, gen.RunPlanSnapshotComponent{
			NodeId:           component.NodeID,
			PlanningOutputId: component.PlanningOutputID,
			CreatedAt:        mustParseRFC3339(component.CreatedAt),
			Content:          component.ContentMarkdown,
		})
	}
	snapshot.PlanRevisionId = &revision.PlanRevisionID
	snapshot.CreatedAt = mustParseRFC3339(revision.CreatedAt)
	return snapshot, true, nil
}

func (s *Server) handleListRuns(w http.ResponseWriter, r *http.Request) {
	limit := 50
	stateFilter := strings.TrimSpace(r.URL.Query().Get("state_filter"))
	search := normalizeRunsSearch(strings.TrimSpace(r.URL.Query().Get("search")))
	rawCursor := strings.TrimSpace(r.URL.Query().Get("cursor"))
	rawLimit := strings.TrimSpace(r.URL.Query().Get("limit"))
	if rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil || parsed <= 0 || parsed > 200 {
			s.writeBadRequest(w, "limit must be an integer between 1 and 200")
			return
		}
		limit = parsed
	}
	if stateFilter != "" && !isSupportedRunStateFilter(stateFilter) {
		s.writeBadRequest(w, "state_filter must be pending, running, awaiting_human_decision, succeeded, failed, or cancelled")
		return
	}

	cursorScope := listRunsCursorScope(stateFilter, search)
	orderField := listRunsOrderField(stateFilter)
	cursor := runsListCursorPayload{}
	if rawCursor != "" {
		decoded, err := s.decodeRunsCursor(cursorScope, rawCursor)
		if err != nil {
			s.writeBadRequest(w, "invalid cursor")
			return
		}
		cursor = decoded
	}

	runIDSearchTerm := runSearchRunIDTerm(search)
	titleSearchTerms := tokenizeRunSearchTerms(search)
	titleSearchQuery := buildRunSearchColumnPrefixQuery("decree_title_search", titleSearchTerms)
	if search != "" && runIDSearchTerm == "" && titleSearchQuery == "" {
		writeJSON(w, http.StatusOK, runListResponse{Items: []gen.RunSummary{}})
		return
	}

	baseSelect := `wr.run_id, wd.workflow_name, wr.decree_id, d.decree_title, wr.run_status, wr.plan_state, wr.created_at, wr.updated_at, ra.last_activity_at,
	        COALESCE(wr.autonomous_approvals, 1), COALESCE(wr.publish_mode, 'direct'), COALESCE(wr.model_selection_json, '{}'), ` + state.PendingApprovalKeySelectExpr("wr")
	baseJoins := `
	 JOIN run_activity ra ON ra.run_id = wr.run_id
	 JOIN workflow_definitions wd ON wd.workflow_definition_id = wr.workflow_definition_id
	 JOIN decrees d ON d.decree_id = wr.decree_id`
	query := `SELECT ` + baseSelect + `
	 FROM workflow_runs wr` + baseJoins
	args := make([]any, 0, 10)
	searchEnabled := search != ""
	if searchEnabled {
		searchClauses := make([]string, 0, 3)
		if runIDSearchTerm != "" {
			searchClauses = append(searchClauses, `SELECT rsd.run_id, 0 AS match_priority
	FROM run_search_documents rsd
	WHERE rsd.run_id_search = ?`)
			args = append(args, runIDSearchTerm)

			searchClauses = append(searchClauses, `SELECT rsd.run_id, 1 AS match_priority
	FROM run_search_fts
	JOIN run_search_documents rsd ON rsd.rowid = run_search_fts.rowid
	WHERE run_search_fts MATCH ?`)
			args = append(args, buildRunSearchColumnPrefixQuery("run_id_search", []string{runIDSearchTerm}))
		}
		if titleSearchQuery != "" {
			searchClauses = append(searchClauses, `SELECT rsd.run_id, 2 AS match_priority
	FROM run_search_fts
	JOIN run_search_documents rsd ON rsd.rowid = run_search_fts.rowid
	WHERE run_search_fts MATCH ?`)
			args = append(args, titleSearchQuery)
		}
		query = `WITH search_matches AS (
	` + strings.Join(searchClauses, `
	UNION
	`) + `
	), ranked_matches AS (
		SELECT run_id, MIN(match_priority) AS match_priority
		FROM search_matches
		GROUP BY run_id
	)
	SELECT ranked_matches.match_priority, ` + baseSelect + `
	FROM ranked_matches
	JOIN workflow_runs wr ON wr.run_id = ranked_matches.run_id` + baseJoins
	}

	whereClauses := make([]string, 0, 3)
	if clause := runStateFilterClause(stateFilter); clause != "" {
		whereClauses = append(whereClauses, clause)
	}
	if rawCursor != "" {
		if searchEnabled {
			whereClauses = append(whereClauses, fmt.Sprintf(`(ranked_matches.match_priority > ? OR (ranked_matches.match_priority = ? AND (%s < ? OR (%s = ? AND wr.run_id < ?))))`, orderField, orderField))
			args = append(args, cursor.MatchPriority, cursor.MatchPriority, cursor.OrderedAt, cursor.OrderedAt, cursor.RunID)
		} else {
			whereClauses = append(whereClauses, fmt.Sprintf(`(%s < ? OR (%s = ? AND wr.run_id < ?))`, orderField, orderField))
			args = append(args, cursor.OrderedAt, cursor.OrderedAt, cursor.RunID)
		}
	}
	if len(whereClauses) > 0 {
		query += "\n WHERE " + strings.Join(whereClauses, "\n   AND ")
	}
	if searchEnabled {
		query += "\n ORDER BY ranked_matches.match_priority ASC, " + orderField + " DESC, wr.run_id DESC"
	} else {
		query += "\n ORDER BY " + orderField + " DESC, wr.run_id DESC"
	}
	query += `
	 LIMIT ?`
	args = append(args, limit+1)

	rows, err := s.db.QueryContext(
		r.Context(),
		query,
		args...,
	)
	if err != nil {
		s.writeInternalError(w, "list runs failed")
		return
	}
	defer rows.Close()

	items := make([]gen.RunSummary, 0, limit+1)
	matchPriorities := make([]int, 0, limit+1)
	for rows.Next() {
		var summary gen.RunSummary
		matchPriority := 0
		var createdAt string
		var updatedAt string
		var lastActivityAt string
		var autonomousApprovalsInt int
		var publishMode string
		var modelSelectionJSON string
		var pendingApprovalKey sql.NullString
		var scanErr error
		if searchEnabled {
			scanErr = rows.Scan(&matchPriority, &summary.RunId, &summary.WorkflowId, &summary.DecreeId, &summary.DecreeTitle, &summary.Status, &summary.PlanState, &createdAt, &updatedAt, &lastActivityAt, &autonomousApprovalsInt, &publishMode, &modelSelectionJSON, &pendingApprovalKey)
		} else {
			scanErr = rows.Scan(&summary.RunId, &summary.WorkflowId, &summary.DecreeId, &summary.DecreeTitle, &summary.Status, &summary.PlanState, &createdAt, &updatedAt, &lastActivityAt, &autonomousApprovalsInt, &publishMode, &modelSelectionJSON, &pendingApprovalKey)
		}
		if scanErr != nil {
			s.writeInternalError(w, "list runs failed")
			return
		}
		summary.CreatedAt = mustParseRFC3339(createdAt)
		summary.UpdatedAt = mustParseRFC3339(updatedAt)
		summary.LastActivityAt = mustParseRFC3339(lastActivityAt)
		autonomousApprovals := autonomousApprovalsInt != 0
		summary.AutonomousApprovals = &autonomousApprovals
		summary.DisplayState = gen.RunDisplayState(state.DeriveRunDisplayState(string(summary.Status), string(summary.PlanState)))
		summary.PublishMode = refRunPublishMode(gen.RunPublishMode(workspace.NormalizePublishMode(publishMode)))
		modelSelection, decodeErr := decodeRunModelSelection(modelSelectionJSON)
		if decodeErr != nil {
			s.writeInternalError(w, "decode run model selection failed")
			return
		}
		summary.ModelSelection = modelSelection
		if pendingApprovalKey.Valid {
			trimmedPendingApprovalKey := strings.TrimSpace(pendingApprovalKey.String)
			if trimmedPendingApprovalKey != "" {
				summary.PendingApprovalKey = &trimmedPendingApprovalKey
			}
		}
		items = append(items, summary)
		matchPriorities = append(matchPriorities, matchPriority)
	}

	if err := rows.Err(); err != nil {
		s.writeInternalError(w, "list runs failed")
		return
	}

	response := runListResponse{}
	if len(items) > limit {
		trimmed := items[:limit]
		last := trimmed[len(trimmed)-1]
		nextCursor := s.encodeRunsCursor(cursorScope, matchPriorities[len(trimmed)-1], listRunOrderedAt(last, stateFilter), last.RunId)
		response.NextCursor = &nextCursor
		response.Items = trimmed
	} else {
		response.Items = items
	}

	writeJSON(w, http.StatusOK, response)
}

func listRunsOrderField(stateFilter string) string {
	if stateFilter == runStateFilterAwaitingHumanDecision {
		return "wr.updated_at"
	}
	return "wr.created_at"
}

func listRunsCursorScope(stateFilter, search string) string {
	return fmt.Sprintf("%s|%s", stateFilter, search)
}

type runsListCursorPayload struct {
	MatchPriority int    `json:"match_priority,omitempty"`
	OrderedAt     string `json:"ordered_at"`
	RunID         string `json:"run_id"`
}

func listRunOrderedAt(summary gen.RunSummary, stateFilter string) string {
	if stateFilter == runStateFilterAwaitingHumanDecision {
		return summary.UpdatedAt.Format("2006-01-02T15:04:05Z07:00")
	}
	return summary.CreatedAt.Format("2006-01-02T15:04:05Z07:00")
}

func (s *Server) encodeRunsCursor(scope string, matchPriority int, orderedAt, runID string) string {
	payload, _ := json.Marshal(runsListCursorPayload{MatchPriority: matchPriority, OrderedAt: orderedAt, RunID: runID})
	return s.cursors.Encode("runs-list", scope, string(payload))
}

func (s *Server) decodeRunsCursor(scope, cursor string) (runsListCursorPayload, error) {
	payload, err := s.cursors.Decode("runs-list", scope, cursor)
	if err != nil {
		return runsListCursorPayload{}, err
	}
	var decoded runsListCursorPayload
	if err := json.Unmarshal([]byte(payload), &decoded); err != nil {
		return runsListCursorPayload{}, fmt.Errorf("invalid runs cursor payload")
	}
	if strings.TrimSpace(decoded.OrderedAt) == "" || strings.TrimSpace(decoded.RunID) == "" || decoded.MatchPriority < 0 {
		return runsListCursorPayload{}, fmt.Errorf("invalid runs cursor values")
	}
	return decoded, nil
}

func normalizeRunsSearch(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func runSearchRunIDTerm(search string) string {
	terms := tokenizeRunSearchTerms(search)
	if len(terms) != 1 || search == "" || terms[0] != search {
		return ""
	}
	return terms[0]
}

func tokenizeRunSearchTerms(search string) []string {
	if search == "" {
		return nil
	}
	parts := strings.FieldsFunc(search, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r) && r != '-' && r != '_'
	})
	terms := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		terms = append(terms, strings.ToLower(trimmed))
	}
	return terms
}

func buildRunSearchColumnPrefixQuery(column string, terms []string) string {
	if len(terms) == 0 {
		return ""
	}
	clauses := make([]string, 0, len(terms))
	for _, term := range terms {
		if term == "" {
			continue
		}
		clauses = append(clauses, fmt.Sprintf(`%s : "%s"*`, column, escapeFTS5Term(term)))
	}
	return strings.Join(clauses, " AND ")
}

func escapeFTS5Term(term string) string {
	return strings.ReplaceAll(term, `"`, `""`)
}

func isSupportedRunStateFilter(value string) bool {
	switch value {
	case runStateFilterPending,
		runStateFilterRunning,
		runStateFilterAwaitingHumanDecision,
		runStateFilterSucceeded,
		runStateFilterFailed,
		runStateFilterCancelled:
		return true
	default:
		return false
	}
}

func runStateFilterClause(value string) string {
	switch value {
	case runStateFilterPending:
		return "wr.run_status = 'pending'"
	case runStateFilterRunning:
		return "wr.run_status = 'running' AND NOT " + state.ManualPlanApprovalActiveSelectExpr("wr")
	case runStateFilterAwaitingHumanDecision:
		return "wr.autonomous_approvals = 0 AND " + state.ManualApprovalActiveSelectExpr("wr")
	case runStateFilterSucceeded:
		return "wr.run_status = 'succeeded'"
	case runStateFilterFailed:
		return "wr.run_status = 'failed'"
	case runStateFilterCancelled:
		return "wr.run_status = 'cancelled'"
	default:
		return ""
	}
}

func (s *Server) handleGetRunGraph(w http.ResponseWriter, r *http.Request) {
	runID := strings.TrimSpace(r.PathValue("id"))

	var workflowDefinitionID string
	err := s.db.QueryRowContext(
		r.Context(),
		`SELECT workflow_definition_id FROM workflow_runs WHERE run_id = ?`,
		runID,
	).Scan(&workflowDefinitionID)
	if errors.Is(err, sql.ErrNoRows) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "run not found", "api.get_run_graph", map[string]any{"run_id": runID})
		return
	}
	if err != nil {
		s.writeInternalError(w, "fetch run graph failed")
		return
	}

	nodes, err := s.fetchRunGraphNodes(r.Context(), runID, workflowDefinitionID)
	if err != nil {
		s.writeInternalError(w, "fetch run graph nodes failed")
		return
	}

	edges, err := s.fetchRunGraphEdges(r.Context(), workflowDefinitionID)
	if err != nil {
		s.writeInternalError(w, "fetch run graph edges failed")
		return
	}

	writeJSON(w, http.StatusOK, gen.RunGraphResponse{
		RunId: runID,
		Nodes: nodes,
		Edges: edges,
	})
}

func (s *Server) handleListRunAgents(w http.ResponseWriter, r *http.Request) {
	runID := strings.TrimSpace(r.PathValue("id"))
	if !s.runExists(r.Context(), runID) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "run not found", "api.list_run_agents", map[string]any{"run_id": runID})
		return
	}

	registry := state.NewAgentRegistry(s.db)
	agents, err := registry.ListRunAgents(r.Context(), runID)
	if err != nil {
		s.writeInternalError(w, "list run agents failed")
		return
	}

	items := make([]runParticipantItem, 0, len(agents))
	for _, agent := range agents {
		item := runParticipantItem{
			AgentID:     agent.AgentID,
			RoleID:      agent.RoleID,
			DisplayName: agent.DisplayName,
			Status:      string(agent.Status),
			SpawnDepth:  agent.SpawnDepth,
			FirstSeenAt: mustParseRFC3339(agent.FirstSeenAt),
		}
		if agent.ParentAgentID.Valid {
			item.ParentAgentID = &agent.ParentAgentID.String
		}
		items = append(items, item)
	}

	writeJSON(w, http.StatusOK, runAgentsResponse{RunID: runID, Items: items})
}

func (s *Server) handleListRunExecutions(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.list_run_executions"); !ok {
		return
	}

	runID := strings.TrimSpace(r.PathValue("id"))
	if !s.runExists(r.Context(), runID) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "run not found", "api.list_run_executions", map[string]any{"run_id": runID})
		return
	}

	store := executionstore.New(s.db)
	records, err := store.ListRunExecutions(r.Context(), runID)
	if err != nil {
		s.writeInternalError(w, "list run executions failed")
		return
	}

	items := make([]runExecutionItem, 0, len(records))
	for _, record := range records {
		items = append(items, buildRunExecutionItem(record))
	}

	writeJSON(w, http.StatusOK, runExecutionsResponse{RunID: runID, Items: items})
}

func (s *Server) handleGetRunExecution(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.get_run_execution"); !ok {
		return
	}

	runID := strings.TrimSpace(r.PathValue("id"))
	if !s.runExists(r.Context(), runID) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "run not found", "api.get_run_execution", map[string]any{"run_id": runID})
		return
	}
	attemptID := strings.TrimSpace(r.PathValue("attemptId"))
	if attemptID == "" {
		s.writeBadRequest(w, "attempt id is required")
		return
	}

	store := executionstore.New(s.db)
	record, found, err := store.GetRunExecutionDetail(r.Context(), runID, attemptID)
	if err != nil {
		s.writeInternalError(w, "get run execution failed")
		return
	}
	if !found {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "run execution not found", "api.get_run_execution", map[string]any{"run_id": runID, "attempt_id": attemptID})
		return
	}

	item := buildRunExecutionDetailItem(record)
	if status, summary, entries, err := s.loadRunExecutionPublication(r.Context(), attemptID); err == nil {
		if strings.TrimSpace(status) != "" {
			item.PublicationStatus = &status
		}
		if strings.TrimSpace(summary) != "" {
			item.PublicationSummary = &summary
		}
		item.PublicationEntries = entries
	}
	writeJSON(w, http.StatusOK, item)
}

// handleGetRunExecutionActivity returns the durable, fine-grained provider activity
// (agent reasoning + tool calls) for a single node attempt. It is the durable
// backstop for the run Activity tab; live activity arrives over the run event
// stream. The payload shape mirrors the planner's activity endpoint so the same
// frontend renderer serves both.
func (s *Server) handleGetRunExecutionActivity(w http.ResponseWriter, r *http.Request) {
	if _, _, ok := s.requirePrincipal(w, r, "api.get_run_execution_activity"); !ok {
		return
	}

	runID := strings.TrimSpace(r.PathValue("id"))
	if !s.runExists(r.Context(), runID) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "run not found", "api.get_run_execution_activity", map[string]any{"run_id": runID})
		return
	}
	attemptID := strings.TrimSpace(r.PathValue("attemptId"))
	if attemptID == "" {
		s.writeBadRequest(w, "attempt id is required")
		return
	}

	store := executionstore.New(s.db)
	if _, found, err := store.GetRunExecutionDetail(r.Context(), runID, attemptID); err != nil {
		s.writeInternalError(w, "get run execution activity failed")
		return
	} else if !found {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "run execution not found", "api.get_run_execution_activity", map[string]any{"run_id": runID, "attempt_id": attemptID})
		return
	}

	rows, err := s.db.QueryContext(
		r.Context(),
		`SELECT event_id, payload_json, created_at
		 FROM events
		 WHERE run_id = ? AND attempt_id = ? AND event_type = ?
		 ORDER BY seq ASC`,
		runID,
		attemptID,
		state.AttemptActivityEventType,
	)
	if err != nil {
		s.writeInternalError(w, "fetch run execution activity failed")
		return
	}
	defer rows.Close()

	items := make([]gen.AcpRunActivityItem, 0)
	for rows.Next() {
		var eventID string
		var payloadJSON string
		var createdAt string
		if err := rows.Scan(&eventID, &payloadJSON, &createdAt); err != nil {
			s.writeInternalError(w, "decode run execution activity row failed")
			return
		}
		if item, ok := acpActivityItemFromPayload(payloadJSON, eventID, createdAt); ok {
			items = append(items, item)
		}
	}
	if err := rows.Err(); err != nil {
		s.writeInternalError(w, "read run execution activity failed")
		return
	}

	writeJSON(w, http.StatusOK, gen.AcpRunActivityResponse{Items: dedupeAcpRunActivityItems(items)})
}

func (s *Server) loadRunExecutionPublication(ctx context.Context, attemptID string) (string, string, []runExecutionPublicationEntryItem, error) {
	if s == nil || s.db == nil || strings.TrimSpace(attemptID) == "" {
		return "", "", nil, nil
	}
	var batchID string
	var status string
	var publishedCount int
	var conflictedCount int
	var failedCount int
	err := s.db.QueryRowContext(ctx, `SELECT publication_batch_id, status, published_count, conflicted_count, failed_count FROM run_publication_batches WHERE attempt_id = ? ORDER BY created_at DESC LIMIT 1`, strings.TrimSpace(attemptID)).Scan(&batchID, &status, &publishedCount, &conflictedCount, &failedCount)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", nil, nil
	}
	if err != nil {
		return "", "", nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT path, action, status, reason_code, reason_text FROM run_publication_entries WHERE publication_batch_id = ? ORDER BY publication_entry_id ASC`, batchID)
	if err != nil {
		return "", "", nil, err
	}
	defer rows.Close()
	entries := make([]runExecutionPublicationEntryItem, 0)
	for rows.Next() {
		var entry runExecutionPublicationEntryItem
		if err := rows.Scan(&entry.Path, &entry.Action, &entry.Status, &entry.ReasonCode, &entry.ReasonText); err != nil {
			return "", "", nil, err
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return "", "", nil, err
	}
	summary := fmt.Sprintf("Published %d path(s); %d conflicted; %d failed.", publishedCount, conflictedCount, failedCount)
	return status, summary, entries, nil
}

func buildRunExecutionItem(record executionstore.RunExecutionRecord) runExecutionItem {
	item := runExecutionItem{
		AttemptID:     record.AttemptID,
		NodeRunID:     record.NodeRunID,
		NodeID:        record.NodeID,
		Attempt:       record.Attempt,
		AgentID:       record.AgentID,
		RequiredRole:  record.RequiredRole,
		ExecutionMode: string(record.ExecutionMode),
		Status:        runExecutionStatus(record),
		QueuedAt:      mustParseRFC3339(record.QueuedAt),
		ActivityAt:    mustParseRFC3339(record.ActivityAt),
		UpdatedAt:     mustParseRFC3339(record.UpdatedAt),
	}
	if record.LogicalSessionID.Valid {
		item.LogicalSessionID = &record.LogicalSessionID.String
	}
	if record.ProviderSessionID.Valid {
		item.ProviderSessionID = &record.ProviderSessionID.String
	}
	if record.ProgressEventType.Valid {
		item.ProgressEventType = &record.ProgressEventType.String
	}
	if record.ProgressReasonCode.Valid {
		item.ProgressReasonCode = &record.ProgressReasonCode.String
	}
	if record.ProgressAt.Valid {
		parsed := mustParseRFC3339(record.ProgressAt.String)
		item.ProgressAt = &parsed
	}
	if record.LaunchStartedAt.Valid {
		parsed := mustParseRFC3339(record.LaunchStartedAt.String)
		item.LaunchStartedAt = &parsed
	}
	if record.StartedAt.Valid {
		parsed := mustParseRFC3339(record.StartedAt.String)
		item.StartedAt = &parsed
	}
	if record.TerminalObservedAt.Valid {
		parsed := mustParseRFC3339(record.TerminalObservedAt.String)
		item.TerminalObservedAt = &parsed
	}
	if record.CommittedAt.Valid {
		parsed := mustParseRFC3339(record.CommittedAt.String)
		item.CommittedAt = &parsed
	}
	if record.FailureCode.Valid {
		item.FailureCode = &record.FailureCode.String
	}
	if record.FailureText.Valid {
		item.FailureText = &record.FailureText.String
	}
	if record.CompletionInvalidReason.Valid {
		item.CompletionInvalidReason = &record.CompletionInvalidReason.String
	}
	if record.CancellationHandoffState.Valid {
		item.HandoffState = &record.CancellationHandoffState.String
	}
	item.StopRetryAvailable = record.StopRetryAvailable
	return item
}

func buildRunExecutionDetailItem(record executionstore.RunExecutionDetailRecord) runExecutionDetailItem {
	item := runExecutionDetailItem{
		runExecutionItem: buildRunExecutionItem(record.RunExecutionRecord),
		SettlementStatus: runExecutionSettlementStatus(record),
		OutputStatus:     runExecutionOutputStatus(record),
	}
	if record.ProviderInvocationID.Valid {
		item.ProviderInvocationID = &record.ProviderInvocationID.String
	}
	if record.ProviderInvocationAt.Valid {
		parsed := mustParseRFC3339(record.ProviderInvocationAt.String)
		item.ProviderInvocationAt = &parsed
	}
	if record.CanonicalSettlementAt.Valid {
		parsed := mustParseRFC3339(record.CanonicalSettlementAt.String)
		item.SettledAt = &parsed
	}
	if record.ObservedOutputText.Valid {
		item.OutputText = &record.ObservedOutputText.String
	}
	if settlementDetail := runExecutionSettlementDetail(record); settlementDetail != nil {
		item.SettlementDetail = settlementDetail
	}
	if verdict, verdictStatus := runExecutionOutputVerdict(record); verdict != "" && verdictStatus != "" {
		item.OutputVerdict = &verdict
		item.OutputVerdictStatus = &verdictStatus
	}
	return item
}

func runExecutionStatus(record executionstore.RunExecutionRecord) string {
	if strings.TrimSpace(record.NodeStatus) == "blocked" {
		if record.CancellationPending && record.FailureCode.Valid && strings.TrimSpace(record.FailureCode.String) == "agent_unresponsive" {
			return "paused"
		}
		return "blocked"
	}
	switch strings.TrimSpace(record.State) {
	case "starting":
		if runExecutionLaunchObserved(record) {
			return "running"
		}
		return "starting"
	case "running":
		return "running"
	case "terminal_observed":
		return "verifying"
	case "committed_success":
		return "succeeded"
	case "committed_failure":
		return "failed"
	case "committed_cancelled":
		return "cancelled"
	default:
		return "queued"
	}
}

func runExecutionLaunchObserved(record executionstore.RunExecutionRecord) bool {
	if record.StartedAt.Valid && strings.TrimSpace(record.StartedAt.String) != "" {
		return true
	}
	return record.ProgressReasonCode.Valid && strings.TrimSpace(record.ProgressReasonCode.String) == "provider_invoked"
}

func runExecutionSettlementStatus(record executionstore.RunExecutionDetailRecord) string {
	if record.ExecutionMode != executionstore.ExecutionModeProviderBacked {
		return "not_applicable"
	}
	switch strings.TrimSpace(record.CanonicalSettlementOutcome.String) {
	case "success":
		return "succeeded"
	case "failed":
		return "failed"
	case "cancelled":
		return "cancelled"
	}
	hasProviderInvocation := record.ProviderInvocationID.Valid && strings.TrimSpace(record.ProviderInvocationID.String) != ""
	switch strings.TrimSpace(record.State) {
	case "terminal_observed":
		if hasProviderInvocation {
			return "pending"
		}
		return "not_started"
	case "committed_success":
		if hasProviderInvocation {
			return "pending"
		}
		return "not_started"
	case "committed_failure":
		if hasProviderInvocation {
			return "failed"
		}
		return "not_started"
	case "committed_cancelled":
		if hasProviderInvocation {
			return "cancelled"
		}
		return "not_started"
	default:
		return "not_started"
	}
}

func runExecutionOutputStatus(record executionstore.RunExecutionDetailRecord) string {
	if record.CanonicalSettlementID.Valid {
		if record.ObservedOutputText.Valid {
			return "settled"
		}
		if record.ExecutionMode == executionstore.ExecutionModeProviderBacked {
			return "unavailable"
		}
		return "none"
	}
	if record.ObservedOutputText.Valid {
		return "observed"
	}
	if record.ExecutionMode == executionstore.ExecutionModeProviderBacked {
		switch strings.TrimSpace(record.State) {
		case "committed_failure", "committed_cancelled":
			return "unavailable"
		}
	}
	return "none"
}

func runExecutionSettlementDetail(record executionstore.RunExecutionDetailRecord) *runExecutionSettlementDetailItem {
	if record.ExecutionMode != executionstore.ExecutionModeProviderBacked {
		return nil
	}
	if record.CanonicalSettlementID.Valid {
		return nil
	}
	hasProviderInvocation := record.ProviderInvocationID.Valid && strings.TrimSpace(record.ProviderInvocationID.String) != ""
	if !hasProviderInvocation {
		return nil
	}
	switch strings.TrimSpace(record.State) {
	case "terminal_observed", "committed_success":
		return buildRunExecutionSettlementDetail(record.FailureCode, record.FailureText)
	case "committed_failure":
		return buildRunExecutionSettlementDetail(record.FailureCode, record.FailureText)
	case "committed_cancelled":
		return buildRunExecutionSettlementDetail(record.FailureCode, record.FailureText)
	default:
		return nil
	}
}

func buildRunExecutionSettlementDetail(reasonCandidates ...sql.NullString) *runExecutionSettlementDetailItem {
	item := &runExecutionSettlementDetailItem{}
	for _, candidate := range reasonCandidates {
		if text := nullableString(candidate); text != nil {
			item.Reason = text
			return item
		}
	}
	return nil
}

func runExecutionOutputVerdict(record executionstore.RunExecutionDetailRecord) (string, string) {
	if verdict := runExecutionStructuredVerdict(record.CanonicalStructuredPayloadJSON); verdict != "" {
		return verdict, "settled"
	}
	if !record.ObservedControlVerdict.Valid {
		return "", ""
	}
	verdict := strings.TrimSpace(record.ObservedControlVerdict.String)
	if verdict == "" {
		return "", ""
	}
	if record.CanonicalSettlementID.Valid {
		return verdict, "settled"
	}
	return verdict, "observed"
}

func runExecutionStructuredVerdict(raw sql.NullString) string {
	if !raw.Valid || strings.TrimSpace(raw.String) == "" {
		return ""
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw.String), &payload); err != nil {
		return ""
	}
	value, ok := payload["verdict"].(string)
	if !ok {
		return ""
	}
	verdict := strings.ToUpper(strings.TrimSpace(value))
	if verdict != "PASS" && verdict != "REWORK" {
		return ""
	}
	return verdict
}

func (s *Server) handleGetRunTimeline(w http.ResponseWriter, r *http.Request) {
	runID := strings.TrimSpace(r.PathValue("id"))
	if !s.runExists(r.Context(), runID) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "run not found", "api.get_run_timeline", map[string]any{"run_id": runID})
		return
	}

	limit := defaultTimelineLimit
	if rawLimit := strings.TrimSpace(r.URL.Query().Get("limit")); rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil || parsed < 1 || parsed > maxTimelineLimit {
			s.writeBadRequest(w, "limit must be between 1 and 200")
			return
		}
		limit = parsed
	}

	beforeRunEventSeq := 0
	if rawCursor := strings.TrimSpace(r.URL.Query().Get("cursor")); rawCursor != "" {
		seq, err := s.decodeTimelineCursor(runID, rawCursor)
		if err != nil {
			s.writeBadRequest(w, "invalid cursor")
			return
		}
		beforeRunEventSeq = seq
	}

	rows, err := s.db.QueryContext(
		r.Context(),
		`SELECT event_id, run_event_seq, seq, event_type, reason_code, payload_json, created_at
		 FROM events
		 WHERE run_id = ?
		   AND event_type != ?
		   AND (? = 0 OR run_event_seq < ?)
		 ORDER BY run_event_seq DESC
		 LIMIT ?`,
		runID,
		state.AttemptActivityEventType,
		beforeRunEventSeq,
		beforeRunEventSeq,
		limit+1,
	)
	if err != nil {
		s.writeInternalError(w, "fetch timeline failed")
		return
	}
	defer rows.Close()

	items := make([]gen.TimelineEvent, 0, limit+1)
	runEventSeqs := make([]int, 0, limit+1)
	for rows.Next() {
		var item gen.TimelineEvent
		var runEventSeq int
		var reasonCode sql.NullString
		var payloadJSON string
		var emittedAt string
		if err := rows.Scan(&item.EventId, &runEventSeq, &item.AttemptSeq, &item.EventType, &reasonCode, &payloadJSON, &emittedAt); err != nil {
			s.writeInternalError(w, "decode timeline row failed")
			return
		}
		item.RunEventSeq = runEventSeq
		if reasonCode.Valid && strings.TrimSpace(reasonCode.String) != "" {
			trimmed := strings.TrimSpace(reasonCode.String)
			item.ReasonCode = &trimmed
		}
		item.EmittedAt = mustParseRFC3339(emittedAt)
		item.PayloadJson = map[string]any{}
		if err := json.Unmarshal([]byte(payloadJSON), &item.PayloadJson); err != nil {
			s.writeInternalError(w, "decode timeline payload failed")
			return
		}
		items = append(items, item)
		runEventSeqs = append(runEventSeqs, runEventSeq)
	}

	for left, right := 0, len(items)-1; left < right; left, right = left+1, right-1 {
		items[left], items[right] = items[right], items[left]
		runEventSeqs[left], runEventSeqs[right] = runEventSeqs[right], runEventSeqs[left]
	}

	response := gen.RunTimelineResponse{RunId: runID}
	if len(items) > limit {
		trimmed := items[1:]
		nextCursor := s.encodeTimelineCursor(runID, runEventSeqs[1])
		response.NextCursor = &nextCursor
		response.Items = trimmed
	} else {
		response.Items = items
	}

	writeJSON(w, http.StatusOK, response)
}

func (s *Server) fetchRunGraphNodes(ctx context.Context, runID string, workflowDefinitionID string) ([]gen.RunGraphNode, error) {
	query := fmt.Sprintf(`WITH latest_node_runs AS (
		  SELECT nr.node_id,
		         nr.node_run_id,
		         nr.attempt,
		         nr.node_status,
		         ROW_NUMBER() OVER (
		           PARTITION BY nr.node_id
		           ORDER BY %s
		         ) AS node_rank
		  FROM node_runs nr
		  WHERE nr.run_id = ?
		), latest_failure_events AS (
		  SELECT e.node_run_id,
		         NULLIF(TRIM(CAST(json_extract(e.payload_json, '$.error') AS TEXT)), '') AS failure_reason,
		         NULLIF(TRIM(e.reason_text), '') AS failure_reason_text,
		         NULLIF(TRIM(e.reason_code), '') AS failure_reason_code,
		         ROW_NUMBER() OVER (
		           PARTITION BY e.node_run_id
		           ORDER BY e.seq DESC, e.event_id DESC
		         ) AS failure_rank
		  FROM events e
		  WHERE e.run_id = ?
		    AND e.event_type = 'attempt.failed'
		)
		SELECT wn.node_id,
		       wn.required_role,
		       COALESCE(wn.node_class, ''),
		       NULLIF(TRIM(lnr.node_run_id), '') AS node_run_id,
		       lnr.attempt,
		       COALESCE(lnr.node_status, 'pending') AS node_state,
		       CASE
		         WHEN COALESCE(lnr.node_status, 'pending') = 'failed' THEN NULLIF(TRIM(ae.failure_text), '')
		         ELSE NULL
		       END AS failure_reason
		     , CASE
		         WHEN COALESCE(lnr.node_status, 'pending') = 'failed' THEN NULLIF(TRIM(ae.failure_code), '')
		         ELSE NULL
		       END AS failure_code
		     , CASE
		         WHEN COALESCE(lnr.node_status, 'pending') = 'failed' THEN COALESCE(NULLIF(TRIM(cpi.invalid_reason), ''), NULLIF(TRIM(api.invalid_reason), ''))
		         ELSE NULL
		       END AS completion_invalid_reason
		     , CASE
		         WHEN COALESCE(lnr.node_status, 'pending') = 'failed' THEN lfe.failure_reason
		         ELSE NULL
		       END AS event_failure_reason
		     , CASE
		         WHEN COALESCE(lnr.node_status, 'pending') = 'failed' THEN lfe.failure_reason_text
		         ELSE NULL
		       END AS event_failure_reason_text
		     , CASE
		         WHEN COALESCE(lnr.node_status, 'pending') = 'failed' THEN lfe.failure_reason_code
		         ELSE NULL
		       END AS event_failure_reason_code
		FROM workflow_nodes wn
		LEFT JOIN latest_node_runs lnr
		  ON lnr.node_id = wn.node_id
		 AND lnr.node_rank = 1
		LEFT JOIN attempt_executions ae
		  ON ae.run_id = ?
		 AND ae.node_run_id = lnr.node_run_id
		LEFT JOIN provider_completion_settlements pcs
		  ON pcs.settlement_id = ae.canonical_settlement_id
		LEFT JOIN provider_invocations api
		  ON api.provider_invocation_id = ae.active_provider_invocation_id
		LEFT JOIN provider_invocations cpi
		  ON cpi.provider_invocation_id = pcs.provider_invocation_id
		LEFT JOIN latest_failure_events lfe
		  ON lfe.node_run_id = lnr.node_run_id
		 AND lfe.failure_rank = 1
		WHERE wn.workflow_definition_id = ?
		ORDER BY wn.node_id ASC`, state.VisibleNodeAttemptOrderSQL("nr"))

	rows, err := s.db.QueryContext(
		ctx,
		query,
		runID,
		runID,
		runID,
		workflowDefinitionID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	nodes := make([]gen.RunGraphNode, 0)
	for rows.Next() {
		var node gen.RunGraphNode
		var nodeClass string
		var nodeRunID sql.NullString
		var attempt sql.NullInt64
		var durableFailureReason sql.NullString
		var durableFailureCode sql.NullString
		var completionInvalidReason sql.NullString
		var failureReason sql.NullString
		var failureReasonText sql.NullString
		var failureReasonCode sql.NullString
		var nodeStatus string
		if err := rows.Scan(&node.NodeId, &node.RequiredRole, &nodeClass, &nodeRunID, &attempt, &nodeStatus, &durableFailureReason, &durableFailureCode, &completionInvalidReason, &failureReason, &failureReasonText, &failureReasonCode); err != nil {
			return nil, err
		}
		node.State = runGraphNodeState(nodeStatus, durableFailureCode, failureReasonCode)
		node.ExecutionOwnership = runGraphNodeExecutionOwnership(node.NodeId, nodeClass)
		if nodeRunID.Valid {
			node.NodeRunId = &nodeRunID.String
		}
		if attempt.Valid {
			parsedAttempt := int(attempt.Int64)
			node.Attempt = &parsedAttempt
		}
		if trimmed, ok := canonicalFailedNodeReason(durableFailureReason, durableFailureCode, completionInvalidReason, failureReason, failureReasonText, failureReasonCode); ok {
			node.FailureReason = &trimmed
		}
		nodes = append(nodes, node)
	}

	return nodes, nil
}

func runGraphNodeExecutionOwnership(nodeID, nodeClass string) gen.RunGraphNodeExecutionOwnership {
	if coreworkflow.BehaviorForNode(nodeID, nodeClass).ExecutionOwnership == coreworkflow.NodeExecutionOwnershipRuntimeOwned {
		return gen.RunGraphNodeExecutionOwnership(coreworkflow.NodeExecutionOwnershipRuntimeOwned)
	}
	return gen.RunGraphNodeExecutionOwnership(coreworkflow.NodeExecutionOwnershipProviderBacked)
}

func (s *Server) fetchRunGraphEdges(ctx context.Context, workflowDefinitionID string) ([]gen.RunGraphEdge, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT from_node_id, to_node_id, on_condition FROM workflow_edges WHERE workflow_definition_id = ? ORDER BY from_node_id ASC, to_node_id ASC, on_condition ASC`,
		workflowDefinitionID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	edges := make([]gen.RunGraphEdge, 0)
	for rows.Next() {
		var edge gen.RunGraphEdge
		if err := rows.Scan(&edge.From, &edge.To, &edge.On); err != nil {
			return nil, err
		}
		edges = append(edges, edge)
	}

	return edges, nil
}

// encodeTimelineCursor signs the timeline sequence to the given run scope so
// cursors cannot be replayed across runs.
func (s *Server) encodeTimelineCursor(runID string, seq int) string {
	payload := strconv.Itoa(seq)
	return s.cursors.Encode("timeline", runID, payload)
}

func (s *Server) decodeTimelineCursor(runID, cursor string) (int, error) {
	payload, err := s.cursors.Decode("timeline", runID, cursor)
	if err != nil {
		return 0, err
	}
	seq, err := strconv.Atoi(payload)
	if err != nil || seq < 0 {
		return 0, fmt.Errorf("invalid cursor sequence")
	}
	return seq, nil
}
