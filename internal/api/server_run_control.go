package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	gen "kingdom/internal/apigen"
	"kingdom/internal/core/errorcatalog"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/state"
)

func (s *Server) handleCancelRun(w http.ResponseWriter, r *http.Request) {
	runID := strings.TrimSpace(r.PathValue("id"))
	if runID == "" {
		s.writeBadRequest(w, "run id is required")
		return
	}
	if !s.runExists(r.Context(), runID) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "run not found", "api.cancel_run", map[string]any{"run_id": runID})
		return
	}

	actorID, roleID, ok := s.requirePrincipal(w, r, "api.cancel_run")
	if !ok {
		return
	}

	reasonText := "run cancelled by user"
	if r.ContentLength > 0 {
		var req gen.CancelRunRequest
		if err := decodeStrictJSON(r, &req); err != nil {
			s.writeBadRequest(w, err.Error())
			return
		}
		if req.Reason != nil && strings.TrimSpace(*req.Reason) != "" {
			reasonText = strings.TrimSpace(*req.Reason)
		}
	}

	for attempt := 0; attempt < 2; attempt++ {
		var runStatus string
		if err := s.db.QueryRowContext(r.Context(), `SELECT run_status FROM workflow_runs WHERE run_id = ?`, runID).Scan(&runStatus); err != nil {
			s.writeInternalError(w, "fetch run status failed")
			return
		}
		if runStatus == "cancelled" {
			s.handleGetRun(w, r)
			return
		}
		if runStatus == "succeeded" || runStatus == "failed" {
			writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, "run is already terminal", "api.cancel_run", map[string]any{"run_id": runID, "run_status": runStatus})
			return
		}

		now := s.now().UTC().Format(time.RFC3339)
		cancelID := s.idGenerator("cancel")
		payloadRaw, err := json.Marshal(map[string]any{
			"run_id":      runID,
			"action":      "cancel",
			"recorded_at": now,
			"actor_id":    actorID,
			"role_id":     roleID,
			"reason":      reasonText,
		})
		if err != nil {
			s.writeInternalError(w, "encode cancel payload failed")
			return
		}

		if err := s.transitionService.TerminalizeRunAndCancelLiveNodes(r.Context(), state.RunTerminalizationRequest{
			Transition: state.TransitionRequest{
				TransitionID:    s.idGenerator("transition"),
				EventID:         s.idGenerator("evt"),
				RunID:           runID,
				Entity:          state.EntityTypeRun,
				FromState:       runStatus,
				ToState:         "cancelled",
				ActorID:         actorID,
				RoleID:          roleID,
				AuthoritySource: "api.cancel_run",
				ReasonCode:      "run_cancelled_by_user",
				ReasonText:      reasonText,
				CorrelationID:   "corr_" + cancelID,
				CausationID:     "caus_" + cancelID,
				OccurredAt:      now,
				AttemptID:       cancelID,
				SchemaVersion:   "v1",
				EventType:       "run.state_changed",
				EventSeq:        1,
				PayloadJSON:     string(payloadRaw),
			},
			GenerateID: s.idGenerator,
		}); err != nil {
			var illegalTransitionErr *state.IllegalTransitionError
			var stateConflictErr *state.StateConflictError
			switch {
			case errors.As(err, &stateConflictErr):
				continue
			case errors.As(err, &illegalTransitionErr):
				writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, "run cancellation is not allowed", "api.cancel_run", map[string]any{"run_id": runID, "run_status": runStatus})
			default:
				s.writeInternalError(w, "apply run cancel transition failed")
			}
			return
		}

		s.handleGetRun(w, r)
		return
	}

	writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, "run cancellation is not allowed", "api.cancel_run", map[string]any{"run_id": runID})
}

func (s *Server) handleRetryExecutionStop(w http.ResponseWriter, r *http.Request) {
	runID := strings.TrimSpace(r.PathValue("id"))
	attemptID := strings.TrimSpace(r.PathValue("attemptId"))
	if runID == "" || attemptID == "" {
		s.writeBadRequest(w, "run id and attempt id are required")
		return
	}
	actorID, roleID, ok := s.requirePrincipal(w, r, "api.retry_execution_stop")
	if !ok {
		return
	}
	if !s.runExists(r.Context(), runID) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "run not found", "api.retry_execution_stop", map[string]any{"run_id": runID})
		return
	}
	_, found, err := executionstore.New(s.db).GetRunExecutionDetail(r.Context(), runID, attemptID)
	if err != nil {
		s.writeInternalError(w, "get run execution failed")
		return
	}
	if !found {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "run execution not found", "api.retry_execution_stop", map[string]any{"run_id": runID, "attempt_id": attemptID})
		return
	}
	if err := s.transitionService.RequestAttemptStopRetry(r.Context(), state.AttemptStopRetryRequest{
		RunID:      runID,
		AttemptID:  attemptID,
		ActorID:    actorID,
		RoleID:     roleID,
		OccurredAt: s.now().UTC().Format(time.RFC3339),
		GenerateID: s.idGenerator,
	}); err != nil {
		var unavailable *state.AttemptStopRetryUnavailableError
		if errors.As(err, &unavailable) {
			writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, "stop retry is not available for this execution", "api.retry_execution_stop", map[string]any{"run_id": runID, "attempt_id": attemptID})
			return
		}
		s.writeInternalError(w, "request execution stop retry failed")
		return
	}

	writeJSON(w, http.StatusAccepted, gen.RetryExecutionStopResult{
		RunId:              runID,
		AttemptId:          attemptID,
		StopRetryRequested: true,
	})
}

func (s *Server) handleRetryNode(w http.ResponseWriter, r *http.Request) {
	runID := strings.TrimSpace(r.PathValue("id"))
	nodeKey := strings.TrimSpace(r.PathValue("nodeKey"))
	if !s.runExists(r.Context(), runID) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "run not found", "api.retry_node", map[string]any{"run_id": runID})
		return
	}
	if !s.nodeExistsForRun(r.Context(), runID, nodeKey) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "node not found for run", "api.retry_node", map[string]any{"run_id": runID, "node_key": nodeKey})
		return
	}

	var req gen.RetryNodeRequest
	if err := decodeStrictJSON(r, &req); err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}

	actorID, roleID, ok := s.requirePrincipal(w, r, "api.retry_node")
	if !ok {
		return
	}
	runStatus, err := s.runStatusForRun(r.Context(), runID)
	if err != nil {
		s.writeInternalError(w, "fetch run status failed")
		return
	}
	if runStatus != "failed" && runStatus != "cancelled" {
		writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, "run must be failed or cancelled before retry", "api.retry_node", map[string]any{"run_id": runID, "run_status": runStatus})
		return
	}

	if _, err := s.transitionService.ReplayTerminalRun(r.Context(), state.ReplayTerminalRunRequest{
		RunID:           runID,
		AnchorNodeID:    nodeKey,
		TargetNodeIDs:   []string{nodeKey},
		Reason:          req.Reason,
		ActorID:         actorID,
		RoleID:          roleID,
		AuthoritySource: "api.retry_node",
		RunEventType:    "run.retry_reopened",
		RunReasonCode:   "manual_retry_requested",
		NodeEventType:   "attempt.retry_queued",
		EventPrefix:     "retry",
		OccurredAt:      s.now().UTC().Format(time.RFC3339),
		GenerateID:      s.idGenerator,
	}); err != nil {
		s.writeRunTransitionApplyError(w, err, "api.retry_node", "retry queue is not allowed for the current run state", "queue node retry failed", map[string]any{"run_id": runID, "node_key": nodeKey})
		return
	}

	writeJSON(w, http.StatusAccepted, gen.RetryNodeResult{
		RunId:         runID,
		NodeKey:       nodeKey,
		RetryEnqueued: true,
	})
}

func (s *Server) handleRerunFromNode(w http.ResponseWriter, r *http.Request) {
	runID := strings.TrimSpace(r.PathValue("id"))
	nodeKey := strings.TrimSpace(r.PathValue("nodeKey"))
	if !s.runExists(r.Context(), runID) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "run not found", "api.rerun_from_node", map[string]any{"run_id": runID})
		return
	}
	if !s.nodeExistsForRun(r.Context(), runID, nodeKey) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "node not found for run", "api.rerun_from_node", map[string]any{"run_id": runID, "node_key": nodeKey})
		return
	}

	var req gen.RerunFromNodeRequest
	if err := decodeStrictJSON(r, &req); err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}

	actorID, roleID, ok := s.requirePrincipal(w, r, "api.rerun_from_node")
	if !ok {
		return
	}
	runStatus, err := s.runStatusForRun(r.Context(), runID)
	if err != nil {
		s.writeInternalError(w, "fetch run status failed")
		return
	}
	if runStatus != "failed" && runStatus != "cancelled" {
		writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, "run must be failed or cancelled before rerun", "api.rerun_from_node", map[string]any{"run_id": runID, "run_status": runStatus})
		return
	}
	replayPlan, err := s.manualRerunPlan(r.Context(), runID, nodeKey)
	if err != nil {
		s.writeInternalError(w, "resolve rerun targets failed")
		return
	}
	if len(replayPlan.TargetNodeIDs) == 0 {
		writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, "no rerun targets resolved for requested node", "api.rerun_from_node", map[string]any{"run_id": runID, "node_key": nodeKey, "effective_node": replayPlan.DisplayAnchorNodeID})
		return
	}

	if _, err := s.transitionService.ReplayTerminalRun(r.Context(), state.ReplayTerminalRunRequest{
		RunID:           runID,
		AnchorNodeID:    replayPlan.DisplayAnchorNodeID,
		TargetNodeIDs:   replayPlan.TargetNodeIDs,
		Reason:          req.Reason,
		ActorID:         actorID,
		RoleID:          roleID,
		AuthoritySource: "api.rerun_from_node",
		RunEventType:    "run.rerun_reopened",
		RunReasonCode:   "manual_rerun_requested",
		NodeEventType:   "attempt.rerun_queued",
		EventPrefix:     "rerun",
		OccurredAt:      s.now().UTC().Format(time.RFC3339),
		GenerateID:      s.idGenerator,
	}); err != nil {
		s.writeRunTransitionApplyError(w, err, "api.rerun_from_node", "rerun queue is not allowed for the current run state", "queue rerun attempts failed", map[string]any{"run_id": runID, "node_key": replayPlan.DisplayAnchorNodeID})
		return
	}

	writeJSON(w, http.StatusAccepted, gen.RerunFromNodeResult{
		RunId:         runID,
		FromNode:      replayPlan.DisplayAnchorNodeID,
		RerunEnqueued: true,
	})
}

type createFinalApprovalRequest struct {
	Action string `json:"action"`
	Reason string `json:"reason"`
}

type createPlanApprovalRequest struct {
	Action         string `json:"action"`
	PlanRevisionID string `json:"plan_revision_id"`
	Reason         string `json:"reason"`
}

func validateGovernanceReason(action, rawReason string) (string, error) {
	trimmedAction := strings.ToLower(strings.TrimSpace(action))
	trimmedReason := strings.TrimSpace(rawReason)
	if trimmedAction == "rework" && trimmedReason == "" {
		return "", fmt.Errorf("reason is required when action is rework")
	}
	return trimmedReason, nil
}

func (s *Server) handleCreatePlanApproval(w http.ResponseWriter, r *http.Request) {
	runID := strings.TrimSpace(r.PathValue("id"))
	if runID == "" {
		s.writeBadRequest(w, "run id is required")
		return
	}
	if !s.runExists(r.Context(), runID) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "run not found", "api.create_plan_approval", map[string]any{"run_id": runID})
		return
	}

	var req createPlanApprovalRequest
	if err := decodeStrictJSON(r, &req); err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}

	actorID, roleID, ok := s.requirePrincipal(w, r, "api.create_plan_approval")
	if !ok {
		return
	}

	action := strings.ToLower(strings.TrimSpace(req.Action))
	if action != "confirm" && action != "rework" {
		s.writeBadRequest(w, "action must be confirm or rework")
		return
	}
	reason, err := validateGovernanceReason(action, req.Reason)
	if err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}

	var runStatus string
	var planState string
	var autonomousApprovals int
	if err := s.db.QueryRowContext(r.Context(), `SELECT run_status, plan_state, COALESCE(autonomous_approvals, 1) FROM workflow_runs WHERE run_id = ?`, runID).Scan(&runStatus, &planState, &autonomousApprovals); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "run not found", "api.create_plan_approval", map[string]any{"run_id": runID})
			return
		}
		s.writeInternalError(w, "fetch run status failed")
		return
	}
	if runStatus != "running" {
		writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, "run is not active for plan approval", "api.create_plan_approval", map[string]any{"run_id": runID, "run_status": runStatus})
		return
	}
	if autonomousApprovals != 0 {
		writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, "run is configured for autonomous plan approvals", "api.create_plan_approval", map[string]any{"run_id": runID})
		return
	}
	if planState != "awaiting_king_decision" {
		writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, "plan is not awaiting allfather approval", "api.create_plan_approval", map[string]any{"run_id": runID, "plan_state": planState})
		return
	}
	planRevisionID := strings.TrimSpace(req.PlanRevisionID)
	if planRevisionID == "" {
		s.writeBadRequest(w, "plan_revision_id is required")
		return
	}
	planSnapshot, hasPlanRevision, err := s.loadRunPlanSnapshot(r.Context(), runID)
	if err != nil {
		s.writeInternalError(w, "load current run plan failed")
		return
	}
	if !hasPlanRevision {
		writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, "run has no current plan to review", "api.create_plan_approval", map[string]any{"run_id": runID})
		return
	}
	currentPlanRevisionID := ""
	if planSnapshot.PlanRevisionId != nil {
		currentPlanRevisionID = strings.TrimSpace(*planSnapshot.PlanRevisionId)
	}
	if currentPlanRevisionID == "" || currentPlanRevisionID != planRevisionID {
		writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, "plan approval must reference the current plan revision", "api.create_plan_approval", map[string]any{"run_id": runID, "plan_revision_id": planRevisionID, "current_plan_revision_id": currentPlanRevisionID})
		return
	}

	now := s.now().UTC().Format(time.RFC3339)
	approvalID := s.idGenerator("plan_approval")
	attemptID := "run:" + runID + ":plan"
	eventSeq := s.nextAttemptEventSeq(r.Context(), attemptID)
	toState := "approved"
	reasonCode := "governance_decision"
	reasonText := "allfather approved plan"
	decision := state.RunPlanReviewDecisionApproved
	if action == "rework" {
		toState = "needs_rework"
		reasonCode = "rework_requested"
		reasonText = "allfather requested plan rework"
		decision = state.RunPlanReviewDecisionNeedsRework
	}
	if reason != "" {
		reasonText = reason
	}

	payloadRaw, err := json.Marshal(map[string]any{
		"approval_id":      approvalID,
		"run_id":           runID,
		"scope":            "plan",
		"action":           action,
		"decision":         decision,
		"plan_revision_id": planRevisionID,
		"recorded_at":      now,
		"actor_id":         actorID,
		"role_id":          roleID,
	})
	if err != nil {
		s.writeInternalError(w, "encode plan approval payload failed")
		return
	}

	if _, err := s.transitionService.RecordPlanDecision(r.Context(), state.PlanDecisionRecordRequest{
		Transition: state.TransitionRequest{
			TransitionID:    s.idGenerator("transition"),
			EventID:         s.idGenerator("evt"),
			RunID:           runID,
			Entity:          state.EntityTypePlan,
			FromState:       "awaiting_king_decision",
			ToState:         toState,
			ActorID:         actorID,
			RoleID:          roleID,
			AuthoritySource: "api.create_plan_approval",
			ReasonCode:      reasonCode,
			ReasonText:      reasonText,
			CorrelationID:   "corr_" + approvalID,
			CausationID:     "caus_" + approvalID,
			OccurredAt:      now,
			AttemptID:       attemptID,
			SchemaVersion:   "v1",
			EventType:       "approval.recorded",
			EventSeq:        eventSeq,
			PayloadJSON:     string(payloadRaw),
		},
		PlanReview: state.CreateRunPlanReviewRequest{
			PlanReviewID:           approvalID,
			RunID:                  runID,
			ReviewedPlanRevisionID: planRevisionID,
			Decision:               decision,
			FeedbackMarkdown:       reason,
			ActorID:                actorID,
			RoleID:                 roleID,
			CreatedAt:              now,
		},
		GenerateID: s.idGenerator,
	}); err != nil {
		s.writeRunTransitionApplyError(w, err, "api.create_plan_approval", "plan transition is not allowed", "apply plan approval transition failed", map[string]any{"run_id": runID, "action": action})
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"approval_id": approvalID,
		"run_id":      runID,
		"status":      toState,
		"recorded_at": now,
	})
}

func (s *Server) nextAttemptEventSeq(ctx context.Context, attemptID string) int {
	trimmedAttemptID := strings.TrimSpace(attemptID)
	if trimmedAttemptID == "" {
		return 1
	}
	var seq int
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq), 0) + 1 FROM events WHERE attempt_id = ?`, trimmedAttemptID).Scan(&seq)
	if err != nil || seq <= 0 {
		return 1
	}
	return seq
}

func (s *Server) handleCreateFinalApproval(w http.ResponseWriter, r *http.Request) {
	runID := strings.TrimSpace(r.PathValue("id"))
	if runID == "" {
		s.writeBadRequest(w, "run id is required")
		return
	}
	if !s.runExists(r.Context(), runID) {
		writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "run not found", "api.create_final_approval", map[string]any{"run_id": runID})
		return
	}

	var req createFinalApprovalRequest
	if err := decodeStrictJSON(r, &req); err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}

	actorID, roleID, ok := s.requirePrincipal(w, r, "api.create_final_approval")
	if !ok {
		return
	}

	action := strings.ToLower(strings.TrimSpace(req.Action))
	if action != "confirm" && action != "rework" {
		s.writeBadRequest(w, "action must be confirm or rework")
		return
	}
	reason, err := validateGovernanceReason(action, req.Reason)
	if err != nil {
		s.writeBadRequest(w, err.Error())
		return
	}

	var runStatus string
	var autonomousApprovals int
	if err := s.db.QueryRowContext(r.Context(), `SELECT run_status, COALESCE(autonomous_approvals, 1) FROM workflow_runs WHERE run_id = ?`, runID).Scan(&runStatus, &autonomousApprovals); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeCanonicalError(w, http.StatusNotFound, errorcatalog.CodeDependencyBlocked, "run not found", "api.create_final_approval", map[string]any{"run_id": runID})
			return
		}
		s.writeInternalError(w, "fetch run status failed")
		return
	}
	if runStatus != "awaiting_human_confirmation" {
		writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, "run is not awaiting final human confirmation", "api.create_final_approval", map[string]any{"run_id": runID, "run_status": runStatus})
		return
	}
	if autonomousApprovals != 0 {
		writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, "run is configured for autonomous final approvals", "api.create_final_approval", map[string]any{"run_id": runID})
		return
	}

	now := s.now().UTC().Format(time.RFC3339)
	approvalID := s.idGenerator("approval")
	toState := "succeeded"
	decision := "approve"
	reasonCode := "governance_decision"
	reasonText := "allfather confirmed decree completion"
	if action == "rework" {
		toState = "running"
		decision = "needs_rework"
		reasonCode = "rework_requested"
		reasonText = "allfather requested decree rework"
	}
	if reason != "" {
		reasonText = reason
	}

	payloadRaw, err := json.Marshal(map[string]any{
		"approval_id": approvalID,
		"run_id":      runID,
		"action":      action,
		"recorded_at": now,
		"actor_id":    actorID,
		"role_id":     roleID,
	})
	if err != nil {
		s.writeInternalError(w, "encode final approval payload failed")
		return
	}

	transition := state.TransitionRequest{
		TransitionID:                s.idGenerator("transition"),
		EventID:                     s.idGenerator("evt"),
		RunID:                       runID,
		Entity:                      state.EntityTypeRun,
		FromState:                   "awaiting_human_confirmation",
		ToState:                     toState,
		ActorID:                     actorID,
		RoleID:                      roleID,
		AuthoritySource:             "api.create_final_approval",
		ReasonCode:                  reasonCode,
		ReasonText:                  reasonText,
		CorrelationID:               "corr_" + approvalID,
		CausationID:                 "caus_" + approvalID,
		OccurredAt:                  now,
		AttemptID:                   approvalID,
		SchemaVersion:               "v1",
		EventType:                   "approval.recorded",
		EventSeq:                    1,
		PayloadJSON:                 string(payloadRaw),
		SkipRunCollectionProjection: action == "rework",
	}

	var applyErr error
	if action == "rework" {
		replayPlan, replayErr := s.finalReworkPlan(r.Context(), runID)
		if replayErr != nil {
			s.writeInternalError(w, "resolve final rework replay targets failed")
			return
		}
		applyErr = s.transitionService.RecordFinalRework(r.Context(), state.FinalReworkRecordRequest{
			RunTransition: transition,
			PlanTransition: state.TransitionRequest{
				TransitionID:    s.idGenerator("transition"),
				EventID:         s.idGenerator("evt"),
				RunID:           runID,
				Entity:          state.EntityTypePlan,
				FromState:       "approved",
				ToState:         "needs_rework",
				ActorID:         actorID,
				RoleID:          roleID,
				AuthoritySource: "api.create_final_approval",
				ReasonCode:      reasonCode,
				ReasonText:      reasonText,
				CorrelationID:   "corr_plan_" + approvalID,
				CausationID:     "caus_plan_" + approvalID,
				OccurredAt:      now,
				AttemptID:       "run:" + runID + ":plan",
				SchemaVersion:   "v1",
				EventType:       "approval.recorded",
				EventSeq:        s.nextAttemptEventSeq(r.Context(), "run:"+runID+":plan"),
				PayloadJSON:     string(payloadRaw),
			},
			ApprovalID:     approvalID,
			ApprovalReason: reasonText,
			FinalReview: state.CreateRunFinalReviewRequest{
				FinalReviewID:    approvalID,
				RunID:            runID,
				Decision:         state.RunFinalReviewDecisionNeedsRework,
				FeedbackMarkdown: reason,
				ActorID:          actorID,
				RoleID:           roleID,
				CreatedAt:        now,
			},
			TargetNodeIDs: replayPlan.TargetNodeIDs,
			GenerateID:    s.idGenerator,
		})
	} else {
		finalReview := state.CreateRunFinalReviewRequest{
			FinalReviewID:    approvalID,
			RunID:            runID,
			Decision:         state.RunFinalReviewDecisionConfirmed,
			FeedbackMarkdown: reason,
			ActorID:          actorID,
			RoleID:           roleID,
			CreatedAt:        now,
		}
		applyErr = s.transitionService.RecordFinalApproval(r.Context(), state.FinalApprovalRecordRequest{
			Transition:  transition,
			ApprovalID:  approvalID,
			Decision:    decision,
			ReasonText:  reasonText,
			FinalReview: &finalReview,
		})
	}

	if applyErr != nil {
		s.writeRunTransitionApplyError(w, applyErr, "api.create_final_approval", "run transition is not allowed", "apply final approval transition failed", map[string]any{"run_id": runID, "action": action})
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"approval_id": approvalID,
		"run_id":      runID,
		"status":      toState,
		"recorded_at": now,
	})
}

func (s *Server) nodeExistsForRun(ctx context.Context, runID string, nodeID string) bool {
	var marker int
	err := s.db.QueryRowContext(
		ctx,
		`SELECT 1
		 FROM workflow_runs wr
		 JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id
		 WHERE wr.run_id = ? AND wn.node_id = ?
		 LIMIT 1`,
		runID,
		nodeID,
	).Scan(&marker)
	return err == nil
}

func (s *Server) writeRunTransitionApplyError(w http.ResponseWriter, err error, operation, blockedMessage, internalMessage string, details map[string]any) {
	var illegalTransitionErr *state.IllegalTransitionError
	var stateConflictErr *state.StateConflictError
	var replayWaveActiveErr *state.ReplayWaveActiveError
	var replayNotQuiescentErr *state.ReplayNotQuiescentError
	var worktreeCleanupInProgressErr *state.RunWorktreeCleanupInProgressError
	switch {
	case errors.As(err, &replayNotQuiescentErr):
		writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, replayNotQuiescentErr.Error(), operation, mergeCanonicalErrorDetails(details, map[string]any{"settling": replayNotQuiescentErr.Blockers}))
	case errors.As(err, &worktreeCleanupInProgressErr):
		writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, worktreeCleanupInProgressErr.Error(), operation, mergeCanonicalErrorDetails(details, map[string]any{"cleanup_started": true}))
	case errors.As(err, &illegalTransitionErr), errors.As(err, &stateConflictErr), errors.As(err, &replayWaveActiveErr):
		writeCanonicalError(w, http.StatusConflict, errorcatalog.CodeDependencyBlocked, blockedMessage, operation, details)
	default:
		s.writeInternalError(w, internalMessage)
	}
}

func mergeCanonicalErrorDetails(base map[string]any, extra map[string]any) map[string]any {
	merged := make(map[string]any, len(base)+len(extra))
	for key, value := range base {
		merged[key] = value
	}
	for key, value := range extra {
		merged[key] = value
	}
	return merged
}

func (s *Server) runStatusForRun(ctx context.Context, runID string) (string, error) {
	var runStatus string
	err := s.db.QueryRowContext(ctx, `SELECT run_status FROM workflow_runs WHERE run_id = ?`, runID).Scan(&runStatus)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(runStatus), nil
}
