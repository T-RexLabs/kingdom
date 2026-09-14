package state

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
)

type QueuedNodeAttempt struct {
	NodeID       string
	NodeRunID    string
	Attempt      int
	Wave         int
	RequiredRole string
	AgentID      string
}

type ReplayTerminalRunRequest struct {
	RunID           string
	AnchorNodeID    string
	TargetNodeIDs   []string
	Reason          string
	ActorID         string
	RoleID          string
	AuthoritySource string
	RunEventType    string
	RunReasonCode   string
	NodeEventType   string
	NodeReasonCode  string
	EventPrefix     string
	OccurredAt      string
	GenerateID      func(string) string
}

type QueuePendingNodeWaveRequest struct {
	RunID               string
	NodeIDs             []string
	IgnoreActiveNodeIDs []string
	OccurredAt          string
	GenerateID          func(string) string
}

type QueuePendingNodeWaveResult struct {
	Queued   bool
	Attempts []QueuedNodeAttempt
}

type FinalApprovalRecordRequest struct {
	Transition  TransitionRequest
	ApprovalID  string
	Decision    string
	ReasonText  string
	FinalReview *CreateRunFinalReviewRequest
}

type FinalReworkRecordRequest struct {
	RunTransition  TransitionRequest
	PlanTransition TransitionRequest
	ApprovalID     string
	ApprovalReason string
	FinalReview    CreateRunFinalReviewRequest
	TargetNodeIDs  []string
	GenerateID     func(string) string
}

type PlanDecisionRecordRequest struct {
	Transition TransitionRequest
	PlanReview CreateRunPlanReviewRequest
	GenerateID func(string) string
}

type PlanDecisionRecordResult struct {
	Review         RunPlanReview
	Queued         bool
	QueuedAttempts []QueuedNodeAttempt
}

type ScheduleFencedStaleRetryRequest struct {
	Attempt            staleAttempt
	ReplacementAgentID string
	HasReplacement     bool
	OccurredAt         string
	CurrentAttemptID   string
	CurrentEventSeq    int
}

type RunTerminalizationRequest struct {
	Transition TransitionRequest
	GenerateID func(string) string
}

type ExecutionStartRecordRequest struct {
	Transition TransitionRequest
}

type ExecutionLaunchRecordRequest struct {
	Transition TransitionRequest
}

type NodeCompletionRecordRequest struct {
	Transition     TransitionRequest
	PlanningOutput *CreateRunPlanningOutputRequest
}

type DirectExecutionTerminalRequest struct {
	Transition TransitionRequest
}

type LoopCapAutoSuccessRecordRequest struct {
	Transition         TransitionRequest
	ParticipantNodeIDs []string
	CompletionOutbox   *executionstore.QueueOutboxRequest
}

type DurableAttemptNodeProjectionRequest struct {
	RunID          string
	NodeRunID      string
	AttemptID      string
	ActorID        string
	OccurredAt     string
	PlanningOutput *CreateRunPlanningOutputRequest
	GenerateID     func(string) string
}

type SupersedeQueuedAttemptsRequest struct {
	RunID       string
	NodeID      string
	KeepAttempt int
	OccurredAt  string
	ActorID     string
	GenerateID  func(string) string
}

type SupersedeQueuedAttemptsResult struct {
	Outcome          SupersedeQueuedAttemptsOutcome
	CancelledNodeRun []string
}

func (r SupersedeQueuedAttemptsResult) AllowsCanonicalAdoption() bool {
	switch r.Outcome {
	case SupersedeQueuedAttemptsOutcomeNoNewerAttempts, SupersedeQueuedAttemptsOutcomeAdopted:
		return true
	default:
		return false
	}
}

type liveNodeAttempt struct {
	RunID                      string
	NodeRunID                  string
	NodeID                     string
	Attempt                    int
	NodeStatus                 string
	SupersededAt               string
	FailureCode                string
	RequiredRole               string
	AgentID                    string
	AttemptID                  string
	SessionID                  string
	ExecutionMode              executionstore.ExecutionMode
	ExecutionState             string
	LaunchStartedAt            string
	StartedAt                  string
	ProviderSessionID          string
	ActiveProviderInvocationID string
}

type attemptSupersessionStage string

type SupersedeQueuedAttemptsOutcome string

type attemptSupersessionDecision struct {
	stage          attemptSupersessionStage
	blockedOutcome SupersedeQueuedAttemptsOutcome
}

const (
	attemptSupersessionStageNone      attemptSupersessionStage = ""
	attemptSupersessionStagePreLaunch attemptSupersessionStage = "pre_launch"
	attemptSupersessionStageLaunched  attemptSupersessionStage = "launched"

	SupersedeQueuedAttemptsOutcomeNoNewerAttempts               SupersedeQueuedAttemptsOutcome = "no_newer_attempts"
	SupersedeQueuedAttemptsOutcomeAdopted                       SupersedeQueuedAttemptsOutcome = "adopted"
	SupersedeQueuedAttemptsOutcomeBlockedByNewerLiveAttempt     SupersedeQueuedAttemptsOutcome = "blocked_by_newer_live_attempt"
	SupersedeQueuedAttemptsOutcomeBlockedByNewerTerminalAttempt SupersedeQueuedAttemptsOutcome = "blocked_by_newer_terminal_attempt"
)

func (s *TransitionService) ReplayTerminalRun(ctx context.Context, req ReplayTerminalRunRequest) ([]QueuedNodeAttempt, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		queued, err := s.replayTerminalRunOnce(ctx, req)
		if err == nil {
			return queued, nil
		}
		if !IsUniqueViolation(err) {
			return nil, err
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("replay queue failed")
	}
	return nil, lastErr
}

func (s *TransitionService) RecordNodeCompletion(ctx context.Context, req NodeCompletionRecordRequest) error {
	if err := validateNodeCompletionRecordRequest(req); err != nil {
		return err
	}
	return s.applyWithEffects(ctx, req.Transition, func(ctx context.Context, tx *sql.Tx) error {
		if req.PlanningOutput == nil {
			return nil
		}
		return persistRunPlanningOutputTx(ctx, tx, s.db, *req.PlanningOutput)
	})
}

func validateNodeCompletionRecordRequest(req NodeCompletionRecordRequest) error {
	transition := req.Transition
	if transition.Entity != EntityTypeNode {
		return fmt.Errorf("node completion requires node transition entity")
	}
	return validatePlanningOutputTransitionLineage(transition, req.PlanningOutput)
}

func validatePlanningOutputTransitionLineage(transition TransitionRequest, planningOutput *CreateRunPlanningOutputRequest) error {
	if planningOutput == nil {
		return nil
	}
	output := *planningOutput
	if strings.TrimSpace(output.RunID) != strings.TrimSpace(transition.RunID) {
		return fmt.Errorf("planning output run id must match transition run id")
	}
	if strings.TrimSpace(output.NodeRunID) != strings.TrimSpace(transition.NodeRunID.String) {
		return fmt.Errorf("planning output node run id must match transition node run id")
	}
	if strings.TrimSpace(output.AttemptID) != strings.TrimSpace(transition.AttemptID) {
		return fmt.Errorf("planning output attempt id must match transition attempt id")
	}
	if strings.TrimSpace(output.CreatedAt) != strings.TrimSpace(transition.OccurredAt) {
		return fmt.Errorf("planning output created_at must match transition occurred_at")
	}
	return validateRunPlanningOutputCreate(output)
}

func (s *TransitionService) QueuePendingNodeWave(ctx context.Context, req QueuePendingNodeWaveRequest) (QueuePendingNodeWaveResult, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		result, err := s.queuePendingNodeWaveOnce(ctx, req)
		if err == nil {
			return result, nil
		}
		if !IsUniqueViolation(err) {
			return QueuePendingNodeWaveResult{}, err
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("queue pending node wave failed")
	}
	return QueuePendingNodeWaveResult{}, lastErr
}

func (s *TransitionService) queuePendingNodeWaveOnce(ctx context.Context, req QueuePendingNodeWaveRequest) (QueuePendingNodeWaveResult, error) {
	if strings.TrimSpace(req.RunID) == "" {
		return QueuePendingNodeWaveResult{}, fmt.Errorf("run id is required")
	}
	if len(req.NodeIDs) == 0 {
		return QueuePendingNodeWaveResult{}, fmt.Errorf("at least one node id is required")
	}
	occurredAt := strings.TrimSpace(req.OccurredAt)
	if occurredAt == "" {
		occurredAt = time.Now().UTC().Format(time.RFC3339)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return QueuePendingNodeWaveResult{}, fmt.Errorf("begin queue pending node wave transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	queued, err := s.queuePendingNodeWaveTx(ctx, tx, req.RunID, req.NodeIDs, req.IgnoreActiveNodeIDs, occurredAt, req.GenerateID)
	if err != nil {
		return QueuePendingNodeWaveResult{}, err
	}
	if len(queued) == 0 {
		return QueuePendingNodeWaveResult{Queued: false}, nil
	}

	if err := tx.Commit(); err != nil {
		return QueuePendingNodeWaveResult{}, fmt.Errorf("commit queue pending node wave transaction: %w", err)
	}
	committed = true
	return QueuePendingNodeWaveResult{Queued: true, Attempts: queued}, nil
}

func filterIgnoredNodeIDs(nodeIDs, ignoredNodeIDs []string) []string {
	if len(nodeIDs) == 0 || len(ignoredNodeIDs) == 0 {
		return nodeIDs
	}
	ignored := make(map[string]struct{}, len(ignoredNodeIDs))
	for _, nodeID := range ignoredNodeIDs {
		trimmed := strings.TrimSpace(nodeID)
		if trimmed == "" {
			continue
		}
		ignored[trimmed] = struct{}{}
	}
	if len(ignored) == 0 {
		return nodeIDs
	}
	filtered := make([]string, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		if _, skip := ignored[strings.TrimSpace(nodeID)]; skip {
			continue
		}
		filtered = append(filtered, nodeID)
	}
	return filtered
}

func (s *TransitionService) queuePendingNodeWaveTx(ctx context.Context, tx *sql.Tx, runID string, nodeIDs, ignoreActiveNodeIDs []string, occurredAt string, generate func(string) string) ([]QueuedNodeAttempt, error) {
	runStatus, err := runStatusForQueuePendingWaveTx(ctx, tx, runID)
	if err != nil {
		return nil, err
	}
	if runStatus != "running" {
		return nil, nil
	}

	active, err := liveNodeAttemptExistsTx(ctx, tx, runID, filterIgnoredNodeIDs(nodeIDs, ignoreActiveNodeIDs))
	if err != nil {
		return nil, err
	}
	if active {
		return nil, nil
	}
	queuedWaves, err := s.resolveQueuedNodeWavesTx(ctx, tx, runID, nodeIDs)
	if err != nil {
		return nil, err
	}

	queued := make([]QueuedNodeAttempt, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		queuedAttempt, err := s.insertPendingNodeAttemptTx(ctx, tx, runID, nodeID, queuedWaves[strings.TrimSpace(nodeID)], occurredAt, generate)
		if err != nil {
			return nil, err
		}
		queued = append(queued, queuedAttempt)
	}
	return queued, nil
}

func runStatusForQueuePendingWaveTx(ctx context.Context, tx *sql.Tx, runID string) (string, error) {
	var runStatus string
	err := tx.QueryRowContext(ctx, `SELECT run_status FROM workflow_runs WHERE run_id = ?`, strings.TrimSpace(runID)).Scan(&runStatus)
	if errors.Is(err, sql.ErrNoRows) {
		return "", &EntityNotFoundError{EntityType: EntityTypeRun, RunID: strings.TrimSpace(runID)}
	}
	if err != nil {
		return "", fmt.Errorf("load run status for pending wave queue: %w", err)
	}
	return strings.TrimSpace(runStatus), nil
}

func (s *TransitionService) queuePlanReworkWaveTx(ctx context.Context, tx *sql.Tx, runID, occurredAt string, generate func(string) string) ([]QueuedNodeAttempt, error) {
	nodeIDs, err := planReworkNodeIDsTx(ctx, tx, runID)
	if err != nil {
		return nil, err
	}
	queued, err := s.queuePendingNodeWaveTx(ctx, tx, runID, nodeIDs, nil, occurredAt, generate)
	if err != nil {
		return nil, err
	}
	if len(queued) != len(nodeIDs) {
		return nil, &ReplayWaveActiveError{RunID: runID, Scope: "plan rework wave"}
	}
	return queued, nil
}

func planReworkNodeIDsTx(ctx context.Context, tx *sql.Tx, runID string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT wn.node_id, wn.node_class
FROM workflow_runs wr
JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id
WHERE wr.run_id = ?
  AND wn.node_class IN ('advisory_synthesis', 'plan_drafting')
ORDER BY CASE wn.node_class
  WHEN 'advisory_synthesis' THEN 1
  WHEN 'plan_drafting' THEN 2
  ELSE 99
END, wn.node_id ASC`, runID)
	if err != nil {
		return nil, fmt.Errorf("list plan rework nodes: %w", err)
	}
	defer rows.Close()

	nodeIDs := make([]string, 0)
	hasAdvisorySynthesis := false
	hasPlanDrafting := false
	for rows.Next() {
		var nodeID string
		var nodeClass string
		if err := rows.Scan(&nodeID, &nodeClass); err != nil {
			return nil, fmt.Errorf("scan plan rework node: %w", err)
		}
		switch strings.TrimSpace(nodeClass) {
		case "advisory_synthesis":
			hasAdvisorySynthesis = true
		case "plan_drafting":
			hasPlanDrafting = true
		}
		nodeIDs = append(nodeIDs, strings.TrimSpace(nodeID))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate plan rework nodes: %w", err)
	}
	if !hasAdvisorySynthesis || !hasPlanDrafting {
		return nil, fmt.Errorf("plan rework requires advisory_synthesis and plan_drafting nodes for run %q", runID)
	}
	return nodeIDs, nil
}

func queuedAttemptNumberForNodeClassTx(ctx context.Context, tx *sql.Tx, runID string, queued []QueuedNodeAttempt, nodeClass string) (*int, error) {
	if len(queued) == 0 {
		return nil, nil
	}
	trimmedRunID := strings.TrimSpace(runID)
	trimmedNodeClass := strings.TrimSpace(nodeClass)
	if trimmedRunID == "" || trimmedNodeClass == "" {
		return nil, fmt.Errorf("run id and node class are required")
	}
	queuedAttemptsByNodeID := make(map[string]int, len(queued))
	nodeIDs := make([]string, 0, len(queued))
	for _, item := range queued {
		trimmedNodeID := strings.TrimSpace(item.NodeID)
		if trimmedNodeID == "" {
			continue
		}
		queuedAttemptsByNodeID[trimmedNodeID] = item.Attempt
		nodeIDs = append(nodeIDs, trimmedNodeID)
	}
	if len(nodeIDs) == 0 {
		return nil, nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(nodeIDs)), ",")
	args := make([]any, 0, len(nodeIDs)+2)
	args = append(args, trimmedRunID, trimmedNodeClass)
	for _, nodeID := range nodeIDs {
		args = append(args, nodeID)
	}
	query := fmt.Sprintf(`
SELECT wn.node_id
FROM workflow_runs wr
JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id
WHERE wr.run_id = ?
  AND wn.node_class = ?
  AND wn.node_id IN (%s)
ORDER BY wn.node_id ASC`, placeholders)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list queued node ids for class %q: %w", trimmedNodeClass, err)
	}
	defer rows.Close()
	resolvedAttempt := 0
	matched := false
	for rows.Next() {
		var nodeID string
		if err := rows.Scan(&nodeID); err != nil {
			return nil, fmt.Errorf("scan queued node id for class %q: %w", trimmedNodeClass, err)
		}
		attempt, ok := queuedAttemptsByNodeID[strings.TrimSpace(nodeID)]
		if !ok {
			continue
		}
		if !matched {
			resolvedAttempt = attempt
			matched = true
			continue
		}
		if resolvedAttempt != attempt {
			return nil, fmt.Errorf("queued %s attempts diverged for run %q", trimmedNodeClass, trimmedRunID)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate queued node ids for class %q: %w", trimmedNodeClass, err)
	}
	if !matched {
		return nil, nil
	}
	attempt := resolvedAttempt
	return &attempt, nil
}

func (s *TransitionService) replayTerminalRunOnce(ctx context.Context, req ReplayTerminalRunRequest) ([]QueuedNodeAttempt, error) {
	if strings.TrimSpace(req.RunID) == "" {
		return nil, fmt.Errorf("run id is required")
	}
	if strings.TrimSpace(req.AnchorNodeID) == "" {
		return nil, fmt.Errorf("anchor node id is required")
	}
	if len(req.TargetNodeIDs) == 0 {
		return nil, fmt.Errorf("at least one replay target is required")
	}
	occurredAt := strings.TrimSpace(req.OccurredAt)
	if occurredAt == "" {
		occurredAt = time.Now().UTC().Format(time.RFC3339)
	}
	if strings.TrimSpace(req.RunReasonCode) == "" {
		return nil, fmt.Errorf("run reason code is required")
	}
	if strings.TrimSpace(req.RunEventType) == "" || strings.TrimSpace(req.NodeEventType) == "" {
		return nil, fmt.Errorf("run and node event types are required")
	}
	if strings.TrimSpace(req.EventPrefix) == "" {
		return nil, fmt.Errorf("event prefix is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin replay transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	runStatus, err := runStatusForQueuePendingWaveTx(ctx, tx, req.RunID)
	if err != nil {
		return nil, err
	}
	if runStatus != "failed" && runStatus != "cancelled" {
		return nil, &StateConflictError{EntityType: EntityTypeRun, ExpectedState: "failed or cancelled", ActualState: runStatus}
	}
	if err := validateTransition(EntityTypeRun, runStatus, "running"); err != nil {
		return nil, err
	}
	if runStatus == "cancelled" {
		blockers, err := cancelledRunReplayBlockersTx(ctx, tx, req.RunID)
		if err != nil {
			return nil, err
		}
		if len(blockers) > 0 {
			return nil, &ReplayNotQuiescentError{RunID: req.RunID, Blockers: blockers}
		}
	}

	reasonText := strings.TrimSpace(req.Reason)
	if reasonText == "" {
		reasonText = "manual replay requested"
	}

	runAttemptID := fmt.Sprintf("run:%s:%s:%s", req.RunID, req.AnchorNodeID, req.RunReasonCode)
	runEventSeq, err := nextEventSeqTx(ctx, tx, runAttemptID)
	if err != nil {
		return nil, err
	}
	runPayload := fmt.Sprintf(`{"run_id":%q,"node_id":%q,"reason":%q,"source_status":%q,"recorded_at":%q}`,
		req.RunID,
		req.AnchorNodeID,
		strings.TrimSpace(req.Reason),
		runStatus,
		occurredAt,
	)
	if err := s.applyTx(ctx, tx, TransitionRequest{
		TransitionID:    generateID(req.GenerateID, "transition"),
		EventID:         generateID(req.GenerateID, "evt"),
		RunID:           req.RunID,
		Entity:          EntityTypeRun,
		FromState:       runStatus,
		ToState:         "running",
		ActorID:         req.ActorID,
		RoleID:          req.RoleID,
		AuthoritySource: req.AuthoritySource,
		ReasonCode:      req.RunReasonCode,
		ReasonText:      reasonText,
		CorrelationID:   "corr_" + req.RunID + "_" + req.AnchorNodeID + "_" + req.RunReasonCode,
		CausationID:     "caus_" + req.RunID + "_" + req.AnchorNodeID + "_" + req.RunReasonCode,
		OccurredAt:      occurredAt,
		AttemptID:       runAttemptID,
		SchemaVersion:   "v1",
		EventType:       req.RunEventType,
		EventSeq:        runEventSeq,
		PayloadJSON:     runPayload,
	}); err != nil {
		return nil, err
	}

	queued := make([]QueuedNodeAttempt, 0, len(req.TargetNodeIDs))
	queuedWaves, err := s.resolveQueuedNodeWavesTx(ctx, tx, req.RunID, req.TargetNodeIDs)
	if err != nil {
		return nil, err
	}
	for idx, nodeID := range req.TargetNodeIDs {
		queuedAttempt, err := s.insertPendingNodeAttemptTx(ctx, tx, req.RunID, nodeID, queuedWaves[strings.TrimSpace(nodeID)], occurredAt, req.GenerateID)
		if err != nil {
			return nil, err
		}
		queued = append(queued, queuedAttempt)
		if idx != 0 {
			continue
		}

		payload := fmt.Sprintf(`{"reason":%q,"node_id":%q,"attempt":%d}`,
			strings.TrimSpace(req.Reason),
			nodeID,
			queuedAttempt.Attempt,
		)
		if err := s.applyTx(ctx, tx, TransitionRequest{
			TransitionID:    generateID(req.GenerateID, "transition"),
			EventID:         generateID(req.GenerateID, "evt"),
			RunID:           req.RunID,
			NodeRunID:       sql.NullString{String: queuedAttempt.NodeRunID, Valid: true},
			Entity:          EntityTypeNode,
			FromState:       "pending",
			ToState:         "ready",
			ActorID:         req.ActorID,
			RoleID:          req.RoleID,
			AuthoritySource: req.AuthoritySource,
			ReasonCode:      firstNonEmptyTrimmed(req.NodeReasonCode, "internal_error"),
			ReasonText:      reasonText,
			CorrelationID:   "corr_" + req.EventPrefix + "_" + req.RunID + "_" + nodeID,
			CausationID:     "caus_" + req.EventPrefix + "_" + req.RunID + "_" + nodeID,
			OccurredAt:      occurredAt,
			AttemptID:       fmt.Sprintf("%s:%s:%d", req.RunID, nodeID, queuedAttempt.Attempt),
			SchemaVersion:   "v1",
			EventType:       req.NodeEventType,
			EventSeq:        1,
			PayloadJSON:     payload,
		}); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit replay transaction: %w", err)
	}
	committed = true
	return queued, nil
}

func cancelledRunReplayBlockersTx(ctx context.Context, tx *sql.Tx, runID string) ([]string, error) {
	var pendingCancellation int
	var pendingCompletion int
	var nonterminalExecution int
	var activeLease int
	err := tx.QueryRowContext(ctx, `
SELECT EXISTS(SELECT 1 FROM attempt_cancel_outbox WHERE run_id = ? AND status = 'pending'),
       EXISTS(SELECT 1 FROM attempt_outbox WHERE run_id = ? AND status = 'pending'),
       EXISTS(SELECT 1 FROM attempt_executions WHERE run_id = ? AND state NOT IN ('committed_success', 'committed_failure', 'committed_cancelled')),
       EXISTS(
         SELECT 1
         FROM session_leases sl
         JOIN attempt_executions ae ON ae.session_ref = sl.session_id
         WHERE ae.run_id = ? AND sl.status = 'active'
       )`, runID, runID, runID, runID).Scan(&pendingCancellation, &pendingCompletion, &nonterminalExecution, &activeLease)
	if err != nil {
		return nil, fmt.Errorf("inspect cancelled run replay quiescence: %w", err)
	}
	blockers := make([]string, 0, 4)
	if pendingCancellation != 0 {
		blockers = append(blockers, "provider cancellation")
	}
	if pendingCompletion != 0 {
		blockers = append(blockers, "completion projection")
	}
	if nonterminalExecution != 0 {
		blockers = append(blockers, "attempt settlement")
	}
	if activeLease != 0 {
		blockers = append(blockers, "session lease")
	}
	return blockers, nil
}

func (s *TransitionService) RecordFinalApproval(ctx context.Context, req FinalApprovalRecordRequest) error {
	if strings.TrimSpace(req.ApprovalID) == "" {
		return fmt.Errorf("approval id is required")
	}
	decision := strings.TrimSpace(req.Decision)
	if decision == "" {
		return fmt.Errorf("approval decision is required")
	}
	var returnErr error
	err := s.applyWithEffects(ctx, req.Transition, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx,
			`INSERT INTO approvals (approval_id, run_id, decision, actor_id, role_id, reason_text, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			req.ApprovalID,
			req.Transition.RunID,
			decision,
			req.Transition.ActorID,
			req.Transition.RoleID,
			strings.TrimSpace(req.ReasonText),
			req.Transition.OccurredAt,
		)
		if err != nil {
			return fmt.Errorf("insert approval record: %w", err)
		}
		if req.FinalReview != nil {
			if _, err := insertRunFinalReview(ctx, tx, *req.FinalReview); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		returnErr = err
	}
	return returnErr
}

func (s *TransitionService) RecordFinalRework(ctx context.Context, req FinalReworkRecordRequest) error {
	if strings.TrimSpace(req.ApprovalID) == "" {
		return fmt.Errorf("approval id is required")
	}
	if len(req.TargetNodeIDs) == 0 {
		return fmt.Errorf("at least one replay target is required")
	}
	if err := validateFinalReworkRecordRequest(req); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin final rework transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if err := s.applyTx(ctx, tx, req.RunTransition); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO approvals (approval_id, run_id, decision, actor_id, role_id, reason_text, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		req.ApprovalID,
		req.RunTransition.RunID,
		"needs_rework",
		req.RunTransition.ActorID,
		req.RunTransition.RoleID,
		strings.TrimSpace(req.ApprovalReason),
		req.RunTransition.OccurredAt,
	); err != nil {
		return fmt.Errorf("insert approval record: %w", err)
	}
	if err := s.applyTx(ctx, tx, req.PlanTransition); err != nil {
		return err
	}

	queued, err := s.queuePendingNodeWaveTx(ctx, tx, req.RunTransition.RunID, req.TargetNodeIDs, nil, req.RunTransition.OccurredAt, req.GenerateID)
	if err != nil {
		return err
	}
	if len(queued) != len(req.TargetNodeIDs) {
		return &ReplayWaveActiveError{RunID: req.RunTransition.RunID, Scope: "final rework replay"}
	}
	if err := s.activateFirstQueuedAttemptTx(ctx, tx, req.RunTransition, queued[0], req.GenerateID, "attempt.rerun_queued"); err != nil {
		return err
	}

	finalReviewReq := req.FinalReview
	finalReviewReq.QueuedPlanAttempt, err = queuedAttemptNumberForNodeClassTx(ctx, tx, req.RunTransition.RunID, queued, "plan_drafting")
	if err != nil {
		return err
	}
	if finalReviewReq.QueuedPlanAttempt == nil {
		return fmt.Errorf("final rework requires queued plan_drafting attempt")
	}
	if _, err := insertRunFinalReview(ctx, tx, finalReviewReq); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit final rework transaction: %w", err)
	}
	committed = true
	return nil
}

func validateFinalReworkRecordRequest(req FinalReworkRecordRequest) error {
	runTransition := req.RunTransition
	planTransition := req.PlanTransition
	if runTransition.Entity != EntityTypeRun {
		return fmt.Errorf("final rework requires run transition entity")
	}
	if runTransition.FromState != "awaiting_human_confirmation" || runTransition.ToState != "running" {
		return fmt.Errorf("final rework requires awaiting_human_confirmation to running transition")
	}
	if planTransition.Entity != EntityTypePlan {
		return fmt.Errorf("final rework requires plan transition entity")
	}
	if planTransition.FromState != "approved" || planTransition.ToState != "needs_rework" {
		return fmt.Errorf("final rework requires approved to needs_rework plan transition")
	}
	if strings.TrimSpace(planTransition.RunID) != strings.TrimSpace(runTransition.RunID) {
		return fmt.Errorf("plan transition run id must match run transition run id")
	}
	if strings.TrimSpace(planTransition.ActorID) != strings.TrimSpace(runTransition.ActorID) {
		return fmt.Errorf("plan transition actor id must match run transition actor id")
	}
	if strings.TrimSpace(planTransition.RoleID) != strings.TrimSpace(runTransition.RoleID) {
		return fmt.Errorf("plan transition role id must match run transition role id")
	}
	if strings.TrimSpace(planTransition.OccurredAt) != strings.TrimSpace(runTransition.OccurredAt) {
		return fmt.Errorf("plan transition occurred_at must match run transition occurred_at")
	}
	if strings.TrimSpace(req.FinalReview.RunID) != strings.TrimSpace(runTransition.RunID) {
		return fmt.Errorf("final review run id must match run transition run id")
	}
	if strings.TrimSpace(req.FinalReview.ActorID) != strings.TrimSpace(runTransition.ActorID) {
		return fmt.Errorf("final review actor id must match run transition actor id")
	}
	if strings.TrimSpace(req.FinalReview.RoleID) != strings.TrimSpace(runTransition.RoleID) {
		return fmt.Errorf("final review role id must match run transition role id")
	}
	if strings.TrimSpace(req.FinalReview.CreatedAt) != strings.TrimSpace(runTransition.OccurredAt) {
		return fmt.Errorf("final review created_at must match run transition occurred_at")
	}
	return validateRunFinalReviewCreate(req.FinalReview)
}

func (s *TransitionService) activateFirstQueuedAttemptTx(ctx context.Context, tx *sql.Tx, transition TransitionRequest, firstQueued QueuedNodeAttempt, generate func(string) string, eventType string) error {
	payload, err := marshalQueuedNodeAttemptEventPayload(transition.RunID, firstQueued, "ready", map[string]any{
		"reason": strings.TrimSpace(transition.ReasonText),
	})
	if err != nil {
		return fmt.Errorf("marshal queued node attempt payload: %w", err)
	}
	err = s.applyTx(ctx, tx, TransitionRequest{
		TransitionID:    generateID(generate, "transition"),
		EventID:         generateID(generate, "evt"),
		RunID:           transition.RunID,
		NodeRunID:       sql.NullString{String: firstQueued.NodeRunID, Valid: true},
		Entity:          EntityTypeNode,
		FromState:       "pending",
		ToState:         "ready",
		ActorID:         transition.ActorID,
		RoleID:          transition.RoleID,
		AuthoritySource: transition.AuthoritySource,
		ReasonCode:      firstNonEmptyTrimmed(transition.ReasonCode, "internal_error"),
		ReasonText:      transition.ReasonText,
		CorrelationID:   "corr_" + transition.RunID + "_" + firstQueued.NodeID + "_reopen",
		CausationID:     "caus_" + transition.RunID + "_" + firstQueued.NodeID + "_reopen",
		OccurredAt:      transition.OccurredAt,
		AttemptID:       fmt.Sprintf("%s:%s:%d", transition.RunID, firstQueued.NodeID, firstQueued.Attempt),
		SchemaVersion:   "v1",
		EventType:       eventType,
		EventSeq:        1,
		PayloadJSON:     payload,
	})
	return err
}

func marshalQueuedNodeAttemptEventPayload(runID string, queued QueuedNodeAttempt, state string, extra map[string]any) (string, error) {
	payload := map[string]any{
		"run_id":         strings.TrimSpace(runID),
		"node_id":        strings.TrimSpace(queued.NodeID),
		"node_run_id":    strings.TrimSpace(queued.NodeRunID),
		"attempt_id":     composeAttemptID(runID, queued.NodeID, queued.Attempt),
		"attempt":        queued.Attempt,
		"agent_id":       strings.TrimSpace(queued.AgentID),
		"required_role":  strings.TrimSpace(queued.RequiredRole),
		"execution_mode": string(workflow.BehaviorForNode(queued.NodeID, "").ExecutionOwnership),
		"state":          strings.TrimSpace(state),
	}
	for key, value := range extra {
		payload[key] = value
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func (s *TransitionService) RecordPlanDecision(ctx context.Context, req PlanDecisionRecordRequest) (PlanDecisionRecordResult, error) {
	if err := validatePlanDecisionRecordRequest(req); err != nil {
		return PlanDecisionRecordResult{}, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PlanDecisionRecordResult{}, fmt.Errorf("begin plan decision transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	currentRevision, ok, err := NewRunPlanRevisionStore(s.db).refreshCurrentRunPlanRevisionTx(ctx, tx, req.Transition.RunID)
	if err != nil {
		return PlanDecisionRecordResult{}, err
	}
	if !ok {
		return PlanDecisionRecordResult{}, fmt.Errorf("current run plan revision is not available for %q", strings.TrimSpace(req.Transition.RunID))
	}
	if strings.TrimSpace(currentRevision.PlanRevisionID) != strings.TrimSpace(req.PlanReview.ReviewedPlanRevisionID) {
		return PlanDecisionRecordResult{}, fmt.Errorf("plan review must reference current run plan revision %q", strings.TrimSpace(currentRevision.PlanRevisionID))
	}

	if err := s.applyTx(ctx, tx, req.Transition); err != nil {
		return PlanDecisionRecordResult{}, err
	}

	planReviewReq := req.PlanReview
	result := PlanDecisionRecordResult{}
	if planReviewReq.Decision == RunPlanReviewDecisionNeedsRework {
		queuedAttempts, err := s.queuePlanReworkWaveTx(ctx, tx, req.Transition.RunID, req.Transition.OccurredAt, req.GenerateID)
		if err != nil {
			return PlanDecisionRecordResult{}, err
		}
		planReviewReq.QueuedAdvisoryAttempt, err = queuedAttemptNumberForNodeClassTx(ctx, tx, req.Transition.RunID, queuedAttempts, "advisory_synthesis")
		if err != nil {
			return PlanDecisionRecordResult{}, err
		}
		planReviewReq.QueuedPlanAttempt, err = queuedAttemptNumberForNodeClassTx(ctx, tx, req.Transition.RunID, queuedAttempts, "plan_drafting")
		if err != nil {
			return PlanDecisionRecordResult{}, err
		}
		result.QueuedAttempts = queuedAttempts
		result.Queued = len(queuedAttempts) > 0
	}

	review, err := insertRunPlanReview(ctx, tx, planReviewReq)
	if err != nil {
		return PlanDecisionRecordResult{}, err
	}
	result.Review = review

	if err := tx.Commit(); err != nil {
		return PlanDecisionRecordResult{}, fmt.Errorf("commit plan decision transaction: %w", err)
	}
	committed = true

	return result, nil
}

func validatePlanDecisionRecordRequest(req PlanDecisionRecordRequest) error {
	transition := req.Transition
	if transition.Entity != EntityTypePlan {
		return fmt.Errorf("plan decision requires plan transition entity")
	}
	if transition.FromState != "awaiting_king_decision" {
		return fmt.Errorf("plan decision requires awaiting_king_decision from_state")
	}
	decision := req.PlanReview.Decision
	if decision == RunPlanReviewDecisionApproved && transition.ToState != "approved" {
		return fmt.Errorf("approved plan review must target approved state")
	}
	if decision == RunPlanReviewDecisionNeedsRework && transition.ToState != "needs_rework" {
		return fmt.Errorf("needs_rework plan review must target needs_rework state")
	}
	if strings.TrimSpace(req.PlanReview.RunID) != strings.TrimSpace(transition.RunID) {
		return fmt.Errorf("plan review run id must match transition run id")
	}
	if strings.TrimSpace(req.PlanReview.ActorID) != strings.TrimSpace(transition.ActorID) {
		return fmt.Errorf("plan review actor id must match transition actor id")
	}
	if strings.TrimSpace(req.PlanReview.RoleID) != strings.TrimSpace(transition.RoleID) {
		return fmt.Errorf("plan review role id must match transition role id")
	}
	if strings.TrimSpace(req.PlanReview.CreatedAt) != strings.TrimSpace(transition.OccurredAt) {
		return fmt.Errorf("plan review created_at must match transition occurred_at")
	}
	return validateRunPlanReviewCreate(req.PlanReview)
}

func (s *TransitionService) RecordExecutionStart(ctx context.Context, req ExecutionStartRecordRequest) error {
	transition := req.Transition
	if err := validateExecutionStartTransition(transition); err != nil {
		return err
	}
	err := s.applyWithEffects(ctx, transition, func(ctx context.Context, tx *sql.Tx) error {
		switch transition.ToState {
		case "starting":
			if err := markAttemptExecutionStartingTx(ctx, tx, transition.AttemptID, transition.RunID, transition.NodeRunID.String, transition.OccurredAt); err != nil {
				return err
			}
		case "running":
			if err := markAttemptExecutionRunningTx(ctx, tx, transition.AttemptID, transition.RunID, transition.NodeRunID.String, transition.OccurredAt); err != nil {
				return err
			}
		default:
			return fmt.Errorf("execution start does not support to_state %q", transition.ToState)
		}
		return insertAttemptExecutionEventTx(ctx, tx, transition)
	})
	return err
}

func (s *TransitionService) RecordExecutionLaunch(ctx context.Context, req ExecutionLaunchRecordRequest) error {
	transition := req.Transition
	if err := validateExecutionLaunchTransition(transition); err != nil {
		return err
	}
	err := s.applyWithEffects(ctx, transition, func(ctx context.Context, tx *sql.Tx) error {
		if err := markAttemptExecutionRunningTx(ctx, tx, transition.AttemptID, transition.RunID, transition.NodeRunID.String, transition.OccurredAt); err != nil {
			return err
		}
		return insertAttemptExecutionEventTx(ctx, tx, transition)
	})
	return err
}

func (s *TransitionService) RecordDirectExecutionTerminal(ctx context.Context, req DirectExecutionTerminalRequest) error {
	transition := req.Transition
	if err := validateDirectExecutionTerminalTransition(transition); err != nil {
		return err
	}
	err := s.applyWithEffects(ctx, transition, func(ctx context.Context, tx *sql.Tx) error {
		if err := ensureAttemptExecutionRowForDirectTerminalTx(ctx, tx, transition.AttemptID, transition.RunID, transition.NodeRunID.String, transition.FromState, transition.OccurredAt); err != nil {
			return err
		}
		if err := ensureGenericRecoveryMayMutateAttemptTx(ctx, tx, transition.AttemptID, transition.RunID, transition.NodeRunID.String); err != nil {
			return err
		}
		if err := insertAttemptExecutionEventTx(ctx, tx, transition); err != nil {
			return err
		}
		return markAttemptExecutionTerminalTx(ctx, tx, transition.AttemptID, transition.RunID, transition.NodeRunID.String, transition.ToState, transition.ReasonCode, transition.ReasonText, transition.OccurredAt)
	})
	return err
}

func (s *TransitionService) RecordLoopCapAutoSuccess(ctx context.Context, req LoopCapAutoSuccessRecordRequest) error {
	transition := req.Transition
	if err := validateLoopCapAutoSuccessTransitionRequest(transition); err != nil {
		return err
	}
	participantNodeIDs, err := validateLoopCapAutoSuccessParticipantNodeIDs(req.ParticipantNodeIDs)
	if err != nil {
		return err
	}
	err = s.applyWithEffectsWithOptions(ctx, transition, func(ctx context.Context, tx *sql.Tx) error {
		if err := ensureAttemptExecutionRowForDirectTerminalTx(ctx, tx, transition.AttemptID, transition.RunID, transition.NodeRunID.String, transition.FromState, transition.OccurredAt); err != nil {
			return err
		}
		if err := insertAttemptExecutionEventTx(ctx, tx, transition); err != nil {
			return err
		}
		if err := markAttemptExecutionTerminalTx(ctx, tx, transition.AttemptID, transition.RunID, transition.NodeRunID.String, transition.ToState, transition.ReasonCode, transition.ReasonText, transition.OccurredAt); err != nil {
			return err
		}
		if req.CompletionOutbox == nil {
			return nil
		}
		return queueExecutionOutboxTx(ctx, tx, *req.CompletionOutbox)
	}, transitionApplyOptions{LoopCapParticipantNodeIDs: participantNodeIDs})
	return err
}

func (s *TransitionService) RecoverLoopCapAutoSuccessConflict(ctx context.Context, req LoopCapAutoSuccessRecordRequest) (bool, error) {
	transition := req.Transition
	if err := validateLoopCapAutoSuccessTransitionRequest(transition); err != nil {
		return false, err
	}
	if err := validateLoopCapAutoSuccessCompletionOutbox(req.CompletionOutbox, transition); err != nil {
		return false, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin loop cap auto-success conflict recovery transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	attempt, err := loadNodeAttemptForProjectionTx(ctx, tx, transition.RunID, transition.NodeRunID.String)
	if err != nil {
		return false, err
	}
	if expectedAttemptID := strings.TrimSpace(transition.AttemptID); expectedAttemptID != "" && expectedAttemptID != attempt.AttemptID {
		return false, fmt.Errorf("loop cap auto-success conflict attempt id %q does not match durable node attempt %q", expectedAttemptID, attempt.AttemptID)
	}

	outboxStatus, outboxFound, err := loadAttemptOutboxStatusTx(ctx, tx, req.CompletionOutbox.OutboxID)
	if err != nil {
		return false, err
	}

	switch strings.TrimSpace(attempt.NodeStatus) {
	case transition.ToState:
		attemptState, found, err := loadAttemptExecutionStateTx(ctx, tx, transition.AttemptID, transition.RunID, transition.NodeRunID.String)
		if err != nil {
			return false, err
		}
		if !found {
			return false, fmt.Errorf("loop cap auto-success conflict is missing durable attempt execution for %q", transition.AttemptID)
		}
		if attemptState != "committed_success" {
			return false, fmt.Errorf("loop cap auto-success conflict found durable attempt %q in unexpected state %q", transition.AttemptID, attemptState)
		}
		projectionRequired := false
		switch {
		case !outboxFound:
			if err := queueExecutionOutboxTx(ctx, tx, *req.CompletionOutbox); err != nil {
				return false, err
			}
			projectionRequired = true
		case outboxStatus == "pending":
			projectionRequired = true
		case outboxStatus == "published":
			projectionRequired = false
		default:
			return false, fmt.Errorf("loop cap auto-success conflict found outbox %q in unexpected status %q", req.CompletionOutbox.OutboxID, outboxStatus)
		}
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("commit loop cap auto-success conflict recovery transaction: %w", err)
		}
		committed = true
		return projectionRequired, nil
	case transition.FromState:
		return false, fmt.Errorf("loop cap auto-success conflict left node %q in ready state", transition.NodeRunID.String)
	default:
		if outboxFound {
			return false, fmt.Errorf("loop cap auto-success conflict found stale outbox %q while node %q is %q", req.CompletionOutbox.OutboxID, transition.NodeRunID.String, attempt.NodeStatus)
		}
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("commit loop cap auto-success conflict winner transaction: %w", err)
		}
		committed = true
		return false, nil
	}
}

func (s *TransitionService) ProjectNodeTerminalFromDurableAttempt(ctx context.Context, req DurableAttemptNodeProjectionRequest) (bool, error) {
	var projected bool
	err := retrySQLiteBusy(ctx, func() error {
		current, err := s.projectNodeTerminalFromDurableAttemptOnce(ctx, req)
		if err == nil {
			projected = current
		}
		return err
	})
	return projected, err
}

func (s *TransitionService) projectNodeTerminalFromDurableAttemptOnce(ctx context.Context, req DurableAttemptNodeProjectionRequest) (bool, error) {
	if strings.TrimSpace(req.RunID) == "" {
		return false, fmt.Errorf("run id is required")
	}
	if strings.TrimSpace(req.NodeRunID) == "" {
		return false, fmt.Errorf("node_run_id is required")
	}
	if strings.TrimSpace(req.OccurredAt) == "" {
		req.OccurredAt = time.Now().UTC().Format(time.RFC3339)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin durable attempt projection transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	attempt, err := loadNodeAttemptForProjectionTx(ctx, tx, req.RunID, req.NodeRunID)
	if err != nil {
		return false, err
	}
	if expectedAttemptID := strings.TrimSpace(req.AttemptID); expectedAttemptID != "" && expectedAttemptID != attempt.AttemptID {
		return false, fmt.Errorf("attempt id %q does not match node projection lineage %q", expectedAttemptID, attempt.AttemptID)
	}
	projectionTransition := TransitionRequest{
		RunID:      attempt.RunID,
		NodeRunID:  sql.NullString{String: attempt.NodeRunID, Valid: true},
		AttemptID:  attempt.AttemptID,
		OccurredAt: req.OccurredAt,
	}
	if err := validatePlanningOutputTransitionLineage(projectionTransition, req.PlanningOutput); err != nil {
		return false, err
	}
	var executionState string
	var failureCode string
	var failureText string
	err = tx.QueryRowContext(ctx,
		`SELECT state, COALESCE(failure_code, ''), COALESCE(failure_text, '')
		 FROM attempt_executions
		 WHERE attempt_id = ? AND run_id = ? AND node_run_id = ?`,
		attempt.AttemptID,
		attempt.RunID,
		attempt.NodeRunID,
	).Scan(&executionState, &failureCode, &failureText)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load durable attempt execution for node projection: %w", err)
	}

	projection, ok := durableAttemptNodeProjectionForState(
		executionState,
		failureCode,
		failureText,
		"durable execution settled as success",
		"durable execution settled as failure",
		"durable execution settled as cancelled",
	)
	if ok && req.PlanningOutput != nil {
		if err := persistRunPlanningOutputTx(ctx, tx, s.db, *req.PlanningOutput); err != nil {
			return false, err
		}
	}
	if !ok || strings.TrimSpace(attempt.NodeStatus) == projection.NodeState {
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("commit durable attempt projection no-op transaction: %w", err)
		}
		committed = true
		return false, nil
	}
	if err := releaseAgentForNodeTerminalTx(ctx, tx, attempt.AgentID, attempt.NodeRunID, attempt.NodeStatus, req.OccurredAt); err != nil {
		return false, err
	}
	eventSeq, err := nextEventSeqTx(ctx, tx, attempt.AttemptID)
	if err != nil {
		return false, err
	}
	payload := fmt.Sprintf(`{"run_id":%q,"node_id":%q,"node_run_id":%q,"state":%q,"durable_execution_state":%q}`,
		attempt.RunID,
		attempt.NodeID,
		attempt.NodeRunID,
		projection.NodeState,
		strings.TrimSpace(executionState),
	)
	transition := TransitionRequest{
		TransitionID:    generateID(req.GenerateID, "transition"),
		EventID:         generateID(req.GenerateID, "evt"),
		RunID:           attempt.RunID,
		NodeRunID:       sql.NullString{String: attempt.NodeRunID, Valid: true},
		Entity:          EntityTypeNode,
		FromState:       attempt.NodeStatus,
		ToState:         projection.NodeState,
		ActorID:         firstNonEmptyTrimmed(req.ActorID, "runtime_orchestrator"),
		RoleID:          attempt.RequiredRole,
		AuthoritySource: durableAttemptProjectionAuthoritySource,
		ReasonCode:      projection.ReasonCode,
		ReasonText:      projection.ReasonText,
		CorrelationID:   "corr_durable_attempt_projection_" + attempt.RunID,
		CausationID:     fmt.Sprintf("caus_durable_attempt_projection_%s_%s", attempt.RunID, attempt.NodeRunID),
		OccurredAt:      req.OccurredAt,
		AttemptID:       attempt.AttemptID,
		SchemaVersion:   "v1",
		EventType:       projection.EventType,
		EventSeq:        eventSeq,
		PayloadJSON:     payload,
	}
	if err := s.applyTx(ctx, tx, transition); err != nil {
		return false, err
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit durable attempt projection transaction: %w", err)
	}
	committed = true
	return true, nil
}

func (s *TransitionService) SupersedeQueuedAttemptsBeforeLaunch(ctx context.Context, req SupersedeQueuedAttemptsRequest) (SupersedeQueuedAttemptsResult, error) {
	result := SupersedeQueuedAttemptsResult{}
	if strings.TrimSpace(req.RunID) == "" {
		return result, fmt.Errorf("run id is required")
	}
	if strings.TrimSpace(req.NodeID) == "" {
		return result, fmt.Errorf("node id is required")
	}
	if req.KeepAttempt <= 0 {
		return result, fmt.Errorf("keep attempt must be >= 1")
	}
	occurredAt := strings.TrimSpace(req.OccurredAt)
	if occurredAt == "" {
		occurredAt = time.Now().UTC().Format(time.RFC3339)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return result, fmt.Errorf("begin supersede queued attempts transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	rows, err := tx.QueryContext(ctx,
		`SELECT nr.run_id,
		        nr.node_run_id,
		        nr.node_id,
		        nr.attempt,
		        nr.node_status,
		        COALESCE(nr.superseded_at, '') AS superseded_at,
		        COALESCE(ae.failure_code, '') AS failure_code,
		        nr.required_role,
		        COALESCE(nr.agent_id, '') AS agent_id,
		        COALESCE(ae.session_ref, '') AS session_ref,
		        COALESCE(ae.execution_mode, 'provider_backed') AS execution_mode,
		        COALESCE(ae.state, '') AS execution_state,
		        COALESCE(ae.launch_started_at, '') AS launch_started_at,
		        COALESCE(ae.started_at, '') AS started_at,
		        COALESCE(ae.provider_session_id, '') AS provider_session_id,
		        COALESCE(ae.active_provider_invocation_id, '') AS active_provider_invocation_id
		 FROM node_runs nr
		 LEFT JOIN attempt_executions ae ON ae.run_id = nr.run_id AND ae.node_run_id = nr.node_run_id
		 WHERE nr.run_id = ?
		   AND nr.node_id = ?
		   AND nr.attempt > ?
		 ORDER BY nr.attempt ASC, nr.node_run_id ASC`,
		strings.TrimSpace(req.RunID),
		strings.TrimSpace(req.NodeID),
		req.KeepAttempt,
	)
	if err != nil {
		return result, fmt.Errorf("list newer queued attempts to supersede: %w", err)
	}
	defer rows.Close()

	type supersededAttempt struct {
		attempt liveNodeAttempt
		stage   attemptSupersessionStage
	}
	attempts := make([]supersededAttempt, 0)
	blockedOutcome := SupersedeQueuedAttemptsOutcome("")
	for rows.Next() {
		var attempt liveNodeAttempt
		var executionMode string
		if err := rows.Scan(&attempt.RunID, &attempt.NodeRunID, &attempt.NodeID, &attempt.Attempt, &attempt.NodeStatus, &attempt.SupersededAt, &attempt.FailureCode, &attempt.RequiredRole, &attempt.AgentID, &attempt.SessionID, &executionMode, &attempt.ExecutionState, &attempt.LaunchStartedAt, &attempt.StartedAt, &attempt.ProviderSessionID, &attempt.ActiveProviderInvocationID); err != nil {
			return result, fmt.Errorf("scan newer queued attempt to supersede: %w", err)
		}
		attempt.ExecutionMode = executionstore.NormalizeExecutionMode(executionMode)
		attempt.AttemptID = composeAttemptID(attempt.RunID, attempt.NodeID, attempt.Attempt)
		decision := attemptSupersessionDecisionForAttempt(attempt)
		if decision.blockedOutcome != "" {
			if blockedOutcome == "" || decision.blockedOutcome == SupersedeQueuedAttemptsOutcomeBlockedByNewerTerminalAttempt {
				blockedOutcome = decision.blockedOutcome
			}
			continue
		}
		if decision.stage == attemptSupersessionStageNone {
			continue
		}
		attempts = append(attempts, supersededAttempt{attempt: attempt, stage: decision.stage})
	}
	if err := rows.Err(); err != nil {
		return result, fmt.Errorf("iterate newer queued attempts to supersede: %w", err)
	}
	if blockedOutcome != "" {
		result.Outcome = blockedOutcome
		if err := tx.Commit(); err != nil {
			return result, fmt.Errorf("commit supersede queued attempts blocked transaction: %w", err)
		}
		committed = true
		return result, nil
	}
	if len(attempts) == 0 {
		result.Outcome = SupersedeQueuedAttemptsOutcomeNoNewerAttempts
		if err := tx.Commit(); err != nil {
			return result, fmt.Errorf("commit supersede queued attempts empty transaction: %w", err)
		}
		committed = true
		return result, nil
	}

	for _, candidate := range attempts {
		attempt := candidate.attempt
		reasonCode := supersessionReasonCode(candidate.stage)
		reasonText := supersessionReasonText(candidate.stage)
		nodeState := supersededNodeState(candidate.stage)
		eventType := supersededNodeEventType(candidate.stage)
		if err := releaseAgentForNodeTerminalTx(ctx, tx, attempt.AgentID, attempt.NodeRunID, attempt.NodeStatus, occurredAt); err != nil {
			return result, err
		}
		if err := cancelAttemptExecutionForSupersessionTx(ctx, tx, attempt, req.KeepAttempt, occurredAt, reasonCode, reasonText); err != nil {
			return result, err
		}
		if err := queueAttemptCancelOutboxForReasonTx(ctx, tx, attempt, reasonCode, reasonText, occurredAt, req.GenerateID); err != nil {
			return result, err
		}
		if err := closeAttemptSessionLeaseTx(ctx, tx, attempt.SessionID, occurredAt); err != nil {
			return result, err
		}
		if err := markNodeAttemptSupersededTx(ctx, tx, attempt.NodeRunID, occurredAt); err != nil {
			return result, err
		}
		if err := s.applySupersededNodeTransitionTx(ctx, tx, attempt, req, occurredAt, nodeState, eventType, reasonCode, reasonText); err != nil {
			return result, err
		}
		result.CancelledNodeRun = append(result.CancelledNodeRun, attempt.NodeRunID)
	}

	if err := tx.Commit(); err != nil {
		return result, fmt.Errorf("commit supersede queued attempts transaction: %w", err)
	}
	committed = true
	result.Outcome = SupersedeQueuedAttemptsOutcomeAdopted
	return result, nil
}

func (s *TransitionService) TerminalizeRunAndCancelLiveNodes(ctx context.Context, req RunTerminalizationRequest) error {
	transition := req.Transition
	if transition.Entity != EntityTypeRun {
		return fmt.Errorf("run terminalization requires run transition entity")
	}
	if strings.TrimSpace(transition.RunID) == "" {
		return fmt.Errorf("run id is required")
	}
	if transition.ToState != "failed" && transition.ToState != "cancelled" {
		return fmt.Errorf("run terminalization requires failed or cancelled target state")
	}
	if strings.TrimSpace(transition.OccurredAt) == "" {
		transition.OccurredAt = time.Now().UTC().Format(time.RFC3339)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin run terminalization transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	liveAttempts, err := s.loadLiveNodeAttemptsTx(ctx, tx, transition.RunID)
	if err != nil {
		return err
	}
	for _, attempt := range liveAttempts {
		projected, err := s.projectLiveNodeAttemptFromDurableExecutionTx(ctx, tx, transition, attempt, req.GenerateID)
		if err != nil {
			return err
		}
		if projected {
			continue
		}
		if err := s.cancelLiveNodeAttemptTx(ctx, tx, transition, attempt, req.GenerateID); err != nil {
			return err
		}
		if err := cancelAttemptExecutionTx(ctx, tx, attempt, transition.ToState, transition.ReasonCode, transition.OccurredAt, req.GenerateID); err != nil {
			return err
		}
		if err := queueAttemptCancelOutboxTx(ctx, tx, attempt, transition, req.GenerateID); err != nil {
			return err
		}
	}
	if err := closeRunSessionLeasesTx(ctx, tx, transition.RunID, transition.OccurredAt); err != nil {
		return err
	}

	if err := s.applyTx(ctx, tx, transition); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit run terminalization transaction: %w", err)
	}
	committed = true
	return nil
}

func (s *TransitionService) ScheduleFencedStaleRetry(ctx context.Context, req ScheduleFencedStaleRetryRequest) (QueuedNodeAttempt, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		queued, err := s.scheduleFencedStaleRetryOnce(ctx, req)
		if err == nil {
			return queued, nil
		}
		if !IsUniqueViolation(err) {
			return QueuedNodeAttempt{}, err
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("stale retry handoff failed")
	}
	return QueuedNodeAttempt{}, lastErr
}

func (s *TransitionService) loadLiveNodeAttemptsTx(ctx context.Context, tx *sql.Tx, runID string) ([]liveNodeAttempt, error) {
	query := fmt.Sprintf(`SELECT nr.run_id,
	       nr.node_run_id,
	       nr.node_id,
	       nr.attempt,
	       nr.node_status,
	       COALESCE(nr.superseded_at, '') AS superseded_at,
	       COALESCE(ae.failure_code, '') AS failure_code,
	       nr.required_role,
	       COALESCE(nr.agent_id, '') AS agent_id,
	       COALESCE(ae.session_ref, '') AS session_ref,
	       COALESCE(ae.execution_mode, 'provider_backed') AS execution_mode,
	       COALESCE(ae.state, '') AS execution_state,
	       COALESCE(ae.launch_started_at, '') AS launch_started_at,
	       COALESCE(ae.started_at, '') AS started_at,
	       COALESCE(ae.provider_session_id, '') AS provider_session_id,
	       COALESCE(ae.active_provider_invocation_id, '') AS active_provider_invocation_id
	FROM node_runs nr
	LEFT JOIN attempt_executions ae ON ae.attempt_id = nr.run_id || ':' || nr.node_id || ':' || nr.attempt
	WHERE nr.run_id = ?
	  AND %s
	ORDER BY nr.node_id ASC, nr.attempt ASC, nr.node_run_id ASC`, LiveNodeAttemptStatusConditionSQL("nr"))
	rows, err := tx.QueryContext(ctx, query, runID)
	if err != nil {
		return nil, fmt.Errorf("load live node attempts: %w", err)
	}
	defer rows.Close()

	attempts := make([]liveNodeAttempt, 0)
	for rows.Next() {
		var attempt liveNodeAttempt
		var executionMode string
		if err := rows.Scan(&attempt.RunID, &attempt.NodeRunID, &attempt.NodeID, &attempt.Attempt, &attempt.NodeStatus, &attempt.SupersededAt, &attempt.FailureCode, &attempt.RequiredRole, &attempt.AgentID, &attempt.SessionID, &executionMode, &attempt.ExecutionState, &attempt.LaunchStartedAt, &attempt.StartedAt, &attempt.ProviderSessionID, &attempt.ActiveProviderInvocationID); err != nil {
			return nil, fmt.Errorf("scan live node attempt: %w", err)
		}
		attempt.ExecutionMode = executionstore.NormalizeExecutionMode(executionMode)
		attempt.AttemptID = composeAttemptID(attempt.RunID, attempt.NodeID, attempt.Attempt)
		attempts = append(attempts, attempt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate live node attempts: %w", err)
	}
	return attempts, nil
}

func liveNodeAttemptExistsTx(ctx context.Context, tx *sql.Tx, runID string, nodeIDs []string) (bool, error) {
	if len(nodeIDs) == 0 {
		return false, nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(nodeIDs)), ",")
	args := make([]any, 0, len(nodeIDs)+1)
	args = append(args, runID)
	for _, nodeID := range nodeIDs {
		args = append(args, nodeID)
	}
	query := fmt.Sprintf(`SELECT 1
	FROM node_runs
	WHERE run_id = ?
	  AND node_id IN (%s)
	  AND %s
	LIMIT 1`, placeholders, LiveNodeAttemptStatusConditionSQL("node_runs"))
	var marker int
	err := tx.QueryRowContext(ctx, query, args...).Scan(&marker)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check live target node attempts: %w", err)
	}
	return true, nil
}

func (s *TransitionService) projectLiveNodeAttemptFromDurableExecutionTx(ctx context.Context, tx *sql.Tx, runTransition TransitionRequest, attempt liveNodeAttempt, generate func(string) string) (bool, error) {
	var executionState string
	var failureCode string
	var failureText string
	err := tx.QueryRowContext(ctx,
		`SELECT state, COALESCE(failure_code, ''), COALESCE(failure_text, '')
		 FROM attempt_executions
		 WHERE attempt_id = ? AND run_id = ? AND node_run_id = ?`,
		attempt.AttemptID,
		attempt.RunID,
		attempt.NodeRunID,
	).Scan(&executionState, &failureCode, &failureText)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load durable attempt execution for run terminalization: %w", err)
	}
	trimmedState := strings.TrimSpace(executionState)
	if !isDurablyTerminalAttemptExecutionState(trimmedState) {
		return false, nil
	}
	if err := releaseAgentForNodeTerminalTx(ctx, tx, attempt.AgentID, attempt.NodeRunID, attempt.NodeStatus, runTransition.OccurredAt); err != nil {
		return false, err
	}
	projection, ok := durableAttemptNodeProjectionForState(
		trimmedState,
		failureCode,
		failureText,
		fmt.Sprintf("durable attempt already succeeded before run moved to %s", runTransition.ToState),
		fmt.Sprintf("durable attempt already failed before run moved to %s", runTransition.ToState),
		fmt.Sprintf("durable attempt already cancelled before run moved to %s", runTransition.ToState),
	)
	if !ok {
		return false, nil
	}

	eventSeq, err := nextEventSeqTx(ctx, tx, attempt.AttemptID)
	if err != nil {
		return false, err
	}
	payload := fmt.Sprintf(`{"run_id":%q,"node_id":%q,"node_run_id":%q,"state":%q,"run_state":%q,"parent_reason_code":%q,"durable_execution_state":%q}`,
		runTransition.RunID,
		attempt.NodeID,
		attempt.NodeRunID,
		projection.NodeState,
		runTransition.ToState,
		strings.TrimSpace(runTransition.ReasonCode),
		trimmedState,
	)
	err = s.applyTx(ctx, tx, TransitionRequest{
		TransitionID:    generateID(generate, "transition"),
		EventID:         generateID(generate, "evt"),
		RunID:           runTransition.RunID,
		NodeRunID:       sql.NullString{String: attempt.NodeRunID, Valid: true},
		Entity:          EntityTypeNode,
		FromState:       attempt.NodeStatus,
		ToState:         projection.NodeState,
		ActorID:         runTransition.ActorID,
		RoleID:          attempt.RequiredRole,
		AuthoritySource: runTerminalizationAuthoritySource,
		ReasonCode:      projection.ReasonCode,
		ReasonText:      projection.ReasonText,
		CorrelationID:   firstNonEmptyTrimmed(runTransition.CorrelationID, "corr_terminalize_"+runTransition.RunID),
		CausationID:     fmt.Sprintf("caus_terminalize_%s_%s", runTransition.RunID, attempt.NodeRunID),
		OccurredAt:      runTransition.OccurredAt,
		AttemptID:       attempt.AttemptID,
		SchemaVersion:   firstNonEmptyTrimmed(runTransition.SchemaVersion, "v1"),
		EventType:       projection.EventType,
		EventSeq:        eventSeq,
		PayloadJSON:     payload,
	})
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *TransitionService) cancelLiveNodeAttemptTx(ctx context.Context, tx *sql.Tx, runTransition TransitionRequest, attempt liveNodeAttempt, generate func(string) string) error {
	if err := releaseAgentForNodeTerminalTx(ctx, tx, attempt.AgentID, attempt.NodeRunID, attempt.NodeStatus, runTransition.OccurredAt); err != nil {
		return err
	}

	eventSeq, err := nextEventSeqTx(ctx, tx, attempt.AttemptID)
	if err != nil {
		return err
	}
	payload := fmt.Sprintf(`{"run_id":%q,"node_id":%q,"node_run_id":%q,"state":%q,"run_state":%q,"parent_reason_code":%q}`,
		runTransition.RunID,
		attempt.NodeID,
		attempt.NodeRunID,
		"cancelled",
		runTransition.ToState,
		strings.TrimSpace(runTransition.ReasonCode),
	)
	err = s.applyTx(ctx, tx, TransitionRequest{
		TransitionID:    generateID(generate, "transition"),
		EventID:         generateID(generate, "evt"),
		RunID:           runTransition.RunID,
		NodeRunID:       sql.NullString{String: attempt.NodeRunID, Valid: true},
		Entity:          EntityTypeNode,
		FromState:       attempt.NodeStatus,
		ToState:         "cancelled",
		ActorID:         runTransition.ActorID,
		RoleID:          attempt.RequiredRole,
		AuthoritySource: runTerminalizationAuthoritySource,
		ReasonCode:      "run_terminalized",
		ReasonText:      fmt.Sprintf("run moved to %s; live child attempt quiesced", runTransition.ToState),
		CorrelationID:   firstNonEmptyTrimmed(runTransition.CorrelationID, "corr_terminalize_"+runTransition.RunID),
		CausationID:     fmt.Sprintf("caus_terminalize_%s_%s", runTransition.RunID, attempt.NodeRunID),
		OccurredAt:      runTransition.OccurredAt,
		AttemptID:       attempt.AttemptID,
		SchemaVersion:   firstNonEmptyTrimmed(runTransition.SchemaVersion, "v1"),
		EventType:       "attempt.cancelled",
		EventSeq:        eventSeq,
		PayloadJSON:     payload,
	})
	return err
}

func releaseAgentForNodeTerminalTx(ctx context.Context, tx *sql.Tx, agentID, nodeRunID, nodeStatus, updatedAt string) error {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return nil
	}
	switch strings.TrimSpace(nodeStatus) {
	case "pending", "ready", "starting", "running", "blocked":
	default:
		return nil
	}
	if err := syncAgentOperationalStatusForReleasedNodeTx(ctx, tx, agentID, nodeRunID, updatedAt); err != nil {
		return fmt.Errorf("release agent for run terminalization: %w", err)
	}
	return nil
}

func syncAgentOperationalStatusForReleasedNodeTx(ctx context.Context, tx *sql.Tx, agentID, releasedNodeRunID, updatedAt string) error {
	trimmedAgentID := strings.TrimSpace(agentID)
	if trimmedAgentID == "" {
		return nil
	}
	desiredStatus, err := projectedAgentStatusFromLiveNodeRunsWithQuerierExcludingNode(ctx, tx, trimmedAgentID, releasedNodeRunID)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE agents
		 SET status = ?,
		     updated_at = ?
		 WHERE agent_id = ?
		   AND status NOT IN ('offline', 'failed')`,
		desiredStatus,
		updatedAt,
		trimmedAgentID,
	); err != nil {
		return fmt.Errorf("sync agent operational status for %q: %w", trimmedAgentID, err)
	}
	return nil
}

func cancelAttemptExecutionForSupersessionTx(ctx context.Context, tx *sql.Tx, attempt liveNodeAttempt, keepAttempt int, occurredAt, reasonCode, reasonText string) error {
	var state string
	err := tx.QueryRowContext(ctx,
		`SELECT state
		 FROM attempt_executions
		 WHERE attempt_id = ? AND run_id = ? AND node_run_id = ?`,
		attempt.AttemptID,
		attempt.RunID,
		attempt.NodeRunID,
	).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("load attempt execution for supersession: %w", err)
	}
	trimmedState := strings.TrimSpace(state)
	if isDurablyTerminalAttemptExecutionState(trimmedState) {
		return nil
	}
	seq, err := nextAttemptExecutionSeqTx(ctx, tx, attempt.AttemptID)
	if err != nil {
		return err
	}
	payload := fmt.Sprintf(`{"node_run_id":%q,"node_id":%q,"superseded_by_attempt":%d}`,
		attempt.NodeRunID,
		attempt.NodeID,
		keepAttempt,
	)
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO attempt_event_log (attempt_event_id, attempt_id, run_id, node_run_id, seq, event_type, reason_code, payload_json, created_at)
		 VALUES (?, ?, ?, ?, ?, 'attempt.cancelled', ?, ?, ?)`,
		fmt.Sprintf("attempt_event:%s:%d", attempt.AttemptID, seq),
		attempt.AttemptID,
		attempt.RunID,
		attempt.NodeRunID,
		seq,
		reasonCode,
		payload,
		occurredAt,
	); err != nil {
		return fmt.Errorf("insert superseded attempt execution event: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE attempt_executions
		 SET state = 'committed_cancelled',
		     terminal_observed_at = ?,
		     committed_at = ?,
		     lease_owner = '',
		     lease_expires_at = NULL,
		     failure_code = ?,
		     failure_text = ?,
		     updated_at = ?
		 WHERE attempt_id = ?
		   AND run_id = ?
		   AND node_run_id = ?
		   AND state NOT IN ('committed_success', 'committed_failure', 'committed_cancelled')`,
		occurredAt,
		occurredAt,
		reasonCode,
		reasonText,
		occurredAt,
		attempt.AttemptID,
		attempt.RunID,
		attempt.NodeRunID,
	); err != nil {
		return fmt.Errorf("mark attempt execution superseded: %w", err)
	}
	return nil
}

func (s *TransitionService) applySupersededNodeTransitionTx(ctx context.Context, tx *sql.Tx, attempt liveNodeAttempt, req SupersedeQueuedAttemptsRequest, occurredAt, toState, eventType, reasonCode, reasonText string) error {
	eventSeq, err := nextEventSeqTx(ctx, tx, attempt.AttemptID)
	if err != nil {
		return err
	}
	payload := fmt.Sprintf(`{"run_id":%q,"node_id":%q,"node_run_id":%q,"state":%q,"superseded_by_attempt":%d}`,
		attempt.RunID,
		attempt.NodeID,
		attempt.NodeRunID,
		toState,
		req.KeepAttempt,
	)
	return s.applyTx(ctx, tx, TransitionRequest{
		TransitionID:    generateID(req.GenerateID, "transition"),
		EventID:         generateID(req.GenerateID, "evt"),
		RunID:           attempt.RunID,
		NodeRunID:       sql.NullString{String: attempt.NodeRunID, Valid: true},
		Entity:          EntityTypeNode,
		FromState:       attempt.NodeStatus,
		ToState:         toState,
		ActorID:         firstNonEmptyTrimmed(req.ActorID, "runtime_orchestrator"),
		RoleID:          attempt.RequiredRole,
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      reasonCode,
		ReasonText:      reasonText,
		CorrelationID:   "corr_supersede_" + attempt.RunID,
		CausationID:     fmt.Sprintf("caus_supersede_%s_%s", attempt.RunID, attempt.NodeRunID),
		OccurredAt:      occurredAt,
		AttemptID:       attempt.AttemptID,
		SchemaVersion:   "v1",
		EventType:       eventType,
		EventSeq:        eventSeq,
		PayloadJSON:     payload,
	})
}

func durableAttemptNodeProjectionForState(executionState, failureCode, failureText, successFallback, failureFallback, cancelFallback string) (durableAttemptNodeProjection, bool) {
	switch strings.TrimSpace(executionState) {
	case "committed_success":
		return durableAttemptNodeProjection{
			NodeState:  "succeeded",
			EventType:  "attempt.succeeded",
			ReasonCode: "execution_completed",
			ReasonText: firstNonEmptyTrimmed(failureText, successFallback),
		}, true
	case "committed_failure":
		return durableAttemptNodeProjection{
			NodeState:  "failed",
			EventType:  "attempt.failed",
			ReasonCode: firstNonEmptyTrimmed(failureCode, "internal_error"),
			ReasonText: firstNonEmptyTrimmed(failureText, failureFallback),
		}, true
	case "committed_cancelled":
		return durableAttemptNodeProjection{
			NodeState:  "cancelled",
			EventType:  "attempt.cancelled",
			ReasonCode: firstNonEmptyTrimmed(failureCode, "run_terminalized"),
			ReasonText: firstNonEmptyTrimmed(failureText, cancelFallback),
		}, true
	default:
		return durableAttemptNodeProjection{}, false
	}
}

type durableAttemptNodeProjection struct {
	NodeState  string
	EventType  string
	ReasonCode string
	ReasonText string
}

func loadNodeAttemptForProjectionTx(ctx context.Context, tx *sql.Tx, runID, nodeRunID string) (liveNodeAttempt, error) {
	var attempt liveNodeAttempt
	var executionMode string
	err := tx.QueryRowContext(ctx,
		`SELECT run_id, node_run_id, node_id, attempt, node_status, required_role, COALESCE(agent_id, ''), '', '', '', '', '', '', ''
		 FROM node_runs
		 WHERE run_id = ? AND node_run_id = ?`,
		strings.TrimSpace(runID),
		strings.TrimSpace(nodeRunID),
	).Scan(&attempt.RunID, &attempt.NodeRunID, &attempt.NodeID, &attempt.Attempt, &attempt.NodeStatus, &attempt.RequiredRole, &attempt.AgentID, &attempt.SessionID, &executionMode, &attempt.ExecutionState, &attempt.LaunchStartedAt, &attempt.StartedAt, &attempt.ProviderSessionID, &attempt.ActiveProviderInvocationID)
	if errors.Is(err, sql.ErrNoRows) {
		return liveNodeAttempt{}, &EntityNotFoundError{EntityType: EntityTypeNode, RunID: strings.TrimSpace(runID), NodeRunID: strings.TrimSpace(nodeRunID)}
	}
	if err != nil {
		return liveNodeAttempt{}, fmt.Errorf("load node attempt for durable projection: %w", err)
	}
	attempt.ExecutionMode = executionstore.NormalizeExecutionMode(executionMode)
	attempt.AttemptID = composeAttemptID(attempt.RunID, attempt.NodeID, attempt.Attempt)
	return attempt, nil
}

func cancelAttemptExecutionTx(ctx context.Context, tx *sql.Tx, attempt liveNodeAttempt, runTerminalState, parentReasonCode, occurredAt string, generate func(string) string) error {
	var state string
	err := tx.QueryRowContext(ctx,
		`SELECT state
		 FROM attempt_executions
		 WHERE attempt_id = ? AND run_id = ? AND node_run_id = ?`,
		attempt.AttemptID,
		attempt.RunID,
		attempt.NodeRunID,
	).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("load attempt execution for terminalization: %w", err)
	}
	trimmedState := strings.TrimSpace(state)
	if trimmedState == "terminal_observed" || isDurablyTerminalAttemptExecutionState(trimmedState) {
		return nil
	}
	if attempt.ExecutionMode == executionstore.ExecutionModeProviderBacked && strings.TrimSpace(attempt.ActiveProviderInvocationID) != "" {
		return nil
	}

	seq, err := nextAttemptExecutionSeqTx(ctx, tx, attempt.AttemptID)
	if err != nil {
		return err
	}
	payload := fmt.Sprintf(`{"run_state":%q,"node_run_id":%q,"node_id":%q,"parent_reason_code":%q}`,
		runTerminalState,
		attempt.NodeRunID,
		attempt.NodeID,
		strings.TrimSpace(parentReasonCode),
	)
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO attempt_event_log (attempt_event_id, attempt_id, run_id, node_run_id, seq, event_type, reason_code, payload_json, created_at)
		 VALUES (?, ?, ?, ?, ?, 'attempt.cancelled', 'run_terminalized', ?, ?)`,
		generateID(generate, "attempt_event"),
		attempt.AttemptID,
		attempt.RunID,
		attempt.NodeRunID,
		seq,
		payload,
		occurredAt,
	); err != nil {
		return fmt.Errorf("insert cancelled attempt event: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE attempt_executions
		 SET state = 'committed_cancelled',
		     committed_at = ?,
		     lease_owner = '',
		     lease_expires_at = NULL,
		     failure_code = 'run_terminalized',
		     failure_text = ?,
		     updated_at = ?
		 WHERE attempt_id = ?
		   AND state NOT IN ('committed_success', 'committed_failure', 'committed_cancelled')`,
		occurredAt,
		fmt.Sprintf("run transitioned to %s before execution completion", runTerminalState),
		occurredAt,
		attempt.AttemptID,
	); err != nil {
		return fmt.Errorf("mark attempt execution cancelled: %w", err)
	}
	return nil
}

func queueAttemptCancelOutboxTx(ctx context.Context, tx *sql.Tx, attempt liveNodeAttempt, runTransition TransitionRequest, generate func(string) string) error {
	reasonCode := firstNonEmptyTrimmed(runTransition.ReasonCode, "run_terminalized")
	reasonText := strings.TrimSpace(runTransition.ReasonText)
	if reasonText == "" {
		reasonText = fmt.Sprintf("run moved to %s", strings.TrimSpace(runTransition.ToState))
	}
	return queueAttemptCancelOutboxForReasonTx(ctx, tx, attempt, reasonCode, reasonText, runTransition.OccurredAt, generate)
}

func queueAttemptCancelOutboxForReasonTx(ctx context.Context, tx *sql.Tx, attempt liveNodeAttempt, reasonCode, reasonText, occurredAt string, generate func(string) string) error {
	if attempt.ExecutionMode != executionstore.ExecutionModeProviderBacked {
		return nil
	}
	if strings.TrimSpace(attempt.SessionID) == "" {
		return nil
	}
	if !attemptCancelDispatchRequired(attempt) {
		return nil
	}
	reasonCode = firstNonEmptyTrimmed(reasonCode, "run_terminalized")
	reasonText = strings.TrimSpace(reasonText)
	if reasonText == "" {
		reasonText = reasonCode
	}
	occurredAt = firstNonEmptyTrimmed(occurredAt, time.Now().UTC().Format(time.RFC3339))
	_, err := tx.ExecContext(ctx,
		`INSERT OR IGNORE INTO attempt_cancel_outbox (cancel_outbox_id, attempt_id, run_id, node_run_id, session_id, reason_code, reason_text, requested_at, status, publish_attempts, last_error, created_at, updated_at, published_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, '', ?, ?, NULL)`,
		generateID(generate, "cancel_outbox"),
		attempt.AttemptID,
		attempt.RunID,
		attempt.NodeRunID,
		strings.TrimSpace(attempt.SessionID),
		reasonCode,
		reasonText,
		occurredAt,
		occurredAt,
		occurredAt,
	)
	if err != nil {
		return fmt.Errorf("queue attempt cancel outbox: %w", err)
	}
	return nil
}

func attemptSupersessionDecisionForAttempt(attempt liveNodeAttempt) attemptSupersessionDecision {
	trimmedNodeStatus := strings.TrimSpace(attempt.NodeStatus)
	trimmedExecutionState := strings.TrimSpace(attempt.ExecutionState)
	alreadySuperseded := attemptMarkedSuperseded(attempt)
	if isDurablyTerminalAttemptExecutionState(trimmedExecutionState) || trimmedExecutionState == "terminal_observed" {
		if !alreadySuperseded {
			return attemptSupersessionDecision{blockedOutcome: SupersedeQueuedAttemptsOutcomeBlockedByNewerTerminalAttempt}
		}
		return attemptSupersessionDecision{}
	}
	switch trimmedNodeStatus {
	case "succeeded", "failed", "cancelled", "skipped":
		if !alreadySuperseded {
			return attemptSupersessionDecision{blockedOutcome: SupersedeQueuedAttemptsOutcomeBlockedByNewerTerminalAttempt}
		}
		return attemptSupersessionDecision{}
	case "pending", "ready", "starting":
		if attemptCancelDispatchRequired(attempt) {
			return attemptSupersessionDecision{stage: attemptSupersessionStageLaunched}
		}
		if trimmedExecutionState == "" || trimmedExecutionState == "queued" || executionstore.AttemptInPreLaunch(trimmedExecutionState, attempt.StartedAt) {
			return attemptSupersessionDecision{stage: attemptSupersessionStagePreLaunch}
		}
		return attemptSupersessionDecision{blockedOutcome: SupersedeQueuedAttemptsOutcomeBlockedByNewerLiveAttempt}
	default:
		return attemptSupersessionDecision{blockedOutcome: SupersedeQueuedAttemptsOutcomeBlockedByNewerLiveAttempt}
	}
}

func attemptMarkedSuperseded(attempt liveNodeAttempt) bool {
	if strings.TrimSpace(attempt.SupersededAt) != "" {
		return true
	}
	switch strings.TrimSpace(attempt.FailureCode) {
	case supersessionReasonCode(attemptSupersessionStagePreLaunch), supersessionReasonCode(attemptSupersessionStageLaunched):
		return true
	default:
		return false
	}
}

func supersessionReasonCode(stage attemptSupersessionStage) string {
	if stage == attemptSupersessionStageLaunched {
		return "superseded_after_launch_handoff"
	}
	return "superseded_before_launch"
}

func supersessionReasonText(stage attemptSupersessionStage) string {
	if stage == attemptSupersessionStageLaunched {
		return "retry attempt superseded after launch handoff by an earlier successful attempt"
	}
	return "retry attempt superseded before launch by an earlier successful attempt"
}

func supersededNodeState(stage attemptSupersessionStage) string {
	if stage == attemptSupersessionStageLaunched {
		return "blocked"
	}
	return "cancelled"
}

func supersededNodeEventType(stage attemptSupersessionStage) string {
	if stage == attemptSupersessionStageLaunched {
		return "node.state_changed"
	}
	return "attempt.cancelled"
}

func attemptCancelDispatchRequired(attempt liveNodeAttempt) bool {
	if strings.TrimSpace(attempt.ExecutionState) == "terminal_observed" || isDurablyTerminalAttemptExecutionState(attempt.ExecutionState) {
		return false
	}
	return executionstore.AttemptReachedLaunchHandoff(
		attempt.ExecutionState,
		attempt.LaunchStartedAt,
		attempt.StartedAt,
		attempt.ProviderSessionID,
		attempt.ActiveProviderInvocationID,
	)
}

func isDurablyTerminalAttemptExecutionState(state string) bool {
	switch strings.TrimSpace(state) {
	case "committed_success", "committed_failure", "committed_cancelled":
		return true
	default:
		return false
	}
}

func queueExecutionOutboxTx(ctx context.Context, tx *sql.Tx, req executionstore.QueueOutboxRequest) error {
	outboxID := strings.TrimSpace(req.OutboxID)
	attemptID := strings.TrimSpace(req.AttemptID)
	runID := strings.TrimSpace(req.RunID)
	nodeRunID := strings.TrimSpace(req.NodeRunID)
	terminalState := strings.TrimSpace(req.TerminalState)
	payload := strings.TrimSpace(req.PayloadJSON)
	createdAt := strings.TrimSpace(req.CreatedAt)
	if outboxID == "" || attemptID == "" || runID == "" || nodeRunID == "" {
		return fmt.Errorf("outbox_id, attempt_id, run_id, and node_run_id are required")
	}
	if req.TerminalSeq <= 0 {
		return fmt.Errorf("terminal_seq must be >= 1")
	}
	if terminalState == "" {
		terminalState = "committed_success"
	}
	if payload == "" {
		return fmt.Errorf("payload_json is required")
	}
	if createdAt == "" {
		createdAt = time.Now().UTC().Format(time.RFC3339)
	}
	_, err := tx.ExecContext(ctx,
		`INSERT INTO attempt_outbox (outbox_id, attempt_id, run_id, node_run_id, terminal_seq, terminal_state, payload_json, status, publish_attempts, last_error, created_at, updated_at, published_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', 0, '', ?, ?, NULL)
		 ON CONFLICT(outbox_id) DO UPDATE SET
		   terminal_state = excluded.terminal_state,
		   payload_json = excluded.payload_json,
		   updated_at = excluded.updated_at
		 WHERE attempt_outbox.status = 'pending'`,
		outboxID,
		attemptID,
		runID,
		nodeRunID,
		req.TerminalSeq,
		terminalState,
		payload,
		createdAt,
		createdAt,
	)
	if err != nil {
		return fmt.Errorf("queue execution outbox %q: %w", outboxID, err)
	}
	return nil
}

func loadAttemptExecutionStateTx(ctx context.Context, tx *sql.Tx, attemptID, runID, nodeRunID string) (string, bool, error) {
	var state string
	err := tx.QueryRowContext(ctx,
		`SELECT state
		 FROM attempt_executions
		 WHERE attempt_id = ? AND run_id = ? AND node_run_id = ?`,
		strings.TrimSpace(attemptID),
		strings.TrimSpace(runID),
		strings.TrimSpace(nodeRunID),
	).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("load attempt execution state for %q: %w", attemptID, err)
	}
	return strings.TrimSpace(state), true, nil
}

func loadAttemptOutboxStatusTx(ctx context.Context, tx *sql.Tx, outboxID string) (string, bool, error) {
	var status string
	err := tx.QueryRowContext(ctx, `SELECT status FROM attempt_outbox WHERE outbox_id = ?`, strings.TrimSpace(outboxID)).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("load attempt outbox %q status: %w", outboxID, err)
	}
	return strings.TrimSpace(status), true, nil
}

func validateLoopCapAutoSuccessCompletionOutbox(outbox *executionstore.QueueOutboxRequest, transition TransitionRequest) error {
	if outbox == nil {
		return fmt.Errorf("loop cap auto-success conflict recovery requires completion outbox")
	}
	if strings.TrimSpace(outbox.OutboxID) == "" {
		return fmt.Errorf("loop cap auto-success conflict recovery requires outbox_id")
	}
	if strings.TrimSpace(outbox.AttemptID) != strings.TrimSpace(transition.AttemptID) {
		return fmt.Errorf("loop cap auto-success conflict recovery outbox attempt id must match transition attempt id")
	}
	if strings.TrimSpace(outbox.RunID) != strings.TrimSpace(transition.RunID) {
		return fmt.Errorf("loop cap auto-success conflict recovery outbox run id must match transition run id")
	}
	if strings.TrimSpace(outbox.NodeRunID) != strings.TrimSpace(transition.NodeRunID.String) {
		return fmt.Errorf("loop cap auto-success conflict recovery outbox node_run_id must match transition node_run_id")
	}
	return nil
}

func validateLoopCapAutoSuccessParticipantNodeIDs(nodeIDs []string) ([]string, error) {
	normalized := normalizeLoopCapParticipantNodeIDs(nodeIDs)
	if len(normalized) == 0 {
		return nil, fmt.Errorf("loop cap auto-success participant node ids are required")
	}
	return normalized, nil
}

func validateExecutionStartTransition(req TransitionRequest) error {
	if req.Entity != EntityTypeNode {
		return fmt.Errorf("execution start requires node transition")
	}
	if !req.NodeRunID.Valid || strings.TrimSpace(req.NodeRunID.String) == "" {
		return fmt.Errorf("execution start requires node_run_id")
	}
	if strings.TrimSpace(req.AttemptID) == "" {
		return fmt.Errorf("execution start requires attempt_id")
	}
	if req.FromState != "ready" {
		return fmt.Errorf("execution start requires ready from_state")
	}
	if req.ToState != "starting" && req.ToState != "running" {
		return fmt.Errorf("execution start requires ready -> starting or ready -> running transition")
	}
	switch req.ToState {
	case "starting":
		if strings.TrimSpace(req.EventType) != "node.state_changed" {
			return fmt.Errorf("execution start requires node.state_changed event_type for ready -> starting")
		}
	case "running":
		if strings.TrimSpace(req.EventType) != "attempt.started" {
			return fmt.Errorf("execution start requires attempt.started event_type for ready -> running")
		}
	}
	if req.EventSeq <= 0 {
		return fmt.Errorf("execution start requires positive event sequence")
	}
	return nil
}

func validateExecutionLaunchTransition(req TransitionRequest) error {
	if req.Entity != EntityTypeNode {
		return fmt.Errorf("execution launch requires node transition")
	}
	if !req.NodeRunID.Valid || strings.TrimSpace(req.NodeRunID.String) == "" {
		return fmt.Errorf("execution launch requires node_run_id")
	}
	if strings.TrimSpace(req.AttemptID) == "" {
		return fmt.Errorf("execution launch requires attempt_id")
	}
	if req.FromState != "starting" || req.ToState != "running" {
		return fmt.Errorf("execution launch requires starting -> running transition")
	}
	if strings.TrimSpace(req.EventType) != "attempt.started" {
		return fmt.Errorf("execution launch requires attempt.started event_type")
	}
	if req.EventSeq <= 0 {
		return fmt.Errorf("execution launch requires positive event sequence")
	}
	return nil
}

func validateDirectExecutionTerminalTransition(req TransitionRequest) error {
	if req.Entity != EntityTypeNode {
		return fmt.Errorf("direct execution terminal transition requires node transition")
	}
	if !req.NodeRunID.Valid || strings.TrimSpace(req.NodeRunID.String) == "" {
		return fmt.Errorf("direct execution terminal transition requires node_run_id")
	}
	if strings.TrimSpace(req.AttemptID) == "" {
		return fmt.Errorf("direct execution terminal transition requires attempt_id")
	}
	if strings.TrimSpace(req.EventType) == "" {
		return fmt.Errorf("direct execution terminal transition requires event_type")
	}
	if req.EventSeq <= 0 {
		return fmt.Errorf("direct execution terminal transition requires positive event sequence")
	}
	switch req.ToState {
	case "failed":
		if strings.TrimSpace(req.EventType) != "attempt.failed" {
			return fmt.Errorf("failed execution terminal transition requires attempt.failed event_type")
		}
	case "cancelled":
		if strings.TrimSpace(req.EventType) != "attempt.cancelled" {
			return fmt.Errorf("cancelled execution terminal transition requires attempt.cancelled event_type")
		}
	case "succeeded":
		if strings.TrimSpace(req.EventType) != "attempt.succeeded" {
			return fmt.Errorf("succeeded execution terminal transition requires attempt.succeeded event_type")
		}
	default:
		return fmt.Errorf("direct execution terminal transition does not support to_state %q", req.ToState)
	}
	return nil
}

func validateLoopCapAutoSuccessTransitionRequest(req TransitionRequest) error {
	if req.Entity != EntityTypeNode {
		return fmt.Errorf("loop cap auto-success requires node transition")
	}
	if !req.NodeRunID.Valid || strings.TrimSpace(req.NodeRunID.String) == "" {
		return fmt.Errorf("loop cap auto-success requires node_run_id")
	}
	if strings.TrimSpace(req.AttemptID) == "" {
		return fmt.Errorf("loop cap auto-success requires attempt_id")
	}
	if req.FromState != "ready" || req.ToState != "succeeded" {
		return fmt.Errorf("loop cap auto-success requires ready -> succeeded transition")
	}
	if strings.TrimSpace(req.EventType) != "attempt.succeeded" {
		return fmt.Errorf("loop cap auto-success requires attempt.succeeded event_type")
	}
	if req.EventSeq <= 0 {
		return fmt.Errorf("loop cap auto-success requires positive event sequence")
	}
	return nil
}

func normalizeLoopCapParticipantNodeIDs(nodeIDs []string) []string {
	if len(nodeIDs) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(nodeIDs))
	normalized := make([]string, 0, len(nodeIDs))
	for _, rawNodeID := range nodeIDs {
		trimmedNodeID := strings.TrimSpace(rawNodeID)
		if trimmedNodeID == "" {
			continue
		}
		if _, exists := seen[trimmedNodeID]; exists {
			continue
		}
		seen[trimmedNodeID] = struct{}{}
		normalized = append(normalized, trimmedNodeID)
	}
	sort.Strings(normalized)
	return normalized
}

func insertAttemptExecutionEventTx(ctx context.Context, tx *sql.Tx, req TransitionRequest) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO attempt_event_log (attempt_event_id, attempt_id, run_id, node_run_id, seq, event_type, reason_code, payload_json, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		fmt.Sprintf("attempt_event:%s:%d", strings.TrimSpace(req.AttemptID), req.EventSeq),
		strings.TrimSpace(req.AttemptID),
		strings.TrimSpace(req.RunID),
		strings.TrimSpace(req.NodeRunID.String),
		req.EventSeq,
		strings.TrimSpace(req.EventType),
		strings.TrimSpace(req.ReasonCode),
		strings.TrimSpace(req.PayloadJSON),
		strings.TrimSpace(req.OccurredAt),
	)
	if err != nil {
		return fmt.Errorf("insert attempt execution event: %w", err)
	}
	return nil
}

func markAttemptExecutionRunningTx(ctx context.Context, tx *sql.Tx, attemptID, runID, nodeRunID, occurredAt string) error {
	res, err := tx.ExecContext(ctx,
		`UPDATE attempt_executions
		 SET state = 'running',
		     started_at = CASE WHEN started_at IS NULL THEN ? ELSE started_at END,
		     updated_at = ?,
		     failure_code = '',
		     failure_text = ''
		 WHERE attempt_id = ? AND run_id = ? AND node_run_id = ?
		   AND state NOT IN ('committed_success', 'committed_failure', 'committed_cancelled')`,
		occurredAt,
		occurredAt,
		strings.TrimSpace(attemptID),
		strings.TrimSpace(runID),
		strings.TrimSpace(nodeRunID),
	)
	if err != nil {
		return fmt.Errorf("mark attempt execution running: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("read running attempt execution rows affected: %w", err)
	}
	if rows == 1 {
		return nil
	}
	state, found, err := loadAttemptExecutionStateTx(ctx, tx, attemptID, runID, nodeRunID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("attempt execution %q not found", strings.TrimSpace(attemptID))
	}
	return fmt.Errorf("attempt execution %q cannot start from state %q", strings.TrimSpace(attemptID), state)
}

func markAttemptExecutionStartingTx(ctx context.Context, tx *sql.Tx, attemptID, runID, nodeRunID, occurredAt string) error {
	res, err := tx.ExecContext(ctx,
		`UPDATE attempt_executions
		 SET state = 'starting',
		     launch_started_at = CASE WHEN launch_started_at IS NULL THEN ? ELSE launch_started_at END,
		     updated_at = ?,
		     failure_code = '',
		     failure_text = ''
		 WHERE attempt_id = ? AND run_id = ? AND node_run_id = ?
		   AND state NOT IN ('committed_success', 'committed_failure', 'committed_cancelled')`,
		occurredAt,
		occurredAt,
		strings.TrimSpace(attemptID),
		strings.TrimSpace(runID),
		strings.TrimSpace(nodeRunID),
	)
	if err != nil {
		return fmt.Errorf("mark attempt execution starting: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("read starting attempt execution rows affected: %w", err)
	}
	if rows == 1 {
		return nil
	}
	state, found, err := loadAttemptExecutionStateTx(ctx, tx, attemptID, runID, nodeRunID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("attempt execution %q not found", strings.TrimSpace(attemptID))
	}
	return fmt.Errorf("attempt execution %q cannot start from state %q", strings.TrimSpace(attemptID), state)
}

func markAttemptExecutionTerminalTx(ctx context.Context, tx *sql.Tx, attemptID, runID, nodeRunID, nodeState, reasonCode, reasonText, occurredAt string) error {
	terminalState, err := attemptExecutionTerminalStateForNodeState(nodeState)
	if err != nil {
		return err
	}
	failureCode := ""
	failureText := ""
	if terminalState != "committed_success" {
		failureCode = strings.TrimSpace(reasonCode)
		failureText = strings.TrimSpace(reasonText)
	}
	res, err := tx.ExecContext(ctx,
		`UPDATE attempt_executions
		 SET state = ?,
		     terminal_observed_at = ?,
		     committed_at = ?,
		     lease_owner = '',
		     lease_expires_at = NULL,
		     failure_code = ?,
		     failure_text = ?,
		     updated_at = ?
		 WHERE attempt_id = ? AND run_id = ? AND node_run_id = ?
		   AND state NOT IN ('committed_success', 'committed_failure', 'committed_cancelled')`,
		terminalState,
		occurredAt,
		occurredAt,
		failureCode,
		failureText,
		occurredAt,
		strings.TrimSpace(attemptID),
		strings.TrimSpace(runID),
		strings.TrimSpace(nodeRunID),
	)
	if err != nil {
		return fmt.Errorf("mark attempt execution terminal: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("read terminal attempt execution rows affected: %w", err)
	}
	if rows == 1 {
		return nil
	}
	state, found, err := loadAttemptExecutionStateTx(ctx, tx, attemptID, runID, nodeRunID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("attempt execution %q not found", strings.TrimSpace(attemptID))
	}
	return fmt.Errorf("attempt execution %q cannot terminalize from state %q", strings.TrimSpace(attemptID), state)
}

func ensureAttemptExecutionRowForDirectTerminalTx(ctx context.Context, tx *sql.Tx, attemptID, runID, nodeRunID, fromState, occurredAt string) error {
	if _, found, err := loadAttemptExecutionStateTx(ctx, tx, attemptID, runID, nodeRunID); err != nil {
		return err
	} else if found {
		return nil
	}

	executionMode, completionMode, err := loadExecutionModesForNodeRunTx(ctx, tx, runID, nodeRunID)
	if err != nil {
		return err
	}

	startedAt, err := loadNodeRunStartedAtTx(ctx, tx, runID, nodeRunID)
	if err != nil {
		return err
	}
	seedState := "queued"
	launchStartedAt := ""
	switch strings.TrimSpace(fromState) {
	case "starting":
		seedState = "starting"
		launchStartedAt = occurredAt
	case "running":
		seedState = "running"
		launchStartedAt = occurredAt
	}
	createdAt := occurredAt
	if strings.TrimSpace(startedAt) != "" {
		createdAt = strings.TrimSpace(startedAt)
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO attempt_executions (attempt_id, run_id, node_run_id, state, execution_mode, completion_mode, lease_owner, lease_expires_at, lease_version, session_ref, launch_started_at, started_at, terminal_observed_at, committed_at, failure_code, failure_text, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, '', NULL, 0, '', NULLIF(?, ''), NULLIF(?, ''), NULL, NULL, '', '', ?, ?)`,
		strings.TrimSpace(attemptID),
		strings.TrimSpace(runID),
		strings.TrimSpace(nodeRunID),
		seedState,
		string(executionMode),
		string(completionMode),
		strings.TrimSpace(launchStartedAt),
		strings.TrimSpace(startedAt),
		createdAt,
		occurredAt,
	)
	if err != nil {
		return fmt.Errorf("insert attempt execution fallback row: %w", err)
	}
	return nil
}

func ensureGenericRecoveryMayMutateAttemptTx(ctx context.Context, tx *sql.Tx, attemptID, runID, nodeRunID string) error {
	var executionMode string
	var activeProviderInvocationID string
	var canonicalSettlementID string
	err := tx.QueryRowContext(ctx, `SELECT execution_mode,
		COALESCE(active_provider_invocation_id, ''),
		COALESCE(canonical_settlement_id, '')
		FROM attempt_executions
		WHERE attempt_id = ? AND run_id = ? AND node_run_id = ?`,
		strings.TrimSpace(attemptID),
		strings.TrimSpace(runID),
		strings.TrimSpace(nodeRunID),
	).Scan(&executionMode, &activeProviderInvocationID, &canonicalSettlementID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("load attempt execution provider authority: %w", err)
	}
	if executionstore.NormalizeExecutionMode(executionMode) == executionstore.ExecutionModeProviderBacked &&
		strings.TrimSpace(activeProviderInvocationID) != "" &&
		strings.TrimSpace(canonicalSettlementID) == "" {
		return fmt.Errorf("provider-backed attempt %q with an active invocation requires a canonical provider settlement", strings.TrimSpace(attemptID))
	}
	return nil
}

func loadExecutionModesForNodeRunTx(ctx context.Context, tx *sql.Tx, runID, nodeRunID string) (executionstore.ExecutionMode, executionstore.ProviderCompletionMode, error) {
	var nodeID string
	var nodeClass string
	err := tx.QueryRowContext(ctx, `
SELECT nr.node_id,
       COALESCE(wn.node_class, '')
FROM node_runs nr
JOIN workflow_runs wr ON wr.run_id = nr.run_id
JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id AND wn.node_id = nr.node_id
WHERE nr.run_id = ? AND nr.node_run_id = ?
LIMIT 1`, strings.TrimSpace(runID), strings.TrimSpace(nodeRunID)).Scan(&nodeID, &nodeClass)
	if err != nil {
		return "", "", fmt.Errorf("load node behavior for attempt execution fallback row: %w", err)
	}
	behavior := workflow.BehaviorForNode(nodeID, nodeClass)
	completionMode, err := executionstore.ParseProviderCompletionMode(string(behavior.ProviderCompletionMode))
	if err != nil {
		return "", "", fmt.Errorf("resolve completion mode for attempt execution fallback row: %w", err)
	}
	if behavior.ExecutionOwnership == workflow.NodeExecutionOwnershipRuntimeOwned {
		if completionMode != executionstore.ProviderCompletionModeNotApplicable {
			return "", "", fmt.Errorf("runtime-owned fallback node has completion mode %q", completionMode)
		}
		return executionstore.ExecutionModeRuntimeOwned, completionMode, nil
	}
	if behavior.ExecutionOwnership != workflow.NodeExecutionOwnershipProviderBacked {
		return "", "", fmt.Errorf("unknown execution ownership for fallback node %q", nodeID)
	}
	if completionMode == executionstore.ProviderCompletionModeNotApplicable {
		return "", "", fmt.Errorf("provider-backed fallback node has completion mode %q", completionMode)
	}
	return executionstore.ExecutionModeProviderBacked, completionMode, nil
}

func loadNodeRunStartedAtTx(ctx context.Context, tx *sql.Tx, runID, nodeRunID string) (string, error) {
	var startedAt sql.NullString
	err := tx.QueryRowContext(ctx,
		`SELECT started_at FROM node_runs WHERE run_id = ? AND node_run_id = ?`,
		strings.TrimSpace(runID),
		strings.TrimSpace(nodeRunID),
	).Scan(&startedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("load node run started_at for attempt execution fallback: %w", err)
	}
	if !startedAt.Valid {
		return "", nil
	}
	return strings.TrimSpace(startedAt.String), nil
}

func attemptExecutionTerminalStateForNodeState(nodeState string) (string, error) {
	switch strings.TrimSpace(nodeState) {
	case "succeeded":
		return "committed_success", nil
	case "failed":
		return "committed_failure", nil
	case "cancelled":
		return "committed_cancelled", nil
	default:
		return "", fmt.Errorf("unsupported attempt execution terminal node state %q", nodeState)
	}
}

func nextAttemptExecutionSeqTx(ctx context.Context, tx *sql.Tx, attemptID string) (int, error) {
	trimmedAttemptID := strings.TrimSpace(attemptID)
	if trimmedAttemptID == "" {
		return 1, nil
	}
	var seq int
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq), 0) + 1 FROM attempt_event_log WHERE attempt_id = ?`, trimmedAttemptID).Scan(&seq)
	if err != nil {
		return 0, fmt.Errorf("load next attempt execution seq: %w", err)
	}
	if seq <= 0 {
		return 1, nil
	}
	return seq, nil
}

func closeRunSessionLeasesTx(ctx context.Context, tx *sql.Tx, runID, updatedAt string) error {
	if _, err := tx.ExecContext(ctx,
		`UPDATE session_leases
		 SET status = 'closed',
		     updated_at = ?
		 WHERE session_id IN (
		 	SELECT DISTINCT session_ref
		 	FROM attempt_executions
		 	WHERE run_id = ? AND TRIM(session_ref) <> ''
		 )
		   AND status IN ('active', 'expired')`,
		updatedAt,
		runID,
	); err != nil {
		return fmt.Errorf("close run session leases: %w", err)
	}
	return nil
}

func closeAttemptSessionLeaseTx(ctx context.Context, tx *sql.Tx, sessionID, updatedAt string) error {
	trimmedSessionID := strings.TrimSpace(sessionID)
	if trimmedSessionID == "" {
		return nil
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE session_leases
		 SET status = 'closed',
		     updated_at = ?
		 WHERE session_id = ?
		   AND status IN ('active', 'expired')`,
		updatedAt,
		trimmedSessionID,
	); err != nil {
		return fmt.Errorf("close attempt session lease: %w", err)
	}
	return nil
}

func (s *TransitionService) scheduleFencedStaleRetryOnce(ctx context.Context, req ScheduleFencedStaleRetryRequest) (QueuedNodeAttempt, error) {
	attempt := req.Attempt
	occurredAt := strings.TrimSpace(req.OccurredAt)
	if occurredAt == "" {
		occurredAt = time.Now().UTC().Format(time.RFC3339)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return QueuedNodeAttempt{}, fmt.Errorf("begin stale retry handoff transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	current, err := loadLiveNodeAttemptForStaleRetryTx(ctx, tx, req.CurrentAttemptID, attempt)
	if err != nil {
		return QueuedNodeAttempt{}, err
	}
	if isDurablyTerminalAttemptExecutionState(current.ExecutionState) {
		return QueuedNodeAttempt{}, fmt.Errorf("stale retry lost terminal authority for attempt %q", current.AttemptID)
	}

	if req.HasReplacement {
		if err := updateAgentStatusTx(ctx, tx, attempt.AgentID, AgentStatusBusy, AgentStatusBlocked, occurredAt); err != nil {
			var conflictErr *AgentStatusConflictError
			if !errors.As(err, &conflictErr) {
				return QueuedNodeAttempt{}, err
			}
		}
	}

	blockedPayload := fmt.Sprintf(`{"node_run_id":%q,"run_id":%q,"node_id":%q,"state":%q,"old_agent_id":%q,"new_agent_id":%q,"retry_strategy":%q}`,
		attempt.NodeRunID,
		attempt.RunID,
		attempt.NodeID,
		"blocked",
		attempt.AgentID,
		req.ReplacementAgentID,
		retryStrategyLabel(req.HasReplacement, !req.HasReplacement && req.ReplacementAgentID == attempt.AgentID),
	)
	if err := s.applyTx(ctx, tx, TransitionRequest{
		TransitionID:    fmt.Sprintf("tr_reconcile_%s_%s_%d_blocked", attempt.RunID, attempt.NodeID, attempt.Attempt),
		EventID:         fmt.Sprintf("evt_reconcile_%s_%s_%d_blocked", attempt.RunID, attempt.NodeID, attempt.Attempt),
		RunID:           attempt.RunID,
		NodeRunID:       sql.NullString{String: attempt.NodeRunID, Valid: true},
		Entity:          EntityTypeNode,
		FromState:       attempt.CurrentStatus,
		ToState:         "blocked",
		ActorID:         "reconciler",
		RoleID:          attempt.RequiredRole,
		AuthoritySource: "system.reconciler",
		ReasonCode:      "agent_unresponsive",
		ReasonText:      "stale attempt blocked for retry",
		CorrelationID:   fmt.Sprintf("corr_reconcile_%s", attempt.RunID),
		CausationID:     fmt.Sprintf("caus_reconcile_%s_%s", attempt.RunID, attempt.NodeRunID),
		OccurredAt:      occurredAt,
		AttemptID:       req.CurrentAttemptID,
		SchemaVersion:   "v1",
		EventType:       "attempt.recovered",
		EventSeq:        req.CurrentEventSeq,
		PayloadJSON:     blockedPayload,
	}); err != nil {
		return QueuedNodeAttempt{}, err
	}
	if err := fenceStaleAttemptExecutionTx(ctx, tx, current, occurredAt); err != nil {
		return QueuedNodeAttempt{}, err
	}
	if err := closeAttemptSessionLeaseTx(ctx, tx, current.SessionID, occurredAt); err != nil {
		return QueuedNodeAttempt{}, err
	}
	if err := queueAttemptCancelOutboxForReasonTx(ctx, tx, current, "agent_unresponsive", staleAttemptFailureReasonText, occurredAt, nil); err != nil {
		return QueuedNodeAttempt{}, err
	}

	nextAttempt, err := nextAttemptNumberTx(ctx, tx, attempt.RunID, attempt.NodeID)
	if err != nil {
		return QueuedNodeAttempt{}, err
	}
	if req.HasReplacement {
		if err := claimReusableAgentTx(ctx, tx, req.ReplacementAgentID, occurredAt); err != nil {
			return QueuedNodeAttempt{}, err
		}
	}
	retryNodeRunID := fmt.Sprintf("%s_retry_%d", attempt.NodeRunID, nextAttempt)
	if err := insertNodeAttemptTx(ctx, tx, CreateNodeAttemptRequest{
		NodeRunID:              retryNodeRunID,
		RunID:                  attempt.RunID,
		NodeID:                 attempt.NodeID,
		Attempt:                nextAttempt,
		Wave:                   attempt.Wave,
		NodeStatus:             "pending",
		RequiredRole:           attempt.RequiredRole,
		AgentID:                req.ReplacementAgentID,
		HandoffSourceAttemptID: current.AttemptID,
		RowVersion:             1,
		CreatedAt:              occurredAt,
	}); err != nil {
		return QueuedNodeAttempt{}, err
	}
	queuedAttempt := QueuedNodeAttempt{
		NodeID:       attempt.NodeID,
		NodeRunID:    retryNodeRunID,
		Attempt:      nextAttempt,
		Wave:         attempt.Wave,
		RequiredRole: attempt.RequiredRole,
		AgentID:      req.ReplacementAgentID,
	}
	retryPayload, err := marshalQueuedNodeAttemptEventPayload(attempt.RunID, queuedAttempt, "pending", nil)
	if err != nil {
		return QueuedNodeAttempt{}, fmt.Errorf("marshal fenced stale retry payload: %w", err)
	}
	retrySeq, err := nextAttemptExecutionSeqTx(ctx, tx, current.AttemptID)
	if err != nil {
		return QueuedNodeAttempt{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO attempt_event_log (attempt_event_id, attempt_id, run_id, node_run_id, seq, event_type, reason_code, payload_json, created_at)
		VALUES (?, ?, ?, ?, ?, 'attempt.retry_scheduled', 'agent_unresponsive', ?, ?)`,
		fmt.Sprintf("attempt_event:%s:%d", current.AttemptID, retrySeq),
		current.AttemptID,
		current.RunID,
		current.NodeRunID,
		retrySeq,
		retryPayload,
		occurredAt,
	); err != nil {
		return QueuedNodeAttempt{}, fmt.Errorf("append fenced stale retry event: %w", err)
	}
	if current.ExecutionMode != executionstore.ExecutionModeProviderBacked || !attemptCancelDispatchRequired(current) {
		if err := s.releaseFencedStaleRetryTx(ctx, tx, current.AttemptID, occurredAt); err != nil {
			return QueuedNodeAttempt{}, err
		}
	}

	if err := tx.Commit(); err != nil {
		return QueuedNodeAttempt{}, fmt.Errorf("commit stale retry handoff transaction: %w", err)
	}
	committed = true
	return queuedAttempt, nil
}

// ReleaseFencedStaleRetryAfterCancellation is the durable handoff boundary:
// cancellation publication and replacement readiness become visible together.
func (s *TransitionService) ReleaseFencedStaleRetryAfterCancellation(ctx context.Context, sourceAttemptID, occurredAt string) (bool, error) {
	if s == nil || s.db == nil {
		return false, fmt.Errorf("transition service database is required")
	}
	sourceAttemptID = strings.TrimSpace(sourceAttemptID)
	if sourceAttemptID == "" {
		return false, fmt.Errorf("source attempt id is required")
	}
	if strings.TrimSpace(occurredAt) == "" {
		occurredAt = time.Now().UTC().Format(time.RFC3339)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin stale cancellation handoff transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	var status string
	err = tx.QueryRowContext(ctx, `SELECT status FROM attempt_cancel_outbox WHERE attempt_id = ?`, sourceAttemptID).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load stale cancellation handoff: %w", err)
	}
	if strings.TrimSpace(status) == "pending" {
		if _, err := tx.ExecContext(ctx, `UPDATE attempt_cancel_outbox
			SET status = 'published', handoff_state = 'released', publish_attempts = publish_attempts + 1, last_error = '', updated_at = ?, published_at = ?
			WHERE attempt_id = ? AND status = 'pending'`, occurredAt, occurredAt, sourceAttemptID); err != nil {
			return false, fmt.Errorf("publish stale cancellation handoff: %w", err)
		}
	}
	if err := s.releaseFencedStaleRetryTx(ctx, tx, sourceAttemptID, occurredAt); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit stale cancellation handoff transaction: %w", err)
	}
	committed = true
	return true, nil
}

func (s *TransitionService) releaseFencedStaleRetryTx(ctx context.Context, tx *sql.Tx, sourceAttemptID, occurredAt string) error {
	var successor QueuedNodeAttempt
	err := tx.QueryRowContext(ctx, `SELECT node_id, node_run_id, attempt, wave, required_role, agent_id
		FROM node_runs
		WHERE handoff_source_attempt_id = ? AND node_status = 'pending'
		ORDER BY attempt ASC, node_run_id ASC
		LIMIT 1`, strings.TrimSpace(sourceAttemptID)).Scan(
		&successor.NodeID,
		&successor.NodeRunID,
		&successor.Attempt,
		&successor.Wave,
		&successor.RequiredRole,
		&successor.AgentID,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("load fenced stale retry successor: %w", err)
	}
	var runID string
	if err := tx.QueryRowContext(ctx, `SELECT run_id FROM node_runs WHERE node_run_id = ?`, successor.NodeRunID).Scan(&runID); err != nil {
		return fmt.Errorf("load fenced stale retry run: %w", err)
	}
	payload, err := marshalQueuedNodeAttemptEventPayload(runID, successor, "ready", nil)
	if err != nil {
		return fmt.Errorf("marshal released stale retry payload: %w", err)
	}
	return s.applyTx(ctx, tx, TransitionRequest{
		TransitionID:    fmt.Sprintf("tr_retry_handoff_%s_%s_%d_ready", runID, successor.NodeID, successor.Attempt),
		EventID:         fmt.Sprintf("evt_retry_handoff_%s_%s_%d_ready", runID, successor.NodeID, successor.Attempt),
		RunID:           runID,
		NodeRunID:       sql.NullString{String: successor.NodeRunID, Valid: true},
		Entity:          EntityTypeNode,
		FromState:       "pending",
		ToState:         "ready",
		ActorID:         "runtime_orchestrator",
		RoleID:          successor.RequiredRole,
		AuthoritySource: "runtime.orchestrator",
		ReasonCode:      "agent_unresponsive",
		ReasonText:      "stale attempt cancellation handoff completed",
		CorrelationID:   fmt.Sprintf("corr_retry_handoff_%s", runID),
		CausationID:     fmt.Sprintf("caus_retry_handoff_%s_%s", runID, successor.NodeRunID),
		OccurredAt:      occurredAt,
		AttemptID:       composeAttemptID(runID, successor.NodeID, successor.Attempt),
		SchemaVersion:   "v1",
		EventType:       "attempt.retry_queued",
		EventSeq:        1,
		PayloadJSON:     payload,
	})
}

func loadLiveNodeAttemptForStaleRetryTx(ctx context.Context, tx *sql.Tx, attemptID string, stale staleAttempt) (liveNodeAttempt, error) {
	current := liveNodeAttempt{
		AttemptID:    strings.TrimSpace(attemptID),
		RunID:        strings.TrimSpace(stale.RunID),
		NodeRunID:    strings.TrimSpace(stale.NodeRunID),
		NodeID:       strings.TrimSpace(stale.NodeID),
		Attempt:      stale.Attempt,
		NodeStatus:   strings.TrimSpace(stale.CurrentStatus),
		RequiredRole: strings.TrimSpace(stale.RequiredRole),
		AgentID:      strings.TrimSpace(stale.AgentID),
	}
	if current.AttemptID == "" || current.RunID == "" || current.NodeRunID == "" {
		return liveNodeAttempt{}, fmt.Errorf("stale retry attempt lineage is required")
	}
	if err := ensureAttemptExecutionRowForDirectTerminalTx(ctx, tx, current.AttemptID, current.RunID, current.NodeRunID, current.NodeStatus, stale.StartedAt); err != nil {
		return liveNodeAttempt{}, err
	}
	var executionMode string
	err := tx.QueryRowContext(ctx, `SELECT state, execution_mode, session_ref, COALESCE(launch_started_at, ''), COALESCE(started_at, ''), COALESCE(provider_session_id, ''), COALESCE(active_provider_invocation_id, '')
		FROM attempt_executions
		WHERE attempt_id = ? AND run_id = ? AND node_run_id = ?`, current.AttemptID, current.RunID, current.NodeRunID).Scan(
		&current.ExecutionState,
		&executionMode,
		&current.SessionID,
		&current.LaunchStartedAt,
		&current.StartedAt,
		&current.ProviderSessionID,
		&current.ActiveProviderInvocationID,
	)
	if err != nil {
		return liveNodeAttempt{}, fmt.Errorf("load stale retry attempt execution: %w", err)
	}
	current.ExecutionMode = executionstore.NormalizeExecutionMode(executionMode)
	return current, nil
}

func fenceStaleAttemptExecutionTx(ctx context.Context, tx *sql.Tx, attempt liveNodeAttempt, occurredAt string) error {
	if attempt.ExecutionMode == executionstore.ExecutionModeProviderBacked && strings.TrimSpace(attempt.ActiveProviderInvocationID) != "" {
		result, err := tx.ExecContext(ctx, `UPDATE provider_invocations
			SET invocation_state = 'superseded',
			    completion_capability_revoked_at = CASE WHEN completion_mode = 'receipt_required' THEN ? ELSE completion_capability_revoked_at END,
			    failure_reason_code = 'agent_unresponsive',
			    failure_reason_text = ?,
			    updated_at = ?
			WHERE provider_invocation_id = ?
			  AND invocation_state IN ('issued', 'open', 'receipt_persisted')`,
			occurredAt,
			staleAttemptFailureReasonText,
			occurredAt,
			attempt.ActiveProviderInvocationID,
		)
		if err != nil {
			return fmt.Errorf("supersede stale provider invocation: %w", err)
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("read stale provider invocation supersession rows: %w", err)
		}
		if rows != 1 {
			return fmt.Errorf("stale retry lost provider invocation authority for %q", attempt.ActiveProviderInvocationID)
		}
	}
	seq, err := nextAttemptExecutionSeqTx(ctx, tx, attempt.AttemptID)
	if err != nil {
		return err
	}
	payload := fmt.Sprintf(`{"node_run_id":%q,"node_id":%q,"reason":"stale_attempt"}`, attempt.NodeRunID, attempt.NodeID)
	if _, err := tx.ExecContext(ctx, `INSERT INTO attempt_event_log (attempt_event_id, attempt_id, run_id, node_run_id, seq, event_type, reason_code, payload_json, created_at)
		VALUES (?, ?, ?, ?, ?, 'attempt.cancelled', 'agent_unresponsive', ?, ?)`,
		fmt.Sprintf("attempt_event:%s:%d", attempt.AttemptID, seq),
		attempt.AttemptID,
		attempt.RunID,
		attempt.NodeRunID,
		seq,
		payload,
		occurredAt,
	); err != nil {
		return fmt.Errorf("append stale attempt cancellation event: %w", err)
	}
	result, err := tx.ExecContext(ctx, `UPDATE attempt_executions
		SET state = 'committed_cancelled',
		    terminal_observed_at = ?,
		    committed_at = ?,
		    lease_owner = '',
		    lease_expires_at = NULL,
		    failure_code = 'agent_unresponsive',
		    failure_text = ?,
		    updated_at = ?
		WHERE attempt_id = ?
		  AND run_id = ?
		  AND node_run_id = ?
		  AND state IN ('queued', 'starting', 'running')`,
		occurredAt,
		occurredAt,
		staleAttemptFailureReasonText,
		occurredAt,
		attempt.AttemptID,
		attempt.RunID,
		attempt.NodeRunID,
	)
	if err != nil {
		return fmt.Errorf("commit stale attempt cancellation: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read stale attempt cancellation rows: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("stale retry lost attempt authority for %q", attempt.AttemptID)
	}
	return nil
}

func claimReusableAgentTx(ctx context.Context, tx *sql.Tx, agentID, updatedAt string) error {
	trimmedAgentID := strings.TrimSpace(agentID)
	if trimmedAgentID == "" {
		return nil
	}
	if err := syncAgentOperationalStatusWithQuerier(ctx, tx, trimmedAgentID, updatedAt); err != nil {
		return err
	}
	if err := updateAgentStatusTx(ctx, tx, trimmedAgentID, AgentStatusIdle, AgentStatusBusy, updatedAt); err != nil {
		return err
	}
	return nil
}

func (s *TransitionService) insertPendingNodeAttemptTx(ctx context.Context, tx *sql.Tx, runID, nodeID string, wave int, occurredAt string, generate func(string) string) (QueuedNodeAttempt, error) {
	requiredRole, err := requiredRoleForRunNodeTx(ctx, tx, runID, nodeID)
	if err != nil {
		return QueuedNodeAttempt{}, err
	}
	agentID, err := ensureSystemAgentForRoleTx(ctx, tx, requiredRole, occurredAt)
	if err != nil {
		return QueuedNodeAttempt{}, err
	}
	return s.insertPendingNodeAttemptWithAgentTx(ctx, tx, runID, nodeID, requiredRole, agentID, wave, occurredAt, generate)
}

func (s *TransitionService) insertPendingNodeAttemptWithAgentTx(ctx context.Context, tx *sql.Tx, runID, nodeID, requiredRole, agentID string, wave int, occurredAt string, generate func(string) string) (QueuedNodeAttempt, error) {
	maxAttempt, err := nextAttemptNumberTx(ctx, tx, runID, nodeID)
	if err != nil {
		return QueuedNodeAttempt{}, err
	}
	if wave <= 0 {
		return QueuedNodeAttempt{}, fmt.Errorf("wave is required for queued node attempt run=%q node=%q", runID, nodeID)
	}
	nodeRunID := generateID(generate, "node_run")
	if err := insertNodeAttemptTx(ctx, tx, CreateNodeAttemptRequest{
		NodeRunID:    nodeRunID,
		RunID:        runID,
		NodeID:       nodeID,
		Attempt:      maxAttempt,
		Wave:         wave,
		NodeStatus:   "pending",
		RequiredRole: requiredRole,
		AgentID:      agentID,
		RowVersion:   1,
		CreatedAt:    occurredAt,
	}); err != nil {
		return QueuedNodeAttempt{}, err
	}
	return QueuedNodeAttempt{NodeID: nodeID, NodeRunID: nodeRunID, Attempt: maxAttempt, Wave: wave, RequiredRole: requiredRole, AgentID: agentID}, nil
}

func (s *TransitionService) resolveQueuedNodeWavesTx(ctx context.Context, tx *sql.Tx, runID string, nodeIDs []string) (map[string]int, error) {
	queuedWaves := make(map[string]int, len(nodeIDs))
	graph, err := workflowGraphForRunTx(ctx, tx, runID)
	if err != nil {
		return nil, err
	}
	loopScopeWaves := make(map[string]int)
	for _, nodeID := range nodeIDs {
		trimmedNodeID := strings.TrimSpace(nodeID)
		if trimmedNodeID == "" {
			continue
		}
		loopPlan, isLoopScoped, err := workflow.ResolveExecutionLoopScopePlan(graph, trimmedNodeID)
		if err != nil {
			return nil, err
		}
		if isLoopScoped {
			queuedParticipants := queuedLoopParticipantNodeIDs(nodeIDs, graph, loopPlan.ScopeID, loopPlan.NodeClass)
			cacheKey := loopPlan.NodeClass + ":" + strings.Join(queuedParticipants, "+")
			if wave, ok := loopScopeWaves[cacheKey]; ok {
				queuedWaves[trimmedNodeID] = wave
				continue
			}
			wave, err := nextLoopScopeWaveNumberTx(ctx, tx, runID, queuedParticipants)
			if err != nil {
				return nil, err
			}
			loopScopeWaves[cacheKey] = wave
			queuedWaves[trimmedNodeID] = wave
			continue
		}
		wave, err := nextNodeWaveNumberTx(ctx, tx, runID, trimmedNodeID)
		if err != nil {
			return nil, err
		}
		queuedWaves[trimmedNodeID] = wave
	}
	return queuedWaves, nil
}

func queuedLoopParticipantNodeIDs(nodeIDs []string, graph *workflow.Graph, scopeID, classID string) []string {
	if len(nodeIDs) == 0 || graph == nil {
		return nil
	}
	participants := make([]string, 0, len(nodeIDs))
	seen := make(map[string]struct{}, len(nodeIDs))
	for _, rawNodeID := range nodeIDs {
		trimmedNodeID := strings.TrimSpace(rawNodeID)
		if trimmedNodeID == "" {
			continue
		}
		loopPlan, ok, err := workflow.ResolveExecutionLoopScopePlan(graph, trimmedNodeID)
		if err != nil || !ok {
			continue
		}
		if loopPlan.ScopeID != scopeID || loopPlan.NodeClass != classID {
			continue
		}
		if _, exists := seen[trimmedNodeID]; exists {
			continue
		}
		seen[trimmedNodeID] = struct{}{}
		participants = append(participants, trimmedNodeID)
	}
	sort.Strings(participants)
	return participants
}

func workflowGraphForRunTx(ctx context.Context, tx *sql.Tx, runID string) (*workflow.Graph, error) {
	var workflowDefinitionID string
	err := tx.QueryRowContext(ctx, `SELECT workflow_definition_id FROM workflow_runs WHERE run_id = ?`, strings.TrimSpace(runID)).Scan(&workflowDefinitionID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("workflow run %q not found", strings.TrimSpace(runID))
	}
	if err != nil {
		return nil, fmt.Errorf("load workflow definition id for run %q: %w", strings.TrimSpace(runID), err)
	}
	nodeRows, err := tx.QueryContext(ctx, `
SELECT node_id, required_role, node_class
FROM workflow_nodes
WHERE workflow_definition_id = ?
ORDER BY node_id ASC`, strings.TrimSpace(workflowDefinitionID))
	if err != nil {
		return nil, fmt.Errorf("list workflow nodes for run %q: %w", strings.TrimSpace(runID), err)
	}
	defer nodeRows.Close()
	definition := workflow.Definition{WorkflowID: strings.TrimSpace(workflowDefinitionID)}
	for nodeRows.Next() {
		var node workflow.Node
		if err := nodeRows.Scan(&node.NodeID, &node.RequiredRole, &node.NodeClass); err != nil {
			return nil, fmt.Errorf("scan workflow node for run %q: %w", strings.TrimSpace(runID), err)
		}
		definition.Nodes = append(definition.Nodes, node)
	}
	if err := nodeRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workflow nodes for run %q: %w", strings.TrimSpace(runID), err)
	}
	edgeRows, err := tx.QueryContext(ctx, `
SELECT from_node_id, to_node_id, on_condition
FROM workflow_edges
WHERE workflow_definition_id = ?
ORDER BY from_node_id ASC, to_node_id ASC, on_condition ASC`, strings.TrimSpace(workflowDefinitionID))
	if err != nil {
		return nil, fmt.Errorf("list workflow edges for run %q: %w", strings.TrimSpace(runID), err)
	}
	defer edgeRows.Close()
	for edgeRows.Next() {
		var edge workflow.Edge
		if err := edgeRows.Scan(&edge.From, &edge.To, &edge.On); err != nil {
			return nil, fmt.Errorf("scan workflow edge for run %q: %w", strings.TrimSpace(runID), err)
		}
		definition.Edges = append(definition.Edges, edge)
	}
	if err := edgeRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workflow edges for run %q: %w", strings.TrimSpace(runID), err)
	}
	graph, err := workflow.NewParser(nil).ParseDefinition(definition)
	if err != nil {
		return nil, fmt.Errorf("build workflow graph for run %q: %w", strings.TrimSpace(runID), err)
	}
	return graph, nil
}

func loadRunLoopSettingsTx(ctx context.Context, tx *sql.Tx, runID string) (workflow.LoopSettings, error) {
	var runUntilAllApprove int
	var maxExecutionReviewCycles int
	var maxValidationReworkCycles int
	var hasExecutionReview int
	var hasValidationGates int
	err := tx.QueryRowContext(ctx, `
SELECT COALESCE(wr.run_until_all_approve, 0),
       COALESCE(wr.max_execution_review_cycles, 0),
       COALESCE(wr.max_validation_rework_cycles, 0),
       COALESCE(MAX(CASE WHEN wn.node_class = 'execution_review' THEN 1 ELSE 0 END), 0),
       COALESCE(MAX(CASE WHEN wn.node_class = 'validation_gates' THEN 1 ELSE 0 END), 0)
FROM workflow_runs wr
LEFT JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id
WHERE wr.run_id = ?
GROUP BY wr.run_id, wr.run_until_all_approve, wr.max_execution_review_cycles, wr.max_validation_rework_cycles`, strings.TrimSpace(runID)).Scan(
		&runUntilAllApprove,
		&maxExecutionReviewCycles,
		&maxValidationReworkCycles,
		&hasExecutionReview,
		&hasValidationGates,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return workflow.LoopSettings{}, fmt.Errorf("run %q not found", runID)
	}
	if err != nil {
		return workflow.LoopSettings{}, fmt.Errorf("load run loop settings: %w", err)
	}
	return workflow.NormalizeLoopSettings(workflow.LoopSettings{
		RunUntilAllApprove:        runUntilAllApprove != 0,
		HasExecutionReview:        hasExecutionReview != 0,
		HasValidationGates:        hasValidationGates != 0,
		MaxExecutionReviewCycles:  maxExecutionReviewCycles,
		MaxValidationReworkCycles: maxValidationReworkCycles,
	}), nil
}

func requiredRoleForRunNodeTx(ctx context.Context, tx *sql.Tx, runID, nodeID string) (string, error) {
	var requiredRole string
	err := tx.QueryRowContext(ctx,
		`SELECT wn.required_role
		 FROM workflow_runs wr
		 JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id
		 WHERE wr.run_id = ? AND wn.node_id = ?
		 LIMIT 1`,
		runID,
		nodeID,
	).Scan(&requiredRole)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("node %q not found for run %q", nodeID, runID)
	}
	if err != nil {
		return "", fmt.Errorf("load required role for run node: %w", err)
	}
	return strings.TrimSpace(requiredRole), nil
}

func nextAttemptNumberTx(ctx context.Context, tx *sql.Tx, runID, nodeID string) (int, error) {
	var maxAttempt int
	err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(attempt), 0) FROM node_runs WHERE run_id = ? AND node_id = ?`,
		runID,
		nodeID,
	).Scan(&maxAttempt)
	if err != nil {
		return 0, fmt.Errorf("load max node attempt: %w", err)
	}
	return maxAttempt + 1, nil
}

func nextNodeWaveNumberTx(ctx context.Context, tx *sql.Tx, runID, nodeID string) (int, error) {
	var maxWave int
	err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(wave), 0) FROM node_runs WHERE run_id = ? AND node_id = ?`,
		runID,
		nodeID,
	).Scan(&maxWave)
	if err != nil {
		return 0, fmt.Errorf("load max node wave: %w", err)
	}
	return maxWave + 1, nil
}

func nextLoopScopeWaveNumberTx(ctx context.Context, tx *sql.Tx, runID string, nodeIDs []string) (int, error) {
	trimmedNodeIDs := make([]string, 0, len(nodeIDs))
	seen := make(map[string]struct{}, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		trimmedNodeID := strings.TrimSpace(nodeID)
		if trimmedNodeID == "" {
			continue
		}
		if _, ok := seen[trimmedNodeID]; ok {
			continue
		}
		seen[trimmedNodeID] = struct{}{}
		trimmedNodeIDs = append(trimmedNodeIDs, trimmedNodeID)
	}
	if len(trimmedNodeIDs) == 0 {
		return 0, fmt.Errorf("loop scope node ids are required")
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(trimmedNodeIDs)), ",")
	args := make([]any, 0, len(trimmedNodeIDs)+1)
	args = append(args, strings.TrimSpace(runID))
	for _, nodeID := range trimmedNodeIDs {
		args = append(args, nodeID)
	}
	query := fmt.Sprintf(`SELECT COALESCE(MAX(wave), 0) FROM node_runs WHERE run_id = ? AND node_id IN (%s)`, placeholders)
	var maxWave int
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&maxWave); err != nil {
		return 0, fmt.Errorf("load max loop scope wave: %w", err)
	}
	return maxWave + 1, nil
}

func nextEventSeqTx(ctx context.Context, tx *sql.Tx, attemptID string) (int, error) {
	trimmedAttemptID := strings.TrimSpace(attemptID)
	if trimmedAttemptID == "" {
		return 1, nil
	}
	var seq int
	err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq), 0) + 1 FROM events WHERE attempt_id = ?`, trimmedAttemptID).Scan(&seq)
	if err != nil || seq <= 0 {
		return 1, nil
	}
	return seq, nil
}

func nextRunEventSeqTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	trimmedRunID := strings.TrimSpace(runID)
	if trimmedRunID == "" {
		return 0, fmt.Errorf("run id is required")
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO run_event_sequences (run_id, last_seq)
		 VALUES (?, COALESCE((SELECT MAX(run_event_seq) FROM events WHERE run_id = ?), 0))
		 ON CONFLICT(run_id) DO UPDATE SET last_seq = CASE
		 	WHEN excluded.last_seq > run_event_sequences.last_seq THEN excluded.last_seq
		 	ELSE run_event_sequences.last_seq
		 END`,
		trimmedRunID,
		trimmedRunID,
	); err != nil {
		return 0, fmt.Errorf("ensure run event sequence state: %w", err)
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE run_event_sequences
		 SET last_seq = last_seq + 1
		 WHERE run_id = ?`,
		trimmedRunID,
	); err != nil {
		return 0, fmt.Errorf("increment run event sequence: %w", err)
	}

	var seq int
	if err := tx.QueryRowContext(ctx,
		`SELECT last_seq FROM run_event_sequences WHERE run_id = ?`,
		trimmedRunID,
	).Scan(&seq); err != nil {
		return 0, fmt.Errorf("load run event sequence: %w", err)
	}
	if seq <= 0 {
		return 0, fmt.Errorf("invalid run event sequence for %q", trimmedRunID)
	}
	return seq, nil
}

func ensureSystemAgentForRoleTx(ctx context.Context, tx *sql.Tx, roleID, now string) (string, error) {
	canonicalAgentID := deterministicSystemAgentIDForRole(roleID)
	var agentID string
	err := tx.QueryRowContext(ctx,
		`SELECT agent_id
		 FROM agents
		 WHERE role_id = ?
		 ORDER BY
		 	CASE
		 		WHEN agent_id = ? THEN 0
		 		WHEN agent_id LIKE 'agent_%' AND agent_id NOT LIKE 'agent_system_%' THEN 1
		 		WHEN agent_id LIKE 'agent_system_%' THEN 2
		 		ELSE 3
		 	END,
		 	CASE status
		 		WHEN 'idle' THEN 0
		 		WHEN 'busy' THEN 1
		 		WHEN 'blocked' THEN 2
		 		WHEN 'offline' THEN 3
		 		WHEN 'failed' THEN 4
		 		ELSE 5
		 	END,
		 	created_at ASC,
		 	agent_id ASC
		 LIMIT 1`,
		roleID,
		canonicalAgentID,
	).Scan(&agentID)
	if err == nil {
		return agentID, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("query agent for role %q: %w", roleID, err)
	}

	displayName := strings.TrimSpace(roleID)
	_ = tx.QueryRowContext(ctx, `SELECT display_name FROM roles WHERE role_id = ? LIMIT 1`, roleID).Scan(&displayName)
	if strings.TrimSpace(displayName) == "" {
		displayName = roleID
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at)
		 VALUES (?, ?, ?, 'idle', NULL, 0, ?, ?)
		 ON CONFLICT(agent_id) DO UPDATE SET display_name = excluded.display_name, status = 'idle', updated_at = excluded.updated_at`,
		canonicalAgentID,
		roleID,
		displayName,
		now,
		now,
	); err != nil {
		return "", fmt.Errorf("upsert system agent for role %q: %w", roleID, err)
	}
	return canonicalAgentID, nil
}

func updateAgentStatusTx(ctx context.Context, tx *sql.Tx, agentID string, fromStatus, toStatus AgentStatus, updatedAt string) error {
	if err := validateAgentStatusTransition(fromStatus, toStatus); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx,
		`UPDATE agents SET status = ?, updated_at = ? WHERE agent_id = ? AND status = ?`,
		toStatus,
		updatedAt,
		agentID,
		fromStatus,
	)
	if err != nil {
		return fmt.Errorf("update agent status: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("agent status rows affected: %w", err)
	}
	if rows == 1 {
		return nil
	}
	var observed AgentStatus
	err = tx.QueryRowContext(ctx, `SELECT status FROM agents WHERE agent_id = ?`, agentID).Scan(&observed)
	if errors.Is(err, sql.ErrNoRows) {
		return &AgentNotFoundError{AgentID: agentID}
	}
	if err != nil {
		return fmt.Errorf("load agent status for %q: %w", agentID, err)
	}
	return &AgentStatusConflictError{AgentID: agentID, ExpectedStatus: fromStatus, ObservedStatus: observed}
}

func deterministicSystemAgentIDForRole(roleID string) string {
	trimmed := strings.ToLower(strings.TrimSpace(roleID))
	cleaned := strings.NewReplacer("-", "_", ".", "_", "/", "_", " ", "_").Replace(trimmed)
	for strings.Contains(cleaned, "__") {
		cleaned = strings.ReplaceAll(cleaned, "__", "_")
	}
	cleaned = strings.Trim(cleaned, "_")
	if cleaned == "" {
		cleaned = "unknown"
	}
	return "agent_" + cleaned
}

func generateID(generate func(string) string, prefix string) string {
	if generate != nil {
		return generate(prefix)
	}
	return fmt.Sprintf("%s_%d", prefix, time.Now().UTC().UnixNano())
}

func firstNonEmptyTrimmed(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
