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
	"kingdom/internal/nodeattempt"
	"kingdom/internal/sqlitex"
	"kingdom/internal/timestamporder"
	"kingdom/internal/workspace"
)

const executionStartReadbackTimeout = time.Second

var (
	errStaleAttemptExecutionLeaseChanged = errors.New("stale attempt execution lease changed")
	errProviderTerminalHandoffPending    = errors.New("provider terminal handoff is pending")
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
	RetryDecision      *RetryDecisionCreateRequest
}

type AttemptStopRetryRequest struct {
	RunID      string
	AttemptID  string
	ActorID    string
	RoleID     string
	OccurredAt string
	GenerateID func(string) string
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

// AutomaticWorkOrderRetryRecordRequest is the single durable boundary between a
// settled provider failure and its policy-directed successor.
type AutomaticWorkOrderRetryRecordRequest struct {
	Transition TransitionRequest
	Decision   RetryDecisionCreateRequest
	GenerateID func(string) string
}

type DirectExecutionTerminalRequest struct {
	Transition   TransitionRequest
	StaleAttempt *staleAttempt
}

// DirectExecutionTerminalWithRetryRequest atomically records a recovery-owned
// terminal attempt and its automatic retry disposition.
type DirectExecutionTerminalWithRetryRequest struct {
	Transition            TransitionRequest
	StaleAttempt          *staleAttempt
	Decision              RetryDecisionCreateRequest
	EscalationFromAgentID string
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
	LeaseOwner                 string
	LeaseVersion               int64
	LeaseExpiresAt             string
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
		if !IsUniqueViolation(err) && !sqlitex.IsBusy(err) {
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
			return resolveRecoveredWorkOrderRetryTx(ctx, tx, req.Transition)
		}
		if err := persistRunPlanningOutputTx(ctx, tx, s.db, *req.PlanningOutput); err != nil {
			return err
		}
		return resolveRecoveredWorkOrderRetryTx(ctx, tx, req.Transition)
	})
}

func (s *TransitionService) RecordAutomaticWorkOrderRetry(ctx context.Context, req AutomaticWorkOrderRetryRecordRequest) (QueuedNodeAttempt, bool, error) {
	if req.Transition.Entity != EntityTypeNode || req.Transition.ToState != "failed" || !req.Transition.NodeRunID.Valid {
		return QueuedNodeAttempt{}, false, fmt.Errorf("automatic work order retry requires a failed node transition")
	}
	if req.Decision.SubjectKind != RetrySubjectKindWorkOrderAttempt || strings.TrimSpace(req.Decision.SourceAttemptID) != strings.TrimSpace(req.Transition.AttemptID) {
		return QueuedNodeAttempt{}, false, fmt.Errorf("automatic work order retry decision must match the failed work order attempt")
	}
	var queued QueuedNodeAttempt
	var scheduled bool
	_, err := sqlitex.Retry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin automatic work order retry transaction: %w", err)
		}
		committed := false
		defer func() {
			if !committed {
				_ = tx.Rollback()
			}
		}()
		transition := req.Transition
		if err := s.applyTx(ctx, tx, transition); err != nil {
			return err
		}
		if err := resolveFailedWorkOrderRetryTx(ctx, tx, transition); err != nil {
			return err
		}
		decisionReq := req.Decision
		decisionReq.RunID = req.Transition.RunID
		decisionReq.NodeRunID = req.Transition.NodeRunID.String
		var source struct {
			NodeID       string
			Wave         int
			RequiredRole string
			AgentID      string
		}
		if err := tx.QueryRowContext(ctx, `SELECT node_id, wave, required_role, agent_id FROM node_runs WHERE node_run_id = ? AND run_id = ?`, decisionReq.NodeRunID, decisionReq.RunID).Scan(&source.NodeID, &source.Wave, &source.RequiredRole, &source.AgentID); err != nil {
			return fmt.Errorf("load failed work order retry source: %w", err)
		}
		decisionReq.NodeID = source.NodeID
		if decisionReq.Decision == RetryDecisionRetry {
			queued, err = s.insertPendingNodeAttemptWithAgentTx(ctx, tx, decisionReq.RunID, source.NodeID, source.RequiredRole, source.AgentID, source.Wave, req.Transition.OccurredAt, req.GenerateID)
			if err != nil {
				return err
			}
			decisionReq.TargetNodeRunID = queued.NodeRunID
			scheduled = true
		}
		decision, err := insertRetryDecisionTx(ctx, tx, decisionReq)
		if err != nil {
			return err
		}
		targetAttemptID := ""
		if scheduled {
			targetAttemptID = composeAttemptID(decision.RunID, queued.NodeID, queued.Attempt)
		}
		payload, err := marshalRetryRecoveryEventPayload(retryRecoveryEventPayload{
			NodeID:          decision.NodeID,
			NodeRunID:       decision.NodeRunID,
			RetryDecisionID: decision.DecisionID,
			Decision:        string(decision.Decision),
			RecoveryTrigger: "automatic_retry",
			RecoveryPhase:   retryRecoveryPhase(decision),
			ErrorCode:       decision.ErrorCode,
			SourceAttemptID: decision.SourceAttemptID,
			SourceNodeRunID: decision.NodeRunID,
			TargetAttemptID: targetAttemptID,
			TargetNodeRunID: decision.TargetNodeRunID,
			RetryIndex:      decision.RetryIndex,
			RetryMax:        decision.MaxRetries,
			ScheduledAt:     retryTimeString(decision.DueAt),
		})
		if err != nil {
			return fmt.Errorf("marshal automatic work order retry event: %w", err)
		}
		if decision.Decision == RetryDecisionRetry {
			if err := s.appendAutomaticRetryScheduledEventTx(ctx, tx, req.Transition, decision, fmt.Sprintf("evt_retry_scheduled_%s", decision.DecisionID), payload); err != nil {
				return err
			}
		} else if decision.Decision == RetryDecisionExhausted {
			seq, err := nextAttemptExecutionSeqTx(ctx, tx, decision.SourceAttemptID)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO attempt_event_log (attempt_event_id, attempt_id, run_id, node_run_id, seq, event_type, reason_code, payload_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, fmt.Sprintf("attempt_event:%s:%d", decision.SourceAttemptID, seq), decision.SourceAttemptID, decision.RunID, decision.NodeRunID, seq, "escalation.raised", decision.ErrorCode, payload, req.Transition.OccurredAt); err != nil {
				return fmt.Errorf("append automatic work order retry event: %w", err)
			}
		}
		if decision.Decision == RetryDecisionExhausted {
			if err := s.deliverWorkOrderRetryExhaustionTx(ctx, tx, decision, source.AgentID); err != nil {
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit automatic work order retry transaction: %w", err)
		}
		committed = true
		return nil
	}, sqlitex.Options{})
	return queued, scheduled, err
}

func (s *TransitionService) deliverWorkOrderRetryExhaustionTx(ctx context.Context, tx *sql.Tx, decision RetryDecision, fromAgentID string) error {
	targetRole := strings.TrimSpace(decision.EscalationTarget)
	if targetRole == "" {
		return fmt.Errorf("retry exhaustion escalation target is required")
	}
	toAgentID, err := resolveRetryEscalationAgentTx(ctx, tx, decision.RunID, targetRole, decision.CreatedAt)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(map[string]any{"run_id": decision.RunID, "node_id": decision.NodeID, "node_run_id": decision.NodeRunID, "retry_decision_id": decision.DecisionID, "reason_code": decision.ErrorCode, "reason_text": decision.OperatorText, "escalation_target": targetRole, "authority_source": "runtime.retry", "message_source": "retry_exhaustion"})
	if err != nil {
		return fmt.Errorf("marshal retry exhaustion escalation: %w", err)
	}
	message := MailboxMessage{MessageID: "msg_retry_exhaustion_" + sanitizeMailboxToken(decision.DecisionID), RunID: decision.RunID, FromAgentID: strings.TrimSpace(fromAgentID), ToAgentID: toAgentID, ThreadID: "thread_retry_" + sanitizeMailboxToken(decision.SourceAttemptID), MessageType: MailboxMessageTypeEscalation, Priority: MailboxPriorityHigh, PayloadJSON: string(payload), CreatedAt: retryTimeString(decision.CreatedAt)}
	result, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`, message.MessageID, message.RunID, message.FromAgentID, message.ToAgentID, message.ThreadID, message.MessageType, message.Priority, message.PayloadJSON, message.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert retry exhaustion escalation: %w", err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read retry exhaustion escalation insert: %w", err)
	}
	if inserted == 0 {
		return nil
	}
	if err := upsertRunParticipantTx(ctx, tx, message.RunID, message.FromAgentID, message.CreatedAt); err != nil {
		return err
	}
	if err := upsertRunParticipantTx(ctx, tx, message.RunID, message.ToAgentID, message.CreatedAt); err != nil {
		return err
	}
	if err := (&MailboxStore{eventSchemas: s.eventSchemas}).recordMailboxEventTx(ctx, tx, message, 1); err != nil {
		return fmt.Errorf("record retry exhaustion mailbox event: %w", err)
	}
	return nil
}

func resolveRetryEscalationAgentTx(ctx context.Context, tx *sql.Tx, runID, roleID string, occurredAt time.Time) (string, error) {
	var agentID string
	err := tx.QueryRowContext(ctx, `SELECT a.agent_id FROM node_runs nr JOIN agents a ON a.agent_id = nr.agent_id WHERE nr.run_id = ? AND a.role_id = ? ORDER BY a.agent_id LIMIT 1`, runID, roleID).Scan(&agentID)
	if err == nil {
		return agentID, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("resolve retry escalation recipient: %w", err)
	}
	agentID = mailboxAgentIDForRole(roleID)
	if err := ensureMirrorAgent(ctx, tx, agentID, roleID, retryTimeString(occurredAt)); err != nil {
		return "", err
	}
	return agentID, nil
}

func (s *TransitionService) CountWorkOrderRetryDecisions(ctx context.Context, runID, nodeID string) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM retry_decisions WHERE subject_kind = 'work_order_attempt' AND run_id = ? AND node_id = ? AND decision = 'retry'`, strings.TrimSpace(runID), strings.TrimSpace(nodeID)).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count work order retry decisions: %w", err)
	}
	return count, nil
}

const workOrderRetryReleasePageSize = 128

type dueWorkOrderRetryCandidate struct {
	DecisionID      string
	DueAt           time.Time
	DueAtRaw        string
	ErrorCode       string
	SourceAttemptID string
	SourceNodeRunID string
	RetryIndex      int
	RetryMax        int
	QueuedNodeAttempt
	HandoffSourceAttemptID string
}

func listDueWorkOrderRetryPageTx(ctx context.Context, tx *sql.Tx, runID, dueAt string, after *dueWorkOrderRetryCandidate) ([]dueWorkOrderRetryCandidate, error) {
	query := `SELECT rd.retry_decision_id, rd.due_at, rd.error_code, rd.source_attempt_id, rd.node_run_id, rd.retry_index, rd.max_retries, rd.target_node_run_id, nr.node_id, nr.attempt, nr.wave, nr.required_role, nr.agent_id, COALESCE(nr.handoff_source_attempt_id, '')
		FROM retry_decisions rd
		JOIN node_runs nr ON nr.node_run_id = rd.target_node_run_id AND nr.run_id = rd.run_id
		WHERE rd.subject_kind = 'work_order_attempt' AND rd.run_id = ? AND rd.decision = 'retry' AND rd.dispatch_state = 'pending' AND rd.resolution = 'pending' AND nr.node_status = 'pending'
		AND ` + timestamporder.ComparisonSQL("rd.due_at", "?", "<=")
	args := []any{runID, dueAt}
	if after != nil {
		query += ` AND (` + timestamporder.ComparisonSQL("rd.due_at", "?", ">") + ` OR (` + timestamporder.ComparisonSQL("rd.due_at", "?", "=") + ` AND (rd.due_at, rd.retry_decision_id) > (?, ?)))`
		args = append(args, after.DueAtRaw, after.DueAtRaw, after.DueAtRaw, after.DecisionID)
	}
	query += ` ORDER BY ` + timestamporder.OrderBySQL("rd.due_at", false) + `, rd.retry_decision_id LIMIT ?`
	args = append(args, workOrderRetryReleasePageSize)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list due work order retry page: %w", err)
	}
	defer rows.Close()
	candidates := make([]dueWorkOrderRetryCandidate, 0, workOrderRetryReleasePageSize)
	for rows.Next() {
		var candidate dueWorkOrderRetryCandidate
		if err := rows.Scan(&candidate.DecisionID, &candidate.DueAtRaw, &candidate.ErrorCode, &candidate.SourceAttemptID, &candidate.SourceNodeRunID, &candidate.RetryIndex, &candidate.RetryMax, &candidate.NodeRunID, &candidate.NodeID, &candidate.Attempt, &candidate.Wave, &candidate.RequiredRole, &candidate.AgentID, &candidate.HandoffSourceAttemptID); err != nil {
			return nil, fmt.Errorf("scan due work order retry: %w", err)
		}
		candidate.DueAt, err = parseRetryTime(candidate.DueAtRaw)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read due work order retry page: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close due work order retry page: %w", err)
	}
	return candidates, nil
}

// ReleaseDueWorkOrderRetries makes one page of due retries visible to the normal dispatcher.
// The decision dispatch marker and node readiness are one transaction so a
// restart can always retry an unclaimed pending decision. Keyset scanning past
// blocked candidates prevents them from starving later eligible retries.
func (s *TransitionService) ReleaseDueWorkOrderRetries(ctx context.Context, runID, occurredAt string) ([]QueuedNodeAttempt, error) {
	if strings.TrimSpace(runID) == "" || strings.TrimSpace(occurredAt) == "" {
		return nil, fmt.Errorf("run id and occurred_at are required")
	}
	occurredAtTime, err := parseRetryTime(strings.TrimSpace(occurredAt))
	if err != nil {
		return nil, fmt.Errorf("parse due work order retry timestamp: %w", err)
	}
	var released []QueuedNodeAttempt
	_, err = sqlitex.Retry(ctx, func() error {
		released = nil
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin due work order retry release transaction: %w", err)
		}
		committed := false
		defer func() {
			if !committed {
				_ = tx.Rollback()
			}
		}()
		var after *dueWorkOrderRetryCandidate
		for len(released) < workOrderRetryReleasePageSize {
			candidates, err := listDueWorkOrderRetryPageTx(ctx, tx, strings.TrimSpace(runID), retryTimeString(occurredAtTime), after)
			if err != nil {
				return err
			}
			if len(candidates) == 0 {
				break
			}
			for _, candidate := range candidates {
				if candidate.DueAt.After(occurredAtTime) {
					continue
				}
				if strings.TrimSpace(candidate.HandoffSourceAttemptID) != "" {
					var handoffState string
					err := tx.QueryRowContext(ctx, `SELECT handoff_state FROM attempt_cancel_outbox WHERE attempt_id = ?`, candidate.HandoffSourceAttemptID).Scan(&handoffState)
					if err != nil && !errors.Is(err, sql.ErrNoRows) {
						return fmt.Errorf("load stale retry quiescence barrier: %w", err)
					}
					if err == nil && strings.TrimSpace(handoffState) != "released" {
						continue
					}
				}
				ready, err := workOrderRetryDependenciesSatisfiedTx(ctx, tx, runID, candidate.NodeID)
				if err != nil {
					return err
				}
				if !ready {
					continue
				}
				result, err := tx.ExecContext(ctx, `UPDATE retry_decisions SET dispatch_state = 'dispatched', dispatched_at = ? WHERE retry_decision_id = ? AND dispatch_state = 'pending' AND resolution = 'pending'`, occurredAt, candidate.DecisionID)
				if err != nil {
					return fmt.Errorf("mark due work order retry dispatched: %w", err)
				}
				changed, err := result.RowsAffected()
				if err != nil {
					return fmt.Errorf("read due work order retry dispatch rows: %w", err)
				}
				if changed != 1 {
					continue
				}
				payload, err := marshalRetryRecoveryEventPayload(retryRecoveryEventPayload{
					NodeID:          candidate.NodeID,
					NodeRunID:       candidate.NodeRunID,
					RetryDecisionID: candidate.DecisionID,
					Decision:        string(RetryDecisionRetry),
					RecoveryTrigger: "automatic_retry",
					RecoveryPhase:   "scheduled",
					ErrorCode:       candidate.ErrorCode,
					SourceAttemptID: candidate.SourceAttemptID,
					SourceNodeRunID: candidate.SourceNodeRunID,
					TargetAttemptID: composeAttemptID(runID, candidate.NodeID, candidate.Attempt),
					TargetNodeRunID: candidate.NodeRunID,
					RetryIndex:      candidate.RetryIndex,
					RetryMax:        candidate.RetryMax,
					ScheduledAt:     retryTimeString(candidate.DueAt),
				})
				if err != nil {
					return err
				}
				if err := s.applyTx(ctx, tx, TransitionRequest{TransitionID: fmt.Sprintf("tr_retry_%s_ready", candidate.DecisionID), EventID: fmt.Sprintf("evt_retry_%s_ready", candidate.DecisionID), RunID: runID, NodeRunID: sql.NullString{String: candidate.NodeRunID, Valid: true}, Entity: EntityTypeNode, FromState: "pending", ToState: "ready", ActorID: "runtime_orchestrator", RoleID: candidate.RequiredRole, AuthoritySource: "runtime.retry", ReasonCode: "automatic_retry", ReasonText: "automatic retry due", CorrelationID: "corr_retry_" + runID, CausationID: "caus_retry_" + candidate.DecisionID, OccurredAt: occurredAt, AttemptID: composeAttemptID(runID, candidate.NodeID, candidate.Attempt), SchemaVersion: "v1", EventType: "attempt.retry_queued", EventSeq: 1, PayloadJSON: payload}); err != nil {
					return err
				}
				released = append(released, candidate.QueuedNodeAttempt)
				if len(released) == workOrderRetryReleasePageSize {
					break
				}
			}
			after = &candidates[len(candidates)-1]
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit due work order retry release transaction: %w", err)
		}
		committed = true
		return nil
	}, sqlitex.Options{})
	if err != nil {
		return nil, err
	}
	return released, nil
}

func workOrderRetryDependenciesSatisfiedTx(ctx context.Context, tx *sql.Tx, runID, nodeID string) (bool, error) {
	rows, err := tx.QueryContext(ctx, `SELECT we.on_condition, COALESCE((SELECT nr.node_status FROM node_runs nr WHERE nr.run_id = ? AND nr.node_id = we.from_node_id AND COALESCE(TRIM(nr.superseded_at), '') = '' ORDER BY nr.attempt DESC, nr.node_run_id DESC LIMIT 1), '') FROM workflow_edges we JOIN workflow_runs wr ON wr.workflow_definition_id = we.workflow_definition_id WHERE wr.run_id = ? AND we.to_node_id = ?`, runID, runID, nodeID)
	if err != nil {
		return false, fmt.Errorf("load work order retry dependencies: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var condition, status string
		if err := rows.Scan(&condition, &status); err != nil {
			return false, fmt.Errorf("scan work order retry dependency: %w", err)
		}
		if !retryConditionMatches(condition, status) {
			return false, nil
		}
	}
	return true, rows.Err()
}

func retryConditionMatches(condition, status string) bool {
	switch strings.ToLower(strings.TrimSpace(condition)) {
	case "failed":
		return status == "failed"
	case "blocked":
		return status == "blocked"
	default:
		return status == "succeeded"
	}
}

func resolveRecoveredWorkOrderRetryTx(ctx context.Context, tx *sql.Tx, transition TransitionRequest) error {
	if transition.Entity != EntityTypeNode || transition.ToState != "succeeded" || !transition.NodeRunID.Valid {
		return nil
	}
	_, err := tx.ExecContext(ctx, `UPDATE retry_decisions SET resolution = ?, resolved_at = ? WHERE subject_kind = 'work_order_attempt' AND target_node_run_id = ? AND decision = 'retry' AND dispatch_state = 'dispatched' AND resolution = ?`, RetryDecisionResolutionRecovered, transition.OccurredAt, transition.NodeRunID.String, RetryDecisionResolutionPending)
	if err != nil {
		return fmt.Errorf("resolve recovered work order retry: %w", err)
	}
	return nil
}

// resolveFailedWorkOrderRetryTx closes the dispatched predecessor before the
// failed retry target schedules its successor or terminal policy decision.
func resolveFailedWorkOrderRetryTx(ctx context.Context, tx *sql.Tx, transition TransitionRequest) error {
	if transition.Entity != EntityTypeNode || transition.ToState != "failed" || !transition.NodeRunID.Valid {
		return nil
	}
	_, err := tx.ExecContext(ctx, `UPDATE retry_decisions SET resolution = ?, resolved_at = ? WHERE subject_kind = 'work_order_attempt' AND target_node_run_id = ? AND decision = 'retry' AND dispatch_state = 'dispatched' AND resolution = ?`, RetryDecisionResolutionFailed, transition.OccurredAt, transition.NodeRunID.String, RetryDecisionResolutionPending)
	if err != nil {
		return fmt.Errorf("resolve failed work order retry: %w", err)
	}
	return nil
}

// resolveCancelledWorkOrderRetryTx retains the decision audit record while making
// a cancelled target ineligible for automatic dispatch.
func resolveCancelledWorkOrderRetryTx(ctx context.Context, tx *sql.Tx, nodeRunID, occurredAt string) error {
	_, err := tx.ExecContext(ctx, `UPDATE retry_decisions
		SET dispatch_state = CASE WHEN dispatch_state = 'pending' THEN 'not_applicable' ELSE dispatch_state END,
		    dispatched_at = CASE WHEN dispatch_state = 'pending' THEN NULL ELSE dispatched_at END,
		    resolution = ?,
		    resolved_at = ?
		WHERE subject_kind = 'work_order_attempt'
		  AND target_node_run_id = ?
		  AND decision = 'retry'
		  AND resolution = ?`, RetryDecisionResolutionCancelled, occurredAt, strings.TrimSpace(nodeRunID), RetryDecisionResolutionPending)
	if err != nil {
		return fmt.Errorf("resolve cancelled work order retry: %w", err)
	}
	return nil
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
	if strings.TrimSpace(req.RunReasonCode) != "manual_retry_requested" && strings.TrimSpace(req.RunReasonCode) != "manual_rerun_requested" {
		return nil, fmt.Errorf("manual retry or rerun run reason code is required")
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
	if err := prepareRunWorktreeForTerminalReplayTx(ctx, tx, req.RunID); err != nil {
		return nil, err
	}

	reasonText := strings.TrimSpace(req.Reason)
	if reasonText == "" {
		reasonText = "manual replay requested"
	}

	anchorNodeRunID, err := latestNodeRunIDForRunNodeTx(ctx, tx, req.RunID, req.AnchorNodeID)
	if err != nil {
		return nil, err
	}
	runAttemptID := fmt.Sprintf("run:%s:%s:%s", req.RunID, req.AnchorNodeID, req.RunReasonCode)
	runEventSeq, err := nextEventSeqTx(ctx, tx, runAttemptID)
	if err != nil {
		return nil, err
	}
	trigger := "operator_retry"
	if strings.TrimSpace(req.RunReasonCode) == "manual_rerun_requested" {
		trigger = "operator_rerun"
	}
	runPayload, err := marshalRetryRecoveryEventPayload(retryRecoveryEventPayload{
		NodeID:          req.AnchorNodeID,
		NodeRunID:       anchorNodeRunID,
		RecoveryTrigger: trigger,
		RecoveryPhase:   "scheduled",
		ErrorCode:       req.RunReasonCode,
		RetryIndex:      0,
		RetryMax:        0,
		ScheduledAt:     occurredAt,
		Reason:          strings.TrimSpace(req.Reason),
	})
	if err != nil {
		return nil, fmt.Errorf("marshal manual recovery run event: %w", err)
	}
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
		sourceAttemptID, err := latestPhysicalAttemptIDForRunNodeTx(ctx, tx, req.RunID, nodeID)
		if err != nil {
			return nil, err
		}
		queuedAttempt, err := s.insertPendingNodeAttemptTx(ctx, tx, req.RunID, nodeID, queuedWaves[strings.TrimSpace(nodeID)], occurredAt, req.GenerateID)
		if err != nil {
			return nil, err
		}
		sourceNodeRunID := ""
		if sourceAttemptID != "" {
			if err := tx.QueryRowContext(ctx, `SELECT node_run_id FROM attempt_executions WHERE run_id = ? AND attempt_id = ?`, req.RunID, sourceAttemptID).Scan(&sourceNodeRunID); err != nil {
				return nil, fmt.Errorf("load manual recovery source lineage: %w", err)
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO node_attempt_recoveries
				(target_node_run_id, run_id, node_id, source_attempt_id, source_node_run_id, trigger, reason_code, scheduled_at)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, queuedAttempt.NodeRunID, req.RunID, nodeID, sourceAttemptID, sourceNodeRunID, trigger, strings.TrimSpace(req.RunReasonCode), occurredAt); err != nil {
				return nil, fmt.Errorf("persist manual recovery provenance: %w", err)
			}
		}
		queued = append(queued, queuedAttempt)
		if idx != 0 {
			continue
		}

		payload, err := marshalRetryRecoveryEventPayload(retryRecoveryEventPayload{
			NodeID:          queuedAttempt.NodeID,
			NodeRunID:       queuedAttempt.NodeRunID,
			RecoveryTrigger: trigger,
			RecoveryPhase:   "scheduled",
			ErrorCode:       req.RunReasonCode,
			SourceAttemptID: sourceAttemptID,
			SourceNodeRunID: sourceNodeRunID,
			TargetAttemptID: composeAttemptID(req.RunID, queuedAttempt.NodeID, queuedAttempt.Attempt),
			TargetNodeRunID: queuedAttempt.NodeRunID,
			RetryIndex:      0,
			RetryMax:        0,
			ScheduledAt:     occurredAt,
			Reason:          strings.TrimSpace(req.Reason),
		})
		if err != nil {
			return nil, fmt.Errorf("marshal manual recovery node event: %w", err)
		}
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
			ReasonCode:      req.RunReasonCode,
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

func prepareRunWorktreeForTerminalReplayTx(ctx context.Context, tx *sql.Tx, runID string) error {
	trimmedRunID := strings.TrimSpace(runID)
	_, err := tx.ExecContext(ctx, `
UPDATE run_worktrees
SET state = CASE WHEN cleanup_started = 0 THEN ? ELSE state END,
    cleanup_cursor = CASE WHEN cleanup_started = 0 THEN '' ELSE cleanup_cursor END,
    cleanup_owner = CASE WHEN cleanup_started = 0 THEN '' ELSE cleanup_owner END,
    cleanup_owner_pid = CASE WHEN cleanup_started = 0 THEN 0 ELSE cleanup_owner_pid END,
    cleanup_owner_identity_token = CASE WHEN cleanup_started = 0 THEN '' ELSE cleanup_owner_identity_token END,
    cleanup_fence = CASE WHEN cleanup_started = 0 THEN cleanup_fence + 1 ELSE cleanup_fence END,
    cleanup_started = CASE WHEN cleanup_started = 0 THEN 0 ELSE cleanup_started END,
    cleanup_after = CASE WHEN cleanup_started = 0 THEN NULL ELSE cleanup_after END,
    cleaned_at = CASE WHEN cleanup_started = 0 THEN NULL ELSE cleaned_at END,
    failure_reason = CASE WHEN cleanup_started = 0 THEN '' ELSE failure_reason END
WHERE run_id = ?
  AND state IN (?, ?)
  AND EXISTS (
    SELECT 1 FROM workflow_runs wr
    WHERE wr.run_id = run_worktrees.run_id
      AND wr.run_status IN ('failed', 'cancelled')
  )`,
		workspace.RunWorktreeStateActive,
		trimmedRunID,
		workspace.RunWorktreeStateCleanupPending,
		workspace.RunWorktreeStateCleanupFailed,
	)
	if err != nil {
		return fmt.Errorf("prepare managed run worktree for terminal replay: %w", err)
	}

	var cleanupState string
	var cleanupStarted int
	err = tx.QueryRowContext(ctx, `SELECT state, cleanup_started FROM run_worktrees WHERE run_id = ?`, trimmedRunID).Scan(&cleanupState, &cleanupStarted)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("load managed run worktree cleanup state for terminal replay: %w", err)
	}
	if (cleanupState == workspace.RunWorktreeStateCleanupPending || cleanupState == workspace.RunWorktreeStateCleanupFailed) && cleanupStarted != 0 {
		return &RunWorktreeCleanupInProgressError{RunID: trimmedRunID}
	}
	if cleanupState == workspace.RunWorktreeStateCleanupPending || cleanupState == workspace.RunWorktreeStateCleanupFailed {
		return fmt.Errorf("managed run worktree cleanup for run %q could not be cancelled", trimmedRunID)
	}
	return nil
}

func latestPhysicalAttemptIDForRunNodeTx(ctx context.Context, tx *sql.Tx, runID, nodeID string) (string, error) {
	var attemptID sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT ae.attempt_id
		FROM attempt_executions ae
		JOIN node_runs nr ON nr.node_run_id = ae.node_run_id AND nr.run_id = ae.run_id
		WHERE ae.run_id = ? AND nr.node_id = ?
		ORDER BY nr.attempt DESC, nr.node_run_id DESC
		LIMIT 1`, strings.TrimSpace(runID), strings.TrimSpace(nodeID)).Scan(&attemptID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("load prior physical attempt for manual recovery: %w", err)
	}
	return strings.TrimSpace(attemptID.String), nil
}

func latestNodeRunIDForRunNodeTx(ctx context.Context, tx *sql.Tx, runID, nodeID string) (string, error) {
	var nodeRunID string
	err := tx.QueryRowContext(ctx, `SELECT node_run_id
		FROM node_runs
		WHERE run_id = ? AND node_id = ?
		ORDER BY attempt DESC, node_run_id DESC
		LIMIT 1`, strings.TrimSpace(runID), strings.TrimSpace(nodeID)).Scan(&nodeRunID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("load manual recovery anchor node lineage: %w", err)
	}
	return strings.TrimSpace(nodeRunID), nil
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
	if err := s.activateFirstFinalReworkAttemptTx(ctx, tx, req.RunTransition, req.ApprovalID, queued[0], req.GenerateID); err != nil {
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

func (s *TransitionService) activateFirstFinalReworkAttemptTx(ctx context.Context, tx *sql.Tx, transition TransitionRequest, approvalID string, firstQueued QueuedNodeAttempt, generate func(string) string) error {
	payload, err := marshalFinalReworkQueuedEventPayload(transition.RunID, approvalID, firstQueued, transition.OccurredAt)
	if err != nil {
		return fmt.Errorf("marshal final rework queued payload: %w", err)
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
		EventType:       "attempt.final_rework_queued",
		EventSeq:        1,
		PayloadJSON:     payload,
	})
	return err
}

type finalReworkQueuedEventPayload struct {
	RunID            string `json:"run_id"`
	ApprovalID       string `json:"approval_id"`
	GovernanceRework string `json:"governance_rework"`
	NodeID           string `json:"node_id"`
	NodeRunID        string `json:"node_run_id"`
	AttemptID        string `json:"attempt_id"`
	Attempt          int    `json:"attempt"`
	AgentID          string `json:"agent_id"`
	RequiredRole     string `json:"required_role"`
	ExecutionMode    string `json:"execution_mode"`
	State            string `json:"state"`
	QueuedAt         string `json:"queued_at"`
}

func marshalFinalReworkQueuedEventPayload(runID, approvalID string, queued QueuedNodeAttempt, queuedAt string) (string, error) {
	raw, err := json.Marshal(finalReworkQueuedEventPayload{
		RunID:            strings.TrimSpace(runID),
		ApprovalID:       strings.TrimSpace(approvalID),
		GovernanceRework: "final",
		NodeID:           strings.TrimSpace(queued.NodeID),
		NodeRunID:        strings.TrimSpace(queued.NodeRunID),
		AttemptID:        composeAttemptID(runID, queued.NodeID, queued.Attempt),
		Attempt:          queued.Attempt,
		AgentID:          strings.TrimSpace(queued.AgentID),
		RequiredRole:     strings.TrimSpace(queued.RequiredRole),
		ExecutionMode:    string(workflow.BehaviorForNode(queued.NodeID, "").ExecutionOwnership),
		State:            "ready",
		QueuedAt:         strings.TrimSpace(queuedAt),
	})
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

type retryRecoveryEventPayload struct {
	NodeID          string `json:"node_id,omitempty"`
	NodeRunID       string `json:"node_run_id,omitempty"`
	RetryDecisionID string `json:"retry_decision_id,omitempty"`
	Decision        string `json:"decision,omitempty"`
	RecoveryTrigger string `json:"recovery_trigger"`
	RecoveryPhase   string `json:"recovery_phase"`
	ErrorCode       string `json:"error_code"`
	SourceAttemptID string `json:"source_attempt_id,omitempty"`
	SourceNodeRunID string `json:"source_node_run_id,omitempty"`
	TargetAttemptID string `json:"target_attempt_id,omitempty"`
	TargetNodeRunID string `json:"target_node_run_id,omitempty"`
	RetryIndex      int    `json:"retry_index"`
	RetryMax        int    `json:"retry_max"`
	ScheduledAt     string `json:"scheduled_at"`
	Reason          string `json:"reason,omitempty"`
}

func marshalRetryRecoveryEventPayload(payload retryRecoveryEventPayload) (string, error) {
	payload.NodeID = strings.TrimSpace(payload.NodeID)
	payload.NodeRunID = strings.TrimSpace(payload.NodeRunID)
	payload.RecoveryTrigger = strings.TrimSpace(payload.RecoveryTrigger)
	payload.RecoveryPhase = strings.TrimSpace(payload.RecoveryPhase)
	payload.ErrorCode = strings.TrimSpace(payload.ErrorCode)
	payload.SourceAttemptID = strings.TrimSpace(payload.SourceAttemptID)
	payload.SourceNodeRunID = strings.TrimSpace(payload.SourceNodeRunID)
	payload.TargetAttemptID = strings.TrimSpace(payload.TargetAttemptID)
	payload.TargetNodeRunID = strings.TrimSpace(payload.TargetNodeRunID)
	payload.ScheduledAt = strings.TrimSpace(payload.ScheduledAt)
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

// appendAutomaticRetryScheduledEventTx records the public scheduling boundary
// and the attempt audit projection together with the retry decision.
func (s *TransitionService) appendAutomaticRetryScheduledEventTx(ctx context.Context, tx *sql.Tx, source TransitionRequest, decision RetryDecision, eventID, payload string) error {
	seq, err := nextEventSeqTx(ctx, tx, decision.SourceAttemptID)
	if err != nil {
		return err
	}
	if err := appendAttemptEventTx(ctx, tx, s.eventSchemas, s.roleCommunicationPolicy, AttemptEventIngestionRequest{
		EventID:         strings.TrimSpace(eventID),
		RunID:           decision.RunID,
		NodeRunID:       sql.NullString{String: decision.NodeRunID, Valid: true},
		AttemptID:       decision.SourceAttemptID,
		SchemaVersion:   "v1",
		EventType:       "attempt.retry_scheduled",
		Seq:             seq,
		PayloadJSON:     payload,
		ActorID:         source.ActorID,
		RoleID:          source.RoleID,
		AuthoritySource: source.AuthoritySource,
		ReasonCode:      decision.ErrorCode,
		ReasonText:      decision.OperatorText,
		CorrelationID:   source.CorrelationID,
		CausationID:     "caus_retry_scheduled_" + decision.DecisionID,
		CreatedAt:       source.OccurredAt,
	}, attemptEventAppendOptions{mirrorToMailbox: true}); err != nil {
		return fmt.Errorf("append automatic work order retry scheduled event: %w", err)
	}
	return nil
}

func retryRecoveryPhase(decision RetryDecision) string {
	if decision.Decision == RetryDecisionExhausted || decision.Decision == RetryDecisionFailFast || decision.Resolution == RetryDecisionResolutionEscalated {
		return "exhausted"
	}
	if decision.Resolution == RetryDecisionResolutionRecovered {
		return "recovered"
	}
	return "scheduled"
}

func attemptIDForNodeRunTx(ctx context.Context, tx *sql.Tx, runID, nodeRunID string) (string, error) {
	if strings.TrimSpace(nodeRunID) == "" {
		return "", nil
	}
	var nodeID string
	var attempt int
	err := tx.QueryRowContext(ctx, `SELECT node_id, attempt FROM node_runs WHERE run_id = ? AND node_run_id = ?`, runID, nodeRunID).Scan(&nodeID, &attempt)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("load retry target attempt lineage: %w", err)
	}
	return composeAttemptID(runID, nodeID, attempt), nil
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
	if transition.FromState != "awaiting_plan_decision" {
		return fmt.Errorf("plan decision requires awaiting_plan_decision from_state")
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
	if s == nil || s.db == nil {
		return fmt.Errorf("transition service database is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	applyStart := func() error {
		return s.applyWithEffects(ctx, transition, func(ctx context.Context, tx *sql.Tx) error {
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
	}
	var (
		writeErr     error
		writeErrs    []error
		readbackErrs []error
	)
	for attempt := 0; attempt < 2; attempt++ {
		writeErr = applyStart()
		if writeErr == nil {
			return nil
		}
		writeErrs = append(writeErrs, writeErr)
		committed, readbackErr := s.confirmExecutionStartCommit(ctx, transition)
		if committed {
			return nil
		}
		if readbackErr == nil {
			return writeErr
		}
		readbackErrs = append(readbackErrs, readbackErr)
	}
	return &ExecutionStartOutcomeUnknownError{
		AttemptID: strings.TrimSpace(transition.AttemptID),
		Cause:     errors.Join(errors.Join(writeErrs...), errors.Join(readbackErrs...)),
	}
}

func (s *TransitionService) confirmExecutionStartCommit(ctx context.Context, transition TransitionRequest) (bool, error) {
	readbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), executionStartReadbackTimeout)
	defer cancel()

	var (
		storedTransitionRunID, storedTransitionEntity, storedTransitionFrom, storedTransitionTo            string
		storedTransitionActorID, storedTransitionRoleID, storedTransitionAuthority                         string
		storedTransitionReasonCode, storedTransitionReasonText, storedTransitionCorrelationID              string
		storedTransitionCausationID, storedTransitionCreatedAt                                             string
		storedTransitionNodeRunID                                                                          sql.NullString
		storedEventRunID, storedEventAttemptID, storedEventType, storedEventPayload, storedEventCreatedAt  string
		storedEventSchemaVersion, storedEventActorID, storedEventRoleID                                    string
		storedEventAuthority, storedEventReasonCode, storedEventReasonText                                 string
		storedEventCorrelationID, storedEventCausationID                                                   string
		storedEventNodeRunID                                                                               sql.NullString
		storedEventSeq                                                                                     int
		storedAttemptEventID, storedAttemptEventRunID, storedAttemptEventNodeRunID, storedAttemptEventType string
		storedAttemptEventReasonCode, storedAttemptEventPayload, storedAttemptEventCreatedAt               string
		storedAttemptEventSeq                                                                              int
		storedExecutionState, storedLaunchStartedAt, storedStartedAt                                       string
		storedNodeStatus                                                                                   string
	)
	transitionErr := s.db.QueryRowContext(readbackCtx, `SELECT run_id, node_run_id, entity_type, from_state, to_state, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, created_at FROM transitions WHERE transition_id = ?`, strings.TrimSpace(transition.TransitionID)).Scan(
		&storedTransitionRunID, &storedTransitionNodeRunID, &storedTransitionEntity, &storedTransitionFrom, &storedTransitionTo,
		&storedTransitionActorID, &storedTransitionRoleID, &storedTransitionAuthority, &storedTransitionReasonCode, &storedTransitionReasonText,
		&storedTransitionCorrelationID, &storedTransitionCausationID, &storedTransitionCreatedAt,
	)
	eventErr := s.db.QueryRowContext(readbackCtx, `SELECT run_id, node_run_id, attempt_id, schema_version, event_type, seq, payload_json, actor_id, role_id, authority_source, reason_code, reason_text, correlation_id, causation_id, created_at FROM events WHERE event_id = ?`, strings.TrimSpace(transition.EventID)).Scan(
		&storedEventRunID, &storedEventNodeRunID, &storedEventAttemptID, &storedEventSchemaVersion, &storedEventType, &storedEventSeq, &storedEventPayload,
		&storedEventActorID, &storedEventRoleID, &storedEventAuthority, &storedEventReasonCode, &storedEventReasonText,
		&storedEventCorrelationID, &storedEventCausationID, &storedEventCreatedAt,
	)
	attemptEventID := fmt.Sprintf("attempt_event:%s:%d", strings.TrimSpace(transition.AttemptID), transition.EventSeq)
	attemptEventErr := s.db.QueryRowContext(readbackCtx, `SELECT attempt_event_id, run_id, node_run_id, seq, event_type, reason_code, payload_json, created_at FROM attempt_event_log WHERE attempt_event_id = ?`, attemptEventID).Scan(
		&storedAttemptEventID, &storedAttemptEventRunID, &storedAttemptEventNodeRunID, &storedAttemptEventSeq, &storedAttemptEventType, &storedAttemptEventReasonCode, &storedAttemptEventPayload, &storedAttemptEventCreatedAt,
	)
	var executionErr error
	var storedExecutionNodeRunID, storedExecutionRunID string
	executionErr = s.db.QueryRowContext(readbackCtx, `SELECT run_id, node_run_id, state, COALESCE(launch_started_at, ''), COALESCE(started_at, '') FROM attempt_executions WHERE attempt_id = ?`, strings.TrimSpace(transition.AttemptID)).Scan(
		&storedExecutionRunID, &storedExecutionNodeRunID, &storedExecutionState, &storedLaunchStartedAt, &storedStartedAt,
	)
	nodeErr := s.db.QueryRowContext(readbackCtx, `SELECT node_status FROM node_runs WHERE run_id = ? AND node_run_id = ?`, strings.TrimSpace(transition.RunID), strings.TrimSpace(transition.NodeRunID.String)).Scan(&storedNodeStatus)

	for name, err := range map[string]error{
		"transition":    transitionErr,
		"event":         eventErr,
		"attempt event": attemptEventErr,
		"attempt state": executionErr,
		"node state":    nodeErr,
	} {
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return false, fmt.Errorf("read back execution start %s: %w", name, err)
		}
	}

	transitionPresent := transitionErr == nil
	eventPresent := eventErr == nil
	attemptEventPresent := attemptEventErr == nil
	if !transitionPresent && !eventPresent && !attemptEventPresent && executionErr == nil &&
		storedExecutionRunID == strings.TrimSpace(transition.RunID) && storedExecutionNodeRunID == strings.TrimSpace(transition.NodeRunID.String) &&
		storedExecutionState == "queued" && storedLaunchStartedAt == "" && storedStartedAt == "" &&
		nodeErr == nil && storedNodeStatus == "ready" {
		return false, nil
	}

	transitionMatches := transitionPresent &&
		storedTransitionRunID == strings.TrimSpace(transition.RunID) &&
		storedTransitionNodeRunID.Valid == transition.NodeRunID.Valid &&
		storedTransitionNodeRunID.String == strings.TrimSpace(transition.NodeRunID.String) &&
		storedTransitionEntity == string(transition.Entity) &&
		storedTransitionFrom == strings.TrimSpace(transition.FromState) &&
		storedTransitionTo == strings.TrimSpace(transition.ToState) &&
		storedTransitionActorID == strings.TrimSpace(transition.ActorID) &&
		storedTransitionRoleID == strings.TrimSpace(transition.RoleID) &&
		storedTransitionAuthority == strings.TrimSpace(transition.AuthoritySource) &&
		storedTransitionReasonCode == strings.TrimSpace(transition.ReasonCode) &&
		storedTransitionReasonText == strings.TrimSpace(transition.ReasonText) &&
		storedTransitionCorrelationID == strings.TrimSpace(transition.CorrelationID) &&
		storedTransitionCausationID == strings.TrimSpace(transition.CausationID) &&
		storedTransitionCreatedAt == strings.TrimSpace(transition.OccurredAt)
	eventMatches := eventPresent &&
		storedEventRunID == strings.TrimSpace(transition.RunID) &&
		storedEventNodeRunID.Valid == transition.NodeRunID.Valid &&
		storedEventNodeRunID.String == strings.TrimSpace(transition.NodeRunID.String) &&
		storedEventAttemptID == strings.TrimSpace(transition.AttemptID) &&
		storedEventSchemaVersion == strings.TrimSpace(transition.SchemaVersion) &&
		storedEventType == strings.TrimSpace(transition.EventType) &&
		storedEventSeq == transition.EventSeq &&
		(transition.EventType == "attempt.started" || storedEventPayload == strings.TrimSpace(transition.PayloadJSON)) &&
		storedEventActorID == strings.TrimSpace(transition.ActorID) &&
		storedEventRoleID == strings.TrimSpace(transition.RoleID) &&
		storedEventAuthority == strings.TrimSpace(transition.AuthoritySource) &&
		storedEventReasonCode == strings.TrimSpace(transition.ReasonCode) &&
		storedEventReasonText == strings.TrimSpace(transition.ReasonText) &&
		storedEventCorrelationID == strings.TrimSpace(transition.CorrelationID) &&
		storedEventCausationID == strings.TrimSpace(transition.CausationID) &&
		storedEventCreatedAt == strings.TrimSpace(transition.OccurredAt)
	attemptEventMatches := attemptEventPresent &&
		storedAttemptEventID == attemptEventID &&
		storedAttemptEventRunID == strings.TrimSpace(transition.RunID) &&
		storedAttemptEventNodeRunID == strings.TrimSpace(transition.NodeRunID.String) &&
		storedAttemptEventSeq == transition.EventSeq &&
		storedAttemptEventType == strings.TrimSpace(transition.EventType) &&
		storedAttemptEventReasonCode == strings.TrimSpace(transition.ReasonCode) &&
		storedAttemptEventPayload == strings.TrimSpace(transition.PayloadJSON) &&
		storedAttemptEventCreatedAt == strings.TrimSpace(transition.OccurredAt)
	if !transitionMatches || !eventMatches || !attemptEventMatches || executionErr != nil || nodeErr != nil ||
		storedExecutionRunID != strings.TrimSpace(transition.RunID) || storedExecutionNodeRunID != strings.TrimSpace(transition.NodeRunID.String) {
		return false, fmt.Errorf("durable execution start records are incomplete or do not match transition %q", strings.TrimSpace(transition.TransitionID))
	}

	switch strings.TrimSpace(transition.ToState) {
	case "starting":
		if storedExecutionState != "starting" || storedLaunchStartedAt != strings.TrimSpace(transition.OccurredAt) || storedStartedAt != "" || storedNodeStatus != "starting" {
			return false, fmt.Errorf("durable provider execution start does not match transition %q", strings.TrimSpace(transition.TransitionID))
		}
	case "running":
		if storedExecutionState != "running" || storedStartedAt != strings.TrimSpace(transition.OccurredAt) || storedNodeStatus != "running" {
			return false, fmt.Errorf("durable runtime-owned execution start does not match transition %q", strings.TrimSpace(transition.TransitionID))
		}
	default:
		return false, fmt.Errorf("unsupported execution start state %q during commit readback", strings.TrimSpace(transition.ToState))
	}
	return true, nil
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
		if req.StaleAttempt != nil {
			current, err := loadLiveNodeAttemptForStaleRetryTx(ctx, tx, transition.AttemptID, *req.StaleAttempt)
			if err != nil {
				return err
			}
			if isDurablyTerminalAttemptExecutionState(current.ExecutionState) {
				return fmt.Errorf("stale terminal transition lost attempt authority for %q", current.AttemptID)
			}
			if err := lockStaleAttemptExecutionLeaseTx(ctx, tx, *req.StaleAttempt, current, transition.OccurredAt); err != nil {
				return err
			}
			if err := ensureProviderTerminalHandoffNotPendingTx(ctx, tx, current); err != nil {
				return err
			}
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

func (s *TransitionService) RecordDirectExecutionTerminalWithRetry(ctx context.Context, req DirectExecutionTerminalWithRetryRequest) error {
	transition := req.Transition
	if err := validateDirectExecutionTerminalTransition(transition); err != nil {
		return err
	}
	if transition.ToState != "failed" || (req.Decision.Decision != RetryDecisionFailFast && req.Decision.Decision != RetryDecisionExhausted) || req.Decision.SubjectKind != RetrySubjectKindWorkOrderAttempt || strings.TrimSpace(req.Decision.SourceAttemptID) != strings.TrimSpace(transition.AttemptID) {
		return fmt.Errorf("direct execution retry disposition requires a matching failed work order attempt")
	}
	_, err := sqlitex.Retry(ctx, func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin direct execution retry disposition transaction: %w", err)
		}
		committed := false
		defer func() {
			if !committed {
				_ = tx.Rollback()
			}
		}()
		stale := staleAttempt{
			NodeRunID:     transition.NodeRunID.String,
			RunID:         transition.RunID,
			NodeID:        req.Decision.NodeID,
			RequiredRole:  transition.RoleID,
			AgentID:       req.EscalationFromAgentID,
			CurrentStatus: transition.FromState,
			StartedAt:     transition.OccurredAt,
		}
		if req.StaleAttempt != nil {
			stale = *req.StaleAttempt
		}
		current, err := loadLiveNodeAttemptForStaleRetryTx(ctx, tx, transition.AttemptID, stale)
		if err != nil {
			return err
		}
		if isDurablyTerminalAttemptExecutionState(current.ExecutionState) {
			return fmt.Errorf("direct execution retry disposition lost terminal authority for attempt %q", current.AttemptID)
		}
		if req.StaleAttempt != nil {
			if err := lockStaleAttemptExecutionLeaseTx(ctx, tx, *req.StaleAttempt, current, transition.OccurredAt); err != nil {
				return err
			}
		}
		if err := supersedeStaleProviderInvocationTx(ctx, tx, current, transition.OccurredAt); err != nil {
			return err
		}
		if err := closeAttemptSessionLeaseTx(ctx, tx, current.SessionID, transition.OccurredAt); err != nil {
			return err
		}
		if err := queueAttemptCancelOutboxForReasonTx(ctx, tx, current, "agent_unresponsive", staleAttemptFailureReasonText, transition.OccurredAt, nil); err != nil {
			return err
		}
		if err := s.applyTx(ctx, tx, transition); err != nil {
			return err
		}
		if err := resolveFailedWorkOrderRetryTx(ctx, tx, transition); err != nil {
			return err
		}
		if err := insertAttemptExecutionEventTx(ctx, tx, transition); err != nil {
			return err
		}
		if err := markAttemptExecutionTerminalTx(ctx, tx, transition.AttemptID, transition.RunID, transition.NodeRunID.String, transition.ToState, transition.ReasonCode, transition.ReasonText, transition.OccurredAt); err != nil {
			return err
		}
		decision, err := insertRetryDecisionTx(ctx, tx, req.Decision)
		if err != nil {
			return err
		}
		targetAttemptID, err := attemptIDForNodeRunTx(ctx, tx, decision.RunID, decision.TargetNodeRunID)
		if err != nil {
			return err
		}
		payload, err := marshalRetryRecoveryEventPayload(retryRecoveryEventPayload{
			NodeID:          decision.NodeID,
			NodeRunID:       decision.NodeRunID,
			RetryDecisionID: decision.DecisionID,
			Decision:        string(decision.Decision),
			RecoveryTrigger: "automatic_retry",
			RecoveryPhase:   retryRecoveryPhase(decision),
			ErrorCode:       decision.ErrorCode,
			SourceAttemptID: decision.SourceAttemptID,
			SourceNodeRunID: decision.NodeRunID,
			TargetAttemptID: targetAttemptID,
			TargetNodeRunID: decision.TargetNodeRunID,
			RetryIndex:      decision.RetryIndex,
			RetryMax:        decision.MaxRetries,
			ScheduledAt:     retryTimeString(decision.DueAt),
		})
		if err != nil {
			return fmt.Errorf("marshal direct execution retry event: %w", err)
		}
		if decision.Decision == RetryDecisionExhausted {
			seq, err := nextAttemptExecutionSeqTx(ctx, tx, decision.SourceAttemptID)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO attempt_event_log (attempt_event_id, attempt_id, run_id, node_run_id, seq, event_type, reason_code, payload_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, fmt.Sprintf("attempt_event:%s:%d", decision.SourceAttemptID, seq), decision.SourceAttemptID, decision.RunID, decision.NodeRunID, seq, "escalation.raised", decision.ErrorCode, payload, transition.OccurredAt); err != nil {
				return fmt.Errorf("append direct execution retry disposition event: %w", err)
			}
		}
		if decision.Decision == RetryDecisionExhausted {
			if err := s.deliverWorkOrderRetryExhaustionTx(ctx, tx, decision, req.EscalationFromAgentID); err != nil {
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit direct execution retry disposition transaction: %w", err)
		}
		committed = true
		return nil
	}, sqlitex.Options{})
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
	_, err := sqlitex.Retry(ctx, func() error {
		current, err := s.projectNodeTerminalFromDurableAttemptOnce(ctx, req)
		if err == nil {
			projected = current
		}
		return err
	}, sqlitex.Options{})
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
		if ok && projection.NodeState == "cancelled" {
			if err := resolveCancelledWorkOrderRetryTx(ctx, tx, attempt.NodeRunID, req.OccurredAt); err != nil {
				return false, err
			}
		}
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
	if projection.NodeState == "cancelled" {
		if err := resolveCancelledWorkOrderRetryTx(ctx, tx, attempt.NodeRunID, req.OccurredAt); err != nil {
			return false, err
		}
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
		if candidate.stage == attemptSupersessionStagePreLaunch {
			if err := resolveCancelledWorkOrderRetryTx(ctx, tx, attempt.NodeRunID, occurredAt); err != nil {
				return result, err
			}
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

	if err := s.terminalizeRunAndCancelLiveNodesTx(ctx, tx, req); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit run terminalization transaction: %w", err)
	}
	committed = true
	return nil
}

func (s *TransitionService) terminalizeRunAndCancelLiveNodesTx(ctx context.Context, tx *sql.Tx, req RunTerminalizationRequest) error {
	transition := req.Transition
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
			var nodeStatus string
			if err := tx.QueryRowContext(ctx, `SELECT node_status FROM node_runs WHERE node_run_id = ?`, attempt.NodeRunID).Scan(&nodeStatus); err != nil {
				return err
			}
			if nodeStatus == "cancelled" {
				if err := resolveCancelledWorkOrderRetryTx(ctx, tx, attempt.NodeRunID, transition.OccurredAt); err != nil {
					return err
				}
			}
			continue
		}
		if err := s.cancelLiveNodeAttemptTx(ctx, tx, transition, attempt, req.GenerateID); err != nil {
			return err
		}
		if err := resolveCancelledWorkOrderRetryTx(ctx, tx, attempt.NodeRunID, transition.OccurredAt); err != nil {
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
	return nil
}

func (s *TransitionService) ScheduleFencedStaleRetry(ctx context.Context, req ScheduleFencedStaleRetryRequest) (QueuedNodeAttempt, error) {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		var queued QueuedNodeAttempt
		_, err := sqlitex.Retry(ctx, func() error {
			var retryErr error
			queued, retryErr = s.scheduleFencedStaleRetryOnce(ctx, req)
			return retryErr
		}, sqlitex.Options{})
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
	ORDER BY nr.node_id ASC, nr.attempt ASC, nr.node_run_id ASC`, nodeattempt.LiveNodeAttemptStatusConditionSQL("nr"))
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
	LIMIT 1`, placeholders, nodeattempt.LiveNodeAttemptStatusConditionSQL("node_runs"))
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
		`INSERT OR IGNORE INTO attempt_cancel_outbox (cancel_outbox_id, attempt_id, run_id, node_run_id, session_id, reason_code, reason_text, requested_at, status, handoff_state, publish_attempts, last_error, created_at, updated_at, published_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', 'cancellation_requested', 0, '', ?, ?, NULL)`,
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

// RequestAttemptStopRetry records one operator-authorized retry of a paused
// stale-attempt stop handoff. The cancellation outbox remains the only gate
// between the stale attempt and its replacement.
func (s *TransitionService) RequestAttemptStopRetry(ctx context.Context, req AttemptStopRetryRequest) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("transition service database is required")
	}
	req.RunID = strings.TrimSpace(req.RunID)
	req.AttemptID = strings.TrimSpace(req.AttemptID)
	req.ActorID = strings.TrimSpace(req.ActorID)
	req.RoleID = strings.TrimSpace(req.RoleID)
	req.OccurredAt = strings.TrimSpace(req.OccurredAt)
	if req.RunID == "" || req.AttemptID == "" || req.ActorID == "" || req.RoleID == "" {
		return fmt.Errorf("run id, attempt id, actor id, and role id are required")
	}
	if req.OccurredAt == "" {
		req.OccurredAt = time.Now().UTC().Format(time.RFC3339)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin attempt stop retry transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	var cancelOutboxID, nodeRunID, status, handoffState, nodeStatus, executionState, reasonCode string
	err = tx.QueryRowContext(ctx, `SELECT aco.cancel_outbox_id, aco.node_run_id, aco.status, aco.handoff_state,
		nr.node_status, ae.state, aco.reason_code
		FROM attempt_cancel_outbox aco
		JOIN node_runs nr ON nr.node_run_id = aco.node_run_id AND nr.run_id = aco.run_id
		JOIN attempt_executions ae ON ae.attempt_id = aco.attempt_id AND ae.run_id = aco.run_id AND ae.node_run_id = aco.node_run_id
		WHERE aco.run_id = ? AND aco.attempt_id = ?`, req.RunID, req.AttemptID).Scan(
		&cancelOutboxID,
		&nodeRunID,
		&status,
		&handoffState,
		&nodeStatus,
		&executionState,
		&reasonCode,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return &AttemptStopRetryUnavailableError{RunID: req.RunID, AttemptID: req.AttemptID, Reason: "no pending cancellation handoff exists"}
	}
	if err != nil {
		return fmt.Errorf("load attempt stop retry handoff: %w", err)
	}
	if status != "pending" || (handoffState != "awaiting_quiescence" && handoffState != "cancellation_dispatched" && handoffState != "stop_retry_dispatched" && handoffState != "guardian_evidence_lost") || nodeStatus != "blocked" || executionState != "committed_cancelled" || reasonCode != "agent_unresponsive" {
		return &AttemptStopRetryUnavailableError{RunID: req.RunID, AttemptID: req.AttemptID, Reason: "attempt is not paused for stale-process recovery"}
	}

	var addressableGuards int
	guardCondition := `lifecycle = 'acknowledged'`
	if handoffState == "guardian_evidence_lost" {
		var unresolved, unquarantined int
		if err := tx.QueryRowContext(ctx, `SELECT
			COALESCE(SUM(CASE WHEN lifecycle <> 'quiescent' THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN lifecycle <> 'quiescent' AND quiescence_evidence_lost_at IS NULL THEN 1 ELSE 0 END), 0)
			FROM provider_process_guards
			WHERE owner_kind = 'work_order_attempt' AND attempt_id = ?`, req.AttemptID).Scan(&unresolved, &unquarantined); err != nil {
			return fmt.Errorf("check guardian evidence recovery quarantine: %w", err)
		}
		if unresolved == 0 || unquarantined != 0 {
			return &AttemptStopRetryUnavailableError{RunID: req.RunID, AttemptID: req.AttemptID, Reason: "the prior provider process absence is not fully quarantined"}
		}
		guardCondition += ` AND quiescence_evidence_lost_at IS NOT NULL`
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(1)
		FROM provider_process_guards
		WHERE owner_kind = 'work_order_attempt' AND attempt_id = ? AND `+guardCondition, req.AttemptID).Scan(&addressableGuards); err != nil {
		return fmt.Errorf("check attempt stop retry guardian: %w", err)
	}
	if addressableGuards == 0 {
		return &AttemptStopRetryUnavailableError{RunID: req.RunID, AttemptID: req.AttemptID, Reason: "the prior provider process is not addressable"}
	}

	nextHandoffState := "stop_retry_requested"
	eventType := "attempt.stop_retry_requested"
	eventReasonCode := "agent_unresponsive"
	if handoffState == "guardian_evidence_lost" {
		nextHandoffState = "guardian_evidence_recovery_requested"
		eventType = "attempt.guardian_evidence_recovery_requested"
		eventReasonCode = "guardian_evidence_lost"
	}
	result, err := tx.ExecContext(ctx, `UPDATE attempt_cancel_outbox
		SET handoff_state = ?, last_error = '', updated_at = ?
		WHERE cancel_outbox_id = ? AND status = 'pending'
		  AND handoff_state IN ('awaiting_quiescence', 'cancellation_dispatched', 'stop_retry_dispatched', 'guardian_evidence_lost')`, nextHandoffState, req.OccurredAt, cancelOutboxID)
	if err != nil {
		return fmt.Errorf("request attempt stop retry: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read attempt stop retry rows affected: %w", err)
	}
	if rows != 1 {
		return &AttemptStopRetryUnavailableError{RunID: req.RunID, AttemptID: req.AttemptID, Reason: "the paused handoff changed"}
	}
	seq, err := nextAttemptExecutionSeqTx(ctx, tx, req.AttemptID)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(map[string]string{
		"cancel_outbox_id": cancelOutboxID,
		"requested_by":     req.ActorID,
	})
	if err != nil {
		return fmt.Errorf("marshal attempt stop retry event: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO attempt_event_log (attempt_event_id, attempt_id, run_id, node_run_id, seq, event_type, reason_code, payload_json, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		generateID(req.GenerateID, "attempt_event"), req.AttemptID, req.RunID, nodeRunID, seq, eventType, eventReasonCode, string(payload), req.OccurredAt); err != nil {
		return fmt.Errorf("append attempt stop retry event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit attempt stop retry transaction: %w", err)
	}
	committed = true
	return nil
}

// ClaimAttemptCancelHandoffDispatch records the point after which a cancellation
// request may perform its external provider stop. A crash after this claim leaves
// the handoff paused instead of replaying an unbounded external side effect.
func (s *TransitionService) ClaimAttemptCancelHandoffDispatch(ctx context.Context, cancelOutboxID, occurredAt string) (bool, error) {
	if s == nil || s.db == nil {
		return false, fmt.Errorf("transition service database is required")
	}
	cancelOutboxID = strings.TrimSpace(cancelOutboxID)
	occurredAt = strings.TrimSpace(occurredAt)
	if cancelOutboxID == "" {
		return false, fmt.Errorf("cancel outbox id is required")
	}
	if occurredAt == "" {
		occurredAt = time.Now().UTC().Format(time.RFC3339)
	}
	result, err := s.db.ExecContext(ctx, `UPDATE attempt_cancel_outbox
		SET handoff_state = CASE handoff_state
			WHEN 'cancellation_requested' THEN 'cancellation_dispatched'
			WHEN 'stop_retry_requested' THEN 'stop_retry_dispatched'
		END,
		updated_at = ?
		WHERE cancel_outbox_id = ? AND status = 'pending'
		  AND handoff_state IN ('cancellation_requested', 'stop_retry_requested')`, occurredAt, cancelOutboxID)
	if err != nil {
		return false, fmt.Errorf("claim attempt cancellation handoff dispatch: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read attempt cancellation dispatch claim rows affected: %w", err)
	}
	return rows == 1, nil
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
	if err := lockStaleAttemptExecutionLeaseTx(ctx, tx, attempt, current, occurredAt); err != nil {
		return QueuedNodeAttempt{}, err
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
		EventType:       "node.state_changed",
		EventSeq:        req.CurrentEventSeq,
		PayloadJSON:     blockedPayload,
	}); err != nil {
		return QueuedNodeAttempt{}, err
	}
	if err := fenceStaleAttemptExecutionTx(ctx, tx, current, occurredAt); err != nil {
		return QueuedNodeAttempt{}, err
	}
	if err := resolveFailedWorkOrderRetryTx(ctx, tx, TransitionRequest{Entity: EntityTypeNode, ToState: "failed", NodeRunID: sql.NullString{String: current.NodeRunID, Valid: true}, OccurredAt: occurredAt}); err != nil {
		return QueuedNodeAttempt{}, err
	}
	if err := closeAttemptSessionLeaseTx(ctx, tx, current.SessionID, occurredAt); err != nil {
		return QueuedNodeAttempt{}, err
	}
	if err := queueAttemptCancelOutboxForReasonTx(ctx, tx, current, "agent_unresponsive", staleAttemptFailureReasonText, occurredAt, nil); err != nil {
		return QueuedNodeAttempt{}, err
	}
	decisionReq := req.RetryDecision

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
	retryDecisionID := ""
	retryIndex, retryMax := 0, 0
	scheduledAt := occurredAt
	if decisionReq != nil {
		decision := *decisionReq
		decision.TargetNodeRunID = retryNodeRunID
		persistedDecision, err := insertRetryDecisionTx(ctx, tx, decision)
		if err != nil {
			return QueuedNodeAttempt{}, err
		}
		retryDecisionID = persistedDecision.DecisionID
		retryIndex, retryMax = persistedDecision.RetryIndex, persistedDecision.MaxRetries
		scheduledAt = retryTimeString(persistedDecision.DueAt)
	}
	retryPayload, err := marshalRetryRecoveryEventPayload(retryRecoveryEventPayload{
		NodeID:          attempt.NodeID,
		NodeRunID:       current.NodeRunID,
		RetryDecisionID: retryDecisionID,
		Decision:        string(RetryDecisionRetry),
		RecoveryTrigger: "automatic_retry",
		RecoveryPhase:   "scheduled",
		ErrorCode:       "agent_unresponsive",
		SourceAttemptID: current.AttemptID,
		SourceNodeRunID: current.NodeRunID,
		TargetAttemptID: composeAttemptID(attempt.RunID, queuedAttempt.NodeID, queuedAttempt.Attempt),
		TargetNodeRunID: queuedAttempt.NodeRunID,
		RetryIndex:      retryIndex,
		RetryMax:        retryMax,
		ScheduledAt:     scheduledAt,
	})
	if err != nil {
		return QueuedNodeAttempt{}, fmt.Errorf("marshal fenced stale retry payload: %w", err)
	}
	eventID := fmt.Sprintf("evt_retry_scheduled_%s", queuedAttempt.NodeRunID)
	if retryDecisionID != "" {
		eventID = fmt.Sprintf("evt_retry_scheduled_%s", retryDecisionID)
	}
	if err := s.appendAutomaticRetryScheduledEventTx(ctx, tx, TransitionRequest{
		ActorID: "reconciler", RoleID: attempt.RequiredRole, AuthoritySource: "system.reconciler",
		CorrelationID: fmt.Sprintf("corr_reconcile_%s", attempt.RunID), OccurredAt: occurredAt,
	}, RetryDecision{
		DecisionID: retryDecisionID, RunID: current.RunID, NodeID: current.NodeID, NodeRunID: current.NodeRunID,
		SourceAttemptID: current.AttemptID, ErrorCode: "agent_unresponsive", OperatorText: "stale attempt scheduled for automatic retry",
	}, eventID, retryPayload); err != nil {
		return QueuedNodeAttempt{}, err
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
	var successor struct {
		QueuedNodeAttempt
		HandoffSourceAttemptID string
	}
	err := tx.QueryRowContext(ctx, `SELECT node_id, node_run_id, attempt, wave, required_role, agent_id, handoff_source_attempt_id
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
		&successor.HandoffSourceAttemptID,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("load fenced stale retry successor: %w", err)
	}
	var pendingDecision int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(1) FROM retry_decisions WHERE target_node_run_id = ? AND decision = 'retry' AND dispatch_state = 'pending'`, successor.NodeRunID).Scan(&pendingDecision); err != nil {
		return fmt.Errorf("load stale retry due barrier: %w", err)
	}
	if pendingDecision > 0 {
		return nil
	}
	var runID string
	if err := tx.QueryRowContext(ctx, `SELECT run_id FROM node_runs WHERE node_run_id = ?`, successor.NodeRunID).Scan(&runID); err != nil {
		return fmt.Errorf("load fenced stale retry run: %w", err)
	}
	var sourceNodeRunID string
	if err := tx.QueryRowContext(ctx, `SELECT node_run_id FROM attempt_executions WHERE run_id = ? AND attempt_id = ?`, runID, successor.HandoffSourceAttemptID).Scan(&sourceNodeRunID); err != nil {
		return fmt.Errorf("load released stale retry source lineage: %w", err)
	}
	payload, err := marshalRetryRecoveryEventPayload(retryRecoveryEventPayload{
		NodeID:          successor.NodeID,
		NodeRunID:       successor.NodeRunID,
		RecoveryTrigger: "automatic_retry",
		RecoveryPhase:   "scheduled",
		ErrorCode:       "agent_unresponsive",
		SourceAttemptID: successor.HandoffSourceAttemptID,
		SourceNodeRunID: sourceNodeRunID,
		TargetAttemptID: composeAttemptID(runID, successor.NodeID, successor.Attempt),
		TargetNodeRunID: successor.NodeRunID,
		RetryIndex:      0,
		RetryMax:        0,
		ScheduledAt:     occurredAt,
	})
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
	err := tx.QueryRowContext(ctx, `SELECT state, execution_mode, session_ref, COALESCE(lease_owner, ''), lease_version, COALESCE(lease_expires_at, ''), COALESCE(launch_started_at, ''), COALESCE(started_at, ''), COALESCE(provider_session_id, ''), COALESCE(active_provider_invocation_id, '')
		FROM attempt_executions
		WHERE attempt_id = ? AND run_id = ? AND node_run_id = ?`, current.AttemptID, current.RunID, current.NodeRunID).Scan(
		&current.ExecutionState,
		&executionMode,
		&current.SessionID,
		&current.LeaseOwner,
		&current.LeaseVersion,
		&current.LeaseExpiresAt,
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

// lockStaleAttemptExecutionLeaseTx validates the candidate's exact lease
// generation and expiry, then performs a compare-and-swap write to hold SQLite's
// writer lock through the stale transition. A renewal between candidate lookup
// and this transaction therefore cannot be revoked by stale recovery.
func lockStaleAttemptExecutionLeaseTx(ctx context.Context, tx *sql.Tx, candidate staleAttempt, current liveNodeAttempt, occurredAt string) error {
	if current.ExecutionMode != executionstore.ExecutionModeProviderBacked {
		if candidate.ExecutionMode == executionstore.ExecutionModeProviderBacked {
			return fmt.Errorf("%w: provider-backed candidate now has execution mode %q", errStaleAttemptExecutionLeaseChanged, current.ExecutionMode)
		}
		return nil
	}
	expectedLeaseOwner := candidate.LeaseOwner
	expectedLeaseVersion := candidate.LeaseVersion
	expectedLeaseExpiresAt := candidate.LeaseExpiresAt
	if candidate.ExecutionMode != executionstore.ExecutionModeProviderBacked {
		// Some recovery callers can construct a stale candidate without an
		// attempt-execution row in the discovery query. Permit that only when
		// the authoritative row currently has no active lease to revoke.
		if candidate.LeaseOwner != "" || candidate.LeaseVersion != 0 || candidate.LeaseExpiresAt != "" || current.LeaseOwner != "" || current.LeaseExpiresAt != "" {
			return fmt.Errorf("%w: provider lease snapshot is unavailable", errStaleAttemptExecutionLeaseChanged)
		}
		expectedLeaseOwner = current.LeaseOwner
		expectedLeaseVersion = current.LeaseVersion
		expectedLeaseExpiresAt = current.LeaseExpiresAt
	} else if current.LeaseOwner != candidate.LeaseOwner || current.LeaseVersion != candidate.LeaseVersion || current.LeaseExpiresAt != candidate.LeaseExpiresAt {
		return fmt.Errorf("%w: candidate mode=%q owner=%q version=%d expiry=%q; current mode=%q owner=%q version=%d expiry=%q",
			errStaleAttemptExecutionLeaseChanged,
			candidate.ExecutionMode, candidate.LeaseOwner, candidate.LeaseVersion, candidate.LeaseExpiresAt,
			current.ExecutionMode, current.LeaseOwner, current.LeaseVersion, current.LeaseExpiresAt,
		)
	}
	if current.LeaseOwner != "" {
		if current.LeaseExpiresAt == "" {
			return fmt.Errorf("provider execution lease expiry is missing for stale attempt %q", current.AttemptID)
		}
		expiresAt, err := time.Parse(time.RFC3339Nano, current.LeaseExpiresAt)
		if err != nil {
			return fmt.Errorf("parse stale provider execution lease expiry for %q: %w", current.AttemptID, err)
		}
		now, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(occurredAt))
		if err != nil {
			return fmt.Errorf("parse stale provider execution lease evaluation time for %q: %w", current.AttemptID, err)
		}
		if now.Before(expiresAt) {
			return errStaleAttemptExecutionLeaseChanged
		}
	} else if current.LeaseExpiresAt != "" {
		return fmt.Errorf("provider execution lease owner is missing for stale attempt %q", current.AttemptID)
	}

	result, err := tx.ExecContext(ctx, `UPDATE attempt_executions
		SET updated_at = updated_at
		WHERE attempt_id = ? AND run_id = ? AND node_run_id = ?
		  AND state IN ('queued', 'starting', 'running')
		  AND COALESCE(lease_owner, '') = ?
		  AND lease_version = ?
		  AND COALESCE(lease_expires_at, '') = ?`,
		current.AttemptID, current.RunID, current.NodeRunID,
		expectedLeaseOwner, expectedLeaseVersion, expectedLeaseExpiresAt,
	)
	if err != nil {
		return fmt.Errorf("fence stale provider execution lease for %q: %w", current.AttemptID, err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read stale provider execution lease fence for %q: %w", current.AttemptID, err)
	}
	if rows != 1 {
		return errStaleAttemptExecutionLeaseChanged
	}
	return nil
}

func fenceStaleAttemptExecutionTx(ctx context.Context, tx *sql.Tx, attempt liveNodeAttempt, occurredAt string) error {
	if err := supersedeStaleProviderInvocationTx(ctx, tx, attempt, occurredAt); err != nil {
		return err
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
		  AND COALESCE(lease_owner, '') = ?
		  AND lease_version = ?
		  AND COALESCE(lease_expires_at, '') = ?
		  AND state IN ('queued', 'starting', 'running')`,
		occurredAt,
		occurredAt,
		staleAttemptFailureReasonText,
		occurredAt,
		attempt.AttemptID,
		attempt.RunID,
		attempt.NodeRunID,
		attempt.LeaseOwner,
		attempt.LeaseVersion,
		attempt.LeaseExpiresAt,
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

func supersedeStaleProviderInvocationTx(ctx context.Context, tx *sql.Tx, attempt liveNodeAttempt, occurredAt string) error {
	if attempt.ExecutionMode == executionstore.ExecutionModeProviderBacked && strings.TrimSpace(attempt.ActiveProviderInvocationID) != "" {
		if err := ensureProviderTerminalHandoffNotPendingTx(ctx, tx, attempt); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE provider_invocations
			SET invocation_state = 'superseded',
			    failure_reason_code = 'agent_unresponsive',
			    failure_reason_text = ?,
			    updated_at = ?
			WHERE provider_invocation_id = ?
			  AND invocation_state IN ('issued', 'open')`,
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
	return nil
}

func ensureProviderTerminalHandoffNotPendingTx(ctx context.Context, tx *sql.Tx, attempt liveNodeAttempt) error {
	if attempt.ExecutionMode != executionstore.ExecutionModeProviderBacked || strings.TrimSpace(attempt.ActiveProviderInvocationID) == "" {
		return nil
	}
	var handoffPending int
	if err := tx.QueryRowContext(ctx, `SELECT
		EXISTS(SELECT 1 FROM provider_terminal_handoffs WHERE provider_invocation_id = ?)
		OR EXISTS(SELECT 1 FROM provider_invocations WHERE provider_invocation_id = ? AND (terminal_handoff_pending_at IS NOT NULL OR terminal_recovery_required_at IS NOT NULL))`,
		attempt.ActiveProviderInvocationID, attempt.ActiveProviderInvocationID,
	).Scan(&handoffPending); err != nil {
		return fmt.Errorf("check provider terminal handoff before stale recovery: %w", err)
	}
	if handoffPending != 0 {
		return fmt.Errorf("%w for attempt %q", errProviderTerminalHandoffPending, attempt.AttemptID)
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
