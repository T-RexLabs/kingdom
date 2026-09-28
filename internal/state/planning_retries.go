package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"kingdom/internal/sqlitex"
	"kingdom/internal/timestamporder"
)

// NextPendingPlanningRetryDue returns the next persisted retry deadline. The
// timer is only a wake-up optimization; ClaimOperation remains authoritative.
func (s *PlanningStore) NextPendingPlanningRetryDue(ctx context.Context) (*time.Time, error) {
	query := fmt.Sprintf(`SELECT due_at FROM retry_decisions WHERE subject_kind = 'planning_invocation' AND decision = 'retry' AND dispatch_state = 'pending' ORDER BY %s, retry_decision_id LIMIT 1`, timestamporder.OrderBySQL("due_at", false))
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list pending planning retry deadlines: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("scan pending planning retry deadline: %w", err)
		}
		due, err := parseRetryTime(raw)
		if err != nil {
			return nil, err
		}
		return &due, nil
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending planning retry deadlines: %w", err)
	}
	return nil, nil
}

// FinalizePlanningFailureWithRetryRequest is the single transaction boundary
// for a provider-backed planner failure and its policy decision. A retry keeps
// the operation/ACP run alive but always settles the failed invocation first.
type FinalizePlanningFailureWithRetryRequest struct {
	SessionID         string
	OperationID       string
	LeaseOwner        string
	LeaseVersion      int
	UpdatedAt         string
	FinishedAt        string
	ErrorMessage      string
	AllowExpiredLease bool

	ProviderSettlement *FinalizePlanningProviderSettlementRequest
	RetryDecision      RetryDecisionCreateRequest
	Event              *AppendPlanningOperationEventRequest
}

func (s *PlanningStore) FinalizePlanningFailureWithRetry(ctx context.Context, req FinalizePlanningFailureWithRetryRequest) (bool, error) {
	if err := validateFinalizePlanningFailureWithRetryRequest(req); err != nil {
		return false, err
	}
	if err := s.replayConfiguredPlanningProcessGuardEvidenceForFinalizer(ctx, req.OperationID, req.ProviderSettlement); err != nil {
		return false, err
	}
	var retry bool
	_, err := sqlitex.Retry(ctx, func() error {
		var retryErr error
		retry, retryErr = s.finalizePlanningFailureWithRetryOnce(ctx, req)
		return retryErr
	}, sqlitex.Options{})
	return retry, err
}

func (s *PlanningStore) finalizePlanningFailureWithRetryOnce(ctx context.Context, req FinalizePlanningFailureWithRetryRequest) (bool, error) {
	if err := validateFinalizePlanningFailureWithRetryRequest(req); err != nil {
		return false, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin finalize planning failure with retry: %w", err)
	}
	operation, found, err := getPlanningOperation(ctx, tx, req.OperationID)
	if err != nil {
		return false, rollbackPlanningTx(tx, "finalize planning failure with retry", err)
	}
	if !found || operation.PlanningSessionID != strings.TrimSpace(req.SessionID) {
		return false, rollbackPlanningTx(tx, "finalize planning failure with retry", ErrPlanningLeaseConflict)
	}
	if err := validateFinalizeOperationProviderContract(operation, FinalizeOperationRequest{OperationID: req.OperationID, ProviderSettlement: req.ProviderSettlement}); err != nil {
		return false, rollbackPlanningTx(tx, "finalize planning failure with retry", err)
	}
	if operation.ActiveProviderInvocationID == nil || strings.TrimSpace(*operation.ActiveProviderInvocationID) != strings.TrimSpace(req.RetryDecision.SourcePlanningInvocationID) {
		return false, rollbackPlanningTx(tx, "finalize planning failure with retry", ErrPlanningLeaseConflict)
	}

	retry := req.RetryDecision.Decision == RetryDecisionRetry
	status := PlanningOperationStatusFailed
	if retry {
		status = PlanningOperationStatusQueued
	}
	update := newUpdateBuilder("planning_operations").
		Set("status", string(status)).
		Set("active_provider_invocation_id", nil).
		Set("lease_owner", "").
		Set("lease_expires_at", nil).
		SetExpr("lease_version", "lease_version + 1").
		Set("last_heartbeat_at", nil)
	if retry {
		// A retry must not inherit a provider session or a prior terminal
		// settlement or provisional output. All are scoped to the failed
		// invocation.
		update.Set("provider_session_id", nil).Set("observed_output_text", "").Set("error", nil).Set("finished_at", nil)
	} else {
		update.Set("error", strings.TrimSpace(req.ErrorMessage)).Set("finished_at", strings.TrimSpace(req.FinishedAt))
	}
	update.
		Where("operation_id = ?", strings.TrimSpace(req.OperationID)).
		Where("status = ?", string(PlanningOperationStatusRunning)).
		Where("lease_owner = ?", strings.TrimSpace(req.LeaseOwner)).
		Where("lease_version = ?", req.LeaseVersion)
	if !req.AllowExpiredLease {
		update.Where(timestamporder.ComparisonSQL("lease_expires_at", "?", ">"), strings.TrimSpace(req.FinishedAt))
	}
	result, err := update.Exec(ctx, tx)
	if err != nil {
		return false, rollbackPlanningTx(tx, "finalize planning failure with retry", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return false, rollbackPlanningTx(tx, "finalize planning failure with retry", err)
	}
	if changed != 1 {
		return false, rollbackPlanningTx(tx, "finalize planning failure with retry", ErrPlanningLeaseConflict)
	}

	if !retry {
		if err := finalizePlanningFailureSessionTx(ctx, tx, operation, req); err != nil {
			return false, rollbackPlanningTx(tx, "finalize planning failure with retry", err)
		}
	}
	event := *req.Event
	if req.RetryDecision.Decision == RetryDecisionExhausted {
		payload, err := planningRetryExhaustionEventPayload(event.PayloadJSON, req.RetryDecision)
		if err != nil {
			return false, rollbackPlanningTx(tx, "finalize planning failure with retry", err)
		}
		event.PayloadJSON = payload
	}
	if _, err := insertPlanningOperationEvent(ctx, tx, event); err != nil {
		return false, rollbackPlanningTx(tx, "finalize planning failure with retry", err)
	}
	if _, err := finalizePlanningProviderSettlementTx(ctx, tx, req.OperationID, req.SessionID, req.ProviderSettlement); err != nil {
		return false, rollbackPlanningTx(tx, "finalize planning failure with retry", err)
	}
	if err := resolveFailedPlanningRetryTx(ctx, tx, req.OperationID, req.FinishedAt); err != nil {
		return false, rollbackPlanningTx(tx, "finalize planning failure with retry", err)
	}
	if retry {
		if _, err := tx.ExecContext(ctx, `UPDATE planning_operations SET canonical_settlement_id = NULL WHERE operation_id = ?`, strings.TrimSpace(req.OperationID)); err != nil {
			return false, rollbackPlanningTx(tx, "finalize planning failure with retry", fmt.Errorf("clear retry operation settlement: %w", err))
		}
	}
	if _, err := insertRetryDecisionTx(ctx, tx, req.RetryDecision); err != nil {
		return false, rollbackPlanningTx(tx, "finalize planning failure with retry", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit finalize planning failure with retry: %w", err)
	}
	return retry, nil
}

func planningRetryExhaustionEventPayload(raw string, decision RetryDecisionCreateRequest) (string, error) {
	payload := map[string]any{}
	if trimmed := strings.TrimSpace(raw); trimmed != "" {
		if err := json.Unmarshal([]byte(trimmed), &payload); err != nil {
			return "", fmt.Errorf("decode planning exhaustion event payload: %w", err)
		}
	}
	payload["phase"] = "retry_exhausted"
	payload["escalation_target"] = strings.TrimSpace(decision.EscalationTarget)
	payload["reason_code"] = strings.TrimSpace(decision.ErrorCode)
	payload["operator_text"] = strings.TrimSpace(decision.OperatorText)
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode planning exhaustion event payload: %w", err)
	}
	return string(encoded), nil
}

func validateFinalizePlanningFailureWithRetryRequest(req FinalizePlanningFailureWithRetryRequest) error {
	if strings.TrimSpace(req.SessionID) == "" || strings.TrimSpace(req.OperationID) == "" || strings.TrimSpace(req.LeaseOwner) == "" || req.LeaseVersion < 0 {
		return fmt.Errorf("planning session, operation, and lease lineage are required")
	}
	if strings.TrimSpace(req.UpdatedAt) == "" || strings.TrimSpace(req.FinishedAt) == "" || strings.TrimSpace(req.ErrorMessage) == "" {
		return fmt.Errorf("planning failure timestamps and error message are required")
	}
	if req.Event == nil {
		return fmt.Errorf("planning failure event is required")
	}
	if err := validatePlanningOperationEventAppend(*req.Event); err != nil {
		return err
	}
	if req.ProviderSettlement == nil {
		return fmt.Errorf("planning provider settlement is required")
	}
	if _, err := normalizeFinalizePlanningProviderSettlementRequest(*req.ProviderSettlement); err != nil {
		return err
	}
	decision, err := normalizeRetryDecisionCreateRequest(req.RetryDecision)
	if err != nil {
		return err
	}
	if decision.SubjectKind != RetrySubjectKindPlanningInvocation {
		return fmt.Errorf("planning failure retry decision must be a planning invocation")
	}
	if decision.PlanningOperationID != strings.TrimSpace(req.OperationID) || decision.SourcePlanningInvocationID != strings.TrimSpace(req.ProviderSettlement.PlanningProviderInvocationID) {
		return fmt.Errorf("planning failure retry decision must match the settled invocation")
	}
	if decision.ErrorCode != strings.TrimSpace(req.ProviderSettlement.FailureReasonCode) {
		return fmt.Errorf("planning failure retry decision error code must match provider settlement")
	}
	return nil
}

func finalizePlanningFailureSessionTx(ctx context.Context, tx *sql.Tx, operation PlanningOperation, req FinalizePlanningFailureWithRetryRequest) error {
	update := newUpdateBuilder("planning_sessions").Set("updated_at", strings.TrimSpace(req.UpdatedAt))
	switch operation.Type {
	case PlanningOperationTypeGenerateTitle:
		update.Set("current_title_operation_id", nil).
			Where("planning_session_id = ?", operation.PlanningSessionID).
			Where("current_title_operation_id = ?", operation.OperationID)
	default:
		update.Set("conversation_state", string(PlanningSessionConversationStateReady))
		if operation.Type == PlanningOperationTypeRegenerate {
			previousLeafID, err := regeneratePreviousActiveLeafMessageID(operation.RequestPayloadJSON)
			if err != nil {
				return err
			}
			update.Set("active_leaf_message_id", finalizeNullable(previousLeafID))
		}
		update.Where("planning_session_id = ?", operation.PlanningSessionID).
			Where("current_operation_id = ?", operation.OperationID)
	}
	result, err := update.Exec(ctx, tx)
	if err != nil {
		return fmt.Errorf("finalize failed planning session: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return ErrPlanningLeaseConflict
	}
	return nil
}

func (s *PlanningStore) CountPlanningRetryDecisions(ctx context.Context, operationID string) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM retry_decisions WHERE subject_kind = 'planning_invocation' AND planning_operation_id = ? AND decision = 'retry'`, strings.TrimSpace(operationID)).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count planning retry decisions: %w", err)
	}
	return count, nil
}

func resolveRecoveredPlanningRetryTx(ctx context.Context, tx *sql.Tx, operationID, occurredAt string) error {
	_, err := tx.ExecContext(ctx, `UPDATE retry_decisions SET resolution = ?, resolved_at = ? WHERE subject_kind = 'planning_invocation' AND planning_operation_id = ? AND decision = 'retry' AND dispatch_state = 'dispatched' AND resolution = ?`, RetryDecisionResolutionRecovered, strings.TrimSpace(occurredAt), strings.TrimSpace(operationID), RetryDecisionResolutionPending)
	if err != nil {
		return fmt.Errorf("resolve recovered planning retry: %w", err)
	}
	return nil
}

// resolveFailedPlanningRetryTx closes the dispatched predecessor before the
// failed retry invocation records its successor or terminal policy decision.
func resolveFailedPlanningRetryTx(ctx context.Context, tx *sql.Tx, operationID, occurredAt string) error {
	_, err := tx.ExecContext(ctx, `UPDATE retry_decisions SET resolution = ?, resolved_at = ? WHERE subject_kind = 'planning_invocation' AND planning_operation_id = ? AND decision = 'retry' AND dispatch_state = 'dispatched' AND resolution = ?`, RetryDecisionResolutionFailed, strings.TrimSpace(occurredAt), strings.TrimSpace(operationID), RetryDecisionResolutionPending)
	if err != nil {
		return fmt.Errorf("resolve failed planning retry: %w", err)
	}
	return nil
}

func resolveCancelledPlanningRetryTx(ctx context.Context, tx *sql.Tx, operationID, occurredAt string) error {
	_, err := tx.ExecContext(ctx, `UPDATE retry_decisions
		SET dispatch_state = CASE WHEN dispatch_state = 'pending' THEN 'not_applicable' ELSE dispatch_state END,
		    dispatched_at = CASE WHEN dispatch_state = 'pending' THEN NULL ELSE dispatched_at END,
		    resolution = ?,
		    resolved_at = ?
		WHERE subject_kind = 'planning_invocation'
		  AND planning_operation_id = ?
		  AND decision = 'retry'
		  AND dispatch_state IN ('pending', 'dispatched')
		  AND resolution = ?`, RetryDecisionResolutionCancelled, strings.TrimSpace(occurredAt), strings.TrimSpace(operationID), RetryDecisionResolutionPending)
	if err != nil {
		return fmt.Errorf("resolve cancelled planning retry: %w", err)
	}
	return nil
}

// PlanningOperationRetryDue is deliberately a read-side prefilter only. The
// authoritative due check and dispatched marker both happen in ClaimOperation.
func (s *PlanningStore) PlanningOperationRetryDue(ctx context.Context, operationID, now string) (bool, error) {
	var dueAt sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT due_at FROM retry_decisions WHERE subject_kind = 'planning_invocation' AND planning_operation_id = ? AND decision = 'retry' AND dispatch_state = 'pending' ORDER BY created_at DESC, retry_decision_id DESC LIMIT 1`, strings.TrimSpace(operationID)).Scan(&dueAt)
	if err == sql.ErrNoRows {
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("check planning retry due: %w", err)
	}
	due, err := parseRetryTime(dueAt.String)
	if err != nil {
		return false, err
	}
	current, err := parseRetryTime(strings.TrimSpace(now))
	if err != nil {
		return false, err
	}
	return !due.After(current), nil
}

// RequeueUnlaunchedPlanningOperation recovers the crash window after a queued
// claim commits but before a provider invocation exists. Invocation creation is
// lease-fenced, so clearing this lease prevents an old worker from launching.
func (s *PlanningStore) RequeueUnlaunchedPlanningOperation(ctx context.Context, operation PlanningOperation) (bool, error) {
	var requeued bool
	_, err := sqlitex.Retry(ctx, func() error {
		var retryErr error
		requeued, retryErr = s.requeueUnlaunchedPlanningOperationOnce(ctx, operation)
		return retryErr
	}, sqlitex.Options{})
	return requeued, err
}

func (s *PlanningStore) requeueUnlaunchedPlanningOperationOnce(ctx context.Context, operation PlanningOperation) (bool, error) {
	if strings.TrimSpace(operation.OperationID) == "" || strings.TrimSpace(operation.LeaseOwner) == "" || operation.LeaseVersion < 1 || operation.ActiveProviderInvocationID != nil {
		return false, fmt.Errorf("unlaunched planning operation lease lineage is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin requeue unlaunched planning operation: %w", err)
	}
	result, err := tx.ExecContext(ctx, `UPDATE planning_operations
		SET status = 'queued', lease_owner = '', lease_expires_at = NULL,
		    lease_version = lease_version + 1, last_heartbeat_at = NULL,
		    active_provider_invocation_id = NULL, finished_at = NULL
		WHERE operation_id = ? AND status = 'running' AND lease_owner = ? AND lease_version = ?
		  AND COALESCE(TRIM(active_provider_invocation_id), '') = ''`, operation.OperationID, operation.LeaseOwner, operation.LeaseVersion)
	if err != nil {
		return false, rollbackPlanningTx(tx, "requeue unlaunched planning operation", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return false, rollbackPlanningTx(tx, "requeue unlaunched planning operation", err)
	}
	if changed == 0 {
		return false, rollbackPlanningTx(tx, "requeue unlaunched planning operation", nil)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE retry_decisions SET dispatch_state = 'pending', dispatched_at = NULL
		WHERE subject_kind = 'planning_invocation' AND planning_operation_id = ?
		  AND decision = 'retry' AND dispatch_state = 'dispatched' AND resolution = 'pending'`, operation.OperationID); err != nil {
		return false, rollbackPlanningTx(tx, "requeue unlaunched planning operation", err)
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit requeue unlaunched planning operation: %w", err)
	}
	return true, nil
}

func (s *PlanningStore) PlanningOperationHasDispatchedRetry(ctx context.Context, operationID string) (bool, error) {
	var exists int
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM retry_decisions WHERE subject_kind = 'planning_invocation' AND planning_operation_id = ? AND decision = 'retry' AND dispatch_state = 'dispatched' AND resolution = 'pending')`, strings.TrimSpace(operationID)).Scan(&exists); err != nil {
		return false, fmt.Errorf("check dispatched planning retry: %w", err)
	}
	return exists != 0, nil
}
