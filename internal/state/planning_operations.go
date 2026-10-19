package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"kingdom/internal/acp"
	"kingdom/internal/sqlitex"
	"kingdom/internal/timestamporder"
)

func (s *PlanningStore) CreateOperation(ctx context.Context, req CreatePlanningOperationRequest) (PlanningOperation, error) {
	if err := validatePlanningOperationCreate(req); err != nil {
		return PlanningOperation{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PlanningOperation{}, fmt.Errorf("begin create planning operation: %w", err)
	}
	operation, err := insertPlanningOperation(ctx, tx, req)
	if err != nil {
		return PlanningOperation{}, rollbackPlanningTx(tx, "create planning operation", err)
	}
	if err := bindPlanningOperationExternalContext(ctx, tx, operation); err != nil {
		return PlanningOperation{}, rollbackPlanningTx(tx, "create planning operation", err)
	}
	if err := tx.Commit(); err != nil {
		return PlanningOperation{}, fmt.Errorf("commit create planning operation: %w", err)
	}
	return operation, nil
}

func insertPlanningOperation(ctx context.Context, exec sqlExecutor, req CreatePlanningOperationRequest) (PlanningOperation, error) {
	row := PlanningOperation{
		OperationID:                strings.TrimSpace(req.OperationID),
		PlanningSessionID:          strings.TrimSpace(req.PlanningSessionID),
		Type:                       req.Type,
		Status:                     req.Status,
		RequestPayloadJSON:         strings.TrimSpace(req.RequestPayloadJSON),
		LeaseOwner:                 strings.TrimSpace(req.LeaseOwner),
		LeaseExpiresAt:             nullableString(req.LeaseExpiresAt),
		LeaseVersion:               req.LeaseVersion,
		LastHeartbeatAt:            nullableString(req.LastHeartbeatAt),
		ProviderSessionID:          nullableString(req.ProviderSessionID),
		ActiveProviderInvocationID: nullableString(req.ActiveProviderInvocationID),
		ObservedOutputText:         strings.TrimSpace(req.ObservedOutputText),
		Error:                      nullableString(req.Error),
		ResultRef:                  nullableString(req.ResultRef),
		CanonicalSettlementID:      nullableString(req.CanonicalSettlementID),
		CreatedAt:                  strings.TrimSpace(req.CreatedAt),
		StartedAt:                  nullableString(req.StartedAt),
		FinishedAt:                 nullableString(req.FinishedAt),
	}
	row.AcpRunID = acp.PlannerRunID(row.OperationID)
	queryer, ok := exec.(sqlQueryRower)
	if !ok {
		return PlanningOperation{}, fmt.Errorf("planning operation context binding requires QueryRow")
	}
	// The outer join is deliberate: no row at all means the session does not
	// exist, while a row with a NULL snapshot means the session's adopted
	// pointer is dangling. Those are different failures and must not share
	// one message.
	var snapshotID sql.NullString
	if err := queryer.QueryRowContext(ctx, `
		SELECT snapshot.planning_context_snapshot_id
		FROM planning_sessions session
		LEFT JOIN planning_context_snapshots snapshot
		  ON snapshot.planning_context_snapshot_id=session.context_snapshot_id
		 AND snapshot.planning_session_id=session.planning_session_id
		WHERE session.planning_session_id=?`, row.PlanningSessionID).Scan(&snapshotID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return PlanningOperation{}, fmt.Errorf("%w: %q", ErrPlanningSessionNotFound, row.PlanningSessionID)
		}
		return PlanningOperation{}, fmt.Errorf("bind planning operation context snapshot: %w", err)
	}
	if !snapshotID.Valid || strings.TrimSpace(snapshotID.String) == "" {
		return PlanningOperation{}, fmt.Errorf("%w: %q", ErrPlanningContextSnapshotUnavailable, row.PlanningSessionID)
	}
	row.PlanningContextSnapshotID = nullableString(snapshotID.String)
	_, err := exec.ExecContext(ctx,
		`INSERT INTO planning_operations (operation_id, acp_run_id, planning_session_id, planning_context_snapshot_id, type, status, request_payload_json, lease_owner, lease_expires_at, lease_version, last_heartbeat_at, provider_session_id, active_provider_invocation_id, observed_output_text, error, result_ref, canonical_settlement_id, created_at, started_at, finished_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		row.OperationID,
		row.AcpRunID,
		row.PlanningSessionID,
		nullableStringValue(row.PlanningContextSnapshotID),
		row.Type,
		row.Status,
		row.RequestPayloadJSON,
		row.LeaseOwner,
		nullableStringValue(row.LeaseExpiresAt),
		row.LeaseVersion,
		nullableStringValue(row.LastHeartbeatAt),
		nullableStringValue(row.ProviderSessionID),
		nullableStringValue(row.ActiveProviderInvocationID),
		row.ObservedOutputText,
		nullableStringValue(row.Error),
		nullableStringValue(row.ResultRef),
		nullableStringValue(row.CanonicalSettlementID),
		row.CreatedAt,
		nullableStringValue(row.StartedAt),
		nullableStringValue(row.FinishedAt),
	)
	if err != nil {
		return PlanningOperation{}, fmt.Errorf("insert planning operation: %w", err)
	}
	return row, nil
}

func (s *PlanningStore) CreateReplyBundle(ctx context.Context, req CreatePlanningReplyBundleRequest) (PlanningOperation, error) {
	if strings.TrimSpace(req.SessionID) == "" {
		return PlanningOperation{}, fmt.Errorf("planning session id is required")
	}
	if err := validatePlanningMessageAppend(req.UserMessage); err != nil {
		return PlanningOperation{}, err
	}
	if err := validatePlanningOperationCreate(req.Operation); err != nil {
		return PlanningOperation{}, err
	}
	if req.InitialEvent != nil {
		if err := validatePlanningOperationEventAppend(*req.InitialEvent); err != nil {
			return PlanningOperation{}, err
		}
	}
	if !isValidPlanningSessionConversationState(req.ConversationState) {
		return PlanningOperation{}, fmt.Errorf("invalid planning session conversation state: %q", req.ConversationState)
	}
	if strings.TrimSpace(req.UpdatedAt) == "" {
		return PlanningOperation{}, fmt.Errorf("updated_at is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PlanningOperation{}, fmt.Errorf("begin create planning reply bundle: %w", err)
	}

	// The user message is appended under its parent up front (the operation's
	// optimistic anchor); the assistant reply is appended at finalize. Editing
	// a message just supplies a different parent — no truncation.
	if _, err := insertPlanningMessage(ctx, tx, req.UserMessage); err != nil {
		return PlanningOperation{}, rollbackPlanningTx(tx, "create planning reply bundle", err)
	}
	operation, err := insertPlanningOperation(ctx, tx, req.Operation)
	if err != nil {
		rollbackErr := rollbackPlanningTx(tx, "create planning reply bundle", err)
		if IsUniqueViolation(err) {
			return PlanningOperation{}, errors.Join(ErrPlanningUniqueViolation, rollbackErr)
		}
		return PlanningOperation{}, rollbackErr
	}
	if err := bindPlanningOperationExternalContext(ctx, tx, operation); err != nil {
		return PlanningOperation{}, rollbackPlanningTx(tx, "create planning reply bundle", err)
	}
	if req.InitialEvent != nil {
		if _, err := insertPlanningOperationEvent(ctx, tx, *req.InitialEvent); err != nil {
			return PlanningOperation{}, rollbackPlanningTx(tx, "create planning reply bundle", err)
		}
	}
	// Move the active leaf onto the new user message so the operator and the
	// planner prompt immediately see the new branch (the assistant reply is
	// appended under it at finalize).
	if err := updatePlanningSession(ctx, tx, UpdatePlanningSessionRequest{
		PlanningSessionID:   req.SessionID,
		ConversationState:   &req.ConversationState,
		CurrentOperationID:  stringPtr(req.Operation.OperationID),
		ActiveLeafMessageID: stringPtr(req.UserMessage.PlanningMessageID),
		UpdatedAt:           req.UpdatedAt,
	}); err != nil {
		return PlanningOperation{}, rollbackPlanningTx(tx, "create planning reply bundle", err)
	}
	if err := tx.Commit(); err != nil {
		return PlanningOperation{}, &CommitOutcomeUnknownError{Operation: "create planning reply bundle", ResourceID: strings.TrimSpace(req.Operation.OperationID), Cause: err}
	}
	return operation, nil
}

func (s *PlanningStore) CreateFormalizeBundle(ctx context.Context, req CreatePlanningFormalizeBundleRequest) (PlanningOperation, error) {
	if strings.TrimSpace(req.SessionID) == "" {
		return PlanningOperation{}, fmt.Errorf("planning session id is required")
	}
	if err := validatePlanningOperationCreate(req.Operation); err != nil {
		return PlanningOperation{}, err
	}
	if req.InitialEvent != nil {
		if err := validatePlanningOperationEventAppend(*req.InitialEvent); err != nil {
			return PlanningOperation{}, err
		}
	}
	if !isValidPlanningSessionConversationState(req.ConversationState) {
		return PlanningOperation{}, fmt.Errorf("invalid planning session conversation state: %q", req.ConversationState)
	}
	if strings.TrimSpace(req.UpdatedAt) == "" {
		return PlanningOperation{}, fmt.Errorf("updated_at is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PlanningOperation{}, fmt.Errorf("begin create planning formalize bundle: %w", err)
	}
	operation, err := insertPlanningOperation(ctx, tx, req.Operation)
	if err != nil {
		rollbackErr := rollbackPlanningTx(tx, "create planning formalize bundle", err)
		if IsUniqueViolation(err) {
			return PlanningOperation{}, errors.Join(ErrPlanningUniqueViolation, rollbackErr)
		}
		return PlanningOperation{}, rollbackErr
	}
	if err := bindPlanningOperationExternalContext(ctx, tx, operation); err != nil {
		return PlanningOperation{}, rollbackPlanningTx(tx, "create planning formalize bundle", err)
	}
	if req.InitialEvent != nil {
		if _, err := insertPlanningOperationEvent(ctx, tx, *req.InitialEvent); err != nil {
			return PlanningOperation{}, rollbackPlanningTx(tx, "create planning formalize bundle", err)
		}
	}
	if err := updatePlanningSession(ctx, tx, UpdatePlanningSessionRequest{
		PlanningSessionID:  req.SessionID,
		ConversationState:  &req.ConversationState,
		CurrentOperationID: stringPtr(req.CurrentOperationID),
		UpdatedAt:          req.UpdatedAt,
	}); err != nil {
		return PlanningOperation{}, rollbackPlanningTx(tx, "create planning formalize bundle", err)
	}
	if err := tx.Commit(); err != nil {
		return PlanningOperation{}, &CommitOutcomeUnknownError{Operation: "create planning formalize bundle", ResourceID: strings.TrimSpace(req.Operation.OperationID), Cause: err}
	}
	return operation, nil
}

func (s *PlanningStore) CreateGenerateTitleBundle(ctx context.Context, req CreatePlanningGenerateTitleBundleRequest) (PlanningOperation, error) {
	if strings.TrimSpace(req.SessionID) == "" {
		return PlanningOperation{}, fmt.Errorf("planning session id is required")
	}
	if err := validatePlanningOperationCreate(req.Operation); err != nil {
		return PlanningOperation{}, err
	}
	if req.InitialEvent != nil {
		if err := validatePlanningOperationEventAppend(*req.InitialEvent); err != nil {
			return PlanningOperation{}, err
		}
	}
	if strings.TrimSpace(req.UpdatedAt) == "" {
		return PlanningOperation{}, fmt.Errorf("updated_at is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PlanningOperation{}, fmt.Errorf("begin create planning generate title bundle: %w", err)
	}
	operation, err := insertPlanningOperation(ctx, tx, req.Operation)
	if err != nil {
		rollbackErr := rollbackPlanningTx(tx, "create planning generate title bundle", err)
		if IsUniqueViolation(err) {
			return PlanningOperation{}, errors.Join(ErrPlanningUniqueViolation, rollbackErr)
		}
		return PlanningOperation{}, rollbackErr
	}
	if err := bindPlanningOperationExternalContext(ctx, tx, operation); err != nil {
		return PlanningOperation{}, rollbackPlanningTx(tx, "create planning generate title bundle", err)
	}
	if req.InitialEvent != nil {
		if _, err := insertPlanningOperationEvent(ctx, tx, *req.InitialEvent); err != nil {
			return PlanningOperation{}, rollbackPlanningTx(tx, "create planning generate title bundle", err)
		}
	}
	if err := updatePlanningSession(ctx, tx, UpdatePlanningSessionRequest{
		PlanningSessionID:       req.SessionID,
		CurrentTitleOperationID: stringPtr(req.CurrentTitleOperationID),
		UpdatedAt:               req.UpdatedAt,
	}); err != nil {
		return PlanningOperation{}, rollbackPlanningTx(tx, "create planning generate title bundle", err)
	}
	if err := tx.Commit(); err != nil {
		return PlanningOperation{}, &CommitOutcomeUnknownError{Operation: "create planning generate title bundle", ResourceID: strings.TrimSpace(req.Operation.OperationID), Cause: err}
	}
	return operation, nil
}

func (s *PlanningStore) CreateRegenerateBundle(ctx context.Context, req CreateRegenerateBundleRequest) (PlanningOperation, error) {
	if strings.TrimSpace(req.SessionID) == "" {
		return PlanningOperation{}, fmt.Errorf("planning session id is required")
	}
	if err := validatePlanningOperationCreate(req.Operation); err != nil {
		return PlanningOperation{}, err
	}
	if req.InitialEvent != nil {
		if err := validatePlanningOperationEventAppend(*req.InitialEvent); err != nil {
			return PlanningOperation{}, err
		}
	}
	if !isValidPlanningSessionConversationState(req.ConversationState) {
		return PlanningOperation{}, fmt.Errorf("invalid planning session conversation state: %q", req.ConversationState)
	}
	if strings.TrimSpace(req.UpdatedAt) == "" {
		return PlanningOperation{}, fmt.Errorf("updated_at is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PlanningOperation{}, fmt.Errorf("begin create regenerate bundle: %w", err)
	}

	operation, err := insertPlanningOperation(ctx, tx, req.Operation)
	if err != nil {
		rollbackErr := rollbackPlanningTx(tx, "create regenerate bundle", err)
		if IsUniqueViolation(err) {
			return PlanningOperation{}, errors.Join(ErrPlanningUniqueViolation, rollbackErr)
		}
		return PlanningOperation{}, rollbackErr
	}
	if err := bindPlanningOperationExternalContext(ctx, tx, operation); err != nil {
		return PlanningOperation{}, rollbackPlanningTx(tx, "create regenerate bundle", err)
	}
	if req.InitialEvent != nil {
		if _, err := insertPlanningOperationEvent(ctx, tx, *req.InitialEvent); err != nil {
			return PlanningOperation{}, rollbackPlanningTx(tx, "create regenerate bundle", err)
		}
	}
	// Move the active leaf to the fork point (the target's parent) so the run is
	// prompted with the conversation up to that message; the regenerated answer
	// is appended as a sibling branch at finalize.
	if err := updatePlanningSession(ctx, tx, UpdatePlanningSessionRequest{
		PlanningSessionID:   req.SessionID,
		ConversationState:   &req.ConversationState,
		CurrentOperationID:  stringPtr(req.Operation.OperationID),
		ActiveLeafMessageID: stringPtr(req.ActiveLeafMessageID),
		UpdatedAt:           req.UpdatedAt,
	}); err != nil {
		return PlanningOperation{}, rollbackPlanningTx(tx, "create regenerate bundle", err)
	}
	if err := tx.Commit(); err != nil {
		return PlanningOperation{}, &CommitOutcomeUnknownError{Operation: "create regenerate bundle", ResourceID: strings.TrimSpace(req.Operation.OperationID), Cause: err}
	}
	return operation, nil
}

// CountOperationsByStatus returns one row per status with a COUNT, in a
// single query — replaces three full table scans (one per status) the metrics
// scrape used to perform.
func (s *PlanningStore) CountOperationsByStatus(ctx context.Context) (map[PlanningOperationStatus]int, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT status, COUNT(*) FROM planning_operations GROUP BY status`)
	if err != nil {
		return nil, fmt.Errorf("count planning operations by status: %w", err)
	}
	defer rows.Close()
	out := make(map[PlanningOperationStatus]int)
	for rows.Next() {
		var status PlanningOperationStatus
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return nil, fmt.Errorf("scan planning operation status count: %w", err)
		}
		out[status] = count
	}
	return out, rows.Err()
}

func (s *PlanningStore) CountOperationsByTypeAndStatus(ctx context.Context, operationType PlanningOperationType) (map[PlanningOperationStatus]int, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT status, COUNT(*) FROM planning_operations WHERE type = ? GROUP BY status`,
		operationType,
	)
	if err != nil {
		return nil, fmt.Errorf("count planning operations by type and status: %w", err)
	}
	defer rows.Close()
	out := make(map[PlanningOperationStatus]int)
	for rows.Next() {
		var status PlanningOperationStatus
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			return nil, fmt.Errorf("scan planning operation type/status count: %w", err)
		}
		out[status] = count
	}
	return out, rows.Err()
}

// OldestQueuedOperationCreatedAt returns the created_at of the longest-queued
// operation, used as a saturation indicator on the metrics endpoint.
func (s *PlanningStore) OldestQueuedOperationCreatedAt(ctx context.Context) (string, error) {
	var createdAt sql.NullString
	err := s.db.QueryRowContext(ctx,
		`SELECT created_at FROM planning_operations
		 WHERE status = ?
		 ORDER BY created_at ASC
		 LIMIT 1`, PlanningOperationStatusQueued,
	).Scan(&createdAt)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("oldest queued planning operation: %w", err)
	}
	return normalizedNullableString(nullableStringFromSQL(createdAt)), nil
}

// OldestRunningOperationStartedAt mirrors OldestQueuedOperationCreatedAt for
// the running set.
func (s *PlanningStore) OldestRunningOperationStartedAt(ctx context.Context) (string, error) {
	var startedAt sql.NullString
	err := s.db.QueryRowContext(ctx,
		`SELECT started_at FROM planning_operations
		 WHERE status = ?
		 ORDER BY COALESCE(started_at, created_at) ASC
		 LIMIT 1`, PlanningOperationStatusRunning,
	).Scan(&startedAt)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("oldest running planning operation: %w", err)
	}
	return normalizedNullableString(nullableStringFromSQL(startedAt)), nil
}

func (s *PlanningStore) OldestQueuedOperationCreatedAtByType(ctx context.Context, operationType PlanningOperationType) (string, error) {
	var createdAt sql.NullString
	err := s.db.QueryRowContext(ctx,
		`SELECT created_at FROM planning_operations
		 WHERE type = ? AND status = ?
		 ORDER BY created_at ASC
		 LIMIT 1`, operationType, PlanningOperationStatusQueued,
	).Scan(&createdAt)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("oldest queued planning operation by type: %w", err)
	}
	return normalizedNullableString(nullableStringFromSQL(createdAt)), nil
}

func (s *PlanningStore) OldestRunningOperationStartedAtByType(ctx context.Context, operationType PlanningOperationType) (string, error) {
	var startedAt sql.NullString
	err := s.db.QueryRowContext(ctx,
		`SELECT started_at FROM planning_operations
		 WHERE type = ? AND status = ?
		 ORDER BY COALESCE(started_at, created_at) ASC
		 LIMIT 1`, operationType, PlanningOperationStatusRunning,
	).Scan(&startedAt)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("oldest running planning operation by type: %w", err)
	}
	return normalizedNullableString(nullableStringFromSQL(startedAt)), nil
}

func (s *PlanningStore) ListRecentCompletedOperationTelemetryByType(ctx context.Context, operationType PlanningOperationType, limit int) ([]PlanningOperationTelemetrySample, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT po.operation_id,
		        po.created_at,
		        po.started_at,
		        po.finished_at,
		        (SELECT MIN(created_at) FROM planning_operation_events WHERE operation_id = po.operation_id AND event_type = 'generic' AND json_extract(payload_json, '$.kind') = 'kingdom.phase' AND json_extract(payload_json, '$.phase') = 'provider_session_open') AS provider_session_open_at,
		        (SELECT MIN(created_at) FROM planning_operation_events WHERE operation_id = po.operation_id AND event_type = 'message.part') AS first_text_delta_at,
		        (SELECT MIN(created_at) FROM planning_operation_events WHERE operation_id = po.operation_id AND event_type = 'generic' AND json_extract(payload_json, '$.kind') = 'kingdom.phase' AND json_extract(payload_json, '$.phase') = 'finalizing') AS finalizing_at
		 FROM planning_operations po
		 WHERE po.type = ? AND po.finished_at IS NOT NULL
		 ORDER BY po.finished_at DESC
		 LIMIT ?`, operationType, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list recent completed planning operation telemetry by type: %w", err)
	}
	defer rows.Close()
	items := make([]PlanningOperationTelemetrySample, 0, limit)
	for rows.Next() {
		var sample PlanningOperationTelemetrySample
		var startedAt sql.NullString
		var finishedAt sql.NullString
		var providerSessionOpenAt sql.NullString
		var firstTextDeltaAt sql.NullString
		var finalizingAt sql.NullString
		if err := rows.Scan(
			&sample.OperationID,
			&sample.CreatedAt,
			&startedAt,
			&finishedAt,
			&providerSessionOpenAt,
			&firstTextDeltaAt,
			&finalizingAt,
		); err != nil {
			return nil, fmt.Errorf("scan planning operation telemetry sample: %w", err)
		}
		sample.StartedAt = nullableStringFromSQL(startedAt)
		sample.FinishedAt = nullableStringFromSQL(finishedAt)
		sample.ProviderSessionOpenAt = nullableStringFromSQL(providerSessionOpenAt)
		sample.FirstTextDeltaAt = nullableStringFromSQL(firstTextDeltaAt)
		sample.FinalizingAt = nullableStringFromSQL(finalizingAt)
		items = append(items, sample)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate planning operation telemetry samples: %w", err)
	}
	return items, nil
}

// FinalizeOperation applies the per-Kind post-conditions for a planning
// operation atomically with a lease-fenced terminal write on both the
// operation row and the session row. If either fence fails (i.e. the worker
// has lost its lease since claiming, or the session has already moved on to a
// newer operation) the entire transaction is rolled back and
// ErrPlanningLeaseConflict is returned, leaving any concurrent recovery cycle
// free to claim and finalize the operation correctly.
func (s *PlanningStore) FinalizeOperation(ctx context.Context, req FinalizeOperationRequest) error {
	if err := validateFinalizeOperationRequest(req); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin finalize planning operation: %w", err)
	}
	operation, found, err := getPlanningOperation(ctx, tx, req.OperationID)
	if err != nil {
		return rollbackPlanningTx(tx, "finalize planning operation", err)
	}
	if !found {
		return rollbackPlanningTx(tx, "finalize planning operation", fmt.Errorf("planning operation %q not found", strings.TrimSpace(req.OperationID)))
	}
	if operation.PlanningSessionID != strings.TrimSpace(req.SessionID) {
		return rollbackPlanningTx(tx, "finalize planning operation", fmt.Errorf("planning operation %q does not belong to session %q", strings.TrimSpace(req.OperationID), strings.TrimSpace(req.SessionID)))
	}
	if err := validateFinalizeOperationProviderContract(operation, req); err != nil {
		return rollbackPlanningTx(tx, "finalize planning operation", err)
	}
	// Success settlement validates the still-open active invocation before this
	// finalizer clears it from the operation row.
	if _, err := finalizePlanningProviderSettlementTx(ctx, tx, req.OperationID, req.SessionID, req.ProviderSettlement); err != nil {
		return rollbackPlanningTx(tx, "finalize planning operation", err)
	}

	var terminalStatus PlanningOperationStatus
	switch req.Kind {
	case FinalizeFail:
		terminalStatus = PlanningOperationStatusFailed
	case FinalizeCancel:
		terminalStatus = PlanningOperationStatusCancelled
	default:
		terminalStatus = PlanningOperationStatusSucceeded
	}

	opUpdate := newUpdateBuilder("planning_operations").
		Set("status", string(terminalStatus)).
		Set("active_provider_invocation_id", nil).
		Set("lease_owner", "").
		Set("lease_expires_at", nil).
		SetExpr("lease_version", "lease_version + 1").
		Set("last_heartbeat_at", nil).
		Set("finished_at", strings.TrimSpace(req.FinishedAt))
	if req.Kind != FinalizeFail && req.Kind != FinalizeCancel {
		// Provider settlement text is the canonical successful result. Always
		// overwrite provisional output from a prior invocation or retry.
		finalText := strings.TrimSpace(req.ObservedOutputText)
		if req.ProviderSettlement != nil {
			finalText = strings.TrimSpace(req.ProviderSettlement.FinalText)
			opUpdate.Set("observed_output_ref_json", planningProviderReferenceJSON(req.ProviderSettlement.FinalTextRef))
		}
		opUpdate.Set("observed_output_text", finalText)
	} else if strings.TrimSpace(req.ObservedOutputText) != "" {
		opUpdate.Set("observed_output_text", strings.TrimSpace(req.ObservedOutputText))
	}
	if req.ProviderSettlement != nil && req.ProviderSettlement.FinalTextRef != nil {
		opUpdate.Set("observed_output_text", "")
		opUpdate.Set("observed_output_ref_json", planningProviderReferenceJSON(req.ProviderSettlement.FinalTextRef))
	}
	switch req.Kind {
	case FinalizeFail, FinalizeCancel:
		opUpdate.Set("error", strings.TrimSpace(req.ErrorMessage))
	default:
		opUpdate.Set("result_ref", strings.TrimSpace(req.ResultRef))
	}
	opUpdate.
		Where("operation_id = ?", strings.TrimSpace(req.OperationID)).
		Where("lease_owner = ?", strings.TrimSpace(req.LeaseOwner)).
		Where("lease_version = ?", req.LeaseVersion).
		Where("status = ?", string(PlanningOperationStatusRunning))
	if !req.AllowExpiredLease {
		opUpdate.Where(timestamporder.ComparisonSQL("lease_expires_at", "?", ">"), strings.TrimSpace(req.FinishedAt))
	}

	opResult, err := opUpdate.Exec(ctx, tx)
	if err != nil {
		return rollbackPlanningTx(tx, "finalize planning operation", fmt.Errorf("finalize planning operation: %w", err))
	}
	if affected, err := opResult.RowsAffected(); err != nil {
		return rollbackPlanningTx(tx, "finalize planning operation", fmt.Errorf("finalize planning operation rows affected: %w", err))
	} else if affected == 0 {
		return rollbackPlanningTx(tx, "finalize planning operation", ErrPlanningLeaseConflict)
	}

	{
		sessionUpdate := newUpdateBuilder("planning_sessions").
			Set("updated_at", strings.TrimSpace(req.UpdatedAt)).
			Set("conversation_state", string(req.ConversationState))
		switch req.Kind {
		case FinalizeReply, FinalizeRegenerate:
			sessionUpdate.Set("current_operation_id", nil)
			sessionUpdate.Set("active_leaf_message_id", finalizeNullable(req.ActiveLeafMessageID))
		case FinalizeFail, FinalizeCancel:
			if operation.Type == PlanningOperationTypeRegenerate {
				previousLeafID, parseErr := regeneratePreviousActiveLeafMessageID(operation.RequestPayloadJSON)
				if parseErr != nil {
					return rollbackPlanningTx(tx, "finalize planning operation", parseErr)
				}
				sessionUpdate.Set("active_leaf_message_id", finalizeNullable(previousLeafID))
			}
		case FinalizeFormalize:
			sessionUpdate.Set("current_operation_id", nil)
			sessionUpdate.Set("current_draft_id", finalizeNullable(req.CurrentDraftID))
		}
		sessionUpdate.
			Where("planning_session_id = ?", strings.TrimSpace(req.SessionID)).
			Where("current_operation_id = ?", strings.TrimSpace(req.OperationID))

		sessionResult, err := sessionUpdate.Exec(ctx, tx)
		if err != nil {
			return rollbackPlanningTx(tx, "finalize planning operation", fmt.Errorf("finalize planning session: %w", err))
		}
		if affected, err := sessionResult.RowsAffected(); err != nil {
			return rollbackPlanningTx(tx, "finalize planning operation", fmt.Errorf("finalize planning session rows affected: %w", err))
		} else if affected == 0 {
			return rollbackPlanningTx(tx, "finalize planning operation", ErrPlanningLeaseConflict)
		}
	}

	// Only apply transcript/draft side effects after both fences land so a
	// stale worker loses cleanly with ErrPlanningLeaseConflict instead of
	// surfacing raw unique-constraint errors from canonical tables.
	switch req.Kind {
	case FinalizeReply:
		// Append-only into the tree. The user message was inserted at create
		// time, but a recovered/re-finalized reply tolerates re-inserting it
		// idempotently; the assistant reply is appended now under that user.
		if req.UserMessage != nil {
			if _, err := insertPlanningMessage(ctx, tx, *req.UserMessage); err != nil && !IsUniqueViolation(err) {
				return rollbackPlanningTx(tx, "finalize planning operation", err)
			}
		}
		if _, err := insertPlanningMessage(ctx, tx, *req.AssistantMessage); err != nil {
			return rollbackPlanningTx(tx, "finalize planning operation", err)
		}
	case FinalizeRegenerate:
		// Regenerate appends a new assistant sibling under the same parent as
		// the message it regenerates. Nothing is deleted — the prior answer and
		// its downstream remain a selectable branch.
		if _, err := insertPlanningMessage(ctx, tx, *req.AssistantMessage); err != nil {
			return rollbackPlanningTx(tx, "finalize planning operation", err)
		}
	case FinalizeFormalize:
		if _, err := insertPlanningDraft(ctx, tx, *req.Draft); err != nil {
			if !IsUniqueViolation(err) {
				return rollbackPlanningTx(tx, "finalize planning operation", err)
			}
			// Idempotent re-finalize: draft already exists.
		}
	case FinalizeFail, FinalizeCancel:
		// No domain insert. Cancel and Fail share the no-side-effects
		// shape — the only difference is the terminal status the row
		// gets and the column the reason is written to (both use
		// `error` since neither produces a result_ref).
	}

	if req.MessageCompletedEvent != nil {
		if _, err := insertPlanningOperationEvent(ctx, tx, *req.MessageCompletedEvent); err != nil {
			return rollbackPlanningTx(tx, "finalize planning operation", err)
		}
	}
	if req.TerminalEvent != nil {
		if _, err := insertPlanningOperationEvent(ctx, tx, *req.TerminalEvent); err != nil {
			return rollbackPlanningTx(tx, "finalize planning operation", err)
		}
	}

	if req.Kind == FinalizeCancel {
		if err := resolveCancelledPlanningRetryTx(ctx, tx, req.OperationID, req.FinishedAt); err != nil {
			return rollbackPlanningTx(tx, "finalize planning operation", err)
		}
	} else if req.Kind == FinalizeFail {
		if err := resolveFailedPlanningRetryTx(ctx, tx, req.OperationID, req.FinishedAt); err != nil {
			return rollbackPlanningTx(tx, "finalize planning operation", err)
		}
	} else {
		if err := resolveRecoveredPlanningRetryTx(ctx, tx, req.OperationID, req.FinishedAt); err != nil {
			return rollbackPlanningTx(tx, "finalize planning operation", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit finalize planning operation: %w", err)
	}
	return nil
}

func (s *PlanningStore) CancelQueuedOperation(ctx context.Context, req CancelQueuedPlanningOperationRequest) error {
	if strings.TrimSpace(req.OperationID) == "" {
		return fmt.Errorf("planning operation id is required")
	}
	if strings.TrimSpace(req.ErrorMessage) == "" {
		return fmt.Errorf("planning operation cancellation reason is required")
	}
	if strings.TrimSpace(req.UpdatedAt) == "" || strings.TrimSpace(req.FinishedAt) == "" {
		return fmt.Errorf("planning operation cancel timestamps are required")
	}
	if req.TerminalEvent != nil {
		if err := validatePlanningOperationEventAppend(*req.TerminalEvent); err != nil {
			return err
		}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin cancel queued planning operation: %w", err)
	}
	operation, found, err := getPlanningOperation(ctx, tx, req.OperationID)
	if err != nil {
		return rollbackPlanningTx(tx, "cancel queued planning operation", err)
	}
	if !found {
		return rollbackPlanningTx(tx, "cancel queued planning operation", fmt.Errorf("planning operation %q not found", strings.TrimSpace(req.OperationID)))
	}
	if operation.Status != PlanningOperationStatusQueued {
		return rollbackPlanningTx(tx, "cancel queued planning operation", ErrPlanningLeaseConflict)
	}

	result, err := tx.ExecContext(ctx,
		`UPDATE planning_operations
		 SET status = ?, error = ?, active_provider_invocation_id = NULL, lease_owner = '', lease_expires_at = NULL, lease_version = lease_version + 1, last_heartbeat_at = NULL, finished_at = ?
		 WHERE operation_id = ? AND status = ?`,
		PlanningOperationStatusCancelled,
		strings.TrimSpace(req.ErrorMessage),
		strings.TrimSpace(req.FinishedAt),
		strings.TrimSpace(req.OperationID),
		PlanningOperationStatusQueued,
	)
	if err != nil {
		return rollbackPlanningTx(tx, "cancel queued planning operation", fmt.Errorf("cancel queued planning operation: %w", err))
	}
	if affected, err := result.RowsAffected(); err != nil {
		return rollbackPlanningTx(tx, "cancel queued planning operation", fmt.Errorf("cancel queued planning operation rows affected: %w", err))
	} else if affected == 0 {
		return rollbackPlanningTx(tx, "cancel queued planning operation", ErrPlanningLeaseConflict)
	}
	if err := resolveCancelledPlanningRetryTx(ctx, tx, operation.OperationID, req.FinishedAt); err != nil {
		return rollbackPlanningTx(tx, "cancel queued planning operation", err)
	}

	if err := restoreQueuedOperationSession(ctx, tx, operation, req.UpdatedAt); err != nil {
		return rollbackPlanningTx(tx, "cancel queued planning operation", fmt.Errorf("cancel queued planning session: %w", err))
	}

	if req.TerminalEvent != nil {
		if _, err := insertPlanningOperationEvent(ctx, tx, *req.TerminalEvent); err != nil {
			return rollbackPlanningTx(tx, "cancel queued planning operation", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit cancel queued planning operation: %w", err)
	}
	return nil
}

// FailQueuedOperation terminalizes queued work that cannot ever be dispatched
// with its immutable input binding. Unlike cancellation, this records a failed
// outcome, while closing any undispatched automatic retry as cancelled.
func (s *PlanningStore) FailQueuedOperation(ctx context.Context, req FailQueuedPlanningOperationRequest) error {
	if strings.TrimSpace(req.OperationID) == "" {
		return fmt.Errorf("planning operation id is required")
	}
	if strings.TrimSpace(req.ErrorMessage) == "" {
		return fmt.Errorf("planning operation failure reason is required")
	}
	if strings.TrimSpace(req.UpdatedAt) == "" || strings.TrimSpace(req.FinishedAt) == "" {
		return fmt.Errorf("planning operation failure timestamps are required")
	}
	if err := validatePlanningOperationEventAppend(req.TerminalEvent); err != nil {
		return err
	}
	if strings.TrimSpace(req.TerminalEvent.OperationID) != strings.TrimSpace(req.OperationID) {
		return fmt.Errorf("planning operation failure event must match its operation")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin fail queued planning operation: %w", err)
	}
	operation, found, err := getPlanningOperation(ctx, tx, req.OperationID)
	if err != nil {
		return rollbackPlanningTx(tx, "fail queued planning operation", err)
	}
	if !found || operation.Status != PlanningOperationStatusQueued || (operation.ActiveProviderInvocationID != nil && strings.TrimSpace(*operation.ActiveProviderInvocationID) != "") {
		return rollbackPlanningTx(tx, "fail queued planning operation", ErrPlanningLeaseConflict)
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE planning_operations
		SET status=?,error=?,active_provider_invocation_id=NULL,lease_owner='',lease_expires_at=NULL,
		    lease_version=lease_version+1,last_heartbeat_at=NULL,finished_at=?
		WHERE operation_id=? AND status=? AND COALESCE(TRIM(active_provider_invocation_id),'')=''`,
		PlanningOperationStatusFailed, strings.TrimSpace(req.ErrorMessage), strings.TrimSpace(req.FinishedAt),
		strings.TrimSpace(req.OperationID), PlanningOperationStatusQueued,
	)
	if err != nil {
		return rollbackPlanningTx(tx, "fail queued planning operation", err)
	}
	if changed, err := result.RowsAffected(); err != nil {
		return rollbackPlanningTx(tx, "fail queued planning operation", err)
	} else if changed != 1 {
		return rollbackPlanningTx(tx, "fail queued planning operation", ErrPlanningLeaseConflict)
	}
	if err := resolveCancelledPlanningRetryTx(ctx, tx, operation.OperationID, req.FinishedAt); err != nil {
		return rollbackPlanningTx(tx, "fail queued planning operation", err)
	}
	if err := restoreQueuedOperationSession(ctx, tx, operation, req.UpdatedAt); err != nil {
		return rollbackPlanningTx(tx, "fail queued planning operation", fmt.Errorf("fail queued planning session: %w", err))
	}
	if _, err := insertPlanningOperationEvent(ctx, tx, req.TerminalEvent); err != nil {
		return rollbackPlanningTx(tx, "fail queued planning operation", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit fail queued planning operation: %w", err)
	}
	return nil
}

func restoreQueuedOperationSession(ctx context.Context, tx *sql.Tx, operation PlanningOperation, updatedAt string) error {
	sessionUpdate := newUpdateBuilder("planning_sessions").Set("updated_at", strings.TrimSpace(updatedAt))
	sessionUpdate.Where("planning_session_id = ?", operation.PlanningSessionID)
	switch operation.Type {
	case PlanningOperationTypeReply, PlanningOperationTypeFormalize, PlanningOperationTypeRegenerate:
		sessionUpdate.Set("conversation_state", string(PlanningSessionConversationStateReady))
		if operation.Type == PlanningOperationTypeRegenerate {
			previousLeafID, err := regeneratePreviousActiveLeafMessageID(operation.RequestPayloadJSON)
			if err != nil {
				return err
			}
			sessionUpdate.Set("active_leaf_message_id", finalizeNullable(previousLeafID))
		}
		sessionUpdate.Where("current_operation_id = ?", operation.OperationID)
	case PlanningOperationTypeGenerateTitle:
		sessionUpdate.Set("current_title_operation_id", nil)
		sessionUpdate.Where("current_title_operation_id = ?", operation.OperationID)
	}
	result, err := sessionUpdate.Exec(ctx, tx)
	if err != nil {
		return err
	}
	if changed, err := result.RowsAffected(); err != nil {
		return err
	} else if changed != 1 {
		return ErrPlanningLeaseConflict
	}
	return nil
}

func regeneratePreviousActiveLeafMessageID(requestPayloadJSON string) (string, error) {
	trimmed := strings.TrimSpace(requestPayloadJSON)
	if trimmed == "" {
		return "", nil
	}
	var request struct {
		PreviousActiveLeafMessageID string `json:"previous_active_leaf_message_id,omitempty"`
	}
	if err := json.Unmarshal([]byte(trimmed), &request); err != nil {
		return "", fmt.Errorf("decode regenerate request payload: %w", err)
	}
	return strings.TrimSpace(request.PreviousActiveLeafMessageID), nil
}

func validateFinalizeOperationRequest(req FinalizeOperationRequest) error {
	if strings.TrimSpace(req.SessionID) == "" || strings.TrimSpace(req.OperationID) == "" {
		return fmt.Errorf("planning session id and operation id are required")
	}
	if strings.TrimSpace(req.LeaseOwner) == "" {
		return fmt.Errorf("planning operation lease owner is required to fence terminal writes")
	}
	if req.LeaseVersion < 0 {
		return fmt.Errorf("planning operation lease version must be non-negative")
	}
	if !isValidPlanningSessionConversationState(req.ConversationState) {
		return fmt.Errorf("invalid planning session conversation state: %q", req.ConversationState)
	}
	if strings.TrimSpace(req.UpdatedAt) == "" || strings.TrimSpace(req.FinishedAt) == "" {
		return fmt.Errorf("planning operation finalize timestamps are required")
	}
	switch req.Kind {
	case FinalizeReply, FinalizeRegenerate:
		if req.UserMessage != nil {
			if err := validatePlanningMessageAppend(*req.UserMessage); err != nil {
				return err
			}
		}
		if req.AssistantMessage == nil {
			return fmt.Errorf("assistant message is required for %s finalize", req.Kind)
		}
		if err := validatePlanningMessageAppend(*req.AssistantMessage); err != nil {
			return err
		}
		if strings.TrimSpace(req.ResultRef) == "" {
			return fmt.Errorf("result_ref is required for assistant finalize")
		}
	case FinalizeFormalize:
		if req.Draft == nil {
			return fmt.Errorf("draft is required for formalize finalize")
		}
		if err := validatePlanningDraftCreate(*req.Draft); err != nil {
			return err
		}
		if strings.TrimSpace(req.ResultRef) == "" {
			return fmt.Errorf("result_ref is required for formalize finalize")
		}
	case FinalizeFail:
		if strings.TrimSpace(req.ErrorMessage) == "" {
			return fmt.Errorf("error message is required for failed finalize")
		}
	case FinalizeCancel:
		if strings.TrimSpace(req.ErrorMessage) == "" {
			return fmt.Errorf("error message (cancellation reason) is required for cancel finalize")
		}
	default:
		return fmt.Errorf("unsupported finalize kind: %d", req.Kind)
	}
	if req.MessageCompletedEvent != nil {
		if req.Kind == FinalizeFail || req.Kind == FinalizeCancel {
			return fmt.Errorf("message completed event is not valid for %s finalize", req.Kind)
		}
		if err := validatePlanningOperationEventAppend(*req.MessageCompletedEvent); err != nil {
			return err
		}
		if strings.TrimSpace(req.MessageCompletedEvent.EventType) != "message.completed" {
			return fmt.Errorf("message completed event has type %q", strings.TrimSpace(req.MessageCompletedEvent.EventType))
		}
		if strings.TrimSpace(req.MessageCompletedEvent.OperationID) != strings.TrimSpace(req.OperationID) {
			return fmt.Errorf("message completed event operation id does not match finalize operation")
		}
	}
	if req.TerminalEvent != nil {
		if err := validatePlanningOperationEventAppend(*req.TerminalEvent); err != nil {
			return err
		}
	}
	if req.ProviderSettlement != nil {
		if _, err := normalizeFinalizePlanningProviderSettlementRequest(*req.ProviderSettlement); err != nil {
			return err
		}
	}
	return nil
}

func validateFinalizeOperationProviderContract(operation PlanningOperation, req FinalizeOperationRequest) error {
	activeInvocationID := ""
	if operation.ActiveProviderInvocationID != nil {
		activeInvocationID = strings.TrimSpace(*operation.ActiveProviderInvocationID)
	}
	if activeInvocationID == "" {
		if req.ProviderSettlement != nil {
			return fmt.Errorf("provider settlement requires an active provider invocation")
		}
		return nil
	}
	if req.ProviderSettlement == nil {
		return fmt.Errorf("provider settlement is required for provider-backed planning finalization")
	}
	if strings.TrimSpace(req.ProviderSettlement.PlanningProviderInvocationID) != activeInvocationID {
		return fmt.Errorf("provider settlement invocation %q does not match active provider invocation %q", strings.TrimSpace(req.ProviderSettlement.PlanningProviderInvocationID), activeInvocationID)
	}
	return nil
}

// String implements fmt.Stringer for FinalizeOperationKind so error messages
// stay readable without callers reaching for a switch.
func (k FinalizeOperationKind) String() string {
	switch k {
	case FinalizeReply:
		return "reply"
	case FinalizeRegenerate:
		return "regenerate"
	case FinalizeFormalize:
		return "formalize"
	case FinalizeFail:
		return "fail"
	case FinalizeCancel:
		return "cancel"
	default:
		return fmt.Sprintf("unknown(%d)", int(k))
	}
}

// finalizeNullable returns nil when the trimmed value is empty so the SQL
// builder writes NULL rather than the empty string for fields that are
// modelled as nullable on the schema (e.g. current_question_message_id).
func finalizeNullable(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return strings.TrimSpace(value)
}

func (s *PlanningStore) GetOperation(ctx context.Context, operationID string) (PlanningOperation, bool, error) {
	return getPlanningOperation(ctx, s.db, operationID)
}

func (s *PlanningStore) GetOperationByAcpRunID(ctx context.Context, acpRunID string) (PlanningOperation, bool, error) {
	return getPlanningOperationByAcpRunID(ctx, s.db, acpRunID)
}

func getPlanningOperation(ctx context.Context, query sqlQueryRower, operationID string) (PlanningOperation, bool, error) {
	var row PlanningOperation
	err := scanPlanningOperation(query.QueryRowContext(ctx,
		`SELECT operation_id, acp_run_id, planning_session_id, planning_context_snapshot_id, type, status, request_payload_json, lease_owner, lease_expires_at, lease_version, last_heartbeat_at, provider_session_id, active_provider_invocation_id, observed_output_text, error, result_ref, canonical_settlement_id, created_at, started_at, finished_at
		 FROM planning_operations WHERE operation_id = ?`,
		strings.TrimSpace(operationID),
	), &row)
	if err == sql.ErrNoRows {
		return PlanningOperation{}, false, nil
	}
	if err != nil {
		return PlanningOperation{}, false, fmt.Errorf("get planning operation: %w", err)
	}
	return row, true, nil
}

func getPlanningOperationByAcpRunID(ctx context.Context, query sqlQueryRower, acpRunID string) (PlanningOperation, bool, error) {
	var row PlanningOperation
	err := scanPlanningOperation(query.QueryRowContext(ctx,
		`SELECT operation_id, acp_run_id, planning_session_id, planning_context_snapshot_id, type, status, request_payload_json, lease_owner, lease_expires_at, lease_version, last_heartbeat_at, provider_session_id, active_provider_invocation_id, observed_output_text, error, result_ref, canonical_settlement_id, created_at, started_at, finished_at
		 FROM planning_operations WHERE acp_run_id = ?`,
		strings.TrimSpace(acpRunID),
	), &row)
	if err == sql.ErrNoRows {
		return PlanningOperation{}, false, nil
	}
	if err != nil {
		return PlanningOperation{}, false, fmt.Errorf("get planning operation by acp run id: %w", err)
	}
	return row, true, nil
}

func (s *PlanningStore) ListSessionOperations(ctx context.Context, planningSessionID string, limit int) ([]PlanningOperation, error) {
	if limit <= 0 {
		limit = DefaultPlanningOperationListLimit
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT operation_id, acp_run_id, planning_session_id, planning_context_snapshot_id, type, status, request_payload_json, lease_owner, lease_expires_at, lease_version, last_heartbeat_at, provider_session_id, active_provider_invocation_id, observed_output_text, error, result_ref, canonical_settlement_id, created_at, started_at, finished_at
		 FROM planning_operations
		 WHERE planning_session_id = ?
		 ORDER BY created_at DESC, operation_id DESC
		 LIMIT ?`,
		strings.TrimSpace(planningSessionID), limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list planning operations: %w", err)
	}
	defer rows.Close()
	items := make([]PlanningOperation, 0)
	for rows.Next() {
		var row PlanningOperation
		if err := scanPlanningOperation(rows, &row); err != nil {
			return nil, fmt.Errorf("scan planning operation: %w", err)
		}
		items = append(items, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate planning operations: %w", err)
	}
	return items, nil
}

func (s *PlanningStore) GetLatestOperationByResultRef(ctx context.Context, planningSessionID string, resultRef string) (PlanningOperation, bool, error) {
	trimmedSessionID := strings.TrimSpace(planningSessionID)
	trimmedResultRef := strings.TrimSpace(resultRef)
	if trimmedSessionID == "" || trimmedResultRef == "" {
		return PlanningOperation{}, false, fmt.Errorf("planning session id and result ref are required")
	}
	var row PlanningOperation
	err := scanPlanningOperation(s.db.QueryRowContext(ctx,
		`SELECT operation_id, acp_run_id, planning_session_id, planning_context_snapshot_id, type, status, request_payload_json, lease_owner, lease_expires_at, lease_version, last_heartbeat_at, provider_session_id, active_provider_invocation_id, observed_output_text, error, result_ref, canonical_settlement_id, created_at, started_at, finished_at
		 FROM planning_operations
		 WHERE planning_session_id = ? AND result_ref = ?
		 ORDER BY created_at DESC, operation_id DESC
		 LIMIT 1`,
		trimmedSessionID,
		trimmedResultRef,
	), &row)
	if err == sql.ErrNoRows {
		return PlanningOperation{}, false, nil
	}
	if err != nil {
		return PlanningOperation{}, false, fmt.Errorf("get planning operation by result ref: %w", err)
	}
	return row, true, nil
}

func (s *PlanningStore) ListLatestOperationsByResultRefs(ctx context.Context, planningSessionID string, resultRefs []string) (map[string]PlanningOperation, error) {
	trimmedSessionID := strings.TrimSpace(planningSessionID)
	if trimmedSessionID == "" {
		return nil, fmt.Errorf("planning session id is required")
	}
	trimmedRefs := make([]string, 0, len(resultRefs))
	seen := make(map[string]struct{}, len(resultRefs))
	for _, resultRef := range resultRefs {
		trimmed := strings.TrimSpace(resultRef)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		trimmedRefs = append(trimmedRefs, trimmed)
	}
	if len(trimmedRefs) == 0 {
		return map[string]PlanningOperation{}, nil
	}
	placeholders := make([]string, 0, len(trimmedRefs))
	args := make([]any, 0, len(trimmedRefs)+1)
	args = append(args, trimmedSessionID)
	for _, resultRef := range trimmedRefs {
		placeholders = append(placeholders, "?")
		args = append(args, resultRef)
	}
	query := `SELECT operation_id, acp_run_id, planning_session_id, planning_context_snapshot_id, type, status, request_payload_json, lease_owner, lease_expires_at, lease_version, last_heartbeat_at, provider_session_id, active_provider_invocation_id, observed_output_text, error, result_ref, canonical_settlement_id, created_at, started_at, finished_at
		 FROM planning_operations
		 WHERE planning_session_id = ? AND result_ref IN (` + strings.Join(placeholders, ",") + `)
		 ORDER BY created_at DESC, operation_id DESC`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list planning operations by result refs: %w", err)
	}
	defer rows.Close()
	items := make(map[string]PlanningOperation, len(trimmedRefs))
	for rows.Next() {
		var row PlanningOperation
		if err := scanPlanningOperation(rows, &row); err != nil {
			return nil, fmt.Errorf("scan planning operation by result refs: %w", err)
		}
		if row.ResultRef == nil {
			continue
		}
		resultRef := strings.TrimSpace(*row.ResultRef)
		if resultRef == "" {
			continue
		}
		if _, exists := items[resultRef]; exists {
			continue
		}
		items[resultRef] = row
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate planning operations by result refs: %w", err)
	}
	return items, nil
}

func (s *PlanningStore) ListOperationsByStatusPage(ctx context.Context, afterCreatedAt, afterOperationID string, limit int, statuses ...PlanningOperationStatus) ([]PlanningOperation, error) {
	if len(statuses) == 0 {
		return nil, nil
	}
	if limit <= 0 {
		limit = DefaultPlanningOperationListLimit
	}
	trimmedAfterCreatedAt := strings.TrimSpace(afterCreatedAt)
	trimmedAfterOperationID := strings.TrimSpace(afterOperationID)
	if trimmedAfterCreatedAt == "" && trimmedAfterOperationID != "" {
		return nil, fmt.Errorf("planning operation cursor timestamp is required when operation cursor id is set")
	}
	placeholders := make([]string, 0, len(statuses))
	args := make([]any, 0, len(statuses)+5)
	for _, status := range statuses {
		if !isValidPlanningOperationStatus(status) {
			return nil, fmt.Errorf("invalid planning operation status: %q", status)
		}
		placeholders = append(placeholders, "?")
		args = append(args, status)
	}
	args = append(args, trimmedAfterCreatedAt, trimmedAfterCreatedAt, trimmedAfterCreatedAt, trimmedAfterOperationID, limit)
	query := `SELECT operation_id, acp_run_id, planning_session_id, planning_context_snapshot_id, type, status, request_payload_json, lease_owner, lease_expires_at, lease_version, last_heartbeat_at, provider_session_id, active_provider_invocation_id, observed_output_text, error, result_ref, canonical_settlement_id, created_at, started_at, finished_at
		 FROM planning_operations
		 WHERE status IN (` + strings.Join(placeholders, ", ") + `)
		   AND (? = '' OR created_at > ? OR (created_at = ? AND operation_id > ?))
		 ORDER BY created_at ASC, operation_id ASC
		 LIMIT ?`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list planning operations by status page: %w", err)
	}
	defer rows.Close()
	items := make([]PlanningOperation, 0)
	for rows.Next() {
		var row PlanningOperation
		if err := scanPlanningOperation(rows, &row); err != nil {
			return nil, fmt.Errorf("scan planning operation by status page: %w", err)
		}
		items = append(items, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate planning operations by status page: %w", err)
	}
	return items, nil
}

func (s *PlanningStore) ClaimOperation(ctx context.Context, operationID string, leaseOwner string, leaseExpiresAt string, heartbeatAt string) (PlanningOperation, bool, error) {
	return s.claimOperation(ctx, operationID, leaseOwner, leaseExpiresAt, heartbeatAt, nil)
}

// ClaimOperationWithObserver reports bounded SQLite contention for callers
// that expose claim-retry telemetry.
func (s *PlanningStore) ClaimOperationWithObserver(ctx context.Context, operationID string, leaseOwner string, leaseExpiresAt string, heartbeatAt string, observe func(sqlitex.Stats)) (PlanningOperation, bool, error) {
	return s.claimOperation(ctx, operationID, leaseOwner, leaseExpiresAt, heartbeatAt, observe)
}

func (s *PlanningStore) claimOperation(ctx context.Context, operationID string, leaseOwner string, leaseExpiresAt string, heartbeatAt string, observe func(sqlitex.Stats)) (PlanningOperation, bool, error) {
	var claimed PlanningOperation
	var claimedOK bool
	_, err := sqlitex.Retry(ctx, func() error {
		var retryErr error
		claimed, claimedOK, retryErr = s.claimOperationOnce(ctx, operationID, leaseOwner, leaseExpiresAt, heartbeatAt)
		return retryErr
	}, sqlitex.Options{Observe: observe})
	return claimed, claimedOK, err
}

func (s *PlanningStore) claimOperationOnce(ctx context.Context, operationID string, leaseOwner string, leaseExpiresAt string, heartbeatAt string) (PlanningOperation, bool, error) {
	trimmedOperationID := strings.TrimSpace(operationID)
	trimmedLeaseOwner := strings.TrimSpace(leaseOwner)
	trimmedLeaseExpiresAt := strings.TrimSpace(leaseExpiresAt)
	trimmedHeartbeatAt := strings.TrimSpace(heartbeatAt)
	if trimmedOperationID == "" || trimmedLeaseOwner == "" || trimmedLeaseExpiresAt == "" || trimmedHeartbeatAt == "" {
		return PlanningOperation{}, false, fmt.Errorf("operation id and lease fields are required")
	}

	// Queued-only claim. The due decision is transitioned to dispatched in this
	// transaction with the lease claim, so a second worker cannot launch the
	// same retry and a crash leaves an unambiguous durable owner.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return PlanningOperation{}, false, fmt.Errorf("begin claim planning operation: %w", err)
	}
	var retryDecisionID, retryDueAt string
	err = tx.QueryRowContext(ctx, `SELECT retry_decision_id FROM retry_decisions
		WHERE subject_kind = 'planning_invocation' AND planning_operation_id = ?
		  AND decision = 'retry' AND dispatch_state = 'pending'
		ORDER BY created_at DESC, retry_decision_id DESC LIMIT 1`, trimmedOperationID).Scan(&retryDecisionID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return PlanningOperation{}, false, rollbackPlanningTx(tx, "claim planning operation", err)
	}
	if retryDecisionID != "" {
		if err := tx.QueryRowContext(ctx, `SELECT due_at FROM retry_decisions WHERE retry_decision_id = ?`, retryDecisionID).Scan(&retryDueAt); err != nil {
			return PlanningOperation{}, false, rollbackPlanningTx(tx, "claim planning operation", err)
		}
		dueAt, dueErr := parseRetryTime(retryDueAt)
		nowAt, nowErr := parseRetryTime(trimmedHeartbeatAt)
		if dueErr != nil || nowErr != nil {
			return PlanningOperation{}, false, rollbackPlanningTx(tx, "claim planning operation", fmt.Errorf("parse planning retry due time: %w", errors.Join(dueErr, nowErr)))
		}
		if dueAt.After(nowAt) {
			return PlanningOperation{}, false, rollbackPlanningTx(tx, "claim planning operation", nil)
		}
	}
	row := tx.QueryRowContext(ctx,
		`UPDATE planning_operations
		 SET status = ?, lease_owner = ?, lease_expires_at = ?, lease_version = lease_version + 1,
		     last_heartbeat_at = ?, started_at = CASE WHEN TRIM(COALESCE(started_at, '')) = '' THEN ? ELSE started_at END,
		     finished_at = NULL
		 WHERE operation_id = ? AND status = ?
		 RETURNING operation_id, acp_run_id, planning_session_id, planning_context_snapshot_id, type, status,
		           request_payload_json, lease_owner, lease_expires_at, lease_version, last_heartbeat_at,
		           provider_session_id, active_provider_invocation_id, observed_output_text, error, result_ref,
		           canonical_settlement_id, created_at, started_at, finished_at`,
		PlanningOperationStatusRunning,
		trimmedLeaseOwner,
		trimmedLeaseExpiresAt,
		trimmedHeartbeatAt,
		trimmedHeartbeatAt,
		trimmedOperationID,
		PlanningOperationStatusQueued,
	)
	var claimed PlanningOperation
	if err := scanPlanningOperation(row, &claimed); err != nil {
		if err == sql.ErrNoRows {
			return PlanningOperation{}, false, rollbackPlanningTx(tx, "claim planning operation", nil)
		}
		return PlanningOperation{}, false, rollbackPlanningTx(tx, "claim planning operation", fmt.Errorf("claim planning operation: %w", err))
	}
	if retryDecisionID != "" {
		result, err := tx.ExecContext(ctx, `UPDATE retry_decisions SET dispatch_state = 'dispatched', dispatched_at = ? WHERE retry_decision_id = ? AND dispatch_state = 'pending'`, trimmedHeartbeatAt, retryDecisionID)
		if err != nil {
			return PlanningOperation{}, false, rollbackPlanningTx(tx, "claim planning operation", fmt.Errorf("mark planning retry dispatched: %w", err))
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return PlanningOperation{}, false, rollbackPlanningTx(tx, "claim planning operation", err)
		}
		if changed != 1 {
			return PlanningOperation{}, false, rollbackPlanningTx(tx, "claim planning operation", ErrPlanningLeaseConflict)
		}
		claimed.AutomaticRetry = true
	}
	if err := tx.Commit(); err != nil {
		return PlanningOperation{}, false, fmt.Errorf("commit claim planning operation: %w", err)
	}
	return claimed, true, nil
}

func scanPlanningOperation(row scannable, dest *PlanningOperation) error {
	var planningContextSnapshotID sql.NullString
	var leaseExpiresAt sql.NullString
	var lastHeartbeatAt sql.NullString
	var providerSessionID sql.NullString
	var activeProviderInvocationID sql.NullString
	var observedOutputText sql.NullString
	var operationError sql.NullString
	var resultRef sql.NullString
	var canonicalSettlementID sql.NullString
	var startedAt sql.NullString
	var finishedAt sql.NullString

	if err := row.Scan(
		&dest.OperationID,
		&dest.AcpRunID,
		&dest.PlanningSessionID,
		&planningContextSnapshotID,
		&dest.Type,
		&dest.Status,
		&dest.RequestPayloadJSON,
		&dest.LeaseOwner,
		&leaseExpiresAt,
		&dest.LeaseVersion,
		&lastHeartbeatAt,
		&providerSessionID,
		&activeProviderInvocationID,
		&observedOutputText,
		&operationError,
		&resultRef,
		&canonicalSettlementID,
		&dest.CreatedAt,
		&startedAt,
		&finishedAt,
	); err != nil {
		return err
	}
	dest.PlanningContextSnapshotID = nullableStringFromSQL(planningContextSnapshotID)
	dest.LeaseExpiresAt = nullableStringFromSQL(leaseExpiresAt)
	dest.LastHeartbeatAt = nullableStringFromSQL(lastHeartbeatAt)
	dest.ProviderSessionID = nullableStringFromSQL(providerSessionID)
	dest.ActiveProviderInvocationID = nullableStringFromSQL(activeProviderInvocationID)
	dest.ObservedOutputText = normalizedNullableString(nullableStringFromSQL(observedOutputText))
	dest.Error = nullableStringFromSQL(operationError)
	dest.ResultRef = nullableStringFromSQL(resultRef)
	dest.CanonicalSettlementID = nullableStringFromSQL(canonicalSettlementID)
	dest.StartedAt = nullableStringFromSQL(startedAt)
	dest.FinishedAt = nullableStringFromSQL(finishedAt)
	return nil
}

func (s *PlanningStore) RenewOperationLease(ctx context.Context, operationID string, leaseOwner string, leaseVersion int, leaseExpiresAt string, heartbeatAt string) error {
	trimmedOperationID := strings.TrimSpace(operationID)
	trimmedLeaseOwner := strings.TrimSpace(leaseOwner)
	trimmedLeaseExpiresAt := strings.TrimSpace(leaseExpiresAt)
	trimmedHeartbeatAt := strings.TrimSpace(heartbeatAt)
	if trimmedOperationID == "" || trimmedLeaseOwner == "" || trimmedLeaseExpiresAt == "" || trimmedHeartbeatAt == "" {
		return fmt.Errorf("operation id and lease fields are required")
	}
	query := fmt.Sprintf(`UPDATE planning_operations
		 SET lease_expires_at = ?, last_heartbeat_at = ?
		 WHERE operation_id = ? AND lease_owner = ? AND lease_version = ? AND status = 'running' AND %s`,
		timestamporder.ComparisonSQL("lease_expires_at", "?", ">"),
	)
	result, err := s.db.ExecContext(ctx, query,
		trimmedLeaseExpiresAt,
		trimmedHeartbeatAt,
		trimmedOperationID,
		trimmedLeaseOwner,
		leaseVersion,
		trimmedHeartbeatAt,
	)
	if err != nil {
		return fmt.Errorf("renew planning operation lease: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("renew planning operation lease rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("planning operation lease lost")
	}
	return nil
}
