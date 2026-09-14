package state

import (
	"context"
	"fmt"
	"strings"
)

// FinalizeGenerateTitle settles a successful generated-title operation on the
// title lane. It fences both the operation lease and the session's
// current_title_operation_id slot, but intentionally leaves the interactive
// lane untouched.
func (s *PlanningStore) FinalizeGenerateTitle(ctx context.Context, req FinalizeGenerateTitleRequest) error {
	if err := validateFinalizeGenerateTitleRequest(req); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin finalize generate title: %w", err)
	}
	operation, found, err := getPlanningOperation(ctx, tx, req.OperationID)
	if err != nil {
		return rollbackPlanningTx(tx, "finalize generate title", err)
	}
	if !found {
		return rollbackPlanningTx(tx, "finalize generate title", fmt.Errorf("planning operation %q not found", strings.TrimSpace(req.OperationID)))
	}
	if operation.PlanningSessionID != strings.TrimSpace(req.SessionID) {
		return rollbackPlanningTx(tx, "finalize generate title", fmt.Errorf("planning operation %q does not belong to session %q", strings.TrimSpace(req.OperationID), strings.TrimSpace(req.SessionID)))
	}
	if err := validateFinalizeOperationProviderContract(operation, FinalizeOperationRequest{
		OperationID:        req.OperationID,
		ProviderSettlement: req.ProviderSettlement,
	}); err != nil {
		return rollbackPlanningTx(tx, "finalize generate title", err)
	}

	opUpdate := newUpdateBuilder("planning_operations").
		Set("status", string(PlanningOperationStatusSucceeded)).
		Set("active_provider_invocation_id", nil).
		Set("lease_owner", "").
		Set("lease_expires_at", nil).
		SetExpr("lease_version", "lease_version + 1").
		Set("last_heartbeat_at", nil).
		Set("finished_at", strings.TrimSpace(req.FinishedAt))
	if strings.TrimSpace(req.ObservedOutputText) != "" {
		opUpdate.Set("observed_output_text", strings.TrimSpace(req.ObservedOutputText))
	}
	opUpdate.
		Where("operation_id = ?", strings.TrimSpace(req.OperationID)).
		Where("lease_owner = ?", strings.TrimSpace(req.LeaseOwner)).
		Where("lease_version = ?", req.LeaseVersion).
		Where("status = ?", string(PlanningOperationStatusRunning)).
		Where("lease_expires_at > ?", strings.TrimSpace(req.FinishedAt))
	res, err := opUpdate.Exec(ctx, tx)
	if err != nil {
		return rollbackPlanningTx(tx, "finalize generate title", fmt.Errorf("finalize generate title operation: %w", err))
	}
	if affected, err := res.RowsAffected(); err != nil {
		return rollbackPlanningTx(tx, "finalize generate title", fmt.Errorf("finalize generate title operation rows affected: %w", err))
	} else if affected == 0 {
		return rollbackPlanningTx(tx, "finalize generate title", ErrPlanningLeaseConflict)
	}

	generatedTitle := strings.TrimSpace(req.GeneratedTitle)
	sessionResult, err := newUpdateBuilder("planning_sessions").
		Set("updated_at", strings.TrimSpace(req.UpdatedAt)).
		Set("current_title_operation_id", nil).
		SetExpr("title", "CASE WHEN title_state = ? THEN ? ELSE title END", "placeholder", generatedTitle).
		SetExpr("title_state", "CASE WHEN title_state = ? THEN ? ELSE title_state END", "placeholder", "generated").
		Where("planning_session_id = ?", strings.TrimSpace(req.SessionID)).
		Where("current_title_operation_id = ?", strings.TrimSpace(req.OperationID)).
		Exec(ctx, tx)
	if err != nil {
		return rollbackPlanningTx(tx, "finalize generate title", fmt.Errorf("finalize generate title session: %w", err))
	}
	if affected, err := sessionResult.RowsAffected(); err != nil {
		return rollbackPlanningTx(tx, "finalize generate title", fmt.Errorf("finalize generate title session rows affected: %w", err))
	} else if affected == 0 {
		return rollbackPlanningTx(tx, "finalize generate title", ErrPlanningLeaseConflict)
	}

	if _, err := insertPlanningOperationEvent(ctx, tx, *req.TerminalEvent); err != nil {
		return rollbackPlanningTx(tx, "finalize generate title", err)
	}

	if _, err := finalizePlanningProviderSettlementTx(ctx, tx, req.OperationID, req.SessionID, req.ProviderSettlement); err != nil {
		return rollbackPlanningTx(tx, "finalize generate title", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit finalize generate title: %w", err)
	}
	return nil
}

// FinalizeGenerateTitleAbort terminates a generated-title operation as failed
// or cancelled without touching the interactive session lane.
func (s *PlanningStore) FinalizeGenerateTitleAbort(ctx context.Context, req FinalizeGenerateTitleAbortRequest) error {
	if err := validateFinalizeGenerateTitleAbortRequest(req); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin finalize generate title abort: %w", err)
	}
	operation, found, err := getPlanningOperation(ctx, tx, req.OperationID)
	if err != nil {
		return rollbackPlanningTx(tx, "finalize generate title abort", err)
	}
	if !found {
		return rollbackPlanningTx(tx, "finalize generate title abort", fmt.Errorf("planning operation %q not found", strings.TrimSpace(req.OperationID)))
	}
	if operation.PlanningSessionID != strings.TrimSpace(req.SessionID) {
		return rollbackPlanningTx(tx, "finalize generate title abort", fmt.Errorf("planning operation %q does not belong to session %q", strings.TrimSpace(req.OperationID), strings.TrimSpace(req.SessionID)))
	}
	if err := validateFinalizeOperationProviderContract(operation, FinalizeOperationRequest{
		OperationID:        req.OperationID,
		ProviderSettlement: req.ProviderSettlement,
	}); err != nil {
		return rollbackPlanningTx(tx, "finalize generate title abort", err)
	}

	opUpdate := newUpdateBuilder("planning_operations").
		Set("status", string(req.Status)).
		Set("active_provider_invocation_id", nil).
		Set("lease_owner", "").
		Set("lease_expires_at", nil).
		SetExpr("lease_version", "lease_version + 1").
		Set("last_heartbeat_at", nil).
		Set("finished_at", strings.TrimSpace(req.FinishedAt)).
		Set("error", strings.TrimSpace(req.ErrorMessage))
	if strings.TrimSpace(req.ObservedOutputText) != "" {
		opUpdate.Set("observed_output_text", strings.TrimSpace(req.ObservedOutputText))
	}
	opUpdate.
		Where("operation_id = ?", strings.TrimSpace(req.OperationID)).
		Where("lease_owner = ?", strings.TrimSpace(req.LeaseOwner)).
		Where("lease_version = ?", req.LeaseVersion).
		Where("status = ?", string(PlanningOperationStatusRunning))
	res, err := opUpdate.Exec(ctx, tx)
	if err != nil {
		return rollbackPlanningTx(tx, "finalize generate title abort", fmt.Errorf("finalize generate title abort operation: %w", err))
	}
	if affected, err := res.RowsAffected(); err != nil {
		return rollbackPlanningTx(tx, "finalize generate title abort", fmt.Errorf("finalize generate title abort operation rows affected: %w", err))
	} else if affected == 0 {
		return rollbackPlanningTx(tx, "finalize generate title abort", ErrPlanningLeaseConflict)
	}

	sessionResult, err := newUpdateBuilder("planning_sessions").
		Set("updated_at", strings.TrimSpace(req.UpdatedAt)).
		Set("current_title_operation_id", nil).
		Where("planning_session_id = ?", strings.TrimSpace(req.SessionID)).
		Where("current_title_operation_id = ?", strings.TrimSpace(req.OperationID)).
		Exec(ctx, tx)
	if err != nil {
		return rollbackPlanningTx(tx, "finalize generate title abort", fmt.Errorf("finalize generate title abort session: %w", err))
	}
	if affected, err := sessionResult.RowsAffected(); err != nil {
		return rollbackPlanningTx(tx, "finalize generate title abort", fmt.Errorf("finalize generate title abort session rows affected: %w", err))
	} else if affected == 0 {
		return rollbackPlanningTx(tx, "finalize generate title abort", ErrPlanningLeaseConflict)
	}

	if _, err := insertPlanningOperationEvent(ctx, tx, *req.TerminalEvent); err != nil {
		return rollbackPlanningTx(tx, "finalize generate title abort", err)
	}

	if _, err := finalizePlanningProviderSettlementTx(ctx, tx, req.OperationID, req.SessionID, req.ProviderSettlement); err != nil {
		return rollbackPlanningTx(tx, "finalize generate title abort", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit finalize generate title abort: %w", err)
	}
	return nil
}

func validateFinalizeGenerateTitleRequest(req FinalizeGenerateTitleRequest) error {
	if strings.TrimSpace(req.SessionID) == "" || strings.TrimSpace(req.OperationID) == "" {
		return fmt.Errorf("planning session id and operation id are required")
	}
	if strings.TrimSpace(req.LeaseOwner) == "" {
		return fmt.Errorf("planning operation lease owner is required to fence terminal writes")
	}
	if req.LeaseVersion < 0 {
		return fmt.Errorf("planning operation lease version must be non-negative")
	}
	if strings.TrimSpace(req.UpdatedAt) == "" || strings.TrimSpace(req.FinishedAt) == "" {
		return fmt.Errorf("planning operation finalize timestamps are required")
	}
	if strings.TrimSpace(req.GeneratedTitle) == "" {
		return fmt.Errorf("generated title is required for generate_title finalize")
	}
	if req.TerminalEvent == nil {
		return fmt.Errorf("terminal event is required for generate_title finalize")
	}
	if err := validatePlanningOperationEventAppend(*req.TerminalEvent); err != nil {
		return err
	}
	if req.ProviderSettlement != nil {
		if _, err := normalizeFinalizePlanningProviderSettlementRequest(*req.ProviderSettlement); err != nil {
			return err
		}
	}
	return nil
}

func validateFinalizeGenerateTitleAbortRequest(req FinalizeGenerateTitleAbortRequest) error {
	if strings.TrimSpace(req.SessionID) == "" || strings.TrimSpace(req.OperationID) == "" {
		return fmt.Errorf("planning session id and operation id are required")
	}
	if strings.TrimSpace(req.LeaseOwner) == "" {
		return fmt.Errorf("planning operation lease owner is required to fence terminal writes")
	}
	if req.LeaseVersion < 0 {
		return fmt.Errorf("planning operation lease version must be non-negative")
	}
	switch req.Status {
	case PlanningOperationStatusFailed, PlanningOperationStatusCancelled:
		// ok
	default:
		return fmt.Errorf("generate_title abort status must be failed or cancelled, got %q", req.Status)
	}
	if strings.TrimSpace(req.UpdatedAt) == "" || strings.TrimSpace(req.FinishedAt) == "" {
		return fmt.Errorf("planning operation finalize timestamps are required")
	}
	if strings.TrimSpace(req.ErrorMessage) == "" {
		return fmt.Errorf("error message is required for generate_title abort")
	}
	if req.TerminalEvent == nil {
		return fmt.Errorf("terminal event is required for generate_title abort")
	}
	if err := validatePlanningOperationEventAppend(*req.TerminalEvent); err != nil {
		return err
	}
	if req.ProviderSettlement != nil {
		if _, err := normalizeFinalizePlanningProviderSettlementRequest(*req.ProviderSettlement); err != nil {
			return err
		}
	}
	return nil
}
