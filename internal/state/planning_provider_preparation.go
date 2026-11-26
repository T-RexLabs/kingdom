package state

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/sqlitex"
)

func (s *PlanningStore) ReservePlanningProviderInvocation(ctx context.Context, req CreatePlanningProviderInvocationRequest) (PlanningProviderInvocation, error) {
	create, err := normalizeCreatePlanningProviderInvocationRequest(req)
	if err != nil {
		return PlanningProviderInvocation{}, err
	}
	var pin providerrequest.WorkspacePin
	if req.WorkspacePath != "" {
		session, ok, e := s.GetSession(ctx, req.PlanningSessionID)
		if e != nil {
			return PlanningProviderInvocation{}, e
		}
		if !ok || session.ProjectRoot != req.WorkspacePath {
			return PlanningProviderInvocation{}, fmt.Errorf("planning workspace does not match session")
		}
		pin, err = providerrequest.PinWorkspace(session.ProjectRoot)
		if err != nil {
			return PlanningProviderInvocation{}, err
		}
	}
	var inv PlanningProviderInvocation
	_, err = sqlitex.Transaction(ctx, s.db, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		inv, err = createPlanningProviderInvocationTx(ctx, tx, create)
		if err != nil {
			return err
		}
		if err = linkPlanningOperationActiveInvocationTx(ctx, tx, inv, planningProviderInvocationOpening{LeaseOwner: req.LeaseOwner, LeaseVersion: req.LeaseVersion, OpenedAt: s.leaseNow().UTC().Format(time.RFC3339Nano)}); err != nil {
			return err
		}
		owner := providerrequest.Owner{Kind: providerrequest.OwnerPlanning, InvocationID: inv.PlanningProviderInvocationID}
		lease := providerrequest.Lease{Owner: req.LeaseOwner, Version: int64(req.LeaseVersion)}
		if err := providerrequest.ReservePreparationTx(ctx, tx, owner, lease, s.leaseNow()); err != nil {
			return err
		}
		if req.WorkspacePath != "" {
			var path string
			if err := tx.QueryRowContext(ctx, `SELECT project_root FROM planning_sessions WHERE planning_session_id=?`, req.PlanningSessionID).Scan(&path); err != nil {
				return err
			}
			if path != req.WorkspacePath {
				return fmt.Errorf("planning workspace changed")
			}
			return providerrequest.RecordPreparationWorkspaceTx(ctx, tx, owner, lease, pin)
		}
		return nil
	}, sqlitex.Options{})
	return inv, err
}

// AbandonPlanningPreparation releases only an issued, effect-free preparation.
// Recovery may close the historical reservation under its current canonical
// lease; it cannot activate that reservation as the new lease owner.
func (s *PlanningStore) AbandonPlanningPreparation(ctx context.Context, operation PlanningOperation) (bool, error) {
	if operation.ActiveProviderInvocationID == nil {
		return false, nil
	}
	abandoned := false
	_, err := sqlitex.Transaction(ctx, s.db, func(ctx context.Context, tx *sql.Tx) error {
		inv, found, err := loadPlanningProviderInvocationTx(ctx, tx, *operation.ActiveProviderInvocationID)
		if err != nil {
			return err
		}
		if !found || inv.InvocationState != PlanningProviderInvocationStateIssued {
			return nil
		}
		if inv.PlanningOperationID != operation.OperationID {
			return fmt.Errorf("preparation operation mismatch")
		}
		if err = providerrequest.AbandonPreparationTx(ctx, tx, providerrequest.Owner{Kind: providerrequest.OwnerPlanning, InvocationID: inv.PlanningProviderInvocationID}); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE planning_operations SET active_provider_invocation_id=NULL WHERE operation_id=? AND active_provider_invocation_id=? AND status='running' AND lease_owner=? AND lease_version=? AND canonical_settlement_id IS NULL`, operation.OperationID, inv.PlanningProviderInvocationID, operation.LeaseOwner, operation.LeaseVersion)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return ErrPlanningLeaseConflict
		}
		_, err = tx.ExecContext(ctx, `UPDATE planning_provider_invocations SET invocation_state='superseded',updated_at=? WHERE planning_provider_invocation_id=? AND invocation_state='issued'`, s.leaseNow().UTC().Format(time.RFC3339Nano), inv.PlanningProviderInvocationID)
		abandoned = err == nil
		return err
	}, sqlitex.Options{})
	return abandoned, err
}
