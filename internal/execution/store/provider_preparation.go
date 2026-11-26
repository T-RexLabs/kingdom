package store

import (
	"context"
	"database/sql"
	"fmt"

	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/sqlitex"
)

type ReserveProviderInvocationRequest struct {
	CreateProviderInvocationRequest
	WorkspacePath        string
	ExpectedLeaseOwner   string
	ExpectedLeaseVersion int64
}

// ReserveProviderInvocation commits an issued owner before provider startup.
// A failed or ambiguous commit never authorizes Prepare; recovery reads the row.
func (s *Store) ReserveProviderInvocation(ctx context.Context, req ReserveProviderInvocationRequest) (ProviderInvocation, error) {
	create, err := normalizeCreateProviderInvocationRequest(req.CreateProviderInvocationRequest)
	if err != nil {
		return ProviderInvocation{}, err
	}
	if create.ProviderSessionID != "" {
		return ProviderInvocation{}, fmt.Errorf("preparation cannot claim a provider session")
	}
	var pin providerrequest.WorkspacePin
	if req.WorkspacePath != "" {
		pin, err = providerrequest.PinWorkspace(req.WorkspacePath)
		if err != nil {
			return ProviderInvocation{}, err
		}
	}
	var inv ProviderInvocation
	_, err = sqlitex.Transaction(ctx, s.db, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		inv, err = createProviderInvocationTx(ctx, tx, create)
		if err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE attempt_executions SET active_provider_invocation_id=? WHERE attempt_id=? AND (active_provider_invocation_id IS NULL OR active_provider_invocation_id='' OR active_provider_invocation_id=?) AND lease_owner=? AND lease_version=? AND canonical_settlement_id IS NULL`, inv.ProviderInvocationID, inv.AttemptID, inv.ProviderInvocationID, req.ExpectedLeaseOwner, req.ExpectedLeaseVersion)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return fmt.Errorf("preparation owner conflict")
		}
		owner := providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: inv.ProviderInvocationID}
		lease := providerrequest.Lease{Owner: req.ExpectedLeaseOwner, Version: req.ExpectedLeaseVersion}
		if err := providerrequest.ReservePreparationTx(ctx, tx, owner, lease, s.currentTime()); err != nil {
			return err
		}
		if req.WorkspacePath != "" {
			return providerrequest.RecordPreparationWorkspaceTx(ctx, tx, owner, lease, pin)
		}
		return nil
	}, sqlitex.Options{})
	return inv, err
}

// abandonExpiredPreparationTx runs only after the caller proved the queued
// preparation lease expired, before granting a replacement generation.
func abandonExpiredPreparationTx(ctx context.Context, tx *sql.Tx, attemptID, now string) error {
	var id, state string
	err := tx.QueryRowContext(ctx, `SELECT p.provider_invocation_id,p.invocation_state FROM attempt_executions a JOIN provider_invocations p ON p.provider_invocation_id=a.active_provider_invocation_id WHERE a.attempt_id=?`, attemptID).Scan(&id, &state)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	if state != "issued" {
		return nil
	}
	if err = providerrequest.AbandonPreparationTx(ctx, tx, providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: id}); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE provider_invocations SET invocation_state='invalidated',invalid_reason='preparation_lease_expired',updated_at=? WHERE provider_invocation_id=?`, now, id); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE attempt_executions SET active_provider_invocation_id=NULL WHERE attempt_id=? AND active_provider_invocation_id=?`, attemptID, id)
	return err
}
