package providerrequest

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"kingdom/internal/execution/access"
)

// ReservePreparationTx retains the exact lease that may prepare and activate an
// issued invocation. The caller links that invocation in this transaction first.
func ReservePreparationTx(ctx context.Context, tx *sql.Tx, owner Owner, lease Lease, now time.Time) error {
	c, err := loadOwner(ctx, tx, owner)
	if err != nil {
		return err
	}
	expiry, e := time.Parse(time.RFC3339Nano, c.expiry)
	if now.IsZero() || e != nil || !now.Before(expiry) || c.invocationState != "issued" || c.activeInvocation != owner.InvocationID || c.leaseOwner != lease.Owner || c.leaseVersion != lease.Version || lease.Owner == "" || lease.Version < 1 || c.settlement != "" {
		return fmt.Errorf("preparation requires current issued owner and lease")
	}
	if owner.Kind == OwnerRun && (c.mode != "provider_backed" || c.runStatus != "running" || (c.state != "queued" && c.state != "starting" && c.state != "running")) {
		return fmt.Errorf("run cannot prepare provider")
	}
	if owner.Kind == OwnerPlanning && (c.state != "running" || c.currentOperation != c.id) {
		return fmt.Errorf("planning operation cannot prepare provider")
	}
	if _, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO provider_preparations(owner_kind,invocation_id,lease_owner,lease_version) VALUES(?,?,?,?)`, owner.Kind, owner.InvocationID, lease.Owner, lease.Version); err != nil {
		return err
	}
	if err := RequirePreparationLeaseTx(ctx, tx, owner, lease); err != nil {
		return err
	}
	return access.CaptureInvocationTx(ctx, tx, string(owner.Kind), owner.InvocationID)
}

// RequirePreparationLeaseTx also supports existing atomic local activation
// callers that have not performed resource-bearing preparation.
func RequirePreparationLeaseTx(ctx context.Context, q Reader, owner Owner, lease Lease) error {
	var saved Lease
	var closed bool
	err := q.QueryRowContext(ctx, `SELECT lease_owner,lease_version,admission_closed FROM provider_preparations WHERE owner_kind=? AND invocation_id=?`, owner.Kind, owner.InvocationID).Scan(&saved.Owner, &saved.Version, &closed)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	if saved != lease || closed {
		return fmt.Errorf("provider preparation lease is stale or closed")
	}
	return nil
}

// AbandonPreparationTx proves that an issued invocation has no remaining provider effects.
// It fences the historical reservation, including when a recovery lease owns the
// canonical operation now. It never creates provider completion evidence.
func AbandonPreparationTx(ctx context.Context, tx *sql.Tx, owner Owner) error {
	c, err := loadOwner(ctx, tx, owner)
	if err != nil {
		return err
	}
	if c.invocationState != "issued" {
		return fmt.Errorf("only issued preparation can be abandoned")
	}
	var count int
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM provider_preparations WHERE owner_kind=? AND invocation_id=?`, owner.Kind, owner.InvocationID).Scan(&count); err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("preparation reservation missing")
	}
	if err = abandonRemotePreparationsTx(ctx, tx, owner); err != nil {
		return err
	}
	_, bound, err := ForOwner(ctx, tx, owner)
	if err != nil {
		return err
	}
	if bound {
		return fmt.Errorf("prepared invocation has a provider binding")
	}
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM provider_process_guards WHERE provider_invocation_id=?`, owner.InvocationID).Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return fmt.Errorf("prepared invocation has local process ownership")
	}
	_, err = tx.ExecContext(ctx, `UPDATE provider_preparations SET admission_closed=1 WHERE owner_kind=? AND invocation_id=?`, owner.Kind, owner.InvocationID)
	return err
}

// RequirePreparationCleanupLeaseTx checks historical ownership without granting
// fresh work. Cleanup must remain possible after its permanent admission fence.
func RequirePreparationCleanupLeaseTx(ctx context.Context, q Reader, owner Owner, lease Lease) error {
	if lease.Owner == "" || lease.Version < 1 {
		return fmt.Errorf("cleanup lease is required")
	}
	var saved Lease
	if err := q.QueryRowContext(ctx, `SELECT lease_owner,lease_version FROM provider_preparations WHERE owner_kind=? AND invocation_id=?`, owner.Kind, owner.InvocationID).Scan(&saved.Owner, &saved.Version); err != nil {
		return err
	}
	if saved != lease {
		return fmt.Errorf("preparation cleanup lease conflicts with historical owner")
	}
	return nil
}
