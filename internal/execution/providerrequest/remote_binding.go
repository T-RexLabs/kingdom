package providerrequest

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/remote"
)

// BindRemoteTx records identity in the canonical invocation activation
// transaction. Creation admission is not readiness or closure evidence, and
// IssueDispatchTx continues to reject this backend until verified execution is
// integrated. No local guardian facts can satisfy this binding's resource debt.
func BindRemoteTx(ctx context.Context, tx *sql.Tx, b Binding, ref remote.Reference, now time.Time) error {
	if b.Backend != providerexecution.BackendOpenShell || b.RemoteAllocation <= 0 || b.RemoteAllocation != ref.Allocation {
		return fmt.Errorf("complete remote provider runtime binding is required")
	}
	reservation, err := remote.ReadRuntime(ctx, tx, ref.Allocation)
	if err != nil {
		return err
	}
	expected := remote.Owner{Kind: string(b.Owner.Kind), InvocationID: b.Owner.InvocationID, BindingID: b.ID, LeaseOwner: b.Lease.Owner, LeaseVersion: b.Lease.Version}
	if reservation.Owner != expected || reservation.Reference != ref || reservation.AdmissionClosed || ref.SandboxID == "" || ref.Generation == "" || ref.MainInstanceID == "" || b.Identity.InstanceID != ref.MainInstanceID {
		return fmt.Errorf("remote provider reservation identity conflict")
	}
	// The provider/plugin generation in RequestIdentity is independent of the
	// sandbox generation in Reference; both remain pinned, never substituted.
	operation, err := remote.ReadOperation(ctx, tx, ref.CreationID)
	if err != nil {
		return err
	}
	if operation.Allocation != ref.Allocation || operation.Kind != remote.Create || operation.PayloadDigest != reservation.SpecDigest || operation.Delivery != remote.Admitted || operation.AdmissionDigest == "" {
		return fmt.Errorf("remote creation admission is not recorded")
	}
	lease, err := preparationLease(ctx, tx, b.Owner)
	if err != nil {
		return err
	}
	if lease != b.Lease {
		return fmt.Errorf("remote preparation lease conflict")
	}
	var local int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM provider_process_guards WHERE provider_invocation_id=?`, b.Owner.InvocationID).Scan(&local); err != nil {
		return err
	}
	if local != 0 {
		return fmt.Errorf("remote provider conflicts with local process ownership")
	}
	return bindRequestTx(ctx, tx, b, now)
}

// Preparation is read from the issued invocation's durable reservation. It is
// not accepted from an HTTP request or a model-generated payload. Every
// subsequent mutation must still validate these facts in its own transaction.
type Preparation struct {
	// ResourceID is the canonical attempt or planning operation, never caller-selected.
	ResourceID string
	Owner      Owner
	Lease      Lease
}

func preparationLease(ctx context.Context, q Reader, owner Owner) (Lease, error) {
	var lease Lease
	var closed bool
	if err := q.QueryRowContext(ctx, `SELECT lease_owner,lease_version,admission_closed FROM provider_preparations WHERE owner_kind=? AND invocation_id=?`, owner.Kind, owner.InvocationID).Scan(&lease.Owner, &lease.Version, &closed); err != nil {
		return Lease{}, err
	}
	if closed || lease.Owner == "" || lease.Version < 1 {
		return Lease{}, fmt.Errorf("provider preparation is closed")
	}
	return lease, nil
}
func (a *Authority) Preparation(ctx context.Context, invocationID string) (Preparation, error) {
	if invocationID == "" {
		return Preparation{}, fmt.Errorf("invocation identity is required")
	}
	tx, err := a.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Preparation{}, err
	}
	defer tx.Rollback()
	result, err := a.preparation(ctx, tx, invocationID)
	if err != nil {
		return Preparation{}, err
	}
	if err := tx.Commit(); err != nil {
		return Preparation{}, err
	}
	return result, nil
}

func (a *Authority) preparation(ctx context.Context, tx Reader, invocationID string) (Preparation, error) {
	rows, err := tx.QueryContext(ctx, `SELECT owner_kind FROM provider_preparations WHERE invocation_id=?`, invocationID)
	if err != nil {
		return Preparation{}, err
	}
	var kinds []OwnerKind
	for rows.Next() {
		var kind OwnerKind
		if err := rows.Scan(&kind); err != nil {
			rows.Close()
			return Preparation{}, err
		}
		kinds = append(kinds, kind)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return Preparation{}, err
	}
	if len(kinds) != 1 {
		return Preparation{}, fmt.Errorf("issued preparation must have exactly one canonical owner")
	}
	owner := Owner{Kind: kinds[0], InvocationID: invocationID}
	lease, err := preparationLease(ctx, tx, owner)
	if err != nil {
		return Preparation{}, err
	}
	current, err := loadOwner(ctx, tx, owner)
	if err != nil {
		return Preparation{}, err
	}
	now := a.now()
	expiry, e := time.Parse(time.RFC3339Nano, current.expiry)
	validState := current.state == "running" || owner.Kind == OwnerRun && (current.state == "queued" || current.state == "starting")
	if now.IsZero() || e != nil || !now.Before(expiry) || !validState || current.invocationState != "issued" || current.activeInvocation != invocationID || current.leaseOwner != lease.Owner || current.leaseVersion != lease.Version || current.settlement != "" || owner.Kind == OwnerRun && (current.mode != "provider_backed" || current.runStatus != "running") || owner.Kind == OwnerPlanning && current.currentOperation != current.id {
		return Preparation{}, fmt.Errorf("issued preparation owner or lease is no longer current")
	}
	return Preparation{Owner: owner, Lease: lease, ResourceID: current.id}, nil
}
