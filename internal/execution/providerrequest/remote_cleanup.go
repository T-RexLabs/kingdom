package providerrequest

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/remote"
)

// FenceRemoteCleanupTx closes admission for an exact historical allocation.
// It needs no current lease and grants no new execution. A replacement owner
// cannot be affected by a stale cleanup request.
func FenceRemoteCleanupTx(ctx context.Context, tx *sql.Tx, owner remote.Owner, ref remote.Reference, now time.Time) error {
	if now.IsZero() {
		return remote.ErrConflict
	}
	r, e := remote.ReadRuntime(ctx, tx, ref.Allocation)
	if e != nil {
		return e
	}
	if r.Owner != owner || r.Reference != ref {
		return remote.ErrConflict
	}
	b, e := Get(ctx, tx, owner.BindingID)
	if e == nil {
		if b.Backend != providerexecution.BackendOpenShell || b.RemoteAllocation != ref.Allocation || string(b.Owner.Kind) != owner.Kind || b.Owner.InvocationID != owner.InvocationID || b.Lease.Owner != owner.LeaseOwner || b.Lease.Version != owner.LeaseVersion || b.Identity.InstanceID != ref.MainInstanceID {
			return remote.ErrConflict
		}
		if b.AdmissionClosedAt == "" {
			if _, e = tx.ExecContext(ctx, `UPDATE provider_requests SET admission_closed_at=? WHERE binding_id=?`, now.UTC().Format(time.RFC3339Nano), b.ID); e != nil {
				return e
			}
		}
	} else if !errors.Is(e, sql.ErrNoRows) {
		return e
	}
	return CloseOwnerRemoteAdmissionTx(ctx, tx, Owner{Kind: OwnerKind(owner.Kind), InvocationID: owner.InvocationID}, now.UTC().Format(time.RFC3339Nano))
}
