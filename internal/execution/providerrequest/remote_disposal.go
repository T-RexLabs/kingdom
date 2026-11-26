package providerrequest

import (
	"context"
	"database/sql"
	"kingdom/internal/execution/remote"
)

func ownerRemoteReservations(ctx context.Context, q Reader, owner Owner) ([]remote.Reservation, error) {
	rows, err := q.QueryContext(ctx, `SELECT allocation FROM remote_runtimes WHERE owner_kind=? AND invocation_id=? ORDER BY allocation`, owner.Kind, owner.InvocationID)
	if err != nil {
		return nil, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	var result []remote.Reservation
	for _, id := range ids {
		r, e := remote.ReadRuntime(ctx, q, id)
		if e != nil {
			return nil, e
		}
		result = append(result, r)
	}
	return result, nil
}

// RequireOwnerRemoteDisposed checks every historical reservation. It does not
// require provider settlement, so failed preparation can dispose an empty runtime.
func RequireOwnerRemoteDisposed(ctx context.Context, q Reader, owner Owner) error {
	reservations, err := ownerRemoteReservations(ctx, q, owner)
	if err != nil {
		return err
	}
	for _, r := range reservations {
		never, e := remote.ReadRuntimeNeverCreated(ctx, q, r.Owner, r.Reference)
		if e != nil {
			return e
		}
		if never {
			continue
		}
		if e = RequireRemoteCapabilitiesClosed(ctx, q, owner, r.Reference.Allocation); e != nil {
			return e
		}
		if e = remote.RequireRuntimeDisposed(ctx, q, r.Owner, r.Reference); e != nil {
			return openResources(OpenUnproven, "remote runtime deletion remains unresolved")
		}
	}
	return nil
}
func abandonRemotePreparationsTx(ctx context.Context, tx *sql.Tx, owner Owner) error {
	reservations, err := ownerRemoteReservations(ctx, tx, owner)
	if err != nil {
		return err
	}
	for _, r := range reservations {
		if r.Reference.SandboxID == "" {
			if err = remote.MarkRuntimeNeverCreatedTx(ctx, tx, r.Owner, r.Reference); err != nil {
				return err
			}
		}
	}
	return RequireOwnerRemoteDisposed(ctx, tx, owner)
}
func requireRemoteOwnersDisposed(ctx context.Context, q Reader, query string, args ...any) error {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	var owners []Owner
	for rows.Next() {
		var owner Owner
		if err = rows.Scan(&owner.Kind, &owner.InvocationID); err != nil {
			rows.Close()
			return err
		}
		owners = append(owners, owner)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, owner := range owners {
		if err = RequireOwnerRemoteDisposed(ctx, q, owner); err != nil {
			return err
		}
	}
	return nil
}

// RequireRemoteDisposalPrerequisites is the canonical worker prerequisite. A
// bound request must have its exact settled or abandoned outcome. Success owes
// its sealed export; unbound failed preparation may discard without inventing
// a provider terminal. It performs no network or deletion mutation.
func RequireRemoteDisposalPrerequisites(ctx context.Context, q Reader, owner remote.Owner, ref remote.Reference) error {
	r, err := remote.ReadRuntime(ctx, q, ref.Allocation)
	if err != nil {
		return err
	}
	if r.Owner != owner || r.Reference != ref || !r.AdmissionClosed {
		return remote.ErrConflict
	}
	canonicalOwner := Owner{Kind: OwnerKind(owner.Kind), InvocationID: owner.InvocationID}
	if err = RequireRemoteCapabilitiesClosed(ctx, q, canonicalOwner, ref.Allocation); err != nil {
		return err
	}
	if _, err = remote.ReadWriterClosures(ctx, q, owner, ref); err != nil {
		return err
	}
	b, found, err := ForOwner(ctx, q, canonicalOwner)
	if err != nil {
		return err
	}
	if !found {
		c, e := loadOwner(ctx, q, canonicalOwner)
		if e != nil {
			return e
		}
		if c.invocationState != "issued" {
			return remote.ErrConflict
		}
		return nil
	}
	if _, err = ReadRemoteWriterClosure(ctx, q, b.ID); err != nil {
		return err
	}
	if err = requireBoundSettlement(ctx, q, b); err != nil {
		return err
	}
	c, err := loadOwner(ctx, q, canonicalOwner)
	if err != nil {
		return err
	}
	if c.invocationState == "settled_success" {
		return remote.RequireWorkspaceExport(ctx, q, owner, ref)
	}
	return nil
}
