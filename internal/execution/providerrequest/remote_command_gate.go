package providerrequest

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/remote"
)

// ActiveRemoteCommandTx checks canonical command authority in the caller's
// transaction. It never issues provider dispatch or accepts local process proof.
func ActiveRemoteCommandTx(ctx context.Context, tx *sql.Tx, identity providerexecution.RequestIdentity, now time.Time) (Binding, remote.Reservation, error) {
	b, r, err := ActiveRemoteNativeTx(ctx, tx, identity, now)
	if err == nil && b.Owner.Kind != OwnerRun {
		err = fmt.Errorf("only run requests admit remote commands")
	}
	return b, r, err
}

// ActiveRemoteNativeTx permits the exact active run or planning request to
// admit a confined helper. It does not grant shell authority to planning.
func ActiveRemoteNativeTx(ctx context.Context, tx *sql.Tx, identity providerexecution.RequestIdentity, now time.Time) (Binding, remote.Reservation, error) {
	b, err := ByIdentity(ctx, tx, identity)
	if err != nil {
		return b, remote.Reservation{}, err
	}
	r, err := remoteBindingOwner(ctx, tx, b)
	if err != nil {
		return b, r, err
	}
	if r.AdmissionClosed || b.TerminalReference != nil {
		return b, r, fmt.Errorf("remote command admission closed")
	}
	if err = requireAdmission(ctx, tx, b, now); err != nil {
		return b, r, err
	}
	var serverID string
	if err = tx.QueryRowContext(ctx, `SELECT m.operation_id FROM managed_executions m JOIN remote_operations o ON o.operation_id=m.operation_id WHERE o.allocation=? AND m.purpose='provider_server'`, r.Reference.Allocation).Scan(&serverID); err != nil {
		return b, r, err
	}
	if _, purpose, err := remote.ReadManagedIdentity(ctx, tx, r.Owner, r.Reference, serverID); err != nil || purpose != remote.ProviderServer {
		return b, r, fmt.Errorf("remote command provider identity unavailable")
	}
	if _, terminal, err := remote.ReadManagedReceipt(ctx, tx, r.Owner, r.Reference, serverID); err != nil || terminal {
		return b, r, fmt.Errorf("remote command provider closed or unverified")
	}
	return b, r, nil
}
