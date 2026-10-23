package providerrequest

import (
	"context"
	"database/sql"
	"fmt"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/remote"
	"time"
)

// BindBackendTx binds a prepared request inside its invocation activation
// transaction. Remote binding IDs come from retained allocation ownership.
func BindBackendTx(ctx context.Context, tx *sql.Tx, b Binding, ref remote.Reference, now time.Time) error {
	switch b.Backend {
	case "", providerexecution.BackendLocal:
		if ref != (remote.Reference{}) || b.RemoteAllocation != 0 {
			return fmt.Errorf("local activation cannot contain remote identity")
		}
		return BindTx(ctx, tx, b, now)
	case providerexecution.BackendOpenShell:
		r, e := remote.ReadRuntime(ctx, tx, ref.Allocation)
		if e != nil {
			return e
		}
		if r.Reference != ref || r.Owner.Kind != string(b.Owner.Kind) || r.Owner.InvocationID != b.Owner.InvocationID || r.Owner.LeaseOwner != b.Lease.Owner || r.Owner.LeaseVersion != b.Lease.Version {
			return fmt.Errorf("remote activation ownership conflict")
		}
		b.ID = r.Owner.BindingID
		b.RemoteAllocation = ref.Allocation
		return BindRemoteTx(ctx, tx, b, ref, now)
	default:
		return fmt.Errorf("unsupported provider execution backend")
	}
}
