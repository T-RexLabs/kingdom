package providerrequest

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// RequireGuardianAdmissionTx fences a new command guardian acknowledgement in
// the transaction that inserts it. Existing immutable receipts grant no new
// execution and are checked by their canonical owner before this gate.
func RequireGuardianAdmissionTx(ctx context.Context, tx *sql.Tx, bindingID string, lease Lease, ordinal int64, now time.Time) error {
	binding, err := Get(ctx, tx, bindingID)
	if err != nil {
		return err
	}
	if ordinal < 1 || binding.Lease != lease {
		return fmt.Errorf("provider guardian command or lease conflict")
	}
	if err := requireAdmission(ctx, tx, binding, now); err != nil {
		return err
	}
	var pending int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM provider_command_calls WHERE binding_id=? AND command_ordinal=? AND guard_id IS NULL AND completed_at IS NULL`, binding.ID, ordinal).Scan(&pending); err != nil {
		return err
	}
	if pending != 1 {
		return fmt.Errorf("provider guardian requires an exact pending command claim")
	}
	return nil
}
