package remote

import (
	"context"
	"database/sql"
)

// MarkExecutionNotStartedTx closes only a dispatch that was never claimed.
// Admission must already be fenced. It does not manufacture native evidence.
func MarkExecutionNotStartedTx(ctx context.Context, tx *sql.Tx, owner Owner, ref Reference, id string) error {
	r, err := requireOwner(ctx, tx, owner, ref, true)
	if err != nil {
		return err
	}
	if !r.AdmissionClosed {
		return ErrAdmissionClosed
	}
	op, err := ReadOperation(ctx, tx, id)
	if err != nil {
		return err
	}
	if op.Allocation != ref.Allocation || op.Kind != Execute || op.Delivery != Prepared {
		return ErrConflict
	}
	var count int
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM managed_execution_identities WHERE operation_id=?`, id).Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return ErrConflict
	}
	_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO remote_unstarted_executions(operation_id) VALUES(?)`, id)
	return err
}

// ReadExecutionNotStarted rechecks the retained no-dispatch fact against its owner.
func ReadExecutionNotStarted(ctx context.Context, q Reader, owner Owner, ref Reference, id string) (bool, error) {
	if _, err := requireOwner(ctx, q, owner, ref, true); err != nil {
		return false, err
	}
	op, err := ReadOperation(ctx, q, id)
	if err != nil {
		return false, err
	}
	if op.Allocation != ref.Allocation || op.Kind != Execute {
		return false, ErrConflict
	}
	var count int
	if err = q.QueryRowContext(ctx, `SELECT count(*) FROM remote_unstarted_executions WHERE operation_id=?`, id).Scan(&count); err != nil {
		return false, err
	}
	if count == 0 {
		return false, nil
	}
	if op.Delivery != Prepared {
		return false, ErrConflict
	}
	if err = q.QueryRowContext(ctx, `SELECT count(*) FROM managed_execution_identities WHERE operation_id=?`, id).Scan(&count); err != nil {
		return false, err
	}
	if count != 0 {
		return false, ErrConflict
	}
	return true, nil
}
