package remote

import (
	"context"
	"database/sql"
)

// MarkRuntimeNeverCreatedTx atomically fences a reservation whose create RPC
// was never claimed. Missing native names after uncertain delivery are not proof.
func MarkRuntimeNeverCreatedTx(ctx context.Context, tx *sql.Tx, owner Owner, ref Reference) error {
	r, err := requireOwner(ctx, tx, owner, ref, false)
	if err != nil {
		return err
	}
	if r.Reference != ref || ref.SandboxID != "" || ref.Generation != "" || ref.MainInstanceID != "" {
		return ErrConflict
	}
	op, err := ReadOperation(ctx, tx, ref.CreationID)
	if err != nil {
		return err
	}
	if op.Kind != Create || op.Allocation != ref.Allocation || op.Delivery != Prepared || op.AdmissionDigest != "" || op.PayloadDigest != r.SpecDigest {
		return ErrConflict
	}
	var others int
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM remote_operations WHERE allocation=? AND operation_id<>?`, ref.Allocation, ref.CreationID).Scan(&others); err != nil {
		return err
	}
	if others != 0 {
		return ErrConflict
	}
	if err = CloseAdmissionTx(ctx, tx, owner, ref); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO remote_runtimes_never_created(allocation,creation_id)VALUES(?,?)`, ref.Allocation, ref.CreationID)
	return err
}
func ReadRuntimeNeverCreated(ctx context.Context, q Reader, owner Owner, ref Reference) (bool, error) {
	r, err := requireOwner(ctx, q, owner, ref, false)
	if err != nil {
		return false, err
	}
	var id string
	err = q.QueryRowContext(ctx, `SELECT creation_id FROM remote_runtimes_never_created WHERE allocation=?`, ref.Allocation).Scan(&id)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if r.Reference != ref || !r.AdmissionClosed || id != ref.CreationID || ref.SandboxID != "" || ref.Generation != "" || ref.MainInstanceID != "" {
		return false, ErrConflict
	}
	op, err := ReadOperation(ctx, q, id)
	if err != nil {
		return false, err
	}
	if op.Kind != Create || op.Allocation != ref.Allocation || op.Delivery != Prepared || op.AdmissionDigest != "" || op.PayloadDigest != r.SpecDigest {
		return false, ErrConflict
	}
	var others int
	if err = q.QueryRowContext(ctx, `SELECT count(*) FROM remote_operations WHERE allocation=? AND operation_id<>?`, ref.Allocation, id).Scan(&others); err != nil {
		return false, err
	}
	if others != 0 {
		return false, ErrConflict
	}
	return true, nil
}
