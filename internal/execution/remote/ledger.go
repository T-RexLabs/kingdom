package remote

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type Reader interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

const runtimeColumns = `owner_kind,invocation_id,binding_id,lease_owner,lease_version,worker_id,gateway_id,workspace_id,allocation,creation_id,sandbox_id,generation,main_instance_id,spec_digest,admission_closed`

func ReadRuntime(ctx context.Context, q Reader, allocation int64) (Reservation, error) {
	var r Reservation
	err := q.QueryRowContext(ctx, `SELECT `+runtimeColumns+` FROM remote_runtimes WHERE allocation=?`, allocation).Scan(&r.Owner.Kind, &r.Owner.InvocationID, &r.Owner.BindingID, &r.Owner.LeaseOwner, &r.Owner.LeaseVersion, &r.Reference.WorkerID, &r.Reference.GatewayID, &r.Reference.WorkspaceID, &r.Reference.Allocation, &r.Reference.CreationID, &r.Reference.SandboxID, &r.Reference.Generation, &r.Reference.MainInstanceID, &r.SpecDigest, &r.AdmissionClosed)
	return r, err
}

// ReserveRuntimeTx reserves allocation and creation dispatch together. Replays
// must match every immutable input. It never grants permission to send an RPC.
func ReserveRuntimeTx(ctx context.Context, tx *sql.Tx, owner Owner, target Target, creationID, specDigest string) (Reservation, error) {
	if !validOwner(owner) || !validTarget(target) || !validUUID(creationID) || !validDigest(specDigest) {
		return Reservation{}, ErrConflict
	}
	var allocation int64
	err := tx.QueryRowContext(ctx, `SELECT allocation FROM remote_runtimes WHERE creation_id=?`, creationID).Scan(&allocation)
	if err == nil {
		r, err := ReadRuntime(ctx, tx, allocation)
		if err != nil {
			return r, err
		}
		if r.Owner != owner || r.Reference.Target != target || r.SpecDigest != specDigest {
			return Reservation{}, ErrConflict
		}
		return r, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Reservation{}, err
	}
	allocation, err = allocateRuntimeTx(ctx, tx)
	if err != nil {
		return Reservation{}, err
	}
	return insertRuntimeTx(ctx, tx, owner, target, creationID, specDigest, allocation)
}
func allocateRuntimeTx(ctx context.Context, tx *sql.Tx) (int64, error) {
	result, err := tx.ExecContext(ctx, `INSERT INTO remote_allocations DEFAULT VALUES`)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}
func insertRuntimeTx(ctx context.Context, tx *sql.Tx, owner Owner, target Target, creationID, specDigest string, allocation int64) (Reservation, error) {
	_, err := tx.ExecContext(ctx, `INSERT INTO remote_runtimes(allocation,creation_id,owner_kind,invocation_id,binding_id,lease_owner,lease_version,worker_id,gateway_id,workspace_id,spec_digest) VALUES(?,?,?,?,?,?,?,?,?,?,?)`, allocation, creationID, owner.Kind, owner.InvocationID, owner.BindingID, owner.LeaseOwner, owner.LeaseVersion, target.WorkerID, target.GatewayID, target.WorkspaceID, specDigest)
	if err != nil {
		return Reservation{}, fmt.Errorf("reserve remote runtime: %w", err)
	}

	if err = insertOperation(ctx, tx, Operation{ID: creationID, Allocation: allocation, Kind: Create, PayloadDigest: specDigest}); err != nil {
		return Reservation{}, err
	}
	return ReadRuntime(ctx, tx, allocation)
}

func requireOwner(ctx context.Context, q Reader, owner Owner, ref Reference, exact bool) (Reservation, error) {
	r, err := ReadRuntime(ctx, q, ref.Allocation)
	if err != nil {
		return r, err
	}
	if !validOwner(owner) || r.Owner != owner || r.Reference.Target != ref.Target || r.Reference.CreationID != ref.CreationID {
		return Reservation{}, ErrConflict
	}
	if exact && (!complete(ref) || r.Reference != ref) {
		return Reservation{}, ErrConflict
	}
	return r, nil
}

// BindRuntimeTx records a complete immutable identity after creation was sent.
// It accepts late exact observations after admission closes, but never replaces
// a sandbox generation. It is not runtime readiness or closure evidence.
func BindRuntimeTx(ctx context.Context, tx *sql.Tx, owner Owner, ref Reference) error {
	if !complete(ref) {
		return ErrConflict
	}
	r, err := requireOwner(ctx, tx, owner, ref, false)
	if err != nil {
		return err
	}
	op, err := ReadOperation(ctx, tx, ref.CreationID)
	if err != nil {
		return err
	}
	if op.Delivery == Prepared {
		return ErrConflict
	}
	if r.Reference.SandboxID != "" {
		if r.Reference != ref {
			return ErrConflict
		}
		return nil
	}
	_, err = tx.ExecContext(ctx, `UPDATE remote_runtimes SET sandbox_id=?,generation=?,main_instance_id=? WHERE allocation=? AND sandbox_id=''`, ref.SandboxID, ref.Generation, ref.MainInstanceID, ref.Allocation)
	return err
}

func CloseAdmissionTx(ctx context.Context, tx *sql.Tx, owner Owner, ref Reference) error {
	if _, err := requireOwner(ctx, tx, owner, ref, false); err != nil {
		return err
	}
	r, err := ReadRuntime(ctx, tx, ref.Allocation)
	if err != nil {
		return err
	}
	if r.Reference != ref {
		return ErrConflict
	}
	_, err = tx.ExecContext(ctx, `UPDATE remote_runtimes SET admission_closed=1 WHERE allocation=?`, ref.Allocation)
	return err
}

func insertOperation(ctx context.Context, tx *sql.Tx, op Operation) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO remote_operations(operation_id,allocation,kind,payload_digest) VALUES(?,?,?,?)`, op.ID, op.Allocation, op.Kind, op.PayloadDigest)
	return err
}

func ReadOperation(ctx context.Context, q Reader, id string) (Operation, error) {
	var op Operation
	err := q.QueryRowContext(ctx, `SELECT operation_id,allocation,kind,payload_digest,delivery,admission_digest FROM remote_operations WHERE operation_id=?`, id).Scan(&op.ID, &op.Allocation, &op.Kind, &op.PayloadDigest, &op.Delivery, &op.AdmissionDigest)
	return op, err
}

// PrepareCommandTx reserves an already-authorized command. The canonical
// provider command claim must be created in this same transaction by the caller.
// Sequence must be the next sequence; a prior exact command can be observed after
// admission closes. Planning command admission remains unsupported.
func PrepareCommandTx(ctx context.Context, tx *sql.Tx, owner Owner, command CommandReference) (Operation, error) {
	r, err := requireOwner(ctx, tx, owner, command.Runtime, true)
	if err != nil {
		return Operation{}, err
	}
	if owner.Kind != "run" || command.OwnerBinding != owner.BindingID || !text(command.ClaimID) || !validUUID(command.CommandID) || !validDigest(command.PayloadDigest) || command.Sequence <= 0 {
		return Operation{}, ErrConflict
	}
	var allocation, sequence int64
	var binding, claim, digest string
	err = tx.QueryRowContext(ctx, `SELECT allocation,binding_id,claim_id,payload_digest,sequence FROM remote_commands WHERE command_id=?`, command.CommandID).Scan(&allocation, &binding, &claim, &digest, &sequence)
	if err == nil {
		if allocation != command.Runtime.Allocation || binding != command.OwnerBinding || claim != command.ClaimID || digest != command.PayloadDigest || sequence != command.Sequence {
			return Operation{}, ErrConflict
		}
		return ReadOperation(ctx, tx, command.CommandID)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Operation{}, err
	}
	if r.AdmissionClosed {
		return Operation{}, ErrAdmissionClosed
	}
	var next int64
	if err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence),0)+1 FROM remote_commands WHERE binding_id=?`, owner.BindingID).Scan(&next); err != nil {
		return Operation{}, err
	}
	if command.Sequence != next {
		return Operation{}, ErrConflict
	}
	op := Operation{ID: command.CommandID, Allocation: command.Runtime.Allocation, Kind: Execute, PayloadDigest: command.PayloadDigest, Delivery: Prepared}
	if err = insertOperation(ctx, tx, op); err != nil {
		return Operation{}, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO remote_commands(command_id,allocation,binding_id,claim_id,payload_digest,sequence) VALUES(?,?,?,?,?,?)`, command.CommandID, command.Runtime.Allocation, command.OwnerBinding, command.ClaimID, command.PayloadDigest, command.Sequence)
	return op, err
}

// PrepareStopTx records a stop after the canonical cancellation fence. Multiple
// stop operation IDs are not allowed: uncertain dispatch must be reconciled.
func PrepareStopTx(ctx context.Context, tx *sql.Tx, owner Owner, ref Reference, id, digest string) (Operation, error) {
	r, err := requireOwner(ctx, tx, owner, ref, true)
	if err != nil {
		return Operation{}, err
	}
	if !validUUID(id) || !validDigest(digest) || !r.AdmissionClosed {
		return Operation{}, ErrConflict
	}
	var existing string
	err = tx.QueryRowContext(ctx, `SELECT operation_id FROM remote_operations WHERE allocation=? AND kind='stop'`, ref.Allocation).Scan(&existing)
	if err == nil {
		op, err := ReadOperation(ctx, tx, existing)
		if err != nil {
			return op, err
		}
		if op.ID != id || op.PayloadDigest != digest {
			return Operation{}, ErrConflict
		}
		return op, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Operation{}, err
	}
	op := Operation{ID: id, Allocation: ref.Allocation, Kind: Stop, PayloadDigest: digest, Delivery: Prepared}
	err = insertOperation(ctx, tx, op)
	return op, err
}

// ClaimDispatchTx changes Prepared to Uncertain before the network effect.
// Only true authorizes this ledger's single dispatch. The transaction must
// commit successfully before sending; an ambiguous commit is not permission.
// A crash after commit but before send remains uncertain, never auto-retried.
func ClaimDispatchTx(ctx context.Context, tx *sql.Tx, owner Owner, ref Reference, id string) (bool, error) {
	r, err := requireOwner(ctx, tx, owner, ref, false)
	if err != nil {
		return false, err
	}
	if r.Reference != ref {
		return false, ErrConflict
	}
	op, err := ReadOperation(ctx, tx, id)
	if err != nil {
		return false, err
	}
	if op.Allocation != ref.Allocation {
		return false, ErrConflict
	}
	if op.Delivery != Prepared {
		return false, nil
	}
	if op.Kind != Stop && r.AdmissionClosed {
		var purpose string
		err := tx.QueryRowContext(ctx, `SELECT purpose FROM managed_executions WHERE operation_id=?`, id).Scan(&purpose)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return false, err
		}
		if purpose != string(WorkspaceExport) {
			return false, ErrAdmissionClosed
		}
		if _, err = ReadWriterClosures(ctx, tx, owner, ref); err != nil {
			return false, err
		}
	}
	result, err := tx.ExecContext(ctx, `UPDATE remote_operations SET delivery='delivery_uncertain' WHERE operation_id=? AND delivery='prepared'`, id)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	return n == 1, err
}

// RecordAdmissionTx records a matching transport response without resolving any
// command or runtime obligation. Late responses remain admissible after fencing.
func RecordAdmissionTx(ctx context.Context, tx *sql.Tx, owner Owner, ref Reference, id, payloadDigest, admissionDigest string) error {
	r, err := requireOwner(ctx, tx, owner, ref, false)
	if err != nil {
		return err
	}
	if r.Reference != ref || !validDigest(admissionDigest) {
		return ErrConflict
	}
	op, err := ReadOperation(ctx, tx, id)
	if err != nil {
		return err
	}
	if op.Allocation != ref.Allocation || op.PayloadDigest != payloadDigest || op.Delivery == Prepared {
		return ErrConflict
	}
	if op.Delivery == Admitted {
		if op.AdmissionDigest != admissionDigest {
			return ErrConflict
		}
		return nil
	}
	_, err = tx.ExecContext(ctx, `UPDATE remote_operations SET delivery='admitted',admission_digest=? WHERE operation_id=? AND delivery='delivery_uncertain'`, admissionDigest, id)
	return err
}

// PendingOperations returns unresolved dispatch records for observation only.
// No state returned here permits resending an uncertain operation.
func PendingOperations(ctx context.Context, q Reader, allocation int64) ([]Operation, error) {
	rows, err := q.QueryContext(ctx, `SELECT operation_id,allocation,kind,payload_digest,delivery,admission_digest FROM remote_operations WHERE allocation=? ORDER BY operation_id`, allocation)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ops []Operation
	for rows.Next() {
		var op Operation
		if err := rows.Scan(&op.ID, &op.Allocation, &op.Kind, &op.PayloadDigest, &op.Delivery, &op.AdmissionDigest); err != nil {
			return nil, err
		}
		ops = append(ops, op)
	}
	return ops, rows.Err()
}
