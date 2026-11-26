package remote

import (
	"context"
	"database/sql"
	"google.golang.org/protobuf/proto"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
)

// RecordRuntimeDeletionTx accepts only a proof returned by the authenticated
// managed archive client, never a caller-created receipt or name observation.
func RecordRuntimeDeletionTx(ctx context.Context, tx *sql.Tx, owner Owner, ref Reference, proof managed.DeletionProof) error {
	if proof.Receipt() == nil {
		return ErrConflict
	}
	archive, e := ReadRuntimeArchive(ctx, tx, owner, ref)
	if e != nil {
		return e
	}
	expected, _, e := DisposalInventory(ctx, tx, owner, ref)
	if e != nil {
		return e
	}
	receipt := proof.Receipt()
	if managed.ValidateDeletionReceipt(receipt, expected, archive) != nil {
		return ErrConflict
	}
	raw, e := managed.Canonical(receipt)
	if e != nil {
		return e
	}
	return allocationBlob(ctx, tx, "remote_runtime_deletions", "receipt", ref.Allocation, raw)
}

// RequireRuntimeDisposed revalidates exact archived command evidence and the
// gateway's durable exact-ID deletion receipt. Historical name-based absence
// rows from migration0019 never establish closure.
func RequireRuntimeDisposed(ctx context.Context, q Reader, owner Owner, ref Reference) error {
	archive, e := ReadRuntimeArchive(ctx, q, owner, ref)
	if e != nil {
		return e
	}
	expected, _, e := DisposalInventory(ctx, q, owner, ref)
	if e != nil {
		return e
	}
	var raw []byte
	if e = q.QueryRowContext(ctx, `SELECT receipt FROM remote_runtime_deletions WHERE allocation=?`, ref.Allocation).Scan(&raw); e != nil {
		return e
	}
	var receipt pb.RuntimeDeletionReceipt
	if e = proto.Unmarshal(raw, &receipt); e != nil {
		return e
	}
	return managed.ValidateDeletionReceipt(&receipt, expected, archive)
}
