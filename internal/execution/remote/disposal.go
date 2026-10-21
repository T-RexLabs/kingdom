package remote

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"google.golang.org/protobuf/proto"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
)

// DisposalInventory revalidates every retained execution and its output. It
// never infers closure from admission, cancellation, or transport completion.
func DisposalInventory(ctx context.Context, q Reader, owner Owner, ref Reference) (managed.Owner, []*pb.CommandIdentity, error) {
	r, e := requireOwner(ctx, q, owner, ref, true)
	if e != nil {
		return managed.Owner{}, nil, e
	}
	if !r.AdmissionClosed {
		return managed.Owner{}, nil, ErrAdmissionClosed
	}
	rows, e := q.QueryContext(ctx, `SELECT operation_id FROM remote_operations WHERE allocation=? AND kind='execute' ORDER BY operation_id`, ref.Allocation)
	if e != nil {
		return managed.Owner{}, nil, e
	}
	var ids []string
	for rows.Next() {
		var id string
		if e = rows.Scan(&id); e != nil {
			break
		}
		ids = append(ids, id)
	}
	if e == nil {
		e = rows.Err()
	}
	rows.Close()
	if e != nil {
		return managed.Owner{}, nil, e
	}
	var expected managed.Owner
	var identities []*pb.CommandIdentity
	for _, id := range ids {
		notStarted, err := ReadExecutionNotStarted(ctx, q, owner, ref, id)
		if err != nil {
			return expected, nil, err
		}
		if notStarted {
			continue
		}
		receipt, found, e := ReadManagedReceipt(ctx, q, owner, ref, id)
		if e != nil {
			return expected, nil, e
		}
		if !found {
			return expected, nil, ErrConflict
		}
		candidate, _, _, e := managedExpectation(ctx, q, owner, ref, id)
		if e != nil {
			return expected, nil, e
		}
		if len(identities) == 0 {
			expected = candidate
		} else if expected.GatewayID != candidate.GatewayID || expected.WorkspaceID != candidate.WorkspaceID || expected.SubjectID != candidate.SubjectID || !proto.Equal(expected.Runtime, candidate.Runtime) {
			return expected, nil, ErrConflict
		}
		identities = append(identities, receipt.Identity)
	}
	if len(identities) == 0 || len(identities) > managed.MaxRuntimeCommands {
		return expected, nil, ErrConflict
	}
	return expected, identities, nil
}

func inventoryBytes(ctx context.Context, q Reader, owner Owner, ref Reference) ([]byte, error) {
	_, ids, e := DisposalInventory(ctx, q, owner, ref)
	if e != nil {
		return nil, e
	}
	type record struct {
		ID, Digest string
		NotStarted bool
		Identity   []byte
	}
	identities := map[string][]byte{}
	for _, id := range ids {
		raw, e := managed.Canonical(id)
		if e != nil {
			return nil, e
		}
		identities[id.RequestId] = raw
	}
	rows, e := q.QueryContext(ctx, `SELECT operation_id,payload_digest FROM remote_operations WHERE allocation=? AND kind='execute' ORDER BY operation_id`, ref.Allocation)
	if e != nil {
		return nil, e
	}
	var records []record
	for rows.Next() {
		var r record
		if e = rows.Scan(&r.ID, &r.Digest); e != nil {
			break
		}
		r.Identity = identities[r.ID]
		r.NotStarted = r.Identity == nil
		records = append(records, r)
	}
	if e == nil {
		e = rows.Err()
	}
	rows.Close()
	if e != nil {
		return nil, e
	}
	return json.Marshal(records)
}

// PrepareRuntimeDisposalTx freezes the exact complete inventory before any
// Finalize RPC. All fresh managed admission must reject this durable fence.
func PrepareRuntimeDisposalTx(ctx context.Context, tx *sql.Tx, owner Owner, ref Reference) error {
	raw, e := inventoryBytes(ctx, tx, owner, ref)
	if e != nil {
		return e
	}
	return allocationBlob(ctx, tx, "remote_runtime_disposal_intents", "inventory", ref.Allocation, raw)
}
func allocationBlob(ctx context.Context, tx *sql.Tx, table, column string, allocation int64, raw []byte) error {
	var saved []byte
	e := tx.QueryRowContext(ctx, `SELECT `+column+` FROM `+table+` WHERE allocation=?`, allocation).Scan(&saved)
	if e == nil {
		if !bytes.Equal(raw, saved) {
			return ErrConflict
		}
		return nil
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return e
	}
	_, e = tx.ExecContext(ctx, `INSERT INTO `+table+`(allocation,`+column+`) VALUES(?,?)`, allocation, raw)
	return e
}
func requireDisposalInventory(ctx context.Context, q Reader, owner Owner, ref Reference) error {
	raw, e := inventoryBytes(ctx, q, owner, ref)
	if e != nil {
		return e
	}
	var saved []byte
	if e = q.QueryRowContext(ctx, `SELECT inventory FROM remote_runtime_disposal_intents WHERE allocation=?`, ref.Allocation).Scan(&saved); e != nil {
		return e
	}
	if !bytes.Equal(raw, saved) {
		return ErrConflict
	}
	return nil
}

// RequireNoDisposalIntent prevents fresh dispatch after the inventory is sealed.
func RequireNoDisposalIntent(ctx context.Context, q Reader, allocation int64) error {
	var count int
	if e := q.QueryRowContext(ctx, `SELECT count(*) FROM remote_runtime_disposal_intents WHERE allocation=?`, allocation).Scan(&count); e != nil {
		return e
	}
	if count != 0 {
		return ErrAdmissionClosed
	}
	return nil
}
func validateRuntimeArchive(ctx context.Context, q Reader, owner Owner, ref Reference, archive *pb.RuntimeArchive) error {
	if e := requireDisposalInventory(ctx, q, owner, ref); e != nil {
		return e
	}
	expected, ids, e := DisposalInventory(ctx, q, owner, ref)
	if e != nil {
		return e
	}
	if managed.ValidateArchive(archive, expected, ids) != nil {
		return ErrConflict
	}
	for _, command := range archive.Manifest.Commands {
		receipt, found, e := ReadManagedReceipt(ctx, q, owner, ref, command.Identity.RequestId)
		if e != nil {
			return e
		}
		if !found || !bytes.Equal(receipt.ReceiptSha256, command.ReceiptSha256) || !proto.Equal(receipt.Output, command.Output) {
			return ErrConflict
		}
	}
	return nil
}

// RecordRuntimeArchiveTx retains the independently checked gateway manifest.
// It does not establish credential revocation, workspace export or disposal.
func RecordRuntimeArchiveTx(ctx context.Context, tx *sql.Tx, owner Owner, ref Reference, archive *pb.RuntimeArchive) error {
	if e := validateRuntimeArchive(ctx, tx, owner, ref, archive); e != nil {
		return e
	}
	raw, e := managed.Canonical(archive)
	if e != nil {
		return e
	}
	return allocationBlob(ctx, tx, "remote_runtime_archives", "archive", ref.Allocation, raw)
}
func ReadRuntimeArchive(ctx context.Context, q Reader, owner Owner, ref Reference) (*pb.RuntimeArchive, error) {
	if _, e := requireOwner(ctx, q, owner, ref, true); e != nil {
		return nil, e
	}
	var raw []byte
	if e := q.QueryRowContext(ctx, `SELECT archive FROM remote_runtime_archives WHERE allocation=?`, ref.Allocation).Scan(&raw); e != nil {
		return nil, e
	}
	var archive pb.RuntimeArchive
	if e := proto.Unmarshal(raw, &archive); e != nil {
		return nil, e
	}
	if e := validateRuntimeArchive(ctx, q, owner, ref, &archive); e != nil {
		return nil, e
	}
	return &archive, nil
}
