package remote

import (
	"bytes"
	"context"
	"kingdom/internal/execution/openshell/managed"
)

// RequireWorkspaceExport checks the sealed download record against the exact
// successful export receipt. Publication must separately reopen and hash the
// retained archive; metadata is not authority to apply unverified local bytes.
func RequireWorkspaceExport(ctx context.Context, q Reader, owner Owner, ref Reference) error {
	if _, err := requireOwner(ctx, q, owner, ref, true); err != nil {
		return err
	}
	rows, err := q.QueryContext(ctx, `SELECT m.operation_id FROM managed_executions m JOIN remote_operations o ON o.operation_id=m.operation_id WHERE o.allocation=? AND m.purpose='workspace_export'`, ref.Allocation)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if len(ids) != 1 {
		return ErrConflict
	}
	id := ids[0]
	receipt, found, err := ReadManagedReceipt(ctx, q, owner, ref, id)
	if err != nil {
		return err
	}
	if !found || !managed.SuccessfulProcess(receipt) {
		return ErrConflict
	}
	var purpose, name, archive, manifest string
	var size int64
	var digest []byte
	err = q.QueryRowContext(ctx, `SELECT purpose,spool_name,archive_sha256,archive_bytes,manifest_sha256,receipt_sha256 FROM remote_workspace_downloads WHERE operation_id=?`, id).Scan(&purpose, &name, &archive, &size, &manifest, &digest)
	if err != nil {
		return err
	}
	if purpose != string(WorkspaceExport) || name != id+".zip" || !validDigest(archive) || !validDigest(manifest) || size < 1 || !bytes.Equal(digest, receipt.ReceiptSha256) {
		return ErrConflict
	}
	return nil
}
