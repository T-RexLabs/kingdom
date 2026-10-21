package remote

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"google.golang.org/protobuf/proto"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
	"kingdom/internal/workspacearchive"
	transfer "kingdom/internal/workspacetransfer"
)

// VerifyWorkspaceImportTx binds locally validated immutable input to the exact
// closed helper and its retained output. A process receipt alone is insufficient.
func VerifyWorkspaceImportTx(ctx context.Context, tx *sql.Tx, o Owner, r Reference, id string, requests *RequestStore, expected *workspacearchive.Staged) error {
	request, e := requests.ReadManaged(ctx, tx, o, r, id)
	if e != nil {
		return e
	}
	job, e := transfer.DecodeJob(bytes.NewReader(request.Command.Stdin))
	if e != nil {
		return e
	}
	fixed, e := transfer.Command(job)
	if e != nil || !proto.Equal(fixed, request.Command) || job.Mode != "import" {
		return ErrConflict
	}
	var purpose string
	if e = tx.QueryRowContext(ctx, `SELECT purpose FROM managed_executions WHERE operation_id=?`, id).Scan(&purpose); e != nil {
		return e
	}
	if purpose != string(WorkspaceImport) {
		return ErrConflict
	}
	receipt, found, e := ReadManagedReceipt(ctx, tx, o, r, id)
	if e != nil {
		return e
	}
	if !found || !managed.SuccessfulProcess(receipt) {
		return ErrConflict
	}
	rows, e := tx.QueryContext(ctx, `SELECT chunk FROM managed_execution_output WHERE operation_id=? ORDER BY sequence`, id)
	if e != nil {
		return e
	}
	var output []byte
	for rows.Next() {
		var raw []byte
		var chunk pb.OutputChunk
		if e = rows.Scan(&raw); e != nil {
			break
		}
		if e = proto.Unmarshal(raw, &chunk); e != nil {
			break
		}
		if chunk.Stream == pb.OutputStream_OUTPUT_STREAM_STDOUT {
			if len(output)+len(chunk.Data) > transfer.MaxCompletionBytes {
				e = ErrConflict
				break
			}
			output = append(output, chunk.Data...)
		}
	}
	if e == nil {
		e = rows.Err()
	}
	rows.Close()
	if e != nil {
		return e
	}
	if e = transfer.ValidateImport(bytes.NewReader(output), job, expected); e != nil {
		return e
	}
	var archive, manifest string
	var size int64
	var savedReceipt []byte
	e = tx.QueryRowContext(ctx, `SELECT archive_sha256,archive_bytes,manifest_sha256,receipt_sha256 FROM remote_workspace_imports WHERE operation_id=?`, id).Scan(&archive, &size, &manifest, &savedReceipt)
	if e == nil {
		if archive != job.ArchiveSHA256 || size != job.ArchiveBytes || manifest != expected.Digest || !bytes.Equal(savedReceipt, receipt.ReceiptSha256) {
			return ErrConflict
		}
		return nil
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return e
	}
	_, e = tx.ExecContext(ctx, `INSERT INTO remote_workspace_imports(operation_id,archive_sha256,archive_bytes,manifest_sha256,receipt_sha256) VALUES(?,?,?,?,?)`, id, job.ArchiveSHA256, job.ArchiveBytes, expected.Digest, receipt.ReceiptSha256)
	return e
}

// RequireWorkspaceImportTx is shared by every provider-server admission path.
func RequireWorkspaceImportTx(ctx context.Context, q Reader, o Owner, r Reference) error {
	if _, e := requireOwner(ctx, q, o, r, true); e != nil {
		return e
	}
	var id string
	var saved []byte
	e := q.QueryRowContext(ctx, `SELECT m.operation_id,v.receipt_sha256 FROM managed_executions m JOIN remote_operations op ON op.operation_id=m.operation_id JOIN remote_workspace_imports v ON v.operation_id=m.operation_id WHERE op.allocation=? AND m.purpose='workspace_import'`, r.Allocation).Scan(&id, &saved)
	if e != nil {
		return ErrConflict
	}
	receipt, found, e := ReadManagedReceipt(ctx, q, o, r, id)
	if e != nil {
		return e
	}
	if !found || !managed.SuccessfulProcess(receipt) || !bytes.Equal(saved, receipt.ReceiptSha256) {
		return ErrConflict
	}
	return nil
}
