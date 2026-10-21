package remote

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"errors"

	"google.golang.org/protobuf/proto"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
)

type ExecutionPurpose string

const (
	WorkspaceImport ExecutionPurpose = "workspace_import"
	ProviderServer  ExecutionPurpose = "provider_server"
	Tool            ExecutionPurpose = "tool"
	WorkspaceExport ExecutionPurpose = "workspace_export"
)

// PrepareManagedExecutionTx reserves a secret-free, exact command expectation.
// The caller must check canonical authority in this same transaction. Tool
// executions require a preexisting canonical remote command claim. Server and
// export processes are separate obligations and never borrow a tool claim.
func PrepareManagedExecutionTx(ctx context.Context, tx *sql.Tx, owner Owner, ref Reference, purpose ExecutionPurpose, expected managed.Owner, request *pb.StartExecutionRequest) (Operation, error) {
	reservation, e := requireOwner(ctx, tx, owner, ref, true)
	if e != nil {
		return Operation{}, e
	}
	if managed.ValidateStart(request) != nil || expected.Validate() != nil || expected.GatewayID != ref.GatewayID || expected.WorkspaceID != ref.WorkspaceID || expected.Runtime.SandboxId != ref.SandboxID || expected.Runtime.BoundaryGeneration != ref.Generation || expected.Runtime.MainProcessInstanceId != ref.MainInstanceID || !proto.Equal(expected.Runtime, request.Execution.Runtime) {
		return Operation{}, ErrConflict
	}
	if purpose != ProviderServer && purpose != Tool && purpose != WorkspaceExport && purpose != WorkspaceImport {
		return Operation{}, ErrConflict
	}
	original, e := managed.Digest(request)
	if e != nil {
		return Operation{}, e
	}
	effective, e := managed.Digest(request.Command)
	if e != nil {
		return Operation{}, e
	}
	runtime, e := managed.Canonical(expected.Runtime)
	if e != nil {
		return Operation{}, e
	}
	id := request.Execution.RequestId
	digest := hex.EncodeToString(original)
	var savedPurpose, savedSubject string
	var savedRuntime, savedOriginal, savedEffective []byte
	e = tx.QueryRowContext(ctx, `SELECT purpose,subject_id,runtime,request_sha256,effective_sha256 FROM managed_executions WHERE operation_id=?`, id).Scan(&savedPurpose, &savedSubject, &savedRuntime, &savedOriginal, &savedEffective)
	if e == nil {
		if savedPurpose != string(purpose) || savedSubject != expected.SubjectID || !bytes.Equal(savedRuntime, runtime) || !bytes.Equal(savedOriginal, original) || !bytes.Equal(savedEffective, effective) {
			return Operation{}, ErrConflict
		}
		op, e := ReadOperation(ctx, tx, id)
		if e == nil && (op.Allocation != ref.Allocation || op.Kind != Execute || op.PayloadDigest != digest) {
			return Operation{}, ErrConflict
		}
		return op, e
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return Operation{}, e
	}
	if purpose == WorkspaceExport {
		if !reservation.AdmissionClosed {
			return Operation{}, ErrConflict
		}
		if _, e = ReadWriterClosures(ctx, tx, owner, ref); e != nil {
			return Operation{}, e
		}
	} else if reservation.AdmissionClosed {
		return Operation{}, ErrAdmissionClosed
	}
	if purpose == WorkspaceImport {
		var count int
		if e = tx.QueryRowContext(ctx, `SELECT count(*) FROM remote_operations WHERE allocation=? AND kind='execute'`, ref.Allocation).Scan(&count); e != nil {
			return Operation{}, e
		}
		if count != 0 {
			return Operation{}, ErrConflict
		}
	}
	if purpose == ProviderServer {
		if e = RequireWorkspaceImportTx(ctx, tx, owner, ref); e != nil {
			return Operation{}, e
		}
	}

	var op Operation
	if purpose == Tool {
		op, e = ReadOperation(ctx, tx, id)
		if e != nil {
			return op, e
		}
		var count int
		if e = tx.QueryRowContext(ctx, `SELECT count(*) FROM remote_commands WHERE command_id=? AND allocation=? AND binding_id=? AND payload_digest=?`, id, ref.Allocation, owner.BindingID, digest).Scan(&count); e != nil {
			return op, e
		}
		if count != 1 || op.Kind != Execute || op.Delivery != Prepared || op.PayloadDigest != digest {
			return op, ErrConflict
		}
	} else {
		var count int
		if e = tx.QueryRowContext(ctx, `SELECT count(*) FROM managed_executions m JOIN remote_operations o ON o.operation_id=m.operation_id WHERE o.allocation=? AND m.purpose=?`, ref.Allocation, string(purpose)).Scan(&count); e != nil {
			return op, e
		}
		if count != 0 {
			return op, ErrConflict
		}
		op = Operation{ID: id, Allocation: ref.Allocation, Kind: Execute, PayloadDigest: digest, Delivery: Prepared}
		if e = insertOperation(ctx, tx, op); e != nil {
			return op, e
		}
	}
	_, e = tx.ExecContext(ctx, `INSERT INTO managed_executions(operation_id,purpose,subject_id,output_limit,runtime,request_sha256,effective_sha256) VALUES(?,?,?,?,?,?,?)`, id, string(purpose), expected.SubjectID, request.Command.OutputLimitBytes, runtime, original, effective)
	return op, e
}

func managedExpectation(ctx context.Context, q Reader, owner Owner, ref Reference, id string) (managed.Owner, []byte, []byte, error) {
	if _, e := requireOwner(ctx, q, owner, ref, true); e != nil {
		return managed.Owner{}, nil, nil, e
	}
	op, e := ReadOperation(ctx, q, id)
	if e != nil {
		return managed.Owner{}, nil, nil, e
	}
	if op.Allocation != ref.Allocation || op.Kind != Execute || op.Delivery == Prepared {
		return managed.Owner{}, nil, nil, ErrConflict
	}
	var subject string
	var runtime, original, effective []byte
	if e = q.QueryRowContext(ctx, `SELECT subject_id,runtime,request_sha256,effective_sha256 FROM managed_executions WHERE operation_id=?`, id).Scan(&subject, &runtime, &original, &effective); e != nil {
		return managed.Owner{}, nil, nil, e
	}
	var binding pb.RuntimeBinding
	if e = proto.Unmarshal(runtime, &binding); e != nil {
		return managed.Owner{}, nil, nil, e
	}
	return managed.Owner{GatewayID: ref.GatewayID, WorkspaceID: ref.WorkspaceID, SubjectID: subject, Runtime: &binding}, original, effective, nil
}

// RecordManagedIdentityTx accepts late matching responses after cancellation.
// It does not reopen admission or establish process closure.
func RecordManagedIdentityTx(ctx context.Context, tx *sql.Tx, owner Owner, ref Reference, identity *pb.CommandIdentity) error {
	if identity == nil {
		return ErrConflict
	}
	expected, original, effective, e := managedExpectation(ctx, tx, owner, ref, identity.RequestId)
	if e != nil {
		return e
	}
	if expected.ValidateIdentity(identity) != nil || !bytes.Equal(identity.OriginalRequestSha256, original) || !bytes.Equal(identity.EffectiveSpecSha256, effective) {
		return ErrConflict
	}
	b, e := managed.Canonical(identity)
	if e != nil {
		return e
	}
	return immutableBlob(ctx, tx, "managed_execution_identities", "identity", identity.RequestId, b)
}
func immutableBlob(ctx context.Context, tx *sql.Tx, table, column, id string, value []byte) error {
	var existing []byte
	e := tx.QueryRowContext(ctx, `SELECT `+column+` FROM `+table+` WHERE operation_id=?`, id).Scan(&existing)
	if e == nil {
		if !bytes.Equal(existing, value) {
			return ErrConflict
		}
		return nil
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return e
	}
	_, e = tx.ExecContext(ctx, `INSERT INTO `+table+`(operation_id,`+column+`) VALUES(?,?)`, id, value)
	return e
}
func retainedIdentity(ctx context.Context, q Reader, owner Owner, ref Reference, id string) (*pb.CommandIdentity, error) {
	expected, original, effective, e := managedExpectation(ctx, q, owner, ref, id)
	if e != nil {
		return nil, e
	}
	var b []byte
	if e = q.QueryRowContext(ctx, `SELECT identity FROM managed_execution_identities WHERE operation_id=?`, id).Scan(&b); e != nil {
		return nil, e
	}
	var i pb.CommandIdentity
	if e = proto.Unmarshal(b, &i); e != nil {
		return nil, e
	}
	if expected.ValidateIdentity(&i) != nil || i.RequestId != id || !bytes.Equal(i.OriginalRequestSha256, original) || !bytes.Equal(i.EffectiveSpecSha256, effective) {
		return nil, ErrConflict
	}
	return &i, nil
}

// AppendManagedOutputTx saves exact sequential output. Exact retries are safe;
// a different chunk at a saved sequence or any gap fails without advancing it.
func AppendManagedOutputTx(ctx context.Context, tx *sql.Tx, owner Owner, ref Reference, id string, chunk *pb.OutputChunk) error {
	if _, e := retainedIdentity(ctx, tx, owner, ref, id); e != nil {
		return e
	}
	if chunk == nil || managed.ValidateChunk(chunk, chunk.Sequence) != nil {
		return ErrConflict
	}
	b, e := managed.Canonical(chunk)
	if e != nil {
		return e
	}
	var saved []byte
	e = tx.QueryRowContext(ctx, `SELECT chunk FROM managed_execution_output WHERE operation_id=? AND sequence=?`, id, chunk.Sequence).Scan(&saved)
	if e == nil {
		if !bytes.Equal(b, saved) {
			return ErrConflict
		}
		return nil
	}
	if !errors.Is(e, sql.ErrNoRows) {
		return e
	}
	var terminal int
	if e = tx.QueryRowContext(ctx, `SELECT count(*) FROM managed_execution_receipts WHERE operation_id=?`, id).Scan(&terminal); e != nil {
		return e
	}
	if terminal != 0 {
		return ErrConflict
	}
	var sequence, total uint64
	if e = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence),0),COALESCE(SUM(data_bytes),0) FROM managed_execution_output WHERE operation_id=?`, id).Scan(&sequence, &total); e != nil {
		return e
	}
	var limit uint64
	if e = tx.QueryRowContext(ctx, `SELECT output_limit FROM managed_executions WHERE operation_id=?`, id).Scan(&limit); e != nil {
		return e
	}
	if chunk.Sequence != sequence+1 || total+uint64(len(chunk.Data)) > limit {
		return ErrConflict
	}
	_, e = tx.ExecContext(ctx, `INSERT INTO managed_execution_output(operation_id,sequence,chunk,data_bytes) VALUES(?,?,?,?)`, id, chunk.Sequence, b, len(chunk.Data))
	return e
}
func retainedOutput(ctx context.Context, q Reader, id string) (*managed.OutputVerifier, uint64, error) {
	rows, e := q.QueryContext(ctx, `SELECT chunk FROM managed_execution_output WHERE operation_id=? ORDER BY sequence`, id)
	if e != nil {
		return nil, 0, e
	}
	defer rows.Close()
	v := managed.NewOutputVerifier()
	next := uint64(1)
	for rows.Next() {
		var b []byte
		if e = rows.Scan(&b); e != nil {
			return nil, 0, e
		}
		var c pb.OutputChunk
		if e = proto.Unmarshal(b, &c); e != nil {
			return nil, 0, e
		}
		if c.Sequence != next || v.Add(&c) != nil {
			return nil, 0, ErrConflict
		}
		next++
	}
	return v, next, rows.Err()
}

// RecordManagedReceiptTx retains kernel closure only after all manifest bytes
// are durably present in the same transaction. It never settles the provider.
func RecordManagedReceiptTx(ctx context.Context, tx *sql.Tx, owner Owner, ref Reference, id string, receipt *pb.TerminalReceipt) error {
	identity, e := retainedIdentity(ctx, tx, owner, ref, id)
	if e != nil {
		return e
	}
	if managed.ValidateReceipt(receipt, identity) != nil {
		return ErrConflict
	}
	output, _, e := retainedOutput(ctx, tx, id)
	if e != nil {
		return e
	}
	if output.Verify(receipt.Output) != nil {
		return ErrConflict
	}
	b, e := managed.Canonical(receipt)
	if e != nil {
		return e
	}
	return immutableBlob(ctx, tx, "managed_execution_receipts", "receipt", id, b)
}

// ReadManagedReceipt revalidates the retained identity and exact output. Absence
// is unresolved debt, including after restart; callers must not infer closure.
func ReadManagedReceipt(ctx context.Context, q Reader, owner Owner, ref Reference, id string) (*pb.TerminalReceipt, bool, error) {
	identity, e := retainedIdentity(ctx, q, owner, ref, id)
	if e != nil {
		return nil, false, e
	}
	var b []byte
	e = q.QueryRowContext(ctx, `SELECT receipt FROM managed_execution_receipts WHERE operation_id=?`, id).Scan(&b)
	if errors.Is(e, sql.ErrNoRows) {
		return nil, false, nil
	}
	if e != nil {
		return nil, false, e
	}
	var receipt pb.TerminalReceipt
	if e = proto.Unmarshal(b, &receipt); e != nil {
		return nil, false, e
	}
	if managed.ValidateReceipt(&receipt, identity) != nil {
		return nil, false, ErrConflict
	}
	output, _, e := retainedOutput(ctx, q, id)
	if e != nil {
		return nil, false, e
	}
	if output.Verify(receipt.Output) != nil {
		return nil, false, ErrConflict
	}
	return &receipt, true, nil
}
