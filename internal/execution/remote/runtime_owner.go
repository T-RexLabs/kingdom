package remote

import (
	"context"
	"database/sql"
	"encoding/hex"

	"google.golang.org/protobuf/proto"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
)

// RecordRuntimeOwnerTx retains authenticated discovery before any command exists.
// It is identity evidence only; it never establishes process or runtime closure.
func RecordRuntimeOwnerTx(ctx context.Context, tx *sql.Tx, owner Owner, ref Reference, discovered managed.Owner) error {
	if _, err := requireOwner(ctx, tx, owner, ref, true); err != nil {
		return err
	}
	if !runtimeOwnerMatches(ref, discovered) {
		return ErrConflict
	}
	d := &pb.DescribeRuntimeResponse{Runtime: discovered.Runtime, Capability: managed.Capability, MaximumOutputBytes: managed.MaxOutputBytes, GatewayId: discovered.GatewayID, WorkspaceId: discovered.WorkspaceID, SubjectId: discovered.SubjectID}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(d)
	if err != nil {
		return err
	}
	hash, err := managed.Digest(d)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO remote_runtime_owners(allocation,descriptor,digest) VALUES(?,?,?)`, ref.Allocation, raw, hex.EncodeToString(hash))
	if err != nil {
		return err
	}
	retained, err := ReadRuntimeOwner(ctx, tx, owner, ref)
	if err != nil {
		return err
	}
	if retained.GatewayID != discovered.GatewayID || retained.WorkspaceID != discovered.WorkspaceID || retained.SubjectID != discovered.SubjectID || !proto.Equal(retained.Runtime, discovered.Runtime) {
		return ErrConflict
	}
	return nil
}
func runtimeOwnerMatches(ref Reference, o managed.Owner) bool {
	return o.Validate() == nil && o.GatewayID == ref.GatewayID && o.WorkspaceID == ref.WorkspaceID && o.Runtime.SandboxId == ref.SandboxID && o.Runtime.BoundaryGeneration == ref.Generation && o.Runtime.MainProcessInstanceId == ref.MainInstanceID
}

// ReadRuntimeOwner validates retained discovery against the exact historical
// owner and runtime. It does not require a current admission lease.
func ReadRuntimeOwner(ctx context.Context, q Reader, owner Owner, ref Reference) (managed.Owner, error) {
	if _, err := requireOwner(ctx, q, owner, ref, true); err != nil {
		return managed.Owner{}, err
	}
	var raw []byte
	var digest string
	if err := q.QueryRowContext(ctx, `SELECT descriptor,digest FROM remote_runtime_owners WHERE allocation=?`, ref.Allocation).Scan(&raw, &digest); err != nil {
		return managed.Owner{}, err
	}
	var d pb.DescribeRuntimeResponse
	if err := proto.Unmarshal(raw, &d); err != nil {
		return managed.Owner{}, ErrConflict
	}
	hash, err := managed.Digest(&d)
	if err != nil || hex.EncodeToString(hash) != digest || len(d.ProtoReflect().GetUnknown()) != 0 || d.Capability != managed.Capability || d.MaximumOutputBytes != managed.MaxOutputBytes {
		return managed.Owner{}, ErrConflict
	}
	o := managed.Owner{GatewayID: d.GatewayId, WorkspaceID: d.WorkspaceId, SubjectID: d.SubjectId, Runtime: d.Runtime}
	if !runtimeOwnerMatches(ref, o) {
		return managed.Owner{}, ErrConflict
	}
	return o, nil
}
