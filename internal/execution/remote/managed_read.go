package remote

import (
	"context"
	pb "kingdom/internal/execution/openshell/managedpb"
)

// ReadManagedIdentity revalidates the retained identity against its immutable
// expected runtime and command digests. It does not establish readiness or closure.
func ReadManagedIdentity(ctx context.Context, q Reader, owner Owner, ref Reference, id string) (*pb.CommandIdentity, ExecutionPurpose, error) {
	identity, err := retainedIdentity(ctx, q, owner, ref, id)
	if err != nil {
		return nil, "", err
	}
	var purpose ExecutionPurpose
	if err := q.QueryRowContext(ctx, `SELECT purpose FROM managed_executions WHERE operation_id=?`, id).Scan(&purpose); err != nil {
		return nil, "", err
	}
	return identity, purpose, nil
}
