package remote

import (
	"context"
	"google.golang.org/protobuf/proto"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
)

// ReadManagedStdout returns retained output only after complete evidence has
// been revalidated. It never fetches output from an untrusted path or URL.
func ReadManagedStdout(ctx context.Context, q Reader, owner Owner, ref Reference, id string, limit int) ([]byte, error) {
	if limit <= 0 || limit > managed.MaxOutputBytes {
		return nil, ErrConflict
	}
	if _, ok, err := ReadManagedReceipt(ctx, q, owner, ref, id); err != nil || !ok {
		return nil, ErrConflict
	}
	rows, err := q.QueryContext(ctx, `SELECT chunk FROM managed_execution_output WHERE operation_id=? ORDER BY sequence`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []byte
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		c := &pb.OutputChunk{}
		if proto.Unmarshal(raw, c) != nil {
			return nil, ErrConflict
		}
		if c.Stream == pb.OutputStream_OUTPUT_STREAM_STDOUT {
			if len(result)+len(c.Data) > limit {
				return nil, ErrConflict
			}
			result = append(result, c.Data...)
		}
	}
	return result, rows.Err()
}
