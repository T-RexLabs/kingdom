package remote

import "context"

// requireExclusiveImport applies at reservation and the final dispatch claim.
// A stable same-ID observation may be replayed, but no later provider intent or
// other command can coexist with a newly dispatched broad-workspace importer.
func requireExclusiveImport(ctx context.Context, q Reader, allocation int64, id string) error {
	var count int
	if e := q.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM remote_operations WHERE allocation=? AND kind='execute' AND operation_id<>?)+(SELECT count(*) FROM remote_provider_launches WHERE allocation=?)`, allocation, id, allocation).Scan(&count); e != nil {
		return e
	}
	if count != 0 {
		return ErrConflict
	}
	return nil
}
