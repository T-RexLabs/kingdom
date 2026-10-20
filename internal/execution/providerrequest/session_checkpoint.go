package providerrequest

import (
	"context"
	"database/sql"
	"time"

	"kingdom/internal/core/providerexecution"
)

// RequireSessionClosed checks the exact source binding and every request that
// shares its provider session. A stale source must not interrupt a newer owner.
// The caller holds the workflow's workspace/recovery reservation while capturing
// the provider and filesystem checkpoint.
func (a *Authority) RequireSessionClosed(ctx context.Context, identity providerexecution.RequestIdentity) error {
	return a.transact(ctx, identity, func(source Binding, _ time.Time, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT binding_id FROM provider_requests WHERE provider_session_id=?`, source.Identity.SessionID)
		if err != nil {
			return err
		}
		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
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
		for _, id := range ids {
			if err := RequireResourcesClosedTx(ctx, tx, id); err != nil {
				return err
			}
		}
		return nil
	})
}
