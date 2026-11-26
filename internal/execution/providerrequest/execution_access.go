package providerrequest

import (
	"context"
	"database/sql"
	"kingdom/internal/execution/access"
)

// ExecutionAccess reads an issued canonical owner's immutable hosted selection.
// It is not a central authorization grant; the worker must reauthorize at every
// reservation and dispatch. False means an explicitly unbound local database.
func (a *Authority) ExecutionAccess(ctx context.Context, invocationID string) (access.Snapshot, bool, error) {
	tx, err := a.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return access.Snapshot{}, false, err
	}
	defer tx.Rollback()
	preparation, err := a.preparation(ctx, tx, invocationID)
	if err != nil {
		return access.Snapshot{}, false, err
	}
	project, err := access.Project(ctx, tx)
	if err != nil {
		return access.Snapshot{}, false, err
	}
	if project == "" {
		return access.Snapshot{}, false, tx.Commit()
	}
	snapshot, err := access.Invocation(ctx, tx, string(preparation.Owner.Kind), invocationID)
	if err != nil {
		return access.Snapshot{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return access.Snapshot{}, false, err
	}
	return snapshot, true, nil
}
