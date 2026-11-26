package worker

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/google/uuid"
	"kingdom/internal/execution/openshell/managed"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/execution/remote"
)

// CloseTransfers fences and resolves only previously retained transfer jobs.
// It never starts a missing or prepared job and never treats absence remotely
// as proof that an uncertain command did not execute.
func (w *Worker) CloseTransfers(ctx context.Context, p providerrequest.Preparation, r remote.Reservation, owner managed.Owner) error {
	if !ownerMatches(owner, r.Reference) {
		return remote.ErrConflict
	}
	if e := w.transaction(ctx, func(tx *sql.Tx) error {
		return providerrequest.FenceRemoteCleanupTx(ctx, tx, r.Owner, r.Reference, w.config.Now())
	}); e != nil {
		return e
	}
	rows, e := w.db.QueryContext(ctx, `SELECT e.operation_id FROM managed_executions e JOIN remote_operations o ON o.operation_id=e.operation_id WHERE o.allocation=? AND e.purpose IN ('workspace_import','workspace_export','native_receipts') ORDER BY e.operation_id`, r.Reference.Allocation)
	if e != nil {
		return e
	}
	var ids []string
	for rows.Next() {
		var id string
		if e = rows.Scan(&id); e != nil {
			rows.Close()
			return e
		}
		ids = append(ids, id)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for _, id := range ids {
		if e = w.closeTransfer(ctx, r, owner, id); e != nil {
			return e
		}
	}
	return nil
}
func (w *Worker) closeTransfer(ctx context.Context, r remote.Reservation, owner managed.Owner, id string) error {
	if _, e := remote.ReadProcessClosure(ctx, w.db, r.Owner, r.Reference, id); e == nil {
		return nil
	}
	op, e := remote.ReadOperation(ctx, w.db, id)
	if e != nil {
		return e
	}
	if op.Delivery == remote.Prepared {
		return w.transaction(ctx, func(tx *sql.Tx) error { return remote.MarkExecutionNotStartedTx(ctx, tx, r.Owner, r.Reference, id) })
	}
	request, e := w.requests.ReadManaged(ctx, w.db, r.Owner, r.Reference, id)
	if e != nil {
		return e
	}
	client, e := managed.New(ctx, w.wire, owner, w.config.Workspace, nativeName(r.Reference))
	if e != nil {
		return e
	}
	status, e := client.ObserveStart(ctx, request)
	if e != nil {
		return errors.Join(ErrUnresolved, e)
	}
	if e = w.retainIdentity(ctx, r, status); e != nil {
		return e
	}
	if status.Terminal == nil {
		// The deterministic method namespace binds cancellation to this immutable
		// retained operation across restart without issuing a new command identity.
		cancelID := uuid.NewSHA1(uuid.NameSpaceOID, []byte("kingdom/transfer-cancel/v1/"+id)).String()
		status, e = client.Cancel(ctx, status.Identity, cancelID)
		if e != nil {
			return errors.Join(ErrUnresolved, e)
		}
	}
	for status.Terminal == nil {
		select {
		case <-ctx.Done():
			return errors.Join(ErrUnresolved, ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
		status, e = client.Get(ctx, status.Identity)
		if e != nil {
			return errors.Join(ErrUnresolved, e)
		}
	}
	if _, e = w.retainTerminal(ctx, client, r, status); e != nil {
		return e
	}
	_, e = remote.ReadProcessClosure(ctx, w.db, r.Owner, r.Reference, id)
	return e
}
