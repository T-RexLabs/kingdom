package worker

import (
	"context"
	"database/sql"
	"errors"

	"google.golang.org/grpc"
	"kingdom/internal/execution/openshell/managed"
	pb "kingdom/internal/execution/openshell/managedpb"
	"kingdom/internal/execution/remote"
)

// DisposalPrerequisites is injected by the trusted assembly. It must verify
// exact capability drain and sealed workspace export for this owner/runtime.
// A caller assertion or provider-generated boolean is not an implementation.
type DisposalPrerequisites interface {
	RequireClosed(context.Context, remote.Owner, remote.Reference) error
}
type Disposer struct {
	worker        *Worker
	prerequisites DisposalPrerequisites
}

func NewDisposer(w *Worker, p DisposalPrerequisites) (*Disposer, error) {
	if w == nil || p == nil {
		return nil, remote.ErrConflict
	}
	return &Disposer{w, p}, nil
}

// Dispose performs one recovery pass under the caller's deadline. Finalize is retried only against
// the exact pinned runtime. Deletion uses the retained archive and immutable
// sandbox ID; no native name-based mutation is permitted.
func (d *Disposer) Dispose(ctx context.Context, r remote.Reservation) error {
	w := d.worker
	if r.Reference.Target != w.config.Target {
		return remote.ErrConflict
	}
	if e := d.prerequisites.RequireClosed(ctx, r.Owner, r.Reference); e != nil {
		return e
	}
	if e := w.transaction(ctx, func(tx *sql.Tx) error { return remote.PrepareRuntimeDisposalTx(ctx, tx, r.Owner, r.Reference) }); e != nil {
		return e
	}
	archive, e := remote.ReadRuntimeArchive(ctx, w.db, r.Owner, r.Reference)
	if errors.Is(e, sql.ErrNoRows) {
		expected, ids, e := remote.DisposalInventory(ctx, w.db, r.Owner, r.Reference)
		if e != nil {
			return e
		}
		create, e := w.requests.ReadCreate(ctx, w.db, r.Owner, r.Reference)
		if e != nil {
			return e
		}
		archive, e = w.wire.FinalizeRuntime(ctx, &pb.FinalizeRuntimeRequest{Workspace: create.GetWorkspaceScope().GetWorkspace(), Sandbox: create.Name, Runtime: expected.Runtime}, grpc.MaxCallRecvMsgSize(managed.MaxControlBytes))
		if e != nil {
			return ErrUnresolved
		}
		if e = managed.ValidateArchive(archive, expected, ids); e != nil {
			return e
		}
		if e = w.transaction(ctx, func(tx *sql.Tx) error { return remote.RecordRuntimeArchiveTx(ctx, tx, r.Owner, r.Reference, archive) }); e != nil {
			return e
		}
	} else if e != nil {
		return e
	}
	if e = d.prerequisites.RequireClosed(ctx, r.Owner, r.Reference); e != nil {
		return e
	}
	expected, ids, e := remote.DisposalInventory(ctx, w.db, r.Owner, r.Reference)
	if e != nil {
		return e
	}
	client, e := managed.NewArchiveClient(w.wire, expected, archive, ids)
	if e != nil {
		return e
	}
	proof, e := client.DeleteFinalized(ctx)
	if e != nil {
		return e
	}
	return w.transaction(ctx, func(tx *sql.Tx) error { return remote.RecordRuntimeDeletionTx(ctx, tx, r.Owner, r.Reference, proof) })
}
