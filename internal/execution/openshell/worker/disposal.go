package worker

import (
	"context"
	"database/sql"
	"errors"

	native "github.com/NVIDIA/OpenShell/sdk/go/proto/openshellv1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
type disposalNative interface {
	remote.NativeGetter
	DeleteSandbox(context.Context, *native.DeleteSandboxRequest, ...grpc.CallOption) (*native.DeleteSandboxResponse, error)
}
type Disposer struct {
	worker        *Worker
	native        disposalNative
	prerequisites DisposalPrerequisites
}

func NewDisposer(w *Worker, p DisposalPrerequisites) (*Disposer, error) {
	if w == nil || p == nil {
		return nil, remote.ErrConflict
	}
	n, ok := w.native.(disposalNative)
	if !ok {
		return nil, remote.ErrConflict
	}
	return &Disposer{w, n, p}, nil
}

// Dispose performs one recovery pass under the caller's deadline. Finalize is retried only against
// the exact pinned runtime. Native deletion still lacks an atomic expected-ID
// precondition; this experimental adapter must not enable product disposal.
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
	intent, e := remote.ReadNativeDeleteIntent(ctx, w.db, r.Owner, r.Reference)
	if errors.Is(e, sql.ErrNoRows) {
		var proof remote.NativeDeleteObservation
		proof, e = remote.ObserveNativeDelete(ctx, w.db, w.requests, d.native, r.Owner, r.Reference)
		if e != nil {
			return e
		}
		if e = w.transaction(ctx, func(tx *sql.Tx) error { return remote.RecordNativeDeleteIntentTx(ctx, tx, r.Owner, r.Reference, proof) }); e != nil {
			return e
		}
		intent, e = remote.ReadNativeDeleteIntent(ctx, w.db, r.Owner, r.Reference)
	}
	if e != nil {
		return e
	}
	// Recheck stable identity before each possible native mutation. A replacement
	// remains debt; a pre-intent absence never enters this branch.
	observed, e := d.native.GetSandbox(ctx, &native.GetSandboxRequest{Name: intent.Name, WorkspaceScope: intent.WorkspaceScope})
	if e == nil {
		if observed.GetSandbox().GetMetadata().GetId() != r.Reference.SandboxID || observed.GetSandbox().GetMetadata().GetName() != intent.Name {
			return remote.ErrConflict
		}
		if e = d.prerequisites.RequireClosed(ctx, r.Owner, r.Reference); e != nil {
			return e
		}
		if _, e = d.native.DeleteSandbox(ctx, intent); e != nil {
			return ErrUnresolved
		}
	} else if status.Code(e) != codes.NotFound {
		return ErrUnresolved
	}
	absence, e := remote.ObserveNativeAbsence(ctx, w.db, d.native, r.Owner, r.Reference)
	if e != nil {
		return ErrUnresolved
	}
	return w.transaction(ctx, func(tx *sql.Tx) error { return remote.RecordNativeAbsenceTx(ctx, tx, r.Owner, r.Reference, absence) })
}
