package orchestration

import (
	"context"
	"errors"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/openshell/managed"
	"kingdom/internal/execution/providerrequest"
	"time"
)

func (o *orchestrationLoop) supportsRemoteCleanup() bool {
	if o == nil || o.db == nil || o.provider == nil {
		return false
	}
	backend, ok := o.provider.(interface {
		ExecutionBackend() providerexecution.Backend
	})
	supportsRemote := ok && backend.ExecutionBackend() == providerexecution.BackendOpenShell
	if routed, ok := o.provider.(interface{ SupportsRemoteCleanup() bool }); ok {
		supportsRemote = routed.SupportsRemoteCleanup()
	}
	return supportsRemote
}

func (o *orchestrationLoop) scheduleRemoteDisposals(ctx context.Context) (int, error) {
	if !o.supportsRemoteCleanup() {
		return 0, nil
	}
	o.maintenance.scanMu.Lock()
	defer o.maintenance.scanMu.Unlock()
	const limit = 64
	if o.maintenance.remoteDisposalEnd == 0 {
		if e := o.db.QueryRowContext(ctx, `SELECT COALESCE(max(allocation),0) FROM remote_runtimes`).Scan(&o.maintenance.remoteDisposalEnd); e != nil {
			return 0, e
		}
		if o.maintenance.remoteDisposalEnd == 0 {
			return 0, nil
		}
	}
	records, e := providerrequest.ListRemoteDisposalCandidatesThrough(ctx, o.db, o.maintenance.remoteDisposalAllocation, o.maintenance.remoteDisposalEnd, limit)
	if e != nil {
		return 0, e
	}
	admitted := 0
	visited := 0
	for _, c := range records {
		if !o.remoteDisposalCapacity() {
			break
		}
		req := maintenanceRequest{kind: "remote_disposal", key: "remote_disposal:" + string(c.Owner.Kind) + ":" + c.Owner.InvocationID, attemptID: c.AttemptID, runID: c.RunID}
		if o.tryScheduleMaintenance(ctx, req, func(ctx context.Context) error {
			ready, e := providerrequest.RemoteDisposalStillEligible(ctx, o.db, c)
			if e != nil || !ready {
				return e
			}
			cleanup, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if e = o.provider.Drain(cleanup, c.Owner.InvocationID); e != nil {
				return e
			}
			ready, e = providerrequest.RemoteDisposalStillEligible(cleanup, o.db, c)
			if e != nil || !ready {
				return e
			}
			e = o.provider.Discard(cleanup, c.Owner.InvocationID)
			// A pending typed deletion is normal durable work for the next fair pass.
			// No local absence or success marker is written here.
			if errors.Is(e, managed.ErrDeletionPending) {
				return nil
			}
			return e
		}) {
			admitted++
		} else if !o.remoteDisposalCapacity() {
			break
		}
		visited++
		o.maintenance.remoteDisposalAllocation = c.Allocation
	}
	if visited == len(records) && len(records) < limit {
		o.maintenance.remoteDisposalAllocation = 0
		o.maintenance.remoteDisposalEnd = 0
	}
	return admitted, nil
}

// Preserve the cursor at the first unvisited record when another category fills
// the worker pool. Busy attempts can still be skipped without starving later ones.
func (o *orchestrationLoop) remoteDisposalCapacity() bool {
	o.execMu.Lock()
	defer o.execMu.Unlock()
	limit := o.maintenance.limit
	if limit <= 0 {
		limit = defaultMaintenanceLimit
	}
	active := 0
	for _, kind := range o.maintenance.active {
		if kind != "cancel_stop" {
			active++
		}
	}
	return active < limit
}
