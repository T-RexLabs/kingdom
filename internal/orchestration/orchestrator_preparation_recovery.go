package orchestration

import (
	"context"
	"time"

	"kingdom/internal/execution/providerrequest"
	executionstore "kingdom/internal/execution/store"
)

// Disposal and invocation abandonment are separate durable obligations. An
// interrupted process can finish deletion without invalidating its unbound
// invocation, so this scan must not depend on outstanding allocation debt.
func (o *orchestrationLoop) scheduleIssuedRemotePreparationRecovery(ctx context.Context) (int, error) {
	if !o.supportsRemoteCleanup() || o.executionStore == nil {
		return 0, nil
	}
	o.maintenance.scanMu.Lock()
	defer o.maintenance.scanMu.Unlock()
	const limit = 64
	if o.maintenance.issuedRemotePreparationEnd == "" {
		if err := o.db.QueryRowContext(ctx, `SELECT COALESCE(max(provider_invocation_id),'') FROM provider_invocations`).Scan(&o.maintenance.issuedRemotePreparationEnd); err != nil {
			return 0, err
		}
		if o.maintenance.issuedRemotePreparationEnd == "" {
			return 0, nil
		}
	}
	rows, err := o.db.QueryContext(ctx, `SELECT i.provider_invocation_id,i.run_id,i.attempt_id,p.lease_owner,p.lease_version
FROM provider_invocations i
JOIN workflow_runs w ON w.run_id=i.run_id
JOIN attempt_executions a ON a.attempt_id=i.attempt_id AND a.run_id=i.run_id AND a.node_run_id=i.node_run_id
JOIN provider_preparations p ON p.owner_kind='run' AND p.invocation_id=i.provider_invocation_id
WHERE i.provider_invocation_id>? AND i.provider_invocation_id<=?
 AND w.run_status IN ('succeeded','failed','cancelled') AND i.invocation_state='issued' AND p.admission_closed=1
 AND a.state='queued' AND a.active_provider_invocation_id=i.provider_invocation_id
 AND a.lease_owner=p.lease_owner AND a.lease_version=p.lease_version
 AND a.launch_started_at IS NULL AND a.started_at IS NULL AND a.canonical_settlement_id IS NULL
 AND NOT EXISTS(SELECT 1 FROM provider_requests b WHERE b.run_invocation_id=i.provider_invocation_id)
 AND EXISTS(SELECT 1 FROM remote_runtimes r WHERE r.owner_kind='run' AND r.invocation_id=i.provider_invocation_id)
 AND NOT EXISTS(SELECT 1 FROM remote_runtimes r WHERE r.owner_kind='run' AND r.invocation_id=i.provider_invocation_id
  AND NOT EXISTS(SELECT 1 FROM remote_runtime_deletions d WHERE d.allocation=r.allocation)
  AND NOT EXISTS(SELECT 1 FROM remote_runtimes_never_created n WHERE n.allocation=r.allocation))
ORDER BY i.provider_invocation_id LIMIT ?`, o.maintenance.issuedRemotePreparationID, o.maintenance.issuedRemotePreparationEnd, limit)
	if err != nil {
		return 0, err
	}
	type candidate struct {
		invocation, run, attempt string
		lease                    providerrequest.Lease
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err = rows.Scan(&c.invocation, &c.run, &c.attempt, &c.lease.Owner, &c.lease.Version); err != nil {
			rows.Close()
			return 0, err
		}
		candidates = append(candidates, c)
	}
	err = rows.Err()
	if closeErr := rows.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return 0, err
	}
	admitted, visited := 0, 0
	for _, c := range candidates {
		if !o.remoteDisposalCapacity() {
			break
		}
		if o.tryScheduleMaintenance(ctx, maintenanceRequest{kind: "remote_preparation", key: "remote_preparation:" + c.invocation, attemptID: c.attempt, runID: c.run}, func(ctx context.Context) error {
			cleanup, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			// Candidate rows grant no authority. The canonical transaction rechecks
			// the exact queued lease, absent dispatch, capability certificates and
			// every historical runtime's verified archive and typed deletion.
			return o.executionStore.InvalidateUnlaunchedProviderInvocation(cleanup, executionstore.InvalidateUnlaunchedProviderInvocationRequest{ProviderInvocationID: c.invocation, InvalidReason: "provider_preparation_failed", UpdatedAt: o.now().UTC().Format(time.RFC3339Nano), ExpectedLeaseOwner: c.lease.Owner, ExpectedLeaseVersion: c.lease.Version})
		}) {
			admitted++
		} else if !o.remoteDisposalCapacity() {
			break
		}
		visited++
		o.maintenance.issuedRemotePreparationID = c.invocation
	}
	if visited == len(candidates) && len(candidates) < limit {
		o.maintenance.issuedRemotePreparationID = ""
		o.maintenance.issuedRemotePreparationEnd = ""
	}
	return admitted, nil
}
