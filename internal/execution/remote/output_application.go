package remote

import (
	"context"
	"database/sql"
	"path/filepath"
)

// RequireWorkspaceOutputApplied accepts only the exact sealed export whose
// private run workspace was restored, or whose read-only contents were verified
// unchanged. An intent alone is unresolved filesystem application debt.
func RequireWorkspaceOutputApplied(ctx context.Context, q Reader, owner Owner, ref Reference) error {
	if e := RequireWorkspaceExport(ctx, q, owner, ref); e != nil {
		return e
	}
	var id, purpose, snapshot, runID, archive, manifest string
	var applied sql.NullString
	e := q.QueryRowContext(ctx, `SELECT i.operation_id,i.purpose,i.snapshot_id,i.run_id,i.archive_sha256,i.manifest_sha256,a.snapshot_id FROM remote_workspace_output_intents i JOIN remote_workspace_output_applications a ON a.operation_id=i.operation_id JOIN remote_workspace_downloads d ON d.operation_id=i.operation_id JOIN managed_executions m ON m.operation_id=i.operation_id JOIN remote_operations o ON o.operation_id=i.operation_id WHERE i.allocation=? AND d.purpose='workspace_export' AND m.purpose='workspace_export' AND o.allocation=i.allocation AND i.archive_sha256=d.archive_sha256 AND i.manifest_sha256=d.manifest_sha256`, ref.Allocation).Scan(&id, &purpose, &snapshot, &runID, &archive, &manifest, &applied)
	if e != nil {
		return e
	}
	m, e := ReadWorkerManifestByAllocation(ctx, q, ref.Allocation)
	if e != nil {
		return e
	}
	if m.Owner != owner || m.ExportID != id {
		return ErrConflict
	}
	if purpose != owner.Kind || !validDigest(archive) || !validDigest(manifest) {
		return ErrConflict
	}
	if purpose == "planning" {
		if applied.Valid || runID != "" {
			return ErrConflict
		}
	} else {
		if runID == "" {
			return ErrConflict
		}
		var canonicalRun, attempt string
		if e = q.QueryRowContext(ctx, `SELECT run_id,attempt_id FROM provider_invocations WHERE provider_invocation_id=?`, owner.InvocationID).Scan(&canonicalRun, &attempt); e != nil {
			return e
		}
		if canonicalRun != runID || attempt != m.ResourceID {
			return ErrConflict
		}
		if !applied.Valid {
			if e = requireReadOnlyRunOutput(ctx, q, owner, m); e != nil {
				return e
			}
		} else {
			if applied.String != snapshot {
				return ErrConflict
			}
			var matches int
			if e = q.QueryRowContext(ctx, `SELECT count(*) FROM filesystem_snapshots WHERE snapshot_id=? AND run_id=? AND snapshot_kind='attempt_result' AND parent_snapshot_id IS NULL`, snapshot, runID).Scan(&matches); e != nil {
				return e
			}
			if matches != 1 {
				return ErrConflict
			}
		}
	}
	closure, e := ReadProcessClosure(ctx, q, owner, ref, id)
	if e != nil {
		return e
	}
	if closure.Purpose != WorkspaceExport || !closure.Successful {
		return ErrConflict
	}
	return nil
}

// A run without an applied snapshot must have a canonical response-only owner,
// its exact retained workspace pin, and no matching writable execution target.
func requireReadOnlyRunOutput(ctx context.Context, q Reader, owner Owner, m WorkerManifest) error {
	var path, completion string
	var execution sql.NullString
	e := q.QueryRowContext(ctx, `SELECT w.workspace_path,t.execution_root,i.completion_mode FROM provider_preparation_workspaces w JOIN provider_invocations i ON i.provider_invocation_id=w.invocation_id AND i.attempt_id=w.resource_id LEFT JOIN run_execution_targets t ON t.run_id=i.run_id WHERE w.owner_kind='run' AND w.invocation_id=? AND w.lease_owner=? AND w.lease_version=? AND w.resource_id=?`, owner.InvocationID, owner.LeaseOwner, owner.LeaseVersion, m.ResourceID).Scan(&path, &execution, &completion)
	if e != nil {
		return e
	}
	if completion != "server_response" || path == "" {
		return ErrConflict
	}
	if execution.Valid && filepath.Clean(execution.String) == path {
		return ErrConflict
	}
	return nil
}
