package remote

import (
	"context"
	"database/sql"
)

// RequireWorkspaceOutputApplied accepts only the exact sealed export whose
// private run workspace was restored, or whose planning contents were verified
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
		if !applied.Valid || applied.String != snapshot || runID == "" {
			return ErrConflict
		}
		var canonicalRun, attempt string
		if e = q.QueryRowContext(ctx, `SELECT run_id,attempt_id FROM provider_invocations WHERE provider_invocation_id=?`, owner.InvocationID).Scan(&canonicalRun, &attempt); e != nil {
			return e
		}
		if canonicalRun != runID || attempt != m.ResourceID {
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
	closure, e := ReadProcessClosure(ctx, q, owner, ref, id)
	if e != nil {
		return e
	}
	if closure.Purpose != WorkspaceExport || !closure.Successful {
		return ErrConflict
	}
	return nil
}
