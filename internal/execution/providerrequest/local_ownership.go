package providerrequest

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	"kingdom/internal/execution/access"
)

// BindLocalOwnerTx enables managed local ownership without rewriting completed
// host-local history. Assembly calls this in the same startup transaction that
// pins its private registry; all previous work must be closed first.
func BindLocalOwnerTx(ctx context.Context, tx *sql.Tx, project, owner string) error {
	for _, raw := range []string{project, owner} {
		id, err := uuid.Parse(raw)
		if err != nil || id == uuid.Nil || id.String() != raw {
			return access.ErrInvalid
		}
	}
	existing, err := access.Project(ctx, tx)
	if err != nil {
		return err
	}
	if existing != "" {
		domain, saved, err := access.ProjectDomain(ctx, tx)
		if err != nil {
			return err
		}
		if existing != project || domain != access.DomainLocalOwner || saved != owner {
			return access.ErrInvalid
		}
		return nil
	}
	var active int
	err = tx.QueryRowContext(ctx, `SELECT
 (SELECT count(*) FROM workflow_runs WHERE run_status NOT IN ('succeeded','failed','cancelled'))+
 (SELECT count(*) FROM planning_operations WHERE status IN ('queued','running'))+
 (SELECT count(*) FROM provider_invocations WHERE invocation_state IN ('issued','open'))+
 (SELECT count(*) FROM planning_provider_invocations WHERE invocation_state IN ('issued','open'))+
 (SELECT count(*) FROM provider_preparations WHERE admission_closed=0)+
 (SELECT count(*) FROM provider_server_instances WHERE quiescent_at IS NULL)+
 (SELECT count(*) FROM provider_process_guards WHERE lifecycle<>'quiescent' OR quiescent_at IS NULL OR quiescence_evidence_sha256='')+
 (SELECT count(*) FROM remote_runtimes r WHERE NOT EXISTS(SELECT 1 FROM remote_runtime_deletions d WHERE d.allocation=r.allocation) AND NOT EXISTS(SELECT 1 FROM remote_runtimes_never_created n WHERE n.allocation=r.allocation))`).Scan(&active)
	if err != nil {
		return err
	}
	if active != 0 {
		return fmt.Errorf("local OpenShell requires all previous work and resources to be closed")
	}
	rows, err := tx.QueryContext(ctx, `SELECT binding_id FROM provider_requests ORDER BY binding_id`)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
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
		if err = RequireResourcesClosedTx(ctx, tx, id); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO execution_access_project(slot,project_id) VALUES(1,?)`, project); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO execution_access_local_owner(slot,owner_id,domain) VALUES(1,?,'local_owner')`, owner)
	return err
}
