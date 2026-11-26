package providerrequest

import (
	"context"
	"fmt"
)

// RequireHostLocalMode refuses to detach the configured managed backend while
// canonical managed work or remote resource debt can still need it.
func RequireHostLocalMode(ctx context.Context, q Reader) error {
	var debt int
	err := q.QueryRowContext(ctx, `SELECT
 (SELECT count(*) FROM workflow_runs r JOIN execution_run_backends b USING(run_id) WHERE b.backend='openshell' AND r.run_status NOT IN('succeeded','failed','cancelled'))+
 (SELECT count(*) FROM planning_operations o JOIN execution_planning_backends b USING(operation_id) WHERE b.backend='openshell' AND o.status IN('queued','running'))+
 (SELECT count(*) FROM provider_invocations i JOIN execution_run_backends b USING(run_id) WHERE b.backend='openshell' AND i.invocation_state IN('issued','open'))+
 (SELECT count(*) FROM planning_provider_invocations i JOIN execution_planning_backends b ON b.operation_id=i.planning_operation_id WHERE b.backend='openshell' AND i.invocation_state IN('issued','open'))+
 (SELECT count(*) FROM remote_runtimes r WHERE NOT EXISTS(SELECT 1 FROM remote_runtime_deletions d WHERE d.allocation=r.allocation) AND NOT EXISTS(SELECT 1 FROM remote_runtimes_never_created n WHERE n.allocation=r.allocation))`).Scan(&debt)
	if err != nil {
		return err
	}
	if debt != 0 {
		return fmt.Errorf("keep local OpenShell configured until managed work and remote cleanup finish")
	}
	return nil
}
