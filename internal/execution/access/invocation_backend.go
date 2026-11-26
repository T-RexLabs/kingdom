package access

import (
	"context"
	"database/sql"
	"fmt"
	"kingdom/internal/core/providerexecution"
)

// InvocationBackend derives routing from immutable source ownership and checks
// every retained effect against it. Deployment defaults are never consulted.
func InvocationBackend(ctx context.Context, q Reader, id string) (providerexecution.Backend, error) {
	var kind, source string
	var count int
	err := q.QueryRowContext(ctx, `SELECT count(*),COALESCE(min(kind),''),COALESCE(min(source),'') FROM (SELECT 'run' AS kind,run_id AS source FROM provider_invocations WHERE provider_invocation_id=? UNION ALL SELECT 'planning',planning_operation_id FROM planning_provider_invocations WHERE planning_provider_invocation_id=?)`, id, id).Scan(&count, &kind, &source)
	if err != nil {
		return "", err
	}
	if count != 1 {
		return "", fmt.Errorf("exact canonical invocation required")
	}
	backend, err := SourceBackend(ctx, q, kind, source)
	if err != nil {
		return "", err
	}
	var conflicts int
	err = q.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM provider_requests WHERE (run_invocation_id=? OR planning_invocation_id=?) AND execution_backend<>?) + (SELECT count(*) FROM remote_runtimes WHERE owner_kind=? AND invocation_id=? AND ?<>'openshell')`, id, id, backend, kind, id, backend).Scan(&conflicts)
	if err != nil {
		return "", err
	}
	if conflicts != 0 {
		return "", fmt.Errorf("retained backend facts conflict")
	}
	return backend, nil
}

// AttemptBackend resolves only the current canonical attempt invocation.
func AttemptBackend(ctx context.Context, q Reader, id string) (providerexecution.Backend, error) {
	var invocation sql.NullString
	if err := q.QueryRowContext(ctx, `SELECT active_provider_invocation_id FROM attempt_executions WHERE attempt_id=?`, id).Scan(&invocation); err != nil {
		return "", err
	}
	if !invocation.Valid || invocation.String == "" {
		return "", fmt.Errorf("active invocation required")
	}
	return InvocationBackend(ctx, q, invocation.String)
}
