package opencode

import (
	"context"
	"database/sql"
	"kingdom/internal/core/providerexecution"
	_ "modernc.org/sqlite"
	"testing"
)

type routeBackend struct {
	kind  providerexecution.Backend
	calls []string
}

func (b *routeBackend) ExecutionBackend() providerexecution.Backend { return b.kind }
func (b *routeBackend) Prepare(context.Context, ExecuteRequest) (Prepared, error) {
	b.calls = append(b.calls, "prepare")
	return Prepared{}, nil
}
func (b *routeBackend) Execute(context.Context, ExecuteRequest) (ExecuteResponse, error) {
	b.calls = append(b.calls, "execute")
	return ExecuteResponse{}, nil
}
func (b *routeBackend) Drain(context.Context, string) error {
	b.calls = append(b.calls, "drain")
	return nil
}
func (b *routeBackend) Discard(context.Context, string) error {
	b.calls = append(b.calls, "discard")
	return nil
}
func (b *routeBackend) Cancel(context.Context, string) error {
	b.calls = append(b.calls, "cancel")
	return nil
}
func (b *routeBackend) CloseContext(context.Context) error { return nil }
func (b *routeBackend) ForceClose() error                  { return nil }
func TestBackendRouterRetainsInvocationAuthorityAcrossDefaultChange(t *testing.T) {
	db, e := sql.Open("sqlite", ":memory:")
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	_, e = db.Exec(`CREATE TABLE provider_invocations(provider_invocation_id TEXT,run_id TEXT);
 CREATE TABLE planning_provider_invocations(planning_provider_invocation_id TEXT,planning_operation_id TEXT);
 CREATE TABLE execution_run_backends(run_id TEXT,backend TEXT);
 CREATE TABLE execution_planning_backends(operation_id TEXT,backend TEXT);
 CREATE TABLE provider_requests(run_invocation_id TEXT,planning_invocation_id TEXT,execution_backend TEXT);
 CREATE TABLE remote_runtimes(owner_kind TEXT,invocation_id TEXT);
 CREATE TABLE attempt_executions(attempt_id TEXT,active_provider_invocation_id TEXT);
 INSERT INTO provider_invocations VALUES('old-local','run'),('missing-pin','missing');
 INSERT INTO planning_provider_invocations VALUES('old-managed','operation');
 INSERT INTO execution_run_backends VALUES('run','local');
 INSERT INTO execution_planning_backends VALUES('operation','openshell');
 INSERT INTO attempt_executions VALUES('attempt','old-local');`)
	if e != nil {
		t.Fatal(e)
	}
	ctx := context.Background()
	local := &routeBackend{kind: providerexecution.BackendLocal}
	managed := &routeBackend{kind: providerexecution.BackendOpenShell}
	for _, selected := range []providerexecution.Backend{providerexecution.BackendLocal, providerexecution.BackendOpenShell} {
		r, e := NewBackendRouter(db, selected, local, managed)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = r.Prepare(ctx, ExecuteRequest{InvocationID: "old-local"}); e != nil {
			t.Fatal(e)
		}
		if e = r.Cancel(ctx, "attempt"); e != nil {
			t.Fatal(e)
		}
		if e = r.Discard(ctx, "old-managed"); e != nil {
			t.Fatal(e)
		}
		if _, e = r.Prepare(ctx, ExecuteRequest{InvocationID: "missing-pin"}); e == nil {
			t.Fatal("missing pin used default")
		}
		if e = r.Drain(ctx, "unknown"); e == nil {
			t.Fatal("unknown invocation used default")
		}
	}
	if len(local.calls) != 4 || len(managed.calls) != 2 {
		t.Fatal(local.calls, managed.calls)
	}
	r, e := NewBackendRouter(db, providerexecution.BackendLocal, local, nil)
	if e != nil {
		t.Fatal(e)
	}
	if e = r.Discard(ctx, "old-managed"); e == nil {
		t.Fatal("missing managed backend fell back")
	}
	if _, e = db.Exec(`INSERT INTO provider_requests VALUES('old-local',NULL,'openshell')`); e != nil {
		t.Fatal(e)
	}
	if e = r.Drain(ctx, "old-local"); e == nil {
		t.Fatal("conflicting retained backend accepted")
	}
}
