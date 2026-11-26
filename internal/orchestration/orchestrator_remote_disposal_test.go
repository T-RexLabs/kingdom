package orchestration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/openshell/managed"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/providers/opencode"
	_ "modernc.org/sqlite"
	"path/filepath"
	"sync"
	"testing"
)

type disposalProvider struct {
	opencode.Provider
	mu       sync.Mutex
	calls    []string
	kind     providerexecution.Backend
	hold     <-chan struct{}
	started  chan struct{}
	drainErr error
}

func (p *disposalProvider) ExecutionBackend() providerexecution.Backend { return p.kind }
func (p *disposalProvider) Drain(ctx context.Context, id string) error {
	if _, ok := ctx.Deadline(); !ok {
		panic("unbounded cleanup")
	}
	p.mu.Lock()
	p.calls = append(p.calls, "drain:"+id)
	err := p.drainErr
	p.mu.Unlock()
	if err != nil {
		return err
	}
	if p.started != nil {
		p.started <- struct{}{}
	}
	if p.hold != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.hold:
		}
	}
	return nil
}
func (p *disposalProvider) Discard(_ context.Context, id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, "discard:"+id)
	return managed.ErrDeletionPending
}
func disposalMaintenanceFixture(t *testing.T, n int) (*orchestrationLoop, *disposalProvider) {
	t.Helper()
	loop, _ := newMaintenanceAdmissionFixture(t, 1)
	db, e := sql.Open("sqlite", filepath.Join(t.TempDir(), "project.db"))
	if e != nil {
		t.Fatal(e)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { waitMaintenanceAdmissionDrain(t, loop); db.Close() })
	loop.db = db
	_, e = db.Exec(`CREATE TABLE remote_runtimes(allocation INTEGER PRIMARY KEY,owner_kind TEXT,invocation_id TEXT,binding_id TEXT,lease_owner TEXT,lease_version INTEGER,admission_closed INTEGER);CREATE TABLE provider_preparations(owner_kind TEXT,invocation_id TEXT,lease_owner TEXT,lease_version INTEGER,admission_closed INTEGER);CREATE TABLE provider_invocations(provider_invocation_id TEXT,run_id TEXT,attempt_id TEXT,invocation_state TEXT);CREATE TABLE planning_provider_invocations(planning_provider_invocation_id TEXT,invocation_state TEXT);CREATE TABLE provider_requests(binding_id TEXT,run_invocation_id TEXT,planning_invocation_id TEXT,execution_backend TEXT,remote_allocation INTEGER,lease_owner TEXT,lease_version INTEGER,admission_closed_at TEXT);CREATE TABLE remote_runtime_deletions(allocation INTEGER);CREATE TABLE remote_runtimes_never_created(allocation INTEGER);`)
	if e != nil {
		t.Fatal(e)
	}
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("inv-%d", i)
		for _, q := range []string{fmt.Sprintf(`INSERT INTO remote_runtimes VALUES(%d,'run','%s','binding-%d','worker',1,1)`, i, id, i), fmt.Sprintf(`INSERT INTO provider_preparations VALUES('run','%s','worker',1,1)`, id), fmt.Sprintf(`INSERT INTO provider_invocations VALUES('%s','run','attempt-%d','settled_success')`, id, i), fmt.Sprintf(`INSERT INTO provider_requests VALUES('binding-%d','%s',NULL,'openshell',%d,'worker',1,'closed')`, i, id, i)} {
			if _, e = db.Exec(q); e != nil {
				t.Fatal(e)
			}
		}
	}
	p := &disposalProvider{kind: providerexecution.BackendOpenShell}
	loop.provider = p
	return loop, p
}
func TestMaintenanceRetriesPostSettlementRemoteDisposal(t *testing.T) {
	loop, p := disposalMaintenanceFixture(t, 1)
	for pass := 0; pass < 2; pass++ {
		if e := loop.scheduleMaintenancePass(context.Background()); e != nil {
			t.Fatal(e)
		}
		waitMaintenanceAdmissionDrain(t, loop)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	want := "[drain:inv-1 discard:inv-1 drain:inv-1 discard:inv-1]"
	if fmt.Sprint(p.calls) != want {
		t.Fatalf("settled remote debt not recovered: %v", p.calls)
	}
	rows, e := providerrequest.ListRemoteDisposalCandidates(context.Background(), loop.db, 0, 64)
	if e != nil || len(rows) != 1 {
		t.Fatal("pending deletion lost durable debt", rows, e)
	}
}
func TestRemoteDisposalPaginationSkipsBusyAttempts(t *testing.T) {
	loop, p := disposalMaintenanceFixture(t, 65)
	for i := 1; i <= 64; i++ {
		if !loop.markExecutionInflight(fmt.Sprintf("attempt-%d", i), "run") {
			t.Fatal("inflight")
		}
	}
	if n, e := loop.scheduleRemoteDisposals(context.Background()); e != nil || n != 0 {
		t.Fatal(n, e)
	}
	if n, e := loop.scheduleRemoteDisposals(context.Background()); e != nil || n != 1 {
		t.Fatal(n, e)
	}
	waitMaintenanceAdmissionDrain(t, loop)
	p.mu.Lock()
	defer p.mu.Unlock()
	if fmt.Sprint(p.calls) != "[drain:inv-65 discard:inv-65]" {
		t.Fatal(p.calls)
	}
}
func TestLocalMaintenanceDoesNotDisposeRemoteDebt(t *testing.T) {
	loop, p := disposalMaintenanceFixture(t, 1)
	p.kind = providerexecution.BackendLocal
	if n, e := loop.scheduleRemoteDisposals(context.Background()); e != nil || n != 0 {
		t.Fatal(n, e)
	}
}
func TestRemoteDisposalCapacityPreservesNextCandidate(t *testing.T) {
	loop, p := disposalMaintenanceFixture(t, 2)
	hold := make(chan struct{})
	p.hold = hold
	p.started = make(chan struct{}, 2)
	if n, e := loop.scheduleRemoteDisposals(context.Background()); e != nil || n != 1 {
		t.Fatal(n, e)
	}
	<-p.started
	if loop.maintenance.remoteDisposalAllocation != 1 {
		t.Fatal("capacity exhaustion skipped next owner")
	}
	close(hold)
	waitMaintenanceAdmissionDrain(t, loop)
	if n, e := loop.scheduleRemoteDisposals(context.Background()); e != nil || n != 1 {
		t.Fatal(n, e)
	}
	waitMaintenanceAdmissionDrain(t, loop)
	p.mu.Lock()
	defer p.mu.Unlock()
	if fmt.Sprint(p.calls) != "[drain:inv-1 discard:inv-1 drain:inv-2 discard:inv-2]" {
		t.Fatal(p.calls)
	}
}
func TestRemoteDisposalHonorsRuntimeCancellation(t *testing.T) {
	loop, p := disposalMaintenanceFixture(t, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loop.maintenance.runtimeCtx = ctx
	p.hold = make(chan struct{})
	p.started = make(chan struct{}, 1)
	if n, e := loop.scheduleRemoteDisposals(context.Background()); e != nil || n != 1 {
		t.Fatal(n, e)
	}
	<-p.started
	cancel()
	waitMaintenanceAdmissionDrain(t, loop)
	p.mu.Lock()
	defer p.mu.Unlock()
	if fmt.Sprint(p.calls) != "[drain:inv-1]" {
		t.Fatal("discard escaped cancelled runtime", p.calls)
	}
}
func TestRemoteDisposalDrainFailureRemainsCandidate(t *testing.T) {
	loop, p := disposalMaintenanceFixture(t, 1)
	p.drainErr = errors.New("closure unresolved")
	if _, e := loop.scheduleRemoteDisposals(context.Background()); e != nil {
		t.Fatal(e)
	}
	waitMaintenanceAdmissionDrain(t, loop)
	rows, e := providerrequest.ListRemoteDisposalCandidates(context.Background(), loop.db, 0, 64)
	if e != nil || len(rows) != 1 {
		t.Fatal("cleanup failure lost debt", e)
	}
	p.mu.Lock()
	if fmt.Sprint(p.calls) != "[drain:inv-1]" {
		t.Fatal("discard ran without writer closure", p.calls)
	}
	p.drainErr = nil
	p.mu.Unlock()
	if _, e = loop.scheduleRemoteDisposals(context.Background()); e != nil {
		t.Fatal(e)
	}
	waitMaintenanceAdmissionDrain(t, loop)
	p.mu.Lock()
	defer p.mu.Unlock()
	if fmt.Sprint(p.calls) != "[drain:inv-1 drain:inv-1 discard:inv-1]" {
		t.Fatal(p.calls)
	}
}

type routedDisposalProvider struct{ *disposalProvider }

func (*routedDisposalProvider) SupportsRemoteCleanup() bool { return true }
func TestRemoteDisposalUsesRetainedCapabilityAfterDefaultSwitch(t *testing.T) {
	loop, p := disposalMaintenanceFixture(t, 1)
	p.kind = providerexecution.BackendLocal
	loop.provider = &routedDisposalProvider{p}
	if n, e := loop.scheduleRemoteDisposals(context.Background()); e != nil || n != 1 {
		t.Fatal(n, e)
	}
	waitMaintenanceAdmissionDrain(t, loop)
	p.mu.Lock()
	defer p.mu.Unlock()
	if fmt.Sprint(p.calls) != "[drain:inv-1 discard:inv-1]" {
		t.Fatal(p.calls)
	}
}
