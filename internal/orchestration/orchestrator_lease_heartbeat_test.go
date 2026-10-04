package orchestration

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/providers/opencode"
)

func TestProviderLeaseHeartbeatRenewsSilentExecution(t *testing.T) {
	db := openMigratedTestDB(t)
	seedMinimumGraph(t, db)
	store := executionstore.New(db)
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	job := seedHeartbeatProviderAttempt(t, db, store, base)

	var clock atomic.Int64
	clock.Store(base.UnixNano())
	adapter := newContextBlockingExecutionAdapter()
	created := make(chan *manualProviderLeaseTicker, 1)
	loop := &orchestrationLoop{
		executionStore: store,
		adapter:        adapter,
		now:            func() time.Time { return time.Unix(0, clock.Load()).UTC() },
		asyncDrain:     newExecutionDrainTracker(),
		newProviderLeaseTicker: func(time.Duration) providerLeaseTicker {
			ticker := newManualProviderLeaseTicker()
			created <- ticker
			return ticker
		},
	}
	executionCtx, cancelExecution := context.WithCancel(context.Background())
	defer cancelExecution()
	heartbeat := loop.startProviderLeaseHeartbeat(executionCtx, cancelExecution, job)
	if heartbeat == nil {
		t.Fatal("expected provider lease heartbeat")
	}
	executeDone := make(chan error, 1)
	go func() {
		_, err := loop.executeWithDurableProgress(executionCtx, job, opencode.ExecuteRequest{AttemptID: job.attemptID})
		executeDone <- err
	}()
	<-adapter.started
	ticker := <-created
	clock.Store(base.Add(10 * time.Second).UnixNano())
	ticker.ticks <- base.Add(10 * time.Second)
	result := heartbeat.stopAndWait()
	if result.err != nil || result.authorityLost {
		t.Fatalf("silent execution heartbeat failed: %#v", result)
	}

	attempt, found, err := store.GetAttempt(context.Background(), job.attemptID)
	if err != nil || !found {
		t.Fatalf("load heartbeat attempt: found=%t err=%v", found, err)
	}
	wantExpiry := base.Add(40 * time.Second).Format(time.RFC3339Nano)
	if attempt.LeaseExpiresAt != wantExpiry {
		t.Fatalf("heartbeat lease expiry=%q want=%q", attempt.LeaseExpiresAt, wantExpiry)
	}
	if attempt.LeaseOwner != job.expectedLeaseOwner || attempt.LeaseVersion != job.expectedLeaseVersion {
		t.Fatalf("heartbeat changed lease generation: %#v", attempt)
	}
	select {
	case err := <-executeDone:
		t.Fatalf("silent provider execution returned before cancellation: %v", err)
	default:
	}
	cancelExecution()
	<-adapter.cancelled
	if err := <-executeDone; err == nil || !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("silent provider execution cancellation error=%v", err)
	}
}

func TestProviderLeaseOwnershipLossCancelsAdapterAndCannotSettle(t *testing.T) {
	db := openMigratedTestDB(t)
	seedMinimumGraph(t, db)
	store := executionstore.New(db)
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	job := seedHeartbeatProviderAttempt(t, db, store, base)
	adapter := newContextBlockingExecutionAdapter()
	created := make(chan *manualProviderLeaseTicker, 1)
	loop := &orchestrationLoop{
		db:             db,
		adapter:        adapter,
		executionStore: store,
		inflightExec:   make(map[string]struct{}),
		asyncDrain:     newExecutionDrainTracker(),
		now:            func() time.Time { return base.Add(10 * time.Second) },
		newProviderLeaseTicker: func(time.Duration) providerLeaseTicker {
			ticker := newManualProviderLeaseTicker()
			created <- ticker
			return ticker
		},
	}

	loop.startAsyncExecution(context.Background(), job)
	<-adapter.started
	ticker := <-created
	mustExec(t, db, `UPDATE attempt_executions SET lease_owner = 'replacement-worker', lease_version = lease_version + 1 WHERE attempt_id = 'run_1:work_intake:1'`)
	ticker.ticks <- base.Add(10 * time.Second)
	<-adapter.cancelled
	waitCtx, cancelWait := context.WithTimeout(context.Background(), time.Second)
	defer cancelWait()
	if err := loop.waitForAsyncDrain(waitCtx); err != nil {
		t.Fatalf("wait for ownership-loss execution drain: %v", err)
	}

	var settlements, outboxRows int
	if err := db.QueryRow(`SELECT COUNT(1) FROM provider_completion_settlements WHERE attempt_id = ?`, job.attemptID).Scan(&settlements); err != nil {
		t.Fatalf("query stale provider settlements: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(1) FROM attempt_outbox WHERE attempt_id = ?`, job.attemptID).Scan(&outboxRows); err != nil {
		t.Fatalf("query stale provider outbox rows: %v", err)
	}
	if settlements != 0 || outboxRows != 0 {
		t.Fatalf("stale worker settled after ownership loss: settlements=%d outbox=%d", settlements, outboxRows)
	}
	var stateValue string
	if err := db.QueryRow(`SELECT state FROM attempt_executions WHERE attempt_id = ?`, job.attemptID).Scan(&stateValue); err != nil {
		t.Fatalf("query ownership-loss attempt state: %v", err)
	}
	if stateValue != "running" {
		t.Fatalf("ownership-loss worker changed canonical attempt state to %q", stateValue)
	}
}

func TestRuntimeOwnedAttemptDoesNotStartProviderLeaseHeartbeat(t *testing.T) {
	var tickerCalls atomic.Int64
	loop := &orchestrationLoop{
		newProviderLeaseTicker: func(time.Duration) providerLeaseTicker {
			tickerCalls.Add(1)
			return newManualProviderLeaseTicker()
		},
	}
	job := asyncExecutionResult{
		attempt:               nodeAttempt{ExecutionMode: executionstore.ExecutionModeRuntimeOwned},
		attemptID:             "runtime-owned-attempt",
		expectedLeaseOwner:    "must-not-be-used",
		expectedLeaseVersion:  1,
		providerLeaseDuration: 30 * time.Second,
	}
	if heartbeat := loop.startProviderLeaseHeartbeat(context.Background(), func() {}, job); heartbeat != nil {
		t.Fatal("runtime-owned attempt started a provider lease heartbeat")
	}
	if calls := tickerCalls.Load(); calls != 0 {
		t.Fatalf("runtime-owned attempt created %d provider lease tickers", calls)
	}
}

func TestProviderLeaseHeartbeatReportsDatabaseErrorsThroughAsyncDrain(t *testing.T) {
	db := openMigratedTestDB(t)
	seedMinimumGraph(t, db)
	store := executionstore.New(db)
	base := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	job := seedHeartbeatProviderAttempt(t, db, store, base)
	created := make(chan *manualProviderLeaseTicker, 1)
	loop := &orchestrationLoop{
		executionStore: store,
		now:            func() time.Time { return base.Add(10 * time.Second) },
		asyncDrain:     newExecutionDrainTracker(),
		newProviderLeaseTicker: func(time.Duration) providerLeaseTicker {
			ticker := newManualProviderLeaseTicker()
			created <- ticker
			return ticker
		},
	}
	executionCtx, cancelExecution := context.WithCancel(context.Background())
	heartbeat := loop.startProviderLeaseHeartbeat(executionCtx, cancelExecution, job)
	if heartbeat == nil {
		t.Fatal("expected provider lease heartbeat")
	}
	ticker := <-created
	if err := db.Close(); err != nil {
		t.Fatalf("close heartbeat database: %v", err)
	}
	ticker.ticks <- base.Add(10 * time.Second)
	result := heartbeat.stopAndWait()
	if result.err == nil || result.authorityLost {
		t.Fatalf("database failure was not returned as a heartbeat error: %#v", result)
	}
	select {
	case <-executionCtx.Done():
	default:
		t.Fatal("heartbeat database error did not cancel execution context")
	}
	if err := loop.waitForAsyncDrain(context.Background()); err == nil || !strings.Contains(err.Error(), "provider lease heartbeat failed") {
		t.Fatalf("heartbeat database error was not reported through async drain: %v", err)
	}
}

func TestProviderLeaseHeartbeatIntervalIsCappedByLeaseDuration(t *testing.T) {
	loop := &orchestrationLoop{providerLeaseHeartbeatInterval: time.Minute}
	if got, want := loop.providerExecutionLeaseHeartbeatEvery(30*time.Second), 10*time.Second; got != want {
		t.Fatalf("heartbeat interval=%s want=%s", got, want)
	}
	loop.providerLeaseHeartbeatInterval = 2 * time.Second
	if got, want := loop.providerExecutionLeaseHeartbeatEvery(30*time.Second), 2*time.Second; got != want {
		t.Fatalf("configured heartbeat interval=%s want=%s", got, want)
	}
}

func seedHeartbeatProviderAttempt(t *testing.T, db *sql.DB, store *executionstore.Store, base time.Time) asyncExecutionResult {
	t.Helper()
	const attemptID = "run_1:work_intake:1"
	if err := store.EnqueueAttempt(context.Background(), executionstore.EnqueueAttemptRequest{
		AttemptID:      attemptID,
		RunID:          "run_1",
		NodeRunID:      "nr_1",
		ExecutionMode:  executionstore.ExecutionModeProviderBacked,
		CompletionMode: executionstore.ProviderCompletionModeServerResponse,
		CreatedAt:      base.Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("enqueue heartbeat provider attempt: %v", err)
	}
	claim, acquired, err := store.ClaimQueuedProviderAttempt(context.Background(), executionstore.ClaimQueuedProviderAttemptRequest{
		AttemptID:  attemptID,
		LeaseOwner: "heartbeat-worker",
		LeaseUntil: base.Add(30 * time.Second).Format(time.RFC3339Nano),
		ClaimedAt:  base.Format(time.RFC3339Nano),
	})
	if err != nil || !acquired {
		t.Fatalf("claim heartbeat provider attempt: acquired=%t err=%v", acquired, err)
	}
	mustExec(t, db, `UPDATE attempt_executions SET state = 'running' WHERE attempt_id = 'run_1:work_intake:1'`)
	return asyncExecutionResult{
		run:                    runSnapshot{RunID: "run_1", RunStatus: "running"},
		node:                   nodeDefinition{NodeID: "work_intake", NodeClass: "work_intake", RequiredRole: "governance"},
		attempt:                nodeAttempt{NodeRunID: "nr_1", NodeID: "work_intake", Attempt: 1, ExecutionMode: executionstore.ExecutionModeProviderBacked},
		attemptID:              attemptID,
		expectedLeaseOwner:     claim.LeaseOwner,
		expectedLeaseVersion:   claim.LeaseVersion,
		expectedLeaseExpiresAt: claim.LeaseExpiresAt,
		providerLeaseDuration:  30 * time.Second,
	}
}

type manualProviderLeaseTicker struct {
	ticks   chan time.Time
	stopped chan struct{}
	once    sync.Once
}

func newManualProviderLeaseTicker() *manualProviderLeaseTicker {
	return &manualProviderLeaseTicker{ticks: make(chan time.Time), stopped: make(chan struct{})}
}

func (t *manualProviderLeaseTicker) C() <-chan time.Time {
	return t.ticks
}

func (t *manualProviderLeaseTicker) Stop() {
	t.once.Do(func() { close(t.stopped) })
}

type contextBlockingExecutionAdapter struct {
	started   chan struct{}
	cancelled chan struct{}
	once      sync.Once
}

func newContextBlockingExecutionAdapter() *contextBlockingExecutionAdapter {
	return &contextBlockingExecutionAdapter{started: make(chan struct{}), cancelled: make(chan struct{})}
}

func (a *contextBlockingExecutionAdapter) Execute(ctx context.Context, _ opencode.ExecuteRequest) (opencode.ExecuteResponse, error) {
	close(a.started)
	<-ctx.Done()
	a.once.Do(func() { close(a.cancelled) })
	return opencode.ExecuteResponse{}, ctx.Err()
}

func (a *contextBlockingExecutionAdapter) Cancel(context.Context, opencode.CancelRequest) error {
	return nil
}

func (a *contextBlockingExecutionAdapter) Stream(context.Context, opencode.StreamRequest) (<-chan opencode.AttemptStreamEvent, <-chan error, error) {
	events := make(chan opencode.AttemptStreamEvent)
	errs := make(chan error)
	close(events)
	close(errs)
	return events, errs, nil
}
