package orchestration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/core/workflow"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/providers/opencode"
	"kingdom/internal/testutil"
)

// requestObserver wraps a provider and records what the canonical records show
// at each step of a request's life, so tests can assert the order in which
// dispatch prepares, binds, runs, cancels and discards a request.
type requestObserver struct {
	opencode.Provider
	db *sql.DB

	prepareErr    error
	beforePrepare func(context.Context, opencode.ExecuteRequest) error
	afterPrepare  func(context.Context, opencode.ExecuteRequest) error
	onDrain       func(string) error
	onDiscard     func(string) error

	mu                   sync.Mutex
	events               []string
	prepared             opencode.Prepared
	invocationsAtPrepare int
	bindingAtExecute     providerrequest.Binding
	boundAtExecute       bool
	leaseAtExecute       providerrequest.Lease
	executed             opencode.ExecuteResponse
	cancelled            []string
}

func newRequestObserver(db *sql.DB, provider opencode.Provider) *requestObserver {
	return &requestObserver{Provider: provider, db: db}
}

func (o *requestObserver) record(event string) {
	o.events = append(o.events, event)
}

func (o *requestObserver) Prepare(ctx context.Context, req opencode.ExecuteRequest) (opencode.Prepared, error) {
	var invocations int
	err := o.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM provider_invocations WHERE provider_invocation_id = ?`, req.InvocationID).Scan(&invocations)
	o.mu.Lock()
	defer o.mu.Unlock()
	o.record("prepare:" + req.InvocationID)
	if err != nil {
		return opencode.Prepared{}, err
	}
	o.invocationsAtPrepare = invocations
	if o.beforePrepare != nil {
		if err := o.beforePrepare(ctx, req); err != nil {
			return opencode.Prepared{}, err
		}
	}
	if o.prepareErr != nil {
		return opencode.Prepared{}, o.prepareErr
	}
	prepared, err := o.Provider.Prepare(ctx, req)
	o.prepared = prepared
	if err == nil && o.afterPrepare != nil {
		err = o.afterPrepare(ctx, req)
	}
	return prepared, err
}

func (o *requestObserver) Execute(ctx context.Context, req opencode.ExecuteRequest) (opencode.ExecuteResponse, error) {
	binding, bound, bindingErr := providerrequest.ForOwner(ctx, o.db, providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: req.InvocationID})
	var lease providerrequest.Lease
	leaseErr := o.db.QueryRowContext(ctx, `SELECT lease_owner, lease_version FROM attempt_executions WHERE attempt_id = ?`, req.AttemptID).Scan(&lease.Owner, &lease.Version)
	o.mu.Lock()
	o.record("execute:" + req.InvocationID)
	o.bindingAtExecute, o.boundAtExecute, o.leaseAtExecute = binding, bound, lease
	o.mu.Unlock()
	if err := errors.Join(bindingErr, leaseErr); err != nil {
		return opencode.ExecuteResponse{}, err
	}
	response, err := o.Provider.Execute(ctx, req)
	o.mu.Lock()
	o.executed = response
	o.mu.Unlock()
	return response, err
}

func (o *requestObserver) Cancel(ctx context.Context, attemptID string) error {
	o.mu.Lock()
	o.record("cancel:" + attemptID)
	o.cancelled = append(o.cancelled, attemptID)
	o.mu.Unlock()
	return o.Provider.Cancel(ctx, attemptID)
}

func (o *requestObserver) Drain(ctx context.Context, invocationID string) error {
	o.mu.Lock()
	o.record("drain:" + invocationID)
	o.mu.Unlock()
	if o.onDrain != nil {
		if err := o.onDrain(invocationID); err != nil {
			return err
		}
	}
	return o.Provider.Drain(ctx, invocationID)
}

func (o *requestObserver) Discard(ctx context.Context, invocationID string) error {
	o.mu.Lock()
	o.record("discard:" + invocationID)
	o.mu.Unlock()
	if o.onDiscard != nil {
		if err := o.onDiscard(invocationID); err != nil {
			return err
		}
	}
	return o.Provider.Discard(ctx, invocationID)
}

func (o *requestObserver) eventsSnapshot() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.events...)
}

const requestDispatchRunID = "run_provider_request"

type requestDispatch struct {
	rt        *Runtime
	run       runSnapshot
	node      nodeDefinition
	attempt   nodeAttempt
	attemptID string
}

// newRequestDispatch seeds a ready plan-drafting node and a runtime whose
// provider is the observer around the given responder.
func newRequestDispatch(t *testing.T, db *sql.DB, responder opencode.Responder) (*requestDispatch, *requestObserver) {
	t.Helper()
	seedReadyDispatchNodeRun(t, db, readyDispatchNodeRunSeed{
		RunID:        requestDispatchRunID,
		WorkOrderID:  "work_order_provider_request",
		WorkflowID:   "wfdef_provider_request",
		NodeRunID:    "nr_provider_request_1",
		NodeID:       "plan_drafting",
		NodeClass:    workflow.NodeClassPlanDrafting,
		RequiredRole: "plan-author",
		AgentID:      "agent_provider_request",
		RoleIDs:      []string{"plan-author", "coordinator"},
		ExtraAgents:  []readyDispatchAgentSeed{{AgentID: "agent_coordinator_provider_request", RoleID: "coordinator"}},
	})
	observer := newRequestObserver(db, fixtureProvider(db, responder))
	rt, err := newMemoryRuntime(t, Config{DB: db, Provider: observer, Paths: DefaultBootstrapPaths(repoRoot(t)), YoloOutputRoot: t.TempDir()})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	return &requestDispatch{
		rt:        rt,
		run:       runSnapshot{RunID: requestDispatchRunID, RunStatus: "running", PlanState: "approved"},
		node:      nodeDefinition{NodeID: "plan_drafting", RequiredRole: "plan-author", NodeClass: workflow.NodeClassPlanDrafting},
		attempt:   nodeAttempt{NodeRunID: "nr_provider_request_1", NodeID: "plan_drafting", Attempt: 1, Wave: 1, NodeStatus: "ready", RequiredRole: "plan-author", AgentID: "agent_provider_request"},
		attemptID: requestDispatchRunID + ":plan_drafting:1",
	}, observer
}

func (d *requestDispatch) dispatch(t *testing.T) {
	t.Helper()
	if err := d.rt.orchestrator.dispatchNodeAttempt(context.Background(), d.run, d.node, d.attempt, "2026-03-01T12:00:02Z"); err != nil {
		t.Fatalf("dispatch node attempt: %v", err)
	}
}

// dispatchAndDrain dispatches the attempt and waits until preparation and any
// provider worker it admitted have finished.
func (d *requestDispatch) dispatchAndDrain(t *testing.T) {
	t.Helper()
	d.dispatch(t)
	awaitPreparationDrain(t, d.rt.orchestrator)
}

func (d *requestDispatch) countRows(t *testing.T, table string) int {
	t.Helper()
	return countTestRows(t, d.rt.db, `SELECT COUNT(1) FROM `+table+` WHERE attempt_id = ?`, d.attemptID)
}

func TestDispatchPreparesRequestBeforeActivatingAndBindsItsIdentityAndLease(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	applyMigration(t, db)
	dispatch, observer := newRequestDispatch(t, db, &sequenceExecutionAdapter{outputs: []string{"Plan drafted."}})

	dispatch.dispatchAndDrain(t)
	testutil.Eventually(t, 10*time.Second, 10*time.Millisecond, func() error {
		if err := dispatch.rt.drainExecutionOutboxUntilIdle(context.Background()); err != nil {
			return err
		}
		var attemptState string
		if err := db.QueryRow(`SELECT state FROM attempt_executions WHERE attempt_id = ?`, dispatch.attemptID).Scan(&attemptState); err != nil {
			return err
		}
		if attemptState != "committed_success" {
			return fmt.Errorf("attempt state=%q", attemptState)
		}
		return nil
	})

	var invocationID string
	if err := db.QueryRow(`SELECT provider_invocation_id FROM provider_invocations WHERE attempt_id = ?`, dispatch.attemptID).Scan(&invocationID); err != nil {
		t.Fatalf("load the attempt's only provider invocation: %v", err)
	}
	if got, want := observer.eventsSnapshot(), []string{"prepare:" + invocationID, "execute:" + invocationID}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("provider calls=%v, want %v", got, want)
	}
	if observer.invocationsAtPrepare != 1 {
		t.Fatalf("provider startup lacked its canonical invocation (%d rows)", observer.invocationsAtPrepare)
	}
	if !observer.boundAtExecute {
		t.Fatal("the request ran before its identity was bound to the canonical invocation")
	}
	if observer.bindingAtExecute.Identity != observer.prepared.Identity {
		t.Fatalf("bound identity %+v differs from the prepared identity %+v", observer.bindingAtExecute.Identity, observer.prepared.Identity)
	}
	if observer.leaseAtExecute.Owner == "" || observer.bindingAtExecute.Lease != observer.leaseAtExecute {
		t.Fatalf("binding lease %+v differs from the attempt's lease %+v", observer.bindingAtExecute.Lease, observer.leaseAtExecute)
	}
	if observer.bindingAtExecute.Owner != (providerrequest.Owner{Kind: providerrequest.OwnerRun, InvocationID: invocationID}) {
		t.Fatalf("binding owner=%+v", observer.bindingAtExecute.Owner)
	}
	var providerSessionID string
	if err := db.QueryRow(`SELECT provider_session_id FROM provider_invocations WHERE provider_invocation_id = ?`, invocationID).Scan(&providerSessionID); err != nil || providerSessionID != observer.prepared.ProviderSessionID {
		t.Fatalf("invocation provider session=%q err=%v, want the prepared session %q", providerSessionID, err, observer.prepared.ProviderSessionID)
	}
}

func TestFailedActivationDiscardsPreparedRequestAndFailsNodeSetup(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	applyMigration(t, db)
	dispatch, observer := newRequestDispatch(t, db, &sequenceExecutionAdapter{outputs: []string{"must not execute"}})
	mustExec(t, db, `CREATE TRIGGER fail_request_binding BEFORE INSERT ON provider_requests BEGIN SELECT RAISE(FAIL, 'request binding unavailable'); END`)

	dispatch.dispatchAndDrain(t)

	var invocationID string
	if events := observer.eventsSnapshot(); len(events) == 3 {
		invocationID = events[0][len("prepare:"):]
	}
	if got, want := observer.eventsSnapshot(), []string{"prepare:" + invocationID, "drain:" + invocationID, "discard:" + invocationID}; invocationID == "" || fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("provider calls=%v, want a prepare followed by a discard of the same request", got)
	}
	assertSetupFailed(t, db, dispatch.attemptID, "create provider invocation")
	assertIssuedPreparationAbandoned(t, db, dispatch.attemptID)
}

func TestPrepareFailureRetainsAbandonedInvocationWithoutProviderEvidence(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	applyMigration(t, db)
	dispatch, observer := newRequestDispatch(t, db, &sequenceExecutionAdapter{outputs: []string{"must not execute"}})
	observer.prepareErr = errors.New("provider server is unavailable")
	observer.onDiscard = func(id string) error {
		var state string
		if err := db.QueryRow(`SELECT invocation_state FROM provider_invocations WHERE provider_invocation_id=?`, id).Scan(&state); err != nil {
			return err
		}
		if state != "issued" {
			t.Fatalf("issued preparation invalidated before disposal: %s", state)
		}
		return nil
	}

	dispatch.dispatchAndDrain(t)

	events := observer.eventsSnapshot()
	if len(events) != 3 || events[1] != "drain:"+events[0][len("prepare:"):] || events[2] != "discard:"+events[0][len("prepare:"):] {
		t.Fatalf("provider calls=%v, want prepare, drain, discard", events)
	}
	assertSetupFailed(t, db, dispatch.attemptID, "prepare provider request")
	assertIssuedPreparationAbandoned(t, db, dispatch.attemptID)
	if rows := countTestRows(t, db, `SELECT COUNT(1) FROM provider_requests`); rows != 0 {
		t.Fatalf("prepare failure bound %d provider requests", rows)
	}
}

func TestLostLeaseBetweenPrepareAndLaunchDrainsPreparedRequest(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	applyMigration(t, db)
	dispatch, observer := newRequestDispatch(t, db, &sequenceExecutionAdapter{outputs: []string{"must not execute"}})
	mustExec(t, db, `CREATE TRIGGER lose_lease_after_start_state AFTER UPDATE OF node_status ON node_runs
		WHEN NEW.node_status = 'starting'
		BEGIN UPDATE attempt_executions SET lease_owner = 'replacement-worker', lease_version = lease_version + 1 WHERE node_run_id = NEW.node_run_id; END`)

	dispatch.dispatchAndDrain(t)

	var invocationID string
	if events := observer.eventsSnapshot(); len(events) == 2 {
		invocationID = events[0][len("prepare:"):]
	}
	if got, want := observer.eventsSnapshot(), []string{"prepare:" + invocationID, "drain:" + invocationID}; invocationID == "" || fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("provider calls=%v, want a prepare followed by a drain and no disposal or execute", got)
	}
	if rows := countTestRows(t, db, `SELECT COUNT(1) FROM provider_requests WHERE dispatch_issued_at IS NOT NULL`); rows != 0 {
		t.Fatalf("a request was dispatched after its lease was lost: %d", rows)
	}
}

func assertSetupFailed(t *testing.T, db *sql.DB, attemptID, step string) {
	t.Helper()
	var attemptState, failureCode string
	if err := db.QueryRow(`SELECT state, failure_code FROM attempt_executions WHERE attempt_id = ?`, attemptID).Scan(&attemptState, &failureCode); err != nil {
		t.Fatalf("load setup-failed attempt: %v", err)
	}
	if attemptState != "committed_failure" || failureCode != "execution_setup_failed" {
		t.Fatalf("attempt state=%q failure_code=%q, want a failed node setup", attemptState, failureCode)
	}
	var failedStep string
	if err := db.QueryRow(`SELECT json_extract(payload_json, '$.step') FROM events WHERE attempt_id = ? AND event_type = 'attempt.failed'`, attemptID).Scan(&failedStep); err != nil || failedStep != step {
		t.Fatalf("setup failure step=%q err=%v, want %q", failedStep, err, step)
	}
}

// cancellableResponder blocks a request until the server is asked to cancel it,
// then reports the request as interrupted.
type cancellableResponder struct {
	started   chan struct{}
	cancelled chan struct{}
	once      sync.Once
}

func newCancellableResponder() *cancellableResponder {
	return &cancellableResponder{started: make(chan struct{}), cancelled: make(chan struct{})}
}

func (r *cancellableResponder) Execute(ctx context.Context, _ opencode.ExecuteRequest) (opencode.ExecuteResponse, error) {
	close(r.started)
	select {
	case <-r.cancelled:
	case <-ctx.Done():
	}
	return interruptedResponse(), context.Canceled
}

func (r *cancellableResponder) Cancel(context.Context, string) error {
	r.once.Do(func() { close(r.cancelled) })
	return nil
}

func TestAttemptCancellationInterruptsProviderRequestAndSettlesInterrupted(t *testing.T) {
	t.Parallel()
	db := openTestDB(t)
	applyMigration(t, db)
	responder := newCancellableResponder()
	dispatch, observer := newRequestDispatch(t, db, responder)
	loop := dispatch.rt.orchestrator

	dispatch.dispatch(t)
	select {
	case <-responder.started:
	case <-time.After(5 * time.Second):
		t.Fatal("provider request did not start")
	}
	terminalizePreparationRun(t, loop, requestDispatchRunID, "cancelled")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := loop.drainAttemptCancelOutbox(ctx); err != nil {
		t.Fatalf("dispatch the attempt cancellation: %v", err)
	}
	if err := loop.waitForAsyncDrain(ctx); err != nil {
		t.Fatalf("drain the interrupted request: %v", err)
	}
	testutil.Eventually(t, 10*time.Second, 10*time.Millisecond, func() error {
		_, err := loop.convergeProviderTerminalHandoffsWithLimit(ctx, 8)
		if err != nil {
			return err
		}
		var settlements int
		if err := db.QueryRow(`SELECT COUNT(1) FROM provider_completion_settlements WHERE attempt_id = ?`, dispatch.attemptID).Scan(&settlements); err != nil {
			return err
		}
		if settlements != 1 {
			return fmt.Errorf("interrupted request is not settled yet: settlements=%d", settlements)
		}
		return nil
	})

	observer.mu.Lock()
	cancelled := append([]string(nil), observer.cancelled...)
	terminal := observer.executed.StreamObservation.Request
	observer.mu.Unlock()
	if len(cancelled) != 1 || cancelled[0] != dispatch.attemptID {
		t.Fatalf("provider cancellations=%+v, want one for %s", cancelled, dispatch.attemptID)
	}
	if terminal == nil || terminal.Terminal.Outcome != providerexecution.RequestOutcomeInterrupted {
		t.Fatalf("recorded request terminal=%+v, want an interrupted request", terminal)
	}
	var outcome, invocationState string
	if err := db.QueryRow(`SELECT s.outcome, pi.invocation_state FROM provider_completion_settlements s JOIN provider_invocations pi ON pi.provider_invocation_id = s.provider_invocation_id WHERE s.attempt_id = ?`, dispatch.attemptID).Scan(&outcome, &invocationState); err != nil {
		t.Fatalf("load cancelled settlement: %v", err)
	}
	if outcome != "cancelled" || invocationState != "settled_cancelled" {
		t.Fatalf("settlement outcome=%q invocation state=%q, want cancelled", outcome, invocationState)
	}
}

func TestRestartConsumesCompletedHandoffWithoutExecutingTheRequestAgain(t *testing.T) {
	h := newTerminalHandoffHarness(t)
	h.loop.now = func() time.Time { return time.Date(2026, 3, 1, 12, 0, 4, 0, time.UTC) }
	invocation := h.addInvocation(t, 1)
	h.persistHandoffFileOnly(t, invocation, 1, "2026-03-01T12:00:04Z")
	if got := h.requests.executions(invocation.ProviderInvocationID); got != 1 {
		t.Fatalf("request executions before the restart=%d, want 1", got)
	}
	mustExec(t, h.db, `UPDATE attempt_executions SET lease_expires_at = '2026-03-01T12:00:05Z'`)
	h.loop.now = func() time.Time { return time.Date(2026, 3, 1, 12, 10, 0, 0, time.UTC) }

	replayStartupHandoffsThroughMaintenance(t, h)

	settlement, found, err := h.store.GetProviderCompletionSettlementByInvocation(context.Background(), invocation.ProviderInvocationID)
	if err != nil || !found || settlement.Outcome != "success" {
		t.Fatalf("restart settlement found=%t settlement=%+v err=%v", found, settlement, err)
	}
	if got := h.requests.executions(invocation.ProviderInvocationID); got != 1 {
		t.Fatalf("restart executed the request again: executions=%d", got)
	}
}

func assertIssuedPreparationAbandoned(t *testing.T, db *sql.DB, attemptID string) {
	t.Helper()
	var state string
	if err := db.QueryRow(`SELECT invocation_state FROM provider_invocations WHERE attempt_id=?`, attemptID).Scan(&state); err != nil || state != "invalidated" {
		t.Fatalf("retained startup state %q: %v", state, err)
	}
	if n := countTestRows(t, db, `SELECT count(*) FROM provider_completion_evidence WHERE attempt_id=?`, attemptID); n != 0 {
		t.Fatalf("preparation created %d provider evidence records", n)
	}
}

func TestCancellationDuringPreparationDiscardsUnactivatedRequest(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	dispatch, observer := newRequestDispatch(t, db, &sequenceExecutionAdapter{outputs: []string{"must not execute"}})
	observer.afterPrepare = func(ctx context.Context, req opencode.ExecuteRequest) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		if err = providerrequest.CloseAttemptAdmissionTx(ctx, tx, req.AttemptID, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
		return tx.Commit()
	}
	dispatch.dispatchAndDrain(t)
	events := observer.eventsSnapshot()
	if len(events) != 3 || events[1] != "drain:"+events[0][len("prepare:"):] || events[2] != "discard:"+events[0][len("prepare:"):] {
		t.Fatalf("cancellation skipped prepared cleanup: %v", events)
	}
}

func TestBoundStartFailureDrainsBeforeInvalidationAndDisposesAfter(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	dispatch, observer := newRequestDispatch(t, db, &sequenceExecutionAdapter{outputs: []string{"must not execute"}})
	mustExec(t, db, `CREATE TRIGGER fail_start BEFORE UPDATE OF node_status ON node_runs WHEN NEW.node_status='starting' BEGIN SELECT RAISE(FAIL,'start persistence failed'); END`)
	observer.onDrain = func(id string) error {
		var state string
		if e := db.QueryRow(`SELECT invocation_state FROM provider_invocations WHERE provider_invocation_id=?`, id).Scan(&state); e != nil {
			return e
		}
		if state != "open" {
			t.Fatalf("drain state=%s", state)
		}
		return nil
	}
	observer.onDiscard = func(id string) error {
		var state string
		if e := db.QueryRow(`SELECT invocation_state FROM provider_invocations WHERE provider_invocation_id=?`, id).Scan(&state); e != nil {
			return e
		}
		if state != "invalidated" {
			t.Fatalf("disposal preceded invalidation: %s", state)
		}
		return nil
	}
	dispatch.dispatchAndDrain(t)
	events := observer.eventsSnapshot()
	if len(events) != 3 || !strings.HasPrefix(events[1], "drain:") || !strings.HasPrefix(events[2], "discard:") {
		t.Fatal(events)
	}
	assertSetupFailed(t, db, dispatch.attemptID, "mark node starting")
}

func TestUnresolvedPreparationDrainRetainsCanonicalOwner(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	dispatch, observer := newRequestDispatch(t, db, &sequenceExecutionAdapter{outputs: []string{"must not execute"}})
	observer.prepareErr = errors.New("prepare failed")
	observer.onDrain = func(string) error { return errors.New("writers unresolved") }
	dispatch.dispatchAndDrain(t)
	events := observer.eventsSnapshot()
	if len(events) != 2 || !strings.HasPrefix(events[1], "drain:") {
		t.Fatal(events)
	}
	var state string
	if err := db.QueryRow(`SELECT invocation_state FROM provider_invocations WHERE attempt_id=?`, dispatch.attemptID).Scan(&state); err != nil || state != "issued" {
		t.Fatalf("owner released before drain: %s %v", state, err)
	}
}
