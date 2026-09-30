package orchestration

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"kingdom/internal/core/workflow"
	"kingdom/internal/testutil"
)

func TestDispatchPreparationDoesNotWaitForBusyWorkspace(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedReadyDispatchNodeRun(t, db, readyDispatchNodeRunSeed{
		RunID: "run_prepare_busy", DecreeID: "decree_prepare_busy", WorkflowID: "wf_prepare_busy",
		NodeRunID: "nr_prepare_busy", NodeID: "delegated_execution_berserker", NodeClass: workflow.NodeClassDelegatedExecution,
		RequiredRole: "builder-berserker", AgentID: "agent_prepare_busy", RoleIDs: []string{"builder-berserker"},
	})
	rt, err := newMemoryRuntime(t, Config{DB: db, Adapter: &sequenceExecutionAdapter{}, Paths: DefaultBootstrapPaths(repoRoot(t)), YoloOutputRoot: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	unlock := rt.orchestrator.lockRunWorkspace("run_prepare_busy")
	var once sync.Once
	defer once.Do(unlock)
	done := make(chan error, 1)
	go func() {
		done <- rt.orchestrator.dispatchNodeAttempt(ctx,
			runSnapshot{RunID: "run_prepare_busy", RunStatus: "running", PlanState: "approved", PublishMode: PublishModeDirect},
			nodeDefinition{NodeID: "delegated_execution_berserker", NodeClass: workflow.NodeClassDelegatedExecution, RequiredRole: "builder-berserker"},
			nodeAttempt{NodeRunID: "nr_prepare_busy", NodeID: "delegated_execution_berserker", Attempt: 1, Wave: 1, NodeStatus: "ready", RequiredRole: "builder-berserker", AgentID: "agent_prepare_busy"}, time.Now().UTC().Format(time.RFC3339Nano))
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		cancel()
		once.Do(unlock)
		<-done
		t.Fatal("dispatch waited for a busy workspace instead of leaving preparation queued")
	}
	if rt.orchestrator.executionAttemptInflight("run_prepare_busy:delegated_execution_berserker:1") {
		t.Fatal("busy workspace parked an admitted preparation worker")
	}
}

func TestDispatchPreparationRenewsQueuedLeaseAndUsesFreshLaunchTime(t *testing.T) {
	rt, adapter, run, node, attempt, entered, release := newBlockedPreparation(t)
	base := time.Now().UTC().Add(-20 * time.Second)
	var clock atomic.Int64
	clock.Store(base.UnixNano())
	rt.orchestrator.now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	tickers := make(chan *manualProviderLeaseTicker, 2)
	rt.orchestrator.newProviderLeaseTicker = func(time.Duration) providerLeaseTicker {
		ticker := newManualProviderLeaseTicker()
		tickers <- ticker
		return ticker
	}
	if err := rt.orchestrator.dispatchNodeAttempt(context.Background(), run, node, attempt, base.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	waitPreparationEntered(t, entered)
	attemptID := composeAttemptID(run.RunID, node.NodeID, attempt.Attempt)
	before, found, err := rt.orchestrator.executionStore.GetAttempt(context.Background(), attemptID)
	if err != nil || !found {
		t.Fatalf("load preparation: found=%t err=%v", found, err)
	}
	if before.State != "queued" || before.LeaseOwner == "" || before.StartedAt != "" || !rt.orchestrator.executionAttemptInflight(attemptID) {
		t.Fatalf("preparation ownership=%#v", before)
	}
	var sessions int
	if err := rt.orchestrator.db.QueryRow(`SELECT COUNT(*) FROM session_leases`).Scan(&sessions); err != nil {
		t.Fatal(err)
	}
	if sessions != 0 || adapter.callsCount() != 0 {
		t.Fatalf("preparation created session=%d or launched provider=%d", sessions, adapter.callsCount())
	}
	var preparingEvents, preparedEvents int
	if err := rt.orchestrator.db.QueryRow(`SELECT COUNT(*) FROM events WHERE attempt_id = ? AND reason_code IN ('preparation_admitted', 'workspace_preparing')`, attemptID).Scan(&preparingEvents); err != nil {
		t.Fatal(err)
	}
	if err := rt.orchestrator.db.QueryRow(`SELECT COUNT(*) FROM events WHERE attempt_id = ? AND reason_code = 'workspace_prepared'`, attemptID).Scan(&preparedEvents); err != nil {
		t.Fatal(err)
	}
	if preparingEvents != 2 || preparedEvents != 0 {
		t.Fatalf("in-progress telemetry=%d/%d", preparingEvents, preparedEvents)
	}
	launchAt := base.Add(20 * time.Second)
	clock.Store(launchAt.UnixNano())
	ticker := <-tickers
	ticker.ticks <- launchAt
	testutil.Eventually(t, time.Second, time.Millisecond, func() error {
		a, _, err := rt.orchestrator.executionStore.GetAttempt(context.Background(), attemptID)
		if err != nil {
			return err
		}
		if a.LeaseExpiresAt == before.LeaseExpiresAt {
			return fmt.Errorf("queued lease has not renewed")
		}
		if a.LeaseOwner != before.LeaseOwner || a.LeaseVersion != before.LeaseVersion || a.State != "queued" {
			return fmt.Errorf("renewal changed lineage: %#v", a)
		}
		return nil
	})
	release()
	awaitPreparationDrain(t, rt.orchestrator)
	requests := adapter.requestsSnapshot()
	if len(requests) != 1 || requests[0].OccurredAt != launchAt.Format(time.RFC3339Nano) {
		t.Fatalf("launch requests=%#v", requests)
	}
	var sessionCreatedAt string
	if err := rt.orchestrator.db.QueryRow(`SELECT created_at FROM session_leases WHERE session_id = ?`, sessionIDForAttempt(attemptID)).Scan(&sessionCreatedAt); err != nil {
		t.Fatal(err)
	}
	if sessionCreatedAt != launchAt.Format(time.RFC3339Nano) {
		t.Fatalf("session created_at=%s want fresh %s", sessionCreatedAt, launchAt)
	}
	var preparedAt, workspaceKind string
	if err := rt.orchestrator.db.QueryRow(`SELECT created_at, json_extract(payload_json, '$.workspace_kind') FROM events WHERE attempt_id = ? AND reason_code = 'workspace_prepared'`, attemptID).Scan(&preparedAt, &workspaceKind); err != nil {
		t.Fatal(err)
	}
	if preparedAt != launchAt.Format(time.RFC3339Nano) || workspaceKind != string(workspacePrivateSource) {
		t.Fatalf("prepared telemetry at=%s kind=%s", preparedAt, workspaceKind)
	}
}

func TestDispatchPreparationBusyPrivateGitRemainsQueued(t *testing.T) {
	rt, adapter, run, node, attempt, _, _ := newBlockedPreparation(t)
	run.PublishMode = PublishModeGitWorktree
	unlock := rt.orchestrator.lockRunWorkspace(run.RunID)
	defer unlock()
	if err := rt.orchestrator.dispatchNodeAttempt(context.Background(), run, node, attempt, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	awaitPreparationDrain(t, rt.orchestrator)
	a, found, err := rt.orchestrator.executionStore.GetAttempt(context.Background(), composeAttemptID(run.RunID, node.NodeID, attempt.Attempt))
	if err != nil || !found || a.State != "queued" || a.LeaseOwner != "" || a.FailureCode != "" || adapter.callsCount() != 0 {
		t.Fatalf("busy private Git preparation terminalized or retained worker: %#v found=%t err=%v calls=%d", a, found, err, adapter.callsCount())
	}
}

func TestDispatchPreparationFailureUsesCurrentLeaseTime(t *testing.T) {
	rt, adapter, run, node, attempt, entered, release := newBlockedPreparation(t)
	base := time.Now().UTC()
	var clock atomic.Int64
	clock.Store(base.UnixNano())
	rt.orchestrator.now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	rt.orchestrator.newProviderLeaseTicker = func(time.Duration) providerLeaseTicker { return newManualProviderLeaseTicker() }
	if err := rt.orchestrator.dispatchNodeAttempt(context.Background(), run, node, attempt, base.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	waitPreparationEntered(t, entered)
	clock.Store(base.Add(rt.orchestrator.providerExecutionLeaseDuration(node.NodeID, node.NodeClass) + time.Second).UnixNano())
	if err := os.RemoveAll(rt.orchestrator.yoloRoot); err != nil {
		t.Fatal(err)
	}
	release()
	awaitPreparationDrain(t, rt.orchestrator)
	a, found, err := rt.orchestrator.executionStore.GetAttempt(context.Background(), composeAttemptID(run.RunID, node.NodeID, attempt.Attempt))
	if err != nil || !found || a.State != "queued" || a.FailureCode != "" || a.LeaseOwner != "" || adapter.callsCount() != 0 {
		t.Fatalf("expired preparer terminalized with old start timestamp: %#v found=%t err=%v calls=%d", a, found, err, adapter.callsCount())
	}
}

func TestDispatchPreparationPrivateAdvisorsCopyConcurrentlyWithinRun(t *testing.T) {
	rt, adapter, run, node, attempt, _, _ := newBlockedPreparation(t)
	entered, released := make(chan struct{}, 2), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(released) }) }
	t.Cleanup(release)
	rt.orchestrator.gitRunner = &fakeGitCommandRunner{runFn: func(path string, _ ...string) (string, error) {
		if path == rt.orchestrator.yoloRoot {
			entered <- struct{}{}
			<-released
		}
		return "", fmt.Errorf("not a git checkout")
	}}
	mustExec(t, rt.orchestrator.db, `INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES ('wn_prepare_parallel', 'wf_prepare', 'parallel_advisor', 'council-member', 'plan_drafting', '2026-03-01T12:00:00Z')`)
	mustExec(t, rt.orchestrator.db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version) VALUES ('nr_prepare_parallel', 'run_prepare', 'parallel_advisor', 1, 1, 'ready', 'council-member', 'agent_prepare', 1)`)
	secondNode, secondAttempt := node, attempt
	secondNode.NodeID, secondAttempt.NodeID, secondAttempt.NodeRunID = "parallel_advisor", "parallel_advisor", "nr_prepare_parallel"
	unlock := rt.orchestrator.lockRunWorkspace(run.RunID)
	defer unlock()
	for _, candidate := range []struct {
		node    nodeDefinition
		attempt nodeAttempt
	}{{node, attempt}, {secondNode, secondAttempt}} {
		if err := rt.orchestrator.dispatchNodeAttempt(context.Background(), run, candidate.node, candidate.attempt, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		waitPreparationEntered(t, entered)
	}
	if adapter.callsCount() != 0 {
		t.Fatal("provider launched before private copying finished")
	}
	release()
	awaitPreparationDrain(t, rt.orchestrator)
	requests := adapter.requestsSnapshot()
	if len(requests) != 2 || requests[0].WorkspacePath == requests[1].WorkspacePath {
		t.Fatalf("private advisors did not execute independently: requests=%#v", requests)
	}
}

func TestDispatchPreparationFreedCapacityRotatesAcrossRuns(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db)
	seedReadyDispatchNodeRun(t, db, readyDispatchNodeRunSeed{RunID: "run_a_large_wave", DecreeID: "decree_a", WorkflowID: "wf_a", NodeRunID: "nr_a_0", NodeID: "advisor_0", NodeClass: workflow.NodeClassAdvisorySynthesis, RequiredRole: "chancellor", AgentID: "agent_a", RoleIDs: []string{"chancellor"}})
	for i := 1; i < 8; i++ {
		if _, err := db.Exec(`INSERT INTO workflow_nodes (workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at) VALUES (?, 'wf_a', ?, 'chancellor', 'advisory_synthesis', '2026-03-01T12:00:00Z')`, fmt.Sprintf("wn_a_%d", i), fmt.Sprintf("advisor_%d", i)); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version) VALUES (?, 'run_a_large_wave', ?, 1, 1, 'ready', 'chancellor', 'agent_a', 1)`, fmt.Sprintf("nr_a_%d", i), fmt.Sprintf("advisor_%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	seedReadyDispatchNodeRun(t, db, readyDispatchNodeRunSeed{RunID: "run_b_waiting", DecreeID: "decree_b", WorkflowID: "wf_b", NodeRunID: "nr_b", NodeID: "advisor_b", NodeClass: workflow.NodeClassAdvisorySynthesis, RequiredRole: "chancellor", AgentID: "agent_b"})
	adapter := newBlockingFirstExecutionAdapter()
	projectRoot := t.TempDir()
	rt, err := newMemoryRuntime(t, Config{DB: db, Adapter: adapter, Paths: DefaultBootstrapPaths(repoRoot(t)), YoloOutputRoot: projectRoot})
	if err != nil {
		t.Fatal(err)
	}
	loop := rt.orchestrator
	loop.preparationLimit = 1
	entered, copyPermits := make(chan struct{}, 2), make(chan struct{})
	t.Cleanup(func() { close(copyPermits); adapter.releaseFirstExecution(); awaitPreparationDrain(t, loop) })
	loop.gitRunner = &fakeGitCommandRunner{runFn: func(path string, _ ...string) (string, error) {
		if path == projectRoot {
			entered <- struct{}{}
			<-copyPermits
		}
		return "", fmt.Errorf("not a git checkout")
	}}
	if err := loop.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitPreparationEntered(t, entered)
	copyPermits <- struct{}{}
	testutil.Eventually(t, 3*time.Second, time.Millisecond, func() error {
		loop.execMu.Lock()
		preparing := len(loop.preparations)
		loop.execMu.Unlock()
		adapter.mu.Lock()
		calls := adapter.callCount
		adapter.mu.Unlock()
		if preparing != 0 || calls != 1 {
			return fmt.Errorf("first preparation has not transferred: preparing=%d provider_calls=%d", preparing, calls)
		}
		return nil
	})
	if err := loop.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitPreparationEntered(t, entered)
	loop.execMu.Lock()
	nextRun := ""
	for id := range loop.preparations {
		nextRun = loop.inflightRunIDs[id]
	}
	loop.execMu.Unlock()
	if nextRun != "run_b_waiting" {
		t.Fatalf("first run refilled freed capacity ahead of waiting run: next=%s; first run still has seven ready advisors", nextRun)
	}
}

func TestDispatchPreparationCancellationAndLeaseLossPreventLaunch(t *testing.T) {
	for _, disposition := range []string{"cancel", "run_cancelled", "replacement"} {
		t.Run(disposition, func(t *testing.T) {
			rt, adapter, run, node, attempt, entered, release := newBlockedPreparation(t)
			attemptID := composeAttemptID(run.RunID, node.NodeID, attempt.Attempt)
			if err := rt.orchestrator.dispatchNodeAttempt(context.Background(), run, node, attempt, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
				t.Fatal(err)
			}
			waitPreparationEntered(t, entered)
			if disposition == "cancel" {
				rt.orchestrator.cancelExecutionPreparation(attemptID)
			} else if disposition == "run_cancelled" {
				if _, err := rt.orchestrator.db.Exec(`UPDATE workflow_runs SET run_status = 'cancelled' WHERE run_id = ?`, run.RunID); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := rt.orchestrator.db.Exec(`UPDATE attempt_executions SET lease_owner = 'replacement', lease_version = lease_version + 1 WHERE attempt_id = ?`, attemptID); err != nil {
					t.Fatal(err)
				}
			}
			release()
			awaitPreparationDrain(t, rt.orchestrator)
			if adapter.callsCount() != 0 {
				t.Fatalf("%s launched provider", disposition)
			}
			var sessions, invocations int
			if err := rt.orchestrator.db.QueryRow(`SELECT COUNT(*) FROM session_leases`).Scan(&sessions); err != nil {
				t.Fatal(err)
			}
			if err := rt.orchestrator.db.QueryRow(`SELECT COUNT(*) FROM provider_invocations`).Scan(&invocations); err != nil {
				t.Fatal(err)
			}
			if sessions != 0 || invocations != 0 {
				t.Fatalf("%s created sessions=%d invocations=%d", disposition, sessions, invocations)
			}
			stored, _, err := rt.orchestrator.executionStore.GetAttempt(context.Background(), attemptID)
			if err != nil {
				t.Fatal(err)
			}
			wantOwner := ""
			if disposition == "replacement" {
				wantOwner = "replacement"
			}
			if stored.State != "queued" || stored.LeaseOwner != wantOwner {
				t.Fatalf("%s mutated authority: %#v", disposition, stored)
			}
		})
	}
}

func TestDispatchPreparationAdmissionIsBounded(t *testing.T) {
	rt, adapter, run, node, attempt, entered, release := newBlockedPreparation(t)
	rt.orchestrator.preparationLimit = 1
	if err := rt.orchestrator.dispatchNodeAttempt(context.Background(), run, node, attempt, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	waitPreparationEntered(t, entered)
	// A separate run remains ready without a claim or goroutine while capacity is
	// full. The next tick can admit it once the first preparation drains.
	seedReadyDispatchNodeRun(t, rt.orchestrator.db, readyDispatchNodeRunSeed{RunID: "run_prepare_next", DecreeID: "decree_prepare_next", WorkflowID: "wf_prepare_next", NodeRunID: "nr_prepare_next", NodeID: "plan_drafting", NodeClass: workflow.NodeClassPlanDrafting, RequiredRole: "council-member", AgentID: "agent_prepare_next"})
	nextRun := runSnapshot{RunID: "run_prepare_next", RunStatus: "running", PlanState: "approved", PublishMode: PublishModeDirect}
	nextAttempt := nodeAttempt{NodeRunID: "nr_prepare_next", NodeID: node.NodeID, Attempt: 1, Wave: 1, NodeStatus: "ready", RequiredRole: node.RequiredRole, AgentID: "agent_prepare_next"}
	if err := rt.orchestrator.dispatchNodeAttempt(context.Background(), nextRun, node, nextAttempt, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if _, found, err := rt.orchestrator.executionStore.GetAttempt(context.Background(), composeAttemptID(nextRun.RunID, node.NodeID, 1)); err != nil || found {
		t.Fatalf("full admission created queued work: found=%t err=%v", found, err)
	}
	release()
	awaitPreparationDrain(t, rt.orchestrator)
	if err := rt.orchestrator.dispatchNodeAttempt(context.Background(), nextRun, node, nextAttempt, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	awaitPreparationDrain(t, rt.orchestrator)
	if adapter.callsCount() != 2 {
		t.Fatalf("released capacity did not admit next run: calls=%d", adapter.callsCount())
	}
}

func newBlockedPreparation(t *testing.T) (*Runtime, *sequenceExecutionAdapter, runSnapshot, nodeDefinition, nodeAttempt, <-chan struct{}, func()) {
	t.Helper()
	db := openTestDB(t)
	applyMigration(t, db)
	seedReadyDispatchNodeRun(t, db, readyDispatchNodeRunSeed{RunID: "run_prepare", DecreeID: "decree_prepare", WorkflowID: "wf_prepare", NodeRunID: "nr_prepare", NodeID: "plan_drafting", NodeClass: workflow.NodeClassPlanDrafting, RequiredRole: "council-member", AgentID: "agent_prepare", RoleIDs: []string{"council-member", "chancellor"}, ExtraAgents: []readyDispatchAgentSeed{{AgentID: "agent_prepare_chancellor", RoleID: "chancellor"}}})
	adapter := &sequenceExecutionAdapter{outputs: []string{"prepared response", "second response"}}
	rt, err := newMemoryRuntime(t, Config{DB: db, Adapter: adapter, Paths: DefaultBootstrapPaths(repoRoot(t)), YoloOutputRoot: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	entered, released := make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(released) }) }
	t.Cleanup(release)
	rt.orchestrator.gitRunner = &fakeGitCommandRunner{runFn: func(string, ...string) (string, error) {
		once.Do(func() { close(entered); <-released })
		return "", fmt.Errorf("not a git checkout")
	}}
	return rt, adapter, runSnapshot{RunID: "run_prepare", RunStatus: "running", PlanState: "approved", PublishMode: PublishModeDirect}, nodeDefinition{NodeID: "plan_drafting", NodeClass: workflow.NodeClassPlanDrafting, RequiredRole: "council-member"}, nodeAttempt{NodeRunID: "nr_prepare", NodeID: "plan_drafting", Attempt: 1, Wave: 1, NodeStatus: "ready", RequiredRole: "council-member", AgentID: "agent_prepare"}, entered, release
}

func waitPreparationEntered(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("preparation did not reach workspace copy")
	}
}

func awaitPreparationDrain(t *testing.T, loop *orchestrationLoop) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := loop.waitForAsyncDrain(ctx); err != nil {
		t.Fatalf("wait for admitted preparation/execution: %v", err)
	}
}

// Provider-worker tests bypass preparation explicitly while retaining its drain ownership.
func (o *orchestrationLoop) startAsyncExecution(ctx context.Context, job asyncExecutionResult) bool {
	if !o.markExecutionInflight(job.attemptID, job.run.RunID) {
		return false
	}

	o.startOwnedAsyncExecution(ctx, job, nil)
	return true
}
