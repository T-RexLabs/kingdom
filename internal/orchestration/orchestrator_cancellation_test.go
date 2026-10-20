package orchestration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/runtimefs"
	"kingdom/internal/state"
)

// staleCancellationFixture fences a stale attempt whose request had begun but
// admitted nothing, which Kingdom abandons so the attempt can be replaced. Its
// local worker is still unwinding.
func staleCancellationFixture(t *testing.T) (terminalHandoffHarness, executionstore.ProviderInvocation, *state.Reconciler) {
	t.Helper()
	h := newTerminalHandoffHarness(t)
	invocation := h.addInvocation(t, 1)
	h.requests.issueDispatch(t, invocation.ProviderInvocationID)
	h.loop.now = func() time.Time { return time.Date(2026, 3, 1, 12, 10, 1, 0, time.UTC) }
	mustExec(t, h.db, `UPDATE attempt_executions SET lease_expires_at='2026-03-01T12:02:00Z'`)
	reconciler := state.NewReconciler(h.db, state.NewSessionManager(h.db, state.NewSessionLifecycleMetrics()), staleRetryChainPlanner{}, h.loop.schemas)
	result, err := reconciler.Reconcile(context.Background(), state.ReconcileRequest{Now: "2026-03-01T12:10:00Z"})
	if err != nil || result.AttemptsReassigned != 1 {
		t.Fatalf("fence stale attempt: result=%+v err=%v", result, err)
	}
	h.loop.inflightExec = map[string]struct{}{invocation.AttemptID: {}}
	h.loop.inflightRunIDs = map[string]string{invocation.AttemptID: invocation.RunID}
	return h, invocation, reconciler
}

func assertCancellationHandoff(t *testing.T, h terminalHandoffHarness, attemptID, wantStatus, wantState string) {
	t.Helper()
	var status, handoff string
	if err := h.db.QueryRow(`SELECT status, handoff_state FROM attempt_cancel_outbox WHERE attempt_id=?`, attemptID).Scan(&status, &handoff); err != nil {
		t.Fatal(err)
	}
	if status != wantStatus || handoff != wantState {
		t.Fatalf("cancellation status=%q handoff=%q; want %q %q", status, handoff, wantStatus, wantState)
	}
}

func TestAbandonedRequestCancellationWaitsForWorkerAndResumes(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprintf("restart=%t", restart), func(t *testing.T) {
			t.Parallel()
			h, invocation, reconciler := staleCancellationFixture(t)
			ctx := context.Background()
			target, err := h.fsStore.GetExecutionTarget(ctx, invocation.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if err := h.fsStore.UpdateExecutionTargetState(ctx, invocation.RunID, "attempt_dirty", target.TrustedSnapshotID, invocation.AttemptID, "2026-03-01T12:01:00Z"); err != nil {
				t.Fatal(err)
			}
			workspaceFile := filepath.Join(target.ExecutionRoot, "README.md")
			if err := os.WriteFile(workspaceFile, []byte("provider worker still cleaning up\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			adapter := &timeoutCancelObserverAdapter{}
			h.requests.respondWith(adapter)
			for range 3 {
				if _, err := h.loop.drainAttemptCancelOutbox(ctx); err != nil {
					t.Fatal(err)
				}
			}
			assertCancellationHandoff(t, h, invocation.AttemptID, "pending", "awaiting_workspace_quiescence")
			if content, err := os.ReadFile(workspaceFile); err != nil || string(content) != "provider worker still cleaning up\n" {
				t.Fatalf("workspace restored before worker drained: content=%q err=%v", content, err)
			}
			var replacementState string
			if err := h.db.QueryRow(`SELECT node_status FROM node_runs WHERE handoff_source_attempt_id=?`, invocation.AttemptID).Scan(&replacementState); err != nil || replacementState != "pending" {
				t.Fatalf("worker released replacement early: state=%q err=%v", replacementState, err)
			}
			h.loop.clearExecutionInflight(invocation.AttemptID)
			if restart {
				runtime := &Runtime{orchestrator: h.loop, reconciler: reconciler}
				runtime.beginStartupRecovery()
				if err := runtime.recoverDurableExecutionState(ctx); err != nil {
					t.Fatal(err)
				}
				if err := runtime.finishStartupRecovery(ctx, false); err != nil {
					t.Fatal(err)
				}
				drainCtx, cancelDrain := context.WithTimeout(ctx, 5*time.Second)
				defer cancelDrain()
				defer runtime.stopMaintenance()
				if err := runtime.drainMaintenance(drainCtx); err != nil {
					t.Fatal(err)
				}
			} else if _, err := h.loop.drainAttemptCancelOutbox(ctx); err != nil {
				t.Fatal(err)
			}
			assertCancellationHandoff(t, h, invocation.AttemptID, "published", "released")
			if content, err := os.ReadFile(workspaceFile); err != nil || string(content) != "trusted baseline\n" {
				t.Fatalf("workspace did not restore after worker drained: content=%q err=%v", content, err)
			}
			if got := adapter.cancelCalls(); got != 0 {
				t.Fatalf("abandoned request needed %d provider stops", got)
			}
		})
	}
}

func TestResponseOnlyCancellationWaitsForWorker(t *testing.T) {
	t.Parallel()
	h, invocation, _ := staleCancellationFixture(t)
	ctx := context.Background()
	mustExec(t, h.db, `UPDATE workflow_nodes SET node_class='advisory_synthesis'`)
	if _, err := h.loop.drainAttemptCancelOutbox(ctx); err != nil {
		t.Fatal(err)
	}
	assertCancellationHandoff(t, h, invocation.AttemptID, "pending", "awaiting_workspace_quiescence")
	h.loop.clearExecutionInflight(invocation.AttemptID)
	if processed, err := h.loop.drainAttemptCancelOutbox(ctx); err != nil || processed != 1 {
		t.Fatalf("release response-only cancellation after worker drain: processed=%d err=%v", processed, err)
	}
	assertCancellationHandoff(t, h, invocation.AttemptID, "published", "released")
}

func TestCancellationPassAdvancesBeyondWaitingBatch(t *testing.T) {
	t.Parallel()
	testCancellationBatchFairness(t, 0)
}

func TestCancellationPassRevisitsWaitingRowsDespiteNewArrivals(t *testing.T) {
	t.Parallel()
	testCancellationBatchFairness(t, 65)
}

func testCancellationBatchFairness(t *testing.T, arrivalsDuringScan int) {
	t.Helper()
	h := newTerminalHandoffHarness(t)
	ctx := context.Background()
	// Independent runs with persistent restoration failures must not hide a
	// later cancellation whose workspace handoff can finish.
	queueCancellation := func(index int) {
		runID := fmt.Sprintf("run_cancel_batch_%03d", index)
		nodeRunID := fmt.Sprintf("nr_cancel_batch_%03d", index)
		attemptID := fmt.Sprintf("%s:implementation:1", runID)
		// These attempts already owned a snapshot, but their targets are
		// missing. A genuinely unprepared cancellation now safely needs no
		// restoration, so it cannot serve as persistent cleanup debt.
		baseSnapshotID := h.baselineSnapshotID
		if index == 64 {
			runID = "run_terminal_handoff"
			attemptID = runID + ":implementation:1"
		} else {
			mustExec(t, h.db, fmt.Sprintf(`INSERT INTO workflow_runs(run_id,work_order_id,workflow_definition_id,run_status,plan_state,row_version,created_at,updated_at) VALUES(%q,'work_order_terminal_handoff','wfdef_terminal_handoff','cancelled','approved',1,'2026-03-01T12:00:00Z','2026-03-01T12:00:00Z')`, runID))
		}
		mustExec(t, h.db, fmt.Sprintf(`INSERT INTO node_runs(node_run_id,run_id,node_id,attempt,wave,node_status,required_role,agent_id,row_version) VALUES(%q,%q,'implementation',1,1,'cancelled','builder','agent_terminal_handoff',1)`, nodeRunID, runID))
		mustExec(t, h.db, fmt.Sprintf(`INSERT INTO attempt_executions(attempt_id,run_id,node_run_id,state,execution_mode,completion_mode,base_snapshot_id,created_at,updated_at) VALUES(%q,%q,%q,'committed_cancelled','provider_backed','server_response',%q,'2026-03-01T12:00:00Z','2026-03-01T12:00:00Z')`, attemptID, runID, nodeRunID, baseSnapshotID))
		if err := h.store.QueueAttemptCancel(ctx, executionstore.QueueAttemptCancelRequest{
			CancelOutboxID: fmt.Sprintf("cancel_batch_%03d", index), AttemptID: attemptID, RunID: runID, NodeRunID: nodeRunID,
			ReasonCode: "run_terminalized", RequestedAt: "2026-03-01T12:00:01Z", CreatedAt: "2026-03-01T12:00:01Z",
		}); err != nil {
			t.Fatal(err)
		}
	}
	for index := 0; index < 65; index++ {
		queueCancellation(index)
	}
	if processed, err := h.loop.drainAttemptCancelOutbox(ctx); err != nil || processed != 0 {
		t.Fatalf("first bounded pass: processed=%d err=%v", processed, err)
	}
	// Arrivals beyond the cycle's original end must not postpone revisiting
	// earlier waiting rows. More than one batch reproduces indefinite drift.
	for index := 65; index < 65+arrivalsDuringScan; index++ {
		queueCancellation(index)
	}
	if processed, err := h.loop.drainAttemptCancelOutbox(ctx); err != nil || processed != 1 {
		t.Fatalf("later eligible cancellation starved: processed=%d err=%v", processed, err)
	}
	assertCancellationHandoff(t, h, "run_terminal_handoff:implementation:1", "published", "released")
	// Once the scan wraps, a previously waiting row can converge. Repeated
	// identical waits should not grow publish_attempts or rewrite bookkeeping.
	if _, err := h.fsStore.EnsureExecutionTarget(ctx, runtimefs.EnsureExecutionTargetRequest{
		RunID: "run_cancel_batch_000", TargetMode: PublishModeDirect, SourceRoot: h.projectRoot,
		ExecutionRoot: h.fsStore.RunTargetRoot("run_cancel_batch_000"), PublishRoot: h.projectRoot,
		SourceSnapshotID: h.baselineSnapshotID, TrustedSnapshotID: h.baselineSnapshotID, PublishedSnapshotID: h.baselineSnapshotID,
		State: "trusted", CreatedAt: terminalHandoffFixtureNow, UpdatedAt: terminalHandoffFixtureNow,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.db.Exec(`UPDATE attempt_executions SET base_snapshot_id=? WHERE run_id='run_cancel_batch_000'`, h.baselineSnapshotID); err != nil {
		t.Fatal(err)
	}
	if processed, err := h.loop.drainAttemptCancelOutbox(ctx); err != nil || processed != 1 {
		t.Fatalf("scan did not wrap to previously waiting cancellation: processed=%d err=%v", processed, err)
	}
	var writeCount int
	if err := h.db.QueryRow(`SELECT publish_attempts FROM attempt_cancel_outbox WHERE cancel_outbox_id='cancel_batch_001'`).Scan(&writeCount); err != nil || writeCount != 1 {
		t.Fatalf("unchanged workspace wait rewrote bookkeeping: writes=%d err=%v", writeCount, err)
	}
}

func TestCancellationWaitingStopResumesAfterWorkerDisposal(t *testing.T) {
	for _, phase := range []string{"awaiting_quiescence", "cancellation_dispatched", "stop_retry_dispatched"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			h, invocation, _ := staleCancellationFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			mustExec(t, h.db, fmt.Sprintf(`UPDATE attempt_cancel_outbox SET handoff_state=%q`, phase))
			// Disposal does not release a handoff while the original worker unwinds.
			if admitted, err := h.loop.scheduleAttemptCancellations(ctx); err != nil || admitted != 0 {
				t.Fatalf("worker still active: admitted=%d err=%v", admitted, err)
			}
			assertCancellationHandoff(t, h, invocation.AttemptID, "pending", phase)
			h.loop.clearExecutionInflight(invocation.AttemptID)
			if admitted, err := h.loop.scheduleAttemptCancellations(ctx); err != nil || admitted != 1 {
				t.Fatalf("disposed request cancellation must resume automatically: admitted=%d err=%v", admitted, err)
			}
			if err := h.loop.waitForMaintenance(ctx); err != nil {
				t.Fatal(err)
			}
			assertCancellationHandoff(t, h, invocation.AttemptID, "published", "released")

		})
	}
}

func TestCancellationWaitingStopDoesNotRedispatchStop(t *testing.T) {
	for _, phase := range []string{"awaiting_quiescence", "cancellation_dispatched", "stop_retry_dispatched"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			h := newTerminalHandoffHarness(t)
			invocation := h.addInvocation(t, 1)
			h.requests.issueDispatch(t, invocation.ProviderInvocationID)
			ctx := context.Background()
			if err := h.store.QueueAttemptCancel(ctx, executionstore.QueueAttemptCancelRequest{
				CancelOutboxID: "cancel_awaiting", AttemptID: invocation.AttemptID, RunID: invocation.RunID, NodeRunID: invocation.NodeRunID,
				SessionID: invocation.LogicalSessionID, ReasonCode: "run_terminalized", RequestedAt: terminalHandoffFixtureNow, CreatedAt: terminalHandoffFixtureNow,
			}); err != nil {
				t.Fatal(err)
			}
			mustExec(t, h.db, fmt.Sprintf(`UPDATE attempt_cancel_outbox SET handoff_state=%q`, phase))
			adapter := &timeoutCancelObserverAdapter{}
			h.requests.respondWith(adapter)
			for range 3 {
				if admitted, err := h.loop.scheduleAttemptCancellations(ctx); err != nil || admitted != 0 {
					t.Fatalf("unsettled request must only be observed: admitted=%d err=%v", admitted, err)
				}
			}
			assertCancellationHandoff(t, h, invocation.AttemptID, "pending", phase)
			if got := adapter.cancelCalls(); got != 0 {
				t.Fatalf("waiting handoff redispatched %d stops", got)
			}

		})
	}
}
