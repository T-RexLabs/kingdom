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

func staleCancellationFixture(t *testing.T) (terminalHandoffHarness, executionstore.ProviderInvocation, *state.Reconciler) {
	t.Helper()
	h := newTerminalHandoffHarness(t)
	h.addPendingGuardEvidence(t, 1)
	invocation, found, err := h.store.GetProviderInvocation(context.Background(), "pinv_terminal_handoff_001")
	if err != nil || !found {
		t.Fatalf("load stale invocation: found=%t err=%v", found, err)
	}
	h.loop.processGuardRuntimeRoot = h.runtimeDir
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

func TestQuiescentCancellationWaitsForWorkerAndResumes(t *testing.T) {
	for _, proofTiming := range []string{"before_stop", "during_stop", "after_stop"} {
		for _, restart := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/restart=%t", proofTiming, restart), func(t *testing.T) {
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
				h.loop.adapter = adapter
				if proofTiming == "before_stop" {
					if err := h.loop.ingestPendingProviderProcessGuards(ctx); err != nil {
						t.Fatal(err)
					}
				}
				if proofTiming == "during_stop" {
					// Execute records guardian proof before its done channel releases
					// Cancel, while the outer worker can still be unwinding.
					adapter.onCancel = func() error { return h.loop.ingestPendingProviderProcessGuards(ctx) }
				}
				if _, err := h.loop.drainAttemptCancelOutbox(ctx); err != nil {
					t.Fatal(err)
				}
				if proofTiming == "after_stop" {
					if err := h.loop.ingestPendingProviderProcessGuards(ctx); err != nil {
						t.Fatal(err)
					}
				}
				for range 2 {
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
				wantCalls := 1
				if proofTiming == "before_stop" {
					wantCalls = 0
				}
				if got := adapter.cancelCalls(); got != wantCalls {
					t.Fatalf("stop calls=%d; want %d", got, wantCalls)
				}
			})
		}
	}
}

func TestCancellationReturnPreservesConcurrentStopRetryAuthorization(t *testing.T) {
	t.Parallel()
	h, invocation, _ := staleCancellationFixture(t)
	ctx := context.Background()
	adapter := &timeoutCancelObserverAdapter{onCancel: func() error {
		return h.loop.transitionService().RequestAttemptStopRetry(ctx, state.AttemptStopRetryRequest{
			RunID: invocation.RunID, AttemptID: invocation.AttemptID, ActorID: "local-operator", RoleID: "allfather",
			OccurredAt: "2026-03-01T12:10:02Z", GenerateID: func(s string) string { return s + "_concurrent_stop_retry" },
		})
	}}
	h.loop.adapter = adapter
	if _, err := h.loop.drainAttemptCancelOutbox(ctx); err != nil {
		t.Fatal(err)
	}
	assertCancellationHandoff(t, h, invocation.AttemptID, "pending", "stop_retry_requested")
	adapter.onCancel = nil
	for range 3 {
		if _, err := h.loop.drainAttemptCancelOutbox(ctx); err != nil {
			t.Fatal(err)
		}
	}
	assertCancellationHandoff(t, h, invocation.AttemptID, "pending", "awaiting_quiescence")
	if got := adapter.cancelCalls(); got != 2 {
		t.Fatalf("stop calls=%d; want original request and one authorized retry", got)
	}
}

func TestCancellationRecoveryResumesInterruptedDispatchFromGuardianProof(t *testing.T) {
	for _, retry := range []bool{false, true} {
		t.Run(fmt.Sprintf("retry=%t", retry), func(t *testing.T) {
			t.Parallel()
			h, invocation, reconciler := staleCancellationFixture(t)
			ctx := context.Background()
			var cancelOutboxID string
			if err := h.db.QueryRow(`SELECT cancel_outbox_id FROM attempt_cancel_outbox WHERE attempt_id=?`, invocation.AttemptID).Scan(&cancelOutboxID); err != nil {
				t.Fatal(err)
			}
			if phase, err := h.loop.transitionService().ClaimAttemptCancelHandoffDispatch(ctx, cancelOutboxID, "2026-03-01T12:10:02Z"); err != nil || phase != "cancellation_dispatched" {
				t.Fatalf("claim interrupted initial dispatch: phase=%q err=%v", phase, err)
			}
			if retry {
				if err := h.loop.transitionService().RequestAttemptStopRetry(ctx, state.AttemptStopRetryRequest{
					RunID: invocation.RunID, AttemptID: invocation.AttemptID, ActorID: "local-operator", RoleID: "allfather", OccurredAt: "2026-03-01T12:10:03Z",
				}); err != nil {
					t.Fatal(err)
				}
				if phase, err := h.loop.transitionService().ClaimAttemptCancelHandoffDispatch(ctx, cancelOutboxID, "2026-03-01T12:10:04Z"); err != nil || phase != "stop_retry_dispatched" {
					t.Fatalf("claim interrupted retry dispatch: phase=%q err=%v", phase, err)
				}
			}
			adapter := &timeoutCancelObserverAdapter{}
			h.loop.adapter = adapter
			h.loop.clearExecutionInflight(invocation.AttemptID)
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
			assertCancellationHandoff(t, h, invocation.AttemptID, "published", "released")
			if calls := adapter.cancelCalls(); calls != 0 {
				t.Fatalf("startup replayed interrupted external stop: calls=%d", calls)
			}
		})
	}
}

func TestResponseOnlyCancellationWaitsForWorker(t *testing.T) {
	t.Parallel()
	h, invocation, _ := staleCancellationFixture(t)
	ctx := context.Background()
	mustExec(t, h.db, `UPDATE workflow_nodes SET node_class='advisory_synthesis'`)
	if err := h.loop.ingestPendingProviderProcessGuards(ctx); err != nil {
		t.Fatal(err)
	}
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
		attemptID := fmt.Sprintf("%s:delegated_execution_berserker:1", runID)
		// These attempts already owned a snapshot, but their targets are
		// missing. A genuinely unprepared cancellation now safely needs no
		// restoration, so it cannot serve as persistent cleanup debt.
		baseSnapshotID := h.baselineSnapshotID
		if index == 64 {
			runID = "run_terminal_handoff"
			attemptID = runID + ":delegated_execution_berserker:1"
		} else {
			mustExec(t, h.db, fmt.Sprintf(`INSERT INTO workflow_runs(run_id,decree_id,workflow_definition_id,run_status,plan_state,row_version,created_at,updated_at) VALUES(%q,'decree_terminal_handoff','wfdef_terminal_handoff','cancelled','approved',1,'2026-03-01T12:00:00Z','2026-03-01T12:00:00Z')`, runID))
		}
		mustExec(t, h.db, fmt.Sprintf(`INSERT INTO node_runs(node_run_id,run_id,node_id,attempt,wave,node_status,required_role,agent_id,row_version) VALUES(%q,%q,'delegated_execution_berserker',1,1,'cancelled','builder-berserker','agent_terminal_handoff',1)`, nodeRunID, runID))
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
	assertCancellationHandoff(t, h, "run_terminal_handoff:delegated_execution_berserker:1", "published", "released")
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
