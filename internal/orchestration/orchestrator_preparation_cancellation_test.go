package orchestration

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"kingdom/internal/core/workflow"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/runtimefs"
	"kingdom/internal/state"
)

func TestRunTerminalizationCancelsBlockedPreparationThroughDurableHandoff(t *testing.T) {
	for _, terminal := range []string{"cancelled", "failed"} {
		t.Run(terminal, func(t *testing.T) {
			rt, adapter, run, node, attempt, entered, release := newBlockedPreparation(t)
			loop := rt.orchestrator
			ctx := context.Background()
			if err := loop.dispatchNodeAttempt(ctx, run, node, attempt, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
				t.Fatal(err)
			}
			waitPreparationEntered(t, entered)
			attemptID := composeAttemptID(run.RunID, node.NodeID, attempt.Attempt)
			loop.execMu.Lock()
			preparationCtx := loop.preparations[attemptID].ctx
			loop.execMu.Unlock()
			terminalizePreparationRun(t, loop, run.RunID, terminal)
			var pending int
			if err := loop.db.QueryRow(`SELECT COUNT(*) FROM attempt_cancel_outbox WHERE attempt_id=? AND status='pending'`, attemptID).Scan(&pending); err != nil {
				t.Fatal(err)
			}
			if pending != 1 {
				t.Fatal("run terminalization discarded the admitted preparation without a durable cancellation handoff")
			}
			if _, err := loop.scheduleAttemptCancellations(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case <-preparationCtx.Done():
			case <-time.After(time.Second):
				t.Fatal("durable cancellation did not signal the blocked preparer")
			}
			if err := loop.db.QueryRow(`SELECT COUNT(*) FROM attempt_cancel_outbox WHERE attempt_id=? AND status='pending'`, attemptID).Scan(&pending); err != nil || pending != 1 {
				t.Fatalf("handoff released before preparation drained: pending=%d err=%v", pending, err)
			}
			release()
			awaitPreparationDrain(t, loop)
			if _, err := loop.scheduleAttemptCancellations(ctx); err != nil {
				t.Fatal(err)
			}
			waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if err := loop.waitForMaintenance(waitCtx); err != nil {
				t.Fatal(err)
			}
			var status, handoff string
			if err := loop.db.QueryRow(`SELECT status,handoff_state FROM attempt_cancel_outbox WHERE attempt_id=?`, attemptID).Scan(&status, &handoff); err != nil || status != "published" || handoff != "released" {
				t.Fatalf("drained preparation cancellation=%s/%s err=%v", status, handoff, err)
			}
			var sessions, invocations, prepared int
			if err := loop.db.QueryRow(`SELECT (SELECT COUNT(*) FROM session_leases), (SELECT COUNT(*) FROM provider_invocations), (SELECT COUNT(*) FROM events WHERE attempt_id=? AND reason_code='workspace_prepared')`, attemptID).Scan(&sessions, &invocations, &prepared); err != nil {
				t.Fatal(err)
			}
			if adapter.callsCount() != 0 || sessions != 0 || invocations != 0 || prepared != 0 {
				t.Fatalf("cancelled preparation advanced: provider=%d sessions=%d invocations=%d prepared=%d", adapter.callsCount(), sessions, invocations, prepared)
			}
		})
	}
}

func terminalizePreparationRun(t *testing.T, loop *orchestrationLoop, runID, terminal string) {
	t.Helper()
	if err := loop.transitionService().TerminalizeRunAndCancelLiveNodes(context.Background(), state.RunTerminalizationRequest{
		Transition: state.TransitionRequest{
			TransitionID: "tr_cancel_preparation", EventID: "evt_cancel_preparation", RunID: runID,
			Entity: state.EntityTypeRun, FromState: "running", ToState: terminal,
			ActorID: "test_operator", RoleID: "king", AuthoritySource: "api.cancel_run",
			ReasonCode: "run_cancelled_by_user", ReasonText: "cancel preparation",
			CorrelationID: "corr_cancel_preparation", CausationID: "caus_cancel_preparation",
			OccurredAt: loop.now().UTC().Format(time.RFC3339Nano), AttemptID: "run_cancel_preparation",
			SchemaVersion: "v1", EventType: "run.state_changed", EventSeq: 1, PayloadJSON: `{}`,
		},
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCancellationBeforeCanonicalPreparationNeedsNoWorkspaceRestore(t *testing.T) {
	for _, scenario := range []string{"no_target", "trusted_target", "dirty_target", "launched", "invocation", "missing_attempt"} {
		t.Run(scenario, func(t *testing.T) {
			db := openTestDB(t)
			applyMigration(t, db)
			seedReadyDispatchNodeRun(t, db, readyDispatchNodeRunSeed{RunID: "run_cancel_prep", DecreeID: "decree_cancel_prep", WorkflowID: "wf_cancel_prep", NodeRunID: "nr_cancel_prep", NodeID: "builder", NodeClass: workflow.NodeClassDelegatedExecution, RequiredRole: "builder-berserker", AgentID: "agent_cancel_prep", RoleIDs: []string{"builder-berserker"}})
			root := t.TempDir()
			rt, err := newMemoryRuntime(t, Config{DB: db, Adapter: &sequenceExecutionAdapter{}, Paths: DefaultBootstrapPaths(repoRoot(t)), YoloOutputRoot: root})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			now := time.Now().UTC().Format(time.RFC3339Nano)
			const attemptID = "run_cancel_prep:builder:1"
			if scenario != "missing_attempt" {
				if err := rt.orchestrator.executionStore.EnqueueAttempt(ctx, executionstore.EnqueueAttemptRequest{AttemptID: attemptID, RunID: "run_cancel_prep", NodeRunID: "nr_cancel_prep", ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeServerResponse, CreatedAt: now}); err != nil {
					t.Fatal(err)
				}
				if scenario == "invocation" {
					if _, err := rt.orchestrator.executionStore.CreateProviderInvocation(ctx, executionstore.CreateProviderInvocationRequest{ProviderInvocationID: "pinv_cancel_prep", AttemptID: attemptID, RunID: "run_cancel_prep", NodeRunID: "nr_cancel_prep", CompletionMode: executionstore.ProviderCompletionModeServerResponse, CreatedAt: now}); err != nil {
						t.Fatal(err)
					}
				}
				mustExec(t, db, `UPDATE attempt_executions SET state = 'committed_cancelled' WHERE attempt_id = 'run_cancel_prep:builder:1'`)
				if scenario == "launched" {
					mustExec(t, db, `UPDATE attempt_executions SET launch_started_at = '`+now+`' WHERE attempt_id = 'run_cancel_prep:builder:1'`)
				}
			}
			if scenario == "trusted_target" || scenario == "dirty_target" {
				if err := os.WriteFile(filepath.Join(root, "untouched.txt"), []byte("trusted bytes"), 0o644); err != nil {
					t.Fatal(err)
				}
				snapshot, err := rt.fsStore.CaptureSnapshot(ctx, runtimefs.CaptureSnapshotRequest{SnapshotID: "snap_cancel_prep", RunID: "run_cancel_prep", SnapshotKind: "baseline", RootPath: root, CreatedAt: now})
				if err != nil {
					t.Fatal(err)
				}
				state := "trusted"
				if scenario == "dirty_target" {
					state = "attempt_dirty"
				}
				if _, err := rt.fsStore.EnsureExecutionTarget(ctx, runtimefs.EnsureExecutionTargetRequest{RunID: "run_cancel_prep", TargetMode: PublishModeDirect, SourceRoot: root, ExecutionRoot: root, PublishRoot: root, SourceSnapshotID: snapshot.SnapshotID, TrustedSnapshotID: snapshot.SnapshotID, PublishedSnapshotID: snapshot.SnapshotID, State: state, CreatedAt: now, UpdatedAt: now}); err != nil {
					t.Fatal(err)
				}
			}
			err = rt.orchestrator.restoreTrustedWorkspaceAfterCancellation(ctx, executionstore.AttemptCancelRecord{AttemptID: attemptID, RunID: "run_cancel_prep", NodeRunID: "nr_cancel_prep", NodeID: "builder", NodeClass: workflow.NodeClassDelegatedExecution}, now)
			if scenario == "no_target" || scenario == "trusted_target" {
				if err != nil {
					t.Fatalf("unprepared cancellation cannot finish: %v", err)
				}
			} else if err == nil {
				t.Fatal("missing workspace ownership evidence was accepted")
			}
			if scenario == "trusted_target" {
				content, err := os.ReadFile(filepath.Join(root, "untouched.txt"))
				if err != nil || string(content) != "trusted bytes" {
					t.Fatalf("unprepared cancellation mutated trusted target: %q err=%v", content, err)
				}
			}
		})
	}
}
