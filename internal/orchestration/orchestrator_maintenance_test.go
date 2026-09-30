package orchestration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	executionstore "kingdom/internal/execution/store"
)

func TestMaintenanceLostFileScanCompletesAcrossCapacityBoundaries(t *testing.T) {
	h := newTerminalHandoffHarness(t)
	h.loop.maintenance.limit = 1
	const attempts = providerTerminalHandoffPassLimit + 1
	invocations := make([]executionstore.ProviderInvocation, 0, attempts)
	for i := 1; i <= attempts; i++ {
		invocation := h.addInvocation(t, i)
		h.runningHandoffJob(t, invocation, i) // Durable quiescence, deliberately no handoff file.
		invocations = append(invocations, invocation)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	runtime := &Runtime{orchestrator: h.loop}
	runtime.setStartupRecoveryPending(false, true, true)
	for pass := 0; pass < attempts*2 && runtime.pendingStartupRecoveryStage() == startupRecoveryStageHandoffs; pass++ {
		// Make admission deterministic: a replay cannot finish and free its sole
		// slot until the scan has returned. No host-speed assumptions are needed.
		unlocks := make([]func(), 0, len(invocations))
		for _, invocation := range invocations {
			unlocks = append(unlocks, h.loop.lockProviderTerminalHandoffFile(invocation.ProviderInvocationID))
		}
		_, err := runtime.replayPendingStartupRecovery(ctx)
		for _, unlock := range unlocks {
			unlock()
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := h.loop.waitForMaintenance(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if got := runtime.pendingStartupRecoveryStage(); got != startupRecoveryStageLostHandoffs {
		t.Fatalf("completed missing-file replay must release lost-handoff recovery after %d candidates; stage=%v", attempts, got)
	}
	for {
		visited, err := runtime.replayPendingLostProviderTerminalHandoffsWithLimit(ctx, 16)
		if err != nil {
			t.Fatal(err)
		}
		if visited < 16 {
			break
		}
	}
	var failed int
	if err := h.db.QueryRow(`SELECT COUNT(*) FROM attempt_executions
		WHERE failure_code = 'provider_terminal_handoff_lost' AND state = 'committed_failure'`).Scan(&failed); err != nil {
		t.Fatal(err)
	}
	if failed != attempts {
		t.Fatalf("genuinely lost handoffs must settle after file authority scan: failed=%d want=%d", failed, attempts)
	}
}

func TestMaintenanceOutboxScanAdvancesPastBusyBatch(t *testing.T) {
	loop, first := completionTimestampFixture(t)
	loop.now = func() time.Time { return time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC) }
	// The first 64 rows belong to attempts still finishing their local handoff.
	if !loop.markExecutionInflight(first.attemptID, first.run.RunID) {
		t.Fatal("reserve first worker")
	}
	defer loop.clearExecutionInflight(first.attemptID)
	var last asyncExecutionResult
	for i := 2; i <= executionOutboxPassLimit+1; i++ {
		result := first
		result.node.NodeID = fmt.Sprintf("intake_%03d", i)
		result.attempt.NodeID = result.node.NodeID
		result.attempt.NodeRunID = fmt.Sprintf("nr_%03d", i)
		result.attemptID = fmt.Sprintf("run_1:%s:1", result.node.NodeID)
		result.occurredAt = time.Date(2026, 9, 30, 11, 59, i, 0, time.UTC).Format(time.RFC3339Nano)
		mustExec(t, loop.db, fmt.Sprintf(`INSERT INTO workflow_nodes
			(workflow_node_id, workflow_definition_id, node_id, required_role, node_class, created_at)
			VALUES (%q, 'wfdef_1', %q, 'king', 'decree_intake', '2026-09-30T11:59:00Z')`, "wn_"+result.node.NodeID, result.node.NodeID))
		mustExec(t, loop.db, fmt.Sprintf(`INSERT INTO node_runs
			(node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at)
			VALUES (%q, 'run_1', %q, 1, 1, 'running', 'king', 'agent_king', 1, '2026-09-30T11:59:00Z')`, result.attempt.NodeRunID, result.node.NodeID))
		seedCompletionTimestampOutbox(t, loop.db, result)
		if i <= executionOutboxPassLimit {
			if !loop.markExecutionInflight(result.attemptID, result.run.RunID) {
				t.Fatal("reserve worker")
			}
			defer loop.clearExecutionInflight(result.attemptID)
		}
		last = result
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if admitted, err := loop.scheduleExecutionOutbox(ctx); err != nil || admitted != 0 {
		t.Fatalf("busy first pass: admitted=%d err=%v", admitted, err)
	}
	if admitted, err := loop.scheduleExecutionOutbox(ctx); err != nil || admitted != 1 {
		t.Fatalf("eligible row after busy batch: admitted=%d err=%v", admitted, err)
	}
	if err := loop.waitForMaintenance(ctx); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := loop.db.QueryRow(`SELECT status FROM attempt_outbox WHERE attempt_id = ?`, last.attemptID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "published" {
		t.Fatalf("row beyond busy batch was not projected: %s", status)
	}
}

func TestProviderWorkerEndsAtDurableHandoffBeforeSnapshotFinalization(t *testing.T) {
	h := newTerminalHandoffHarness(t)
	invocation := h.addInvocation(t, 1)
	job := h.runningHandoffJob(t, invocation, 1)
	target, err := h.fsStore.GetExecutionTarget(context.Background(), invocation.RunID)
	if err != nil {
		t.Fatal(err)
	}
	job.workspacePath = target.ExecutionRoot
	job.baseSnapshotID = h.baselineSnapshotID
	job.sessionID = invocation.LogicalSessionID
	job.expectedLeaseExpiresAt = "2099-03-01T12:10:00Z"
	h.loop.adapter = newScriptedExecutionAdapter(nil)
	h.loop.asyncDrain = newExecutionDrainTracker()
	h.loop.now = func() time.Time { return time.Date(2026, 3, 1, 12, 0, 2, 0, time.UTC) }
	unlock := h.loop.lockRunWorkspace(invocation.RunID)
	locked := true
	defer func() {
		if locked {
			unlock()
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if !h.loop.startAsyncExecution(ctx, job) {
		t.Fatal("provider worker was not admitted")
	}
	if err := h.loop.waitForAsyncDrain(ctx); err != nil {
		t.Fatalf("provider worker waited for filesystem finalization: %v", err)
	}
	var snapshot string
	if err := h.db.QueryRow(`SELECT COALESCE(result_snapshot_id, '') FROM attempt_executions WHERE attempt_id=?`, invocation.AttemptID).Scan(&snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot != "" {
		t.Fatalf("provider worker captured a result snapshot: %s", snapshot)
	}
	handoffs, err := h.store.ListPendingProviderTerminalHandoffs(ctx, "", 1)
	if err != nil || len(handoffs) != 1 {
		t.Fatalf("worker did not leave durable finalization: handoffs=%d err=%v", len(handoffs), err)
	}
	if got := countTestRows(t, h.db, `SELECT COUNT(*) FROM provider_completion_settlements`); got != 0 {
		t.Fatal("provider worker settled synchronously")
	}
	if admitted, err := h.loop.scheduleProviderTerminalFinalization(ctx); err != nil || admitted != 0 {
		t.Fatalf("busy canonical workspace: admitted=%d err=%v", admitted, err)
	}
	unlock()
	locked = false
	if admitted, err := h.loop.scheduleProviderTerminalFinalization(ctx); err != nil || admitted != 1 {
		t.Fatalf("admit shared finalizer: admitted=%d err=%v", admitted, err)
	}
	if err := h.loop.waitForMaintenance(ctx); err != nil {
		t.Fatal(err)
	}
	settlement, found, err := h.store.GetProviderCompletionSettlementByInvocation(ctx, invocation.ProviderInvocationID)
	if err != nil || !found || settlement.Outcome != executionstore.ProviderCompletionOutcomeSuccess {
		t.Fatalf("shared finalizer settlement: found=%t outcome=%s err=%v", found, settlement.Outcome, err)
	}
	if err := h.db.QueryRow(`SELECT COALESCE(result_snapshot_id, '') FROM attempt_executions WHERE attempt_id=?`, invocation.AttemptID).Scan(&snapshot); err != nil || snapshot == "" {
		t.Fatalf("finalizer did not capture canonical result: snapshot=%q err=%v", snapshot, err)
	}
}

func TestCancellationStopDoesNotRestoreUntilOrdinaryMaintenanceAdmission(t *testing.T) {
	h, invocation, _ := staleCancellationFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	target, err := h.fsStore.GetExecutionTarget(ctx, invocation.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.fsStore.UpdateExecutionTargetState(ctx, invocation.RunID, "attempt_dirty", target.TrustedSnapshotID, invocation.AttemptID, "2026-03-01T12:01:00Z"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(target.ExecutionRoot, "README.md")
	if err := os.WriteFile(path, []byte("unfinished provider changes\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		h.loop.clearExecutionInflight(invocation.AttemptID)
		_ = h.loop.waitForMaintenance(ctx)
	}()
	for i := 0; i < defaultMaintenanceLimit; i++ {
		if !h.loop.tryScheduleMaintenance(ctx, maintenanceRequest{kind: "finalize", attemptID: fmt.Sprintf("other-%d", i)}, func(ctx context.Context) error {
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}) {
			t.Fatal("could not saturate filesystem capacity")
		}
	}
	h.loop.adapter = &timeoutCancelObserverAdapter{onCancel: func() error { return h.loop.ingestPendingProviderProcessGuards(ctx) }}
	if admitted, err := h.loop.scheduleAttemptCancellations(ctx); err != nil || admitted != 1 {
		t.Fatalf("bounded stop blocked by filesystem workers: admitted=%d err=%v", admitted, err)
	}
	for {
		h.loop.execMu.Lock()
		active := false
		for _, kind := range h.loop.maintenance.active {
			if kind == "cancel_stop" {
				active = true
			}
		}
		changed := h.loop.maintenance.changed
		h.loop.execMu.Unlock()
		if !active {
			break
		}
		select {
		case <-changed:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	assertCancellationHandoff(t, h, invocation.AttemptID, "pending", "awaiting_workspace_quiescence")
	if content, err := os.ReadFile(path); err != nil || string(content) != "unfinished provider changes\n" {
		t.Fatalf("stop slot performed filesystem restoration: content=%q err=%v", content, err)
	}
	h.loop.clearExecutionInflight(invocation.AttemptID)
	if admitted, err := h.loop.scheduleAttemptCancellations(ctx); err != nil || admitted != 0 {
		t.Fatalf("restore bypassed ordinary worker limit: admitted=%d err=%v", admitted, err)
	}
	close(release)
	if err := h.loop.waitForMaintenance(ctx); err != nil {
		t.Fatal(err)
	}
	if admitted, err := h.loop.scheduleAttemptCancellations(ctx); err != nil || admitted != 1 {
		t.Fatalf("admit cancellation finish: admitted=%d err=%v", admitted, err)
	}
	if err := h.loop.waitForMaintenance(ctx); err != nil {
		t.Fatal(err)
	}
	assertCancellationHandoff(t, h, invocation.AttemptID, "published", "released")
	if content, err := os.ReadFile(path); err != nil || string(content) != "trusted baseline\n" {
		t.Fatalf("cancellation finalizer did not restore: content=%q err=%v", content, err)
	}
}

func TestMaintenanceDrainReportsMalformedDurableHandoff(t *testing.T) {
	h := newTerminalHandoffHarness(t)
	invocation := h.addInvocation(t, 1)
	h.persistHandoffJob(t, invocation, 1)
	mustExec(t, h.db, `UPDATE provider_terminal_handoffs SET payload_json='{}'`)
	runtime := &Runtime{orchestrator: h.loop, completionEvery: time.Millisecond}
	defer runtime.stopMaintenance()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := runtime.drainMaintenance(ctx)
	if err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("malformed handoff was retried until deadline instead of reporting failure: %v", err)
	}
	if err := h.loop.waitForMaintenance(ctx); err != nil {
		t.Fatal(err)
	}
}
