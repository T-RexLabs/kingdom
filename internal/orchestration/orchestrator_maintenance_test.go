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
			VALUES (%q, 'wfdef_1', %q, 'governance', 'work_intake', '2026-09-30T11:59:00Z')`, "wn_"+result.node.NodeID, result.node.NodeID))
		mustExec(t, loop.db, fmt.Sprintf(`INSERT INTO node_runs
			(node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at)
			VALUES (%q, 'run_1', %q, 1, 1, 'running', 'governance', 'agent_governance', 1, '2026-09-30T11:59:00Z')`, result.attempt.NodeRunID, result.node.NodeID))
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
	h.requests.respondWith(newScriptedExecutionAdapter(nil))
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

func TestCancellationStopRunsInItsOwnSlotAndNeverRestoresTheWorkspace(t *testing.T) {
	h := newTerminalHandoffHarness(t)
	invocation := h.addInvocation(t, 1)
	h.requests.issueDispatch(t, invocation.ProviderInvocationID)
	if err := h.store.QueueAttemptCancel(context.Background(), executionstore.QueueAttemptCancelRequest{
		CancelOutboxID: "cancel_stop_slot", AttemptID: invocation.AttemptID, RunID: invocation.RunID, NodeRunID: invocation.NodeRunID,
		SessionID: invocation.LogicalSessionID, ReasonCode: "run_terminalized", RequestedAt: terminalHandoffFixtureNow, CreatedAt: terminalHandoffFixtureNow,
	}); err != nil {
		t.Fatal(err)
	}
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
	adapter := &timeoutCancelObserverAdapter{}
	h.requests.respondWith(adapter)
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
	if got := adapter.cancelCalls(); got != 1 {
		t.Fatalf("provider stops=%d, want the one the stop slot sent", got)
	}
	// The request's resources are not closed until its worker settles it, so the
	// handoff waits for that proof instead of restoring the workspace.
	assertCancellationHandoff(t, h, invocation.AttemptID, "pending", "awaiting_quiescence")
	if content, err := os.ReadFile(path); err != nil || string(content) != "unfinished provider changes\n" {
		t.Fatalf("stop slot performed filesystem restoration: content=%q err=%v", content, err)
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
