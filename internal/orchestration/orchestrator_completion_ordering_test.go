package orchestration

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/state"
)

func privateCompletionOrderingFixture(t *testing.T) (terminalHandoffHarness, asyncExecutionResult) {
	t.Helper()
	h := newTerminalHandoffHarness(t)
	invocation := h.addInvocation(t, 1)
	job := h.runningHandoffJob(t, invocation, 1)
	mustExec(t, h.db, `INSERT INTO roles(role_id,display_name,can_spawn,created_at) VALUES ('qa-validator','QA',0,'2026-03-01T12:00:00Z')`)
	mustExec(t, h.db, `UPDATE workflow_nodes SET required_role='qa-validator', node_class='validation_gates'`)
	mustExec(t, h.db, `UPDATE node_runs SET required_role='qa-validator'`)
	mustExec(t, h.db, `UPDATE agents SET role_id='qa-validator'`)
	job.node.RequiredRole, job.node.NodeClass = "qa-validator", "validation_gates"
	job.attempt.RequiredRole, job.delegator = job.node.RequiredRole, job.attempt.AgentID
	h.loop.events = state.NewAttemptEventIngestor(h.db, h.loop.schemas)
	h.loop.mailbox = state.NewMailboxStore(h.db, h.loop.schemas)
	path, err := h.fsStore.PrepareResponseOnlyWorkspace(context.Background(), invocation.RunID, invocation.AttemptID, terminalHandoffFixtureNow)
	if err != nil {
		t.Fatal(err)
	}
	job.workspacePath = path
	if err := os.WriteFile(filepath.Join(path, "private.txt"), []byte("inspection input"), 0o644); err != nil {
		t.Fatal(err)
	}
	h.configureSuccessfulHandoff(t, &job, "VERDICT: PASS\nVerified pinned implementation.")
	h.loop.emitCompletionLifecycleObservation(context.Background(), job, "before_settlement", terminalHandoffFixtureNow, nil)
	persistProviderTerminalHandoffForTest(t, h.loop, &job)
	return h, job
}

func TestPrivateCleanupWaitsForSettlementAndProjection(t *testing.T) {
	h, job := privateCompletionOrderingFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, stage := range []string{"handoff", "settled"} {
		admitted, err := h.loop.schedulePrivateWorkspaceCleanups(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := h.loop.waitForMaintenance(ctx); err != nil {
			t.Fatal(err)
		}
		if admitted != 0 {
			t.Fatalf("private cleanup reserved the attempt before %s completion", stage)
		}
		if _, err := os.Stat(filepath.Join(job.workspacePath, "private.txt")); err != nil {
			t.Fatalf("cleanup removed pending completion input at %s: %v", stage, err)
		}
		if stage == "handoff" {
			if admitted, err := h.loop.scheduleProviderTerminalFinalization(ctx); err != nil || admitted != 1 {
				t.Fatalf("finalizer admission: %d %v", admitted, err)
			}
			if err := h.loop.waitForMaintenance(ctx); err != nil {
				t.Fatal(err)
			}
		}
	}
	if admitted, err := h.loop.scheduleExecutionOutbox(ctx); err != nil || admitted != 1 {
		t.Fatalf("projection admission: %d %v", admitted, err)
	}
	if err := h.loop.waitForMaintenance(ctx); err != nil {
		t.Fatal(err)
	}
	if admitted, err := h.loop.schedulePrivateWorkspaceCleanups(ctx); err != nil || admitted != 1 {
		t.Fatalf("cleanup after projection: %d %v", admitted, err)
	}
	if err := h.loop.waitForMaintenance(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(job.workspacePath); !os.IsNotExist(err) {
		t.Fatalf("private cleanup did not converge: %v", err)
	}
}

func TestProviderCompletionObservationsFollowCanonicalSequence(t *testing.T) {
	h, job := privateCompletionOrderingFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if admitted, err := h.loop.scheduleProviderTerminalFinalization(ctx); err != nil || admitted != 1 {
		t.Fatalf("finalizer admission: %d %v", admitted, err)
	}
	if err := h.loop.waitForMaintenance(ctx); err != nil {
		t.Fatal(err)
	}
	var canonicalSeq int
	var canonicalID string
	if err := h.db.QueryRow(`SELECT seq,attempt_event_id FROM attempt_event_log WHERE attempt_id=? AND event_type='attempt.succeeded'`, job.attemptID).Scan(&canonicalSeq, &canonicalID); err != nil {
		t.Fatal(err)
	}
	if admitted, err := h.loop.scheduleExecutionOutbox(ctx); err != nil || admitted != 1 {
		t.Fatalf("projection admission: %d %v", admitted, err)
	}
	if err := h.loop.waitForMaintenance(ctx); err != nil {
		t.Fatal(err)
	}
	previousSeq := canonicalSeq
	for _, phase := range []string{"exec_outbox_dequeued", "complete_execution_entered", "complete_execution_exited"} {
		var seq int
		if err := h.db.QueryRow(`SELECT seq FROM events WHERE attempt_id=? AND json_extract(payload_json,'$.phase')=?`, job.attemptID, phase).Scan(&seq); err != nil {
			t.Errorf("missing real provider projection phase %s: %v", phase, err)
			continue
		}
		if seq <= previousSeq {
			t.Errorf("phase %s reused a durable sequence: got=%d previous=%d", phase, seq, previousSeq)
		}
		previousSeq = seq
	}
	var count, persistedSeq int
	var persistedID string
	if err := h.db.QueryRow(`SELECT COUNT(*),MIN(seq),MIN(attempt_event_id) FROM attempt_event_log WHERE attempt_id=? AND event_type='attempt.succeeded'`, job.attemptID).Scan(&count, &persistedSeq, &persistedID); err != nil || count != 1 || canonicalSeq != persistedSeq || canonicalID != persistedID {
		t.Fatalf("completion observations changed canonical terminal identity: count=%d seq=%d id=%s err=%v", count, persistedSeq, persistedID, err)
	}
	if count := countTestRows(t, h.db, `SELECT COUNT(*) FROM events WHERE event_type='attempt.succeeded'`); count != 1 {
		t.Fatalf("canonical terminal projection count=%d", count)
	}
	if count := countTestRows(t, h.db, `SELECT COUNT(*) FROM events WHERE event_type='attempt.sequence_gap'`); count != 0 {
		t.Fatalf("canonical settlement is not a missing event: gap count=%d", count)
	}
}

func TestPrivateCleanupFollowsCancellationHandoff(t *testing.T) {
	h, job := privateCompletionOrderingFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// The request is disposed once its canonical settlement exists; the attempt
	// is then fenced as cancelled before its result is projected.
	if _, err := h.loop.convergeProviderTerminalHandoffsWithLimit(ctx, 8); err != nil {
		t.Fatal(err)
	}
	mustExec(t, h.db, `UPDATE attempt_outbox SET status='published', published_at=created_at`)
	mustExec(t, h.db, `UPDATE attempt_executions SET state='committed_cancelled',lease_owner='',lease_expires_at=NULL`)
	mustExec(t, h.db, `UPDATE node_runs SET node_status='cancelled'`)
	if err := h.store.QueueAttemptCancel(ctx, executionstore.QueueAttemptCancelRequest{
		CancelOutboxID: "cancel_private", AttemptID: job.attemptID, RunID: job.run.RunID,
		NodeRunID: job.attempt.NodeRunID, SessionID: job.sessionID, ReasonCode: "run_terminalized",
		RequestedAt: terminalHandoffFixtureNow, CreatedAt: terminalHandoffFixtureNow,
	}); err != nil {
		t.Fatal(err)
	}
	if admitted, err := h.loop.schedulePrivateWorkspaceCleanups(ctx); err != nil || admitted != 0 {
		t.Fatalf("cleanup reserved pending cancellation: %d %v", admitted, err)
	}
	if admitted, err := h.loop.scheduleAttemptCancellations(ctx); err != nil || admitted != 1 {
		t.Fatalf("private cancellation admission: %d %v", admitted, err)
	}
	if err := h.loop.waitForMaintenance(ctx); err != nil {
		t.Fatal(err)
	}
	assertCancellationHandoff(t, h, job.attemptID, "published", "released")
	if admitted, err := h.loop.schedulePrivateWorkspaceCleanups(ctx); err != nil || admitted != 1 {
		t.Fatalf("private cleanup stranded after cancellation: %d %v", admitted, err)
	}
	if err := h.loop.waitForMaintenance(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(job.workspacePath); !os.IsNotExist(err) {
		t.Fatalf("cancelled private workspace remains: %v", err)
	}
}
