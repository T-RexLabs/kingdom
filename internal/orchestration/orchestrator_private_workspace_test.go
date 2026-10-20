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
	"kingdom/internal/runtimefs"
	"kingdom/internal/state"
)

// Cancel once the first real atomic restore has changed the target. This
// exercises cancellation between files without a filesystem timing race.
type cancelAfterRestoredFile struct {
	context.Context
	cancel  context.CancelFunc
	path    string
	content string
}

func (c cancelAfterRestoredFile) Err() error {
	if c.Context.Err() == nil {
		if data, err := os.ReadFile(c.path); err == nil && string(data) == c.content {
			c.cancel()
		}
	}
	return c.Context.Err()
}

func TestCancelledBuilderPreparationRestoresPartiallyMaterializedTarget(t *testing.T) {
	loop, run, source := newInspectionWorkspaceFixture(t)
	ctx := context.Background()
	if err := os.WriteFile(filepath.Join(source, "zz_tail.txt"), []byte("tail input"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loop.fsStore.CaptureSnapshot(ctx, runtimefs.CaptureSnapshotRequest{SnapshotID: "canonical_input", RunID: run.RunID, SnapshotKind: "baseline", RootPath: source, CreatedAt: loop.now().Format(time.RFC3339Nano)}); err != nil {
		t.Fatal(err)
	}
	targetRoot := t.TempDir()
	for _, file := range []string{"implementation.txt", "zz_tail.txt"} {
		if err := os.WriteFile(filepath.Join(targetRoot, file), []byte("old target"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := loop.fsStore.EnsureExecutionTarget(ctx, runtimefs.EnsureExecutionTargetRequest{RunID: run.RunID, TargetMode: PublishModeDirect, ExecutionRoot: targetRoot, SourceRoot: source, PublishRoot: source, TrustedSnapshotID: "canonical_input", State: "trusted", CreatedAt: loop.now().Format(time.RFC3339Nano), UpdatedAt: loop.now().Format(time.RFC3339Nano)}); err != nil {
		t.Fatal(err)
	}
	mustExec(t, loop.db, `UPDATE node_runs SET node_status='ready' WHERE node_run_id='nr_builder'`)
	mustExec(t, loop.db, `UPDATE attempt_executions SET state='queued',base_snapshot_id='',result_snapshot_id='',lease_expires_at='2099-09-30T12:00:00Z' WHERE attempt_id='run_1:builder:1'`)
	parent, cancel := context.WithCancel(ctx)
	defer cancel()
	preparationCtx := cancelAfterRestoredFile{Context: parent, cancel: cancel, path: filepath.Join(targetRoot, "implementation.txt"), content: "reviewed input"}
	_, err := loop.prepareNodeWorkspace(preparationCtx, run, nodeDefinition{NodeID: "builder", NodeClass: "delegated_execution"}, nodeAttempt{NodeID: "builder", NodeRunID: "nr_builder", Attempt: 1, LeaseOwner: "owner", LeaseVersion: 1}, "run_1:builder:1")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation between restored files: %v", err)
	}
	assertWorkspaceFile(t, targetRoot, "reviewed input")
	tail, err := os.ReadFile(filepath.Join(targetRoot, "zz_tail.txt"))
	if err != nil || string(tail) != "old target" {
		t.Fatalf("restore did not stop between files: %q %v", tail, err)
	}
	target, err := loop.fsStore.GetExecutionTarget(ctx, run.RunID)
	if err != nil || target.State != "attempt_dirty" {
		t.Fatalf("partial restore remained trusted: %+v %v", target, err)
	}
	base, _, err := loop.loadAttemptSnapshotIDs(ctx, "run_1:builder:1")
	if err != nil || base != "canonical_input" {
		t.Fatalf("partial restore lost cancellation baseline: %q %v", base, err)
	}
	// The public run transition must retain recovery even after the cancelled
	// preparer has drained and released its current lease owner.
	if err := loop.executionStore.ReleaseQueuedProviderAttemptLease(ctx, "run_1:builder:1", "owner", 1, loop.now().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	terminalizePreparationRun(t, loop, run.RunID, "cancelled")
	if _, err := loop.scheduleAttemptCancellations(ctx); err != nil {
		t.Fatal(err)
	}
	waitCtx, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	if err := loop.waitForMaintenance(waitCtx); err != nil {
		t.Fatal(err)
	}
	tail, err = os.ReadFile(filepath.Join(targetRoot, "zz_tail.txt"))
	if err != nil || string(tail) != "tail input" {
		t.Fatalf("cancellation skipped partial target recovery: %q %v", tail, err)
	}
	target, err = loop.fsStore.GetExecutionTarget(ctx, run.RunID)
	if err != nil || target.State != "trusted" || target.TrustedSnapshotID != base {
		t.Fatalf("cancelled preparation target did not recover: %+v %v", target, err)
	}
}

func TestPrivateInspectionCompletionNeverCapturesOrRestoresWorkspace(t *testing.T) {
	for _, stage := range []string{"settlement", "projection"} {
		t.Run(stage, func(t *testing.T) {
			h := newTerminalHandoffHarness(t)
			invocation := h.addInvocation(t, 1)
			job := h.runningHandoffJob(t, invocation, 1)
			mustExec(t, h.db, `INSERT INTO roles(role_id,display_name,can_spawn,created_at) VALUES ('qa-validator','QA',0,'2026-03-01T12:00:00Z')`)
			mustExec(t, h.db, `UPDATE workflow_nodes SET required_role='qa-validator', node_class='validation_gates'`)
			mustExec(t, h.db, `UPDATE node_runs SET required_role='qa-validator'`)
			mustExec(t, h.db, `UPDATE agents SET role_id='qa-validator'`)
			job.node.RequiredRole, job.node.NodeClass = "qa-validator", "validation_gates"
			job.attempt.RequiredRole = job.node.RequiredRole
			job.baseSnapshotID = h.baselineSnapshotID
			job.workspacePath = t.TempDir()
			job.delegator = job.attempt.AgentID
			if err := os.WriteFile(filepath.Join(job.workspacePath, "implementation.txt"), []byte("private test output"), 0o644); err != nil {
				t.Fatal(err)
			}
			h.configureSuccessfulHandoff(t, &job, "VERDICT: PASS\nVerified pinned implementation.")
			h.loop.mailbox = state.NewMailboxStore(h.db, h.loop.schemas)
			h.loop.events = state.NewAttemptEventIngestor(h.db, h.loop.schemas)
			if err := h.fsStore.UpdateExecutionTargetState(context.Background(), invocation.RunID, "attempt_dirty", h.baselineSnapshotID, "", terminalHandoffFixtureNow); err != nil {
				t.Fatal(err)
			}
			persistProviderTerminalHandoffForTest(t, h.loop, &job)
			if stage == "settlement" {
				if err := h.loop.commitExecutionCompletion(context.Background(), job); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := h.loop.commitProviderExecutionCompletion(context.Background(), job); err != nil {
					t.Fatal(err)
				}
				if err := h.loop.completeExecution(context.Background(), job); err != nil {
					t.Fatal(err)
				}
			}
			var resultSnapshot string
			if err := h.db.QueryRow(`SELECT COALESCE(result_snapshot_id,'') FROM attempt_executions WHERE attempt_id=?`, job.attemptID).Scan(&resultSnapshot); err != nil {
				t.Fatal(err)
			}
			if resultSnapshot != "" {
				t.Fatalf("private inspection persisted result snapshot %q", resultSnapshot)
			}
			assertWorkspaceFile(t, job.workspacePath, "private test output")
			target, err := h.fsStore.GetExecutionTarget(context.Background(), invocation.RunID)
			if err != nil || target.State != "attempt_dirty" || target.TrustedSnapshotID != h.baselineSnapshotID {
				t.Fatalf("private completion mutated builder target: %+v, %v", target, err)
			}
		})
	}
}

func TestInspectionPinsInputBeforeLaterBuilderPromotion(t *testing.T) {
	loop, run, root := newInspectionWorkspaceFixture(t)
	ctx := context.Background()
	review := nodeDefinition{NodeID: "review", NodeClass: "execution_review", RequiredRole: "governance"}
	attempt := nodeAttempt{NodeID: "review", NodeRunID: "nr_review", Attempt: 1, LeaseOwner: "owner", LeaseVersion: 1}
	attemptID := composeAttemptID(run.RunID, review.NodeID, 1)
	if err := loop.pinAttemptWorkspaceInput(ctx, run, review, attempt, attemptID); err != nil {
		t.Fatal(err)
	}
	// A later builder may promote another tree while an inspection waits for a
	// preparation slot. Its admitted input remains the preceding reviewed tree.
	if err := os.WriteFile(filepath.Join(root, "implementation.txt"), []byte("later builder"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := loop.fsStore.CaptureSnapshot(ctx, runtimefs.CaptureSnapshotRequest{SnapshotID: "later", RunID: run.RunID, SnapshotKind: "attempt_result", ParentSnapshotID: "input", RootPath: root, CreatedAt: loop.now().Format(time.RFC3339Nano)}); err != nil {
		t.Fatal(err)
	}
	mustExec(t, loop.db, `UPDATE attempt_executions SET result_snapshot_id = 'later' WHERE attempt_id = 'run_1:builder:1'`)
	if err := loop.pinAttemptWorkspaceInput(ctx, run, review, attempt, attemptID); err != nil {
		t.Fatal(err)
	}
	prepared, err := loop.prepareNodeWorkspace(ctx, run, review, attempt, attemptID)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.BaseSnapshotID != "input" || prepared.CleanupMarkedAt == "" {
		t.Fatalf("lost pinned input or cleanup identity: %#v", prepared)
	}
	assertWorkspaceFile(t, prepared.WorkspacePath, "reviewed input")
	assertWorkspaceFile(t, root, "later builder")
	var count int
	if err := loop.db.QueryRow(`SELECT COUNT(*) FROM run_execution_targets`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("inspection created canonical ownership: count=%d err=%v", count, err)
	}
}

func TestParallelValidatorsHaveIndependentPinnedWorkspaces(t *testing.T) {
	loop, run, source := newInspectionWorkspaceFixture(t)
	ctx := context.Background()
	// Review auto-success intentionally has no result snapshot. Validators
	// inherit its input, including the cap-skipped path through a builder.
	mustExec(t, loop.db, `UPDATE node_runs SET node_status = 'succeeded' WHERE node_id = 'review'`)
	mustExec(t, loop.db, `UPDATE attempt_executions SET state = 'committed_success', base_snapshot_id = 'input' WHERE attempt_id = 'run_1:review:1'`)
	type result struct {
		workspace preparedNodeWorkspace
		err       error
	}
	results := make(chan result, 2)
	for _, id := range []string{"qa", "security"} {
		node := nodeDefinition{NodeID: id, NodeClass: "validation_gates", RequiredRole: "governance"}
		a := nodeAttempt{NodeID: id, NodeRunID: "nr_" + id, Attempt: 1, LeaseOwner: "owner", LeaseVersion: 1}
		attemptID := composeAttemptID(run.RunID, id, 1)
		if err := loop.pinAttemptWorkspaceInput(ctx, run, node, a, attemptID); err != nil {
			t.Fatal(err)
		}
		go func() { w, err := loop.prepareNodeWorkspace(ctx, run, node, a, attemptID); results <- result{w, err} }()
	}
	first, second := <-results, <-results
	if first.err != nil || second.err != nil {
		t.Fatalf("prepare validators: %v / %v", first.err, second.err)
	}
	if first.workspace.WorkspacePath == second.workspace.WorkspacePath {
		t.Fatal("validators share writable input")
	}
	for _, w := range []preparedNodeWorkspace{first.workspace, second.workspace} {
		if w.BaseSnapshotID != "input" {
			t.Fatalf("validator input = %q", w.BaseSnapshotID)
		}
		assertWorkspaceFile(t, w.WorkspacePath, "reviewed input")
	}
	if err := os.WriteFile(filepath.Join(first.workspace.WorkspacePath, "implementation.txt"), []byte("test side effect"), 0o644); err != nil {
		t.Fatal(err)
	}
	assertWorkspaceFile(t, second.workspace.WorkspacePath, "reviewed input")
	assertWorkspaceFile(t, source, "reviewed input")
	var snapshots int
	if err := loop.db.QueryRow(`SELECT COUNT(*) FROM filesystem_snapshots`).Scan(&snapshots); err != nil || snapshots != 1 {
		t.Fatalf("private preparation captured result snapshots: %d, %v", snapshots, err)
	}
}

func TestCapSkippedReviewStillPinsBuilderInput(t *testing.T) {
	loop, run, _ := newInspectionWorkspaceFixture(t)
	mustExec(t, loop.db, `UPDATE node_runs SET node_status = 'succeeded' WHERE node_id = 'review'`)
	mustExec(t, loop.db, `UPDATE attempt_executions SET state = 'committed_success', base_snapshot_id = '' WHERE attempt_id = 'run_1:review:1'`)
	got, err := loop.inspectionInputSnapshot(context.Background(), run, nodeDefinition{NodeID: "qa", NodeClass: "validation_gates"})
	if err != nil || got != "input" {
		t.Fatalf("cap-skipped review lost builder input: %q, %v", got, err)
	}
}

func TestBuilderSnapshotBaseRequiresCurrentUnexpiredPreparationLease(t *testing.T) {
	loop, _, _ := newInspectionWorkspaceFixture(t)
	const attemptID = "run_1:builder:1"
	const at = "2026-09-30T12:00:00Z"
	loop.now = func() time.Time { return time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC) }
	mustExec(t, loop.db, `UPDATE attempt_executions SET state='queued', lease_expires_at='2026-09-30T12:01:00Z' WHERE attempt_id='run_1:builder:1'`)
	for _, fence := range []struct {
		owner   string
		version int64
	}{{"other", 1}, {"owner", 2}, {"", 0}} {
		if err := loop.recordAttemptSnapshotBase(context.Background(), attemptID, "input", fence.owner, fence.version); err == nil {
			t.Fatalf("accepted foreign snapshot ownership: %+v", fence)
		}
	}
	mustExec(t, loop.db, `UPDATE attempt_executions SET lease_expires_at='`+at+`' WHERE attempt_id='run_1:builder:1'`)
	if err := loop.recordAttemptSnapshotBase(context.Background(), attemptID, "input", "owner", 1); err == nil {
		t.Fatal("accepted expired snapshot ownership")
	}
	base, _, err := loop.loadAttemptSnapshotIDs(context.Background(), attemptID)
	if err != nil || base != "" {
		t.Fatalf("stale preparation changed baseline: %q %v", base, err)
	}
	mustExec(t, loop.db, `UPDATE attempt_executions SET lease_expires_at='2026-09-30T12:01:00Z' WHERE attempt_id='run_1:builder:1'`)
	if err := loop.recordAttemptSnapshotBase(context.Background(), attemptID, "input", "owner", 1); err != nil {
		t.Fatal(err)
	}
	base, _, err = loop.loadAttemptSnapshotIDs(context.Background(), attemptID)
	if err != nil || base != "input" {
		t.Fatalf("current preparation did not record baseline: %q %v", base, err)
	}
}

func newInspectionWorkspaceFixture(t *testing.T) (*orchestrationLoop, runSnapshot, string) {
	t.Helper()
	db := openTestDB(t)
	applyMigration(t, db)
	seedMinimumGraph(t, db)
	const at = "2026-09-30T12:00:00Z"
	for _, n := range []struct{ id, class, status string }{{"builder", "delegated_execution", "succeeded"}, {"review", "execution_review", "ready"}, {"qa", "validation_gates", "ready"}, {"security", "validation_gates", "ready"}} {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_nodes (workflow_node_id,workflow_definition_id,node_id,required_role,node_class,created_at) VALUES ('wn_%s','wfdef_1','%s','governance','%s','%s')`, n.id, n.id, n.class, at))
		mustExec(t, db, fmt.Sprintf(`INSERT INTO node_runs (node_run_id,run_id,node_id,attempt,wave,node_status,required_role,agent_id,row_version) VALUES ('nr_%s','run_1','%s',1,1,'%s','governance','agent_governance',1)`, n.id, n.id, n.status))
		state := "queued"
		snapshot := ""
		if n.id == "builder" {
			state = "committed_success"
			snapshot = "input"
		}
		mustExec(t, db, fmt.Sprintf(`INSERT INTO attempt_executions (attempt_id,run_id,node_run_id,state,execution_mode,completion_mode,lease_owner,lease_version,lease_expires_at,result_snapshot_id,created_at,updated_at) VALUES ('run_1:%s:1','run_1','nr_%s','%s','provider_backed','server_response','owner',1,'2099-09-30T12:00:00Z','%s','%s','%s')`, n.id, n.id, state, snapshot, at, at))
	}
	for i, e := range [][2]string{{"builder", "review"}, {"review", "qa"}, {"review", "security"}} {
		mustExec(t, db, fmt.Sprintf(`INSERT INTO workflow_edges (workflow_edge_id,workflow_definition_id,from_node_id,to_node_id,on_condition,created_at) VALUES ('edge_%d','wfdef_1','%s','%s','success','%s')`, i, e[0], e[1], at))
	}
	store, err := runtimefs.NewStore(db, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "implementation.txt"), []byte("reviewed input"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CaptureSnapshot(context.Background(), runtimefs.CaptureSnapshotRequest{SnapshotID: "input", RunID: "run_1", SnapshotKind: "attempt_result", RootPath: root, CreatedAt: at}); err != nil {
		t.Fatal(err)
	}
	return &orchestrationLoop{db: db, fsStore: store, executionStore: executionstore.New(db), yoloRoot: root, now: time.Now}, runSnapshot{RunID: "run_1", WorkflowDefinitionID: "wfdef_1", PublishMode: PublishModeDirect}, root
}

func assertWorkspaceFile(t *testing.T, root, expected string) {
	t.Helper()
	got, err := os.ReadFile(filepath.Join(root, "implementation.txt"))
	if err != nil || string(got) != expected {
		t.Fatalf("workspace %s = %q, want %q; %v", root, got, expected, err)
	}
}

func TestInspectionCancellationDoesNotRestoreBuilderWorkspace(t *testing.T) {
	for _, class := range []string{"execution_review", "validation_gates"} {
		t.Run(class, func(t *testing.T) {
			db := openTestDB(t)
			applyMigration(t, db)
			seedMinimumGraph(t, db)
			ctx := context.Background()
			store, err := runtimefs.NewStore(db, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			path := filepath.Join(root, "implementation.txt")
			if err := os.WriteFile(path, []byte("reviewed input"), 0o644); err != nil {
				t.Fatal(err)
			}
			const at = "2026-09-30T12:00:00Z"
			if _, err := store.CaptureSnapshot(ctx, runtimefs.CaptureSnapshotRequest{SnapshotID: "input", RunID: "run_1", SnapshotKind: "baseline", RootPath: root, CreatedAt: at}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.EnsureExecutionTarget(ctx, runtimefs.EnsureExecutionTargetRequest{RunID: "run_1", TargetMode: "direct", ExecutionRoot: root, SourceRoot: root, PublishRoot: root, TrustedSnapshotID: "input", State: "attempt_dirty", CreatedAt: at, UpdatedAt: at}); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte("new builder work"), 0o644); err != nil {
				t.Fatal(err)
			}
			loop := orchestrationLoop{db: db, fsStore: store, executionStore: executionstore.New(db)}
			if err := loop.restoreTrustedWorkspaceAfterCancellation(ctx, executionstore.AttemptCancelRecord{RunID: "run_1", AttemptID: "inspection", NodeID: "inspection", NodeClass: class, BaseSnapshotID: "input"}, at); err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != "new builder work" {
				t.Fatalf("cancelling a private inspection changed builder workspace: %q", got)
			}
			target, err := store.GetExecutionTarget(ctx, "run_1")
			if err != nil {
				t.Fatal(err)
			}
			if target.State != "attempt_dirty" {
				t.Fatalf("inspection changed builder ownership state: %s", target.State)
			}
		})
	}
}
