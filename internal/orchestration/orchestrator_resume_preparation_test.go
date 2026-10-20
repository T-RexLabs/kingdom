package orchestration

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"kingdom/internal/execution/providerrequest"
	executionstore "kingdom/internal/execution/store"
	"kingdom/internal/state"
)

func resumePreparationFixture(t *testing.T) (terminalHandoffHarness, nodeAttempt, string) {
	t.Helper()
	h := newTerminalHandoffHarness(t)
	ctx := context.Background()
	invocation := h.addInvocation(t, 1)
	binding := h.requests.issueDispatch(t, invocation.ProviderInvocationID)
	if err := h.requests.authority.CloseAdmission(ctx, binding.Identity); err != nil {
		t.Fatal(err)
	}
	target, err := h.fsStore.GetExecutionTarget(ctx, invocation.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.fsStore.UpdateExecutionTargetState(ctx, invocation.RunID, "attempt_dirty", target.TrustedSnapshotID, "", terminalHandoffFixtureNow); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(target.ExecutionRoot, "README.md"), []byte("partial implementation to continue\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	snapshot, err := h.loop.captureAttemptResultSnapshot(ctx, asyncExecutionResult{attemptID: invocation.AttemptID, run: runSnapshot{RunID: invocation.RunID}, node: nodeDefinition{NodeID: "implementation", NodeClass: "delegated_execution"}, workspacePath: target.ExecutionRoot, baseSnapshotID: h.baselineSnapshotID}, terminalHandoffFixtureNow)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, h.db, `UPDATE attempt_executions SET state='committed_failure' WHERE attempt_id='`+invocation.AttemptID+`'`)
	if _, err := state.PersistAttemptResumeCheckpoint(ctx, h.db, state.AttemptResumeCheckpointRequest{
		SourceAttemptID: invocation.AttemptID, BindingID: binding.ID, WorkspaceSnapshotID: snapshot.SnapshotID, CapturedAt: terminalHandoffFixtureNow,
		Boundary: state.AttemptResumeBoundary{InvocationID: binding.Owner.InvocationID, SessionID: binding.Identity.SessionID, MessageID: "last-message", MessagesSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := h.loop.restoreExecutionTarget(ctx, invocation.RunID, target.ExecutionRoot, h.baselineSnapshotID, terminalHandoffFixtureNow); err != nil {
		t.Fatal(err)
	}
	const nodeRunID = "nr_resume_preparation"
	const attemptID = "run_terminal_handoff:implementation:2"
	mustExec(t, h.db, `INSERT INTO node_runs(node_run_id,run_id,node_id,attempt,wave,node_status,required_role,agent_id) VALUES ('`+nodeRunID+`','run_terminal_handoff','implementation',2,1,'ready','builder','agent_terminal_handoff')`)
	mustExec(t, h.db, `INSERT INTO node_attempt_recoveries(target_node_run_id,run_id,node_id,source_attempt_id,source_node_run_id,trigger,reason_code,scheduled_at,checkpoint_source_attempt_id) VALUES ('`+nodeRunID+`','run_terminal_handoff','implementation','`+invocation.AttemptID+`','`+invocation.NodeRunID+`','operator_resume','manual_resume_requested','`+terminalHandoffFixtureNow+`','`+invocation.AttemptID+`')`)
	if err := h.store.EnqueueAttempt(ctx, executionstore.EnqueueAttemptRequest{AttemptID: attemptID, RunID: invocation.RunID, NodeRunID: nodeRunID, ExecutionMode: executionstore.ExecutionModeProviderBacked, CompletionMode: executionstore.ProviderCompletionModeServerResponse, SessionRef: binding.LogicalSessionID, CreatedAt: terminalHandoffFixtureNow}); err != nil {
		t.Fatal(err)
	}
	claim, acquired, err := h.store.ClaimQueuedProviderAttempt(ctx, executionstore.ClaimQueuedProviderAttemptRequest{AttemptID: attemptID, LeaseOwner: "resume-worker", LeaseUntil: "2099-03-01T12:00:00Z", ClaimedAt: terminalHandoffFixtureNow})
	if err != nil || !acquired {
		t.Fatalf("claim resume attempt: acquired=%v err=%v", acquired, err)
	}
	if err := providerrequest.RequireResourcesClosedTx(ctx, h.db, binding.ID); err != nil {
		t.Fatal(err)
	}
	return h, nodeAttempt{NodeID: "implementation", NodeRunID: nodeRunID, Attempt: 2, LeaseOwner: claim.LeaseOwner, LeaseVersion: claim.LeaseVersion}, snapshot.SnapshotID
}

func TestResumePreparationSeedsPartialFilesWithoutPromotingTheirTrust(t *testing.T) {
	h, attempt, seed := resumePreparationFixture(t)
	ctx := context.Background()
	workspace, err := h.loop.prepareNodeWorkspace(ctx, runSnapshot{RunID: "run_terminal_handoff", PublishMode: PublishModeDirect}, nodeDefinition{NodeID: "implementation", NodeClass: "delegated_execution"}, attempt, "run_terminal_handoff:implementation:2")
	if err != nil {
		t.Fatal(err)
	}
	if raw, err := os.ReadFile(filepath.Join(workspace.WorkspacePath, "README.md")); err != nil || string(raw) != "partial implementation to continue\n" {
		t.Fatalf("Resume discarded saved progress: %q %v", raw, err)
	}
	base, _, err := h.loop.loadAttemptSnapshotIDs(ctx, "run_terminal_handoff:implementation:2")
	if err != nil || base != h.baselineSnapshotID || base == seed {
		t.Fatalf("partial seed became trusted base: base=%q seed=%q err=%v", base, seed, err)
	}
	target, err := h.fsStore.GetExecutionTarget(ctx, "run_terminal_handoff")
	if err != nil || target.TrustedSnapshotID != h.baselineSnapshotID || target.PublishedSnapshotID != h.baselineSnapshotID || target.State != "attempt_dirty" {
		t.Fatalf("Resume promoted unverified progress: %#v %v", target, err)
	}
}

func TestResumePreparationRefusesUnexpectedWorkspaceChanges(t *testing.T) {
	h, attempt, _ := resumePreparationFixture(t)
	ctx := context.Background()
	target, err := h.fsStore.GetExecutionTarget(ctx, "run_terminal_handoff")
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(target.ExecutionRoot, "README.md")
	if err := os.WriteFile(file, []byte("operator edits after interruption\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := h.loop.prepareNodeWorkspace(ctx, runSnapshot{RunID: "run_terminal_handoff", PublishMode: PublishModeDirect}, nodeDefinition{NodeID: "implementation", NodeClass: "delegated_execution"}, attempt, "run_terminal_handoff:implementation:2"); err == nil {
		t.Fatal("Resume overwrote unexpected workspace edits")
	}
	if raw, err := os.ReadFile(file); err != nil || string(raw) != "operator edits after interruption\n" {
		t.Fatalf("conflicting edits were changed: %q %v", raw, err)
	}
}
