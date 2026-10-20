package state

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"
)

func resumeReplayFixture(t *testing.T) (*sql.DB, ReplayTerminalRunRequest, AttemptResumeCheckpoint) {
	t.Helper()
	db, capture := resumeCheckpointFixture(t)
	checkpoint, err := PersistAttemptResumeCheckpoint(context.Background(), db, capture)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, db, `UPDATE workflow_runs SET run_status='failed'`)
	mustExec(t, db, `UPDATE node_runs SET node_status='failed' WHERE node_run_id='nr_1'`)
	seq := 0
	return db, ReplayTerminalRunRequest{
		RunID: "run_1", AnchorNodeID: "work_intake", TargetNodeIDs: []string{"work_intake"},
		ResumeSources: map[string]string{"work_intake": capture.SourceAttemptID},
		Reason:        "Continue saved work.", ActorID: "local-operator", RoleID: "governance", AuthoritySource: "api.resume_run",
		RunEventType: "run.resume_reopened", RunReasonCode: "manual_resume_requested", NodeEventType: "attempt.resume_queued", EventPrefix: "resume", OccurredAt: capture.CapturedAt,
		GenerateID: func(prefix string) string { seq++; return fmt.Sprintf("%s_resume_%d", prefix, seq) },
	}, checkpoint
}

func TestResumeReplayPinsExactCheckpointAndLoadsVerifiedInput(t *testing.T) {
	db, req, saved := resumeReplayFixture(t)
	queued, err := NewTransitionService(db, nil).ReplayTerminalRun(context.Background(), req)
	if err != nil || len(queued) != 1 {
		t.Fatalf("queue=%+v err=%v", queued, err)
	}
	if queued[0].Attempt != 2 {
		t.Fatalf("did not create fresh attempt: %+v", queued)
	}
	input, err := LoadAttemptResumeInput(context.Background(), db, queued[0].NodeRunID)
	if err != nil {
		t.Fatal(err)
	}
	if input.Checkpoint != saved || input.Binding.LogicalSessionID != "conversation" || input.Boundary.InvocationID != input.Binding.Owner.InvocationID || input.Boundary.SessionID != input.Binding.Identity.SessionID || input.Boundary.MessageID != "last-message" {
		t.Fatalf("resume input lost source identity: %+v", input)
	}
	var trigger, checkpointID string
	if err := db.QueryRow(`SELECT trigger,checkpoint_source_attempt_id FROM node_attempt_recoveries WHERE target_node_run_id=?`, queued[0].NodeRunID).Scan(&trigger, &checkpointID); err != nil || trigger != "operator_resume" || checkpointID != saved.SourceAttemptID {
		t.Fatalf("provenance=%s/%s err=%v", trigger, checkpointID, err)
	}
	if _, err := NewTransitionService(db, nil).ReplayTerminalRun(context.Background(), req); err == nil {
		t.Fatal("repeated resume created another attempt")
	}
	if _, err := db.Exec(`UPDATE node_attempt_recoveries SET checkpoint_source_attempt_id=NULL`); err == nil {
		t.Fatal("checkpoint selection is mutable")
	}
}

func TestResumeReplayRejectsStaleUnavailableAndUnsettledSource(t *testing.T) {
	for _, scenario := range []string{"stale", "not_frontier", "not_interrupted", "missing_checkpoint", "pending_completion", "pending_cancel", "pending_handoff", "open_resources", "rerun_with_resume_sources", "missing_sources", "unrelated_target", "partial_parallel_frontier"} {
		t.Run(scenario, func(t *testing.T) {
			db, req, _ := resumeReplayFixture(t)
			switch scenario {
			case "stale":
				req.ResumeSources["work_intake"] = "old-attempt"
			case "not_frontier":
				mustExec(t, db, `INSERT INTO workflow_nodes(workflow_node_id,workflow_definition_id,node_id,required_role,node_class,created_at) VALUES ('wn_parent','wfdef_1','parent','governance','work_intake','2026-03-01T12:00:00Z')`)
				mustExec(t, db, `INSERT INTO workflow_edges(workflow_edge_id,workflow_definition_id,from_node_id,to_node_id,on_condition,created_at) VALUES ('edge_parent','wfdef_1','parent','work_intake','always','2026-03-01T12:00:00Z')`)
				mustExec(t, db, `INSERT INTO node_runs(node_run_id,run_id,node_id,attempt,wave,node_status,required_role,agent_id,row_version) VALUES ('nr_parent','run_1','parent',1,1,'cancelled','governance','agent_governance',1)`)
			case "not_interrupted":
				mustExec(t, db, `UPDATE node_runs SET node_status='succeeded'`)
			case "missing_checkpoint":
				mustExec(t, db, `DROP TRIGGER attempt_resume_checkpoints_retained`)
				mustExec(t, db, `DELETE FROM attempt_resume_checkpoints`)
			case "pending_completion":
				mustExec(t, db, `INSERT INTO attempt_outbox(outbox_id,attempt_id,run_id,node_run_id,terminal_seq,terminal_state,payload_json,status,created_at,updated_at) SELECT 'outbox',attempt_id,run_id,node_run_id,1,state,'{}','pending',created_at,created_at FROM attempt_executions`)
			case "pending_cancel":
				mustExec(t, db, `INSERT INTO attempt_cancel_outbox(cancel_outbox_id,attempt_id,run_id,node_run_id,session_id,status,requested_at,created_at,updated_at) SELECT 'cancel',attempt_id,run_id,node_run_id,'conversation','pending',created_at,created_at,created_at FROM attempt_executions`)
			case "pending_handoff":
				mustExec(t, db, `UPDATE provider_invocations SET terminal_handoff_pending_at=created_at`)
			case "open_resources":
				mustExec(t, db, `INSERT INTO provider_command_calls(claim_id,binding_id,message_id,call_id,tool,input_json,identity_digest,command_ordinal,created_at) SELECT 'claim',binding_id,'message','call','shell','{}','`+strings.Repeat("b", 64)+`',1,created_at FROM provider_requests`)
			case "rerun_with_resume_sources":
				req.RunReasonCode = "manual_rerun_requested"
			case "missing_sources":
				req.ResumeSources = nil
			case "unrelated_target":
				req.TargetNodeIDs = append(req.TargetNodeIDs, "parent")
			case "partial_parallel_frontier":
				mustExec(t, db, `INSERT INTO workflow_nodes(workflow_node_id,workflow_definition_id,node_id,required_role,node_class,created_at) VALUES ('wn_sibling','wfdef_1','sibling','governance','work_intake','2026-03-01T12:00:00Z')`)
				mustExec(t, db, `INSERT INTO node_runs(node_run_id,run_id,node_id,attempt,wave,node_status,required_role,agent_id,row_version) VALUES ('nr_sibling','run_1','sibling',1,1,'cancelled','governance','agent_governance',1)`)
			}
			_, err := NewTransitionService(db, nil).ReplayTerminalRun(context.Background(), req)
			if err == nil {
				t.Fatal("invalid resume accepted")
			}
			if scenario == "partial_parallel_frontier" && !strings.Contains(err.Error(), "every interrupted frontier step") {
				t.Fatalf("partial frontier rejected for the wrong reason: %v", err)
			}
			if got := queryRunStatus(t, db, "run_1"); got != "failed" {
				t.Fatalf("failed recovery changed run state: %s", got)
			}
			var count int
			if err := db.QueryRow(`SELECT count(*) FROM node_attempt_recoveries`).Scan(&count); err != nil || count != 0 {
				t.Fatalf("rejected resume left recovery rows=%d err=%v", count, err)
			}
		})
	}
}

func TestResumeReplayFreshDownstreamIsNotConversationResume(t *testing.T) {
	db, req, _ := resumeReplayFixture(t)
	mustExec(t, db, `INSERT INTO node_runs(node_run_id,run_id,node_id,attempt,wave,node_status,required_role,agent_id,row_version) VALUES ('nr_downstream','run_1','completion',1,1,'cancelled','governance','agent_governance',1)`)
	mustExec(t, db, `INSERT INTO attempt_executions(attempt_id,run_id,node_run_id,state,execution_mode,completion_mode,created_at,updated_at) VALUES ('downstream-attempt','run_1','nr_downstream','committed_cancelled','runtime_owned','not_applicable','2026-03-01T12:00:00Z','2026-03-01T12:00:00Z')`)
	req.TargetNodeIDs = append(req.TargetNodeIDs, "completion")
	queued, err := NewTransitionService(db, nil).ReplayTerminalRun(context.Background(), req)
	if err != nil || len(queued) != 2 {
		t.Fatalf("queue=%+v err=%v", queued, err)
	}
	var trigger string
	var source sql.NullString
	if err := db.QueryRow(`SELECT trigger,checkpoint_source_attempt_id FROM node_attempt_recoveries WHERE target_node_run_id=?`, queued[1].NodeRunID).Scan(&trigger, &source); err != nil || trigger != "operator_rerun" || source.Valid {
		t.Fatalf("downstream incorrectly resumed: %s %+v %v", trigger, source, err)
	}
	if _, err := LoadAttemptResumeInput(context.Background(), db, queued[1].NodeRunID); err != sql.ErrNoRows {
		t.Fatalf("fresh downstream has resume input: %v", err)
	}
}

func pinResumeRunConfiguration(t *testing.T, db *sql.DB) {
	t.Helper()
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := CaptureRunAgentConfigurationSnapshotTx(context.Background(), tx, "run_1", "2026-03-01T12:00:00Z"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestRunResumePlanDerivesFrontierAndExplainsEligibility(t *testing.T) {
	ctx := context.Background()
	db, req, saved := resumeReplayFixture(t)
	pinResumeRunConfiguration(t, db)
	plan, err := LoadRunResumePlan(ctx, db, "run_1")
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Available() || len(plan.Steps) != 1 || plan.Steps[0] != (RunResumeStep{NodeID: "work_intake", SourceAttemptID: saved.SourceAttemptID, CheckpointCapturedAt: saved.CapturedAt}) {
		t.Fatalf("eligible frontier was not offered: %+v", plan)
	}
	if plan.TargetNodeIDs[0] != "work_intake" || plan.Sources()["work_intake"] != req.ResumeSources["work_intake"] {
		t.Fatalf("plan does not match the replay scope: %+v", plan)
	}
	for _, scenario := range []struct{ name, setup, reason string }{
		{"running", `UPDATE workflow_runs SET run_status='running'`, "Only a failed or cancelled run can be resumed."},
		{"runtime_owned", `UPDATE attempt_executions SET execution_mode='runtime_owned', completion_mode='not_applicable'`, "This step runs inside Kingdom and has no conversation to resume."},
		{"settling", `INSERT INTO attempt_cancel_outbox(cancel_outbox_id,attempt_id,run_id,node_run_id,session_id,status,requested_at,created_at,updated_at) SELECT 'cancel',attempt_id,run_id,node_run_id,'conversation','pending',created_at,created_at,created_at FROM attempt_executions`, "Kingdom is still settling the interrupted work: provider cancellation."},
		{"unpinned", "", "This run has no pinned agent configuration to resume with."},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			db, _, _ := resumeReplayFixture(t)
			if scenario.setup != "" {
				pinResumeRunConfiguration(t, db)
				mustExec(t, db, scenario.setup)
			}
			plan, err := LoadRunResumePlan(ctx, db, "run_1")
			if err != nil {
				t.Fatal(err)
			}
			reason := plan.UnavailableReason
			if reason == "" && len(plan.Steps) == 1 {
				reason = plan.Steps[0].UnavailableReason
			}
			if plan.Available() || reason != scenario.reason {
				t.Fatalf("unavailable reason = %q, want %q (%+v)", reason, scenario.reason, plan)
			}
		})
	}
}

func TestRunResumePlanReportsMissingCheckpointWithoutChoosingHistory(t *testing.T) {
	db, _ := resumeCheckpointFixture(t)
	pinResumeRunConfiguration(t, db)
	mustExec(t, db, `UPDATE workflow_runs SET run_status='failed'`)
	mustExec(t, db, `UPDATE node_runs SET node_status='failed' WHERE node_run_id='nr_1'`)
	plan, err := LoadRunResumePlan(context.Background(), db, "run_1")
	if err != nil {
		t.Fatal(err)
	}
	if plan.Available() || len(plan.Steps) != 1 || plan.Steps[0].UnavailableReason != "No checkpoint was saved when this step stopped." {
		t.Fatalf("missing checkpoint was not explained: %+v", plan)
	}
}

// Settlement leaves the pre-publication handoff marker in place as history.
// A settled source must stay resumable; only an unsettled handoff blocks it.
func TestRunResumePlanIgnoresSettledHistoricalHandoffMarker(t *testing.T) {
	db, _, _ := resumeReplayFixture(t)
	pinResumeRunConfiguration(t, db)
	mustExec(t, db, `UPDATE provider_invocations SET invocation_state='settled_cancelled', settled_at=created_at, terminal_handoff_pending_at=created_at`)
	plan, err := LoadRunResumePlan(context.Background(), db, "run_1")
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Available() {
		t.Fatalf("settled handoff history blocked resume: %+v", plan)
	}
}

// failQueuedResumeAttempt records the resumed attempt failing during setup,
// before it ever reached the provider, as a workspace conflict does.
func failQueuedResumeAttempt(t *testing.T, db *sql.DB, target QueuedNodeAttempt, withInvocation bool) {
	t.Helper()
	attemptID := fmt.Sprintf("run_1:work_intake:%d", target.Attempt)
	mustExec(t, db, `INSERT INTO attempt_executions(attempt_id,run_id,node_run_id,state,execution_mode,completion_mode,failure_code,failure_text,created_at,updated_at) VALUES ('`+attemptID+`','run_1','`+target.NodeRunID+`','committed_failure','provider_backed','server_response','execution_setup_failed','workspace changed','2026-03-01T13:00:00Z','2026-03-01T13:00:00Z')`)
	if withInvocation {
		mustExec(t, db, `INSERT INTO provider_invocations(provider_invocation_id,attempt_id,run_id,node_run_id,logical_session_id,provider_session_id,invocation_state,completion_mode,created_at,updated_at) VALUES ('inv_started','`+attemptID+`','run_1','`+target.NodeRunID+`','conversation','shared-session','settled_failed','server_response','2026-03-01T13:00:00Z','2026-03-01T13:00:00Z')`)
	}
	mustExec(t, db, `UPDATE node_runs SET node_status='failed' WHERE node_run_id='`+target.NodeRunID+`'`)
	mustExec(t, db, `UPDATE workflow_runs SET run_status='failed'`)
}

func TestResumeThatNeverStartedHandsItsCheckpointBack(t *testing.T) {
	ctx := context.Background()
	db, req, saved := resumeReplayFixture(t)
	pinResumeRunConfiguration(t, db)
	queued, err := NewTransitionService(db, nil).ReplayTerminalRun(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	failQueuedResumeAttempt(t, db, queued[0], false)

	plan, err := LoadRunResumePlan(ctx, db, "run_1")
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Available() || plan.Steps[0].SourceAttemptID != saved.SourceAttemptID || plan.Steps[0].CheckpointCapturedAt != saved.CapturedAt {
		t.Fatalf("checkpoint of a resume that never started was not offered again: %+v", plan)
	}
	req.ResumeSources = plan.Sources()
	req.GenerateID = func(prefix string) string { return prefix + "_retry_resume_" + fmt.Sprint(time.Now().UnixNano()) }
	again, err := NewTransitionService(db, nil).ReplayTerminalRun(ctx, req)
	if err != nil {
		t.Fatalf("resume could not retry its unconsumed checkpoint: %v", err)
	}
	input, err := LoadAttemptResumeInput(ctx, db, again[0].NodeRunID)
	if err != nil || input.Checkpoint.SourceAttemptID != saved.SourceAttemptID {
		t.Fatalf("retried resume lost its checkpoint: %+v %v", input, err)
	}
}

func TestResumeThatReachedTheProviderConsumesItsCheckpoint(t *testing.T) {
	ctx := context.Background()
	db, req, saved := resumeReplayFixture(t)
	pinResumeRunConfiguration(t, db)
	queued, err := NewTransitionService(db, nil).ReplayTerminalRun(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	failQueuedResumeAttempt(t, db, queued[0], true)

	plan, err := LoadRunResumePlan(ctx, db, "run_1")
	if err != nil {
		t.Fatal(err)
	}
	if plan.Available() || plan.Steps[0].SourceAttemptID == saved.SourceAttemptID {
		t.Fatalf("consumed checkpoint was offered again: %+v", plan)
	}
	req.GenerateID = func(prefix string) string { return prefix + "_reuse_" + fmt.Sprint(time.Now().UnixNano()) }
	if _, err := NewTransitionService(db, nil).ReplayTerminalRun(ctx, req); err == nil {
		t.Fatal("checkpoint consumed by provider work was pinned again")
	}
}
