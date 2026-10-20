package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"kingdom/internal/execution/providerrequest"
)

func resumeCheckpointFixture(t *testing.T) (*sql.DB, AttemptResumeCheckpointRequest) {
	t.Helper()
	db, attempt, binding, stamp := boundTransitionRequestFixture(t)
	mustExec(t, db, `INSERT INTO filesystem_snapshots(snapshot_id,run_id,snapshot_kind,created_at) VALUES ('resume-base','run_1','baseline','`+stamp+`'),('resume-partial','run_1','attempt_result','`+stamp+`')`)
	mustExec(t, db, `UPDATE attempt_executions SET state='committed_failure',base_snapshot_id='resume-base',result_snapshot_id='resume-partial' WHERE attempt_id='`+attempt.AttemptID+`'`)
	mustExec(t, db, `UPDATE provider_requests SET admission_closed_at='`+stamp+`'`)
	return db, AttemptResumeCheckpointRequest{
		SourceAttemptID: attempt.AttemptID, BindingID: binding.ID, WorkspaceSnapshotID: "resume-partial", CapturedAt: stamp,
		Boundary: AttemptResumeBoundary{InvocationID: binding.Owner.InvocationID, SessionID: binding.Identity.SessionID, MessageID: "last-message", MessagesSHA256: strings.Repeat("a", 64)},
	}
}

func TestAttemptResumeCheckpointRetainsOriginalBaseAndImmutableBoundary(t *testing.T) {
	db, req := resumeCheckpointFixture(t)
	ctx := context.Background()
	checkpoint, err := PersistAttemptResumeCheckpoint(ctx, db, req)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.BaseSnapshotID != "resume-base" || checkpoint.WorkspaceSnapshotID != "resume-partial" || checkpoint.ConversationBoundary.ContentID == "" {
		t.Fatalf("checkpoint conflates seed and trust: %+v", checkpoint)
	}
	reader, err := openVerifiedExecutionContent(ctx, db, checkpoint.ConversationBoundary)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := io.ReadAll(reader)
	reader.Close()
	if err != nil {
		t.Fatal(err)
	}
	var boundary struct {
		RequestID    string `json:"request_id"`
		InvocationID string `json:"invocation_id"`
		MessageID    string `json:"message_id"`
	}
	if err := json.Unmarshal(raw, &boundary); err != nil || boundary.RequestID != "request" || boundary.InvocationID != req.Boundary.InvocationID || boundary.MessageID != "last-message" {
		t.Fatalf("boundary=%s err=%v", raw, err)
	}
	// The API is idempotent for an identical capture, including its exact time.
	if again, err := PersistAttemptResumeCheckpoint(ctx, db, req); err != nil || again != checkpoint {
		t.Fatalf("replay=%+v err=%v", again, err)
	}
	req.Boundary.MessageID = "different-message"
	if _, err := PersistAttemptResumeCheckpoint(ctx, db, req); err == nil {
		t.Fatal("checkpoint content was replaced")
	}
	if _, err := db.Exec(`UPDATE attempt_resume_checkpoints SET unavailable_reason='changed'`); err == nil {
		t.Fatal("checkpoint is mutable through SQL")
	}
	if _, err := db.Exec(`DELETE FROM attempt_resume_checkpoints`); err == nil {
		t.Fatal("referenced checkpoint was deleted")
	}
	loaded, err := LoadAttemptResumeCheckpoint(ctx, db, req.SourceAttemptID)
	if err != nil || loaded != checkpoint {
		t.Fatalf("load=%+v err=%v", loaded, err)
	}
}

func TestAttemptResumeCheckpointRejectsUntrustedSourceAndOpenResources(t *testing.T) {
	for _, scenario := range []string{"success", "active", "wrong_invocation", "wrong_session", "missing_base", "wrong_snapshot", "other_run_snapshot", "open_admission", "pending_native", "pending_command", "invalid_digest", "missing_binding"} {
		t.Run(scenario, func(t *testing.T) {
			db, req := resumeCheckpointFixture(t)
			switch scenario {
			case "success":
				mustExec(t, db, `UPDATE attempt_executions SET state='committed_success'`)
			case "active":
				mustExec(t, db, `UPDATE attempt_executions SET state='running'`)
			case "wrong_invocation":
				req.Boundary.InvocationID = "other-invocation"
			case "wrong_session":
				req.Boundary.SessionID = "other-session"
			case "missing_base":
				mustExec(t, db, `UPDATE attempt_executions SET base_snapshot_id=''`)
			case "wrong_snapshot":
				req.WorkspaceSnapshotID = "resume-base"
			case "other_run_snapshot":
				mustExec(t, db, `INSERT INTO workflow_runs(run_id,work_order_id,workflow_definition_id,run_status,plan_state,created_at,updated_at) SELECT 'other-run',work_order_id,workflow_definition_id,'failed','draft',created_at,updated_at FROM workflow_runs WHERE run_id='run_1'`)
				mustExec(t, db, `UPDATE filesystem_snapshots SET run_id='other-run' WHERE snapshot_id='resume-partial'`)
			case "open_admission":
				// The monotonic closure trigger correctly prevents reopening, so
				// remove only that test fixture trigger before constructing debt.
				mustExec(t, db, `DROP TRIGGER provider_requests_facts_monotonic`)
				mustExec(t, db, `UPDATE provider_requests SET admission_closed_at=NULL`)
			case "pending_native":
				mustExecArgs(t, db, `INSERT INTO provider_native_grants(grant_id,binding_id,message_id,call_id,tool,input_json,identity_digest,created_at) VALUES ('grant',?,'message','call','write','{}',?,?)`, req.BindingID, strings.Repeat("b", 64), req.CapturedAt)
			case "pending_command":
				mustExecArgs(t, db, `INSERT INTO provider_command_calls(claim_id,binding_id,message_id,call_id,tool,input_json,identity_digest,command_ordinal,created_at) VALUES ('claim',?,'message','call','shell','{}',?,1,?)`, req.BindingID, strings.Repeat("b", 64), req.CapturedAt)
			case "invalid_digest":
				req.Boundary.MessagesSHA256 = strings.Repeat("A", 64)
			case "missing_binding":
				req.BindingID = ""
			}
			_, err := PersistAttemptResumeCheckpoint(context.Background(), db, req)
			if err == nil {
				t.Fatal("invalid checkpoint accepted")
			}
			if strings.HasPrefix(scenario, "pending_") || scenario == "open_admission" {
				if !errors.Is(err, providerrequest.ErrResourcesOpen) {
					t.Fatalf("resource error=%v", err)
				}
			}
			for _, table := range []string{"attempt_resume_checkpoints", "request_contents"} {
				var count int
				if err := db.QueryRow(`SELECT count(*) FROM ` + table).Scan(&count); err != nil || count != 0 {
					t.Fatalf("%s left %d rows: %v", table, count, err)
				}
			}
		})
	}
}

func TestAttemptResumeCheckpointAcceptsCancellationAndRejectsLostReferences(t *testing.T) {
	db, req := resumeCheckpointFixture(t)
	mustExec(t, db, `UPDATE attempt_executions SET state='committed_cancelled'`)
	checkpoint, err := PersistAttemptResumeCheckpoint(context.Background(), db, req)
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`DELETE FROM filesystem_snapshots WHERE snapshot_id='resume-partial'`,
		`DELETE FROM request_contents WHERE content_id='` + checkpoint.ConversationBoundary.ContentID + `'`,
		`DELETE FROM provider_requests`,
	} {
		if _, err := db.Exec(query); err == nil {
			t.Fatalf("checkpoint reference removed: %s", query)
		}
	}
}

func TestAttemptResumeCheckpointUnavailableCannotBypassResourceDebt(t *testing.T) {
	for _, omitBinding := range []bool{false, true} {
		t.Run(fmt.Sprint(omitBinding), func(t *testing.T) {
			db, req := resumeCheckpointFixture(t)
			req.UnavailableReason = "Capture failed."
			req.WorkspaceSnapshotID = ""
			req.Boundary = AttemptResumeBoundary{}
			mustExecArgs(t, db, `INSERT INTO provider_command_calls(claim_id,binding_id,message_id,call_id,tool,input_json,identity_digest,command_ordinal,created_at) VALUES ('claim',?,'message','call','shell','{}',?,1,?)`, req.BindingID, strings.Repeat("b", 64), req.CapturedAt)
			if omitBinding {
				req.BindingID = ""
			}
			if _, err := PersistAttemptResumeCheckpoint(context.Background(), db, req); err == nil {
				t.Fatal("unavailable marker bypassed source resource debt")
			}
		})
	}
}

func TestAttemptResumeCheckpointUnavailableIsExplicitAndCannotBecomeReady(t *testing.T) {
	db, req := resumeCheckpointFixture(t)
	req.UnavailableReason = "The provider conversation could not be captured."
	req.WorkspaceSnapshotID = ""
	req.Boundary = AttemptResumeBoundary{}
	checkpoint, err := PersistAttemptResumeCheckpoint(context.Background(), db, req)
	if err != nil || checkpoint.UnavailableReason != req.UnavailableReason || checkpoint.ConversationBoundary.ContentID != "" {
		t.Fatalf("unavailable checkpoint=%+v err=%v", checkpoint, err)
	}
	if _, err := PersistAttemptResumeCheckpoint(context.Background(), db, req); err != nil {
		t.Fatal(err)
	}
	req.WorkspaceSnapshotID = "resume-partial"
	if _, err := PersistAttemptResumeCheckpoint(context.Background(), db, req); err == nil {
		t.Fatal("mixed ready/unavailable checkpoint accepted")
	}
}
