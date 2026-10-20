package orchestration

import (
	"context"
	"io"
	"strings"
	"testing"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/execution/providerrequest"
	"kingdom/internal/state"
)

func TestResumePromptContinuesFromCheckpointAndEndsWithPinnedFinalChecks(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	applyMigration(t, db)
	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('builder', 'Builder', 0, '2026-03-01T12:00:00Z')`)
	seedReadyDispatchNodeRun(t, db, readyDispatchNodeRunSeed{
		RunID: "run_resume_prompt", WorkOrderID: "wo_resume_prompt", WorkflowID: "wf_resume_prompt", NodeRunID: "nr_resume_prompt", NodeID: "implementation", NodeClass: "delegated_execution", RequiredRole: "builder", AgentID: "agent_resume_prompt",
	})
	mustExec(t, db, `INSERT INTO attempt_executions(attempt_id,run_id,node_run_id,state,execution_mode,completion_mode,failure_code,created_at,updated_at) VALUES ('source-attempt','run_resume_prompt','nr_resume_prompt','committed_cancelled','provider_backed','server_response','agent_unresponsive','2026-03-01T12:00:00Z','2026-03-01T12:00:00Z')`)
	loop := orchestrationLoop{db: db}
	file, config, err := loop.prepareResumePrompt(ctx, runSnapshot{RunID: "run_resume_prompt"}, nodeDefinition{NodeID: "implementation", NodeClass: "delegated_execution", RequiredRole: "builder"}, state.AttemptResumeInput{
		Checkpoint: state.AttemptResumeCheckpoint{SourceAttemptID: "source-attempt", CapturedAt: "2026-03-01T12:00:05Z"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	raw, err := io.ReadAll(io.NewSectionReader(file.File, 0, file.size))
	if err != nil {
		t.Fatal(err)
	}
	prompt := string(raw)
	for _, required := range []string{"Resumed Attempt:\n- ", "unverified progress", "Previous attempt: cancelled (agent_unresponsive)", "Saved at: 2026-03-01T12:00:05Z"} {
		if !strings.Contains(prompt, required) {
			t.Errorf("resume prompt is missing %q:\n%s", required, prompt)
		}
	}
	if strings.Contains(prompt, "Current Work Order:") {
		t.Fatalf("resume resent run context the conversation already holds:\n%s", prompt)
	}
	if final := strings.LastIndex(prompt, "Final Checks:\n"); final < 0 || strings.Contains(prompt[final:], instructionSectionSeparator) {
		t.Fatalf("pinned Final Checks are not the last section:\n%s", prompt)
	}
	if !strings.Contains(config.SystemPrompt, "Kingdom Contract") {
		t.Fatal("resume lost the run-pinned system prompt")
	}
}

func TestResumeProviderRequestAdoptsOnlyTheCheckpointedConversation(t *testing.T) {
	open := func(context.Context) (io.ReadCloser, error) { return io.NopCloser(strings.NewReader("continue")), nil }
	source := providerexecution.RequestIdentity{SessionID: "ses_source", RequestID: "req_source"}
	loop := orchestrationLoop{}
	if request := loop.providerRequest(asyncExecutionResult{openPrompt: open}); request.ResumeFrom != nil || request.OpenContinuationPrompt != nil {
		t.Fatalf("fresh work adopted a conversation: %+v", request)
	}
	request := loop.providerRequest(asyncExecutionResult{openPrompt: open, resume: &state.AttemptResumeInput{
		Boundary: state.AttemptResumeBoundary{MessageID: "msg_last", MessagesSHA256: strings.Repeat("a", 64)},
		Binding:  providerrequest.Binding{Identity: source},
	}})
	if request.ResumeFrom == nil || request.ResumeFrom.Source != source || request.ResumeFrom.Checkpoint.MessageID != "msg_last" || request.ResumeFrom.Checkpoint.MessagesSHA256 != strings.Repeat("a", 64) {
		t.Fatalf("resume request lost its checkpoint: %+v", request.ResumeFrom)
	}
	if request.OpenContinuationPrompt == nil || request.ProviderSessionID != "" || request.ForceNewSession {
		t.Fatalf("resume request must continue only with its new input: %+v", request)
	}
}
