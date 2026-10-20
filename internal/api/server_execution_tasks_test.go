package api

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"testing"

	gen "kingdom/internal/apigen"
	"kingdom/internal/core/providerexecution"
	"kingdom/internal/state"
)

func TestServerRunExecutionTasksServesLatestReportedList(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunWithLinearExecutionWorkflow(t, handler)
	const attemptID = "attempt-tasks-1"
	now := "2026-06-14T10:00:00Z"
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, display_name, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-tasks-1', 'builder', 'Builder', 'busy', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('nr-tasks-1', ?, 'implementation', 1,1, 'running', 'builder', 'agent-tasks-1', 1, ?, NULL)`, runID, now)
	seedAttemptExecution(t, db, attemptID, runID, "nr-tasks-1", now)
	read := func(attempt string) (int, gen.RunExecutionTaskList) {
		t.Helper()
		res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/executions/" + attempt + "/tasks"})
		var list gen.RunExecutionTaskList
		if res.Code == http.StatusOK {
			if err := json.Unmarshal(res.Body.Bytes(), &list); err != nil {
				t.Fatal(err)
			}
		}
		return res.Code, list
	}

	if code, list := read(attemptID); code != http.StatusOK || list.State != gen.None || list.Tasks == nil || len(list.Tasks) != 0 || list.Sequence != nil {
		t.Fatalf("empty task list: %d %+v", code, list)
	}

	ingestor := state.NewAttemptEventIngestor(db, nil)
	for seq, body := range []string{
		`{"todos":[{"content":"Add slugify","status":"in_progress"}]}`,
		`{"todos":[{"content":"Add slugify","status":"completed"},{"content":"Add wrap","status":"in_progress"}]}`,
	} {
		if _, err := ingestor.Ingest(t.Context(), state.AttemptEventIngestionRequest{
			EventID: "evt-tasks-" + string(rune('a'+seq)), RunID: runID, NodeRunID: sql.NullString{String: "nr-tasks-1", Valid: true},
			AttemptID: attemptID, SchemaVersion: "v1", EventType: state.AttemptActivityEventType, Seq: seq + 1,
			PayloadJSON: `{"kind":"kingdom.tool","activity_id":"todo","tool_name":"todowrite","tool_status":"completed"}`,
			ActorID:     "runtime_orchestrator", RoleID: "builder", AuthoritySource: "runtime.orchestrator",
			ReasonCode: "provider_activity", ReasonText: "provider activity telemetry", CorrelationID: "corr-tasks", CausationID: "caus-tasks",
			CreatedAt: now, ActivityContents: map[string]providerexecution.ActivityContent{"input": providerexecution.ActivityText(body)},
		}); err != nil {
			t.Fatal(err)
		}
	}
	code, list := read(attemptID)
	if code != http.StatusOK || list.State != gen.Available || list.Sequence == nil || *list.Sequence != 2 || list.RecordedAt == nil {
		t.Fatalf("latest task list: %d %+v", code, list)
	}
	if len(list.Tasks) != 2 || list.Tasks[0] != (gen.RunExecutionTask{Content: "Add slugify", Status: "completed"}) || list.Tasks[1].Status != "in_progress" {
		t.Fatalf("tasks changed in transport: %+v", list.Tasks)
	}

	if code, _ := read("attempt-missing"); code != http.StatusNotFound {
		t.Fatalf("unknown attempt must be 404, got %d", code)
	}
}
