package api

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
)

func seedRunProvenanceAttempt(t *testing.T, db *sql.DB, runID, id, mode, state, started string, attempt int) {
	t.Helper()
	const at = "2026-04-30T12:00:00Z"
	mustExecSQL(t, db, `INSERT OR IGNORE INTO agents (agent_id,role_id,status,spawn_depth,created_at,updated_at) VALUES ('provenance-agent','plan-author','idle',0,?,?)`, at, at)
	mustExecSQL(t, db, `INSERT INTO node_runs (node_run_id,run_id,node_id,attempt,wave,node_status,required_role,agent_id,row_version) VALUES (?,?, 'kickoff',?,1,'pending','plan-author','provenance-agent',1)`, id, runID, attempt)
	completion := "server_response"
	if mode == "runtime_owned" {
		completion = "not_applicable"
	}
	mustExecSQL(t, db, `INSERT INTO attempt_executions (attempt_id,run_id,node_run_id,state,execution_mode,completion_mode,started_at,created_at,updated_at) VALUES (?,?,?,?,?,?,NULLIF(?,''),?,?)`, id, runID, id, state, mode, completion, started, at, at)
}

func seedRunProvenanceSettlement(t *testing.T, db *sql.DB, runID, id string, launched bool) {
	t.Helper()
	const at = "2026-04-30T12:01:00Z"
	mustExecSQL(t, db, `INSERT INTO provider_invocations (provider_invocation_id,attempt_id,run_id,node_run_id,invocation_state,completion_mode,created_at,updated_at) VALUES (?,?,?,?, 'settled_failed','server_response',?,?)`, "inv-"+id, id, runID, id, at, at)
	// A reused session identity alone is not proof that this invocation ran.
	payload := fmt.Sprintf(`{"process_exited":%t,"provider_session_id":"known-session"}`, launched)
	mustExecSQL(t, db, `INSERT INTO provider_completion_evidence (evidence_id,provider_invocation_id,attempt_id,run_id,node_run_id,evidence_kind,payload_json,created_at) VALUES (?,?,?,?,?,'stream_observation',?,?)`, "evidence-"+id, "inv-"+id, id, runID, id, payload, at)
	mustExecSQL(t, db, `INSERT INTO provider_completion_settlements (settlement_id,provider_invocation_id,attempt_id,run_id,node_run_id,outcome,primary_evidence_id,settled_at,created_at) VALUES (?,?,?,?,?,'failed',?,?,?)`, "settlement-"+id, "inv-"+id, id, runID, id, "evidence-"+id, at, at)
	mustExecSQL(t, db, `UPDATE attempt_executions SET canonical_settlement_id=? WHERE attempt_id=?`, "settlement-"+id, id)
}

func TestRunStartProjectionDistinguishesExecutionEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, mode, executionState, started, wantState string
		settle, launched                               bool
	}{
		{"queued", "provider_backed", "queued", "", "not_started", false, false},
		{"launch handoff", "provider_backed", "starting", "", "not_started", false, false},
		{"cancelled before launch", "provider_backed", "committed_cancelled", "", "not_started", false, false},
		{"setup failed without process", "provider_backed", "committed_failure", "", "not_started", true, false},
		{"settled without running observation", "provider_backed", "committed_failure", "", "unrecorded", true, true},
		{"runtime ceremony", "runtime_owned", "committed_success", "2026-04-30T12:00:00Z", "not_started", false, false},
		{"observed execution", "provider_backed", "running", "2026-04-30T12:00:01Z", "observed", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler, db := newAPIServerForTest(t)
			runID := seedRunViaAPI(t, handler)
			seedRunProvenanceAttempt(t, db, runID, "start-test", tc.mode, tc.executionState, tc.started, 1)
			if tc.settle {
				seedRunProvenanceSettlement(t, db, runID, "start-test", tc.launched)
			}
			res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID})
			if res.Code != http.StatusOK {
				t.Fatalf("status %d: %s", res.Code, res.Body.String())
			}
			var got struct {
				StartState string  `json:"start_state"`
				StartedAt  *string `json:"started_at"`
			}
			if err := json.Unmarshal(res.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			if got.StartState != tc.wantState {
				t.Fatalf("start_state=%q want %q", got.StartState, tc.wantState)
			}
			if tc.wantState == "observed" {
				if got.StartedAt == nil || *got.StartedAt != tc.started {
					t.Fatalf("started_at=%v", got.StartedAt)
				}
			} else if got.StartedAt != nil {
				t.Fatalf("invented started_at=%s", *got.StartedAt)
			}
		})
	}
}

func TestRunStartProjectionKeepsFirstObservedAttemptAcrossRetry(t *testing.T) {
	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	seedRunProvenanceAttempt(t, db, runID, "first", "provider_backed", "committed_failure", "2026-04-30T12:00:01Z", 1)
	mustExecSQL(t, db, `UPDATE node_runs SET superseded_at='2026-04-30T12:01:00Z' WHERE node_run_id='first'`)
	seedRunProvenanceAttempt(t, db, runID, "retry", "provider_backed", "running", "2026-04-30T12:02:00Z", 2)
	for _, path := range []string{"/api/v1/runs/" + runID, "/api/v1/runs"} {
		res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: path})
		if res.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", path, res.Code, res.Body.String())
		}
		var value map[string]any
		if err := json.Unmarshal(res.Body.Bytes(), &value); err != nil {
			t.Fatal(err)
		}
		if items, ok := value["items"].([]any); ok {
			value = items[0].(map[string]any)
		}
		if value["started_at"] != "2026-04-30T12:00:01Z" || value["start_state"] != "observed" {
			t.Fatalf("%s: start projection=%v/%v", path, value["started_at"], value["start_state"])
		}
	}
}

func TestRunArtifactProvenanceSeparatesSettledBuilderLineage(t *testing.T) {
	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	mustExecSQL(t, db, `UPDATE workflow_nodes SET node_class='delegated_execution', required_role='builder' WHERE workflow_definition_id=(SELECT workflow_definition_id FROM workflow_runs WHERE run_id=?)`, runID)
	for i, id := range []string{"old", "current", "failed", "queued", "superseded", "unsettled"} {
		state := "committed_success"
		if id == "failed" {
			state = "committed_failure"
		}
		if id == "queued" {
			state = "queued"
		}
		seedRunProvenanceAttempt(t, db, runID, id, "provider_backed", state, "", i+1)
		if state == "committed_success" && id != "unsettled" {
			seedRunProvenanceSettlement(t, db, runID, id, true)
			mustExecSQL(t, db, `UPDATE provider_completion_settlements SET outcome='success' WHERE attempt_id=?`, id)
		}
		if id == "superseded" {
			mustExecSQL(t, db, `UPDATE node_runs SET superseded_at='2026-04-30T12:05:00Z' WHERE node_run_id=?`, id)
		}
		mustExecSQL(t, db, `UPDATE node_runs SET required_role='builder' WHERE node_run_id=?`, id)
		mustExecSQL(t, db, `INSERT INTO artifacts (artifact_id,run_id,node_run_id,attempt_id,artifact_kind,artifact_action,name,mime,path,sha256,size_bytes,producer_role,created_at) VALUES (?,?,?,?, 'deliverable','delete','removed.txt','text/plain','removed.txt','',0,'builder','2026-04-30T12:05:00Z')`, id, runID, id, id)
	}
	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/artifacts"})
	if res.Code != http.StatusOK {
		t.Fatalf("%d %s", res.Code, res.Body.String())
	}
	var got struct {
		Items []struct {
			ID          string `json:"artifact_id"`
			NodeID      string `json:"node_id"`
			AttemptID   string `json:"attempt_id"`
			AgentID     string `json:"producer_agent_id"`
			Lineage     string `json:"lineage"`
			DownloadURL string `json:"download_url"`
		} `json:"items"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Items) != 6 {
		t.Fatalf("items=%d", len(got.Items))
	}
	for _, item := range got.Items {
		want := "audit"
		if item.ID == "current" {
			want = "canonical_deliverable"
		}
		if item.Lineage != want || item.NodeID != "kickoff" || item.AttemptID != item.ID || item.AgentID != "provenance-agent" {
			t.Fatalf("wrong provenance %+v, want lineage %s", item, want)
		}
		if item.DownloadURL != "" {
			t.Fatalf("deletion has content URL: %+v", item)
		}
	}
}

func TestRunExecutionPublicationReportsStructuredPartialOutcome(t *testing.T) {
	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	seedRunProvenanceAttempt(t, db, runID, "publication", "provider_backed", "committed_failure", "", 1)
	mustExecSQL(t, db, `INSERT INTO run_publication_batches (publication_batch_id,run_id,attempt_id,target_mode,status,published_count,conflicted_count,failed_count,entries_revision,created_at) VALUES ('batch',?,'publication','direct','partial_conflict',1,1,0,1,'2026-04-30T12:05:00Z')`, runID)
	mustExecSQL(t, db, `INSERT INTO run_publication_entries (publication_entry_id,publication_batch_id,run_id,attempt_id,path,action,status,reason_code,reason_text,created_at) VALUES ('entry','batch',?,'publication','removed.txt','delete','conflicted','changed_since_snapshot','File changed outside this run.','2026-04-30T12:05:00Z')`, runID)
	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/executions/publication"})
	if res.Code != http.StatusOK {
		t.Fatalf("%d %s", res.Code, res.Body.String())
	}
	var got struct {
		Publication *struct {
			BatchID         string          `json:"batch_id"`
			Status          string          `json:"status"`
			Published       int             `json:"published_count"`
			Conflicted      int             `json:"conflicted_count"`
			EntriesRevision int64           `json:"entries_revision"`
			Entries         json.RawMessage `json:"entries"`
		} `json:"publication"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Publication == nil {
		t.Fatal("publication omitted")
	}
	p := got.Publication
	if p.BatchID != "batch" || p.Status != "partial_conflict" || p.Published != 1 || p.Conflicted != 1 || p.EntriesRevision != 1 || len(p.Entries) != 0 {
		t.Fatalf("wrong publication: %+v", p)
	}
	entries := readHistoryTestPage(t, handler, "/api/v1/runs/"+runID+"/executions/publication/publication/batch/entries")
	if len(entries.Items) != 1 || entries.Items[0]["action"] != "delete" || entries.Items[0]["status"] != "conflicted" || entries.Items[0]["reason_code"] != "changed_since_snapshot" {
		t.Fatalf("wrong publication entry: %+v", entries)
	}
}
