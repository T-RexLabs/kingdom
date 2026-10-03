package api

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/state"
)

func mustMailboxJSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

type mailboxContentFixture struct {
	handler                                         http.Handler
	db                                              *sql.DB
	runID, otherRunID, attemptID, nodeRunID, nodeID string
	instruction                                     map[string]any
	response                                        providerexecution.ContentReference
	text                                            string
}

func seedMailboxReferencedContent(t *testing.T) mailboxContentFixture {
	t.Helper()
	handler, db := newAPIServerForTest(t)
	f := mailboxContentFixture{handler: handler, db: db, runID: seedRunViaAPI(t, handler)}
	f.otherRunID = seedOtherHistoryRun(t, handler)
	for _, id := range []string{"ref-agent-a", "ref-agent-b"} {
		mustExecSQL(t, db, `INSERT INTO agents(agent_id,role_id,status,spawn_depth,created_at,updated_at) VALUES (?,'coordinator','idle',0,'2026-09-30T10:00:00Z','2026-09-30T10:00:00Z')`, id)
	}
	f.nodeRunID, f.nodeID = "mailbox-content-node", "kickoff"
	mustExecSQL(t, db, `INSERT INTO node_runs(node_run_id,run_id,node_id,attempt,wave,node_status,required_role,agent_id,row_version) VALUES (?,?,'kickoff',1,1,'succeeded','coordinator','ref-agent-a',1)`, f.nodeRunID, f.runID)
	f.attemptID = "mailbox-content-attempt"
	seedAttemptExecution(t, db, f.attemptID, f.runID, f.nodeRunID, "2026-09-30T10:00:00Z")
	f.text = "# Canonical Content\n\n" + strings.Repeat("完整 content beyond the preview.\n", 10000) + "\nCANONICAL TAIL\n"
	f.response = providerexecution.ReferenceText("mailbox-content-invocation", f.text)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	ref, err := state.WriteRequestContentTx(t.Context(), tx, "mailbox-prompt", state.RequestContentSource{Reader: strings.NewReader(f.text), SHA256: f.response.SHA256, SizeBytes: f.response.SizeBytes}, "2026-09-30T10:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(`INSERT INTO execution_prompt_refs(attempt_id,run_id,node_run_id,content_id,system_content_id,created_at) VALUES (?,?,?,?,?,'2026-09-30T10:00:00Z')`, f.attemptID, f.runID, f.nodeRunID, ref.ContentID, ref.ContentID); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	f.instruction = map[string]any{"content_id": ref.ContentID, "sha256": ref.SHA256, "size_bytes": ref.SizeBytes}
	mustExecSQL(t, db, `INSERT INTO provider_invocations(provider_invocation_id,attempt_id,run_id,node_run_id,invocation_state,completion_mode,created_at,updated_at) VALUES (?,?,?,?,'settled_success','server_response','2026-09-30T10:00:00Z','2026-09-30T10:00:00Z')`, f.response.InvocationID, f.attemptID, f.runID, f.nodeRunID)
	mustExecSQL(t, db, `INSERT INTO provider_contents(provider_invocation_id,sha256,size_bytes) VALUES (?,?,?)`, f.response.InvocationID, f.response.SHA256, f.response.SizeBytes)
	for offset, ordinal := 0, 0; offset < len(f.text); ordinal++ {
		end := min(offset+providerexecution.ContentChunkBytes, len(f.text))
		mustExecSQL(t, db, `INSERT INTO provider_content_chunks(provider_invocation_id,sha256,ordinal,content) VALUES (?,?,?,?)`, f.response.InvocationID, f.response.SHA256, ordinal, []byte(f.text[offset:end]))
		offset = end
	}
	return f
}

func (f mailboxContentFixture) message(t *testing.T, id, runID, kind string, changes map[string]any) string {
	t.Helper()
	payload := map[string]any{"message_source": "communication", "communication_type": kind, "run_id": f.runID, "node_id": f.nodeID, "node_run_id": f.nodeRunID, "attempt_id": f.attemptID, "body_markdown": "Generic dispatch summary"}
	if kind == "instruction_dispatched" {
		payload["instruction_full_ref"] = f.instruction
	} else {
		payload["response_full_ref"] = f.response
		payload["response_preview"] = "An honest short response preview"
	}
	for key, value := range changes {
		payload[key] = value
	}
	mustExecSQL(t, f.db, `INSERT INTO mailbox_messages(message_id,run_id,from_agent_id,to_agent_id,thread_id,message_type,priority,payload_json,created_at) VALUES (?,?,'ref-agent-a','ref-agent-b','ref-thread','status_update','normal',?,'2026-09-30T10:00:00Z')`, id, runID, mustMailboxJSON(t, payload))
	return "/api/v1/runs/" + runID + "/mailbox/messages/" + id
}

func TestMailboxFullContentReferencesAreLazyCompleteAndScoped(t *testing.T) {
	f := seedMailboxReferencedContent(t)
	for _, kind := range []string{"instruction_dispatched", "response_received"} {
		path := f.message(t, kind, f.runID, kind, nil)
		detail := performJSONRequest(t, f.handler, requestSpec{method: http.MethodGet, path: path})
		if detail.Code != 200 {
			t.Fatalf("detail: %d %s", detail.Code, detail.Body.String())
		}
		var metadata struct {
			Has     bool   `json:"has_full_content"`
			Preview bool   `json:"content_is_preview"`
			URL     string `json:"full_content_url"`
			Content string `json:"content"`
		}
		if err := json.Unmarshal(detail.Body.Bytes(), &metadata); err != nil {
			t.Fatal(err)
		}
		if !metadata.Has || !metadata.Preview || metadata.URL != path+"/content" || strings.Contains(detail.Body.String(), "CANONICAL TAIL") || detail.Body.Len() > 4096 {
			t.Fatalf("detail expanded or lost content metadata: %s", detail.Body.String())
		}
		if kind == "response_received" && metadata.Content != "An honest short response preview" {
			t.Fatalf("response detail lost preview: %q", metadata.Content)
		}
		if kind == "instruction_dispatched" && metadata.Content != "" {
			t.Fatalf("generic dispatch text was presented as instruction preview: %q", metadata.Content)
		}
		res := performJSONRequest(t, f.handler, requestSpec{method: http.MethodGet, path: metadata.URL})
		if res.Code != 200 || res.Body.String() != f.text {
			t.Fatalf("%s content: status=%d bytes=%d", kind, res.Code, res.Body.Len())
		}
		if res.Header().Get("Content-Length") != strconv.Itoa(len(f.text)) || res.Header().Get("X-Content-Type-Options") != "nosniff" || !strings.HasPrefix(res.Header().Get("Content-Disposition"), "attachment;") {
			t.Fatalf("content headers: %v", res.Header())
		}
		anonymous := httptest.NewRecorder()
		f.handler.ServeHTTP(anonymous, httptest.NewRequest(http.MethodGet, metadata.URL, nil))
		if anonymous.Code != 401 {
			t.Fatalf("anonymous content=%d", anonymous.Code)
		}
		crossPath := strings.Replace(metadata.URL, f.runID, f.otherRunID, 1)
		if res := performJSONRequest(t, f.handler, requestSpec{method: http.MethodGet, path: crossPath}); res.Code != 404 {
			t.Fatalf("cross-run message=%d", res.Code)
		}
	}
	list := performJSONRequest(t, f.handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + f.runID + "/mailbox/messages"})
	if list.Code != 200 || strings.Contains(list.Body.String(), "CANONICAL TAIL") || !strings.Contains(list.Body.String(), `"has_full_content":true`) || !strings.Contains(list.Body.String(), `"full_content_url":`) {
		t.Fatalf("list metadata: %d %s", list.Code, list.Body.String())
	}
}

func TestMailboxFullContentRejectsForeignLineageAndCorruptionBeforeHeaders(t *testing.T) {
	f := seedMailboxReferencedContent(t)
	otherNode, otherAttempt, otherInvocation := "foreign-content-node", "foreign-content-attempt", "foreign-content-invocation"
	mustExecSQL(t, f.db, `INSERT INTO node_runs(node_run_id,run_id,node_id,attempt,wave,node_status,required_role,agent_id,row_version) VALUES (?,?,'kickoff',1,1,'succeeded','coordinator','ref-agent-a',1)`, otherNode, f.otherRunID)
	seedAttemptExecution(t, f.db, otherAttempt, f.otherRunID, otherNode, "2026-09-30T10:00:00Z")
	mustExecSQL(t, f.db, `INSERT INTO provider_invocations(provider_invocation_id,attempt_id,run_id,node_run_id,invocation_state,completion_mode,created_at,updated_at) VALUES (?,?,?,?,'settled_success','server_response','2026-09-30T10:00:00Z','2026-09-30T10:00:00Z')`, otherInvocation, otherAttempt, f.otherRunID, otherNode)
	foreignRef := f.response
	foreignRef.InvocationID = otherInvocation
	foreignPath := f.message(t, "foreign-invocation-ref", f.runID, "response_received", map[string]any{"response_full_ref": foreignRef})
	if res := performJSONRequest(t, f.handler, requestSpec{method: http.MethodGet, path: foreignPath + "/content"}); res.Code != 404 {
		t.Fatalf("foreign invocation reference=%d", res.Code)
	}
	// Both the mailbox row and payload lineage claim the legitimate target run;
	// the actual immutable prompt/ref still belongs to the original attempt.
	for _, kind := range []string{"instruction_dispatched", "response_received"} {
		path := f.message(t, "foreign-bound-"+kind, f.otherRunID, kind, map[string]any{"run_id": f.otherRunID, "node_run_id": otherNode, "attempt_id": otherAttempt})
		if res := performJSONRequest(t, f.handler, requestSpec{method: http.MethodGet, path: path + "/content"}); res.Code != 404 {
			t.Fatalf("foreign bound %s reference=%d", kind, res.Code)
		}
	}
	for _, kind := range []string{"instruction_dispatched", "response_received"} {
		// A reference copied into another run must not become readable even when
		// the payload lies about its run, attempt, or node lineage.
		for _, test := range []struct {
			name, run string
			edits     map[string]any
		}{
			{"foreign-row", f.otherRunID, nil},
			{"foreign-claim", f.otherRunID, map[string]any{"run_id": f.otherRunID}},
			{"wrong-attempt", f.runID, map[string]any{"attempt_id": "other-attempt"}},
			{"wrong-node", f.runID, map[string]any{"node_id": "other-node"}},
		} {
			path := f.message(t, kind+"-"+test.name, test.run, kind, test.edits)
			res := performJSONRequest(t, f.handler, requestSpec{method: http.MethodGet, path: path + "/content"})
			if res.Code != 404 || strings.Contains(res.Body.String(), "CANONICAL TAIL") {
				t.Fatalf("lineage %s %s: %d", kind, test.name, res.Code)
			}
		}
	}
	badResponse := f.response
	badResponse.SHA256 = strings.Repeat("0", 64)
	badPrompt := map[string]any{"content_id": f.instruction["content_id"], "sha256": strings.Repeat("0", 64), "size_bytes": f.response.SizeBytes}
	for _, test := range []struct {
		name, kind, field string
		ref               any
	}{
		{"bad-prompt", "instruction_dispatched", "instruction_full_ref", badPrompt},
		{"bad-response", "response_received", "response_full_ref", badResponse},
		{"null-prompt", "instruction_dispatched", "instruction_full_ref", nil},
	} {
		path := f.message(t, test.name, f.runID, test.kind, map[string]any{test.field: test.ref})
		res := performJSONRequest(t, f.handler, requestSpec{method: http.MethodGet, path: path + "/content"})
		if res.Code < 400 || res.Header().Get("Content-Disposition") != "" || strings.Contains(res.Body.String(), "Generic dispatch summary") {
			t.Fatalf("bad ref escaped verification: %d %s", res.Code, res.Body.String())
		}
	}
	// Corrupt stored bytes without changing their declared digest. No bytes
	// should be published until the existing verified readers have reached EOF.
	mustExecSQL(t, f.db, `DROP TRIGGER provider_content_chunks_immutable`)
	mustExecSQL(t, f.db, `UPDATE provider_content_chunks SET content=zeroblob(length(content)) WHERE provider_invocation_id=? AND ordinal=0`, f.response.InvocationID)
	path := f.message(t, "corrupt-response", f.runID, "response_received", nil)
	res := performJSONRequest(t, f.handler, requestSpec{method: http.MethodGet, path: path + "/content"})
	if res.Code != 500 || res.Header().Get("Content-Disposition") != "" {
		t.Fatalf("corrupt response published: %d", res.Code)
	}
	mustExecSQL(t, f.db, `DROP TRIGGER request_content_chunks_no_update`)
	mustExecSQL(t, f.db, `UPDATE request_content_chunks SET content=zeroblob(length(content)) WHERE content_id='mailbox-prompt' AND ordinal=0`)
	path = f.message(t, "corrupt-instruction", f.runID, "instruction_dispatched", nil)
	res = performJSONRequest(t, f.handler, requestSpec{method: http.MethodGet, path: path + "/content"})
	if res.Code != 500 || res.Header().Get("Content-Disposition") != "" {
		t.Fatalf("corrupt instruction published: %d", res.Code)
	}
	// Hidden system rows cannot use the content route as an alternate read path.
	path = f.message(t, "hidden", f.runID, "response_received", map[string]any{"message_source": "system"})
	res = performJSONRequest(t, f.handler, requestSpec{method: http.MethodGet, path: path + "/content"})
	if res.Code != 404 {
		t.Fatalf("hidden content=%d", res.Code)
	}
}

func TestMailboxFullContentInlinePreservesCompleteBytes(t *testing.T) {
	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	for _, id := range []string{"mail-content-a", "mail-content-b"} {
		mustExecSQL(t, db, `INSERT INTO agents(agent_id,role_id,status,spawn_depth,created_at,updated_at) VALUES (?,'coordinator','idle',0,'2026-09-30T10:00:00Z','2026-09-30T10:00:00Z')`, id)
	}
	text := "# Complete instruction\n" + strings.Repeat("Full inline content. ", 5000) + "\nINLINE TAIL\n"
	payload := mustMailboxJSON(t, map[string]any{"message_source": "communication", "communication_type": "instruction_dispatched", "instruction_full": text, "body_markdown": "Generic dispatch summary"})
	mustExecSQL(t, db, `INSERT INTO mailbox_messages(message_id,run_id,from_agent_id,to_agent_id,thread_id,message_type,priority,payload_json,created_at) VALUES ('full-inline',?,'mail-content-a','mail-content-b','content-thread','status_update','normal',?,'2026-09-30T10:00:00Z')`, runID, payload)
	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/mailbox/messages/full-inline/content"})
	if res.Code != http.StatusOK || res.Body.String() != text {
		t.Fatalf("full inline content: status=%d bytes=%d want=%d", res.Code, res.Body.Len(), len(text))
	}
	if res.Header().Get("X-Content-Type-Options") != "nosniff" || !strings.HasPrefix(res.Header().Get("Content-Disposition"), "attachment;") {
		t.Fatalf("unsafe content headers: %v", res.Header())
	}
	for _, kind := range []string{"response_received", "approval_response"} {
		responseText := "  # Complete response\n" + strings.Repeat("Response body. ", 5000) + "\nRESPONSE TAIL\n"
		payload := mustMailboxJSON(t, map[string]any{"message_source": "communication", "communication_type": kind, "instruction_full": "Instruction must not replace the response", "response_full": responseText, "body_markdown": "Generic dispatch summary"})
		mustExecSQL(t, db, `INSERT INTO mailbox_messages(message_id,run_id,from_agent_id,to_agent_id,thread_id,message_type,priority,payload_json,created_at) VALUES (?,?,'mail-content-a','mail-content-b','content-thread','status_update','normal',?,'2026-09-30T10:00:00Z')`, kind, runID, payload)
		res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: "/api/v1/runs/" + runID + "/mailbox/messages/" + kind + "/content"})
		if res.Code != 200 || res.Body.String() != responseText {
			t.Fatalf("inline %s body changed: %d", kind, res.Code)
		}
	}
}
