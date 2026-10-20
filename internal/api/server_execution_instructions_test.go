package api

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"kingdom/internal/state"
)

func TestExecutionInstructionsExactScopedAndVerified(t *testing.T) {
	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)
	otherRun := seedOtherHistoryRun(t, handler)
	const attemptID = "instructions-attempt"
	const nodeRunID = "instructions-node"
	mustExecSQL(t, db, `INSERT INTO agents(agent_id,role_id,status,spawn_depth,created_at,updated_at) VALUES ('instructions-agent','builder','idle',0,'2026-09-30T10:00:00Z','2026-09-30T10:00:00Z')`)
	mustExecSQL(t, db, `INSERT INTO node_runs(node_run_id,run_id,node_id,attempt,wave,node_status,required_role,agent_id,row_version) VALUES (?,?,'implementation',1,1,'succeeded','builder','instructions-agent',1)`, nodeRunID, runID)
	seedAttemptExecution(t, db, attemptID, runID, nodeRunID, "2026-09-30T10:00:00Z")
	path := "/api/v1/runs/" + runID + "/executions/" + attemptID + "/instructions"
	if res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: path}); res.Code != 404 {
		t.Fatalf("missing prompt: %d", res.Code)
	}
	system := "Protected Role Instruction:\nYou are the Reviewer.\n\nCommunication Style:\nNeutral\n\nAgent Guidance:\nCaptured guidance.\n\nKingdom Contract:\nVERDICT required."
	text := "  # Exact Agent Instructions\n" + strings.Repeat("完整 instructions including context and output contract.\n", 10000) + "EXACT TAIL\n\n"
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	ref, err := state.WriteRequestContentTx(t.Context(), tx, "instructions-content", state.RequestContentSource{Reader: strings.NewReader(text), SizeBytes: int64(len(text)), SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(text)))}, "2026-09-30T10:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	systemRef, err := state.WriteRequestContentTx(t.Context(), tx, "instructions-system", state.RequestContentSource{Reader: strings.NewReader(system), SizeBytes: int64(len(system)), SHA256: fmt.Sprintf("%x", sha256.Sum256([]byte(system)))}, "2026-09-30T10:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(`INSERT INTO execution_prompt_refs(attempt_id,run_id,node_run_id,content_id,system_content_id,created_at) VALUES (?,?,?,?,?,?)`, attemptID, runID, nodeRunID, ref.ContentID, systemRef.ContentID, "2026-09-30T10:00:00Z"); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	// Capture occurs before workspace preparation; do not publish input that
	// has not been issued to a provider.
	pending := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: path})
	if pending.Code != http.StatusNotFound || pending.Header().Get("Content-Disposition") != "" {
		t.Fatalf("unissued input published: status=%d", pending.Code)
	}

	mustExecSQL(t, db, `INSERT INTO provider_invocations(provider_invocation_id,attempt_id,run_id,node_run_id,invocation_state,completion_mode,created_at,updated_at) VALUES (?,?,?, ?,'settled_success','server_response','2026-09-30T10:00:00Z','2026-09-30T10:00:00Z')`, "instructions-invocation", attemptID, runID, nodeRunID)
	expected := "# System Instructions\n\n" + system + "\n\n# User Instructions\n\n" + text
	// Later continuation authority must not rewrite the initial captured input.
	mustExecSQL(t, db, `INSERT INTO provider_invocations(provider_invocation_id,attempt_id,run_id,node_run_id,invocation_state,completion_mode,created_at,updated_at) VALUES (?,?,?,?,'settled_success','server_response','2026-09-30T10:01:00Z','2026-09-30T10:01:00Z')`, "instructions-continuation", attemptID, runID, nodeRunID)

	res := performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: path})
	if res.Code != 200 || res.Body.String() != expected {
		t.Fatalf("exact content: status=%d bytes=%d", res.Code, res.Body.Len())
	}
	if res.Header().Get("Cache-Control") != "no-store" || res.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatalf("unsafe headers: %v", res.Header())
	}
	anonymous := httptest.NewRecorder()
	handler.ServeHTTP(anonymous, httptest.NewRequest(http.MethodGet, path, nil))
	if anonymous.Code != 401 {
		t.Fatalf("anonymous=%d", anonymous.Code)
	}
	foreign := strings.Replace(path, runID, otherRun, 1)
	if res = performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: foreign}); res.Code != 404 {
		t.Fatalf("cross-run=%d", res.Code)
	}
	mustExecSQL(t, db, `DROP TRIGGER request_content_chunks_no_update`)
	mustExecSQL(t, db, `UPDATE request_content_chunks SET content=zeroblob(length(content)) WHERE content_id=? AND ordinal=0`, systemRef.ContentID)
	res = performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: path})
	if res.Code != 500 || strings.Contains(res.Body.String(), "# System Instructions") || res.Header().Get("Content-Disposition") != "" {
		t.Fatalf("corrupt system bytes published: status=%d", res.Code)
	}
	mustExecSQL(t, db, `UPDATE request_content_chunks SET content=? WHERE content_id=? AND ordinal=0`, []byte(system), systemRef.ContentID)
	mustExecSQL(t, db, `UPDATE request_content_chunks SET content=zeroblob(length(content)) WHERE content_id=? AND ordinal=0`, ref.ContentID)
	res = performJSONRequest(t, handler, requestSpec{method: http.MethodGet, path: path})
	if res.Code != 500 || strings.Contains(res.Body.String(), "EXACT TAIL") || res.Header().Get("Content-Disposition") != "" {
		t.Fatalf("corrupt bytes published: status=%d", res.Code)
	}
}
