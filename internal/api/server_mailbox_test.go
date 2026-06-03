package api

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestServerRunMailboxMessageEndpoints(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)

	now := time.Now().UTC().Format(time.RFC3339)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-mail-a', 'chancellor', 'idle', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-mail-b', 'council-member', 'idle', NULL, 0, ?, ?)`, now, now)

	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
		"msg-mail-1",
		runID,
		"agent-mail-a",
		"agent-mail-b",
		"thread-mail-1",
		"status_update",
		"normal",
		`{"body_markdown":"# Council Update\nProceeding with caution."}`,
		"2026-03-02T20:00:00Z",
	)
	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"msg-mail-2",
		runID,
		"agent-mail-b",
		"agent-mail-a",
		"thread-mail-1",
		"clarification",
		"high",
		`{"body_text":"Need final sign-off."}`,
		"2026-03-02T20:01:00Z",
		"2026-03-02T20:02:00Z",
	)
	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
		"msg_comm_mail_3",
		runID,
		"agent-mail-a",
		"agent-mail-b",
		"thread-mail-2",
		"status_update",
		"normal",
		`{"message_source":"communication","communication_type":"instruction_dispatched","body_text":"Begin implementation now.","instruction_full":"Build the homepage with orbiting planets.\nKeep the layout mobile-friendly."}`,
		"2026-03-02T20:03:00Z",
	)
	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
		"msg_comm_mail_warning",
		runID,
		"agent-mail-a",
		"agent-mail-b",
		"thread-mail-2",
		"status_update",
		"normal",
		`{"message_source":"communication","communication_type":"instruction_dispatched","reason_code":"advisor_scope_drift","instruction_full":"advisor response drifted into implementation claims; return advice-only recommendations"}`,
		"2026-03-02T20:04:00Z",
	)
	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
		"msg_comm_mail_warning_upper",
		runID,
		"agent-mail-a",
		"agent-mail-b",
		"thread-mail-2",
		"status_update",
		"normal",
		`{"message_source":"communication","communication_type":"instruction_dispatched","reason_code":"COUNCIL_SCOPE_DRIFT","instruction_full":"council response drifted into implementation claims; return planning-only recommendations"}`,
		"2026-03-02T20:04:30Z",
	)
	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
		"msg_comm_mail_no_source",
		runID,
		"agent-mail-a",
		"agent-mail-b",
		"thread-mail-2",
		"status_update",
		"normal",
		`{"communication_type":"instruction_dispatched","instruction_full":"Legacy row without explicit source should not be inferred as communication."}`,
		"2026-03-02T20:04:45Z",
	)
	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
		"msg_comm_mail_self",
		runID,
		"agent-mail-a",
		"agent-mail-a",
		"thread-mail-self",
		"status_update",
		"normal",
		`{"message_source":"communication","communication_type":"instruction_dispatched","instruction_full":"Internal note that should stay hidden from user-facing mailbox surfaces."}`,
		"2026-03-02T20:05:00Z",
	)
	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
		"msg_comm_mail_plan_suppressed",
		runID,
		"agent-mail-a",
		"agent-mail-b",
		"thread-mail-plan",
		"status_update",
		"normal",
		`{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","response_full":"Planning response should remain visible in mailbox surfaces."}`,
		"2026-03-02T20:05:30Z",
	)

	listRes := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/mailbox/messages?unread_only=true&limit=10",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if listRes.Code != http.StatusOK {
		t.Fatalf("expected 200 for mailbox list, got %d (%s)", listRes.Code, listRes.Body.String())
	}

	var listed struct {
		RunID string `json:"run_id"`
		Items []struct {
			MessageID string  `json:"message_id"`
			FromName  string  `json:"from_agent_name"`
			ToName    string  `json:"to_agent_name"`
			Source    string  `json:"message_source"`
			Preview   string  `json:"preview"`
			ReadAt    *string `json:"read_at"`
			Format    string  `json:"format"`
		} `json:"items"`
	}
	if err := json.Unmarshal(listRes.Body.Bytes(), &listed); err != nil {
		t.Fatalf("decode mailbox list payload: %v", err)
	}
	if listed.RunID != runID {
		t.Fatalf("expected run id %q, got %q", runID, listed.RunID)
	}
	if len(listed.Items) != 2 {
		t.Fatalf("expected communication-only mailbox list to return 2 items, got %d", len(listed.Items))
	}
	instructionIndex := -1
	for index := range listed.Items {
		if listed.Items[index].MessageID == "msg_comm_mail_3" {
			instructionIndex = index
			break
		}
	}
	if instructionIndex == -1 {
		t.Fatalf("expected communication message msg_comm_mail_3 in mailbox list, got %#v", listed.Items)
	}
	instructionItem := listed.Items[instructionIndex]
	if instructionItem.ReadAt != nil {
		t.Fatalf("expected unread message read_at to be nil")
	}
	if instructionItem.Format != "plain_text" {
		t.Fatalf("expected plain_text format, got %s", instructionItem.Format)
	}
	if instructionItem.FromName != "Chancellor" || instructionItem.ToName != "Council Member" {
		t.Fatalf("expected resolved mailbox names Chancellor -> Council Member, got %q -> %q", instructionItem.FromName, instructionItem.ToName)
	}
	if instructionItem.Source != "communication" {
		t.Fatalf("expected mailbox source communication, got %q", instructionItem.Source)
	}
	if !strings.Contains(strings.ToLower(instructionItem.Preview), "orbiting planets") {
		t.Fatalf("expected instruction preview to include real instruction content, got %q", instructionItem.Preview)
	}
	if !strings.Contains(instructionItem.Preview, "\n") {
		t.Fatalf("expected preview to preserve newline structure, got %q", instructionItem.Preview)
	}

	filteredRes := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/mailbox/messages?message_source=communication&communication_type=instruction_dispatched&limit=10",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if filteredRes.Code != http.StatusOK {
		t.Fatalf("expected 200 for mailbox filtered list, got %d (%s)", filteredRes.Code, filteredRes.Body.String())
	}

	var filtered struct {
		Items []struct {
			MessageID string `json:"message_id"`
			Source    string `json:"message_source"`
			CommType  string `json:"communication_type"`
		} `json:"items"`
	}
	if err := json.Unmarshal(filteredRes.Body.Bytes(), &filtered); err != nil {
		t.Fatalf("decode filtered mailbox payload: %v", err)
	}
	if len(filtered.Items) != 1 {
		t.Fatalf("expected filtered mailbox list to return 1 item, got %d", len(filtered.Items))
	}
	if filtered.Items[0].MessageID != "msg_comm_mail_3" {
		t.Fatalf("expected filtered message msg_comm_mail_3, got %s", filtered.Items[0].MessageID)
	}
	if filtered.Items[0].Source != "communication" || filtered.Items[0].CommType != "instruction_dispatched" {
		t.Fatalf("expected communication filtered values, got source=%q comm=%q", filtered.Items[0].Source, filtered.Items[0].CommType)
	}
	if filtered.Items[0].MessageID == "msg_comm_mail_warning" {
		t.Fatalf("expected drift-warning communication message to be suppressed from mailbox list")
	}
	if filtered.Items[0].MessageID == "msg_comm_mail_warning_upper" {
		t.Fatalf("expected uppercase drift-warning reason code message to be suppressed from mailbox list")
	}
	warningDetailRes := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/mailbox/messages/msg_comm_mail_warning",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if warningDetailRes.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for suppressed drift-warning mailbox detail, got %d (%s)", warningDetailRes.Code, warningDetailRes.Body.String())
	}

	upperWarningDetailRes := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/mailbox/messages/msg_comm_mail_warning_upper",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if upperWarningDetailRes.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for suppressed uppercase drift-warning mailbox detail, got %d (%s)", upperWarningDetailRes.Code, upperWarningDetailRes.Body.String())
	}

	suppressedPlanDetailRes := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/mailbox/messages/msg_comm_mail_plan_suppressed",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if suppressedPlanDetailRes.Code != http.StatusOK {
		t.Fatalf("expected 200 for planning response mailbox detail, got %d (%s)", suppressedPlanDetailRes.Code, suppressedPlanDetailRes.Body.String())
	}

	selfDetailRes := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/mailbox/messages/msg_comm_mail_self",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if selfDetailRes.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for self-contact communication mailbox detail, got %d (%s)", selfDetailRes.Code, selfDetailRes.Body.String())
	}

	legacyInferredDetailRes := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/mailbox/messages/msg_comm_mail_no_source",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if legacyInferredDetailRes.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for msg_comm_ row without explicit message_source, got %d (%s)", legacyInferredDetailRes.Code, legacyInferredDetailRes.Body.String())
	}

	nonCommDetailRes := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/mailbox/messages/msg-mail-1",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if nonCommDetailRes.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for mirrored/system mailbox detail, got %d (%s)", nonCommDetailRes.Code, nonCommDetailRes.Body.String())
	}

	detailRes := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/mailbox/messages/msg_comm_mail_3",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if detailRes.Code != http.StatusOK {
		t.Fatalf("expected 200 for communication mailbox detail, got %d (%s)", detailRes.Code, detailRes.Body.String())
	}

	var detail struct {
		MessageID string         `json:"message_id"`
		FromName  string         `json:"from_agent_name"`
		ToName    string         `json:"to_agent_name"`
		Source    string         `json:"message_source"`
		Format    string         `json:"format"`
		Content   string         `json:"content"`
		Payload   map[string]any `json:"payload_json"`
	}
	if err := json.Unmarshal(detailRes.Body.Bytes(), &detail); err != nil {
		t.Fatalf("decode mailbox detail payload: %v", err)
	}
	if detail.MessageID != "msg_comm_mail_3" {
		t.Fatalf("expected message id msg_comm_mail_3, got %s", detail.MessageID)
	}
	if detail.Format != "plain_text" {
		t.Fatalf("expected detail format plain_text, got %s", detail.Format)
	}
	if !strings.Contains(detail.Content, "Begin implementation") {
		t.Fatalf("expected detail content to include communication body")
	}
	if detail.FromName != "Chancellor" || detail.ToName != "Council Member" {
		t.Fatalf("expected detail mailbox names Chancellor -> Council Member, got %q -> %q", detail.FromName, detail.ToName)
	}
	if detail.Source != "communication" {
		t.Fatalf("expected detail source communication, got %q", detail.Source)
	}

	markRes := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/runs/" + runID + "/mailbox/messages/msg_comm_mail_3/read",
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-mail-read",
	})
	if markRes.Code != http.StatusNoContent {
		t.Fatalf("expected 204 for mark-read, got %d (%s)", markRes.Code, markRes.Body.String())
	}

	var readAt sql.NullString
	if err := db.QueryRow(`SELECT read_at FROM mailbox_messages WHERE message_id = 'msg_comm_mail_3'`).Scan(&readAt); err != nil {
		t.Fatalf("query mark-read row: %v", err)
	}
	if !readAt.Valid {
		t.Fatalf("expected read_at to be set after mark-read")
	}
}

func TestServerRunMailboxMessagesReturnsLatestCommunicationWindow(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)

	now := time.Now().UTC().Format(time.RFC3339)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-mail-old-a', 'chancellor', 'idle', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-mail-old-b', 'council-member', 'idle', NULL, 0, ?, ?)`, now, now)

	for i := 0; i < 120; i++ {
		createdAt := fmt.Sprintf("2026-03-02T20:%02d:%02dZ", (i/60)%60, i%60)
		payload := `{"message_source":"mirrored_event","body_text":"event mirror"}`
		mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
			fmt.Sprintf("msg-mirror-%d", i),
			runID,
			"agent-mail-old-a",
			"agent-mail-old-b",
			"thread-mail-window",
			"status_update",
			"normal",
			payload,
			createdAt,
		)
	}

	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
		"msg-window-comm",
		runID,
		"agent-mail-old-a",
		"agent-mail-old-b",
		"thread-mail-window",
		"status_update",
		"normal",
		`{"message_source":"communication","communication_type":"response_received","response_full":"Latest execution response from builders."}`,
		"2026-03-02T23:59:59Z",
	)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/mailbox/messages?limit=50",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200 for mailbox list, got %d (%s)", res.Code, res.Body.String())
	}

	var listed struct {
		Items []struct {
			MessageID string `json:"message_id"`
		} `json:"items"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &listed); err != nil {
		t.Fatalf("decode mailbox list payload: %v", err)
	}
	if len(listed.Items) != 1 {
		t.Fatalf("expected latest communication message to be returned in window, got %d items", len(listed.Items))
	}
	if listed.Items[0].MessageID != "msg-window-comm" {
		t.Fatalf("expected latest communication message id msg-window-comm, got %s", listed.Items[0].MessageID)
	}
}

func TestServerMarkRunMailboxMessageReadRejectsCrossRunMessageID(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runA := seedRunViaAPI(t, handler)
	now := time.Now().UTC().Format(time.RFC3339)
	runB := "run-cross-scope-b"
	mustExecSQL(t, db, `INSERT INTO workflow_runs (run_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at)
		SELECT ?, decree_id, workflow_definition_id, run_status, plan_state, 1, ?, ?
		FROM workflow_runs WHERE run_id = ?`, runB, now, now, runA)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-cross-run-a', 'chancellor', 'idle', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-cross-run-b', 'council-member', 'idle', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
		"msg-cross-run",
		runB,
		"agent-cross-run-a",
		"agent-cross-run-b",
		"thread-cross-run",
		"status_update",
		"normal",
		`{"message_source":"communication","communication_type":"response_received","response_full":"Wrong run route should not mark me read."}`,
		now,
	)

	res := performJSONRequest(t, handler, requestSpec{
		method:         http.MethodPost,
		path:           "/api/v1/runs/" + runA + "/mailbox/messages/msg-cross-run/read",
		actorID:        "actor-1",
		roleID:         "chancellor",
		idempotencyKey: "idem-cross-run-read",
	})
	if res.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for cross-run mailbox read, got %d (%s)", res.Code, res.Body.String())
	}

	var readAt sql.NullString
	if err := db.QueryRow(`SELECT read_at FROM mailbox_messages WHERE message_id = 'msg-cross-run'`).Scan(&readAt); err != nil {
		t.Fatalf("query cross-run read_at: %v", err)
	}
	if readAt.Valid {
		t.Fatalf("expected cross-run mailbox message to remain unread, got %+v", readAt)
	}
}

func TestServerRunMailboxMessagesDoNotInferPassFromMailboxText(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)

	now := time.Now().UTC().Format(time.RFC3339)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-reviewer', 'reviewer-shieldmaiden', 'idle', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-council', 'council-member', 'idle', NULL, 0, ?, ?)`, now, now)

	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
		"msg-review-pass",
		runID,
		"agent-reviewer",
		"agent-council",
		"thread-review",
		"status_update",
		"normal",
		`{"message_source":"communication","communication_type":"response_received","node_id":"execution_review_shieldmaiden","reason_code":"execution_completed","response_full":"VERDICT: PASS\nLooks good."}`,
		"2026-03-03T10:00:00Z",
	)
	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
		"msg-validation-invalid",
		runID,
		"agent-reviewer",
		"agent-council",
		"thread-validation",
		"status_update",
		"normal",
		`{"message_source":"communication","communication_type":"response_received","node_id":"validation_gates_qa_scout","reason_code":"invalid_validation_verdict","response_full":"Missing required verdict line."}`,
		"2026-03-03T10:01:00Z",
	)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/mailbox/messages?limit=10",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200 for mailbox list, got %d (%s)", res.Code, res.Body.String())
	}

	var listed struct {
		Items []struct {
			MessageID     string  `json:"message_id"`
			ReasonCode    *string `json:"reason_code"`
			VerdictType   *string `json:"verdict_type"`
			VerdictStatus *string `json:"verdict_status"`
		} `json:"items"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &listed); err != nil {
		t.Fatalf("decode mailbox list payload: %v", err)
	}
	if len(listed.Items) != 2 {
		t.Fatalf("expected 2 mailbox items, got %d", len(listed.Items))
	}

	byID := make(map[string]struct {
		ReasonCode    *string
		VerdictType   *string
		VerdictStatus *string
	}, len(listed.Items))
	for _, item := range listed.Items {
		byID[item.MessageID] = struct {
			ReasonCode    *string
			VerdictType   *string
			VerdictStatus *string
		}{item.ReasonCode, item.VerdictType, item.VerdictStatus}
	}

	pass := byID["msg-review-pass"]
	if pass.ReasonCode == nil || *pass.ReasonCode != "execution_completed" {
		t.Fatalf("expected execution_completed reason code, got %#v", pass.ReasonCode)
	}
	if pass.VerdictType == nil || *pass.VerdictType != "review" {
		t.Fatalf("expected review verdict_type, got %#v", pass.VerdictType)
	}
	if pass.VerdictStatus != nil {
		t.Fatalf("expected mailbox text to avoid pass verdict_status, got %#v", pass.VerdictStatus)
	}

	invalid := byID["msg-validation-invalid"]
	if invalid.ReasonCode == nil || *invalid.ReasonCode != "invalid_validation_verdict" {
		t.Fatalf("expected invalid_validation_verdict reason code, got %#v", invalid.ReasonCode)
	}
	if invalid.VerdictType == nil || *invalid.VerdictType != "validation" {
		t.Fatalf("expected validation verdict_type, got %#v", invalid.VerdictType)
	}
	if invalid.VerdictStatus == nil || *invalid.VerdictStatus != "invalid" {
		t.Fatalf("expected invalid verdict_status, got %#v", invalid.VerdictStatus)
	}
}

func TestServerRunMailboxMessageDetailCarriesStructuredVerdictFields(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)

	now := time.Now().UTC().Format(time.RFC3339)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-reviewer-detail', 'reviewer-shieldmaiden', 'idle', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-council-detail', 'council-member', 'idle', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
		"msg-review-cap-detail",
		runID,
		"agent-reviewer-detail",
		"agent-council-detail",
		"thread-review-detail",
		"status_update",
		"normal",
		`{"message_source":"communication","communication_type":"response_received","node_id":"execution_review_shieldmaiden","reason_code":"execution_review_cap_auto_succeeded","response_full":"Review cycle cap reached; auto-succeeded."}`,
		"2026-03-03T10:02:00Z",
	)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/mailbox/messages/msg-review-cap-detail",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200 for mailbox detail, got %d (%s)", res.Code, res.Body.String())
	}

	var detail struct {
		ReasonCode    *string `json:"reason_code"`
		VerdictType   *string `json:"verdict_type"`
		VerdictStatus *string `json:"verdict_status"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &detail); err != nil {
		t.Fatalf("decode mailbox detail payload: %v", err)
	}
	if detail.ReasonCode == nil || *detail.ReasonCode != "execution_review_cap_auto_succeeded" {
		t.Fatalf("expected auto-succeeded reason code, got %#v", detail.ReasonCode)
	}
	if detail.VerdictType == nil || *detail.VerdictType != "review" {
		t.Fatalf("expected review verdict_type, got %#v", detail.VerdictType)
	}
	if detail.VerdictStatus == nil || *detail.VerdictStatus != "auto_succeeded" {
		t.Fatalf("expected auto_succeeded verdict_status, got %#v", detail.VerdictStatus)
	}
}

func TestServerRunMailboxMessagesPreferAutoSucceededReasonOverStaleExplicitReworkVerdict(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)

	now := time.Now().UTC().Format(time.RFC3339)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-validation-cap-a', 'intelligence-qa-scout', 'idle', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-council-cap-a', 'council-member', 'idle', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
		"msg-validation-cap-overrides-rework",
		runID,
		"agent-validation-cap-a",
		"agent-council-cap-a",
		"thread-validation-cap-a",
		"status_update",
		"normal",
		`{"message_source":"communication","communication_type":"response_received","node_id":"validation_gates_qa_scout","reason_code":"validation_gate_cap_auto_succeeded","response_full":"VERDICT: REWORK\n\nOriginal validator body should remain visible."}`,
		"2026-03-03T10:02:00Z",
	)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/mailbox/messages",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200 for mailbox messages, got %d (%s)", res.Code, res.Body.String())
	}

	var payload struct {
		Items []struct {
			MessageID     string  `json:"message_id"`
			VerdictType   *string `json:"verdict_type"`
			VerdictStatus *string `json:"verdict_status"`
		} `json:"items"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode mailbox messages payload: %v", err)
	}
	if len(payload.Items) == 0 {
		t.Fatal("expected mailbox items")
	}
	item := payload.Items[0]
	if item.MessageID != "msg-validation-cap-overrides-rework" {
		t.Fatalf("expected auto-succeeded validation message first, got %q", item.MessageID)
	}
	if item.VerdictType == nil || *item.VerdictType != "validation" {
		t.Fatalf("expected validation verdict_type, got %#v", item.VerdictType)
	}
	if item.VerdictStatus == nil || *item.VerdictStatus != "auto_succeeded" {
		t.Fatalf("expected auto_succeeded verdict_status, got %#v", item.VerdictStatus)
	}
}

func TestServerRunMailboxMessagesRejectsLimitAboveMax(t *testing.T) {
	t.Parallel()

	handler, _ := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/mailbox/messages?limit=201",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for mailbox limit > 200, got %d (%s)", res.Code, res.Body.String())
	}
}

func TestServerRunMailboxMessagesDoesNotDropOlderUserFacingMessagesWhenNewerSystemRowsExist(t *testing.T) {
	t.Parallel()

	handler, db := newAPIServerForTest(t)
	runID := seedRunViaAPI(t, handler)

	now := time.Now().UTC().Format(time.RFC3339)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-mail-reg-a', 'chancellor', 'idle', NULL, 0, ?, ?)`, now, now)
	mustExecSQL(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent-mail-reg-b', 'council-member', 'idle', NULL, 0, ?, ?)`, now, now)

	mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
		"msg-old-user-facing",
		runID,
		"agent-mail-reg-a",
		"agent-mail-reg-b",
		"thread-mail-regression",
		"status_update",
		"normal",
		`{"message_source":"communication","communication_type":"response_received","response_full":"Advisor evidence packet"}`,
		"2026-03-02T09:00:00Z",
	)

	for i := 0; i < 120; i++ {
		createdAt := fmt.Sprintf("2026-03-02T20:%02d:%02dZ", (i/60)%60, i%60)
		payload := `{"message_source":"communication","communication_type":"progress_update","body_text":"non user-facing progress"}`
		mustExecSQL(t, db, `INSERT INTO mailbox_messages (message_id, run_id, from_agent_id, to_agent_id, thread_id, message_type, priority, payload_json, created_at, read_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)`,
			fmt.Sprintf("msg-progress-%d", i),
			runID,
			"agent-mail-reg-a",
			"agent-mail-reg-b",
			"thread-mail-regression",
			"status_update",
			"normal",
			payload,
			createdAt,
		)
	}

	res := performJSONRequest(t, handler, requestSpec{
		method:  http.MethodGet,
		path:    "/api/v1/runs/" + runID + "/mailbox/messages?limit=50",
		actorID: "actor-1",
		roleID:  "chancellor",
	})
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200 for mailbox list, got %d (%s)", res.Code, res.Body.String())
	}

	var listed struct {
		Items []struct {
			MessageID string `json:"message_id"`
		} `json:"items"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &listed); err != nil {
		t.Fatalf("decode mailbox list payload: %v", err)
	}
	if len(listed.Items) != 1 {
		t.Fatalf("expected only user-facing response message, got %d items", len(listed.Items))
	}
	if listed.Items[0].MessageID != "msg-old-user-facing" {
		t.Fatalf("expected old user-facing message to survive filtering, got %s", listed.Items[0].MessageID)
	}
}
