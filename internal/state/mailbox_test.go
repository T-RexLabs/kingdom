package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	coreschemas "kingdom/internal/core/schemas"
)

func TestMailboxStoreSendListAndMarkReadFlow(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedDelegationPolicyFixture(t, db)

	store := NewMailboxStore(db, loadStateTestSchemas(t))

	firstMessage, err := store.SendMessage(context.Background(), SendMailboxMessageRequest{
		RunID:       "run_1",
		FromAgentID: "agent_council",
		ToAgentID:   "agent_chancellor",
		ThreadID:    "thread_run_1_governance",
		MessageType: MailboxMessageTypeClarification,
		Priority:    MailboxPriorityNormal,
		PayloadJSON: `{"question":"Need approval context"}`,
		CreatedAt:   "2026-03-01T16:00:00Z",
	})
	if err != nil {
		t.Fatalf("send first mailbox message: %v", err)
	}

	secondMessage, err := store.SendMessage(context.Background(), SendMailboxMessageRequest{
		RunID:       "run_1",
		FromAgentID: "agent_chancellor",
		ToAgentID:   "agent_council",
		ThreadID:    "thread_run_1_governance",
		MessageType: MailboxMessageTypeStatus,
		Priority:    MailboxPriorityHigh,
		PayloadJSON: `{"status":"reviewing"}`,
		CreatedAt:   "2026-03-01T16:01:00Z",
	})
	if err != nil {
		t.Fatalf("send second mailbox message: %v", err)
	}

	messages, err := store.ListMessages(context.Background(), ListMailboxMessagesRequest{
		RunID:    "run_1",
		ThreadID: "thread_run_1_governance",
		Limit:    10,
	})
	if err != nil {
		t.Fatalf("list mailbox messages: %v", err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(messages))
	}
	if messages[0].MessageID != secondMessage.MessageID {
		t.Fatalf("expected latest message id %q, got %q", secondMessage.MessageID, messages[0].MessageID)
	}
	if messages[1].MessageID != firstMessage.MessageID {
		t.Fatalf("expected older message id %q, got %q", firstMessage.MessageID, messages[1].MessageID)
	}

	if err := store.MarkRead(context.Background(), MarkMailboxMessageReadRequest{
		RunID:     "run_1",
		MessageID: firstMessage.MessageID,
		ReadAt:    "2026-03-01T16:05:00Z",
	}); err != nil {
		t.Fatalf("mark mailbox message read: %v", err)
	}

	gotReadAt := queryMailboxReadAt(t, db, firstMessage.MessageID)
	if !gotReadAt.Valid || gotReadAt.String != "2026-03-01T16:05:00Z" {
		t.Fatalf("expected read_at to be set, got %+v", gotReadAt)
	}
}

func TestMailboxStoreRejectsUnsupportedMessageTypeAndPriority(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedDelegationPolicyFixture(t, db)

	store := NewMailboxStore(db, loadStateTestSchemas(t))

	_, err := store.SendMessage(context.Background(), SendMailboxMessageRequest{
		RunID:       "run_1",
		FromAgentID: "agent_council",
		ToAgentID:   "agent_chancellor",
		ThreadID:    "thread_run_1_governance",
		MessageType: MailboxMessageType("policy_override"),
		Priority:    MailboxPriority("urgent"),
		PayloadJSON: `{"status":"invalid"}`,
		CreatedAt:   "2026-03-01T16:00:00Z",
	})
	if err == nil {
		t.Fatal("expected validation error for unsupported mailbox message type and priority")
	}

	var invalidErr *InvalidMailboxMessageError
	if !errors.As(err, &invalidErr) {
		t.Fatalf("expected InvalidMailboxMessageError, got %T (%v)", err, err)
	}
}

func TestMailboxStoreRecordsThinMailboxEventProjection(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedDelegationPolicyFixture(t, db)

	store := NewMailboxStore(db, loadStateTestSchemas(t))
	message, err := store.SendMessage(context.Background(), SendMailboxMessageRequest{
		RunID:       "run_1",
		FromAgentID: "agent_council",
		ToAgentID:   "agent_chancellor",
		ThreadID:    "thread_run_1_mailbox_projection",
		MessageType: MailboxMessageTypeStatus,
		Priority:    MailboxPriorityNormal,
		PayloadJSON: `{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","node_run_id":"nr_s4","reason_code":"execution_completed","reason_text":"execution completed","instruction_full":"draft plan","response_full":"full response body","response_raw":{"verdict":"PASS"}}`,
		CreatedAt:   "2026-03-01T16:10:00Z",
	})
	if err != nil {
		t.Fatalf("send mailbox message: %v", err)
	}

	var payloadJSON string
	if err := db.QueryRow(`SELECT payload_json FROM events WHERE event_id = ?`, "evt_mailbox_"+message.MessageID).Scan(&payloadJSON); err != nil {
		t.Fatalf("query mailbox event payload: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode mailbox event payload: %v", err)
	}
	if _, ok := payload["instruction_full"]; ok {
		t.Fatalf("expected mailbox event payload to omit instruction_full, got %s", payloadJSON)
	}
	if _, ok := payload["response_full"]; ok {
		t.Fatalf("expected mailbox event payload to omit response_full, got %s", payloadJSON)
	}
	if _, ok := payload["response_raw"]; ok {
		t.Fatalf("expected mailbox event payload to omit response_raw, got %s", payloadJSON)
	}
	if payload["message_source"] != "communication" {
		t.Fatalf("expected message_source=communication, got %#v", payload["message_source"])
	}
	if payload["communication_type"] != "response_received" {
		t.Fatalf("expected communication_type=response_received, got %#v", payload["communication_type"])
	}
	if payload["visible_to_operator"] != true {
		t.Fatalf("expected visible_to_operator=true, got %#v", payload["visible_to_operator"])
	}
}

func TestMailboxStoreSendMessageUpsertsExistingMessagePayload(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedDelegationPolicyFixture(t, db)

	store := NewMailboxStore(db, loadStateTestSchemas(t))
	messageID := "msg_mailbox_upsert"
	if _, err := store.SendMessage(context.Background(), SendMailboxMessageRequest{
		MessageID:   messageID,
		RunID:       "run_1",
		FromAgentID: "agent_council",
		ToAgentID:   "agent_chancellor",
		ThreadID:    "thread_run_1_mailbox_upsert",
		MessageType: MailboxMessageTypeStatus,
		Priority:    MailboxPriorityNormal,
		PayloadJSON: `{"message_source":"communication","communication_type":"response_received","node_id":"validation_gates_qa_scout","reason_code":"validation_gate_rework","response_full":"validator requested rework"}`,
		CreatedAt:   "2026-03-01T16:20:00Z",
	}); err != nil {
		t.Fatalf("send initial mailbox message: %v", err)
	}
	if _, err := store.SendMessage(context.Background(), SendMailboxMessageRequest{
		MessageID:   messageID,
		RunID:       "run_1",
		FromAgentID: "agent_council",
		ToAgentID:   "agent_chancellor",
		ThreadID:    "thread_run_1_mailbox_upsert",
		MessageType: MailboxMessageTypeStatus,
		Priority:    MailboxPriorityNormal,
		PayloadJSON: `{"message_source":"communication","communication_type":"response_received","node_id":"validation_gates_qa_scout","reason_code":"validation_gate_cap_auto_succeeded","response_full":"validation cap reached; auto-succeeded"}`,
		CreatedAt:   "2026-03-01T16:21:00Z",
	}); err != nil {
		t.Fatalf("send replacement mailbox message: %v", err)
	}

	var payloadJSON string
	if err := db.QueryRow(`SELECT payload_json FROM mailbox_messages WHERE message_id = ?`, messageID).Scan(&payloadJSON); err != nil {
		t.Fatalf("query mailbox payload: %v", err)
	}
	if !strings.Contains(payloadJSON, `"validation_gate_cap_auto_succeeded"`) {
		t.Fatalf("expected mailbox payload to be replaced with latest canonical reason code, got %s", payloadJSON)
	}

	var eventCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE event_type = 'mailbox.message_recorded' AND json_extract(payload_json, '$.message_id') = ?`, messageID).Scan(&eventCount); err != nil {
		t.Fatalf("count mailbox events: %v", err)
	}
	if eventCount != 2 {
		t.Fatalf("expected updated mailbox payload to append a second mailbox event revision, got %d rows", eventCount)
	}

	var newestEventID string
	if err := db.QueryRow(`SELECT event_id FROM events WHERE event_type = 'mailbox.message_recorded' AND json_extract(payload_json, '$.message_id') = ? ORDER BY run_event_seq DESC LIMIT 1`, messageID).Scan(&newestEventID); err != nil {
		t.Fatalf("query latest mailbox event id: %v", err)
	}
	if newestEventID != "evt_mailbox_"+messageID+"_v2" {
		t.Fatalf("expected latest mailbox event revision id, got %q", newestEventID)
	}

	if _, err := store.SendMessage(context.Background(), SendMailboxMessageRequest{
		MessageID:   messageID,
		RunID:       "run_1",
		FromAgentID: "agent_council",
		ToAgentID:   "agent_chancellor",
		ThreadID:    "thread_run_1_mailbox_upsert",
		MessageType: MailboxMessageTypeStatus,
		Priority:    MailboxPriorityNormal,
		PayloadJSON: `{"message_source":"communication","communication_type":"response_received","node_id":"validation_gates_qa_scout","reason_code":"validation_gate_cap_auto_succeeded","response_full":"validation cap reached; auto-succeeded"}`,
		CreatedAt:   "2026-03-01T16:22:00Z",
	}); err != nil {
		t.Fatalf("send identical mailbox message replay: %v", err)
	}

	if err := db.QueryRow(`SELECT COUNT(1) FROM events WHERE event_type = 'mailbox.message_recorded' AND json_extract(payload_json, '$.message_id') = ?`, messageID).Scan(&eventCount); err != nil {
		t.Fatalf("count mailbox events after identical replay: %v", err)
	}
	if eventCount != 2 {
		t.Fatalf("expected unchanged mailbox replay to no-op, got %d mailbox event revisions", eventCount)
	}
}

func TestMailboxStoreUpdatedLogicalMessageBecomesLatestInMailboxList(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedDelegationPolicyFixture(t, db)

	store := NewMailboxStore(db, loadStateTestSchemas(t))
	if _, err := store.SendMessage(context.Background(), SendMailboxMessageRequest{
		MessageID:   "msg_mailbox_reordered",
		RunID:       "run_1",
		FromAgentID: "agent_council",
		ToAgentID:   "agent_chancellor",
		ThreadID:    "thread_run_1_mailbox_reordered",
		MessageType: MailboxMessageTypeStatus,
		Priority:    MailboxPriorityNormal,
		PayloadJSON: `{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","reason_code":"execution_completed","response_full":"Initial plan message"}`,
		CreatedAt:   "2026-03-01T16:20:00Z",
	}); err != nil {
		t.Fatalf("send original logical mailbox message: %v", err)
	}
	otherMessage, err := store.SendMessage(context.Background(), SendMailboxMessageRequest{
		MessageID:   "msg_mailbox_other",
		RunID:       "run_1",
		FromAgentID: "agent_chancellor",
		ToAgentID:   "agent_council",
		ThreadID:    "thread_run_1_mailbox_other",
		MessageType: MailboxMessageTypeStatus,
		Priority:    MailboxPriorityNormal,
		PayloadJSON: `{"message_source":"communication","communication_type":"response_received","node_id":"advisory_synthesis","reason_code":"execution_completed","response_full":"Other message"}`,
		CreatedAt:   "2026-03-01T16:21:00Z",
	})
	if err != nil {
		t.Fatalf("send second mailbox message: %v", err)
	}
	if _, err := store.SendMessage(context.Background(), SendMailboxMessageRequest{
		MessageID:   "msg_mailbox_reordered",
		RunID:       "run_1",
		FromAgentID: "agent_council",
		ToAgentID:   "agent_chancellor",
		ThreadID:    "thread_run_1_mailbox_reordered",
		MessageType: MailboxMessageTypeStatus,
		Priority:    MailboxPriorityNormal,
		PayloadJSON: `{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","reason_code":"plan_rework","response_full":"Updated plan message"}`,
		CreatedAt:   "2026-03-01T16:22:00Z",
	}); err != nil {
		t.Fatalf("send updated logical mailbox message: %v", err)
	}

	messages, err := store.ListMessages(context.Background(), ListMailboxMessagesRequest{RunID: "run_1", Limit: 10})
	if err != nil {
		t.Fatalf("list mailbox messages: %v", err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected 2 logical mailbox messages, got %d", len(messages))
	}
	if messages[0].MessageID != "msg_mailbox_reordered" {
		t.Fatalf("expected revised logical message to become latest in list, got %q", messages[0].MessageID)
	}
	if messages[1].MessageID != otherMessage.MessageID {
		t.Fatalf("expected unchanged other message second, got %q", messages[1].MessageID)
	}
}

func TestMailboxStoreHidesSuppressedPlanningMessagesFromOperatorProjection(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedDelegationPolicyFixture(t, db)

	store := NewMailboxStore(db, loadStateTestSchemas(t))
	message, err := store.SendMessage(context.Background(), SendMailboxMessageRequest{
		RunID:       "run_1",
		FromAgentID: "agent_council",
		ToAgentID:   "agent_king",
		ThreadID:    "thread_run_1_plan_suppressed",
		MessageType: MailboxMessageTypeStatus,
		Priority:    MailboxPriorityNormal,
		PayloadJSON: `{"message_source":"communication","communication_type":"response_received","node_id":"plan_drafting","reason_code":"execution_completed","reason_text":"planning response recorded","response_full":"Planning response"}`,
		CreatedAt:   "2026-03-01T16:12:00Z",
	})
	if err != nil {
		t.Fatalf("send mailbox message: %v", err)
	}

	var payloadJSON string
	if err := db.QueryRow(`SELECT payload_json FROM events WHERE event_id = ?`, "evt_mailbox_"+message.MessageID).Scan(&payloadJSON); err != nil {
		t.Fatalf("query mailbox event payload: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadJSON), &payload); err != nil {
		t.Fatalf("decode mailbox event payload: %v", err)
	}
	if payload["visible_to_operator"] != true {
		t.Fatalf("expected visible_to_operator=true for planning response, got %#v", payload["visible_to_operator"])
	}
}

func TestMailboxStoreRejectsMailboxEventEnvelopeThatViolatesSchema(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedDelegationPolicyFixture(t, db)

	store := NewMailboxStore(db, loadMailboxSchemaSetWithoutMailboxEvents(t))
	_, err := store.SendMessage(context.Background(), SendMailboxMessageRequest{
		MessageID:   "msg_invalid_contract",
		RunID:       "run_1",
		FromAgentID: "agent_council",
		ToAgentID:   "agent_chancellor",
		ThreadID:    "thread_run_1_mailbox_projection",
		MessageType: MailboxMessageTypeStatus,
		Priority:    MailboxPriorityNormal,
		PayloadJSON: `{"message_source":"communication","communication_type":"response_received"}`,
		CreatedAt:   "2026-03-01T16:10:00Z",
	})
	if err == nil {
		t.Fatal("expected event contract validation failure")
	}

	var validationErr *EventContractValidationError
	if !errors.As(err, &validationErr) {
		t.Fatalf("expected EventContractValidationError, got %T: %v", err, err)
	}

	var mailboxRows int
	if err := db.QueryRow(`SELECT COUNT(1) FROM mailbox_messages`).Scan(&mailboxRows); err != nil {
		t.Fatalf("count mailbox messages: %v", err)
	}
	if mailboxRows != 0 {
		t.Fatalf("expected mailbox message rollback after contract failure, got %d persisted rows", mailboxRows)
	}

	if got := queryEventExists(t, db, "evt_mailbox_msg_invalid_contract"); got {
		t.Fatal("mailbox event should not have been inserted after contract failure")
	}
}

func TestDelegationHardFailWritesEscalationMailboxMessage(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedDelegationPolicyFixture(t, db)

	mailbox := NewMailboxStore(db, loadStateTestSchemas(t))
	engine := NewDelegationPolicyEngineWithMailbox(db, NewAgentRegistry(db), mailbox)

	decision, err := engine.Delegate(context.Background(), DelegationRequest{
		RunID:            "run_1",
		NodeID:           "plan_drafting",
		DelegatorAgentID: "agent_noble_existing",
		DelegateAgentID:  "agent_council_2",
		DelegateRoleID:   "council-member",
		MaxSpawnDepth:    3,
		OccurredAt:       "2026-03-01T17:00:00Z",
	})
	if err != nil {
		t.Fatalf("delegate expected hard-fail with mailbox escalation write: %v", err)
	}
	if decision.Outcome != DelegationOutcomeHardFail {
		t.Fatalf("expected hard-fail decision, got %+v", decision)
	}

	messages, err := mailbox.ListMessages(context.Background(), ListMailboxMessagesRequest{
		RunID:     "run_1",
		ThreadID:  "thread_run_1_delegation_plan_drafting",
		ToAgentID: "agent_chancellor",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("list escalation mailbox messages: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected exactly one escalation message, got %d", len(messages))
	}
	if messages[0].MessageType != MailboxMessageTypeEscalation {
		t.Fatalf("expected escalation message type, got %q", messages[0].MessageType)
	}
	if messages[0].Priority != MailboxPriorityHigh {
		t.Fatalf("expected high priority, got %q", messages[0].Priority)
	}
}

func queryMailboxReadAt(t *testing.T, db *sql.DB, messageID string) sql.NullString {
	t.Helper()

	var readAt sql.NullString
	err := db.QueryRow(`SELECT read_at FROM mailbox_messages WHERE message_id = ?`, messageID).Scan(&readAt)
	if err != nil {
		t.Fatalf("query mailbox read_at: %v", err)
	}
	return readAt
}

func loadMailboxSchemaSetWithoutMailboxEvents(t *testing.T) *coreschemas.Set {
	t.Helper()

	schemaDir := filepath.Join("..", "..", "contracts", "schemas")
	tempDir := t.TempDir()
	entries, err := os.ReadDir(schemaDir)
	if err != nil {
		t.Fatalf("read schema dir: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		sourcePath := filepath.Join(schemaDir, entry.Name())
		raw, readErr := os.ReadFile(sourcePath)
		if readErr != nil {
			t.Fatalf("read schema file %q: %v", entry.Name(), readErr)
		}
		contents := raw
		if entry.Name() == "event_v1.schema.json" {
			contents = []byte(strings.ReplaceAll(string(raw), `,
        "mailbox.message_recorded"`, ""))
		}
		if writeErr := os.WriteFile(filepath.Join(tempDir, entry.Name()), contents, 0o644); writeErr != nil {
			t.Fatalf("write temp schema file %q: %v", entry.Name(), writeErr)
		}
	}

	schemaSet, err := coreschemas.Load(tempDir)
	if err != nil {
		t.Fatalf("load temp schema set: %v", err)
	}
	return schemaSet
}
