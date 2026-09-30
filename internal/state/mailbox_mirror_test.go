package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"kingdom/internal/core/rolecomm"
)

func TestTransitionServiceMirrorsEventIntoMailbox(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	policy := loadRoleCommunicationPolicyForStateTest(t)

	svc := NewTransitionService(db, nil).WithRoleCommunicationPolicy(policy)
	req := baseTransitionRequest(EntityTypeRun, "running", "succeeded")
	req.TransitionID = "tr_mailbox_mirror_run"
	req.EventID = "evt_mailbox_mirror_run"
	req.AttemptID = "att_mailbox_mirror_run"
	req.EventSeq = 2

	if err := svc.Apply(context.Background(), req); err != nil {
		t.Fatalf("apply run transition: %v", err)
	}

	messageID := "msg_evt_evt_mailbox_mirror_run"
	body := queryMailboxBodyMarkdown(t, db, messageID)
	if !strings.Contains(body, "Recorded by\nGovernance") {
		t.Fatalf("expected governance signoff in mirrored mailbox message, got %q", body)
	}
	if !strings.Contains(body, "`state.transitioned`") {
		t.Fatalf("expected mirrored event type in mailbox body, got %q", body)
	}
	if source := queryMailboxMessageSource(t, db, messageID); source != "mirrored_event" {
		t.Fatalf("expected mirrored message source mirrored_event, got %q", source)
	}
}

func TestAttemptEventIngestorMirrorsEventIntoMailbox(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedMinimumGraph(t, db)
	policy := loadRoleCommunicationPolicyForStateTest(t)

	ingestor := NewAttemptEventIngestor(db, nil).WithRoleCommunicationPolicy(policy)
	req := baseAttemptEventIngestionRequest()
	req.EventID = "evt_mailbox_mirror_ingest"
	req.AttemptID = "att_mailbox_mirror_ingest"
	req.Seq = 2
	req.EventType = "attempt.progress"
	req.RoleID = "governance"

	if _, err := ingestor.Ingest(context.Background(), req); err != nil {
		t.Fatalf("ingest event: %v", err)
	}

	messageID := "msg_evt_evt_mailbox_mirror_ingest"
	body := queryMailboxBodyMarkdown(t, db, messageID)
	if !strings.Contains(body, "Recorded by\nGovernance") {
		t.Fatalf("expected governance signoff in mirrored mailbox message, got %q", body)
	}
}

func TestMirrorEventToMailboxUsesRoleSignoffForCoordinator(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	seedDelegationPolicyFixture(t, db)
	policy := loadRoleCommunicationPolicyForStateTest(t)

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}

	err = MirrorEventToMailbox(context.Background(), tx, policy, EventMailboxMirrorRequest{
		EventID:     "evt_mailbox_mirror_coordinator",
		RunID:       "run_1",
		AttemptID:   "att_mailbox_mirror_coordinator",
		EventType:   "approval.recorded",
		PayloadJSON: `{"decision":"approve"}`,
		RoleID:      "coordinator",
		ReasonCode:  "internal_error",
		ReasonText:  "council review complete",
		CreatedAt:   "2026-03-02T21:00:00Z",
	})
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("mirror mailbox event: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit mirror mailbox transaction: %v", err)
	}

	body := queryMailboxBodyMarkdown(t, db, "msg_evt_evt_mailbox_mirror_coordinator")
	if !strings.Contains(body, "Recorded by\nCoordinator") {
		t.Fatalf("expected coordinator signoff in mirrored mailbox message, got %q", body)
	}
}

func queryMailboxBodyMarkdown(t *testing.T, db *sql.DB, messageID string) string {
	t.Helper()

	var payloadRaw string
	if err := db.QueryRow(`SELECT payload_json FROM mailbox_messages WHERE message_id = ?`, messageID).Scan(&payloadRaw); err != nil {
		t.Fatalf("query mailbox payload for %s: %v", messageID, err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadRaw), &payload); err != nil {
		t.Fatalf("decode mailbox payload json for %s: %v", messageID, err)
	}

	body, _ := payload["body_markdown"].(string)
	if strings.TrimSpace(body) == "" {
		t.Fatalf("expected body_markdown for %s", messageID)
	}
	return body
}

func queryMailboxMessageSource(t *testing.T, db *sql.DB, messageID string) string {
	t.Helper()

	var payloadRaw string
	if err := db.QueryRow(`SELECT payload_json FROM mailbox_messages WHERE message_id = ?`, messageID).Scan(&payloadRaw); err != nil {
		t.Fatalf("query mailbox payload for %s: %v", messageID, err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(payloadRaw), &payload); err != nil {
		t.Fatalf("decode mailbox payload json for %s: %v", messageID, err)
	}

	source, _ := payload["message_source"].(string)
	return strings.TrimSpace(strings.ToLower(source))
}

func loadRoleCommunicationPolicyForStateTest(t *testing.T) rolecomm.Policy {
	t.Helper()

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve caller")
	}
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))

	policy, err := rolecomm.LoadPolicyFromFile(
		filepath.Join(repoRoot, "policies", "role_communication_policy.yaml"),
		filepath.Join(repoRoot, "contracts", "schemas", "role_communication_policy.schema.json"),
		CanonicalRoleIDs(),
	)
	if err != nil {
		t.Fatalf("load role communication policy: %v", err)
	}

	return policy
}
