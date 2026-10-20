package state

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"kingdom/internal/core/providerexecution"
	"kingdom/internal/dbmigrations"
)

func TestCreatePlanningSessionOwnsAndDeletesCanonicalTitle(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewPlanningStore(db)
	ctx := context.Background()
	const sessionID = "planning-title-owner"
	const at = "2026-10-03T12:00:00Z"

	if _, err := store.CreateSession(ctx, CreatePlanningSessionRequest{
		PlanningSessionID: sessionID, TitleSource: PlanningTextFromString("Captured title"), TitleState: "custom",
		LifecycleState: PlanningSessionLifecycleStateActive, ConversationState: PlanningSessionConversationStateReady,
		ProjectRoot: "/workspace", ModelSelectionJSON: `{}`, CreatedAt: at, UpdatedAt: at,
	}); err != nil {
		t.Fatalf("create session with canonical title: %v", err)
	}
	var contentID, owner string
	if err := db.QueryRowContext(ctx, `SELECT title_content_id FROM planning_sessions WHERE planning_session_id=?`, sessionID).Scan(&contentID); err != nil {
		t.Fatalf("load canonical title reference: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT planning_session_id FROM request_contents WHERE content_id=?`, contentID).Scan(&owner); err != nil || owner != sessionID {
		t.Fatalf("title content owner=%q err=%v", owner, err)
	}
	planRows, err := db.QueryContext(ctx, `EXPLAIN QUERY PLAN SELECT content_id FROM request_contents WHERE planning_session_id=?`, sessionID)
	if err != nil {
		t.Fatalf("explain planner content owner lookup: %v", err)
	}
	usesOwnerIndex := false
	for planRows.Next() {
		var id, parent, notUsed int
		var detail string
		if err := planRows.Scan(&id, &parent, &notUsed, &detail); err != nil {
			planRows.Close()
			t.Fatalf("scan planner content owner query plan: %v", err)
		}
		if strings.Contains(detail, "idx_request_contents_planning_session") {
			usesOwnerIndex = true
		}
	}
	if err := planRows.Err(); err != nil {
		planRows.Close()
		t.Fatalf("read planner content owner query plan: %v", err)
	}
	if err := planRows.Close(); err != nil {
		t.Fatalf("close planner content owner query plan: %v", err)
	}
	if !usesOwnerIndex {
		t.Fatal("planner content lookup does not use the session owner index")
	}
	if deleted, err := store.DeleteSession(ctx, sessionID); err != nil || !deleted {
		t.Fatalf("delete session with owned title: deleted=%t err=%v", deleted, err)
	}
	var remaining int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM request_contents WHERE content_id=?`, contentID).Scan(&remaining); err != nil || remaining != 0 {
		t.Fatalf("deleted session retained title content: count=%d err=%v", remaining, err)
	}
}

func TestDeletePlanningSessionRemovesOwnedSnapshotAndActivityData(t *testing.T) {
	db := openTestDB(t)
	applyMigrationsBeforePlanningDeleteFix(t, db)
	store := NewPlanningStore(db)
	ctx := context.Background()
	const at = "2026-10-03T12:00:00Z"

	if _, err := store.CreateSession(ctx, CreatePlanningSessionRequest{
		PlanningSessionID: "delete-owned-session", Title: "Delete owned data", TitleState: "custom",
		LifecycleState: PlanningSessionLifecycleStateActive, ConversationState: PlanningSessionConversationStateReady,
		ProjectRoot: "/workspace", ModelSelectionJSON: `{}`, CreatedAt: at, UpdatedAt: at,
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	legacyMessageID := "delete-owned-legacy-message"
	legacyMessageContentID := PlanningMessageContentID(legacyMessageID)
	legacyMessageText := "Legacy planner message"
	legacyMessageDigest := sha256.Sum256([]byte(legacyMessageText))
	legacyTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin legacy message fixture: %v", err)
	}
	if _, err := WriteRequestContentTx(ctx, legacyTx, legacyMessageContentID, RequestContentSource{
		Reader: strings.NewReader(legacyMessageText), SHA256: hex.EncodeToString(legacyMessageDigest[:]), SizeBytes: int64(len(legacyMessageText)),
	}, at); err != nil {
		_ = legacyTx.Rollback()
		t.Fatalf("write legacy message content: %v", err)
	}
	if _, err := legacyTx.ExecContext(ctx, `INSERT INTO planning_messages (planning_message_id,planning_session_id,parent_message_id,role,content_markdown,question_payload_json,answer_payload_json,created_at,content_content_id) VALUES (?,?,NULL,'user','', '{}','{}',?,?)`, legacyMessageID, "delete-owned-session", at, legacyMessageContentID); err != nil {
		_ = legacyTx.Rollback()
		t.Fatalf("insert legacy message: %v", err)
	}
	if err := legacyTx.Commit(); err != nil {
		t.Fatalf("commit legacy message fixture: %v", err)
	}
	snapshot, err := store.CreateContextSnapshot(ctx, CreatePlanningContextSnapshotRequest{
		PlanningContextSnapshotID: "delete-owned-context", PlanningSessionID: "delete-owned-session",
		SummaryMarkdown: "Captured workspace", IncludedFilesJSON: `[{"path":"README.md","size_bytes":12}]`,
		ExcludedFilesJSON: `[]`, CreatedAt: at,
	})
	if err != nil {
		t.Fatalf("create context snapshot with entries: %v", err)
	}
	if _, err := store.PublishContextSnapshot(ctx, "delete-owned-session", snapshot.PlanningContextSnapshotID, at); err != nil {
		t.Fatalf("publish context snapshot: %v", err)
	}
	operation, err := store.CreateOperation(ctx, CreatePlanningOperationRequest{
		OperationID: "delete-owned-operation", PlanningSessionID: "delete-owned-session",
		Type: PlanningOperationTypeReply, Status: PlanningOperationStatusRunning,
		RequestPayloadJSON: `{}`, LeaseOwner: "worker", LeaseExpiresAt: "2026-10-03T12:05:00Z",
		LeaseVersion: 1, CreatedAt: at,
	})
	if err != nil {
		t.Fatalf("create operation: %v", err)
	}
	if _, err := store.CreatePlanningProviderInvocation(ctx, CreatePlanningProviderInvocationRequest{
		PlanningProviderInvocationID: "delete-owned-invocation", PlanningOperationID: operation.OperationID,
		PlanningSessionID: operation.PlanningSessionID, LogicalSessionID: "planner-delete-owned-session",
		CreatedAt: at, LeaseOwner: "worker", LeaseVersion: 1,
	}); err != nil {
		t.Fatalf("create provider invocation: %v", err)
	}

	providerBytes := []byte("provider output")
	providerDigest := sha256.Sum256(providerBytes)
	providerSHA := hex.EncodeToString(providerDigest[:])
	if _, err := db.ExecContext(ctx, `INSERT INTO planning_provider_contents(planning_provider_invocation_id,sha256,size_bytes) VALUES(?,?,?)`, "delete-owned-invocation", providerSHA, len(providerBytes)); err != nil {
		t.Fatalf("insert provider content manifest: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO planning_provider_content_chunks(planning_provider_invocation_id,sha256,ordinal,content) VALUES(?,?,0,?)`, "delete-owned-invocation", providerSHA, providerBytes); err != nil {
		t.Fatalf("insert provider content chunk: %v", err)
	}
	applyMigration(t, db, "0001_initial_schema.up.sql")

	var legacyOwner string
	if err := db.QueryRowContext(ctx, `SELECT planning_session_id FROM request_contents WHERE content_id=?`, legacyMessageContentID).Scan(&legacyOwner); err != nil || legacyOwner != "delete-owned-session" {
		t.Fatalf("migration did not assign legacy planning content owner: owner=%q err=%v", legacyOwner, err)
	}
	if err := store.UpdateSession(ctx, UpdatePlanningSessionRequest{
		PlanningSessionID: "delete-owned-session", TitleContentID: "delete-owned-title",
		TitleSource: PlanningTextFromString("Deleted planner title"), UpdatedAt: at,
	}); err != nil {
		t.Fatalf("set planner title content: %v", err)
	}
	message, err := store.AppendMessage(ctx, AppendPlanningMessageRequest{
		PlanningMessageID: "delete-owned-message", PlanningSessionID: "delete-owned-session",
		Role: PlanningMessageRoleUser, ContentSource: PlanningTextFromString("User message"),
		QuestionPayloadJSON: `{}`, AnswerPayloadJSON: `{}`, CreatedAt: at,
	})
	if err != nil {
		t.Fatalf("append planner message with content: %v", err)
	}
	if _, err := store.CreateDraft(ctx, CreatePlanningDraftRequest{
		PlanningDraftID: "delete-owned-draft", PlanningSessionID: "delete-owned-session",
		WorkOrderTitle: "Draft", TextSource: PlanningTextFromString("Draft text"), SummaryJSON: `{}`, CreatedAt: at,
	}); err != nil {
		t.Fatalf("create planner draft with content: %v", err)
	}
	event, err := store.AppendOperationEvent(ctx, AppendPlanningOperationEventRequest{
		EventID: "delete-owned-activity-event", OperationID: operation.OperationID, Seq: 1,
		EventType: "generic", PayloadJSON: `{"kind":"kingdom.tool","tool_name":"read"}`,
		CreatedAt: at, ActivityContents: map[string]providerexecution.ActivityContent{
			"output": providerexecution.ActivityText("tool output"),
		},
	})
	if err != nil {
		t.Fatalf("append activity event: %v", err)
	}
	var activityContentID string
	if err := db.QueryRowContext(ctx, `SELECT content_id FROM activity_event_contents WHERE planning_event_id=? AND field='output'`, event.EventID).Scan(&activityContentID); err != nil {
		t.Fatalf("load activity content id: %v", err)
	}
	if _, err := store.AppendOperationEvent(ctx, AppendPlanningOperationEventRequest{
		EventID: "delete-owned-text-ref-event", OperationID: operation.OperationID, Seq: 2,
		EventType: "generic", PayloadJSON: `{}`, CreatedAt: at,
		TextRefs: map[string]string{"output": PlanningMessageContentID(message.PlanningMessageID)},
	}); err != nil {
		t.Fatalf("append text-ref event: %v", err)
	}

	for _, contentID := range []string{legacyMessageContentID, PlanningMessageContentID(message.PlanningMessageID), "delete-owned-title", PlanningDraftContentID("delete-owned-draft"), activityContentID} {
		var owner string
		if err := db.QueryRowContext(ctx, `SELECT planning_session_id FROM request_contents WHERE content_id=?`, contentID).Scan(&owner); err != nil || owner != "delete-owned-session" {
			t.Errorf("planner content %q owner=%q err=%v", contentID, owner, err)
		}
	}

	for _, query := range []string{
		`DELETE FROM planning_context_entries WHERE snapshot_id=?`,
		`DELETE FROM planning_provider_contents WHERE planning_provider_invocation_id=?`,
		`DELETE FROM planning_provider_content_chunks WHERE planning_provider_invocation_id=?`,
		`DELETE FROM activity_event_contents WHERE planning_event_id=?`,
		`DELETE FROM planning_event_text_refs WHERE event_id=?`,
		`DELETE FROM activity_content_chunk_digests WHERE content_id=?`,
		`DELETE FROM request_content_chunks WHERE content_id=?`,
		`DELETE FROM request_contents WHERE content_id=?`,
	} {
		args := []any{"delete-owned-context"}
		if query == `DELETE FROM planning_provider_contents WHERE planning_provider_invocation_id=?` || query == `DELETE FROM planning_provider_content_chunks WHERE planning_provider_invocation_id=?` {
			args[0] = "delete-owned-invocation"
		} else if query == `DELETE FROM activity_event_contents WHERE planning_event_id=?` {
			args[0] = event.EventID
		} else if query == `DELETE FROM planning_event_text_refs WHERE event_id=?` {
			args[0] = "delete-owned-text-ref-event"
		} else if query == `DELETE FROM activity_content_chunk_digests WHERE content_id=?` || query == `DELETE FROM request_content_chunks WHERE content_id=?` || query == `DELETE FROM request_contents WHERE content_id=?` {
			args[0] = activityContentID
		}
		if _, err := db.ExecContext(ctx, query, args...); err == nil {
			t.Fatalf("immutable planner record accepted direct deletion: %s", query)
		}
	}
	applyMigration(t, db, "0001_initial_schema.up.sql")

	deleted, err := store.DeleteSession(ctx, "delete-owned-session")
	if err != nil || !deleted {
		t.Fatalf("delete session with immutable dependents: deleted=%t err=%v", deleted, err)
	}
	for _, table := range []string{
		"planning_sessions", "planning_messages", "planning_drafts", "planning_context_snapshots", "planning_context_entries", "planning_operations",
		"planning_provider_invocations", "planning_provider_contents", "planning_provider_content_chunks",
		"planning_operation_events", "activity_event_contents", "planning_event_text_refs",
		"request_contents", "request_content_chunks", "activity_content_chunk_digests",
	} {
		var count int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&count); err != nil {
			t.Fatalf("count remaining %s: %v", table, err)
		}
		if count != 0 {
			t.Errorf("session deletion left %d rows in %s", count, table)
		}
	}
}

func TestDeletePlanningSessionKeepsContentReferencedByAnotherSession(t *testing.T) {
	db := openTestDB(t)
	applyMigrationsBeforePlanningDeleteFix(t, db)
	store := NewPlanningStore(db)
	ctx := context.Background()
	const at = "2026-10-03T12:00:00Z"

	for _, id := range []string{"content-owner-session", "content-reader-session"} {
		if _, err := store.CreateSession(ctx, CreatePlanningSessionRequest{
			PlanningSessionID: id, Title: id, TitleState: "custom",
			LifecycleState: PlanningSessionLifecycleStateActive, ConversationState: PlanningSessionConversationStateReady,
			ProjectRoot: "/workspace", ModelSelectionJSON: `{}`, CreatedAt: at, UpdatedAt: at,
		}); err != nil {
			t.Fatalf("create session %s: %v", id, err)
		}
	}
	applyMigration(t, db, "0001_initial_schema.up.sql")

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin shared content fixture: %v", err)
	}
	const sharedID = "planning-session:content-owner-session:title"
	sharedText := "content still used by another session"
	sharedDigest := sha256.Sum256([]byte(sharedText))
	if _, err := WritePlanningRequestContentTx(ctx, tx, "content-owner-session", sharedID, RequestContentSource{
		Reader: strings.NewReader(sharedText), SHA256: hex.EncodeToString(sharedDigest[:]), SizeBytes: int64(len(sharedText)),
	}, at); err != nil {
		_ = tx.Rollback()
		t.Fatalf("write shared planner content: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE planning_sessions SET title_content_id=? WHERE planning_session_id='content-owner-session'`, sharedID); err != nil {
		_ = tx.Rollback()
		t.Fatalf("bind owner session title: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE planning_sessions SET title_content_id=? WHERE planning_session_id='content-reader-session'`, sharedID); err != nil {
		_ = tx.Rollback()
		t.Fatalf("bind second session title: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit shared content fixture: %v", err)
	}

	if deleted, err := store.DeleteSession(ctx, "content-owner-session"); err == nil || deleted {
		t.Fatalf("deleted owner session despite cross-session content reference: deleted=%t err=%v", deleted, err)
	}
	var sessions, contents int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM planning_sessions`).Scan(&sessions); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM request_contents WHERE content_id=?`, sharedID).Scan(&contents); err != nil {
		t.Fatal(err)
	}
	if sessions != 2 || contents != 1 {
		t.Fatalf("failed shared-content deletion was not atomic: sessions=%d content=%d", sessions, contents)
	}
	if deleted, err := store.DeleteSession(ctx, "content-reader-session"); err != nil || !deleted {
		t.Fatalf("delete content reader session: deleted=%t err=%v", deleted, err)
	}
	if deleted, err := store.DeleteSession(ctx, "content-owner-session"); err != nil || !deleted {
		t.Fatalf("delete content owner session: deleted=%t err=%v", deleted, err)
	}
}

func applyMigrationsBeforePlanningDeleteFix(t *testing.T, db *sql.DB) {
	t.Helper()
	names, err := dbmigrations.List()
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	migrations := make([]SQLMigration, 0, len(names))
	for _, name := range names {
		if name >= "0006_planning_session_owned_cascades.up.sql" || !strings.HasSuffix(name, ".up.sql") {
			continue
		}
		raw, err := dbmigrations.Read(name)
		if err != nil {
			t.Fatalf("read migration %s: %v", name, err)
		}
		migrations = append(migrations, SQLMigration{Name: name, SQL: raw})
	}
	if err := ApplySQLMigrations(context.Background(), db, migrations, time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("apply migrations before planning deletion fix: %v", err)
	}
}
