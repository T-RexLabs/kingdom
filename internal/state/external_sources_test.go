package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestExternalSourceCaptureDeduplicatesAndPreservesImmutableDecreeProvenance(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	ctx := context.Background()
	at := "2026-08-12T10:00:00Z"
	_, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-1", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", InstallationID: "2", CredentialHandle: "github-app:conn-1", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-1"})
	if err != nil {
		t.Fatal(err)
	}
	base := CaptureExternalIssueRequest{SourceID: "source-1", SnapshotID: "snapshot-1", ConnectionID: "conn-1", ProviderNativeID: "100", RepositoryNativeID: "200", Issue: NormalizedExternalIssue{Title: "Issue", Body: "body", State: "open", Repository: "owner/repo", IssueNumber: 7, CanonicalURL: "https://github.com/owner/repo/issues/7", CreatedAt: at, UpdatedAt: at}, Profile: ExternalImportProfile{IncludeComments: true}, ProviderUpdatedAt: at, FetchedAt: at, Comments: []ExternalComment{{ProviderCommentID: "comment-1", Author: "octocat", Body: "immutable comment", CanonicalURL: "https://github.com/owner/repo/issues/7#issuecomment-1", CreatedAt: at, UpdatedAt: at}}}
	first, created, err := store.CaptureIssue(ctx, base)
	if err != nil || !created {
		t.Fatalf("first capture: created=%v err=%v", created, err)
	}
	base.SourceID = "source-ignored"
	base.SnapshotID = "snapshot-ignored"
	second, created, err := store.CaptureIssue(ctx, base)
	if err != nil || created {
		t.Fatalf("duplicate capture: created=%v err=%v", created, err)
	}
	if second.SourceID != first.SourceID || second.SnapshotID != first.SnapshotID {
		t.Fatalf("capture did not deduplicate: %#v %#v", first, second)
	}
	for i, id := range []string{"decree-1", "decree-2"} {
		if err = store.CreateDecree(ctx, CreateDecreeRequest{DecreeID: id, Title: "D", Text: "immutable", CreatedAt: at, DirectSnapshotID: first.SnapshotID, NewLinkID: func() string { return "link-" + id }, ActorID: "local-operator"}); err != nil {
			t.Fatal(err)
		}
		_ = i
	}
	var sourceCount, linkCount int
	_ = db.QueryRow(`SELECT COUNT(*) FROM external_sources`).Scan(&sourceCount)
	_ = db.QueryRow(`SELECT COUNT(*) FROM external_source_links`).Scan(&linkCount)
	if sourceCount != 1 || linkCount != 2 {
		t.Fatalf("sources=%d links=%d", sourceCount, linkCount)
	}
	if _, err = db.Exec(`UPDATE external_source_snapshots SET normalized_issue_json='{}' WHERE snapshot_id=?`, first.SnapshotID); err == nil || !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("expected immutable trigger, got %v", err)
	}
	if _, err = db.Exec(`UPDATE external_source_snapshot_comments SET body='changed' WHERE snapshot_id=?`, first.SnapshotID); err == nil || !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("expected immutable comment trigger, got %v", err)
	}
}

func TestCreateDecreeExternalLinkRollsBackOnUnknownSnapshot(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	err := store.CreateDecree(context.Background(), CreateDecreeRequest{DecreeID: "decree-x", Title: "D", Text: "T", CreatedAt: "2026-08-12T10:00:00Z", DirectSnapshotID: "missing", NewLinkID: func() string { return "link-x" }, ActorID: "local-operator"})
	if !errors.Is(err, ErrExternalNotFound) {
		t.Fatalf("expected external not found, got %v", err)
	}
	var count int
	_ = db.QueryRow(`SELECT COUNT(*) FROM decrees WHERE decree_id='decree-x'`).Scan(&count)
	if count != 0 {
		t.Fatal("decree escaped failed transaction")
	}
}

func TestCreateDecreeDerivesExactFormalizeBindingsAtomically(t *testing.T) {
	fixture := newDecreeLineageFixture(t)
	linkIndex := 0
	err := fixture.store.CreateDecree(context.Background(), CreateDecreeRequest{
		DecreeID: "decree-planned", Title: "Planned", Text: "Exact draft", CreatedAt: fixture.at,
		Metadata: map[string]any{
			"priority":                     "high",
			"planning_session_id":          "untrusted-session",
			"planning_draft_id":            "untrusted-draft",
			"planning_context_snapshot_id": "untrusted-context",
		},
		PlanningLineage: &DecreePlanningLineage{PlanningSessionID: "session-a", PlanningDraftID: "draft-a"},
		NewLinkID: func() string {
			linkIndex++
			return fmt.Sprintf("decree-link-%d", linkIndex)
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	rows, err := fixture.db.Query(`SELECT used_snapshot_id FROM external_source_links WHERE target_kind='decree' AND target_id='decree-planned' ORDER BY used_snapshot_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var snapshots []string
	for rows.Next() {
		var snapshotID string
		if err := rows.Scan(&snapshotID); err != nil {
			t.Fatal(err)
		}
		snapshots = append(snapshots, snapshotID)
	}
	if got, want := strings.Join(snapshots, ","), "snapshot-a,snapshot-b"; got != want {
		t.Fatalf("decree snapshots=%q, want %q", got, want)
	}
	links, err := fixture.store.GetLinksForTarget(context.Background(), "decree", "decree-planned")
	if err != nil {
		t.Fatal(err)
	}
	if len(links) != 2 || links[0].LinkID != "decree-link-1" || links[0].UsedSnapshotID != "snapshot-a" || links[1].LinkID != "decree-link-2" || links[1].UsedSnapshotID != "snapshot-b" {
		t.Fatalf("deterministic decree links=%#v", links)
	}

	var metadataJSON string
	if err := fixture.db.QueryRow(`SELECT metadata_json FROM decrees WHERE decree_id='decree-planned'`).Scan(&metadataJSON); err != nil {
		t.Fatal(err)
	}
	var metadata map[string]any
	if err := json.Unmarshal([]byte(metadataJSON), &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata["priority"] != "high" || metadata["planning_session_id"] != "session-a" || metadata["planning_draft_id"] != "draft-a" || metadata["planning_context_snapshot_id"] != "context-a" {
		t.Fatalf("validated metadata lineage was not persisted: %#v", metadata)
	}
}

func TestCreateDecreeRejectsInvalidPlanningLineageWithoutWrites(t *testing.T) {
	tests := []struct {
		name    string
		lineage DecreePlanningLineage
	}{
		{name: "cross-session draft", lineage: DecreePlanningLineage{PlanningSessionID: "session-a", PlanningDraftID: "draft-b"}},
		{name: "formalize missing context binding", lineage: DecreePlanningLineage{PlanningSessionID: "session-a", PlanningDraftID: "draft-unbound"}},
		{name: "failed formalize", lineage: DecreePlanningLineage{PlanningSessionID: "session-a", PlanningDraftID: "draft-failed"}},
		{name: "non-formalize operation", lineage: DecreePlanningLineage{PlanningSessionID: "session-a", PlanningDraftID: "draft-reply"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newDecreeLineageFixture(t)
			err := fixture.store.CreateDecree(context.Background(), CreateDecreeRequest{DecreeID: "decree-invalid", Title: "D", Text: "T", CreatedAt: fixture.at, PlanningLineage: &test.lineage, NewLinkID: func() string { return "unused-link" }})
			if !errors.Is(err, ErrDecreeLineageInvalid) {
				t.Fatalf("expected invalid lineage, got %v", err)
			}
			assertDecreeProvenanceWriteCount(t, fixture.db, "decree-invalid", 0, 0)
		})
	}
}

func TestCreateDecreeRejectsPlanningAndDirectSnapshotWithoutWrites(t *testing.T) {
	fixture := newDecreeLineageFixture(t)
	err := fixture.store.CreateDecree(context.Background(), CreateDecreeRequest{DecreeID: "decree-mixed", Title: "D", Text: "T", CreatedAt: fixture.at, DirectSnapshotID: "snapshot-a", PlanningLineage: &DecreePlanningLineage{PlanningSessionID: "session-a", PlanningDraftID: "draft-a"}, NewLinkID: func() string { return "unused-link" }})
	if !errors.Is(err, ErrDecreeLineageInvalid) {
		t.Fatalf("expected invalid lineage, got %v", err)
	}
	assertDecreeProvenanceWriteCount(t, fixture.db, "decree-mixed", 0, 0)
}

func TestCreateDecreeRejectsDuplicateSourceBindingsWithoutWrites(t *testing.T) {
	fixture := newDecreeLineageFixture(t)
	if _, err := fixture.db.Exec(`INSERT INTO planning_operation_external_snapshots (operation_id,snapshot_id,assembler_version,context_digest,issue_bytes,comment_bytes,bound_at) VALUES ('operation-good','snapshot-a-later',?, ?,0,0,?)`, ExternalAssemblerVersion, strings.Repeat("0", 64), fixture.at); err != nil {
		t.Fatal(err)
	}
	err := fixture.store.CreateDecree(context.Background(), CreateDecreeRequest{DecreeID: "decree-conflict", Title: "D", Text: "T", CreatedAt: fixture.at, PlanningLineage: &DecreePlanningLineage{PlanningSessionID: "session-a", PlanningDraftID: "draft-a"}, NewLinkID: func() string { return "unused-link" }})
	if !errors.Is(err, ErrDecreeProvenanceConflict) {
		t.Fatalf("expected provenance conflict, got %v", err)
	}
	assertDecreeProvenanceWriteCount(t, fixture.db, "decree-conflict", 0, 0)
}

type decreeLineageFixture struct {
	db    *sql.DB
	store *ExternalStore
	at    string
}

func newDecreeLineageFixture(t *testing.T) decreeLineageFixture {
	t.Helper()
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	ctx := context.Background()
	at := "2026-08-12T10:00:00Z"
	if _, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-lineage", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", InstallationID: "2", CredentialHandle: "github-app:conn-lineage", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-lineage"}); err != nil {
		t.Fatal(err)
	}
	capture := func(sourceID, snapshotID, nativeID, title, predecessor, fetchedAt string) ExternalSourceSnapshot {
		t.Helper()
		snapshot, _, err := store.CaptureIssue(ctx, CaptureExternalIssueRequest{SourceID: sourceID, SnapshotID: snapshotID, ConnectionID: "conn-lineage", ProviderNativeID: nativeID, RepositoryNativeID: "repo-" + nativeID, PredecessorSnapshotID: predecessor, Issue: NormalizedExternalIssue{Title: title, Body: "body", State: "open", Repository: "owner/repo", IssueNumber: 7, CanonicalURL: "https://github.com/owner/repo/issues/7", CreatedAt: at, UpdatedAt: fetchedAt}, ProviderUpdatedAt: fetchedAt, FetchedAt: fetchedAt})
		if err != nil {
			t.Fatal(err)
		}
		return snapshot
	}
	snapshotA := capture("source-a", "snapshot-a", "100", "A", "", at)
	snapshotALater := capture("ignored-source", "snapshot-a-later", "100", "A later", snapshotA.SnapshotID, "2026-08-12T11:00:00Z")
	snapshotB := capture("source-b", "snapshot-b", "101", "B", "", at)
	for _, sessionID := range []string{"session-a", "session-b"} {
		mustExecArgs(t, db, `INSERT INTO planning_sessions (planning_session_id,title,title_state,lifecycle_state,conversation_state,project_root,model_selection_json,created_at,updated_at) VALUES (?,?,'custom','active','ready','/tmp','{}',?,?)`, sessionID, sessionID, at, at)
	}
	for _, draft := range []struct{ id, session string }{{"draft-a", "session-a"}, {"draft-b", "session-b"}, {"draft-failed", "session-a"}, {"draft-reply", "session-a"}, {"draft-unbound", "session-a"}} {
		mustExecArgs(t, db, `INSERT INTO planning_drafts (planning_draft_id,planning_session_id,decree_title,decree_text,summary_json,created_at) VALUES (?,?,?,?,'{}',?)`, draft.id, draft.session, draft.id, "text", at)
	}
	for _, contextSnapshot := range []struct{ id, session string }{{"context-a", "session-a"}, {"context-b", "session-b"}} {
		mustExecArgs(t, db, `INSERT INTO planning_context_snapshots (planning_context_snapshot_id,planning_session_id,summary_markdown,included_files_json,excluded_files_json,created_at) VALUES (?,?,?,'[]','[]',?)`, contextSnapshot.id, contextSnapshot.session, "context", at)
	}
	mustExecArgs(t, db, `INSERT INTO planning_context_snapshots (planning_context_snapshot_id,planning_session_id,summary_markdown,included_files_json,excluded_files_json,created_at) VALUES ('context-later','session-a','later context','[]','[]','2026-08-12T12:00:00Z')`)
	mustExec(t, db, `UPDATE planning_sessions SET context_snapshot_id='context-later' WHERE planning_session_id='session-a'`)
	for _, operation := range []struct{ id, kind, status, result, context string }{{"operation-good", "formalize", "succeeded", "draft-a", "context-a"}, {"operation-failed", "formalize", "failed", "draft-failed", "context-a"}, {"operation-reply", "reply", "succeeded", "draft-reply", "context-a"}, {"operation-unbound", "formalize", "succeeded", "draft-unbound", ""}} {
		mustExecArgs(t, db, `INSERT INTO planning_operations (operation_id,acp_run_id,planning_session_id,planning_context_snapshot_id,type,status,request_payload_json,lease_owner,lease_version,observed_output_text,result_ref,created_at) VALUES (?,?, 'session-a',NULLIF(?,''),?,?, '{}','',0,'',?,?)`, operation.id, operation.id, operation.context, operation.kind, operation.status, operation.result, at)
	}
	for _, snapshotID := range []string{snapshotA.SnapshotID, snapshotB.SnapshotID} {
		mustExecArgs(t, db, `INSERT INTO planning_operation_external_snapshots (operation_id,snapshot_id,assembler_version,context_digest,issue_bytes,comment_bytes,bound_at) VALUES ('operation-good',?,?,?,0,0,?)`, snapshotID, ExternalAssemblerVersion, strings.Repeat("0", 64), at)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := createExternalLinkTx(ctx, tx, "planning-link", "planning_session", "session-a", snapshotA.SourceID, snapshotA.SnapshotID, "local-operator", "adoption-a", at); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	mustExecArgs(t, db, `INSERT INTO planning_external_source_adoptions (adoption_id,link_id,planning_session_id,snapshot_id,actor_id,adopted_at) VALUES ('adoption-later','planning-link','session-a',?,'local-operator','2026-08-12T11:00:00Z')`, snapshotALater.SnapshotID)
	return decreeLineageFixture{db: db, store: store, at: at}
}

func mustExecArgs(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatal(err)
	}
}

func assertDecreeProvenanceWriteCount(t *testing.T, db *sql.DB, decreeID string, wantDecrees, wantLinks int) {
	t.Helper()
	var decrees, links int
	if err := db.QueryRow(`SELECT COUNT(*) FROM decrees WHERE decree_id=?`, decreeID).Scan(&decrees); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM external_source_links WHERE target_kind='decree' AND target_id=?`, decreeID).Scan(&links); err != nil {
		t.Fatal(err)
	}
	if decrees != wantDecrees || links != wantLinks {
		t.Fatalf("writes for %s: decrees=%d links=%d, want %d/%d", decreeID, decrees, links, wantDecrees, wantLinks)
	}
}

func TestExternalCommentBounds(t *testing.T) {
	comments := make([]ExternalComment, 105)
	for i := range comments {
		comments[i] = ExternalComment{ProviderCommentID: string(rune(i + 1)), Body: strings.Repeat("x", MaxExternalCommentBytes+10)}
	}
	retained, total, truncated := boundExternalComments(comments)
	if !truncated || len(retained) > MaxExternalComments || total > MaxExternalCommentsBytes {
		t.Fatalf("retained=%d total=%d truncated=%v", len(retained), total, truncated)
	}
}

func TestExternalCommentBoundsPreserveUTF8(t *testing.T) {
	comments := []ExternalComment{{Body: strings.Repeat("x", MaxExternalCommentBytes-1) + "界"}}
	retained, _, truncated := boundExternalComments(comments)
	if !truncated || len(retained) != 1 || !utf8.ValidString(retained[0].Body) || len(retained[0].Body) > MaxExternalCommentBytes {
		t.Fatalf("invalid per-comment truncation: %#v", retained)
	}

	comments = make([]ExternalComment, 0, 5)
	for range 4 {
		comments = append(comments, ExternalComment{Body: strings.Repeat("x", MaxExternalCommentBytes-1)})
	}
	comments = append(comments, ExternalComment{Body: "xx界"})
	retained, total, truncated := boundExternalComments(comments)
	if !truncated || total > MaxExternalCommentsBytes {
		t.Fatalf("aggregate truncation total=%d truncated=%v", total, truncated)
	}
	for _, comment := range retained {
		if !utf8.ValidString(comment.Body) {
			t.Fatalf("aggregate truncation produced invalid UTF-8: %q", comment.Body)
		}
	}
}

func TestPlanningOperationExternalContextIsBoundAtEnqueue(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	external := NewExternalStore(db)
	planning := NewPlanningStore(db)
	ctx := context.Background()
	at := "2026-08-12T10:00:00Z"

	if _, err := external.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-bind", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", InstallationID: "2", CredentialHandle: "github-app:conn-bind", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-bind"}); err != nil {
		t.Fatal(err)
	}
	first, _, err := external.CaptureIssue(ctx, CaptureExternalIssueRequest{SourceID: "source-bind", SnapshotID: "snapshot-a", ConnectionID: "conn-bind", ProviderNativeID: "100", RepositoryNativeID: "200", Issue: NormalizedExternalIssue{Title: "Version A", Body: "first", State: "open", Repository: "owner/repo", IssueNumber: 7, CanonicalURL: "https://github.com/owner/repo/issues/7", CreatedAt: at, UpdatedAt: at}, Profile: ExternalImportProfile{}, ProviderUpdatedAt: at, FetchedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO planning_sessions (planning_session_id,title,title_state,lifecycle_state,conversation_state,project_root,model_selection_json,created_at,updated_at) VALUES ('session-bind','Issue','custom','active','ready','/tmp','{}',?,?)`, at, at); err != nil {
		t.Fatal(err)
	}
	mustExecArgs(t, db, `INSERT INTO planning_context_snapshots (planning_context_snapshot_id,planning_session_id,summary_markdown,included_files_json,excluded_files_json,created_at) VALUES ('context-bind','session-bind','context a','[]','[]',?)`, at)
	mustExec(t, db, `UPDATE planning_sessions SET context_snapshot_id='context-bind' WHERE planning_session_id='session-bind'`)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := createExternalLinkTx(ctx, tx, "link-bind", "planning_session", "session-bind", first.SourceID, first.SnapshotID, "local-operator", "adopt-a", at); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if _, err := planning.CreateOperation(ctx, CreatePlanningOperationRequest{OperationID: "operation-bind", PlanningSessionID: "session-bind", Type: PlanningOperationTypeReply, Status: PlanningOperationStatusQueued, RequestPayloadJSON: `{}`, CreatedAt: at}); err != nil {
		t.Fatal(err)
	}
	secondAt := "2026-08-12T11:00:00Z"
	second, _, err := external.CaptureIssue(ctx, CaptureExternalIssueRequest{SourceID: "source-ignored", SnapshotID: "snapshot-b", ConnectionID: "conn-bind", ProviderNativeID: "100", RepositoryNativeID: "200", PredecessorSnapshotID: first.SnapshotID, Issue: NormalizedExternalIssue{Title: "Version B", Body: "second", State: "open", Repository: "owner/repo", IssueNumber: 7, CanonicalURL: "https://github.com/owner/repo/issues/7", CreatedAt: at, UpdatedAt: secondAt}, Profile: ExternalImportProfile{}, ProviderUpdatedAt: secondAt, FetchedAt: secondAt})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO planning_external_source_adoptions (adoption_id,link_id,planning_session_id,snapshot_id,actor_id,adopted_at) VALUES ('adopt-b','link-bind','session-bind',?,'local-operator',?)`, second.SnapshotID, secondAt); err != nil {
		t.Fatal(err)
	}

	contextText, snapshotIDs, err := planning.LoadPlanningOperationExternalContext(ctx, "operation-bind")
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshotIDs) != 1 || snapshotIDs[0] != first.SnapshotID {
		t.Fatalf("queued operation binding changed after adoption: %#v", snapshotIDs)
	}
	if !strings.Contains(contextText, "Version A") || strings.Contains(contextText, "Version B") {
		t.Fatalf("unexpected bound context: %s", contextText)
	}
}

func TestAttachPlanningExternalSourceIsAtomicAndSessionScoped(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	ctx := context.Background()
	at := "2026-08-12T10:00:00Z"
	connection, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-attach", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", InstallationID: "2", CredentialHandle: "github-app:conn-attach", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-attach-connection"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateConnectionState(ctx, connection.ConnectionID, "enabled", "healthy", "", at, true, "10", "octocat"); err != nil {
		t.Fatal(err)
	}
	connection, err = store.GetConnection(ctx, connection.ConnectionID)
	if err != nil {
		t.Fatal(err)
	}
	for _, sessionID := range []string{"session-attach-a", "session-attach-b"} {
		mustExecArgs(t, db, `INSERT INTO planning_sessions (planning_session_id,title,title_state,lifecycle_state,conversation_state,active_leaf_message_id,context_snapshot_id,project_root,model_selection_json,created_at,updated_at) VALUES (?,?,'custom','active','ready','message-stable','context-stable','/tmp','{}',?,?)`, sessionID, sessionID, at, at)
	}

	capture := CaptureExternalIssueRequest{SourceID: "source-attach", SnapshotID: "snapshot-attach", ConnectionID: connection.ConnectionID, ConnectionRowVersion: connection.RowVersion, ProviderNativeID: "100", RepositoryNativeID: "200", Issue: NormalizedExternalIssue{Title: "Issue", Body: "body", State: "open", Repository: "owner/repo", IssueNumber: 7, CanonicalURL: "https://github.com/owner/repo/issues/7", CreatedAt: at, UpdatedAt: at}, Profile: ExternalImportProfile{}, ProviderUpdatedAt: at, FetchedAt: at}
	first, err := store.AttachPlanningExternalSource(ctx, AttachPlanningExternalSourceRequest{PlanningSessionID: "session-attach-a", Capture: capture, LinkID: "link-attach-a", AdoptionID: "adoption-attach-a", AuditEventID: "audit-attach-a", ActorID: "local-operator", AttachedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	if first.TargetID != "session-attach-a" || first.ActiveSnapshotID != capture.SnapshotID {
		t.Fatalf("unexpected attached source: %#v", first)
	}

	duplicate := capture
	duplicate.SourceID, duplicate.SnapshotID = "source-duplicate", "snapshot-duplicate"
	if _, err := store.AttachPlanningExternalSource(ctx, AttachPlanningExternalSourceRequest{PlanningSessionID: "session-attach-a", Capture: duplicate, LinkID: "link-duplicate", AdoptionID: "adoption-duplicate", AuditEventID: "audit-duplicate", ActorID: "local-operator", AttachedAt: at}); !errors.Is(err, ErrExternalSourceAlreadyAttached) {
		t.Fatalf("duplicate attach error=%v", err)
	}
	assertExternalAttachCounts(t, db, 1, 1, 1, 1)

	crossSession := capture
	crossSession.SourceID, crossSession.SnapshotID = "source-cross-session", "snapshot-cross-session"
	second, err := store.AttachPlanningExternalSource(ctx, AttachPlanningExternalSourceRequest{PlanningSessionID: "session-attach-b", Capture: crossSession, LinkID: "link-attach-b", AdoptionID: "adoption-attach-b", AuditEventID: "audit-attach-b", ActorID: "local-operator", AttachedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	if second.TargetID != "session-attach-b" || second.SourceID != first.SourceID {
		t.Fatalf("cross-session source identity was not independently linked: first=%#v second=%#v", first, second)
	}

	missing := capture
	missing.SourceID, missing.SnapshotID, missing.ProviderNativeID = "source-missing", "snapshot-missing", "101"
	if _, err := store.AttachPlanningExternalSource(ctx, AttachPlanningExternalSourceRequest{PlanningSessionID: "session-deleted", Capture: missing, LinkID: "link-missing", AdoptionID: "adoption-missing", AuditEventID: "audit-missing", ActorID: "local-operator", AttachedAt: at}); !errors.Is(err, ErrExternalPlanningSessionNotFound) {
		t.Fatalf("missing session attach error=%v", err)
	}
	assertExternalAttachCounts(t, db, 1, 1, 2, 2)
	mustExecArgs(t, db, `INSERT INTO planning_sessions (planning_session_id,title,title_state,lifecycle_state,conversation_state,project_root,model_selection_json,created_at,updated_at) VALUES ('session-archived','Archived','custom','archived','ready','/tmp','{}',?,?)`, at, at)
	archived := capture
	archived.SourceID, archived.SnapshotID, archived.ProviderNativeID = "source-archived", "snapshot-archived", "103"
	if _, err := store.AttachPlanningExternalSource(ctx, AttachPlanningExternalSourceRequest{PlanningSessionID: "session-archived", Capture: archived, LinkID: "link-archived", AdoptionID: "adoption-archived", AuditEventID: "audit-archived", ActorID: "local-operator", AttachedAt: at}); !errors.Is(err, ErrExternalPlanningSessionInactive) {
		t.Fatalf("archived session attach error=%v", err)
	}
	assertExternalAttachCounts(t, db, 1, 1, 2, 2)

	if _, err := db.Exec(`UPDATE external_connections SET lifecycle='disabled',health_class='disabled',validated_generation=0,row_version=row_version+1 WHERE connection_id='conn-attach'`); err != nil {
		t.Fatal(err)
	}
	disabled := capture
	disabled.SourceID, disabled.SnapshotID, disabled.ProviderNativeID = "source-disabled", "snapshot-disabled", "102"
	if _, err := store.AttachPlanningExternalSource(ctx, AttachPlanningExternalSourceRequest{PlanningSessionID: "session-attach-a", Capture: disabled, LinkID: "link-disabled", AdoptionID: "adoption-disabled", AuditEventID: "audit-disabled", ActorID: "local-operator", AttachedAt: at}); !errors.Is(err, ErrExternalConflict) {
		t.Fatalf("disabled connection attach error=%v", err)
	}
	assertExternalAttachCounts(t, db, 1, 1, 2, 2)

	var activeLeaf, contextSnapshot string
	if err := db.QueryRow(`SELECT active_leaf_message_id,context_snapshot_id FROM planning_sessions WHERE planning_session_id='session-attach-a'`).Scan(&activeLeaf, &contextSnapshot); err != nil {
		t.Fatal(err)
	}
	if activeLeaf != "message-stable" || contextSnapshot != "context-stable" {
		t.Fatalf("attach moved planning transcript/context: leaf=%q context=%q", activeLeaf, contextSnapshot)
	}
}

func assertExternalAttachCounts(t *testing.T, db *sql.DB, sources, snapshots, links, adoptions int) {
	t.Helper()
	for table, want := range map[string]int{"external_sources": sources, "external_source_snapshots": snapshots, "external_source_links": links, "planning_external_source_adoptions": adoptions} {
		var got int
		if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("%s count=%d, want %d", table, got, want)
		}
	}
}

func TestPlanningOperationExternalContextBindsLatestAdoptionPerLink(t *testing.T) {
	fixture := newPlanningExternalBindingFixture(t, "latest")
	firstA := fixture.capture(t, "source-a", "snapshot-a", "100", "Source A", "body a", nil, fixture.at)
	firstB := fixture.capture(t, "source-b", "snapshot-b-old", "101", "Source B old", "old", nil, fixture.at)
	secondB := fixture.capture(t, "ignored-b-2", "snapshot-b-second", "101", "Source B second", "second", nil, "2026-08-12T11:00:00Z")
	activeB := fixture.capture(t, "ignored-b-3", "snapshot-b-active", "101", "Source B active", "active", nil, "2026-08-12T11:30:00Z")
	laterB := fixture.capture(t, "ignored-b-4", "snapshot-b-later", "101", "Source B later", "later", nil, "2026-08-12T13:00:00Z")

	fixture.link(t, "link-b", firstB, "adopt-b-old", fixture.at)
	mustExecArgs(t, fixture.db, `INSERT INTO planning_external_source_adoptions (adoption_id,link_id,planning_session_id,snapshot_id,actor_id,adopted_at) VALUES (?,?,?,?,?,'2026-08-12T11:00:00Z')`, "adopt-b-a", "link-b", fixture.sessionID, secondB.SnapshotID, "local-operator")
	// Equal timestamps are resolved by adoption_id, matching the active-link read path.
	mustExecArgs(t, fixture.db, `INSERT INTO planning_external_source_adoptions (adoption_id,link_id,planning_session_id,snapshot_id,actor_id,adopted_at) VALUES (?,?,?,?,?,'2026-08-12T11:00:00Z')`, "adopt-b-z", "link-b", fixture.sessionID, activeB.SnapshotID, "local-operator")
	fixture.link(t, "link-a", firstA, "adopt-a", "2026-08-12T12:00:00Z")
	links, err := fixture.external.GetLinksForTarget(fixture.ctx, "planning_session", fixture.sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if len(links) != 2 || links[0].LinkID != "link-b" || links[0].ActiveSnapshotID != activeB.SnapshotID || links[1].LinkID != "link-a" || links[1].ActiveSnapshotID != firstA.SnapshotID {
		t.Fatalf("planning target links hid or misordered active provenance: %#v", links)
	}

	fixture.createOperation(t, "operation-multi")
	mustExecArgs(t, fixture.db, `INSERT INTO planning_external_source_adoptions (adoption_id,link_id,planning_session_id,snapshot_id,actor_id,adopted_at) VALUES (?,?,?,?,?,'2026-08-12T13:00:00Z')`, "adopt-b-later", "link-b", fixture.sessionID, laterB.SnapshotID, "local-operator")

	firstContext, firstIDs, err := fixture.planning.LoadPlanningOperationExternalContext(fixture.ctx, "operation-multi")
	if err != nil {
		t.Fatal(err)
	}
	secondContext, secondIDs, err := fixture.planning.LoadPlanningOperationExternalContext(fixture.ctx, "operation-multi")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(firstIDs, ","), "snapshot-a,snapshot-b-active"; got != want {
		t.Fatalf("bound snapshots=%q, want %q", got, want)
	}
	if strings.Join(secondIDs, ",") != strings.Join(firstIDs, ",") || secondContext != firstContext {
		t.Fatal("immutable context load was not deterministic")
	}
	for _, expected := range []string{"Source A", "Source B active"} {
		if !strings.Contains(firstContext, expected) {
			t.Fatalf("bound context is missing %q: %s", expected, firstContext)
		}
	}
	for _, excluded := range []string{"Source B old", "Source B second", "Source B later"} {
		if strings.Contains(firstContext, excluded) {
			t.Fatalf("bound context unexpectedly contains %q: %s", excluded, firstContext)
		}
	}
}

func TestPlanningOperationExternalContextUsesAggregateBudgets(t *testing.T) {
	t.Run("comment budget", func(t *testing.T) {
		fixture := newPlanningExternalBindingFixture(t, "comment-budget")
		comments := []ExternalComment{
			{ProviderCommentID: "1", Author: "one", Body: strings.Repeat("a", 20*1024), CanonicalURL: "https://example.test/1", CreatedAt: fixture.at, UpdatedAt: fixture.at},
			{ProviderCommentID: "2", Author: "two", Body: strings.Repeat("界", 7*1024), CanonicalURL: "https://example.test/2", CreatedAt: fixture.at, UpdatedAt: fixture.at},
		}
		first := fixture.capture(t, "source-a", "snapshot-a", "100", "A", "body", comments, fixture.at)
		second := fixture.capture(t, "source-b", "snapshot-b", "101", "B", "body", comments, fixture.at)
		fixture.link(t, "link-a", first, "adopt-a", fixture.at)
		fixture.link(t, "link-b", second, "adopt-b", fixture.at)
		fixture.createOperation(t, "operation-budget")

		contextText, _, err := fixture.planning.LoadPlanningOperationExternalContext(fixture.ctx, "operation-budget")
		if err != nil {
			t.Fatal(err)
		}
		issueBytes, commentBytes := planningBindingByteTotals(t, fixture.db, "operation-budget")
		if commentBytes > MaxPlannerCommentBytes || len(contextText) > MaxPlannerExternalBytes {
			t.Fatalf("aggregate bytes issue=%d comments=%d", issueBytes, commentBytes)
		}
		assertExternalContextJSONValid(t, contextText)
	})

	t.Run("total external budget", func(t *testing.T) {
		fixture := newPlanningExternalBindingFixture(t, "external-budget")
		comments := []ExternalComment{{ProviderCommentID: "1", Author: "one", Body: strings.Repeat("界", 12*1024), CanonicalURL: "https://example.test/1", CreatedAt: fixture.at, UpdatedAt: fixture.at}}
		first := fixture.capture(t, "source-a", "snapshot-a", "100", "A", strings.Repeat("a", 48*1024), comments, fixture.at)
		second := fixture.capture(t, "source-b", "snapshot-b", "101", "B", strings.Repeat("b", 48*1024), comments, fixture.at)
		fixture.link(t, "link-a", first, "adopt-a", fixture.at)
		fixture.link(t, "link-b", second, "adopt-b", fixture.at)
		fixture.createOperation(t, "operation-budget")

		contextText, _, err := fixture.planning.LoadPlanningOperationExternalContext(fixture.ctx, "operation-budget")
		if err != nil {
			t.Fatal(err)
		}
		issueBytes, commentBytes := planningBindingByteTotals(t, fixture.db, "operation-budget")
		unusedBytes := MaxPlannerExternalBytes - len(contextText)
		if unusedBytes < 0 || commentBytes == 0 || commentBytes > MaxPlannerCommentBytes {
			t.Fatalf("aggregate bytes issue=%d comments=%d", issueBytes, commentBytes)
		}
		assertExternalContextJSONValid(t, contextText)
	})
}

func TestPlanningOperationExternalContextAccountsForCompleteFrames(t *testing.T) {
	fixture := newPlanningExternalBindingFixture(t, "complete-frames")
	comments := make([]ExternalComment, 40)
	for i := range comments {
		comments[i] = ExternalComment{
			ProviderCommentID: fmt.Sprintf("comment-%03d", i),
			Author:            strings.Repeat(`author\"界`, 80),
			Body:              strings.Repeat("quoted \\\" multibyte 界 ", 40),
			CanonicalURL:      "https://example.test/" + strings.Repeat("metadata/", 80) + fmt.Sprint(i),
			CreatedAt:         fixture.at,
			UpdatedAt:         fixture.at,
		}
	}
	first := fixture.capture(t, "source-a", "snapshot-a", "100", `A "quoted"`, `body \\ path 界`, comments, fixture.at)
	second := fixture.capture(t, "source-b", "snapshot-b", "101", `B \\ slash`, `body " quote 界`, comments, fixture.at)
	fixture.link(t, "link-a", first, "adopt-a", fixture.at)
	fixture.link(t, "link-b", second, "adopt-b", fixture.at)
	fixture.createOperation(t, "operation-complete-frames")

	contextText, snapshotIDs, err := fixture.planning.LoadPlanningOperationExternalContext(fixture.ctx, "operation-complete-frames")
	if err != nil {
		t.Fatal(err)
	}
	if len(contextText) > MaxPlannerExternalBytes || !utf8.ValidString(contextText) {
		t.Fatalf("assembled bytes=%d valid_utf8=%v", len(contextText), utf8.ValidString(contextText))
	}
	if got := strings.Count(contextText, "\n\n"); got != 1 {
		t.Fatalf("separator count=%d, want 1", got)
	}
	if got := strings.Join(snapshotIDs, ","); got != "snapshot-a,snapshot-b" {
		t.Fatalf("snapshot bindings=%q", got)
	}
	assertExternalContextJSONValid(t, contextText)

	sections := strings.Split(contextText, "\n\n")
	rows, err := fixture.db.Query(`SELECT context_digest FROM planning_operation_external_snapshots WHERE operation_id='operation-complete-frames' ORDER BY snapshot_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for i := 0; rows.Next(); i++ {
		var storedDigest string
		if err := rows.Scan(&storedDigest); err != nil {
			t.Fatal(err)
		}
		if storedDigest != digest([]byte(sections[i])) {
			t.Fatalf("recovery digest mismatch for section %d", i)
		}
	}
}

func TestPlanningOperationExternalContextFillsExactWholePromptBoundary(t *testing.T) {
	fixture := newPlanningExternalBindingFixture(t, "exact-boundary")
	snapshot := fixture.capture(t, "source-boundary", "snapshot-boundary", "100", "Boundary", strings.Repeat("x", MaxPlannerExternalBytes), nil, fixture.at)
	fixture.link(t, "link-boundary", snapshot, "adopt-boundary", fixture.at)
	fixture.createOperation(t, "operation-boundary")
	contextText, _, err := fixture.planning.LoadPlanningOperationExternalContext(fixture.ctx, "operation-boundary")
	if err != nil {
		t.Fatal(err)
	}
	if len(contextText) != MaxPlannerExternalBytes {
		t.Fatalf("assembled bytes=%d, want exact boundary %d", len(contextText), MaxPlannerExternalBytes)
	}
	assertExternalContextJSONValid(t, contextText)
}

func TestPlanningOperationExternalContextOmitsUnsafeFrameButKeepsBinding(t *testing.T) {
	fixture := newPlanningExternalBindingFixture(t, "omitted-frame")
	firstID := "snapshot-a-" + strings.Repeat("a", 70*1024)
	secondID := "snapshot-b-" + strings.Repeat("b", 70*1024)
	first := fixture.capture(t, "source-a", firstID, "100", "A", "body", nil, fixture.at)
	second := fixture.capture(t, "source-b", secondID, "101", "B", "body", nil, fixture.at)
	fixture.link(t, "link-a", first, "adopt-a", fixture.at)
	fixture.link(t, "link-b", second, "adopt-b", fixture.at)
	fixture.createOperation(t, "operation-omitted-frame")
	contextText, snapshotIDs, err := fixture.planning.LoadPlanningOperationExternalContext(fixture.ctx, "operation-omitted-frame")
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshotIDs) != 2 || snapshotIDs[0] != firstID || snapshotIDs[1] != secondID {
		t.Fatal("omitted frame changed immutable snapshot bindings")
	}
	if strings.Count(contextText, "<KINGDOM_UNTRUSTED_EXTERNAL_SOURCE") != 1 || len(contextText) > MaxPlannerExternalBytes {
		t.Fatalf("unexpected emitted context bytes=%d", len(contextText))
	}
	var omittedDigest string
	var issueBytes, commentBytes int
	if err := fixture.db.QueryRow(`SELECT context_digest,issue_bytes,comment_bytes FROM planning_operation_external_snapshots WHERE operation_id='operation-omitted-frame' AND snapshot_id=?`, secondID).Scan(&omittedDigest, &issueBytes, &commentBytes); err != nil {
		t.Fatal(err)
	}
	if omittedDigest != digest(nil) || issueBytes != 0 || commentBytes != 0 {
		t.Fatalf("omitted binding metadata digest=%q issue=%d comments=%d", omittedDigest, issueBytes, commentBytes)
	}
}

func TestPlanningOperationExternalContextRejectsBindingMetadataTampering(t *testing.T) {
	fixture := newPlanningExternalBindingFixture(t, "tamper")
	snapshot := fixture.capture(t, "source-a", "snapshot-a", "100", "A", "body", nil, fixture.at)
	fixture.link(t, "link-a", snapshot, "adopt-a", fixture.at)
	fixture.createOperation(t, "operation-tamper")

	var version, contextDigest string
	var issueBytes, commentBytes int
	if err := fixture.db.QueryRow(`SELECT assembler_version,context_digest,issue_bytes,comment_bytes FROM planning_operation_external_snapshots WHERE operation_id='operation-tamper'`).Scan(&version, &contextDigest, &issueBytes, &commentBytes); err != nil {
		t.Fatal(err)
	}
	mustExec(t, fixture.db, `DROP TRIGGER planning_operation_external_no_update`)
	tests := []struct {
		name     string
		column   string
		tampered any
		original any
	}{
		{name: "assembler version", column: "assembler_version", tampered: "unknown", original: version},
		{name: "digest", column: "context_digest", tampered: strings.Repeat("0", 64), original: contextDigest},
		{name: "issue bytes", column: "issue_bytes", tampered: issueBytes + 1, original: issueBytes},
		{name: "comment bytes", column: "comment_bytes", tampered: commentBytes + 1, original: commentBytes},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mustExecArgs(t, fixture.db, `UPDATE planning_operation_external_snapshots SET `+test.column+`=? WHERE operation_id='operation-tamper'`, test.tampered)
			if _, _, err := fixture.planning.LoadPlanningOperationExternalContext(fixture.ctx, "operation-tamper"); err == nil || !strings.Contains(err.Error(), "metadata mismatch") {
				t.Fatalf("tampered %s was accepted: %v", test.column, err)
			}
			mustExecArgs(t, fixture.db, `UPDATE planning_operation_external_snapshots SET `+test.column+`=? WHERE operation_id='operation-tamper'`, test.original)
		})
	}
}

type planningExternalBindingFixture struct {
	db        *sql.DB
	external  *ExternalStore
	planning  *PlanningStore
	ctx       context.Context
	sessionID string
	at        string
}

func newPlanningExternalBindingFixture(t *testing.T, suffix string) planningExternalBindingFixture {
	t.Helper()
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	fixture := planningExternalBindingFixture{db: db, external: NewExternalStore(db), planning: NewPlanningStore(db), ctx: context.Background(), sessionID: "session-" + suffix, at: "2026-08-12T10:00:00Z"}
	if _, err := fixture.external.CreateConnection(fixture.ctx, CreateExternalConnectionRequest{ConnectionID: "conn-" + suffix, CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", InstallationID: "2", CredentialHandle: "github-app:conn-" + suffix, ActorID: "local-operator", CreatedAt: fixture.at, AuditEventID: "audit-" + suffix}); err != nil {
		t.Fatal(err)
	}
	mustExecArgs(t, db, `INSERT INTO planning_sessions (planning_session_id,title,title_state,lifecycle_state,conversation_state,project_root,model_selection_json,created_at,updated_at) VALUES (?,?,'custom','active','ready','/tmp','{}',?,?)`, fixture.sessionID, "Issue", fixture.at, fixture.at)
	mustExecArgs(t, db, `INSERT INTO planning_context_snapshots (planning_context_snapshot_id,planning_session_id,summary_markdown,included_files_json,excluded_files_json,created_at) VALUES (?,?,?,'[]','[]',?)`, "context-"+suffix, fixture.sessionID, "bound context", fixture.at)
	mustExecArgs(t, db, `UPDATE planning_sessions SET context_snapshot_id=? WHERE planning_session_id=?`, "context-"+suffix, fixture.sessionID)
	return fixture
}

func (f planningExternalBindingFixture) capture(t *testing.T, sourceID, snapshotID, nativeID, title, body string, comments []ExternalComment, fetchedAt string) ExternalSourceSnapshot {
	t.Helper()
	snapshot, _, err := f.external.CaptureIssue(f.ctx, CaptureExternalIssueRequest{SourceID: sourceID, SnapshotID: snapshotID, ConnectionID: "conn-" + strings.TrimPrefix(f.sessionID, "session-"), ProviderNativeID: nativeID, RepositoryNativeID: "repo-" + nativeID, Issue: NormalizedExternalIssue{Title: title, Body: body, State: "open", Repository: "owner/repo", IssueNumber: 7, CanonicalURL: "https://github.com/owner/repo/issues/7", CreatedAt: f.at, UpdatedAt: fetchedAt}, Profile: ExternalImportProfile{IncludeComments: len(comments) > 0}, ProviderUpdatedAt: fetchedAt, FetchedAt: fetchedAt, Comments: comments})
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func (f planningExternalBindingFixture) link(t *testing.T, linkID string, snapshot ExternalSourceSnapshot, adoptionID, adoptedAt string) {
	t.Helper()
	tx, err := f.db.BeginTx(f.ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := createExternalLinkTx(f.ctx, tx, linkID, "planning_session", f.sessionID, snapshot.SourceID, snapshot.SnapshotID, "local-operator", adoptionID, adoptedAt); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func (f planningExternalBindingFixture) createOperation(t *testing.T, operationID string) {
	t.Helper()
	if _, err := f.planning.CreateOperation(f.ctx, CreatePlanningOperationRequest{OperationID: operationID, PlanningSessionID: f.sessionID, Type: PlanningOperationTypeReply, Status: PlanningOperationStatusQueued, RequestPayloadJSON: `{}`, CreatedAt: f.at}); err != nil {
		t.Fatal(err)
	}
}

func planningBindingByteTotals(t *testing.T, db *sql.DB, operationID string) (int, int) {
	t.Helper()
	var issueBytes, commentBytes int
	if err := db.QueryRow(`SELECT COALESCE(SUM(issue_bytes),0),COALESCE(SUM(comment_bytes),0) FROM planning_operation_external_snapshots WHERE operation_id=?`, operationID).Scan(&issueBytes, &commentBytes); err != nil {
		t.Fatal(err)
	}
	return issueBytes, commentBytes
}

func assertExternalContextJSONValid(t *testing.T, contextText string) {
	t.Helper()
	if !utf8.ValidString(contextText) {
		t.Fatal("assembled context is not valid UTF-8")
	}
	sections := strings.Split(contextText, "\n\n")
	for _, section := range sections {
		var foundIssue, foundComments bool
		for _, line := range strings.Split(section, "\n") {
			switch {
			case strings.HasPrefix(line, "issue_json="):
				foundIssue = json.Valid([]byte(strings.TrimPrefix(line, "issue_json=")))
			case strings.HasPrefix(line, "comments_json="):
				foundComments = json.Valid([]byte(strings.TrimPrefix(line, "comments_json=")))
			}
		}
		if !foundIssue || !foundComments {
			t.Fatalf("assembled section contains malformed JSON: %s", section)
		}
	}
}

func TestExternalCommentOmissionMetadataIsPersistedAndAffectsDigest(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	ctx := context.Background()
	at := "2026-08-12T10:00:00Z"
	if _, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-comments", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", InstallationID: "2", CredentialHandle: "github-app:conn-comments", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-comments"}); err != nil {
		t.Fatal(err)
	}
	comments := make([]ExternalComment, 100)
	for i := range comments {
		comments[i] = ExternalComment{ProviderCommentID: string(rune(i + 1)), Body: "body"}
	}
	req := CaptureExternalIssueRequest{SourceID: "source-comments", SnapshotID: "snapshot-comments", ConnectionID: "conn-comments", ProviderNativeID: "100", RepositoryNativeID: "200", Issue: NormalizedExternalIssue{Title: "Issue", Body: "body", State: "open", Repository: "owner/repo", IssueNumber: 7, CanonicalURL: "https://github.com/owner/repo/issues/7", CreatedAt: at, UpdatedAt: at}, Profile: ExternalImportProfile{IncludeComments: true}, ProviderUpdatedAt: at, FetchedAt: at, ProviderCommentCount: 150, FetchedCommentCount: 100, FetchedCommentPages: 1, OmittedCommentCount: 50, OmittedCommentPages: 1, Comments: comments}
	first, created, err := store.CaptureIssue(ctx, req)
	if err != nil || !created {
		t.Fatalf("capture: created=%v err=%v", created, err)
	}
	loaded, _, err := store.GetSnapshot(ctx, first.SnapshotID)
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.CommentsTruncated || loaded.FetchedCommentPages != 1 || loaded.OmittedCommentCount != 50 || loaded.OmittedCommentPages != 1 {
		t.Fatalf("omission metadata lost: %#v", loaded)
	}
	var truncation string
	if err := db.QueryRow(`SELECT truncation_json FROM external_source_snapshots WHERE snapshot_id=?`, first.SnapshotID).Scan(&truncation); err != nil {
		t.Fatal(err)
	}
	var metadata map[string]any
	if json.Unmarshal([]byte(truncation), &metadata) != nil || metadata["omitted_count"] != float64(50) {
		t.Fatalf("truncation metadata=%s", truncation)
	}
	req.SnapshotID = "snapshot-comments-second"
	req.OmittedCommentCount = 49
	second, created, err := store.CaptureIssue(ctx, req)
	if err != nil || !created || second.ContentDigest == first.ContentDigest {
		t.Fatalf("omission-only version was coalesced: %#v %#v err=%v", first, second, err)
	}
}

func TestUpdateConnectionDisplayNamePreservesValidatedIdentity(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	ctx := context.Background()
	at := "2026-08-12T10:00:00Z"
	if _, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-edit", CredentialStrategy: "app_private_key", DisplayName: "Old", AppID: "1", ClientID: "Iv1", AppSlug: "app", InstallationID: "2", CredentialHandle: "github-app:conn-edit", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-edit-create"}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateConnectionState(ctx, "conn-edit", "enabled", "healthy", "", at, true, "10", "octocat"); err != nil {
		t.Fatal(err)
	}
	name := "New name"
	row, err := store.UpdateConnection(ctx, "conn-edit", UpdateExternalConnectionRequest{DisplayName: &name, ActorID: "local-operator", UpdatedAt: "2026-08-12T11:00:00Z", AuditEventID: "audit-edit-name"})
	if err != nil || row.HealthClass != "healthy" || row.DisplayName != name || row.AppID != "1" || row.ClientID != "Iv1" || row.InstallationID != "2" {
		t.Fatalf("display update=%#v err=%v", row, err)
	}
	var payload string
	if err := db.QueryRow(`SELECT safe_payload_json FROM external_integration_audit_events WHERE audit_event_id='audit-edit-name'`).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(payload, `"before"`) || !strings.Contains(payload, `"installation_id":"2"`) {
		t.Fatalf("incomplete audit payload: %s", payload)
	}
}

func TestFinalizeConnectionCreatesHealthyValidatedGenerationAtomically(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	at := "2026-08-15T12:00:00Z"
	connection, err := store.FinalizeConnection(context.Background(), FinalizeExternalConnectionRequest{
		CreateExternalConnectionRequest: CreateExternalConnectionRequest{ConnectionID: "conn-final", CredentialStrategy: "app_private_key", DisplayName: "Kingdom Issues", AppID: "1", ClientID: "Iv1", AppSlug: "kingdom-issues", InstallationID: "2", CredentialHandle: "github-app:conn-final", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-final"},
		InstallationAccountID:           "10", InstallationAccountLogin: "octocat",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !connection.HasCurrentValidation() || connection.CredentialGeneration != 1 || connection.ValidatedGeneration != 1 || connection.CredentialStrategy != "app_private_key" || connection.AppSlug != "kingdom-issues" {
		t.Fatalf("finalized connection=%#v", connection)
	}
	var auditCount int
	if err := db.QueryRow(`SELECT COUNT(1) FROM external_integration_audit_events WHERE connection_id=? AND event_type='connection.manifest_finalized'`, connection.ConnectionID).Scan(&auditCount); err != nil || auditCount != 1 {
		t.Fatalf("manifest finalization audit count=%d err=%v", auditCount, err)
	}

	_, err = store.FinalizeConnection(context.Background(), FinalizeExternalConnectionRequest{
		CreateExternalConnectionRequest: CreateExternalConnectionRequest{ConnectionID: "conn-rollback", CredentialStrategy: "app_private_key", DisplayName: "Rollback", AppID: "3", ClientID: "Iv2", AppSlug: "rollback", InstallationID: "4", CredentialHandle: "github-app:conn-rollback", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-final"},
		InstallationAccountID:           "11", InstallationAccountLogin: "owner",
	})
	if err == nil {
		t.Fatal("expected duplicate audit id to roll back finalization")
	}
	var rowCount int
	if queryErr := db.QueryRow(`SELECT COUNT(1) FROM external_connections WHERE connection_id='conn-rollback'`).Scan(&rowCount); queryErr != nil || rowCount != 0 {
		t.Fatalf("rolled-back connection count=%d err=%v", rowCount, queryErr)
	}
}

func TestSharedReauthorizationAdvancesGenerationWithoutChangingPinnedIdentity(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	ctx := context.Background()
	at := "2026-08-15T10:00:00Z"
	connection, err := store.FinalizeConnection(ctx, FinalizeExternalConnectionRequest{CreateExternalConnectionRequest: CreateExternalConnectionRequest{ConnectionID: "conn-shared", CredentialStrategy: "user_access_token", DisplayName: "Shared", AppID: "1", ClientID: "IvShared", AppSlug: "kingdom", InstallationID: "2", CredentialHandle: "github-user:conn-shared", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-shared-create"}, InstallationAccountID: "10", InstallationAccountLogin: "old-login"})
	if err != nil {
		t.Fatal(err)
	}
	updated, err := store.ReauthorizeSharedConnection(ctx, connection, "kingdom-renamed", "new-login", "local-operator", "audit-shared-reauthorize", "2026-08-15T11:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	if updated.CredentialGeneration != 2 || updated.ValidatedGeneration != 2 || updated.AppSlug != "kingdom-renamed" || updated.InstallationAccountLogin != "new-login" || updated.AppID != "1" || updated.ClientID != "IvShared" || updated.InstallationID != "2" || updated.InstallationAccountID != "10" {
		t.Fatalf("reauthorized connection=%#v", updated)
	}
	if _, err := db.Exec(`UPDATE external_connections SET client_id='spoofed' WHERE connection_id='conn-shared'`); err == nil {
		t.Fatal("database allowed pinned shared App identity mutation")
	}
}

func TestCredentialGenerationFencesStaleCaptureAndHealth(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	ctx := context.Background()
	at := "2026-08-12T10:00:00Z"
	conn, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-fence", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", InstallationID: "2", CredentialHandle: "github-app:conn-fence", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-fence-create"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCredentialEvent(ctx, conn.ConnectionID, conn.RowVersion, "local-operator", "audit-fence-key", "credential.provisioned", at); err != nil {
		t.Fatal(err)
	}
	err = store.UpdateConnectionStateAuditedVersion(ctx, conn.ConnectionID, "enabled", "healthy", "", at, true, "10", "octocat", "local-operator", "audit-stale-health", "connection.health_checked", conn.RowVersion)
	if !errors.Is(err, ErrExternalConflict) {
		t.Fatalf("stale health result persisted: %v", err)
	}
	_, _, err = store.CaptureIssue(ctx, CaptureExternalIssueRequest{SourceID: "source-fence", SnapshotID: "snapshot-fence", ConnectionID: conn.ConnectionID, ConnectionRowVersion: conn.RowVersion, ProviderNativeID: "100", RepositoryNativeID: "200", Issue: NormalizedExternalIssue{Title: "Issue", Body: "body", State: "open", Repository: "owner/repo", IssueNumber: 7, CanonicalURL: "https://github.com/owner/repo/issues/7", CreatedAt: at, UpdatedAt: at}, ProviderUpdatedAt: at, FetchedAt: at})
	if !errors.Is(err, ErrExternalConflict) {
		t.Fatalf("stale capture persisted: %v", err)
	}
}

func TestConnectionHealthPinsInstallationAccountIdentity(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	ctx := context.Background()
	at := "2026-08-12T10:00:00Z"
	connection, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-identity", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", InstallationID: "2", CredentialHandle: "github-app:conn-identity", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-identity-create"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateConnectionStateAuditedVersion(ctx, connection.ConnectionID, "enabled", "healthy", "", at, true, "10", "octocat", "local-operator", "audit-identity-health", "connection.health_checked", connection.RowVersion); err != nil {
		t.Fatal(err)
	}
	validated, err := store.GetConnection(ctx, connection.ConnectionID)
	if err != nil {
		t.Fatal(err)
	}
	if validated.InstallationAccountID != "10" || validated.InstallationAccountLogin != "octocat" || !validated.HasCurrentValidation() {
		t.Fatalf("validated identity was not persisted: %#v", validated)
	}
	if err := store.UpdateConnectionStateAuditedVersion(ctx, connection.ConnectionID, "enabled", "healthy", "", at, true, "11", "impostor", "local-operator", "audit-identity-mismatch", "connection.health_checked", validated.RowVersion); !errors.Is(err, ErrExternalConflict) {
		t.Fatalf("mismatched installation account identity was accepted: %v", err)
	}
	after, err := store.GetConnection(ctx, connection.ConnectionID)
	if err != nil {
		t.Fatal(err)
	}
	if after.InstallationAccountID != "10" || after.InstallationAccountLogin != "octocat" || after.RowVersion != validated.RowVersion {
		t.Fatalf("identity changed after mismatch: before=%#v after=%#v", validated, after)
	}
	if err := store.UpdateConnectionStateAuditedVersion(ctx, connection.ConnectionID, "enabled", "healthy", "", at, true, "10", "octocat-renamed", "local-operator", "audit-identity-rename", "connection.health_checked", after.RowVersion); err != nil {
		t.Fatalf("refresh renamed installation account login: %v", err)
	}
	renamed, err := store.GetConnection(ctx, connection.ConnectionID)
	if err != nil {
		t.Fatal(err)
	}
	if renamed.InstallationAccountID != "10" || renamed.InstallationAccountLogin != "octocat-renamed" {
		t.Fatalf("renamed account login was not refreshed: %#v", renamed)
	}
	if _, err := db.Exec(`UPDATE external_connections SET installation_account_id='11' WHERE connection_id='conn-identity'`); err == nil || !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("expected pinned installation identity trigger, got %v", err)
	}
}

func TestExternalHistorySurvivesDetachedTargetRemoval(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	ctx := context.Background()
	at := "2026-08-12T10:00:00Z"
	if _, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-detached", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", InstallationID: "2", CredentialHandle: "github-app:conn-detached", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-detached"}); err != nil {
		t.Fatal(err)
	}
	snapshot, _, err := store.CaptureIssue(ctx, CaptureExternalIssueRequest{SourceID: "source-detached", SnapshotID: "snapshot-detached", ConnectionID: "conn-detached", ProviderNativeID: "100", RepositoryNativeID: "200", Issue: NormalizedExternalIssue{Title: "Issue", Body: "body", State: "open", Repository: "owner/repo", IssueNumber: 7, CanonicalURL: "https://github.com/owner/repo/issues/7", CreatedAt: at, UpdatedAt: at}, Profile: ExternalImportProfile{}, ProviderUpdatedAt: at, FetchedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, db, `INSERT INTO planning_sessions (planning_session_id,title,title_state,lifecycle_state,conversation_state,project_root,model_selection_json,created_at,updated_at) VALUES ('session-detached','Issue','custom','active','ready','/tmp','{}','2026-08-12T10:00:00Z','2026-08-12T10:00:00Z')`)
	bindTestPlanningContext(t, db, "session-detached")
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := createExternalLinkTx(ctx, tx, "link-detached", "planning_session", "session-detached", snapshot.SourceID, snapshot.SnapshotID, "local-operator", "adoption-detached", at); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	planning := NewPlanningStore(db)
	if _, err := planning.CreateOperation(ctx, CreatePlanningOperationRequest{OperationID: "operation-removed", PlanningSessionID: "session-detached", Type: PlanningOperationTypeReply, Status: PlanningOperationStatusQueued, RequestPayloadJSON: `{}`, CreatedAt: at}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM planning_operation_external_snapshots WHERE operation_id='operation-removed'`); err == nil || !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("expected immutable operation binding trigger, got %v", err)
	}
	mustExec(t, db, `DELETE FROM planning_operations WHERE operation_id='operation-removed'`)
	mustExec(t, db, `DELETE FROM planning_sessions WHERE planning_session_id='session-detached'`)

	for table, idColumn := range map[string]string{"external_source_links": "link_id", "planning_external_source_adoptions": "adoption_id", "planning_operation_external_snapshots": "operation_id"} {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table + ` WHERE ` + idColumn + ` IN ('link-detached','adoption-detached','operation-removed')`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("detached history in %s was lost", table)
		}
	}
	if _, err := store.LinkHistory(ctx, "link-detached"); err != nil {
		t.Fatalf("detached link history is unreadable: %v", err)
	}
}

// A removed connection stays in SQLite so provenance links keep resolving and
// the same installation can be added again, but it is not part of the
// operator's connection surface. Removal reconciliation still needs to see it.
func TestListConnectionsExcludesRemovedButReconciliationSeesThem(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	ctx := context.Background()
	at := "2026-08-12T10:00:00Z"

	for i, id := range []string{"conn-live", "conn-pending", "conn-gone"} {
		if _, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{
			ConnectionID: id, CredentialStrategy: "app_private_key", DisplayName: id, AppID: "1", ClientID: "Iv1",
			AppSlug: "app", InstallationID: fmt.Sprintf("%d", 100+i), CredentialHandle: "github-app:" + id,
			ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-create-" + id,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.UpdateConnectionStateAudited(ctx, "conn-pending", "removal_pending", "removal_pending", "", at, false, "local-operator", "audit-pending", "connection.removal_pending"); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateConnectionStateAudited(ctx, "conn-gone", "removed", "removed", "", at, false, "local-operator", "audit-removed", "connection.removed"); err != nil {
		t.Fatal(err)
	}

	operatorView, err := store.ListConnections(ctx)
	if err != nil {
		t.Fatal(err)
	}
	listed := make(map[string]string, len(operatorView))
	for _, item := range operatorView {
		listed[item.ConnectionID] = item.Lifecycle
	}
	if _, present := listed["conn-gone"]; present {
		t.Fatalf("removed connection was listed to the operator: %+v", listed)
	}
	if listed["conn-live"] != "enabled" || listed["conn-pending"] != "removal_pending" {
		t.Fatalf("expected live and unfinished-removal connections to stay listed, got %+v", listed)
	}

	// The tombstone row itself survives for provenance.
	if _, err := store.GetConnection(ctx, "conn-gone"); err != nil {
		t.Fatalf("removed connection row was deleted: %v", err)
	}

	reconcilable, err := store.ListConnectionsAwaitingRemovalReconciliation(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pending := make(map[string]bool, len(reconcilable))
	for _, item := range reconcilable {
		pending[item.ConnectionID] = true
	}
	if !pending["conn-pending"] || !pending["conn-gone"] {
		t.Fatalf("startup reconciliation lost sight of removal work: %+v", pending)
	}
	if pending["conn-live"] {
		t.Fatalf("reconciliation picked up a live connection: %+v", pending)
	}
}

func TestCredentialMutationRejectsPendingAndRemovedConnections(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	ctx := context.Background()
	at := "2026-08-12T10:00:00Z"
	connection, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-credential", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", InstallationID: "2", CredentialHandle: "github-app:conn-credential", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-credential-create"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCredentialEvent(ctx, connection.ConnectionID, connection.RowVersion, "local-operator", "audit-credential-present", "credential.provisioned", at); err != nil {
		t.Fatal(err)
	}
	provisioned, err := store.GetConnection(ctx, connection.ConnectionID)
	if err != nil || provisioned.HealthClass != "never_checked" || provisioned.CredentialGeneration != 1 || provisioned.ValidatedGeneration != 0 || provisioned.LastSuccessAt != nil {
		t.Fatalf("credential provision did not invalidate health: %#v err=%v", provisioned, err)
	}
	if err := store.UpdateConnectionStateAudited(ctx, connection.ConnectionID, "removal_pending", "removal_pending", "", at, false, "local-operator", "audit-credential-pending", "connection.removal_pending"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetConnectionForCredentialMutation(ctx, connection.ConnectionID); !errors.Is(err, ErrExternalNotFound) {
		t.Fatalf("pending connection authorized credential mutation: %v", err)
	}
	if err := store.RecordCredentialEvent(ctx, connection.ConnectionID, connection.RowVersion, "local-operator", "audit-credential-stale", "credential.rotated", at); !errors.Is(err, ErrExternalConflict) {
		t.Fatalf("pending connection accepted credential audit: %v", err)
	}
	name := "Impossible edit"
	if _, err := store.UpdateConnection(ctx, connection.ConnectionID, UpdateExternalConnectionRequest{DisplayName: &name, ActorID: "local-operator", UpdatedAt: at, AuditEventID: "audit-credential-edit"}); !errors.Is(err, ErrExternalNotFound) {
		t.Fatalf("pending connection accepted metadata mutation: %v", err)
	}
}

func TestApplyRefreshPersistsTransferEvidence(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	ctx := context.Background()
	at := "2026-08-12T10:00:00Z"
	if _, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-transfer", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", InstallationID: "2", CredentialHandle: "github-app:conn-transfer", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-transfer-create"}); err != nil {
		t.Fatal(err)
	}
	first, _, err := store.CaptureIssue(ctx, CaptureExternalIssueRequest{SourceID: "source-transfer", SnapshotID: "snapshot-transfer-old", ConnectionID: "conn-transfer", ProviderNativeID: "100", RepositoryNativeID: "200", Issue: NormalizedExternalIssue{Title: "Moved", Body: "body", State: "open", Repository: "owner/old", IssueNumber: 7, CanonicalURL: "https://github.com/owner/old/issues/7", CreatedAt: at, UpdatedAt: at}, Profile: ExternalImportProfile{}, ProviderUpdatedAt: at, FetchedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateDecree(ctx, CreateDecreeRequest{DecreeID: "decree-transfer", Title: "D", Text: "immutable", CreatedAt: at, DirectSnapshotID: first.SnapshotID, NewLinkID: func() string { return "link-transfer" }, ActorID: "local-operator"}); err != nil {
		t.Fatal(err)
	}
	basis, err := store.GetRefreshBasis(ctx, "link-transfer")
	if err != nil {
		t.Fatal(err)
	}
	finished := "2026-08-12T11:00:00Z"
	capture := CaptureExternalIssueRequest{SourceID: basis.Link.SourceID, SnapshotID: "snapshot-transfer-new", ConnectionID: basis.Connection.ConnectionID, ProviderNativeID: basis.ProviderNativeID, RepositoryNativeID: "300", Issue: NormalizedExternalIssue{Title: "Moved", Body: "body", State: "open", Repository: "owner/new", IssueNumber: 9, CanonicalURL: "https://github.com/owner/new/issues/9", CreatedAt: at, UpdatedAt: finished}, Profile: basis.LatestSnapshot.Profile, ProviderUpdatedAt: finished, FetchedAt: finished}
	link, created, outcome, err := store.ApplyRefresh(ctx, ApplyExternalRefreshRequest{Basis: basis, Capture: &capture, ObservationID: "observation-transfer", AuditEventID: "audit-transfer-refresh", AliasID: "alias-transfer", ActorID: "local-operator", Outcome: "changed", StartedAt: finished, FinishedAt: finished})
	if err != nil || !created || outcome != "changed" || link.LatestSnapshotID != capture.SnapshotID {
		t.Fatalf("apply transfer link=%#v created=%v outcome=%q err=%v", link, created, outcome, err)
	}
	var transfer string
	if err := db.QueryRow(`SELECT transfer_json FROM external_source_observations WHERE observation_id='observation-transfer'`).Scan(&transfer); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{`"provider_native_id":"100"`, `"repository_id":"200"`, `"repository_id":"300"`, `"repository":"owner/old"`, `"repository":"owner/new"`} {
		if !strings.Contains(transfer, expected) {
			t.Fatalf("transfer evidence %s is missing %s", transfer, expected)
		}
	}
	if _, err := db.Exec(`UPDATE external_source_aliases SET repository_locator='owner/changed' WHERE alias_id='alias-transfer'`); err == nil || !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("expected immutable alias trigger, got %v", err)
	}
}

func TestTransferredSourceKeepsTargetProvenanceAndRefreshCoordinatesSeparate(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	ctx := context.Background()
	at := "2026-08-12T10:00:00Z"
	connection, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-two-links", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", InstallationID: "2", CredentialHandle: "github-app:conn-two-links", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-two-links-create"})
	if err != nil {
		t.Fatal(err)
	}
	oldIssue := NormalizedExternalIssue{Title: "Moved", Body: "body", State: "open", Repository: "owner/old", IssueNumber: 7, CanonicalURL: "https://github.com/owner/old/issues/7", CreatedAt: at, UpdatedAt: at}
	oldSnapshot, _, err := store.CaptureIssue(ctx, CaptureExternalIssueRequest{SourceID: "source-two-links", SnapshotID: "snapshot-two-links-old", ConnectionID: connection.ConnectionID, ProviderNativeID: "100", RepositoryNativeID: "200", Issue: oldIssue, Profile: ExternalImportProfile{}, ProviderUpdatedAt: at, FetchedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateDecree(ctx, CreateDecreeRequest{DecreeID: "decree-two-links", Title: "D", Text: "immutable", CreatedAt: at, DirectSnapshotID: oldSnapshot.SnapshotID, NewLinkID: func() string { return "link-two-links-decree" }, ActorID: "local-operator"}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO planning_sessions (planning_session_id,title,title_state,lifecycle_state,conversation_state,project_root,model_selection_json,created_at,updated_at) VALUES ('planning-two-links','Moved','custom','active','ready','/tmp','{}',?,?)`, at, at); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := createExternalLinkTx(ctx, tx, "link-two-links-planning", "planning_session", "planning-two-links", oldSnapshot.SourceID, oldSnapshot.SnapshotID, "local-operator", "adoption-two-links-old", at); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	decreeBasis, err := store.GetRefreshBasis(ctx, "link-two-links-decree")
	if err != nil {
		t.Fatal(err)
	}
	finished := "2026-08-12T11:00:00Z"
	newIssue := NormalizedExternalIssue{Title: "Moved", Body: "body", State: "open", Repository: "owner/new", IssueNumber: 9, CanonicalURL: "https://github.com/owner/new/issues/9", CreatedAt: at, UpdatedAt: finished}
	newCapture := CaptureExternalIssueRequest{SourceID: oldSnapshot.SourceID, SnapshotID: "snapshot-two-links-new", ConnectionID: connection.ConnectionID, ProviderNativeID: "100", RepositoryNativeID: "300", Issue: newIssue, Profile: oldSnapshot.Profile, ProviderUpdatedAt: finished, FetchedAt: finished}
	if _, created, outcome, err := store.ApplyRefresh(ctx, ApplyExternalRefreshRequest{Basis: decreeBasis, Capture: &newCapture, ObservationID: "observation-two-links-decree", AuditEventID: "audit-two-links-decree", AliasID: "alias-two-links-old", ActorID: "local-operator", Outcome: "changed", StartedAt: finished, FinishedAt: finished}); err != nil || !created || outcome != "changed" {
		t.Fatalf("decree transfer created=%v outcome=%q err=%v", created, outcome, err)
	}

	var currentRepositoryID, currentRepository string
	var currentIssueNumber int
	if err := db.QueryRow(`SELECT repository_native_id,repository_locator,issue_number FROM external_sources WHERE source_id=?`, oldSnapshot.SourceID).Scan(&currentRepositoryID, &currentRepository, &currentIssueNumber); err != nil {
		t.Fatal(err)
	}
	if currentRepositoryID != "300" || currentRepository != "owner/new" || currentIssueNumber != 9 {
		t.Fatalf("current source locator did not move: repository_id=%q repository=%q issue=%d", currentRepositoryID, currentRepository, currentIssueNumber)
	}
	decreeLinks, err := store.GetLinksForTarget(ctx, "decree", "decree-two-links")
	if err != nil || len(decreeLinks) != 1 {
		t.Fatalf("decree links=%#v err=%v", decreeLinks, err)
	}
	assertExternalLinkCoordinates(t, decreeLinks[0], oldSnapshot.SnapshotID, "", newCapture.SnapshotID, "owner/old", 7)

	planningLinks, err := store.GetLinksForTarget(ctx, "planning_session", "planning-two-links")
	if err != nil || len(planningLinks) != 1 {
		t.Fatalf("planning links=%#v err=%v", planningLinks, err)
	}
	assertExternalLinkCoordinates(t, planningLinks[0], oldSnapshot.SnapshotID, oldSnapshot.SnapshotID, oldSnapshot.SnapshotID, "owner/old", 7)
	planningBasis, err := store.GetRefreshBasis(ctx, "link-two-links-planning")
	if err != nil {
		t.Fatal(err)
	}
	assertExternalLinkCoordinates(t, planningBasis.Link, oldSnapshot.SnapshotID, oldSnapshot.SnapshotID, oldSnapshot.SnapshotID, "owner/old", 7)
	if planningBasis.FetchRepositoryNativeID != "300" || planningBasis.FetchRepository != "owner/new" || planningBasis.FetchIssueNumber != 9 || planningBasis.FetchCanonicalURL != newIssue.CanonicalURL {
		t.Fatalf("planning refresh did not follow current source locator: %#v", planningBasis)
	}

	secondCapture := newCapture
	secondCapture.SnapshotID = "snapshot-two-links-new-duplicate"
	secondCapture.FetchedAt = "2026-08-12T12:00:00Z"
	if _, created, outcome, err := store.ApplyRefresh(ctx, ApplyExternalRefreshRequest{Basis: planningBasis, Capture: &secondCapture, ObservationID: "observation-two-links-planning", AuditEventID: "audit-two-links-planning", AliasID: "alias-two-links-unused", ActorID: "local-operator", Outcome: "changed", StartedAt: secondCapture.FetchedAt, FinishedAt: secondCapture.FetchedAt}); err != nil || created || outcome != "changed" {
		t.Fatalf("planning refresh created=%v outcome=%q err=%v", created, outcome, err)
	}
	planningLinks, err = store.GetLinksForTarget(ctx, "planning_session", "planning-two-links")
	if err != nil || len(planningLinks) != 1 {
		t.Fatalf("refreshed planning links=%#v err=%v", planningLinks, err)
	}
	assertExternalLinkCoordinates(t, planningLinks[0], oldSnapshot.SnapshotID, oldSnapshot.SnapshotID, newCapture.SnapshotID, "owner/old", 7)
	if err := store.AdoptLatest(ctx, "link-two-links-planning", "adoption-two-links-new", "local-operator", "2026-08-12T12:01:00Z", "audit-two-links-adopt"); err != nil {
		t.Fatal(err)
	}
	planningLinks, err = store.GetLinksForTarget(ctx, "planning_session", "planning-two-links")
	if err != nil || len(planningLinks) != 1 {
		t.Fatalf("adopted planning links=%#v err=%v", planningLinks, err)
	}
	assertExternalLinkCoordinates(t, planningLinks[0], oldSnapshot.SnapshotID, newCapture.SnapshotID, newCapture.SnapshotID, "owner/new", 9)
	decreeLinks, err = store.GetLinksForTarget(ctx, "decree", "decree-two-links")
	if err != nil || len(decreeLinks) != 1 {
		t.Fatalf("final decree links=%#v err=%v", decreeLinks, err)
	}
	assertExternalLinkCoordinates(t, decreeLinks[0], oldSnapshot.SnapshotID, "", newCapture.SnapshotID, "owner/old", 7)

	for _, linkID := range []string{"link-two-links-decree", "link-two-links-planning"} {
		history, err := store.LinkHistory(ctx, linkID)
		if err != nil || len(history) != 2 || history[0].SnapshotID != newCapture.SnapshotID || history[1].SnapshotID != oldSnapshot.SnapshotID {
			t.Fatalf("history for %s=%#v err=%v", linkID, history, err)
		}
	}
	var decreeTransfer, planningTransfer string
	if err := db.QueryRow(`SELECT transfer_json FROM external_source_observations WHERE observation_id='observation-two-links-decree'`).Scan(&decreeTransfer); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT transfer_json FROM external_source_observations WHERE observation_id='observation-two-links-planning'`).Scan(&planningTransfer); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(decreeTransfer, `"repository":"owner/old"`) || !strings.Contains(decreeTransfer, `"repository":"owner/new"`) || planningTransfer != `{}` {
		t.Fatalf("incoherent transfer evidence decree=%s planning=%s", decreeTransfer, planningTransfer)
	}
	if err := NewAuditIntegrityVerifier(db).VerifyOrError(ctx); err != nil {
		t.Fatalf("external audit chain failed verification: %v", err)
	}
}

func assertExternalLinkCoordinates(t *testing.T, link ExternalLinkSummary, used, active, latest, repository string, issueNumber int) {
	t.Helper()
	if link.UsedSnapshotID != used || link.ActiveSnapshotID != active || link.LatestSnapshotID != latest || link.Repository != repository || link.IssueNumber != issueNumber || !strings.Contains(link.CanonicalURL, "/"+repository+"/issues/") {
		t.Fatalf("link coordinates=%#v, want used=%q active=%q latest=%q repository=%q issue=%d", link, used, active, latest, repository, issueNumber)
	}
}

func TestApplyRefreshPersistsActionableFailureStatus(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	ctx := context.Background()
	at := "2026-08-12T10:00:00Z"
	connection, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-failure", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", InstallationID: "2", CredentialHandle: "github-app:conn-failure", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-failure-create"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _, err := store.CaptureIssue(ctx, CaptureExternalIssueRequest{SourceID: "source-failure", SnapshotID: "snapshot-failure", ConnectionID: connection.ConnectionID, ProviderNativeID: "100", RepositoryNativeID: "200", Issue: NormalizedExternalIssue{Title: "Issue", Body: "body", State: "open", Repository: "owner/repo", IssueNumber: 7, CanonicalURL: "https://github.com/owner/repo/issues/7", CreatedAt: at, UpdatedAt: at}, Profile: ExternalImportProfile{}, ProviderUpdatedAt: at, FetchedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateDecree(ctx, CreateDecreeRequest{DecreeID: "decree-failure", Title: "D", Text: "immutable", CreatedAt: at, DirectSnapshotID: snapshot.SnapshotID, NewLinkID: func() string { return "link-failure" }, ActorID: "local-operator"}); err != nil {
		t.Fatal(err)
	}
	basis, err := store.GetRefreshBasis(ctx, "link-failure")
	if err != nil {
		t.Fatal(err)
	}
	finished := "2026-08-12T11:00:00Z"
	link, created, outcome, err := store.ApplyRefresh(ctx, ApplyExternalRefreshRequest{Basis: basis, ObservationID: "observation-failure", AuditEventID: "audit-failure-refresh", ActorID: "local-operator", Outcome: "revoked", StartedAt: finished, FinishedAt: finished})
	if err != nil || created || outcome != "revoked" {
		t.Fatalf("apply revoked refresh link=%#v created=%v outcome=%q err=%v", link, created, outcome, err)
	}
	if link.Status != "revoked" {
		t.Fatalf("actionable failure collapsed to %q", link.Status)
	}
	reloaded, err := store.GetLinksForTarget(ctx, "decree", "decree-failure")
	if err != nil {
		t.Fatal(err)
	}
	if len(reloaded) != 1 || reloaded[0].Status != "revoked" {
		t.Fatalf("reloaded failure links=%#v, want one revoked link", reloaded)
	}
}

func TestDetachPlanningExternalSourceStopsFutureOperationsAndAllowsReattach(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	planning := NewPlanningStore(db)
	ctx := context.Background()
	at := "2026-08-12T10:00:00Z"

	connection, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-detach", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", InstallationID: "2", CredentialHandle: "github-app:conn-detach", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-detach-connection"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateConnectionState(ctx, connection.ConnectionID, "enabled", "healthy", "", at, true, "10", "octocat"); err != nil {
		t.Fatal(err)
	}
	if connection, err = store.GetConnection(ctx, connection.ConnectionID); err != nil {
		t.Fatal(err)
	}
	mustExecArgs(t, db, `INSERT INTO planning_sessions (planning_session_id,title,title_state,lifecycle_state,conversation_state,project_root,model_selection_json,created_at,updated_at) VALUES ('session-detach','Detach','custom','active','ready','/tmp','{}',?,?)`, at, at)
	mustExecArgs(t, db, `INSERT INTO planning_context_snapshots (planning_context_snapshot_id,planning_session_id,summary_markdown,included_files_json,excluded_files_json,created_at) VALUES ('context-detach','session-detach','context','[]','[]',?)`, at)
	mustExec(t, db, `UPDATE planning_sessions SET context_snapshot_id='context-detach' WHERE planning_session_id='session-detach'`)

	capture := CaptureExternalIssueRequest{SourceID: "source-detach", SnapshotID: "snapshot-detach-a", ConnectionID: connection.ConnectionID, ConnectionRowVersion: connection.RowVersion, ProviderNativeID: "100", RepositoryNativeID: "200", Issue: NormalizedExternalIssue{Title: "Version A", Body: "first", State: "open", Repository: "owner/repo", IssueNumber: 7, CanonicalURL: "https://github.com/owner/repo/issues/7", CreatedAt: at, UpdatedAt: at}, Profile: ExternalImportProfile{}, ProviderUpdatedAt: at, FetchedAt: at}
	link, err := store.AttachPlanningExternalSource(ctx, AttachPlanningExternalSourceRequest{PlanningSessionID: "session-detach", Capture: capture, LinkID: "link-detach", AdoptionID: "adoption-detach", AuditEventID: "audit-detach-attach", ActorID: "local-operator", AttachedAt: at})
	if err != nil {
		t.Fatal(err)
	}

	// An operation enqueued while the source is attached keeps its binding
	// forever: detaching must not rewrite queued or historical work.
	if _, err := planning.CreateOperation(ctx, CreatePlanningOperationRequest{OperationID: "operation-before-detach", PlanningSessionID: "session-detach", Type: PlanningOperationTypeReply, Status: PlanningOperationStatusQueued, RequestPayloadJSON: `{}`, CreatedAt: at}); err != nil {
		t.Fatal(err)
	}

	mustExec(t, db, `UPDATE planning_operations SET status='succeeded' WHERE operation_id='operation-before-detach'`)

	detachedAt := "2026-08-12T11:00:00Z"
	if err := store.DetachPlanningExternalSource(ctx, DetachPlanningExternalSourceRequest{LinkID: link.LinkID, ChangeID: "change-detach", ActorID: "local-operator", DetachedAt: detachedAt, AuditEventID: "audit-detach"}); err != nil {
		t.Fatal(err)
	}

	links, err := store.GetLinksForTarget(ctx, "planning_session", "session-detach")
	if err != nil {
		t.Fatal(err)
	}
	if len(links) != 0 {
		t.Fatalf("detached source is still presented: %#v", links)
	}
	if err := store.DetachPlanningExternalSource(ctx, DetachPlanningExternalSourceRequest{LinkID: link.LinkID, ChangeID: "change-detach-repeat", ActorID: "local-operator", DetachedAt: detachedAt, AuditEventID: "audit-detach-repeat"}); !errors.Is(err, ErrExternalSourceNotAttached) {
		t.Fatalf("repeat detach error=%v", err)
	}

	if _, err := planning.CreateOperation(ctx, CreatePlanningOperationRequest{OperationID: "operation-after-detach", PlanningSessionID: "session-detach", Type: PlanningOperationTypeReply, Status: PlanningOperationStatusQueued, RequestPayloadJSON: `{}`, CreatedAt: detachedAt}); err != nil {
		t.Fatal(err)
	}
	_, afterIDs, err := planning.LoadPlanningOperationExternalContext(ctx, "operation-after-detach")
	if err != nil {
		t.Fatal(err)
	}
	if len(afterIDs) != 0 {
		t.Fatalf("detached source still fed a later operation: %#v", afterIDs)
	}
	beforeText, beforeIDs, err := planning.LoadPlanningOperationExternalContext(ctx, "operation-before-detach")
	if err != nil {
		t.Fatal(err)
	}
	if len(beforeIDs) != 1 || beforeIDs[0] != capture.SnapshotID || !strings.Contains(beforeText, "Version A") {
		t.Fatalf("detach changed an already-bound operation: ids=%#v text=%s", beforeIDs, beforeText)
	}

	mustExec(t, db, `UPDATE planning_operations SET status='succeeded' WHERE operation_id='operation-after-detach'`)

	// Re-adding the same issue reuses the immutable link and resumes from what
	// was just captured.
	reattachAt := "2026-08-12T12:00:00Z"
	fresh := capture
	fresh.SourceID, fresh.SnapshotID, fresh.PredecessorSnapshotID = "source-ignored", "snapshot-detach-b", capture.SnapshotID
	fresh.Issue.Title, fresh.Issue.Body, fresh.Issue.UpdatedAt = "Version B", "second", reattachAt
	fresh.ProviderUpdatedAt, fresh.FetchedAt = reattachAt, reattachAt
	reattached, err := store.AttachPlanningExternalSource(ctx, AttachPlanningExternalSourceRequest{PlanningSessionID: "session-detach", Capture: fresh, LinkID: "change-reattach", AdoptionID: "adoption-reattach", AuditEventID: "audit-reattach", ActorID: "local-operator", AttachedAt: reattachAt})
	if err != nil {
		t.Fatal(err)
	}
	if reattached.LinkID != link.LinkID {
		t.Fatalf("re-adding a removed source created a second link: %q want %q", reattached.LinkID, link.LinkID)
	}
	if reattached.UsedSnapshotID != capture.SnapshotID || reattached.ActiveSnapshotID != fresh.SnapshotID {
		t.Fatalf("re-attach provenance: used=%q active=%q", reattached.UsedSnapshotID, reattached.ActiveSnapshotID)
	}
	links, err = store.GetLinksForTarget(ctx, "planning_session", "session-detach")
	if err != nil {
		t.Fatal(err)
	}
	if len(links) != 1 || links[0].LinkID != link.LinkID || links[0].ActiveSnapshotID != fresh.SnapshotID {
		t.Fatalf("re-added source not presented from current content: %#v", links)
	}
	if _, err := planning.CreateOperation(ctx, CreatePlanningOperationRequest{OperationID: "operation-after-reattach", PlanningSessionID: "session-detach", Type: PlanningOperationTypeReply, Status: PlanningOperationStatusQueued, RequestPayloadJSON: `{}`, CreatedAt: reattachAt}); err != nil {
		t.Fatal(err)
	}
	afterText, afterReattachIDs, err := planning.LoadPlanningOperationExternalContext(ctx, "operation-after-reattach")
	if err != nil {
		t.Fatal(err)
	}
	if len(afterReattachIDs) != 1 || afterReattachIDs[0] != fresh.SnapshotID || !strings.Contains(afterText, "Version B") {
		t.Fatalf("re-added source did not feed later operations: ids=%#v text=%s", afterReattachIDs, afterText)
	}
}

func TestDetachPlanningExternalSourceRejectsUnsupportedTargets(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	ctx := context.Background()
	at := "2026-08-12T10:00:00Z"

	if _, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-detach-guard", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", InstallationID: "2", CredentialHandle: "github-app:conn-detach-guard", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-detach-guard"}); err != nil {
		t.Fatal(err)
	}
	snapshot, _, err := store.CaptureIssue(ctx, CaptureExternalIssueRequest{SourceID: "source-guard", SnapshotID: "snapshot-guard", ConnectionID: "conn-detach-guard", ProviderNativeID: "100", RepositoryNativeID: "200", Issue: NormalizedExternalIssue{Title: "Issue", Body: "body", State: "open", Repository: "owner/repo", IssueNumber: 7, CanonicalURL: "https://github.com/owner/repo/issues/7", CreatedAt: at, UpdatedAt: at}, Profile: ExternalImportProfile{}, ProviderUpdatedAt: at, FetchedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	mustExecArgs(t, db, `INSERT INTO planning_sessions (planning_session_id,title,title_state,lifecycle_state,conversation_state,project_root,model_selection_json,created_at,updated_at) VALUES ('session-archived-detach','Archived','custom','archived','ready','/tmp','{}',?,?)`, at, at)
	mustExecArgs(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree-1', 'Decree', 'text', '{}', ?)`, at)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := createExternalLinkTx(ctx, tx, "link-decree", "decree", "decree-1", snapshot.SourceID, snapshot.SnapshotID, "local-operator", "", at); err != nil {
		t.Fatal(err)
	}
	if err := createExternalLinkTx(ctx, tx, "link-archived", "planning_session", "session-archived-detach", snapshot.SourceID, snapshot.SnapshotID, "local-operator", "adopt-archived", at); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Issued decrees keep their exact source; only planning sources are removable.
	if err := store.DetachPlanningExternalSource(ctx, DetachPlanningExternalSourceRequest{LinkID: "link-decree", ChangeID: "change-decree", ActorID: "local-operator", DetachedAt: at, AuditEventID: "audit-decree-detach"}); !errors.Is(err, ErrExternalDetachUnsupported) {
		t.Fatalf("decree detach error=%v", err)
	}
	if err := store.DetachPlanningExternalSource(ctx, DetachPlanningExternalSourceRequest{LinkID: "link-archived", ChangeID: "change-archived", ActorID: "local-operator", DetachedAt: at, AuditEventID: "audit-archived-detach"}); !errors.Is(err, ErrExternalPlanningSessionInactive) {
		t.Fatalf("archived session detach error=%v", err)
	}
	if err := store.DetachPlanningExternalSource(ctx, DetachPlanningExternalSourceRequest{LinkID: "link-missing", ChangeID: "change-missing", ActorID: "local-operator", DetachedAt: at, AuditEventID: "audit-missing-detach"}); !errors.Is(err, ErrExternalNotFound) {
		t.Fatalf("unknown link detach error=%v", err)
	}
	var changes int
	if err := db.QueryRow(`SELECT COUNT(*) FROM external_source_link_attachment_changes`).Scan(&changes); err != nil {
		t.Fatal(err)
	}
	if changes != 0 {
		t.Fatalf("rejected detach wrote %d attachment changes", changes)
	}
}

func includedRepositoriesFixture(t *testing.T) (*ExternalStore, context.Context, ExternalConnection) {
	t.Helper()
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	ctx := context.Background()
	at := "2026-08-19T10:00:00Z"
	connection, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-1", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", InstallationID: "2", CredentialHandle: "github-app:conn-1", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-create"})
	if err != nil {
		t.Fatal(err)
	}
	return store, ctx, connection
}

func TestConnectionTracksTheWholeGrantUntilItIsNarrowed(t *testing.T) {
	store, ctx, connection := includedRepositoriesFixture(t)
	if connection.IncludedRepositoryMode != "all" {
		t.Fatalf("new connection mode=%q, want all so it tracks the installation grant", connection.IncludedRepositoryMode)
	}
	included, err := store.GetIncludedRepositories(ctx, connection.ConnectionID)
	if err != nil || len(included) != 0 {
		t.Fatalf("all-mode connection stores no repositories: included=%#v err=%v", included, err)
	}
}

func TestNarrowingAConnectionReplacesTheSetAndIsAudited(t *testing.T) {
	store, ctx, connection := includedRepositoriesFixture(t)
	at := "2026-08-19T11:00:00Z"
	narrowed, err := store.SetIncludedRepositories(ctx, connection.ConnectionID, SetIncludedRepositoriesRequest{
		Mode:               "selected",
		Repositories:       []IncludedRepository{{RepositoryNativeID: "200", RepositoryLocator: "octocat/api"}, {RepositoryNativeID: "201", RepositoryLocator: "octocat/web"}},
		ExpectedRowVersion: connection.RowVersion, ActorID: "local-operator", UpdatedAt: at, AuditEventID: "audit-narrow",
	})
	if err != nil {
		t.Fatal(err)
	}
	if narrowed.IncludedRepositoryMode != "selected" || narrowed.RowVersion != connection.RowVersion+1 {
		t.Fatalf("narrowed=%#v", narrowed)
	}
	included, err := store.GetIncludedRepositories(ctx, connection.ConnectionID)
	if err != nil || len(included) != 2 || included[0].RepositoryLocator != "octocat/api" || included[1].RepositoryNativeID != "201" {
		t.Fatalf("included=%#v err=%v", included, err)
	}

	// Replacing the set is a whole-set write, not an append.
	replaced, err := store.SetIncludedRepositories(ctx, connection.ConnectionID, SetIncludedRepositoriesRequest{
		Mode:               "selected",
		Repositories:       []IncludedRepository{{RepositoryNativeID: "202", RepositoryLocator: "octocat/infra"}},
		ExpectedRowVersion: narrowed.RowVersion, ActorID: "local-operator", UpdatedAt: at, AuditEventID: "audit-replace",
	})
	if err != nil {
		t.Fatal(err)
	}
	included, _ = store.GetIncludedRepositories(ctx, connection.ConnectionID)
	if len(included) != 1 || included[0].RepositoryNativeID != "202" {
		t.Fatalf("replacement did not replace: %#v", included)
	}

	// Returning to all drops the narrowing entirely.
	widened, err := store.SetIncludedRepositories(ctx, connection.ConnectionID, SetIncludedRepositoriesRequest{
		Mode: "all", ExpectedRowVersion: replaced.RowVersion, ActorID: "local-operator", UpdatedAt: at, AuditEventID: "audit-widen",
	})
	if err != nil || widened.IncludedRepositoryMode != "all" {
		t.Fatalf("widened=%#v err=%v", widened, err)
	}
	included, _ = store.GetIncludedRepositories(ctx, connection.ConnectionID)
	if len(included) != 0 {
		t.Fatalf("all mode still stores rows: %#v", included)
	}

	var audited int
	_ = store.db.QueryRow(`SELECT COUNT(*) FROM external_integration_audit_events WHERE event_type='connection.included_repositories_changed'`).Scan(&audited)
	if audited != 3 {
		t.Fatalf("audit events=%d, want one per change", audited)
	}
}

func TestNarrowingRejectsStaleRowVersionAndAnEmptySelection(t *testing.T) {
	store, ctx, connection := includedRepositoriesFixture(t)
	at := "2026-08-19T11:00:00Z"
	if _, err := store.SetIncludedRepositories(ctx, connection.ConnectionID, SetIncludedRepositoriesRequest{
		Mode: "selected", ExpectedRowVersion: connection.RowVersion, ActorID: "local-operator", UpdatedAt: at, AuditEventID: "audit-empty",
	}); err == nil {
		t.Fatal("selected mode with no repositories must be rejected")
	}
	if _, err := store.SetIncludedRepositories(ctx, connection.ConnectionID, SetIncludedRepositoriesRequest{
		Mode:               "selected",
		Repositories:       []IncludedRepository{{RepositoryNativeID: "200", RepositoryLocator: "octocat/api"}},
		ExpectedRowVersion: connection.RowVersion + 7, ActorID: "local-operator", UpdatedAt: at, AuditEventID: "audit-stale",
	}); !errors.Is(err, ErrExternalConflict) {
		t.Fatalf("stale row version error=%v, want conflict", err)
	}
	included, _ := store.GetIncludedRepositories(ctx, connection.ConnectionID)
	if len(included) != 0 {
		t.Fatalf("a rejected narrowing must leave nothing behind: %#v", included)
	}
}
