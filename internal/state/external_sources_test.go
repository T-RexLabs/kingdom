package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"unicode/utf8"
)

func readExternalSnapshotForTest(ctx context.Context, store *ExternalStore, id string) (ExternalSourceSnapshot, []ExternalComment, error) {
	snapshot, err := store.GetSnapshotMetadata(ctx, id)
	if err != nil {
		return snapshot, nil, err
	}
	var comments []ExternalComment
	err = store.SnapshotComments(id).Replay(ctx, func(c ExternalComment) error { comments = append(comments, c); return nil })
	return snapshot, comments, err
}

type changingExternalCommentSource struct {
	reads  int
	closed bool
}

func (s *changingExternalCommentSource) Replay(ctx context.Context, visit func(ExternalComment) error) error {
	s.reads++
	return visit(ExternalComment{ProviderCommentID: "1", Body: fmt.Sprintf("version-%d", s.reads)})
}
func (s *changingExternalCommentSource) Close() error { s.closed = true; return nil }

func TestExternalCommentCaptureRejectsChangedReplayAtomically(t *testing.T) {
	fixture := newPlanningExternalBindingFixture(t, "changing-comments")
	source := &changingExternalCommentSource{}
	req := CaptureExternalIssueRequest{SourceID: "changing-source", SnapshotID: "changing-snapshot", ConnectionID: "conn-changing-comments", ProviderNativeID: "100", RepositoryNativeID: "200", Issue: NormalizedExternalIssue{Title: "Issue", Body: "body", State: "open", Repository: "owner/repo", IssueNumber: 7, CanonicalURL: "https://github.com/owner/repo/issues/7", CreatedAt: fixture.at, UpdatedAt: fixture.at}, Profile: ExternalImportProfile{IncludeComments: true}, FetchedAt: fixture.at, Comments: source}
	if _, _, err := fixture.external.CaptureIssue(fixture.ctx, req); err == nil || !strings.Contains(err.Error(), "changed during replay") {
		t.Fatalf("mutable comment source accepted: %v", err)
	}
	if !source.closed {
		t.Fatal("failed capture retained its comment source")
	}
	var count int
	if err := fixture.db.QueryRow(`SELECT COUNT(*) FROM external_source_snapshots WHERE snapshot_id='changing-snapshot'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("partial snapshot committed: %d %v", count, err)
	}
}

func TestCompleteCommentBaselinePreservesSnapshotProvenance(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	at := "2026-08-12T10:00:00Z"
	_, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "migration-connection", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []ConnectionInstallation{{InstallationID: "2"}}, CredentialHandle: "github-app:migration-connection", ActorID: "operator", CreatedAt: at, AuditEventID: "migration-audit"})
	if err != nil {
		t.Fatal(err)
	}
	req := CaptureExternalIssueRequest{SourceID: "migration-source", SnapshotID: "migration-old", ConnectionID: "migration-connection", ProviderNativeID: "100", RepositoryNativeID: "200", Issue: NormalizedExternalIssue{Title: "Issue", Body: "body", State: "open", Repository: "owner/repo", IssueNumber: 7, CanonicalURL: "https://github.com/owner/repo/issues/7", CreatedAt: at, UpdatedAt: at}, Profile: ExternalImportProfile{IncludeComments: true}, FetchedAt: at, ProviderCommentCount: 1, FetchedCommentCount: 1, FetchedCommentPages: 1, Comments: ExternalCommentList{{ProviderCommentID: "1", Body: "original complete comment"}}}
	old, _, err := store.CaptureIssue(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateWorkOrder(ctx, CreateWorkOrderRequest{WorkOrderID: "migration-work-order", Title: "Work Order", Text: "immutable", CreatedAt: at, DirectSnapshotID: old.SnapshotID, NewLinkID: func() string { return "migration-link" }, ActorID: "operator"}); err != nil {
		t.Fatal(err)
	}
	spool, err := NewExternalCommentSpool()
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Close()
	body := strings.Repeat("界", 14000) + "tail"
	for i := 0; i < 205; i++ {
		if err := spool.Append(ExternalComment{ProviderCommentID: fmt.Sprint(i), Body: body}); err != nil {
			t.Fatal(err)
		}
	}
	if err := spool.Seal(); err != nil {
		t.Fatal(err)
	}
	req.SnapshotID = "migration-full"
	req.PredecessorSnapshotID = old.SnapshotID
	req.Comments = spool
	req.ProviderCommentCount = 205
	req.FetchedCommentCount = 205
	req.FetchedCommentPages = 3
	req.OmittedCommentCount = 0
	req.OmittedCommentPages = 0
	full, _, err := store.CaptureIssue(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if full.RetainedCommentCount != 205 || full.RetainedCommentBytes != 205*len(body) || full.CommentsTruncated {
		t.Fatalf("full capture metadata: %#v", full)
	}
	after, comments, err := readExternalSnapshotForTest(ctx, store, old.SnapshotID)
	if err != nil || after != old || len(comments) != 1 || comments[0].Body != "original complete comment" || comments[0].TruncationReason != "" {
		t.Fatalf("new capture rewrote original source: %v", err)
	}
	for _, stmt := range []string{`UPDATE external_source_snapshots SET content_digest='changed' WHERE snapshot_id='migration-old'`, `DELETE FROM external_source_snapshot_comments WHERE snapshot_id='migration-old'`, `INSERT INTO external_source_snapshot_comments VALUES('migration-old','extra',1,'','injected','','','',8,8,'')`} {
		if _, err := db.Exec(stmt); err == nil {
			t.Fatalf("immutable source accepted %s", stmt)
		}
	}
	links, err := store.GetLinksForTarget(ctx, "work_order", "migration-work-order")
	if err != nil || len(links) != 1 || links[0].UsedSnapshotID != old.SnapshotID {
		t.Fatalf("historical work order binding changed: %v", err)
	}
	rows, err := db.Query(`PRAGMA foreign_key_check`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("snapshot capture broke foreign keys")
	}
}

func TestExternalSourceCaptureDeduplicatesAndPreservesImmutableWorkOrderProvenance(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	ctx := context.Background()
	at := "2026-08-12T10:00:00Z"
	_, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-1", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []ConnectionInstallation{{InstallationID: "2", AccountID: "11", AccountLogin: "owner"}}, CredentialHandle: "github-app:conn-1", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-1"})
	if err != nil {
		t.Fatal(err)
	}
	base := CaptureExternalIssueRequest{SourceID: "source-1", SnapshotID: "snapshot-1", ConnectionID: "conn-1", ProviderNativeID: "100", RepositoryNativeID: "200", Issue: NormalizedExternalIssue{Title: "Issue", Body: "body", State: "open", Repository: "owner/repo", IssueNumber: 7, CanonicalURL: "https://github.com/owner/repo/issues/7", CreatedAt: at, UpdatedAt: at}, Profile: ExternalImportProfile{IncludeComments: true}, ProviderUpdatedAt: at, FetchedAt: at, Comments: ExternalCommentList{{ProviderCommentID: "comment-1", Author: "octocat", Body: "immutable comment", CanonicalURL: "https://github.com/owner/repo/issues/7#issuecomment-1", CreatedAt: at, UpdatedAt: at}}}
	first, created, err := store.CaptureIssue(ctx, base)
	if err != nil || !created {
		t.Fatalf("first capture: created=%v err=%v", created, err)
	}
	base.SourceID = "source-ignored"
	base.SnapshotID = "snapshot-ignored"
	base.FetchedAt = "2026-08-12T11:00:00Z"
	base.ETag = `"new-validator-same-content"`
	second, created, err := store.CaptureIssue(ctx, base)
	if err != nil || created {
		t.Fatalf("duplicate capture: created=%v err=%v", created, err)
	}
	if second != first {
		t.Fatalf("capture did not deduplicate: %#v %#v", first, second)
	}
	for i, id := range []string{"work order-1", "work order-2"} {
		if err = store.CreateWorkOrder(ctx, CreateWorkOrderRequest{WorkOrderID: id, Title: "D", Text: "immutable", CreatedAt: at, DirectSnapshotID: first.SnapshotID, NewLinkID: func() string { return "link-" + id }, ActorID: "local-operator"}); err != nil {
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

func TestRefreshBasisRetainsRepositoryGrantingInstallation(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	ctx := context.Background()
	at := "2026-08-12T10:00:00Z"
	if _, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-six", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []ConnectionInstallation{{InstallationID: "6", AccountID: "16", AccountLogin: "owner"}}, CredentialHandle: "github-app:conn-six", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-six"}); err != nil {
		t.Fatal(err)
	}
	snapshot, _, err := store.CaptureIssue(ctx, CaptureExternalIssueRequest{SourceID: "source-six", SnapshotID: "snapshot-six", ConnectionID: "conn-six", ProviderNativeID: "100", RepositoryNativeID: "600", RepositoryInstallationID: "6", Issue: NormalizedExternalIssue{Title: "Issue", Body: "body", State: "open", Repository: "owner/six", IssueNumber: 7, CanonicalURL: "https://github.com/owner/six/issues/7", CreatedAt: at, UpdatedAt: at}, Profile: ExternalImportProfile{}, ProviderUpdatedAt: at, FetchedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateWorkOrder(ctx, CreateWorkOrderRequest{WorkOrderID: "work-order-six", Title: "WorkOrder", Text: "text", CreatedAt: at, DirectSnapshotID: snapshot.SnapshotID, NewLinkID: func() string { return "link-six" }, ActorID: "local-operator"}); err != nil {
		t.Fatal(err)
	}
	basis, err := store.GetRefreshBasis(ctx, "link-six")
	if err != nil || basis.FetchRepositoryNativeID != "600" || basis.FetchRepositoryInstallationID != "6" {
		t.Fatalf("refresh basis=%#v err=%v", basis, err)
	}
}

func TestCreateWorkOrderExternalLinkRollsBackOnUnknownSnapshot(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	err := store.CreateWorkOrder(context.Background(), CreateWorkOrderRequest{WorkOrderID: "work-order-x", Title: "D", Text: "T", CreatedAt: "2026-08-12T10:00:00Z", DirectSnapshotID: "missing", NewLinkID: func() string { return "link-x" }, ActorID: "local-operator"})
	if !errors.Is(err, ErrExternalNotFound) {
		t.Fatalf("expected external not found, got %v", err)
	}
	var count int
	_ = db.QueryRow(`SELECT COUNT(*) FROM work_orders WHERE work_order_id='work-order-x'`).Scan(&count)
	if count != 0 {
		t.Fatal("work order escaped failed transaction")
	}
}

func TestCreateWorkOrderDerivesExactFormalizeBindingsAtomically(t *testing.T) {
	fixture := newWorkOrderLineageFixture(t)
	linkIndex := 0
	err := fixture.store.CreateWorkOrder(context.Background(), CreateWorkOrderRequest{
		WorkOrderID: "work-order-planned", Title: "Planned", Text: "Exact draft", CreatedAt: fixture.at,
		Metadata: map[string]any{
			"priority":                     "high",
			"planning_session_id":          "untrusted-session",
			"planning_draft_id":            "untrusted-draft",
			"planning_context_snapshot_id": "untrusted-context",
		},
		PlanningLineage: &WorkOrderPlanningLineage{PlanningSessionID: "session-a", PlanningDraftID: "draft-a"},
		NewLinkID: func() string {
			linkIndex++
			return fmt.Sprintf("work-order-link-%d", linkIndex)
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	rows, err := fixture.db.Query(`SELECT used_snapshot_id FROM external_source_links WHERE target_kind='work_order' AND target_id='work-order-planned' ORDER BY used_snapshot_id`)
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
		t.Fatalf("work order snapshots=%q, want %q", got, want)
	}
	links, err := fixture.store.GetLinksForTarget(context.Background(), "work_order", "work-order-planned")
	if err != nil {
		t.Fatal(err)
	}
	if len(links) != 2 || links[0].LinkID != "work-order-link-1" || links[0].UsedSnapshotID != "snapshot-a" || links[1].LinkID != "work-order-link-2" || links[1].UsedSnapshotID != "snapshot-b" {
		t.Fatalf("deterministic work order links=%#v", links)
	}

	var metadataJSON string
	if err := fixture.db.QueryRow(`SELECT metadata_json FROM work_orders WHERE work_order_id='work-order-planned'`).Scan(&metadataJSON); err != nil {
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

func TestCreateWorkOrderRejectsInvalidPlanningLineageWithoutWrites(t *testing.T) {
	tests := []struct {
		name    string
		lineage WorkOrderPlanningLineage
	}{
		{name: "cross-session draft", lineage: WorkOrderPlanningLineage{PlanningSessionID: "session-a", PlanningDraftID: "draft-b"}},
		{name: "formalize missing context binding", lineage: WorkOrderPlanningLineage{PlanningSessionID: "session-a", PlanningDraftID: "draft-unbound"}},
		{name: "failed formalize", lineage: WorkOrderPlanningLineage{PlanningSessionID: "session-a", PlanningDraftID: "draft-failed"}},
		{name: "non-formalize operation", lineage: WorkOrderPlanningLineage{PlanningSessionID: "session-a", PlanningDraftID: "draft-reply"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newWorkOrderLineageFixture(t)
			err := fixture.store.CreateWorkOrder(context.Background(), CreateWorkOrderRequest{WorkOrderID: "work-order-invalid", Title: "D", Text: "T", CreatedAt: fixture.at, PlanningLineage: &test.lineage, NewLinkID: func() string { return "unused-link" }})
			if !errors.Is(err, ErrWorkOrderLineageInvalid) {
				t.Fatalf("expected invalid lineage, got %v", err)
			}
			assertWorkOrderProvenanceWriteCount(t, fixture.db, "work-order-invalid", 0, 0)
		})
	}
}

func TestCreateWorkOrderRejectsPlanningAndDirectSnapshotWithoutWrites(t *testing.T) {
	fixture := newWorkOrderLineageFixture(t)
	err := fixture.store.CreateWorkOrder(context.Background(), CreateWorkOrderRequest{WorkOrderID: "work-order-mixed", Title: "D", Text: "T", CreatedAt: fixture.at, DirectSnapshotID: "snapshot-a", PlanningLineage: &WorkOrderPlanningLineage{PlanningSessionID: "session-a", PlanningDraftID: "draft-a"}, NewLinkID: func() string { return "unused-link" }})
	if !errors.Is(err, ErrWorkOrderLineageInvalid) {
		t.Fatalf("expected invalid lineage, got %v", err)
	}
	assertWorkOrderProvenanceWriteCount(t, fixture.db, "work-order-mixed", 0, 0)
}

func TestCreateWorkOrderRejectsDuplicateSourceBindingsWithoutWrites(t *testing.T) {
	fixture := newWorkOrderLineageFixture(t)
	if _, err := fixture.db.Exec(`INSERT INTO planning_operation_external_snapshots (operation_id,snapshot_id,assembler_version,context_digest,issue_bytes,comment_bytes,bound_at) VALUES ('operation-good','snapshot-a-later',?, ?,0,0,?)`, ExternalAssemblerVersion, strings.Repeat("0", 64), fixture.at); err != nil {
		t.Fatal(err)
	}
	err := fixture.store.CreateWorkOrder(context.Background(), CreateWorkOrderRequest{WorkOrderID: "work-order-conflict", Title: "D", Text: "T", CreatedAt: fixture.at, PlanningLineage: &WorkOrderPlanningLineage{PlanningSessionID: "session-a", PlanningDraftID: "draft-a"}, NewLinkID: func() string { return "unused-link" }})
	if !errors.Is(err, ErrWorkOrderProvenanceConflict) {
		t.Fatalf("expected provenance conflict, got %v", err)
	}
	assertWorkOrderProvenanceWriteCount(t, fixture.db, "work-order-conflict", 0, 0)
}

type workOrderLineageFixture struct {
	db    *sql.DB
	store *ExternalStore
	at    string
}

func newWorkOrderLineageFixture(t *testing.T) workOrderLineageFixture {
	t.Helper()
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	ctx := context.Background()
	at := "2026-08-12T10:00:00Z"
	if _, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-lineage", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []ConnectionInstallation{{InstallationID: "2"}}, CredentialHandle: "github-app:conn-lineage", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-lineage"}); err != nil {
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
		mustExecArgs(t, db, `INSERT INTO planning_drafts (planning_draft_id,planning_session_id,work_order_title,work_order_text,summary_json,created_at) VALUES (?,?,?,?,'{}',?)`, draft.id, draft.session, draft.id, "text", at)
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
	return workOrderLineageFixture{db: db, store: store, at: at}
}

func mustExecArgs(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatal(err)
	}
}

func assertWorkOrderProvenanceWriteCount(t *testing.T, db *sql.DB, workOrderID string, wantWorkOrders, wantLinks int) {
	t.Helper()
	var workOrders, links int
	if err := db.QueryRow(`SELECT COUNT(*) FROM work_orders WHERE work_order_id=?`, workOrderID).Scan(&workOrders); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM external_source_links WHERE target_kind='work_order' AND target_id=?`, workOrderID).Scan(&links); err != nil {
		t.Fatal(err)
	}
	if workOrders != wantWorkOrders || links != wantLinks {
		t.Fatalf("writes for %s: work orders=%d links=%d, want %d/%d", workOrderID, workOrders, links, wantWorkOrders, wantLinks)
	}
}

func TestCaptureValidationAcceptsCompleteIssueContent(t *testing.T) {
	req := CaptureExternalIssueRequest{
		SourceID: "source-boundary", SnapshotID: "snapshot-boundary", ConnectionID: "connection-boundary", ProviderNativeID: "100", RepositoryNativeID: "200",
		Issue: NormalizedExternalIssue{
			Title:        strings.Repeat("界", 2048),
			Body:         strings.Repeat("b", 2<<20),
			State:        "open",
			Repository:   "owner/repo",
			IssueNumber:  1,
			CanonicalURL: "https://github.com/owner/repo/issues/1",
			CreatedAt:    "2026-08-12T10:00:00Z",
			UpdatedAt:    "2026-08-12T10:00:00Z",
		},
		FetchedAt: "2026-08-12T10:00:00Z",
	}
	if err := validateCapture(req); err != nil {
		t.Fatalf("complete external issue rejected: %v", err)
	}
}

func TestExternalCommentSpoolPreservesFullUnicodeAndReplay(t *testing.T) {
	spool, err := NewExternalCommentSpool()
	if err != nil {
		t.Fatal(err)
	}
	defer spool.Close()
	body := strings.Repeat("x", 40<<10) + "界-tail"
	for i := 0; i < 205; i++ {
		if err := spool.Append(ExternalComment{ProviderCommentID: fmt.Sprint(i), Body: body}); err != nil {
			t.Fatal(err)
		}
	}
	if err := spool.Seal(); err != nil {
		t.Fatal(err)
	}
	for pass := 0; pass < 2; pass++ {
		count := 0
		err := ReplayExternalComments(context.Background(), spool, func(c ExternalComment) error {
			if c.ProviderCommentID != fmt.Sprint(count) || c.Body != body || c.RetainedBytes != len(body) || c.TruncationReason != "" {
				t.Fatal("comment changed across spool replay")
			}
			count++
			return nil
		})
		if err != nil || count != 205 {
			t.Fatalf("replay count=%d err=%v", count, err)
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

	if _, err := external.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-bind", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []ConnectionInstallation{{InstallationID: "2"}}, CredentialHandle: "github-app:conn-bind", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-bind"}); err != nil {
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
	connection, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-attach", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []ConnectionInstallation{{InstallationID: "2"}}, CredentialHandle: "github-app:conn-attach", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-attach-connection"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCredentialEvent(ctx, connection.ConnectionID, connection.RowVersion, 1, "local-operator", "audit-attach-credential", "credential.provisioned", at); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateConnectionState(ctx, connection.ConnectionID, "enabled", "healthy", "", at, true, []ConnectionInstallation{{InstallationID: "2", AccountID: "10", AccountLogin: "octocat"}}); err != nil {
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

func TestPlanningOperationExternalContextBindsEveryActiveSource(t *testing.T) {
	fixture := newPlanningExternalBindingFixture(t, "source-cap")
	const sources = 205
	for i := 0; i < sources; i++ {
		snapshot := fixture.capture(t,
			fmt.Sprintf("source-%02d", i),
			fmt.Sprintf("snapshot-%02d", i),
			fmt.Sprintf("%d", 100+i),
			fmt.Sprintf("Source %02d", i),
			fmt.Sprintf("body %02d", i),
			nil,
			fixture.at,
		)
		fixture.link(t, fmt.Sprintf("link-%02d", i), snapshot, fmt.Sprintf("adopt-%02d", i), fixture.at)
	}
	fixture.createOperation(t, "operation-source-cap")

	contextText, snapshotIDs, err := fixture.planning.LoadPlanningOperationExternalContext(fixture.ctx, "operation-source-cap")
	if err != nil {
		t.Fatal(err)
	}
	wantIDs := make([]string, 0, sources)
	for i := 0; i < sources; i++ {
		wantIDs = append(wantIDs, fmt.Sprintf("snapshot-%02d", i))
		if !strings.Contains(contextText, fmt.Sprintf("Source %02d", i)) {
			t.Fatalf("bound context is missing recent source %d: %s", i, contextText)
		}
	}
	sort.Strings(wantIDs)
	if got, want := strings.Join(snapshotIDs, ","), strings.Join(wantIDs, ","); got != want {
		t.Fatalf("bound snapshots=%q, want %q", got, want)
	}
	replayed, replayIDs, err := fixture.planning.LoadPlanningOperationExternalContext(fixture.ctx, "operation-source-cap")
	if err != nil || replayed != contextText || strings.Join(replayIDs, ",") != strings.Join(snapshotIDs, ",") {
		t.Fatalf("immutable replay differs: %v", err)
	}
}

func TestPlanningOperationExternalContextIncludesCompleteRetainedContentBeyondFormerBudgets(t *testing.T) {
	fixture := newPlanningExternalBindingFixture(t, "complete-retained")
	if ExternalAssemblerVersion != "external-context-v3" {
		t.Fatalf("assembler version=%q, want external-context-v3", ExternalAssemblerVersion)
	}
	issueBody := strings.Repeat(`Complete issue evidence with UTF-8 界 and "quotes". `, 25000)
	comments := make([]ExternalComment, 205)
	for i := range comments {
		body := fmt.Sprintf(`Comment %03d includes UTF-8 界 and "quotes". `, i)
		comments[i] = ExternalComment{
			ProviderCommentID: fmt.Sprintf("comment-%03d", i),
			Author:            fmt.Sprintf("author-%03d-界", i),
			Body:              strings.Repeat(body, 1000),
			CanonicalURL:      fmt.Sprintf("https://example.test/issues/7#comment-%03d", i),
			CreatedAt:         fixture.at,
			UpdatedAt:         fixture.at,
		}
	}
	snapshot := fixture.capture(t, "source-complete", "snapshot-complete", "100", strings.Repeat(`Complete "issue" 界 `, 200), issueBody, comments, fixture.at)
	fixture.link(t, "link-complete", snapshot, "adopt-complete", fixture.at)
	fixture.createOperation(t, "operation-complete-retained")

	contextText, snapshotIDs, err := fixture.planning.LoadPlanningOperationExternalContext(fixture.ctx, "operation-complete-retained")
	if err != nil {
		t.Fatal(err)
	}
	secondContext, secondIDs, err := fixture.planning.LoadPlanningOperationExternalContext(fixture.ctx, "operation-complete-retained")
	if err != nil {
		t.Fatal(err)
	}
	if contextText != secondContext || strings.Join(snapshotIDs, ",") != strings.Join(secondIDs, ",") {
		t.Fatal("reloading immutable external context was not deterministic")
	}
	if len(snapshotIDs) != 1 || snapshotIDs[0] != snapshot.SnapshotID {
		t.Fatalf("bound snapshots=%#v, want %q", snapshotIDs, snapshot.SnapshotID)
	}

	storedSnapshot, retainedComments, err := readExternalSnapshotForTest(fixture.ctx, fixture.external, snapshot.SnapshotID)
	if err != nil {
		t.Fatal(err)
	}
	if len(retainedComments) != len(comments) || storedSnapshot.RetainedCommentCount != len(comments) {
		t.Fatalf("stored retained comments=%d metadata=%d, want %d", len(retainedComments), storedSnapshot.RetainedCommentCount, len(comments))
	}
	retainedCommentBytes := 0
	expectedComments := make([]planningExternalComment, 0, len(retainedComments))
	for i, comment := range retainedComments {
		if comment.ProviderCommentID != comments[i].ProviderCommentID || comment.Body != comments[i].Body {
			t.Fatalf("stored comment %d differs from retained source: got=%#v want=%#v", i, comment, comments[i])
		}
		if comment.RetainedBytes != len(comment.Body) {
			t.Fatalf("stored comment %d retained bytes=%d body bytes=%d", i, comment.RetainedBytes, len(comment.Body))
		}
		retainedCommentBytes += comment.RetainedBytes
		expectedComments = append(expectedComments, planningExternalComment{
			Author:       comment.Author,
			Body:         comment.Body,
			CanonicalURL: comment.CanonicalURL,
			CreatedAt:    comment.CreatedAt,
			UpdatedAt:    comment.UpdatedAt,
		})
	}
	if retainedCommentBytes != storedSnapshot.RetainedCommentBytes || retainedCommentBytes <= 128<<10 || storedSnapshot.CommentsTruncated {
		t.Fatalf("stored comment body bytes=%d metadata=%d", retainedCommentBytes, storedSnapshot.RetainedCommentBytes)
	}

	issueJSON, err := json.Marshal(storedSnapshot.Issue)
	if err != nil {
		t.Fatal(err)
	}
	commentsJSON, err := json.Marshal(expectedComments)
	if err != nil {
		t.Fatal(err)
	}
	if len(issueJSON) <= 6*1024 || len(commentsJSON) <= 4*1024 || len(contextText) <= 6*1024 {
		t.Fatalf("assembled content did not exceed former budgets: issue=%d comments=%d frame=%d", len(issueJSON), len(commentsJSON), len(contextText))
	}
	if !utf8.ValidString(contextText) {
		t.Fatal("assembled context is not valid UTF-8")
	}
	assertExternalContextJSONValid(t, contextText)
	wantOpeningFrame := fmt.Sprintf("<KINGDOM_UNTRUSTED_EXTERNAL_SOURCE snapshot_id=%q assembler=%q>\n", snapshot.SnapshotID, ExternalAssemblerVersion)
	wantJSONPayload := fmt.Sprintf("\nissue_bytes=%d\nissue_json=%s\ncomment_bytes=%d\ncomments_json=%s\n", len(issueJSON), issueJSON, len(commentsJSON), commentsJSON)
	if !strings.HasPrefix(contextText, wantOpeningFrame) || !strings.Contains(contextText, wantJSONPayload) || !strings.HasSuffix(contextText, "\n</KINGDOM_UNTRUSTED_EXTERNAL_SOURCE>") {
		t.Fatal("assembled external context lost its complete versioned frame or JSON payload")
	}
	if got := strings.Count(contextText, "<KINGDOM_UNTRUSTED_EXTERNAL_SOURCE"); got != 1 {
		t.Fatalf("external frame count=%d, want 1", got)
	}

	var version, storedDigest string
	var issueBytes, commentBytes int
	if err := fixture.db.QueryRow(`SELECT assembler_version,context_digest,issue_bytes,comment_bytes FROM planning_operation_external_snapshots WHERE operation_id='operation-complete-retained' AND snapshot_id=?`, snapshot.SnapshotID).Scan(&version, &storedDigest, &issueBytes, &commentBytes); err != nil {
		t.Fatal(err)
	}
	if version != ExternalAssemblerVersion || storedDigest != digest([]byte(contextText)) || issueBytes != len(issueJSON) || commentBytes != len(commentsJSON) {
		t.Fatalf("immutable binding metadata version=%q digest=%q issue_bytes=%d comment_bytes=%d", version, storedDigest, issueBytes, commentBytes)
	}
	if issueBytes <= 131072 || commentBytes <= 65536 {
		t.Fatalf("binding metadata did not exceed removed schema ceilings: issue_bytes=%d comment_bytes=%d", issueBytes, commentBytes)
	}
}

func TestPlanningOperationExternalContextAccountsForCompleteFrames(t *testing.T) {
	fixture := newPlanningExternalBindingFixture(t, "complete-frames")
	comments := make([]ExternalComment, 12)
	for i := range comments {
		comments[i] = ExternalComment{
			ProviderCommentID: fmt.Sprintf("comment-%03d", i),
			Author:            strings.Repeat(`author\"界`, 20),
			Body:              strings.Repeat("quoted \\\" multibyte 界 ", 8),
			CanonicalURL:      "https://example.test/" + strings.Repeat("metadata/", 20) + fmt.Sprint(i),
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
	if !utf8.ValidString(contextText) {
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

func TestPlanningOperationExternalContextKeepsCompleteFramesBeyondFormerBudget(t *testing.T) {
	fixture := newPlanningExternalBindingFixture(t, "large-frame")
	firstID := "snapshot-a-" + strings.Repeat("a", 7*1024)
	secondID := "snapshot-b-" + strings.Repeat("b", 7*1024)
	first := fixture.capture(t, "source-a", firstID, "100", "A", "body", nil, fixture.at)
	second := fixture.capture(t, "source-b", secondID, "101", "B", "body", nil, fixture.at)
	fixture.link(t, "link-a", first, "adopt-a", fixture.at)
	fixture.link(t, "link-b", second, "adopt-b", fixture.at)
	fixture.createOperation(t, "operation-large-frame")
	contextText, snapshotIDs, err := fixture.planning.LoadPlanningOperationExternalContext(fixture.ctx, "operation-large-frame")
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshotIDs) != 2 || snapshotIDs[0] != firstID || snapshotIDs[1] != secondID {
		t.Fatalf("large frames changed immutable snapshot bindings: %#v", snapshotIDs)
	}
	if got := strings.Count(contextText, "<KINGDOM_UNTRUSTED_EXTERNAL_SOURCE"); got != 2 || !strings.Contains(contextText, firstID) || !strings.Contains(contextText, secondID) {
		t.Fatalf("large snapshot frames were omitted: frames=%d bytes=%d", got, len(contextText))
	}
	if !utf8.ValidString(contextText) {
		t.Fatal("assembled large frames are not valid UTF-8")
	}
	assertExternalContextJSONValid(t, contextText)
	sections := strings.Split(contextText, "\n\n")
	for i, snapshotID := range snapshotIDs {
		var storedDigest string
		var issueBytes, commentBytes int
		if err := fixture.db.QueryRow(`SELECT context_digest,issue_bytes,comment_bytes FROM planning_operation_external_snapshots WHERE operation_id='operation-large-frame' AND snapshot_id=?`, snapshotID).Scan(&storedDigest, &issueBytes, &commentBytes); err != nil {
			t.Fatal(err)
		}
		if storedDigest != digest([]byte(sections[i])) || issueBytes == 0 || commentBytes != len("[]") {
			t.Fatalf("large frame %d binding digest=%q issue=%d comments=%d", i, storedDigest, issueBytes, commentBytes)
		}
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
		name                string
		column              string
		tampered            any
		original            any
		wantVersionMismatch bool
	}{
		{name: "legacy v2 assembler binding", column: "assembler_version", tampered: "external-context-v2", original: version, wantVersionMismatch: true},
		{name: "unknown assembler version", column: "assembler_version", tampered: "unknown", original: version, wantVersionMismatch: true},
		{name: "digest", column: "context_digest", tampered: strings.Repeat("0", 64), original: contextDigest},
		{name: "issue bytes", column: "issue_bytes", tampered: issueBytes + 1, original: issueBytes},
		{name: "comment bytes", column: "comment_bytes", tampered: commentBytes + 1, original: commentBytes},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mustExecArgs(t, fixture.db, `UPDATE planning_operation_external_snapshots SET `+test.column+`=? WHERE operation_id='operation-tamper'`, test.tampered)
			contextText, snapshotIDs, err := fixture.planning.LoadPlanningOperationExternalContext(fixture.ctx, "operation-tamper")
			var versionErr *UnsupportedPlanningExternalContextVersionError
			if test.wantVersionMismatch {
				if !errors.As(err, &versionErr) || versionErr.BoundVersion != test.tampered || versionErr.CurrentVersion != ExternalAssemblerVersion {
					t.Fatalf("tampered %s error=%v, want typed context-version mismatch", test.column, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "metadata mismatch") || !strings.Contains(err.Error(), snapshot.SnapshotID) {
				t.Fatalf("tampered %s was accepted: %v", test.column, err)
			}
			if contextText != "" || len(snapshotIDs) != 0 {
				t.Fatalf("tampered %s returned partial context: bytes=%d snapshots=%#v", test.column, len(contextText), snapshotIDs)
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
	if _, err := fixture.external.CreateConnection(fixture.ctx, CreateExternalConnectionRequest{ConnectionID: "conn-" + suffix, CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []ConnectionInstallation{{InstallationID: "2"}}, CredentialHandle: "github-app:conn-" + suffix, ActorID: "local-operator", CreatedAt: fixture.at, AuditEventID: "audit-" + suffix}); err != nil {
		t.Fatal(err)
	}
	mustExecArgs(t, db, `INSERT INTO planning_sessions (planning_session_id,title,title_state,lifecycle_state,conversation_state,project_root,model_selection_json,created_at,updated_at) VALUES (?,?,'custom','active','ready','/tmp','{}',?,?)`, fixture.sessionID, "Issue", fixture.at, fixture.at)
	mustExecArgs(t, db, `INSERT INTO planning_context_snapshots (planning_context_snapshot_id,planning_session_id,summary_markdown,included_files_json,excluded_files_json,created_at) VALUES (?,?,?,'[]','[]',?)`, "context-"+suffix, fixture.sessionID, "bound context", fixture.at)
	mustExecArgs(t, db, `UPDATE planning_sessions SET context_snapshot_id=? WHERE planning_session_id=?`, "context-"+suffix, fixture.sessionID)
	return fixture
}

func (f planningExternalBindingFixture) capture(t *testing.T, sourceID, snapshotID, nativeID, title, body string, comments []ExternalComment, fetchedAt string) ExternalSourceSnapshot {
	t.Helper()
	snapshot, _, err := f.external.CaptureIssue(f.ctx, CaptureExternalIssueRequest{SourceID: sourceID, SnapshotID: snapshotID, ConnectionID: "conn-" + strings.TrimPrefix(f.sessionID, "session-"), ProviderNativeID: nativeID, RepositoryNativeID: "repo-" + nativeID, Issue: NormalizedExternalIssue{Title: title, Body: body, State: "open", Repository: "owner/repo", IssueNumber: 7, CanonicalURL: "https://github.com/owner/repo/issues/7", CreatedAt: f.at, UpdatedAt: fetchedAt}, Profile: ExternalImportProfile{IncludeComments: len(comments) > 0}, ProviderUpdatedAt: fetchedAt, FetchedAt: fetchedAt, Comments: ExternalCommentList(comments)})
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
	if _, err := f.planning.CreateOperation(f.ctx, CreatePlanningOperationRequest{OperationID: operationID, PlanningSessionID: f.sessionID, Type: PlanningOperationTypeReply, Status: PlanningOperationStatusQueued, RequestPayloadJSON: fmt.Sprintf(`{"assistant_message_id":%q,"assistant_created_at":%q}`, operationID+"_reply", f.at), CreatedAt: f.at}); err != nil {
		t.Fatal(err)
	}
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
	if _, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-comments", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []ConnectionInstallation{{InstallationID: "2"}}, CredentialHandle: "github-app:conn-comments", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-comments"}); err != nil {
		t.Fatal(err)
	}
	comments := make([]ExternalComment, 100)
	for i := range comments {
		comments[i] = ExternalComment{ProviderCommentID: string(rune(i + 1)), Body: "body"}
	}
	req := CaptureExternalIssueRequest{SourceID: "source-comments", SnapshotID: "snapshot-comments", ConnectionID: "conn-comments", ProviderNativeID: "100", RepositoryNativeID: "200", Issue: NormalizedExternalIssue{Title: "Issue", Body: "body", State: "open", Repository: "owner/repo", IssueNumber: 7, CanonicalURL: "https://github.com/owner/repo/issues/7", CreatedAt: at, UpdatedAt: at}, Profile: ExternalImportProfile{IncludeComments: true}, ProviderUpdatedAt: at, FetchedAt: at, ProviderCommentCount: 150, FetchedCommentCount: 100, FetchedCommentPages: 1, OmittedCommentCount: 50, OmittedCommentPages: 1, Comments: ExternalCommentList(comments)}
	first, created, err := store.CaptureIssue(ctx, req)
	if err != nil || !created {
		t.Fatalf("capture: created=%v err=%v", created, err)
	}
	loaded, _, err := readExternalSnapshotForTest(ctx, store, first.SnapshotID)
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

func onlyInstallation(t *testing.T, c ExternalConnection) ConnectionInstallation {
	t.Helper()
	if len(c.Installations) != 1 {
		t.Fatalf("connection covers %d installations, want 1: %#v", len(c.Installations), c.Installations)
	}
	return c.Installations[0]
}

func TestUpdateConnectionDisplayNamePreservesValidatedIdentity(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	ctx := context.Background()
	at := "2026-08-12T10:00:00Z"
	connection, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-edit", CredentialStrategy: "app_private_key", DisplayName: "Old", AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []ConnectionInstallation{{InstallationID: "2"}}, CredentialHandle: "github-app:conn-edit", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-edit-create"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCredentialEvent(ctx, connection.ConnectionID, connection.RowVersion, 1, "local-operator", "audit-edit-credential", "credential.provisioned", at); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateConnectionState(ctx, "conn-edit", "enabled", "healthy", "", at, true, []ConnectionInstallation{{InstallationID: "2", AccountID: "10", AccountLogin: "octocat"}}); err != nil {
		t.Fatal(err)
	}
	name := "New name"
	row, err := store.UpdateConnection(ctx, "conn-edit", UpdateExternalConnectionRequest{DisplayName: &name, ActorID: "local-operator", UpdatedAt: "2026-08-12T11:00:00Z", AuditEventID: "audit-edit-name"})
	if err != nil || row.HealthClass != "healthy" || row.DisplayName != name || row.AppID != "1" || row.ClientID != "Iv1" || onlyInstallation(t, row).InstallationID != "2" {
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
		CreateExternalConnectionRequest: CreateExternalConnectionRequest{ConnectionID: "conn-final", CredentialStrategy: "app_private_key", DisplayName: "Kingdom Issues", AppID: "1", ClientID: "Iv1", AppSlug: "kingdom-issues", Installations: []ConnectionInstallation{{InstallationID: "2", AccountID: "10", AccountLogin: "octocat"}}, CredentialHandle: "github-app:conn-final", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-final"},
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
		CreateExternalConnectionRequest: CreateExternalConnectionRequest{ConnectionID: "conn-rollback", CredentialStrategy: "app_private_key", DisplayName: "Rollback", AppID: "3", ClientID: "Iv2", AppSlug: "rollback", Installations: []ConnectionInstallation{{InstallationID: "4"}}, CredentialHandle: "github-app:conn-rollback", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-final"},
	})
	if err == nil {
		t.Fatal("expected duplicate audit id to roll back finalization")
	}
	var rowCount int
	if queryErr := db.QueryRow(`SELECT COUNT(1) FROM external_connections WHERE connection_id='conn-rollback'`).Scan(&rowCount); queryErr != nil || rowCount != 0 {
		t.Fatalf("rolled-back connection count=%d err=%v", rowCount, queryErr)
	}
}

func TestCurrentValidationRequiresPositivePublishedGeneration(t *testing.T) {
	connection := ExternalConnection{
		Lifecycle: "enabled", HealthClass: "healthy", CredentialGeneration: 0, ValidatedGeneration: 0,
		Installations: []ConnectionInstallation{{InstallationID: "2", AccountID: "10", AccountLogin: "octocat"}},
	}
	if connection.HasCurrentValidation() {
		t.Fatal("generation-zero connection was treated as currently validated")
	}

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	ctx := context.Background()
	at := "2026-08-15T12:00:00Z"
	created, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-zero", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []ConnectionInstallation{{InstallationID: "2"}}, CredentialHandle: "github-app:conn-zero", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-zero-create"})
	if err != nil {
		t.Fatal(err)
	}
	err = store.UpdateConnectionStateAuditedVersion(ctx, created.ConnectionID, "enabled", "healthy", "", at, true, []ConnectionInstallation{{InstallationID: "2", AccountID: "10", AccountLogin: "octocat"}}, "local-operator", "audit-zero-health", "connection.health_checked", created.RowVersion)
	if !errors.Is(err, ErrExternalConflict) {
		t.Fatalf("healthy generation-zero state error=%v", err)
	}
	if err := store.RecordCredentialEvent(ctx, created.ConnectionID, created.RowVersion, 2, "local-operator", "audit-skipped-generation", "credential.provisioned", at); !errors.Is(err, ErrExternalConflict) {
		t.Fatalf("store accepted skipped credential generation: %v", err)
	}
}

func TestSharedReauthorizationAdvancesGenerationWithoutChangingPinnedIdentity(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	ctx := context.Background()
	at := "2026-08-15T10:00:00Z"
	connection, err := store.FinalizeConnection(ctx, FinalizeExternalConnectionRequest{CreateExternalConnectionRequest: CreateExternalConnectionRequest{ConnectionID: "conn-shared", CredentialStrategy: "user_access_token", DisplayName: "Shared", AppID: "1", ClientID: "IvShared", AppSlug: "kingdom", Installations: []ConnectionInstallation{{InstallationID: "2", AccountID: "10", AccountLogin: "old-login"}}, CredentialHandle: "github-user:conn-shared", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-shared-create"}})
	if err != nil {
		t.Fatal(err)
	}
	updated, err := store.ReauthorizeSharedConnection(ctx, connection, "kingdom-renamed", []ConnectionInstallation{{InstallationID: "2", AccountID: "10", AccountLogin: "new-login"}}, "local-operator", "audit-shared-reauthorize", "2026-08-15T11:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	if updated.CredentialGeneration != 2 || updated.ValidatedGeneration != 2 || updated.AppSlug != "kingdom-renamed" || updated.AppID != "1" || updated.ClientID != "IvShared" || onlyInstallation(t, updated) != (ConnectionInstallation{InstallationID: "2", AccountID: "10", AccountLogin: "new-login"}) {
		t.Fatalf("reauthorized connection=%#v", updated)
	}
	if _, err := db.Exec(`UPDATE external_connections SET client_id='spoofed' WHERE connection_id='conn-shared'`); err == nil {
		t.Fatal("database allowed pinned shared App identity mutation")
	}
}

func TestSharedReauthorizationReturnsCommittedProjectionWithoutContextRead(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	ctx, cancel := context.WithCancel(context.Background())
	at := "2026-08-24T12:00:00Z"
	connection, err := store.FinalizeConnection(ctx, FinalizeExternalConnectionRequest{CreateExternalConnectionRequest: CreateExternalConnectionRequest{ConnectionID: "conn-projection", CredentialStrategy: "user_access_token", DisplayName: "Shared", AppID: "1", ClientID: "IvShared", AppSlug: "kingdom", Installations: []ConnectionInstallation{{InstallationID: "2", AccountID: "10", AccountLogin: "old-login"}}, CredentialHandle: "github-user:conn-projection", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-projection-create"}})
	if err != nil {
		t.Fatal(err)
	}
	store.commitTx = func(tx *sql.Tx) error {
		err := tx.Commit()
		cancel()
		return err
	}
	updated, err := store.ReauthorizeSharedConnection(ctx, connection, "kingdom", []ConnectionInstallation{{InstallationID: "2", AccountID: "10", AccountLogin: "new-login"}}, "local-operator", "audit-projection-reauthorize", "2026-08-24T12:01:00Z")
	if err != nil {
		t.Fatalf("reauthorization performed a fallible post-commit read: %v", err)
	}
	if updated.CredentialGeneration != 2 || updated.ValidatedGeneration != 2 || updated.RowVersion != connection.RowVersion+1 || updated.LastCheckedAt == nil || *updated.LastCheckedAt != "2026-08-24T12:01:00Z" || onlyInstallation(t, updated).AccountLogin != "new-login" {
		t.Fatalf("committed projection=%#v", updated)
	}
}

func TestSharedReauthorizationCommitAmbiguityReturnsTargetProjection(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	at := "2026-08-24T13:00:00Z"
	connection, err := store.FinalizeConnection(context.Background(), FinalizeExternalConnectionRequest{CreateExternalConnectionRequest: CreateExternalConnectionRequest{ConnectionID: "conn-ambiguous", CredentialStrategy: "user_access_token", DisplayName: "Shared", AppID: "1", ClientID: "IvShared", AppSlug: "kingdom", Installations: []ConnectionInstallation{{InstallationID: "2", AccountID: "10", AccountLogin: "octocat"}}, CredentialHandle: "github-user:conn-ambiguous", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-ambiguous-create"}})
	if err != nil {
		t.Fatal(err)
	}
	ambiguous := errors.New("commit outcome unknown")
	store.commitTx = func(tx *sql.Tx) error {
		if err := tx.Commit(); err != nil {
			return err
		}
		return ambiguous
	}
	projected, err := store.ReauthorizeSharedConnection(context.Background(), connection, "kingdom", connection.Installations, "local-operator", "audit-ambiguous-reauthorize", "2026-08-24T13:01:00Z")
	if !errors.Is(err, ambiguous) || projected.ConnectionID != connection.ConnectionID || projected.CredentialGeneration != 2 || projected.ValidatedGeneration != 2 {
		t.Fatalf("ambiguous result projection=%#v err=%v", projected, err)
	}
	published, err := store.GetCredentialPublication(context.Background(), connection.ConnectionID)
	if err != nil || published.CredentialStrategy != "user_access_token" || published.CredentialGeneration != 2 {
		t.Fatalf("published generation=%#v err=%v", published, err)
	}
}

func TestUpdateConnectionConfirmsDurableCommitError(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	ctx := context.Background()
	const at = "2026-08-24T14:00:00Z"
	connection, err := store.FinalizeConnection(ctx, FinalizeExternalConnectionRequest{CreateExternalConnectionRequest: CreateExternalConnectionRequest{
		ConnectionID: "conn-update-commit-ambiguous", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "kingdom",
		Installations: []ConnectionInstallation{{InstallationID: "2", AccountID: "10", AccountLogin: "octocat"}}, CredentialHandle: "github-app:conn-update-commit-ambiguous",
		ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-update-commit-create",
	}})
	if err != nil {
		t.Fatalf("create connection: %v", err)
	}
	displayName := "Renamed App"
	enabled := false
	commitErr := errors.New("commit outcome unknown")
	store.commitTx = func(tx *sql.Tx) error {
		if err := tx.Commit(); err != nil {
			return err
		}
		return commitErr
	}
	updated, err := store.UpdateConnection(ctx, connection.ConnectionID, UpdateExternalConnectionRequest{
		Enabled: &enabled, DisplayName: &displayName, ActorID: "local-operator", UpdatedAt: "2026-08-24T14:01:00Z", AuditEventID: "audit-update-commit-ambiguous",
	})
	if err != nil {
		t.Fatalf("confirm exact update after durable commit error: %v", err)
	}
	if updated.RowVersion != connection.RowVersion+1 || updated.DisplayName != displayName || updated.Lifecycle != "disabled" || updated.HealthClass != "disabled" {
		t.Fatalf("unexpected confirmed update projection: %#v", updated)
	}
}

func TestUpdateConnectionRejectsMismatchedDurableCommitReadback(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	ctx := context.Background()
	const at = "2026-08-24T15:00:00Z"
	connection, err := store.FinalizeConnection(ctx, FinalizeExternalConnectionRequest{CreateExternalConnectionRequest: CreateExternalConnectionRequest{
		ConnectionID: "conn-update-commit-mismatch", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "kingdom",
		Installations: []ConnectionInstallation{{InstallationID: "2", AccountID: "10", AccountLogin: "octocat"}}, CredentialHandle: "github-app:conn-update-commit-mismatch",
		ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-update-mismatch-create",
	}})
	if err != nil {
		t.Fatalf("create connection: %v", err)
	}
	displayName := "Renamed App"
	commitErr := errors.New("commit outcome unknown")
	store.commitTx = func(tx *sql.Tx) error {
		if err := tx.Commit(); err != nil {
			return err
		}
		if _, err := db.Exec(`UPDATE external_connections SET display_name = 'Later Mutation', row_version = row_version + 1 WHERE connection_id = ?`, connection.ConnectionID); err != nil {
			return err
		}
		return commitErr
	}
	_, err = store.UpdateConnection(ctx, connection.ConnectionID, UpdateExternalConnectionRequest{
		DisplayName: &displayName, ActorID: "local-operator", UpdatedAt: "2026-08-24T15:01:00Z", AuditEventID: "audit-update-commit-mismatch",
	})
	var unknown *CommitOutcomeUnknownError
	if !errors.As(err, &unknown) || !errors.Is(err, commitErr) {
		t.Fatalf("mismatched durable update readback error=%v, want unknown outcome preserving commit error", err)
	}
}

func TestFinalizeConnectionCommitAmbiguityReturnsTargetProjection(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	ambiguous := errors.New("commit outcome unknown")
	store.commitTx = func(tx *sql.Tx) error {
		if err := tx.Commit(); err != nil {
			return err
		}
		return ambiguous
	}
	projected, err := store.FinalizeConnection(context.Background(), FinalizeExternalConnectionRequest{CreateExternalConnectionRequest: CreateExternalConnectionRequest{ConnectionID: "conn-initial-ambiguous", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "kingdom", Installations: []ConnectionInstallation{{InstallationID: "2", AccountID: "10", AccountLogin: "octocat"}}, CredentialHandle: "github-app:conn-initial-ambiguous", ActorID: "local-operator", CreatedAt: "2026-08-24T13:00:00Z", AuditEventID: "audit-initial-ambiguous"}})
	if !errors.Is(err, ambiguous) || projected.ConnectionID != "conn-initial-ambiguous" || projected.CredentialGeneration != 1 || projected.LastSuccessAt == nil {
		t.Fatalf("ambiguous result projection=%#v err=%v", projected, err)
	}
	published, err := store.GetCredentialPublication(context.Background(), projected.ConnectionID)
	if err != nil || published.CredentialStrategy != "app_private_key" || published.CredentialGeneration != 1 {
		t.Fatalf("published generation=%#v err=%v", published, err)
	}
}

func TestCredentialGenerationFencesStaleCaptureAndHealth(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	ctx := context.Background()
	at := "2026-08-12T10:00:00Z"
	conn, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-fence", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []ConnectionInstallation{{InstallationID: "2"}}, CredentialHandle: "github-app:conn-fence", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-fence-create"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCredentialEvent(ctx, conn.ConnectionID, conn.RowVersion, 1, "local-operator", "audit-fence-key", "credential.provisioned", at); err != nil {
		t.Fatal(err)
	}
	err = store.UpdateConnectionStateAuditedVersion(ctx, conn.ConnectionID, "enabled", "healthy", "", at, true, []ConnectionInstallation{{InstallationID: "2", AccountID: "10", AccountLogin: "octocat"}}, "local-operator", "audit-stale-health", "connection.health_checked", conn.RowVersion)
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
	connection, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-identity", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []ConnectionInstallation{{InstallationID: "2"}}, CredentialHandle: "github-app:conn-identity", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-identity-create"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCredentialEvent(ctx, connection.ConnectionID, connection.RowVersion, 1, "local-operator", "audit-identity-credential", "credential.provisioned", at); err != nil {
		t.Fatal(err)
	}
	connection, err = store.GetConnection(ctx, connection.ConnectionID)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateConnectionStateAuditedVersion(ctx, connection.ConnectionID, "enabled", "healthy", "", at, true, []ConnectionInstallation{{InstallationID: "2", AccountID: "10", AccountLogin: "octocat"}}, "local-operator", "audit-identity-health", "connection.health_checked", connection.RowVersion); err != nil {
		t.Fatal(err)
	}
	validated, err := store.GetConnection(ctx, connection.ConnectionID)
	if err != nil {
		t.Fatal(err)
	}
	if onlyInstallation(t, validated) != (ConnectionInstallation{InstallationID: "2", AccountID: "10", AccountLogin: "octocat"}) || !validated.HasCurrentValidation() {
		t.Fatalf("validated identity was not persisted: %#v", validated)
	}
	if err := store.UpdateConnectionStateAuditedVersion(ctx, connection.ConnectionID, "enabled", "healthy", "", at, true, []ConnectionInstallation{{InstallationID: "2", AccountID: "11", AccountLogin: "impostor"}}, "local-operator", "audit-identity-mismatch", "connection.health_checked", validated.RowVersion); !errors.Is(err, ErrExternalConflict) {
		t.Fatalf("mismatched installation account identity was accepted: %v", err)
	}
	after, err := store.GetConnection(ctx, connection.ConnectionID)
	if err != nil {
		t.Fatal(err)
	}
	if onlyInstallation(t, after) != (ConnectionInstallation{InstallationID: "2", AccountID: "10", AccountLogin: "octocat"}) || after.RowVersion != validated.RowVersion {
		t.Fatalf("identity changed after mismatch: before=%#v after=%#v", validated, after)
	}
	if err := store.UpdateConnectionStateAuditedVersion(ctx, connection.ConnectionID, "enabled", "healthy", "", at, true, []ConnectionInstallation{{InstallationID: "2", AccountID: "10", AccountLogin: "octocat-renamed"}}, "local-operator", "audit-identity-rename", "connection.health_checked", after.RowVersion); err != nil {
		t.Fatalf("refresh renamed installation account login: %v", err)
	}
	renamed, err := store.GetConnection(ctx, connection.ConnectionID)
	if err != nil {
		t.Fatal(err)
	}
	if onlyInstallation(t, renamed) != (ConnectionInstallation{InstallationID: "2", AccountID: "10", AccountLogin: "octocat-renamed"}) {
		t.Fatalf("renamed account login was not refreshed: %#v", renamed)
	}
	if _, err := db.Exec(`UPDATE external_installations SET account_id='11' WHERE installation_id='2'`); err == nil || !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("expected pinned installation identity trigger, got %v", err)
	}
	if _, err := db.Exec(`UPDATE external_connection_installations SET connection_id='conn-other' WHERE connection_id='conn-identity'`); err == nil || !strings.Contains(err.Error(), "immutable") {
		t.Fatalf("an installation must not be moved between connections, got %v", err)
	}
}

func TestExternalHistorySurvivesDetachedTargetRemoval(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	ctx := context.Background()
	at := "2026-08-12T10:00:00Z"
	if _, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-detached", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []ConnectionInstallation{{InstallationID: "2"}}, CredentialHandle: "github-app:conn-detached", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-detached"}); err != nil {
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

func TestLinkHistoryOrdersRFC3339NanoInstantsExactlyAcrossPages(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	ctx := context.Background()
	const at = "2026-08-12T10:00:00Z"

	if _, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-history-order", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []ConnectionInstallation{{InstallationID: "2"}}, CredentialHandle: "github-app:conn-history-order", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-history-order"}); err != nil {
		t.Fatal(err)
	}
	capture := func(snapshotID, predecessor, fetchedAt string) ExternalSourceSnapshot {
		t.Helper()
		snapshot, _, err := store.CaptureIssue(ctx, CaptureExternalIssueRequest{
			SourceID:              "source-history-order",
			SnapshotID:            snapshotID,
			ConnectionID:          "conn-history-order",
			ProviderNativeID:      "100",
			RepositoryNativeID:    "200",
			PredecessorSnapshotID: predecessor,
			Issue:                 NormalizedExternalIssue{Title: snapshotID, State: "open", Repository: "owner/repo", IssueNumber: 7, CanonicalURL: "https://github.com/owner/repo/issues/7", CreatedAt: at, UpdatedAt: fetchedAt},
			Profile:               ExternalImportProfile{},
			ProviderUpdatedAt:     fetchedAt,
			FetchedAt:             fetchedAt,
		})
		if err != nil {
			t.Fatal(err)
		}
		return snapshot
	}
	oldest := capture("snapshot-zero", "", at)
	middle := capture("snapshot-zulu", oldest.SnapshotID, "2026-08-12T10:00:00.000000001Z")
	newest := capture("snapshot-alpha", middle.SnapshotID, "2026-08-12T12:00:00.000000002+02:00")
	var gotUnixNano int64
	if err := db.QueryRow(`SELECT created_at_unix_nano FROM external_source_snapshots WHERE snapshot_id=?`, newest.SnapshotID).Scan(&gotUnixNano); err != nil {
		t.Fatal(err)
	}
	wantUnixNano, err := rfc3339NanoUnixNano("2026-08-12T12:00:00.000000002+02:00")
	if err != nil {
		t.Fatal(err)
	}
	if gotUnixNano != wantUnixNano {
		t.Fatalf("stored nanosecond sort key=%d, want %d", gotUnixNano, wantUnixNano)
	}

	mustExecArgs(t, db, `INSERT INTO planning_sessions (planning_session_id,title,title_state,lifecycle_state,conversation_state,project_root,model_selection_json,created_at,updated_at) VALUES ('session-history-order','Issue','custom','active','ready','/tmp','{}',?,?)`, at, at)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := createExternalLinkTx(ctx, tx, "link-history-order", "planning_session", "session-history-order", oldest.SourceID, oldest.SnapshotID, "local-operator", "adoption-history-order", at); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	assertIDs := func(got []ExternalHistoryEntry, want ...string) {
		t.Helper()
		ids := make([]string, 0, len(got))
		for _, entry := range got {
			ids = append(ids, entry.SnapshotID)
		}
		if actual := strings.Join(ids, ","); actual != strings.Join(want, ",") {
			t.Fatalf("history order=%q, want %q", actual, strings.Join(want, ","))
		}
	}

	history, err := store.LinkHistory(ctx, "link-history-order")
	if err != nil {
		t.Fatal(err)
	}
	assertIDs(history, newest.SnapshotID, middle.SnapshotID, oldest.SnapshotID)

	var paged []ExternalHistoryEntry
	for offset := 0; ; offset++ {
		page, hasMore, err := store.LinkHistoryPage(ctx, "link-history-order", offset, 1)
		if err != nil {
			t.Fatal(err)
		}
		paged = append(paged, page...)
		if !hasMore {
			break
		}
	}
	assertIDs(paged, newest.SnapshotID, middle.SnapshotID, oldest.SnapshotID)
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
			AppSlug: "app", Installations: []ConnectionInstallation{{InstallationID: fmt.Sprintf("%d", 100+i)}}, CredentialHandle: "github-app:" + id,
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
	connection, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-credential", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []ConnectionInstallation{{InstallationID: "2"}}, CredentialHandle: "github-app:conn-credential", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-credential-create"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCredentialEvent(ctx, connection.ConnectionID, connection.RowVersion, 1, "local-operator", "audit-credential-present", "credential.provisioned", at); err != nil {
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
	if err := store.RecordCredentialEvent(ctx, connection.ConnectionID, connection.RowVersion, 1, "local-operator", "audit-credential-stale", "credential.rotated", at); !errors.Is(err, ErrExternalConflict) {
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
	if _, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-transfer", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []ConnectionInstallation{{InstallationID: "2"}}, CredentialHandle: "github-app:conn-transfer", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-transfer-create"}); err != nil {
		t.Fatal(err)
	}
	first, _, err := store.CaptureIssue(ctx, CaptureExternalIssueRequest{SourceID: "source-transfer", SnapshotID: "snapshot-transfer-old", ConnectionID: "conn-transfer", ProviderNativeID: "100", RepositoryNativeID: "200", Issue: NormalizedExternalIssue{Title: "Moved", Body: "body", State: "open", Repository: "owner/old", IssueNumber: 7, CanonicalURL: "https://github.com/owner/old/issues/7", CreatedAt: at, UpdatedAt: at}, Profile: ExternalImportProfile{}, ProviderUpdatedAt: at, FetchedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateWorkOrder(ctx, CreateWorkOrderRequest{WorkOrderID: "work-order-transfer", Title: "D", Text: "immutable", CreatedAt: at, DirectSnapshotID: first.SnapshotID, NewLinkID: func() string { return "link-transfer" }, ActorID: "local-operator"}); err != nil {
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
	connection, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-two-links", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []ConnectionInstallation{{InstallationID: "2"}}, CredentialHandle: "github-app:conn-two-links", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-two-links-create"})
	if err != nil {
		t.Fatal(err)
	}
	oldIssue := NormalizedExternalIssue{Title: "Moved", Body: "body", State: "open", Repository: "owner/old", IssueNumber: 7, CanonicalURL: "https://github.com/owner/old/issues/7", CreatedAt: at, UpdatedAt: at}
	oldSnapshot, _, err := store.CaptureIssue(ctx, CaptureExternalIssueRequest{SourceID: "source-two-links", SnapshotID: "snapshot-two-links-old", ConnectionID: connection.ConnectionID, ProviderNativeID: "100", RepositoryNativeID: "200", Issue: oldIssue, Profile: ExternalImportProfile{}, ProviderUpdatedAt: at, FetchedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateWorkOrder(ctx, CreateWorkOrderRequest{WorkOrderID: "work-order-two-links", Title: "D", Text: "immutable", CreatedAt: at, DirectSnapshotID: oldSnapshot.SnapshotID, NewLinkID: func() string { return "link-two-links-work order" }, ActorID: "local-operator"}); err != nil {
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

	workOrderBasis, err := store.GetRefreshBasis(ctx, "link-two-links-work order")
	if err != nil {
		t.Fatal(err)
	}
	finished := "2026-08-12T11:00:00Z"
	newIssue := NormalizedExternalIssue{Title: "Moved", Body: "body", State: "open", Repository: "owner/new", IssueNumber: 9, CanonicalURL: "https://github.com/owner/new/issues/9", CreatedAt: at, UpdatedAt: finished}
	newCapture := CaptureExternalIssueRequest{SourceID: oldSnapshot.SourceID, SnapshotID: "snapshot-two-links-new", ConnectionID: connection.ConnectionID, ProviderNativeID: "100", RepositoryNativeID: "300", Issue: newIssue, Profile: oldSnapshot.Profile, ProviderUpdatedAt: finished, FetchedAt: finished}
	if _, created, outcome, err := store.ApplyRefresh(ctx, ApplyExternalRefreshRequest{Basis: workOrderBasis, Capture: &newCapture, ObservationID: "observation-two-links-work order", AuditEventID: "audit-two-links-work order", AliasID: "alias-two-links-old", ActorID: "local-operator", Outcome: "changed", StartedAt: finished, FinishedAt: finished}); err != nil || !created || outcome != "changed" {
		t.Fatalf("work order transfer created=%v outcome=%q err=%v", created, outcome, err)
	}

	var currentRepositoryID, currentRepository string
	var currentIssueNumber int
	if err := db.QueryRow(`SELECT repository_native_id,repository_locator,issue_number FROM external_sources WHERE source_id=?`, oldSnapshot.SourceID).Scan(&currentRepositoryID, &currentRepository, &currentIssueNumber); err != nil {
		t.Fatal(err)
	}
	if currentRepositoryID != "300" || currentRepository != "owner/new" || currentIssueNumber != 9 {
		t.Fatalf("current source locator did not move: repository_id=%q repository=%q issue=%d", currentRepositoryID, currentRepository, currentIssueNumber)
	}
	workOrderLinks, err := store.GetLinksForTarget(ctx, "work_order", "work-order-two-links")
	if err != nil || len(workOrderLinks) != 1 {
		t.Fatalf("work order links=%#v err=%v", workOrderLinks, err)
	}
	assertExternalLinkCoordinates(t, workOrderLinks[0], oldSnapshot.SnapshotID, "", newCapture.SnapshotID, "owner/old", 7)

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
	workOrderLinks, err = store.GetLinksForTarget(ctx, "work_order", "work-order-two-links")
	if err != nil || len(workOrderLinks) != 1 {
		t.Fatalf("final work order links=%#v err=%v", workOrderLinks, err)
	}
	assertExternalLinkCoordinates(t, workOrderLinks[0], oldSnapshot.SnapshotID, "", newCapture.SnapshotID, "owner/old", 7)

	for _, linkID := range []string{"link-two-links-work order", "link-two-links-planning"} {
		history, err := store.LinkHistory(ctx, linkID)
		if err != nil || len(history) != 2 || history[0].SnapshotID != newCapture.SnapshotID || history[1].SnapshotID != oldSnapshot.SnapshotID {
			t.Fatalf("history for %s=%#v err=%v", linkID, history, err)
		}
	}
	var workOrderTransfer, planningTransfer string
	if err := db.QueryRow(`SELECT transfer_json FROM external_source_observations WHERE observation_id='observation-two-links-work order'`).Scan(&workOrderTransfer); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT transfer_json FROM external_source_observations WHERE observation_id='observation-two-links-planning'`).Scan(&planningTransfer); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(workOrderTransfer, `"repository":"owner/old"`) || !strings.Contains(workOrderTransfer, `"repository":"owner/new"`) || planningTransfer != `{}` {
		t.Fatalf("incoherent transfer evidence work order=%s planning=%s", workOrderTransfer, planningTransfer)
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
	connection, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-failure", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []ConnectionInstallation{{InstallationID: "2"}}, CredentialHandle: "github-app:conn-failure", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-failure-create"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _, err := store.CaptureIssue(ctx, CaptureExternalIssueRequest{SourceID: "source-failure", SnapshotID: "snapshot-failure", ConnectionID: connection.ConnectionID, ProviderNativeID: "100", RepositoryNativeID: "200", Issue: NormalizedExternalIssue{Title: "Issue", Body: "body", State: "open", Repository: "owner/repo", IssueNumber: 7, CanonicalURL: "https://github.com/owner/repo/issues/7", CreatedAt: at, UpdatedAt: at}, Profile: ExternalImportProfile{}, ProviderUpdatedAt: at, FetchedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateWorkOrder(ctx, CreateWorkOrderRequest{WorkOrderID: "work-order-failure", Title: "D", Text: "immutable", CreatedAt: at, DirectSnapshotID: snapshot.SnapshotID, NewLinkID: func() string { return "link-failure" }, ActorID: "local-operator"}); err != nil {
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
	reloaded, err := store.GetLinksForTarget(ctx, "work_order", "work-order-failure")
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

	connection, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-detach", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []ConnectionInstallation{{InstallationID: "2"}}, CredentialHandle: "github-app:conn-detach", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-detach-connection"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCredentialEvent(ctx, connection.ConnectionID, connection.RowVersion, 1, "local-operator", "audit-detach-credential", "credential.provisioned", at); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateConnectionState(ctx, connection.ConnectionID, "enabled", "healthy", "", at, true, []ConnectionInstallation{{InstallationID: "2", AccountID: "10", AccountLogin: "octocat"}}); err != nil {
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
	if err := store.CreateWorkOrder(ctx, CreateWorkOrderRequest{WorkOrderID: "work-order-detach-control", Title: "Control", Text: "immutable", CreatedAt: at, DirectSnapshotID: capture.SnapshotID, NewLinkID: func() string { return "link-detach-work order" }, ActorID: "local-operator"}); err != nil {
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
	var adoptionsBefore, auditEventsBefore int
	if err := db.QueryRow(`SELECT COUNT(*) FROM planning_external_source_adoptions WHERE link_id=?`, link.LinkID).Scan(&adoptionsBefore); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM external_integration_audit_events`).Scan(&auditEventsBefore); err != nil {
		t.Fatal(err)
	}
	if err := store.AdoptLatest(ctx, link.LinkID, "adoption-detached-rejected", "local-operator", detachedAt, "audit-detached-rejected"); !errors.Is(err, ErrExternalSourceNotAttached) {
		t.Fatalf("adopting a detached source error=%v, want ErrExternalSourceNotAttached", err)
	}
	var adoptionsAfter, auditEventsAfter int
	if err := db.QueryRow(`SELECT COUNT(*) FROM planning_external_source_adoptions WHERE link_id=?`, link.LinkID).Scan(&adoptionsAfter); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM external_integration_audit_events`).Scan(&auditEventsAfter); err != nil {
		t.Fatal(err)
	}
	if adoptionsAfter != adoptionsBefore || auditEventsAfter != auditEventsBefore {
		t.Fatalf("rejected adoption changed durable history: adoptions=%d (before %d), audit events=%d (before %d)", adoptionsAfter, adoptionsBefore, auditEventsAfter, auditEventsBefore)
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
	if reattached.UsedSnapshotID != capture.SnapshotID || reattached.LatestSnapshotID != fresh.SnapshotID || reattached.ActiveSnapshotID != fresh.SnapshotID {
		t.Fatalf("re-attach provenance: used=%q latest=%q active=%q", reattached.UsedSnapshotID, reattached.LatestSnapshotID, reattached.ActiveSnapshotID)
	}
	if reattached.RowVersion != link.RowVersion+1 {
		t.Fatalf("re-attach head row version=%d, want %d", reattached.RowVersion, link.RowVersion+1)
	}
	links, err = store.GetLinksForTarget(ctx, "planning_session", "session-detach")
	if err != nil {
		t.Fatal(err)
	}
	if len(links) != 1 || links[0].LinkID != link.LinkID || links[0].LatestSnapshotID != fresh.SnapshotID || links[0].ActiveSnapshotID != fresh.SnapshotID {
		t.Fatalf("re-added source not presented from current content: %#v", links)
	}
	if err := store.AdoptLatest(ctx, link.LinkID, "adoption-after-reattach", "local-operator", "2026-08-12T12:01:00Z", "audit-adopt-after-reattach"); err != nil {
		t.Fatal(err)
	}
	links, err = store.GetLinksForTarget(ctx, "planning_session", "session-detach")
	if err != nil {
		t.Fatal(err)
	}
	if len(links) != 1 || links[0].LatestSnapshotID != fresh.SnapshotID || links[0].ActiveSnapshotID != fresh.SnapshotID {
		t.Fatalf("adopting latest after re-attach regressed the source: %#v", links)
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
	workOrderLinks, err := store.GetLinksForTarget(ctx, "work_order", "work-order-detach-control")
	if err != nil || len(workOrderLinks) != 1 {
		t.Fatalf("control work order links=%#v err=%v", workOrderLinks, err)
	}
	assertExternalLinkCoordinates(t, workOrderLinks[0], capture.SnapshotID, "", capture.SnapshotID, "owner/repo", 7)
	var attachmentChanges, adoptions int
	if err := db.QueryRow(`SELECT COUNT(*) FROM external_source_link_attachment_changes WHERE link_id=?`, link.LinkID).Scan(&attachmentChanges); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM planning_external_source_adoptions WHERE link_id=?`, link.LinkID).Scan(&adoptions); err != nil {
		t.Fatal(err)
	}
	if attachmentChanges != 2 || adoptions != 3 {
		t.Fatalf("re-attach provenance rows: attachment_changes=%d adoptions=%d", attachmentChanges, adoptions)
	}
}

func TestDetachPlanningExternalSourceRejectsUnsupportedTargets(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	ctx := context.Background()
	at := "2026-08-12T10:00:00Z"

	if _, err := store.CreateConnection(ctx, CreateExternalConnectionRequest{ConnectionID: "conn-detach-guard", CredentialStrategy: "app_private_key", DisplayName: "App", AppID: "1", ClientID: "Iv1", AppSlug: "app", Installations: []ConnectionInstallation{{InstallationID: "2"}}, CredentialHandle: "github-app:conn-detach-guard", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-detach-guard"}); err != nil {
		t.Fatal(err)
	}
	snapshot, _, err := store.CaptureIssue(ctx, CaptureExternalIssueRequest{SourceID: "source-guard", SnapshotID: "snapshot-guard", ConnectionID: "conn-detach-guard", ProviderNativeID: "100", RepositoryNativeID: "200", Issue: NormalizedExternalIssue{Title: "Issue", Body: "body", State: "open", Repository: "owner/repo", IssueNumber: 7, CanonicalURL: "https://github.com/owner/repo/issues/7", CreatedAt: at, UpdatedAt: at}, Profile: ExternalImportProfile{}, ProviderUpdatedAt: at, FetchedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	mustExecArgs(t, db, `INSERT INTO planning_sessions (planning_session_id,title,title_state,lifecycle_state,conversation_state,project_root,model_selection_json,created_at,updated_at) VALUES ('session-archived-detach','Archived','custom','archived','ready','/tmp','{}',?,?)`, at, at)
	mustExecArgs(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work order-1', 'WorkOrder', 'text', '{}', ?)`, at)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := createExternalLinkTx(ctx, tx, "link-work order", "work_order", "work order-1", snapshot.SourceID, snapshot.SnapshotID, "local-operator", "", at); err != nil {
		t.Fatal(err)
	}
	if err := createExternalLinkTx(ctx, tx, "link-archived", "planning_session", "session-archived-detach", snapshot.SourceID, snapshot.SnapshotID, "local-operator", "adopt-archived", at); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Issued work orders keep their exact source; only planning sources are removable.
	if err := store.DetachPlanningExternalSource(ctx, DetachPlanningExternalSourceRequest{LinkID: "link-work order", ChangeID: "change-work order", ActorID: "local-operator", DetachedAt: at, AuditEventID: "audit-work-order-detach"}); !errors.Is(err, ErrExternalDetachUnsupported) {
		t.Fatalf("work order detach error=%v", err)
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

func multiInstallationFixture(t *testing.T) (*ExternalStore, context.Context, ExternalConnection) {
	t.Helper()
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewExternalStore(db)
	ctx := context.Background()
	at := "2026-08-19T10:00:00Z"
	connection, err := store.FinalizeConnection(ctx, FinalizeExternalConnectionRequest{CreateExternalConnectionRequest: CreateExternalConnectionRequest{
		ConnectionID: "conn-multi", CredentialStrategy: "user_access_token", DisplayName: "Kingdom Issues",
		AppID: "1", ClientID: "IvShared", AppSlug: "kingdom",
		Installations: []ConnectionInstallation{
			{InstallationID: "10", AccountID: "100", AccountLogin: "monalisa"},
			{InstallationID: "11", AccountID: "101", AccountLogin: "T-RexLabs"},
		},
		CredentialHandle: "github-user:conn-multi", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-multi",
	}})
	if err != nil {
		t.Fatal(err)
	}
	return store, ctx, connection
}

func TestOneConnectionCoversSeveralInstallations(t *testing.T) {
	store, ctx, connection := multiInstallationFixture(t)
	if len(connection.Installations) != 2 || !connection.HasCurrentValidation() {
		t.Fatalf("finalized connection=%#v", connection)
	}
	reloaded, err := store.GetConnection(ctx, "conn-multi")
	if err != nil {
		t.Fatal(err)
	}
	if got := reloaded.InstallationIDs(); len(got) != 2 || got[0] != "10" || got[1] != "11" {
		t.Fatalf("installation ids=%v, want a stable [10 11]", got)
	}
	listed, err := store.ListConnections(ctx)
	if err != nil || len(listed) != 1 || len(listed[0].Installations) != 2 {
		t.Fatalf("listing must carry installations: %#v err=%v", listed, err)
	}
}

func TestAConnectionWithoutInstallationsIsNeverCurrent(t *testing.T) {
	store, ctx, connection := multiInstallationFixture(t)
	if _, err := store.SetConnectionInstallations(ctx, connection.ConnectionID, SetConnectionInstallationsRequest{
		Installations: nil, ExpectedRowVersion: connection.RowVersion, ActorID: "local-operator", UpdatedAt: "2026-08-19T11:00:00Z", AuditEventID: "audit-empty",
	}); err == nil {
		t.Fatal("a connection must not be emptied of installations")
	}
	// The parent row is the credential; without a covered installation it can
	// authenticate but has nothing to reach, so it cannot be current.
	bare := ExternalConnection{Lifecycle: "enabled", HealthClass: "healthy", CredentialGeneration: 1, ValidatedGeneration: 1}
	if bare.HasCurrentValidation() {
		t.Fatal("a connection covering no installation must not be current")
	}
}

func TestChangingTheCoveredSetKeepsPinnedAccountsAndReleasesTheRest(t *testing.T) {
	store, ctx, connection := multiInstallationFixture(t)
	at := "2026-08-19T11:00:00Z"
	updated, err := store.SetConnectionInstallations(ctx, connection.ConnectionID, SetConnectionInstallationsRequest{
		Installations: []ConnectionInstallation{
			{InstallationID: "10", AccountID: "100", AccountLogin: "monalisa"},
			{InstallationID: "12", AccountID: "102", AccountLogin: "third-org"},
		},
		ExpectedRowVersion: connection.RowVersion, ActorID: "local-operator", UpdatedAt: at, AuditEventID: "audit-change",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := updated.InstallationIDs(); len(got) != 2 || got[0] != "10" || got[1] != "12" {
		t.Fatalf("installation ids=%v", got)
	}
	if updated.HealthClass != "never_checked" || updated.FailureClass != "" || updated.ValidatedGeneration != 0 || updated.LastCheckedAt != nil || updated.LastSuccessAt != nil {
		t.Fatalf("changed installation set retained stale health: %#v", updated)
	}
	if updated.RowVersion != connection.RowVersion+1 {
		t.Fatalf("changed installation set row version=%d, want %d", updated.RowVersion, connection.RowVersion+1)
	}
	if updated.Installations[0].AccountID != "100" || updated.Installations[0].AccountLogin != "monalisa" {
		t.Fatalf("surviving installation lost pinned identity: %#v", updated.Installations[0])
	}
	var auditCount int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM external_integration_audit_events WHERE audit_event_id='audit-change' AND event_type='connection.installations_changed'`).Scan(&auditCount); err != nil || auditCount != 1 {
		t.Fatalf("installation change audit count=%d err=%v", auditCount, err)
	}
	if err := NewAuditIntegrityVerifier(store.db).VerifyOrError(ctx); err != nil {
		t.Fatalf("installation change broke audit integrity: %v", err)
	}
	// The dropped installation is free for another connection to hold.
	var held int
	_ = store.db.QueryRow(`SELECT COUNT(*) FROM external_connection_installations WHERE installation_id='11'`).Scan(&held)
	if held != 0 {
		t.Fatal("a dropped installation must be released")
	}
	// An account cannot be swapped under an installation that already pinned one.
	if _, err := store.SetConnectionInstallations(ctx, connection.ConnectionID, SetConnectionInstallationsRequest{
		Installations:      []ConnectionInstallation{{InstallationID: "10", AccountID: "999", AccountLogin: "impostor"}},
		ExpectedRowVersion: updated.RowVersion, ActorID: "local-operator", UpdatedAt: at, AuditEventID: "audit-swap",
	}); !errors.Is(err, ErrExternalConflict) {
		t.Fatalf("account swap error=%v, want conflict", err)
	}
}

func TestSettingTheSameNormalizedInstallationSetDoesNotChurnState(t *testing.T) {
	store, ctx, connection := multiInstallationFixture(t)
	updated, err := store.SetConnectionInstallations(ctx, connection.ConnectionID, SetConnectionInstallationsRequest{
		Installations: []ConnectionInstallation{
			{InstallationID: " 11 ", AccountID: " 101 ", AccountLogin: " T-RexLabs "},
			{InstallationID: " 10 ", AccountID: " 100 ", AccountLogin: " monalisa "},
		},
		ExpectedRowVersion: connection.RowVersion, ActorID: "local-operator", UpdatedAt: "2026-08-19T11:00:00Z", AuditEventID: "audit-noop",
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated.RowVersion != connection.RowVersion || updated.UpdatedAt != connection.UpdatedAt || updated.HealthClass != "healthy" || updated.ValidatedGeneration != connection.ValidatedGeneration {
		t.Fatalf("equivalent installation set changed connection state: before=%#v after=%#v", connection, updated)
	}
	if updated.LastCheckedAt == nil || updated.LastSuccessAt == nil || *updated.LastCheckedAt != *connection.LastCheckedAt || *updated.LastSuccessAt != *connection.LastSuccessAt {
		t.Fatalf("equivalent installation set changed health timestamps: before=%#v after=%#v", connection, updated)
	}
	var auditCount int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM external_integration_audit_events WHERE audit_event_id='audit-noop'`).Scan(&auditCount); err != nil || auditCount != 0 {
		t.Fatalf("equivalent installation set audit count=%d err=%v", auditCount, err)
	}
	if err := NewAuditIntegrityVerifier(store.db).VerifyOrError(ctx); err != nil {
		t.Fatalf("equivalent installation set broke audit integrity: %v", err)
	}
}

func TestTombstoneReleasesInstallationsButRetainsAccountIdentity(t *testing.T) {
	store, ctx, connection := multiInstallationFixture(t)
	at := "2026-08-19T11:00:00Z"
	if err := store.UpdateConnectionStateAudited(ctx, connection.ConnectionID, "removed", "removed", "", at, false, "local-operator", "audit-removed", "connection.removed"); err != nil {
		t.Fatal(err)
	}
	var remaining int
	_ = store.db.QueryRow(`SELECT COUNT(*) FROM external_connection_installations WHERE connection_id='conn-multi'`).Scan(&remaining)
	if remaining != 0 {
		t.Fatalf("tombstone left %d installations held", remaining)
	}
	if _, err := store.FinalizeConnection(ctx, FinalizeExternalConnectionRequest{CreateExternalConnectionRequest: CreateExternalConnectionRequest{
		ConnectionID: "conn-impostor", CredentialStrategy: "user_access_token", DisplayName: "Impostor",
		AppID: "1", ClientID: "IvShared", AppSlug: "kingdom",
		Installations:    []ConnectionInstallation{{InstallationID: "10", AccountID: "999", AccountLogin: "impostor"}},
		CredentialHandle: "github-user:conn-impostor", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-impostor",
	}}); !errors.Is(err, ErrExternalConflict) {
		t.Fatalf("re-added installation accepted a different account identity: %v", err)
	}
	readded, err := store.FinalizeConnection(ctx, FinalizeExternalConnectionRequest{CreateExternalConnectionRequest: CreateExternalConnectionRequest{
		ConnectionID: "conn-again", CredentialStrategy: "user_access_token", DisplayName: "Again",
		AppID: "1", ClientID: "IvShared", AppSlug: "kingdom",
		Installations:    []ConnectionInstallation{{InstallationID: "10", AccountID: "100", AccountLogin: "monalisa-renamed"}},
		CredentialHandle: "github-user:conn-again", ActorID: "local-operator", CreatedAt: at, AuditEventID: "audit-again",
	}})
	if err != nil {
		t.Fatalf("a released installation must be connectable again: %v", err)
	}
	if onlyInstallation(t, readded) != (ConnectionInstallation{InstallationID: "10", AccountID: "100", AccountLogin: "monalisa-renamed"}) {
		t.Fatalf("re-added installation did not retain its account identity: %#v", readded.Installations)
	}
}

func TestReauthorizationReprovesTheSetRatherThanAdoptingOne(t *testing.T) {
	store, ctx, connection := multiInstallationFixture(t)
	at := "2026-08-19T11:00:00Z"
	proved := []ConnectionInstallation{
		{InstallationID: "10", AccountID: "100", AccountLogin: "monalisa"},
		{InstallationID: "11", AccountID: "101", AccountLogin: "T-RexLabs-renamed"},
	}
	updated, err := store.ReauthorizeSharedConnection(ctx, connection, "kingdom", proved, "local-operator", "audit-reauth", at)
	if err != nil {
		t.Fatal(err)
	}
	if updated.CredentialGeneration != connection.CredentialGeneration+1 || updated.Installations[1].AccountLogin != "T-RexLabs-renamed" {
		t.Fatalf("reauthorized=%#v", updated)
	}
	// A newly visible installation is not silently adopted.
	widened := append(append([]ConnectionInstallation{}, proved...), ConnectionInstallation{InstallationID: "12", AccountID: "102", AccountLogin: "third-org"})
	if _, err := store.ReauthorizeSharedConnection(ctx, updated, "kingdom", widened, "local-operator", "audit-reauth-wide", at); !errors.Is(err, ErrExternalConflict) {
		t.Fatalf("adopting a new installation on reconnect error=%v, want conflict", err)
	}
	if _, err := store.ReauthorizeSharedConnection(ctx, updated, "kingdom", proved[:1], "local-operator", "audit-reauth-subset", at); !errors.Is(err, ErrExternalConflict) {
		t.Fatalf("dropping an installation on reconnect error=%v, want conflict", err)
	}
	impostor := append([]ConnectionInstallation(nil), proved...)
	impostor[1].AccountID = "999"
	if _, err := store.ReauthorizeSharedConnection(ctx, updated, "kingdom", impostor, "local-operator", "audit-reauth-account", at); !errors.Is(err, ErrExternalConflict) {
		t.Fatalf("changing a pinned account on reconnect error=%v, want conflict", err)
	}
}
