package state

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"kingdom/internal/acp"
	"kingdom/internal/dbmigrations"
)

func TestPlanningContentMigrationPreservesHistoricalSnapshotBytes(t *testing.T) {
	db := openTestDB(t)
	raw, err := dbmigrations.Read("0001_initial_schema.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(raw)); err != nil {
		t.Fatal(err)
	}
	store := NewPlanningStore(db)
	ctx := context.Background()
	at := "2026-04-21T16:14:00Z"
	if _, err := store.CreateSession(ctx, CreatePlanningSessionRequest{PlanningSessionID: "historical-content", Title: "Historical", TitleState: "custom", LifecycleState: PlanningSessionLifecycleStateActive, ConversationState: PlanningSessionConversationStateReady, ProjectRoot: "/project", ModelSelectionJSON: `{}`, CreatedAt: at, UpdatedAt: at}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO planning_context_snapshots(planning_context_snapshot_id,planning_session_id,summary_markdown,included_files_json,excluded_files_json,created_at) VALUES('historical-snapshot','historical-content','Original summary',?,?,?)`, `[{"path":" leading 界.go ","size_bytes":123}]`, `[{"path":".env","reason":"secret"}]`, at); err != nil {
		t.Fatal(err)
	}
	before, err := store.GetContextSnapshot(ctx, "historical-snapshot")
	if err != nil {
		t.Fatal(err)
	}
	raw, err = dbmigrations.Read("0009_planning_content.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(raw)); err != nil {
		t.Fatal(err)
	}
	after, err := store.GetContextSnapshot(ctx, before.PlanningContextSnapshotID)
	if err != nil || after != before {
		t.Fatalf("migration rewrote historical snapshot: %v", err)
	}
	entries, cursor, err := store.ListContextEntriesPage(ctx, before.PlanningContextSnapshotID, "included", -1, 1)
	if err != nil || len(entries) != 1 || cursor != 0 || entries[0].Path != " leading 界.go " || entries[0].Size != 123 {
		t.Fatalf("historical included entry: %#v %d %v", entries, cursor, err)
	}
	entries, cursor, err = store.ListContextEntriesPage(ctx, before.PlanningContextSnapshotID, "excluded", -1, 1)
	if err != nil || len(entries) != 1 || cursor != 1 || entries[0].Reason != "secret" {
		t.Fatalf("historical excluded entry: %#v %d %v", entries, cursor, err)
	}
}

func TestPlanningMessagePagesPreserveDeepPathAndRejectCycles(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewPlanningStore(db)
	ctx := context.Background()
	at := "2026-04-21T16:14:00Z"
	if _, err := store.CreateSession(ctx, CreatePlanningSessionRequest{PlanningSessionID: "deep-messages", Title: "Deep", TitleState: "custom", LifecycleState: PlanningSessionLifecycleStateActive, ConversationState: PlanningSessionConversationStateReady, ProjectRoot: "/project", ModelSelectionJSON: `{}`, CreatedAt: at, UpdatedAt: at}); err != nil {
		t.Fatal(err)
	}
	parent := ""
	for i := 0; i < 305; i++ {
		id := fmt.Sprintf("message-%03d", 304-i) // Parent order, not lexical order.
		if _, err := store.AppendMessage(ctx, AppendPlanningMessageRequest{PlanningMessageID: id, PlanningSessionID: "deep-messages", ParentMessageID: parent, Role: PlanningMessageRoleUser, ContentMarkdown: fmt.Sprintf("turn-%d", i), QuestionPayloadJSON: `{}`, AnswerPayloadJSON: `{}`, CreatedAt: at}); err != nil {
			t.Fatal(err)
		}
		parent = id
	}
	if err := store.UpdateSession(ctx, UpdatePlanningSessionRequest{PlanningSessionID: "deep-messages", ActiveLeafMessageID: &parent, UpdatedAt: at}); err != nil {
		t.Fatal(err)
	}
	before, total := 305, 0
	for before > 0 {
		page, err := store.ListMessagesPage(ctx, "deep-messages", 41, &before)
		if err != nil || page.TotalCount != 305 {
			t.Fatalf("message page: %v", err)
		}
		for i, message := range page.Items {
			if message.ContentMarkdown != fmt.Sprintf("turn-%d", page.FirstItemIndex+i) {
				t.Fatal("page reordered ancestry")
			}
		}
		total += len(page.Items)
		before = page.FirstItemIndex
	}
	if total != 305 {
		t.Fatalf("paged %d messages", total)
	}
	if _, err := db.Exec(`UPDATE planning_messages SET parent_message_id='message-000' WHERE planning_message_id='message-304'`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ListMessages(ctx, "deep-messages"); err == nil {
		t.Fatal("cyclic history returned a hidden partial transcript")
	}
	if _, err := descendToLeaf(ctx, db, "deep-messages", "message-000"); err == nil {
		t.Fatal("cyclic descendants returned a hidden partial branch")
	}
}

func TestPlanningStoreCreateListAndGetSession(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	store := NewPlanningStore(db)
	created, err := store.CreateSession(context.Background(), CreatePlanningSessionRequest{
		PlanningSessionID:  "ps_1",
		Title:              "Planner session",
		TitleState:         "custom",
		LifecycleState:     PlanningSessionLifecycleStateActive,
		ConversationState:  PlanningSessionConversationStateReady,
		ProjectRoot:        "/workspace/project",
		ModelSelectionJSON: `{"model":"github-copilot/gpt-5.4","thinking_effort":"medium"}`,
		CreatedAt:          "2026-04-21T16:00:00Z",
		UpdatedAt:          "2026-04-21T16:00:00Z",
	})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	if created.PlanningSessionID != "ps_1" {
		t.Fatalf("expected planning session id ps_1, got %q", created.PlanningSessionID)
	}
	if created.ConversationState != PlanningSessionConversationStateReady {
		t.Fatalf("expected conversation_state ready, got %q", created.ConversationState)
	}

	items, err := store.ListSessions(context.Background(), 10)
	if err != nil {
		t.Fatalf("list sessions: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 session, got %d", len(items))
	}

	fetched, ok, err := store.GetSession(context.Background(), "ps_1")
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if !ok {
		t.Fatal("expected session to exist")
	}
	if fetched.ProjectRoot != "/workspace/project" {
		t.Fatalf("expected project root /workspace/project, got %q", fetched.ProjectRoot)
	}
	if fetched.TitleState != "custom" {
		t.Fatalf("expected title state custom, got %q", fetched.TitleState)
	}
	if fetched.LifecycleState != PlanningSessionLifecycleStateActive {
		t.Fatalf("expected active lifecycle state, got %q", fetched.LifecycleState)
	}
}

func TestValidatePlanningContextSnapshotCreatePreservesFullFileJSON(t *testing.T) {
	t.Parallel()

	valid := CreatePlanningContextSnapshotRequest{
		PlanningContextSnapshotID: "pcs_bounds",
		PlanningSessionID:         "ps_bounds",
		SummaryMarkdown:           "context",
		IncludedFilesJSON:         `[]`,
		ExcludedFilesJSON:         `[]`,
		CreatedAt:                 "2026-04-21T16:14:00Z",
	}
	tests := []struct {
		name    string
		edit    func(*CreatePlanningContextSnapshotRequest)
		invalid bool
	}{
		{
			name: "more than 200 excluded entries",
			edit: func(req *CreatePlanningContextSnapshotRequest) {
				req.ExcludedFilesJSON = "[" + strings.Repeat(`{"path":".env","reason":"secret"},`, 250) + `{"path":".env","reason":"secret"}]`
			},
		},
		{
			name: "oversized included json",
			edit: func(req *CreatePlanningContextSnapshotRequest) {
				req.IncludedFilesJSON = `[{"path":"` + strings.Repeat("a", 1<<20) + `","reason":"` + strings.Repeat("r", 256) + `"}]`
				req.SummaryMarkdown = strings.Repeat("summary", 10000)
			},
		},
		{
			name:    "malformed excluded json",
			invalid: true,
			edit: func(req *CreatePlanningContextSnapshotRequest) {
				req.ExcludedFilesJSON = `not-json`
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := valid
			tt.edit(&req)
			if err := validatePlanningContextSnapshotCreate(req); (err != nil) != tt.invalid {
				t.Fatalf("validation error=%v, want invalid=%v", err, tt.invalid)
			}
		})
	}
}

func TestPlanningContentBundleRollsBackAndSealsCompleteEntries(t *testing.T) {
	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewPlanningStore(db)
	ctx := context.Background()
	at := "2026-04-21T16:14:00Z"
	req := CreatePlanningSessionBundleRequest{
		Session:         CreatePlanningSessionRequest{PlanningSessionID: "content-session", Title: "Content", TitleState: "custom", LifecycleState: PlanningSessionLifecycleStateActive, ConversationState: PlanningSessionConversationStateReady, ProjectRoot: t.TempDir(), ModelSelectionJSON: `{}`, CreatedAt: at, UpdatedAt: at},
		OpeningMessage:  AppendPlanningMessageRequest{PlanningMessageID: "content-opening", PlanningSessionID: "content-session", Role: PlanningMessageRoleAssistant, ContentMarkdown: "Hello", QuestionPayloadJSON: `{}`, AnswerPayloadJSON: `{}`, CreatedAt: at},
		ContextSnapshot: CreatePlanningContextSnapshotRequest{PlanningContextSnapshotID: "content-snapshot", PlanningSessionID: "content-session", SummaryMarkdown: strings.Repeat("summary ", 5000), IncludedFilesJSON: `[]`, ExcludedFilesJSON: `[]`, CreatedAt: at},
	}
	fail := true
	source := func(ctx context.Context, visit func(string, PlanningContextFileEntry) error) error {
		for i := 0; i < 250; i++ {
			if err := visit("included", PlanningContextFileEntry{Path: fmt.Sprintf("path-%03d-%s", i, strings.Repeat("x", 5000)), Reason: strings.Repeat("r", 256), Size: 128 << 10}); err != nil {
				return err
			}
		}
		if fail {
			return fmt.Errorf("interrupted content spool")
		}
		return nil
	}
	req.ContextSnapshot.Entries = source
	if _, err := store.CreateSessionBundle(ctx, req); err == nil {
		t.Fatal("failed spool was published")
	}
	for _, table := range []string{"planning_sessions", "planning_messages", "planning_context_snapshots", "planning_context_entries"} {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("%s partially committed: %d %v", table, count, err)
		}
	}
	fail = false
	session, err := store.CreateSessionBundle(ctx, req)
	if err != nil || session.ContextSnapshotID == nil || *session.ContextSnapshotID != "content-snapshot" {
		t.Fatalf("atomic publication: %#v %v", session, err)
	}
	for _, statement := range []string{
		`INSERT INTO planning_context_entries VALUES ('content-snapshot',250,'included','injected','',0)`,
		`UPDATE planning_context_entries SET path='mutated' WHERE snapshot_id='content-snapshot'`,
		`DELETE FROM planning_context_entries WHERE snapshot_id='content-snapshot'`,
	} {
		if _, err := db.Exec(statement); err == nil {
			t.Fatalf("immutable snapshot accepted %s", statement)
		}
	}
	snapshot, err := store.GetContextSnapshot(ctx, "content-snapshot")
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.IncludedFilesJSON != "[]" {
		t.Fatal("summary must not hydrate all entries")
	}
	after, count, bytes := -1, 0, 0
	var lastPath string
	for {
		entries, cursor, err := store.ListContextEntriesPage(ctx, snapshot.PlanningContextSnapshotID, "included", after, 100)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			count++
			bytes += len(entry.Path)
			lastPath = entry.Path
		}
		if len(entries) < 100 {
			break
		}
		after = cursor
	}
	if count != 250 || bytes <= 1<<20 || !strings.Contains(lastPath, "path-249-") {
		t.Fatalf("entry pages lost content: count=%d bytes=%d", count, bytes)
	}
}

func TestPlanningActivityContinuationBatchesPreserveLargePayloadAndLeaseFence(t *testing.T) {
	store, operation := newPlanningActivityReconciliationTestStore(t)
	ctx := context.Background()
	now, expiry := "2026-04-21T16:00:00Z", "2026-04-21T16:01:00Z"
	claimed, ok, err := store.ClaimPlanningActivityReconciliation(ctx, operation.OperationID, "worker", expiry, now)
	if err != nil || !ok {
		t.Fatalf("claim: %v", err)
	}
	payload := `{"detail":"` + strings.Repeat("x", 300<<10) + `tail"}`
	for batch := 0; batch < 3; batch++ {
		events := make([]AppendPlanningOperationEventRequest, 100)
		for i := range events {
			events[i] = AppendPlanningOperationEventRequest{EventID: fmt.Sprintf("event-%d-%d", batch, i), OperationID: operation.OperationID, Seq: i + 1, EventType: "generic", PayloadJSON: `{}`, CreatedAt: now}
		}
		if batch == 0 {
			events[0].PayloadJSON = payload
		}
		if err := store.AppendPlanningActivityReconciliationBatch(ctx, claimed, events, now, expiry); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.CompletePlanningActivityReconciliation(ctx, reconciliationCompletion(operation.OperationID, claimed.LeaseOwner, claimed.LeaseVersion, now, nil)); err != nil {
		t.Fatal(err)
	}
	if count, err := store.PlanningActivityDeliveredCount(ctx, operation.OperationID); err != nil || count != 300 {
		t.Fatalf("durable continuation count=%d err=%v", count, err)
	}
	events, err := store.ListOperationEventsAfterSeqPage(ctx, operation.OperationID, 0, 400)
	if err != nil || len(events) != 301 || events[0].PayloadJSON != payload || events[300].EventType != "run.activity.reconciled" {
		t.Fatalf("batched diagnostic replay: count=%d err=%v", len(events), err)
	}
	late := AppendPlanningOperationEventRequest{EventID: "late", OperationID: operation.OperationID, Seq: 1, EventType: "generic", PayloadJSON: `{}`, CreatedAt: now}
	if err := store.AppendPlanningActivityReconciliationBatch(ctx, claimed, []AppendPlanningOperationEventRequest{late}, now, expiry); !errors.Is(err, ErrPlanningLeaseConflict) {
		t.Fatalf("completed reconciliation accepted stale writer: %v", err)
	}
}

func TestListRecentCompletedOperationTelemetryUsesCurrentACPEvents(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewPlanningStore(db)
	ctx := context.Background()

	if _, err := store.CreateSession(ctx, CreatePlanningSessionRequest{
		PlanningSessionID:  "ps_telemetry",
		Title:              "Telemetry",
		TitleState:         "custom",
		LifecycleState:     PlanningSessionLifecycleStateActive,
		ConversationState:  PlanningSessionConversationStateReady,
		ProjectRoot:        "/workspace/project",
		ModelSelectionJSON: `{"model":"github-copilot/gpt-5.4"}`,
		CreatedAt:          "2026-08-24T10:00:00Z",
		UpdatedAt:          "2026-08-24T10:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	bindTestPlanningContext(t, db, "ps_telemetry")
	if _, err := store.CreateOperation(ctx, CreatePlanningOperationRequest{
		OperationID:        "po_telemetry",
		PlanningSessionID:  "ps_telemetry",
		Type:               PlanningOperationTypeReply,
		Status:             PlanningOperationStatusSucceeded,
		RequestPayloadJSON: `{}`,
		CreatedAt:          "2026-08-24T10:00:01Z",
		StartedAt:          "2026-08-24T10:00:02Z",
		FinishedAt:         "2026-08-24T10:00:09Z",
	}); err != nil {
		t.Fatalf("create operation: %v", err)
	}

	events := []AppendPlanningOperationEventRequest{
		{EventID: "po_telemetry:1", OperationID: "po_telemetry", Seq: 1, EventType: "provider_session_open", PayloadJSON: `{}`, CreatedAt: "2026-08-24T10:00:03Z"},
		{EventID: "po_telemetry:2", OperationID: "po_telemetry", Seq: 2, EventType: acp.GenericEventType, PayloadJSON: `{"kind":"not-a-phase","phase":"provider_session_open"}`, CreatedAt: "2026-08-24T10:00:04Z"},
		{EventID: "po_telemetry:3", OperationID: "po_telemetry", Seq: 3, EventType: acp.GenericEventType, PayloadJSON: `{"kind":"kingdom.phase","phase":"provider_session_open"}`, CreatedAt: "2026-08-24T10:00:05Z"},
		{EventID: "po_telemetry:4", OperationID: "po_telemetry", Seq: 4, EventType: "text_delta", PayloadJSON: `{"text":"obsolete"}`, CreatedAt: "2026-08-24T10:00:03Z"},
		{EventID: "po_telemetry:5", OperationID: "po_telemetry", Seq: 5, EventType: acp.MessagePartEventType, PayloadJSON: `{"text":"current"}`, CreatedAt: "2026-08-24T10:00:06Z"},
		{EventID: "po_telemetry:6", OperationID: "po_telemetry", Seq: 6, EventType: "finalizing", PayloadJSON: `{}`, CreatedAt: "2026-08-24T10:00:03Z"},
		{EventID: "po_telemetry:7", OperationID: "po_telemetry", Seq: 7, EventType: acp.GenericEventType, PayloadJSON: `{"kind":"kingdom.phase","phase":"finalizing"}`, CreatedAt: "2026-08-24T10:00:07Z"},
	}
	for _, event := range events {
		if _, err := store.AppendOperationEvent(ctx, event); err != nil {
			t.Fatalf("append operation event %s: %v", event.EventID, err)
		}
	}

	samples, err := store.ListRecentCompletedOperationTelemetryByType(ctx, PlanningOperationTypeReply, 10)
	if err != nil {
		t.Fatalf("list telemetry samples: %v", err)
	}
	if len(samples) != 1 {
		t.Fatalf("expected one telemetry sample, got %d", len(samples))
	}
	sample := samples[0]
	assertPlanningTelemetryTimestamp(t, "started", sample.StartedAt, "2026-08-24T10:00:02Z")
	assertPlanningTelemetryTimestamp(t, "finished", sample.FinishedAt, "2026-08-24T10:00:09Z")
	assertPlanningTelemetryTimestamp(t, "provider session open", sample.ProviderSessionOpenAt, "2026-08-24T10:00:05Z")
	assertPlanningTelemetryTimestamp(t, "first text", sample.FirstTextDeltaAt, "2026-08-24T10:00:06Z")
	assertPlanningTelemetryTimestamp(t, "finalizing", sample.FinalizingAt, "2026-08-24T10:00:07Z")
}

func assertPlanningTelemetryTimestamp(t *testing.T, name string, got *string, want string) {
	t.Helper()
	if got == nil || *got != want {
		t.Fatalf("expected nonzero %s timestamp %q, got %v", name, want, got)
	}
}

func TestSetActiveBranchRefreshesSessionRecency(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewPlanningStore(db)
	ctx := context.Background()

	if _, err := store.CreateSession(ctx, CreatePlanningSessionRequest{
		PlanningSessionID:  "ps_old_branch",
		Title:              "older branch session",
		TitleState:         "custom",
		LifecycleState:     PlanningSessionLifecycleStateActive,
		ConversationState:  PlanningSessionConversationStateReady,
		ProjectRoot:        "/workspace/project",
		ModelSelectionJSON: `{"model":"m"}`,
		CreatedAt:          "2026-04-21T16:00:00Z",
		UpdatedAt:          "2026-04-21T16:00:00Z",
	}); err != nil {
		t.Fatalf("create older session: %v", err)
	}
	if _, err := store.CreateSession(ctx, CreatePlanningSessionRequest{
		PlanningSessionID:  "ps_recent",
		Title:              "recent session",
		TitleState:         "custom",
		LifecycleState:     PlanningSessionLifecycleStateActive,
		ConversationState:  PlanningSessionConversationStateReady,
		ProjectRoot:        "/workspace/project",
		ModelSelectionJSON: `{"model":"m"}`,
		CreatedAt:          "2026-04-21T16:10:00Z",
		UpdatedAt:          "2026-04-21T16:10:00Z",
	}); err != nil {
		t.Fatalf("create recent session: %v", err)
	}

	if _, err := store.AppendMessage(ctx, AppendPlanningMessageRequest{
		PlanningMessageID:   "pm_root_old",
		PlanningSessionID:   "ps_old_branch",
		Role:                PlanningMessageRoleAssistant,
		ContentMarkdown:     "opening",
		QuestionPayloadJSON: `{}`,
		AnswerPayloadJSON:   `{}`,
		CreatedAt:           "2026-04-21T16:00:05Z",
	}); err != nil {
		t.Fatalf("append root: %v", err)
	}
	if _, err := store.AppendMessage(ctx, AppendPlanningMessageRequest{
		PlanningMessageID:   "pm_branch_old",
		PlanningSessionID:   "ps_old_branch",
		ParentMessageID:     "pm_root_old",
		Role:                PlanningMessageRoleUser,
		ContentMarkdown:     "older branch",
		QuestionPayloadJSON: `{}`,
		AnswerPayloadJSON:   `{"text":"older branch"}`,
		CreatedAt:           "2026-04-21T16:00:06Z",
	}); err != nil {
		t.Fatalf("append old branch: %v", err)
	}
	if _, err := store.AppendMessage(ctx, AppendPlanningMessageRequest{
		PlanningMessageID:   "pm_branch_newer",
		PlanningSessionID:   "ps_old_branch",
		ParentMessageID:     "pm_root_old",
		Role:                PlanningMessageRoleUser,
		ContentMarkdown:     "newer branch",
		QuestionPayloadJSON: `{}`,
		AnswerPayloadJSON:   `{"text":"newer branch"}`,
		CreatedAt:           "2026-04-21T16:00:07Z",
	}); err != nil {
		t.Fatalf("append newer branch: %v", err)
	}

	before, err := store.ListSessions(ctx, 10)
	if err != nil {
		t.Fatalf("list sessions before activation: %v", err)
	}
	if len(before) != 2 || before[0].PlanningSessionID != "ps_recent" {
		t.Fatalf("expected recent session first before activation, got %+v", before)
	}

	if _, err := store.SetActiveBranch(ctx, "ps_old_branch", "pm_branch_old"); err != nil {
		t.Fatalf("activate older branch: %v", err)
	}

	after, err := store.ListSessions(ctx, 10)
	if err != nil {
		t.Fatalf("list sessions after activation: %v", err)
	}
	if len(after) != 2 {
		t.Fatalf("expected 2 sessions after activation, got %d", len(after))
	}
	if after[0].PlanningSessionID != "ps_old_branch" {
		t.Fatalf("expected activated session to become most recent, got %+v", after)
	}
	if after[0].UpdatedAt <= "2026-04-21T16:10:00Z" {
		t.Fatalf("expected activated session updated_at to move forward, got %q", after[0].UpdatedAt)
	}
	if after[0].ActiveLeafMessageID == nil || *after[0].ActiveLeafMessageID != "pm_branch_old" {
		t.Fatalf("expected active leaf to move to selected branch, got %+v", after[0].ActiveLeafMessageID)
	}
}

func TestPlanningStoreAppendListMessagesAndLatestRecords(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	store := NewPlanningStore(db)
	_, err := store.CreateSession(context.Background(), CreatePlanningSessionRequest{
		PlanningSessionID:  "ps_flow",
		Title:              "Planner flow",
		TitleState:         "custom",
		LifecycleState:     PlanningSessionLifecycleStateActive,
		ConversationState:  PlanningSessionConversationStateReady,
		ProjectRoot:        "/workspace/project",
		ModelSelectionJSON: `{"model":"github-copilot/gpt-5.4"}`,
		CreatedAt:          "2026-04-21T16:10:00Z",
		UpdatedAt:          "2026-04-21T16:10:00Z",
	})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}

	// pm_user is the conversation root; pm_assistant is its child. ListMessages
	// walks the active path from the session's active leaf, so point the leaf at
	// the deepest message.
	_, err = store.AppendMessage(context.Background(), AppendPlanningMessageRequest{
		PlanningMessageID:   "pm_user",
		PlanningSessionID:   "ps_flow",
		Role:                PlanningMessageRoleUser,
		ContentMarkdown:     "Need a planner",
		QuestionPayloadJSON: `{}`,
		AnswerPayloadJSON:   `{"text":"Need a planner"}`,
		CreatedAt:           "2026-04-21T16:11:00Z",
	})
	if err != nil {
		t.Fatalf("append user message: %v", err)
	}

	_, err = store.AppendMessage(context.Background(), AppendPlanningMessageRequest{
		PlanningMessageID:   "pm_assistant",
		PlanningSessionID:   "ps_flow",
		ParentMessageID:     "pm_user",
		Role:                PlanningMessageRoleAssistant,
		ContentMarkdown:     "What scope should we cover?",
		QuestionPayloadJSON: `{"prompt":"What scope should we cover?","type":"long_text"}`,
		AnswerPayloadJSON:   `{}`,
		CreatedAt:           "2026-04-21T16:12:00Z",
	})
	if err != nil {
		t.Fatalf("append assistant message: %v", err)
	}
	if err := store.UpdateSession(context.Background(), UpdatePlanningSessionRequest{
		PlanningSessionID:   "ps_flow",
		ActiveLeafMessageID: refString("pm_assistant"),
		UpdatedAt:           "2026-04-21T16:12:30Z",
	}); err != nil {
		t.Fatalf("set active leaf: %v", err)
	}

	messages, err := store.ListMessages(context.Background(), "ps_flow")
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(messages))
	}
	if messages[0].PlanningMessageID != "pm_user" || messages[1].PlanningMessageID != "pm_assistant" {
		t.Fatalf("unexpected message order: %+v", messages)
	}

	_, err = store.CreateDraft(context.Background(), CreatePlanningDraftRequest{
		PlanningDraftID:   "pd_1",
		PlanningSessionID: "ps_flow",
		DecreeTitle:       "Planner title",
		DecreeText:        "Planner text",
		SummaryJSON:       `{"ready":false}`,
		CreatedAt:         "2026-04-21T16:13:00Z",
	})
	if err != nil {
		t.Fatalf("create draft: %v", err)
	}

	_, err = store.CreateContextSnapshot(context.Background(), CreatePlanningContextSnapshotRequest{
		PlanningContextSnapshotID: "pcs_1",
		PlanningSessionID:         "ps_flow",
		SummaryMarkdown:           "Scanned 2 files",
		IncludedFilesJSON:         `[{"path":"README.md","size_bytes":1},{"path":"docs/system-architecture.md","size_bytes":1}]`,
		ExcludedFilesJSON:         `[{"path":".env","reason":"secret"}]`,
		CreatedAt:                 "2026-04-21T16:14:00Z",
	})
	if err != nil {
		t.Fatalf("create context snapshot: %v", err)
	}

	draft, ok, err := store.GetLatestDraft(context.Background(), "ps_flow")
	if err != nil {
		t.Fatalf("get latest draft: %v", err)
	}
	if !ok || draft.PlanningDraftID != "pd_1" {
		t.Fatalf("expected latest draft pd_1, got ok=%v draft=%+v", ok, draft)
	}

	snapshot, err := store.GetContextSnapshot(context.Background(), "pcs_1")
	if err != nil {
		t.Fatalf("get context snapshot: %v", err)
	}
	if snapshot.PlanningSessionID != "ps_flow" {
		t.Fatalf("expected context snapshot pcs_1 in ps_flow, got %+v", snapshot)
	}
	entries, _, err := store.ListContextEntriesPage(context.Background(), "pcs_1", "included", -1, 100)
	if err != nil || len(entries) != 2 || entries[1].Path != "docs/system-architecture.md" || snapshot.IncludedFilesJSON != "[]" {
		t.Fatalf("canonical snapshot path failed to normalize entries: %#v %v", entries, err)
	}

	conversationState := PlanningSessionConversationStateProcessing
	if err := store.UpdateSession(context.Background(), UpdatePlanningSessionRequest{
		PlanningSessionID:   "ps_flow",
		Title:               refString("Planner flow updated"),
		TitleState:          refString("generated"),
		ConversationState:   &conversationState,
		ActiveLeafMessageID: refString("pm_assistant"),
		CurrentDraftID:      refString("pd_1"),
		ContextSnapshotID:   refString("pcs_1"),
		ModelSelectionJSON:  refString(`{"model":"opencode/gpt-5-nano"}`),
		UpdatedAt:           "2026-04-21T16:15:00Z",
	}); err != nil {
		t.Fatalf("update session: %v", err)
	}

	fetched, ok := PlanningSession{}, false
	fetched, ok, err = store.GetSession(context.Background(), "ps_flow")
	if err != nil {
		t.Fatalf("get updated session: %v", err)
	}
	if !ok {
		t.Fatal("expected updated session to exist")
	}
	if fetched.CurrentDraftID == nil || *fetched.CurrentDraftID != "pd_1" || fetched.ContextSnapshotID == nil {
		t.Fatalf("unexpected updated session: %+v", fetched)
	}
	if fetched.TitleState != "generated" {
		t.Fatalf("expected updated title state generated, got %q", fetched.TitleState)
	}
	if fetched.ConversationState != PlanningSessionConversationStateProcessing {
		t.Fatalf("expected updated conversation state processing, got %q", fetched.ConversationState)
	}
	if fetched.ActiveLeafMessageID == nil || *fetched.ActiveLeafMessageID != "pm_assistant" {
		t.Fatalf("expected active leaf message id pm_assistant, got %+v", fetched.ActiveLeafMessageID)
	}
}

func TestPlanningStoreOperationsAndEvents(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	store := NewPlanningStore(db)
	_, err := store.CreateSession(context.Background(), CreatePlanningSessionRequest{
		PlanningSessionID:  "ps_ops",
		Title:              "Planner ops",
		TitleState:         "custom",
		LifecycleState:     PlanningSessionLifecycleStateActive,
		ConversationState:  PlanningSessionConversationStateReady,
		ProjectRoot:        "/workspace/project",
		ModelSelectionJSON: `{"model":"github-copilot/gpt-5.4"}`,
		CreatedAt:          "2026-04-21T16:30:00Z",
		UpdatedAt:          "2026-04-21T16:30:00Z",
	})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}

	bindTestPlanningContext(t, db, "ps_ops")
	operation, err := store.CreateOperation(context.Background(), CreatePlanningOperationRequest{
		OperationID:        "po_1",
		PlanningSessionID:  "ps_ops",
		Type:               PlanningOperationTypeReply,
		Status:             PlanningOperationStatusQueued,
		RequestPayloadJSON: `{"content_markdown":"hello"}`,
		CreatedAt:          "2026-04-21T16:31:00Z",
	})
	if err != nil {
		t.Fatalf("create operation: %v", err)
	}
	if operation.Type != PlanningOperationTypeReply {
		t.Fatalf("expected reply operation type, got %q", operation.Type)
	}

	startedAt := "2026-04-21T16:31:10Z"
	statusRunning := PlanningOperationStatusRunning
	if err := store.UpdateOperation(context.Background(), UpdatePlanningOperationRequest{
		OperationID: operation.OperationID,
		Status:      &statusRunning,
		StartedAt:   &startedAt,
	}); err != nil {
		t.Fatalf("update operation running: %v", err)
	}

	_, err = store.AppendOperationEvent(context.Background(), AppendPlanningOperationEventRequest{
		EventID:     "poe_1",
		OperationID: operation.OperationID,
		Seq:         1,
		EventType:   "user_message",
		PayloadJSON: `{"planning_message_id":"pm_1"}`,
		CreatedAt:   "2026-04-21T16:31:11Z",
	})
	if err != nil {
		t.Fatalf("append operation event 1: %v", err)
	}
	_, err = store.AppendOperationEvent(context.Background(), AppendPlanningOperationEventRequest{
		EventID:     "poe_2",
		OperationID: operation.OperationID,
		Seq:         2,
		EventType:   "question_ready",
		PayloadJSON: `{"planning_message_id":"pm_2"}`,
		CreatedAt:   "2026-04-21T16:31:12Z",
	})
	if err != nil {
		t.Fatalf("append operation event 2: %v", err)
	}

	resultRef := "pm_2"
	finishedAt := "2026-04-21T16:31:13Z"
	statusSucceeded := PlanningOperationStatusSucceeded
	if err := store.UpdateOperation(context.Background(), UpdatePlanningOperationRequest{
		OperationID: operation.OperationID,
		Status:      &statusSucceeded,
		ResultRef:   &resultRef,
		FinishedAt:  &finishedAt,
	}); err != nil {
		t.Fatalf("update operation succeeded: %v", err)
	}

	fetchedOperation, ok, err := store.GetOperation(context.Background(), operation.OperationID)
	if err != nil {
		t.Fatalf("get operation: %v", err)
	}
	if !ok {
		t.Fatal("expected operation to exist")
	}
	if fetchedOperation.Status != PlanningOperationStatusSucceeded {
		t.Fatalf("expected succeeded operation status, got %q", fetchedOperation.Status)
	}
	if fetchedOperation.ResultRef == nil || *fetchedOperation.ResultRef != "pm_2" {
		t.Fatalf("expected result_ref pm_2, got %+v", fetchedOperation.ResultRef)
	}

	events, err := store.ListOperationEventsAfterSeqPage(context.Background(), operation.OperationID, 1, 10)
	if err != nil {
		t.Fatalf("list operation events after seq: %v", err)
	}
	if len(events) != 1 || events[0].EventID != "poe_2" {
		t.Fatalf("expected one replayable event poe_2, got %+v", events)
	}

	queuedAndRunning, err := store.ListOperationsByStatusPage(context.Background(), "", "", 10, PlanningOperationStatusQueued, PlanningOperationStatusRunning)
	if err != nil {
		t.Fatalf("list operations by status: %v", err)
	}
	if len(queuedAndRunning) != 0 {
		t.Fatalf("expected no queued/running operations, got %+v", queuedAndRunning)
	}
}

func TestPlanningStoreOperationAcpRunIDIsCanonicalAndCorruptionRemainsVisible(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	ctx := context.Background()
	store := NewPlanningStore(db)
	if _, err := store.CreateSession(ctx, CreatePlanningSessionRequest{
		PlanningSessionID:  "ps_acp_run_id",
		Title:              "ACP run ID",
		TitleState:         "custom",
		LifecycleState:     PlanningSessionLifecycleStateActive,
		ConversationState:  PlanningSessionConversationStateReady,
		ProjectRoot:        "/workspace/project",
		ModelSelectionJSON: `{"model":"github-copilot/gpt-5.4"}`,
		CreatedAt:          "2026-04-21T16:40:00Z",
		UpdatedAt:          "2026-04-21T16:40:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	bindTestPlanningContext(t, db, "ps_acp_run_id")

	const operationID = "po_acp_run_id"
	operation, err := store.CreateOperation(ctx, CreatePlanningOperationRequest{
		OperationID:        operationID,
		PlanningSessionID:  "ps_acp_run_id",
		Type:               PlanningOperationTypeReply,
		Status:             PlanningOperationStatusQueued,
		RequestPayloadJSON: `{}`,
		CreatedAt:          "2026-04-21T16:40:01Z",
	})
	if err != nil {
		t.Fatalf("create operation: %v", err)
	}
	want := acp.PlannerRunID(operationID)
	if operation.AcpRunID != want {
		t.Fatalf("expected canonical ACP run ID %q, got %q", want, operation.AcpRunID)
	}
	var storedRunID string
	if err := db.QueryRowContext(ctx, `SELECT acp_run_id FROM planning_operations WHERE operation_id = ?`, operationID).Scan(&storedRunID); err != nil {
		t.Fatalf("read stored ACP run ID: %v", err)
	}
	if storedRunID != want {
		t.Fatalf("expected stored canonical ACP run ID %q, got %q", want, storedRunID)
	}

	if _, err := db.ExecContext(ctx, `UPDATE planning_operations SET acp_run_id = operation_id WHERE operation_id = ?`, operationID); err != nil {
		t.Fatalf("persist operation-id ACP run ID corruption: %v", err)
	}
	corrupted, ok, err := NewPlanningStore(db).GetOperation(ctx, operationID)
	if err != nil || !ok {
		t.Fatalf("read corrupted operation: ok=%v err=%v", ok, err)
	}
	if corrupted.AcpRunID != operationID {
		t.Fatalf("expected stored ACP run ID %q unchanged, got %q", operationID, corrupted.AcpRunID)
	}

	const noncanonicalRunID = "noncanonical-planner-run"
	if _, err := db.ExecContext(ctx, `UPDATE planning_operations SET acp_run_id = ? WHERE operation_id = ?`, noncanonicalRunID, operationID); err != nil {
		t.Fatalf("persist noncanonical ACP run ID: %v", err)
	}
	if _, found, err := store.GetOperationByAcpRunID(ctx, operationID); err != nil {
		t.Fatalf("look up operation ID as ACP run ID: %v", err)
	} else if found {
		t.Fatal("expected operation ID not to be accepted as an ACP run ID")
	}
	corrupted, found, err := store.GetOperationByAcpRunID(ctx, noncanonicalRunID)
	if err != nil || !found {
		t.Fatalf("look up stored noncanonical ACP run ID: found=%v err=%v", found, err)
	}
	if corrupted.AcpRunID != noncanonicalRunID {
		t.Fatalf("expected stored ACP run ID %q unchanged, got %q", noncanonicalRunID, corrupted.AcpRunID)
	}
}

func TestPlanningStoreOperationPagingUsesLimitAndCursor(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	store := NewPlanningStore(db)
	ctx := context.Background()

	// A session admits at most one active interactive operation and one
	// active title operation, so paging across three queued rows means three
	// sessions. Each is created through the bundle, which is the only way a
	// session gets the context snapshot every operation binds at enqueue.
	for i := 1; i <= 3; i++ {
		at := fmt.Sprintf("2026-04-27T09:00:0%dZ", i)
		sessionID := fmt.Sprintf("ps_paging_%d", i)
		if _, err := store.CreateSessionBundle(ctx, CreatePlanningSessionBundleRequest{
			Session: CreatePlanningSessionRequest{
				PlanningSessionID:   sessionID,
				Title:               "Paging",
				TitleState:          "custom",
				LifecycleState:      PlanningSessionLifecycleStateActive,
				ConversationState:   PlanningSessionConversationStateReady,
				ActiveLeafMessageID: fmt.Sprintf("pm_paging_%d", i),
				ProjectRoot:         "/workspace/project",
				ModelSelectionJSON:  `{"model":"github-copilot/gpt-5.4"}`,
				CreatedAt:           at,
				UpdatedAt:           at,
			},
			OpeningMessage: AppendPlanningMessageRequest{
				PlanningMessageID:   fmt.Sprintf("pm_paging_%d", i),
				PlanningSessionID:   sessionID,
				Role:                PlanningMessageRoleAssistant,
				ContentMarkdown:     "What outcome should this plan achieve?",
				QuestionPayloadJSON: `{}`,
				AnswerPayloadJSON:   `{}`,
				CreatedAt:           at,
			},
			ContextSnapshot: CreatePlanningContextSnapshotRequest{
				PlanningContextSnapshotID: fmt.Sprintf("pcs_paging_%d", i),
				PlanningSessionID:         sessionID,
				SummaryMarkdown:           "- Included files: 0",
				IncludedFilesJSON:         "[]",
				ExcludedFilesJSON:         "[]",
				CreatedAt:                 at,
			},
		}); err != nil {
			t.Fatalf("create session %d: %v", i, err)
		}
		if _, err := store.CreateOperation(ctx, CreatePlanningOperationRequest{
			OperationID:        fmt.Sprintf("po_page_%d", i),
			PlanningSessionID:  sessionID,
			Type:               PlanningOperationTypeGenerateTitle,
			Status:             PlanningOperationStatusQueued,
			RequestPayloadJSON: `{}`,
			LeaseOwner:         "",
			LeaseVersion:       0,
			CreatedAt:          at,
		}); err != nil {
			t.Fatalf("create operation %d: %v", i, err)
		}
	}

	firstPage, err := store.ListOperationsByStatusPage(ctx, "", "", 2, PlanningOperationStatusQueued)
	if err != nil {
		t.Fatalf("list first operation page: %v", err)
	}
	if got := len(firstPage); got != 2 {
		t.Fatalf("expected 2 operations in first page, got %d", got)
	}
	if firstPage[0].OperationID != "po_page_1" || firstPage[1].OperationID != "po_page_2" {
		t.Fatalf("unexpected first page ordering: %+v", firstPage)
	}

	secondPage, err := store.ListOperationsByStatusPage(ctx, firstPage[len(firstPage)-1].CreatedAt, firstPage[len(firstPage)-1].OperationID, 2, PlanningOperationStatusQueued)
	if err != nil {
		t.Fatalf("list second operation page: %v", err)
	}
	if got := len(secondPage); got != 1 {
		t.Fatalf("expected 1 operation in second page, got %d", got)
	}
	if secondPage[0].OperationID != "po_page_3" {
		t.Fatalf("expected cursor to resume at po_page_3, got %+v", secondPage)
	}
}

func TestPlanningStoreOperationEventPagingUsesLimitAndCursor(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	store := NewPlanningStore(db)
	ctx := context.Background()

	if _, err := store.CreateSession(ctx, CreatePlanningSessionRequest{
		PlanningSessionID:  "ps_event_paging",
		Title:              "Event paging",
		TitleState:         "custom",
		LifecycleState:     PlanningSessionLifecycleStateActive,
		ConversationState:  PlanningSessionConversationStateReady,
		ProjectRoot:        "/workspace/project",
		ModelSelectionJSON: `{"model":"github-copilot/gpt-5.4"}`,
		CreatedAt:          "2026-04-27T09:30:00Z",
		UpdatedAt:          "2026-04-27T09:30:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	bindTestPlanningContext(t, db, "ps_event_paging")
	if _, err := store.CreateOperation(ctx, CreatePlanningOperationRequest{
		OperationID:        "po_event_paging",
		PlanningSessionID:  "ps_event_paging",
		Type:               PlanningOperationTypeReply,
		Status:             PlanningOperationStatusRunning,
		RequestPayloadJSON: `{}`,
		LeaseOwner:         "planner",
		LeaseExpiresAt:     "2026-04-27T09:35:00Z",
		LeaseVersion:       1,
		CreatedAt:          "2026-04-27T09:30:01Z",
		StartedAt:          "2026-04-27T09:30:02Z",
	}); err != nil {
		t.Fatalf("create operation: %v", err)
	}

	for i := 1; i <= 3; i++ {
		if _, err := store.AppendOperationEvent(ctx, AppendPlanningOperationEventRequest{
			EventID:     fmt.Sprintf("po_event_paging:%d", i),
			OperationID: "po_event_paging",
			Seq:         i,
			EventType:   "text_delta",
			PayloadJSON: `{}`,
			CreatedAt:   fmt.Sprintf("2026-04-27T09:30:0%dZ", i+2),
		}); err != nil {
			t.Fatalf("append event %d: %v", i, err)
		}
	}

	firstPage, err := store.ListOperationEventsAfterSeqPage(ctx, "po_event_paging", 0, 2)
	if err != nil {
		t.Fatalf("list first event page: %v", err)
	}
	if got := len(firstPage); got != 2 {
		t.Fatalf("expected 2 events in first page, got %d", got)
	}
	if firstPage[0].EventID != "po_event_paging:1" || firstPage[1].EventID != "po_event_paging:2" {
		t.Fatalf("unexpected first event page ordering: %+v", firstPage)
	}

	secondPage, err := store.ListOperationEventsAfterEventIDPage(ctx, "po_event_paging", firstPage[len(firstPage)-1].EventID, 2)
	if err != nil {
		t.Fatalf("list second event page: %v", err)
	}
	if got := len(secondPage); got != 1 {
		t.Fatalf("expected 1 event in second page, got %d", got)
	}
	if secondPage[0].EventID != "po_event_paging:3" {
		t.Fatalf("expected cursor to resume at event 3, got %+v", secondPage)
	}
}

func TestPlanningStoreUpdateSessionOptionalFields(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	store := NewPlanningStore(db)
	_, err := store.CreateSession(context.Background(), CreatePlanningSessionRequest{
		PlanningSessionID:   "ps_update_optional",
		Title:               "Original title",
		TitleState:          "custom",
		LifecycleState:      PlanningSessionLifecycleStateActive,
		ConversationState:   PlanningSessionConversationStateReady,
		ActiveLeafMessageID: "pm_original",
		CurrentOperationID:  "po_original",
		CurrentDraftID:      "pd_original",
		ContextSnapshotID:   "pcs_original",
		ProjectRoot:         "/workspace/project",
		ModelSelectionJSON:  `{"model":"github-copilot/gpt-5.4"}`,
		CreatedAt:           "2026-04-21T16:00:00Z",
		UpdatedAt:           "2026-04-21T16:00:00Z",
	})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}

	if err := store.UpdateSession(context.Background(), UpdatePlanningSessionRequest{
		PlanningSessionID: "ps_update_optional",
		UpdatedAt:         "2026-04-21T16:01:00Z",
	}); err != nil {
		t.Fatalf("update session without optional changes: %v", err)
	}

	session, ok, err := store.GetSession(context.Background(), "ps_update_optional")
	if err != nil {
		t.Fatalf("get unchanged session: %v", err)
	}
	if !ok {
		t.Fatal("expected unchanged session to exist")
	}
	if session.ActiveLeafMessageID == nil || *session.ActiveLeafMessageID != "pm_original" {
		t.Fatalf("expected current_question_message_id to stay set, got %+v", session.ActiveLeafMessageID)
	}
	if session.CurrentOperationID == nil || *session.CurrentOperationID != "po_original" {
		t.Fatalf("expected current_operation_id to stay set, got %+v", session.CurrentOperationID)
	}
	if session.CurrentDraftID == nil || *session.CurrentDraftID != "pd_original" {
		t.Fatalf("expected current_draft_id to stay set, got %+v", session.CurrentDraftID)
	}
	if session.ContextSnapshotID == nil || *session.ContextSnapshotID != "pcs_original" {
		t.Fatalf("expected context_snapshot_id to stay set, got %+v", session.ContextSnapshotID)
	}

	if err := store.UpdateSession(context.Background(), UpdatePlanningSessionRequest{
		PlanningSessionID:   "ps_update_optional",
		ActiveLeafMessageID: refString(""),
		CurrentOperationID:  refString(""),
		CurrentDraftID:      refString(""),
		ContextSnapshotID:   refString(""),
		UpdatedAt:           "2026-04-21T16:02:00Z",
	}); err != nil {
		t.Fatalf("clear session optional fields: %v", err)
	}

	session, ok, err = store.GetSession(context.Background(), "ps_update_optional")
	if err != nil {
		t.Fatalf("get cleared session: %v", err)
	}
	if !ok {
		t.Fatal("expected cleared session to exist")
	}
	if session.ActiveLeafMessageID != nil || session.CurrentOperationID != nil || session.CurrentDraftID != nil || session.ContextSnapshotID != nil {
		t.Fatalf("expected nullable fields to clear to NULL, got %+v", session)
	}

	if err := store.UpdateSession(context.Background(), UpdatePlanningSessionRequest{
		PlanningSessionID:   "ps_update_optional",
		ActiveLeafMessageID: refString("pm_new"),
		CurrentOperationID:  refString("po_new"),
		CurrentDraftID:      refString("pd_new"),
		ContextSnapshotID:   refString("pcs_new"),
		UpdatedAt:           "2026-04-21T16:03:00Z",
	}); err != nil {
		t.Fatalf("set session optional fields: %v", err)
	}

	session, ok, err = store.GetSession(context.Background(), "ps_update_optional")
	if err != nil {
		t.Fatalf("get updated session: %v", err)
	}
	if !ok {
		t.Fatal("expected updated session to exist")
	}
	if session.ActiveLeafMessageID == nil || *session.ActiveLeafMessageID != "pm_new" {
		t.Fatalf("expected current_question_message_id pm_new, got %+v", session.ActiveLeafMessageID)
	}
	if session.CurrentOperationID == nil || *session.CurrentOperationID != "po_new" {
		t.Fatalf("expected current_operation_id po_new, got %+v", session.CurrentOperationID)
	}
	if session.CurrentDraftID == nil || *session.CurrentDraftID != "pd_new" {
		t.Fatalf("expected current_draft_id pd_new, got %+v", session.CurrentDraftID)
	}
	if session.ContextSnapshotID == nil || *session.ContextSnapshotID != "pcs_new" {
		t.Fatalf("expected context_snapshot_id pcs_new, got %+v", session.ContextSnapshotID)
	}
}

func TestPlanningStoreUpdateOperationOptionalFields(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	store := NewPlanningStore(db)
	_, err := store.CreateSession(context.Background(), CreatePlanningSessionRequest{
		PlanningSessionID:  "ps_operation_optional",
		Title:              "Planner ops optional",
		TitleState:         "custom",
		LifecycleState:     PlanningSessionLifecycleStateActive,
		ConversationState:  PlanningSessionConversationStateReady,
		ProjectRoot:        "/workspace/project",
		ModelSelectionJSON: `{"model":"github-copilot/gpt-5.4"}`,
		CreatedAt:          "2026-04-21T16:30:00Z",
		UpdatedAt:          "2026-04-21T16:30:00Z",
	})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}

	bindTestPlanningContext(t, db, "ps_operation_optional")
	_, err = store.CreateOperation(context.Background(), CreatePlanningOperationRequest{
		OperationID:        "po_optional",
		PlanningSessionID:  "ps_operation_optional",
		Type:               PlanningOperationTypeReply,
		Status:             PlanningOperationStatusRunning,
		RequestPayloadJSON: `{"content_markdown":"hello"}`,
		LeaseOwner:         "worker-a",
		LeaseExpiresAt:     "2026-04-21T16:31:00Z",
		LeaseVersion:       1,
		LastHeartbeatAt:    "2026-04-21T16:30:30Z",
		Error:              "temporary",
		ResultRef:          "pm_original",
		CreatedAt:          "2026-04-21T16:30:00Z",
		StartedAt:          "2026-04-21T16:30:05Z",
		FinishedAt:         "2026-04-21T16:30:10Z",
	})
	if err != nil {
		t.Fatalf("create operation: %v", err)
	}

	if err := store.UpdateOperation(context.Background(), UpdatePlanningOperationRequest{OperationID: "po_optional"}); err != nil {
		t.Fatalf("update operation without optional changes: %v", err)
	}

	operation, ok, err := store.GetOperation(context.Background(), "po_optional")
	if err != nil {
		t.Fatalf("get unchanged operation: %v", err)
	}
	if !ok {
		t.Fatal("expected unchanged operation to exist")
	}
	if operation.LeaseOwner != "worker-a" || operation.LeaseExpiresAt == nil || operation.LastHeartbeatAt == nil || operation.Error == nil || operation.ResultRef == nil || operation.StartedAt == nil || operation.FinishedAt == nil {
		t.Fatalf("expected unchanged optional fields to remain set, got %+v", operation)
	}

	if err := store.UpdateOperation(context.Background(), UpdatePlanningOperationRequest{
		OperationID:     "po_optional",
		LeaseOwner:      refString(""),
		LeaseExpiresAt:  refString(""),
		LastHeartbeatAt: refString(""),
		Error:           refString(""),
		ResultRef:       refString(""),
		StartedAt:       refString(""),
		FinishedAt:      refString(""),
	}); err != nil {
		t.Fatalf("clear operation optional fields: %v", err)
	}

	operation, ok, err = store.GetOperation(context.Background(), "po_optional")
	if err != nil {
		t.Fatalf("get cleared operation: %v", err)
	}
	if !ok {
		t.Fatal("expected cleared operation to exist")
	}
	if operation.LeaseOwner != "" {
		t.Fatalf("expected lease_owner to clear to empty string, got %q", operation.LeaseOwner)
	}
	if operation.LeaseExpiresAt != nil || operation.LastHeartbeatAt != nil || operation.Error != nil || operation.ResultRef != nil || operation.StartedAt != nil || operation.FinishedAt != nil {
		t.Fatalf("expected nullable operation fields to clear to NULL, got %+v", operation)
	}

	leaseVersion := 2
	statusSucceeded := PlanningOperationStatusSucceeded
	if err := store.UpdateOperation(context.Background(), UpdatePlanningOperationRequest{
		OperationID:     "po_optional",
		Status:          &statusSucceeded,
		LeaseOwner:      refString("worker-b"),
		LeaseExpiresAt:  refString("2026-04-21T16:32:00Z"),
		LeaseVersion:    &leaseVersion,
		LastHeartbeatAt: refString("2026-04-21T16:31:30Z"),
		Error:           refString("resolved"),
		ResultRef:       refString("pm_updated"),
		StartedAt:       refString("2026-04-21T16:31:05Z"),
		FinishedAt:      refString("2026-04-21T16:31:10Z"),
	}); err != nil {
		t.Fatalf("set operation optional fields: %v", err)
	}

	operation, ok, err = store.GetOperation(context.Background(), "po_optional")
	if err != nil {
		t.Fatalf("get updated operation: %v", err)
	}
	if !ok {
		t.Fatal("expected updated operation to exist")
	}
	if operation.Status != PlanningOperationStatusSucceeded || operation.LeaseOwner != "worker-b" || operation.LeaseVersion != 2 {
		t.Fatalf("expected status/lease fields to update, got %+v", operation)
	}
	if operation.LeaseExpiresAt == nil || *operation.LeaseExpiresAt != "2026-04-21T16:32:00Z" {
		t.Fatalf("expected lease_expires_at to update, got %+v", operation.LeaseExpiresAt)
	}
	if operation.LastHeartbeatAt == nil || *operation.LastHeartbeatAt != "2026-04-21T16:31:30Z" {
		t.Fatalf("expected last_heartbeat_at to update, got %+v", operation.LastHeartbeatAt)
	}
	if operation.Error == nil || *operation.Error != "resolved" {
		t.Fatalf("expected error to update, got %+v", operation.Error)
	}
	if operation.ResultRef == nil || *operation.ResultRef != "pm_updated" {
		t.Fatalf("expected result_ref to update, got %+v", operation.ResultRef)
	}
	if operation.StartedAt == nil || *operation.StartedAt != "2026-04-21T16:31:05Z" {
		t.Fatalf("expected started_at to update, got %+v", operation.StartedAt)
	}
	if operation.FinishedAt == nil || *operation.FinishedAt != "2026-04-21T16:31:10Z" {
		t.Fatalf("expected finished_at to update, got %+v", operation.FinishedAt)
	}
}

func TestPlanningStoreClaimOperationPreventsDualExecution(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")

	store := NewPlanningStore(db)
	_, err := store.CreateSession(context.Background(), CreatePlanningSessionRequest{
		PlanningSessionID:  "ps_claim",
		Title:              "Planner claim",
		TitleState:         "custom",
		LifecycleState:     PlanningSessionLifecycleStateActive,
		ConversationState:  PlanningSessionConversationStateReady,
		ProjectRoot:        "/workspace/project",
		ModelSelectionJSON: `{"model":"github-copilot/gpt-5.4"}`,
		CreatedAt:          "2026-04-21T16:40:00Z",
		UpdatedAt:          "2026-04-21T16:40:00Z",
	})
	if err != nil {
		t.Fatalf("create session: %v", err)
	}

	bindTestPlanningContext(t, db, "ps_claim")
	_, err = store.CreateOperation(context.Background(), CreatePlanningOperationRequest{
		OperationID:        "po_claim_1",
		PlanningSessionID:  "ps_claim",
		Type:               PlanningOperationTypeReply,
		Status:             PlanningOperationStatusQueued,
		RequestPayloadJSON: `{"reply_markdown":"hello"}`,
		LeaseOwner:         "",
		LeaseVersion:       0,
		CreatedAt:          "2026-04-21T16:40:01Z",
	})
	if err != nil {
		t.Fatalf("create operation: %v", err)
	}

	claimed, ok, err := store.ClaimOperation(context.Background(), "po_claim_1", "worker-a", "2026-04-21T16:41:00Z", "2026-04-21T16:40:10Z")
	if err != nil {
		t.Fatalf("claim operation first time: %v", err)
	}
	if !ok {
		t.Fatal("expected first claim to succeed")
	}
	if claimed.Status != PlanningOperationStatusRunning || claimed.LeaseOwner != "worker-a" || claimed.LeaseVersion != 1 {
		t.Fatalf("unexpected claimed operation: %+v", claimed)
	}

	_, ok, err = store.ClaimOperation(context.Background(), "po_claim_1", "worker-b", "2026-04-21T16:41:00Z", "2026-04-21T16:40:11Z")
	if err != nil {
		t.Fatalf("claim operation second time: %v", err)
	}
	if ok {
		t.Fatal("expected second claim to be rejected while lease is active")
	}

	if err := store.RenewOperationLease(context.Background(), "po_claim_1", "worker-a", 1, "2026-04-21T16:41:30Z", "2026-04-21T16:40:20Z"); err != nil {
		t.Fatalf("renew operation lease: %v", err)
	}

	if err := store.RenewOperationLease(context.Background(), "po_claim_1", "worker-b", 1, "2026-04-21T16:41:30Z", "2026-04-21T16:40:21Z"); err == nil {
		t.Fatal("expected renew with wrong owner to fail")
	}
}

func refString(value string) *string {
	return &value
}

// TestClaimOperationRejectsExpiredRunningRow pins the queued-only claim
// contract: a running row whose lease has expired is still not reclaimable.
// Recovery owns abandoned running rows after the grace window; claiming them
// early would race the original worker and duplicate planner side effects.
func TestClaimOperationRejectsExpiredRunningRow(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewPlanningStore(db)
	ctx := context.Background()

	if _, err := store.CreateSession(ctx, CreatePlanningSessionRequest{
		PlanningSessionID:  "ps_claim_expired",
		Title:              "claim-expired",
		TitleState:         "custom",
		LifecycleState:     PlanningSessionLifecycleStateActive,
		ConversationState:  PlanningSessionConversationStateProcessing,
		ProjectRoot:        "/workspace/project",
		ModelSelectionJSON: `{"model":"m"}`,
		CreatedAt:          "2026-04-26T10:00:00Z",
		UpdatedAt:          "2026-04-26T10:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	// Seed a running row with an already-expired lease.
	bindTestPlanningContext(t, db, "ps_claim_expired")
	if _, err := store.CreateOperation(ctx, CreatePlanningOperationRequest{
		OperationID:        "po_running_expired",
		PlanningSessionID:  "ps_claim_expired",
		Type:               PlanningOperationTypeReply,
		Status:             PlanningOperationStatusRunning,
		RequestPayloadJSON: `{}`,
		LeaseOwner:         "abandoned-worker",
		LeaseExpiresAt:     "2026-04-26T09:00:00Z", // 1h before our heartbeat clock
		LeaseVersion:       1,
		CreatedAt:          "2026-04-26T08:55:00Z",
		StartedAt:          "2026-04-26T08:55:01Z",
	}); err != nil {
		t.Fatalf("seed running op: %v", err)
	}

	_, ok, err := store.ClaimOperation(ctx, "po_running_expired", "new-worker", "2026-04-26T11:00:00Z", "2026-04-26T10:00:00Z")
	if err != nil {
		t.Fatalf("claim attempt: %v", err)
	}
	if ok {
		t.Fatal("expected expired-running row claim to be rejected (queued-only contract)")
	}

	// Row state must be unchanged — no partial mutation from the
	// rejected claim attempt.
	op, found, err := store.GetOperation(ctx, "po_running_expired")
	if err != nil || !found {
		t.Fatalf("get op: ok=%v err=%v", found, err)
	}
	if op.LeaseOwner != "abandoned-worker" || op.LeaseVersion != 1 {
		t.Fatalf("expected row lease unchanged, got owner=%q version=%d", op.LeaseOwner, op.LeaseVersion)
	}
}

// TestFinalizeOperationRejectsStaleLeaseOwner verifies the dual-row fence:
// a worker that loses its lease cannot finalize successfully just because it
// still holds the operation id.
func TestFinalizeOperationRejectsStaleLeaseOwner(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewPlanningStore(db)
	ctx := context.Background()

	if _, err := store.CreateSession(ctx, CreatePlanningSessionRequest{
		PlanningSessionID:  "ps_fence",
		Title:              "fence",
		TitleState:         "custom",
		LifecycleState:     PlanningSessionLifecycleStateActive,
		ConversationState:  PlanningSessionConversationStateProcessing,
		CurrentOperationID: "po_fence",
		ProjectRoot:        "/workspace/project",
		ModelSelectionJSON: `{"model":"m"}`,
		CreatedAt:          "2026-04-25T16:40:00Z",
		UpdatedAt:          "2026-04-25T16:40:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	bindTestPlanningContext(t, db, "ps_fence")
	if _, err := store.CreateOperation(ctx, CreatePlanningOperationRequest{
		OperationID:        "po_fence",
		PlanningSessionID:  "ps_fence",
		Type:               PlanningOperationTypeReply,
		Status:             PlanningOperationStatusQueued,
		RequestPayloadJSON: `{}`,
		CreatedAt:          "2026-04-25T16:40:01Z",
	}); err != nil {
		t.Fatalf("create operation: %v", err)
	}
	claimed, ok, err := store.ClaimOperation(ctx, "po_fence", "worker-a", "2026-04-25T17:40:00Z", "2026-04-25T16:40:10Z")
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}

	// Recovery sees the lease as expired and re-claims with worker-b.
	// Easiest way to simulate is a direct UPDATE that bumps lease_version,
	// matching what ClaimOperation would do.
	if _, err := db.ExecContext(ctx,
		`UPDATE planning_operations SET lease_owner = 'worker-b', lease_version = lease_version + 1 WHERE operation_id = 'po_fence'`); err != nil {
		t.Fatalf("simulate recovery reclaim: %v", err)
	}

	// Worker-a comes back online and tries to finalize using its stale
	// LeaseOwner / LeaseVersion. The fence must reject this cleanly.
	err = store.FinalizeOperation(ctx, FinalizeOperationRequest{
		Kind:              FinalizeReply,
		SessionID:         "ps_fence",
		OperationID:       "po_fence",
		LeaseOwner:        claimed.LeaseOwner,
		LeaseVersion:      claimed.LeaseVersion,
		ConversationState: PlanningSessionConversationStateReady,
		UpdatedAt:         "2026-04-25T16:41:00Z",
		FinishedAt:        "2026-04-25T16:41:00Z",
		ResultRef:         "pm_done",
		AssistantMessage: &AppendPlanningMessageRequest{
			PlanningMessageID:   "pm_done",
			PlanningSessionID:   "ps_fence",
			Role:                PlanningMessageRoleAssistant,
			ContentMarkdown:     "stale",
			QuestionPayloadJSON: `{}`,
			AnswerPayloadJSON:   `{}`,
			CreatedAt:           "2026-04-25T16:41:00Z",
		},
	})
	if !errors.Is(err, ErrPlanningLeaseConflict) {
		t.Fatalf("expected ErrPlanningLeaseConflict, got %v", err)
	}

	op, found, err := store.GetOperation(ctx, "po_fence")
	if err != nil || !found {
		t.Fatalf("get operation: ok=%v err=%v", found, err)
	}
	if op.Status != PlanningOperationStatusRunning {
		t.Fatalf("operation status should remain running after rejected finalize, got %q", op.Status)
	}
}

func TestFinalizeOperationReplyStaleFinalizerReturnsLeaseConflictBeforeMessageInsert(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewPlanningStore(db)
	ctx := context.Background()

	if _, err := store.CreateSession(ctx, CreatePlanningSessionRequest{
		PlanningSessionID:  "ps_reply_race",
		Title:              "reply-race",
		TitleState:         "custom",
		LifecycleState:     PlanningSessionLifecycleStateActive,
		ConversationState:  PlanningSessionConversationStateProcessing,
		CurrentOperationID: "po_reply_race",
		ProjectRoot:        "/workspace/project",
		ModelSelectionJSON: `{"model":"m"}`,
		CreatedAt:          "2026-04-27T10:00:00Z",
		UpdatedAt:          "2026-04-27T10:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	bindTestPlanningContext(t, db, "ps_reply_race")
	if _, err := store.CreateOperation(ctx, CreatePlanningOperationRequest{
		OperationID:        "po_reply_race",
		PlanningSessionID:  "ps_reply_race",
		Type:               PlanningOperationTypeReply,
		Status:             PlanningOperationStatusQueued,
		RequestPayloadJSON: `{}`,
		CreatedAt:          "2026-04-27T10:00:01Z",
	}); err != nil {
		t.Fatalf("create operation: %v", err)
	}
	claimed, ok, err := store.ClaimOperation(ctx, "po_reply_race", "worker-a", "2026-04-27T11:00:00Z", "2026-04-27T10:00:10Z")
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}

	finalizeReq := FinalizeOperationRequest{
		Kind:              FinalizeReply,
		SessionID:         "ps_reply_race",
		OperationID:       "po_reply_race",
		LeaseOwner:        claimed.LeaseOwner,
		LeaseVersion:      claimed.LeaseVersion,
		ConversationState: PlanningSessionConversationStateReady,
		UpdatedAt:         "2026-04-27T10:01:00Z",
		FinishedAt:        "2026-04-27T10:01:00Z",
		ResultRef:         "pm_reply_done",
		AssistantMessage: &AppendPlanningMessageRequest{
			PlanningMessageID:   "pm_reply_done",
			PlanningSessionID:   "ps_reply_race",
			Role:                PlanningMessageRoleAssistant,
			ContentMarkdown:     "Canonical reply",
			QuestionPayloadJSON: `{}`,
			AnswerPayloadJSON:   `{}`,
			CreatedAt:           "2026-04-27T10:01:00Z",
		},
		ActiveLeafMessageID: "pm_reply_done",
	}
	if err := store.FinalizeOperation(ctx, finalizeReq); err != nil {
		t.Fatalf("winner finalize: %v", err)
	}

	err = store.FinalizeOperation(ctx, finalizeReq)
	if !errors.Is(err, ErrPlanningLeaseConflict) {
		t.Fatalf("expected ErrPlanningLeaseConflict, got %v", err)
	}
	if err != nil && (IsUniqueViolation(err) || strings.Contains(err.Error(), "UNIQUE")) {
		t.Fatalf("stale reply finalizer surfaced unique constraint instead of lease conflict: %v", err)
	}

	messages, err := store.ListMessages(ctx, "ps_reply_race")
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 1 || messages[0].PlanningMessageID != "pm_reply_done" {
		t.Fatalf("expected exactly one canonical assistant message, got %+v", messages)
	}

	session, found, err := store.GetSession(ctx, "ps_reply_race")
	if err != nil || !found {
		t.Fatalf("get session: ok=%v err=%v", found, err)
	}
	if session.CurrentOperationID != nil {
		t.Fatalf("expected current_operation_id cleared, got %+v", session.CurrentOperationID)
	}
	if session.ActiveLeafMessageID == nil || *session.ActiveLeafMessageID != "pm_reply_done" {
		t.Fatalf("expected current question to point at canonical reply, got %+v", session.ActiveLeafMessageID)
	}

	op, found, err := store.GetOperation(ctx, "po_reply_race")
	if err != nil || !found {
		t.Fatalf("get operation: ok=%v err=%v", found, err)
	}
	if op.Status != PlanningOperationStatusSucceeded {
		t.Fatalf("expected reply operation succeeded, got %q", op.Status)
	}
}

func TestFinalizeOperationRegenerateStaleFinalizerReturnsLeaseConflictBeforeTranscriptMutation(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewPlanningStore(db)
	ctx := context.Background()

	if _, err := store.CreateSession(ctx, CreatePlanningSessionRequest{
		PlanningSessionID:   "ps_regen_race",
		Title:               "regen-race",
		TitleState:          "custom",
		LifecycleState:      PlanningSessionLifecycleStateActive,
		ConversationState:   PlanningSessionConversationStateProcessing,
		ActiveLeafMessageID: "pm_regen_old",
		CurrentOperationID:  "po_regen_race",
		ProjectRoot:         "/workspace/project",
		ModelSelectionJSON:  `{"model":"m"}`,
		CreatedAt:           "2026-04-27T11:00:00Z",
		UpdatedAt:           "2026-04-27T11:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := store.AppendMessage(ctx, AppendPlanningMessageRequest{
		PlanningMessageID:   "pm_regen_old",
		PlanningSessionID:   "ps_regen_race",
		Role:                PlanningMessageRoleAssistant,
		ContentMarkdown:     "Old answer",
		QuestionPayloadJSON: `{}`,
		AnswerPayloadJSON:   `{}`,
		CreatedAt:           "2026-04-27T11:00:01Z",
	}); err != nil {
		t.Fatalf("append old assistant message: %v", err)
	}
	if _, err := store.AppendMessage(ctx, AppendPlanningMessageRequest{
		PlanningMessageID:   "pm_regen_future_user",
		PlanningSessionID:   "ps_regen_race",
		Role:                PlanningMessageRoleUser,
		ContentMarkdown:     "Future turn",
		QuestionPayloadJSON: `{}`,
		AnswerPayloadJSON:   `{}`,
		CreatedAt:           "2026-04-27T11:00:02Z",
	}); err != nil {
		t.Fatalf("append future user message: %v", err)
	}
	bindTestPlanningContext(t, db, "ps_regen_race")
	if _, err := store.CreateOperation(ctx, CreatePlanningOperationRequest{
		OperationID:        "po_regen_race",
		PlanningSessionID:  "ps_regen_race",
		Type:               PlanningOperationTypeRegenerate,
		Status:             PlanningOperationStatusQueued,
		RequestPayloadJSON: `{}`,
		CreatedAt:          "2026-04-27T11:00:03Z",
	}); err != nil {
		t.Fatalf("create operation: %v", err)
	}
	claimed, ok, err := store.ClaimOperation(ctx, "po_regen_race", "worker-a", "2026-04-27T12:00:00Z", "2026-04-27T11:00:10Z")
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}

	finalizeReq := FinalizeOperationRequest{
		Kind:                FinalizeRegenerate,
		SessionID:           "ps_regen_race",
		OperationID:         "po_regen_race",
		LeaseOwner:          claimed.LeaseOwner,
		LeaseVersion:        claimed.LeaseVersion,
		ConversationState:   PlanningSessionConversationStateReady,
		UpdatedAt:           "2026-04-27T11:01:00Z",
		FinishedAt:          "2026-04-27T11:01:00Z",
		ActiveLeafMessageID: "pm_regen_new",
		ResultRef:           "pm_regen_new",
		AssistantMessage: &AppendPlanningMessageRequest{
			PlanningMessageID:   "pm_regen_new",
			PlanningSessionID:   "ps_regen_race",
			Role:                PlanningMessageRoleAssistant,
			ContentMarkdown:     "New answer",
			QuestionPayloadJSON: `{}`,
			AnswerPayloadJSON:   `{}`,
			CreatedAt:           "2026-04-27T11:01:00Z",
		},
	}
	if err := store.FinalizeOperation(ctx, finalizeReq); err != nil {
		t.Fatalf("winner finalize: %v", err)
	}

	err = store.FinalizeOperation(ctx, finalizeReq)
	if !errors.Is(err, ErrPlanningLeaseConflict) {
		t.Fatalf("expected ErrPlanningLeaseConflict, got %v", err)
	}
	if err != nil && (IsUniqueViolation(err) || strings.Contains(err.Error(), "UNIQUE")) {
		t.Fatalf("stale regenerate finalizer surfaced unique constraint instead of lease conflict: %v", err)
	}

	messages, err := store.ListMessages(ctx, "ps_regen_race")
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 1 || messages[0].PlanningMessageID != "pm_regen_new" {
		t.Fatalf("expected only the regenerated assistant message to remain active, got %+v", messages)
	}

	session, found, err := store.GetSession(ctx, "ps_regen_race")
	if err != nil || !found {
		t.Fatalf("get session: ok=%v err=%v", found, err)
	}
	if session.CurrentOperationID != nil {
		t.Fatalf("expected current_operation_id cleared, got %+v", session.CurrentOperationID)
	}
	if session.ActiveLeafMessageID == nil || *session.ActiveLeafMessageID != "pm_regen_new" {
		t.Fatalf("expected regenerated assistant to remain canonical, got %+v", session.ActiveLeafMessageID)
	}
}

func TestFinalizeOperationRegenerateCancelRestoresPreviousActiveLeaf(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewPlanningStore(db)
	ctx := context.Background()

	if _, err := store.CreateSession(ctx, CreatePlanningSessionRequest{
		PlanningSessionID:   "ps_regen_cancel_restore",
		Title:               "regen-cancel-restore",
		TitleState:          "custom",
		LifecycleState:      PlanningSessionLifecycleStateActive,
		ConversationState:   PlanningSessionConversationStateProcessing,
		ActiveLeafMessageID: "pm_followup",
		CurrentOperationID:  "po_regen_cancel_restore",
		ProjectRoot:         "/workspace/project",
		ModelSelectionJSON:  `{"model":"m"}`,
		CreatedAt:           "2026-04-27T12:00:00Z",
		UpdatedAt:           "2026-04-27T12:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := store.AppendMessage(ctx, AppendPlanningMessageRequest{
		PlanningMessageID:   "pm_target",
		PlanningSessionID:   "ps_regen_cancel_restore",
		Role:                PlanningMessageRoleAssistant,
		ContentMarkdown:     "Original answer",
		QuestionPayloadJSON: `{}`,
		AnswerPayloadJSON:   `{}`,
		CreatedAt:           "2026-04-27T12:00:01Z",
	}); err != nil {
		t.Fatalf("append target assistant: %v", err)
	}
	if _, err := store.AppendMessage(ctx, AppendPlanningMessageRequest{
		PlanningMessageID:   "pm_followup",
		PlanningSessionID:   "ps_regen_cancel_restore",
		ParentMessageID:     "pm_target",
		Role:                PlanningMessageRoleUser,
		ContentMarkdown:     "Follow-up question",
		QuestionPayloadJSON: `{}`,
		AnswerPayloadJSON:   `{}`,
		CreatedAt:           "2026-04-27T12:00:02Z",
	}); err != nil {
		t.Fatalf("append follow-up user: %v", err)
	}
	bindTestPlanningContext(t, db, "ps_regen_cancel_restore")
	if _, err := store.CreateOperation(ctx, CreatePlanningOperationRequest{
		OperationID:        "po_regen_cancel_restore",
		PlanningSessionID:  "ps_regen_cancel_restore",
		Type:               PlanningOperationTypeRegenerate,
		Status:             PlanningOperationStatusQueued,
		RequestPayloadJSON: `{"target_message_id":"pm_target","parent_message_id":"","previous_active_leaf_message_id":"pm_followup"}`,
		CreatedAt:          "2026-04-27T12:00:03Z",
	}); err != nil {
		t.Fatalf("create operation: %v", err)
	}
	claimed, ok, err := store.ClaimOperation(ctx, "po_regen_cancel_restore", "worker-a", "2026-04-27T13:00:00Z", "2026-04-27T12:00:10Z")
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}

	if err := store.FinalizeOperation(ctx, FinalizeOperationRequest{
		Kind:              FinalizeCancel,
		SessionID:         "ps_regen_cancel_restore",
		OperationID:       "po_regen_cancel_restore",
		LeaseOwner:        claimed.LeaseOwner,
		LeaseVersion:      claimed.LeaseVersion,
		ConversationState: PlanningSessionConversationStateReady,
		UpdatedAt:         "2026-04-27T12:01:00Z",
		FinishedAt:        "2026-04-27T12:01:00Z",
		ErrorMessage:      "cancelled during shutdown",
		TerminalEvent: &AppendPlanningOperationEventRequest{
			EventID:     "po_regen_cancel_restore:1",
			OperationID: "po_regen_cancel_restore",
			Seq:         1,
			EventType:   "cancelled",
			PayloadJSON: `{"reason":"cancelled during shutdown"}`,
			CreatedAt:   "2026-04-27T12:01:00Z",
		},
	}); err != nil {
		t.Fatalf("finalize cancel: %v", err)
	}

	session, found, err := store.GetSession(ctx, "ps_regen_cancel_restore")
	if err != nil || !found {
		t.Fatalf("get session: ok=%v err=%v", found, err)
	}
	if session.ActiveLeafMessageID == nil || *session.ActiveLeafMessageID != "pm_followup" {
		t.Fatalf("expected active leaf restored to previous transcript tip, got %+v", session.ActiveLeafMessageID)
	}
	if session.ConversationState != PlanningSessionConversationStateReady {
		t.Fatalf("expected conversation_state=ready, got %q", session.ConversationState)
	}
	if session.CurrentOperationID == nil || *session.CurrentOperationID != "po_regen_cancel_restore" {
		t.Fatalf("expected cancelled regenerate lineage retained, got %+v", session.CurrentOperationID)
	}
	messages, err := store.ListMessages(ctx, "ps_regen_cancel_restore")
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected no new messages on cancelled regenerate, got %+v", messages)
	}
}

func TestFinalizeOperationRegenerateFailRestoresPreviousActiveLeaf(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewPlanningStore(db)
	ctx := context.Background()

	if _, err := store.CreateSession(ctx, CreatePlanningSessionRequest{
		PlanningSessionID:   "ps_regen_fail_restore",
		Title:               "regen-fail-restore",
		TitleState:          "custom",
		LifecycleState:      PlanningSessionLifecycleStateActive,
		ConversationState:   PlanningSessionConversationStateProcessing,
		ActiveLeafMessageID: "pm_followup_fail",
		CurrentOperationID:  "po_regen_fail_restore",
		ProjectRoot:         "/workspace/project",
		ModelSelectionJSON:  `{"model":"m"}`,
		CreatedAt:           "2026-04-27T14:00:00Z",
		UpdatedAt:           "2026-04-27T14:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := store.AppendMessage(ctx, AppendPlanningMessageRequest{
		PlanningMessageID:   "pm_target_fail",
		PlanningSessionID:   "ps_regen_fail_restore",
		Role:                PlanningMessageRoleAssistant,
		ContentMarkdown:     "Original answer",
		QuestionPayloadJSON: `{}`,
		AnswerPayloadJSON:   `{}`,
		CreatedAt:           "2026-04-27T14:00:01Z",
	}); err != nil {
		t.Fatalf("append target assistant: %v", err)
	}
	if _, err := store.AppendMessage(ctx, AppendPlanningMessageRequest{
		PlanningMessageID:   "pm_followup_fail",
		PlanningSessionID:   "ps_regen_fail_restore",
		ParentMessageID:     "pm_target_fail",
		Role:                PlanningMessageRoleUser,
		ContentMarkdown:     "Follow-up question",
		QuestionPayloadJSON: `{}`,
		AnswerPayloadJSON:   `{}`,
		CreatedAt:           "2026-04-27T14:00:02Z",
	}); err != nil {
		t.Fatalf("append follow-up user: %v", err)
	}
	bindTestPlanningContext(t, db, "ps_regen_fail_restore")
	if _, err := store.CreateOperation(ctx, CreatePlanningOperationRequest{
		OperationID:        "po_regen_fail_restore",
		PlanningSessionID:  "ps_regen_fail_restore",
		Type:               PlanningOperationTypeRegenerate,
		Status:             PlanningOperationStatusQueued,
		RequestPayloadJSON: `{"target_message_id":"pm_target_fail","parent_message_id":"","previous_active_leaf_message_id":"pm_followup_fail"}`,
		CreatedAt:          "2026-04-27T14:00:03Z",
	}); err != nil {
		t.Fatalf("create operation: %v", err)
	}
	claimed, ok, err := store.ClaimOperation(ctx, "po_regen_fail_restore", "worker-a", "2026-04-27T15:00:00Z", "2026-04-27T14:00:10Z")
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}

	if err := store.FinalizeOperation(ctx, FinalizeOperationRequest{
		Kind:              FinalizeFail,
		SessionID:         "ps_regen_fail_restore",
		OperationID:       "po_regen_fail_restore",
		LeaseOwner:        claimed.LeaseOwner,
		LeaseVersion:      claimed.LeaseVersion,
		ConversationState: PlanningSessionConversationStateReady,
		UpdatedAt:         "2026-04-27T14:01:00Z",
		FinishedAt:        "2026-04-27T14:01:00Z",
		ErrorMessage:      "planner failed",
		TerminalEvent: &AppendPlanningOperationEventRequest{
			EventID:     "po_regen_fail_restore:1",
			OperationID: "po_regen_fail_restore",
			Seq:         1,
			EventType:   "failed",
			PayloadJSON: `{"reason":"planner failed"}`,
			CreatedAt:   "2026-04-27T14:01:00Z",
		},
	}); err != nil {
		t.Fatalf("finalize fail: %v", err)
	}

	session, found, err := store.GetSession(ctx, "ps_regen_fail_restore")
	if err != nil || !found {
		t.Fatalf("get session: ok=%v err=%v", found, err)
	}
	if session.ActiveLeafMessageID == nil || *session.ActiveLeafMessageID != "pm_followup_fail" {
		t.Fatalf("expected active leaf restored to previous transcript tip, got %+v", session.ActiveLeafMessageID)
	}
	if session.ConversationState != PlanningSessionConversationStateReady {
		t.Fatalf("expected conversation_state=ready, got %q", session.ConversationState)
	}
}

// TestFinalizeOperationCancel pins the FinalizeCancel post-conditions:
// status = cancelled, lease cleared, reason stored in error column,
// session current_operation_id cleared, session conversation_state
// returned to ready, terminal event recorded. Cancel and Fail share
// most of the no-side-effects shape, but the row's status must be
// cancelled (not failed) so failure-rate metrics aren't polluted by
// shutdowns.
func TestFinalizeOperationCancel(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewPlanningStore(db)
	ctx := context.Background()

	if _, err := store.CreateSession(ctx, CreatePlanningSessionRequest{
		PlanningSessionID:  "ps_cancel",
		Title:              "cancel",
		TitleState:         "custom",
		LifecycleState:     PlanningSessionLifecycleStateActive,
		ConversationState:  PlanningSessionConversationStateProcessing,
		CurrentOperationID: "po_cancel",
		ProjectRoot:        "/workspace/project",
		ModelSelectionJSON: `{"model":"m"}`,
		CreatedAt:          "2026-04-26T12:00:00Z",
		UpdatedAt:          "2026-04-26T12:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	bindTestPlanningContext(t, db, "ps_cancel")
	if _, err := store.CreateOperation(ctx, CreatePlanningOperationRequest{
		OperationID:        "po_cancel",
		PlanningSessionID:  "ps_cancel",
		Type:               PlanningOperationTypeReply,
		Status:             PlanningOperationStatusQueued,
		RequestPayloadJSON: `{}`,
		CreatedAt:          "2026-04-26T12:00:01Z",
	}); err != nil {
		t.Fatalf("create operation: %v", err)
	}
	claimed, ok, err := store.ClaimOperation(ctx, "po_cancel", "worker-a", "2026-04-26T13:00:00Z", "2026-04-26T12:00:10Z")
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}

	const reason = "operation cancelled during shutdown"
	if err := store.FinalizeOperation(ctx, FinalizeOperationRequest{
		Kind:              FinalizeCancel,
		SessionID:         "ps_cancel",
		OperationID:       "po_cancel",
		LeaseOwner:        claimed.LeaseOwner,
		LeaseVersion:      claimed.LeaseVersion,
		ConversationState: PlanningSessionConversationStateReady,
		UpdatedAt:         "2026-04-26T12:00:30Z",
		FinishedAt:        "2026-04-26T12:00:30Z",
		ErrorMessage:      reason,
		TerminalEvent: &AppendPlanningOperationEventRequest{
			EventID:     "po_cancel:1",
			OperationID: "po_cancel",
			Seq:         1,
			EventType:   "cancelled",
			PayloadJSON: `{"reason":"operation cancelled during shutdown"}`,
			CreatedAt:   "2026-04-26T12:00:30Z",
		},
	}); err != nil {
		t.Fatalf("finalize cancel: %v", err)
	}

	op, found, err := store.GetOperation(ctx, "po_cancel")
	if err != nil || !found {
		t.Fatalf("get operation: ok=%v err=%v", found, err)
	}
	if op.Status != PlanningOperationStatusCancelled {
		t.Fatalf("expected status=cancelled, got %q", op.Status)
	}
	if op.LeaseOwner != "" {
		t.Fatalf("expected lease_owner cleared, got %q", op.LeaseOwner)
	}
	if op.LeaseVersion != claimed.LeaseVersion+1 {
		t.Fatalf("expected lease_version to bump from %d to %d, got %d", claimed.LeaseVersion, claimed.LeaseVersion+1, op.LeaseVersion)
	}
	if op.Error == nil || *op.Error != reason {
		t.Fatalf("expected error column = %q, got %+v", reason, op.Error)
	}
	if op.ResultRef != nil && *op.ResultRef != "" {
		t.Fatalf("expected result_ref empty for cancelled op, got %q", *op.ResultRef)
	}

	session, found, err := store.GetSession(ctx, "ps_cancel")
	if err != nil || !found {
		t.Fatalf("get session: ok=%v err=%v", found, err)
	}
	if session.ConversationState != PlanningSessionConversationStateReady {
		t.Fatalf("expected conversation_state=ready, got %q", session.ConversationState)
	}
	if session.CurrentOperationID == nil || *session.CurrentOperationID != "po_cancel" {
		t.Fatalf("expected current_operation_id to retain the cancelled lineage, got %+v", session.CurrentOperationID)
	}

	events, err := store.ListOperationEventsAfterEventIDPage(ctx, "po_cancel", "", DefaultPlanningOperationEventListLimit)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	if len(events) != 1 || events[0].EventType != "cancelled" {
		t.Fatalf("expected one cancelled terminal event, got %+v", events)
	}
}

// TestFinalizeOperationCancelRequiresReason locks in the validation
// gate: a FinalizeCancel without an ErrorMessage is meaningless to the
// UI and to operators reading metrics, and we'd rather catch missing
// reasons in tests than ship empty cancellations.
func TestFinalizeOperationCancelRequiresReason(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewPlanningStore(db)
	ctx := context.Background()

	err := store.FinalizeOperation(ctx, FinalizeOperationRequest{
		Kind:              FinalizeCancel,
		SessionID:         "ps_x",
		OperationID:       "po_x",
		LeaseOwner:        "worker",
		LeaseVersion:      1,
		ConversationState: PlanningSessionConversationStateReady,
		UpdatedAt:         "2026-04-26T12:00:00Z",
		FinishedAt:        "2026-04-26T12:00:00Z",
	})
	if err == nil {
		t.Fatal("expected validation error for FinalizeCancel without ErrorMessage")
	}
	if !strings.Contains(err.Error(), "cancel") {
		t.Fatalf("expected cancel-specific validation error, got %v", err)
	}
}

func TestFinalizeGenerateTitleWhileForegroundOpHoldsInteractiveSlot(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewPlanningStore(db)
	ctx := context.Background()

	if _, err := store.CreateSession(ctx, CreatePlanningSessionRequest{
		PlanningSessionID:       "ps_title_fg",
		Title:                   "plan_title_fg",
		TitleState:              "placeholder",
		LifecycleState:          PlanningSessionLifecycleStateActive,
		ConversationState:       PlanningSessionConversationStateProcessing,
		CurrentOperationID:      "po_reply_fg",
		CurrentTitleOperationID: "po_title_fg",
		ProjectRoot:             "/workspace/project",
		ModelSelectionJSON:      `{"model":"m"}`,
		CreatedAt:               "2026-04-26T13:00:00Z",
		UpdatedAt:               "2026-04-26T13:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	bindTestPlanningContext(t, db, "ps_title_fg")
	if _, err := store.CreateOperation(ctx, CreatePlanningOperationRequest{
		OperationID:        "po_reply_fg",
		PlanningSessionID:  "ps_title_fg",
		Type:               PlanningOperationTypeReply,
		Status:             PlanningOperationStatusRunning,
		RequestPayloadJSON: `{}`,
		LeaseOwner:         "fg-worker",
		LeaseExpiresAt:     "2026-04-26T13:30:00Z",
		LeaseVersion:       1,
		CreatedAt:          "2026-04-26T13:00:01Z",
	}); err != nil {
		t.Fatalf("create reply op: %v", err)
	}
	if _, err := store.CreateOperation(ctx, CreatePlanningOperationRequest{
		OperationID:        "po_title_fg",
		PlanningSessionID:  "ps_title_fg",
		Type:               PlanningOperationTypeGenerateTitle,
		Status:             PlanningOperationStatusQueued,
		RequestPayloadJSON: `{}`,
		CreatedAt:          "2026-04-26T13:00:02Z",
	}); err != nil {
		t.Fatalf("create title op: %v", err)
	}
	claimed, ok, err := store.ClaimOperation(ctx, "po_title_fg", "title-worker", "2026-04-26T14:00:00Z", "2026-04-26T13:00:03Z")
	if err != nil || !ok {
		t.Fatalf("claim title: ok=%v err=%v", ok, err)
	}

	if err := store.FinalizeGenerateTitle(ctx, FinalizeGenerateTitleRequest{
		SessionID:      "ps_title_fg",
		OperationID:    "po_title_fg",
		LeaseOwner:     claimed.LeaseOwner,
		LeaseVersion:   claimed.LeaseVersion,
		UpdatedAt:      "2026-04-26T13:00:30Z",
		FinishedAt:     "2026-04-26T13:00:30Z",
		GeneratedTitle: "Foreground-safe title",
		TerminalEvent: &AppendPlanningOperationEventRequest{
			EventID:     "po_title_fg:1",
			OperationID: "po_title_fg",
			Seq:         1,
			EventType:   "done",
			PayloadJSON: `{"title":"Foreground-safe title","title_state":"generated"}`,
			CreatedAt:   "2026-04-26T13:00:30Z",
		},
	}); err != nil {
		t.Fatalf("finalize generate title: %v", err)
	}

	titleOp, found, err := store.GetOperation(ctx, "po_title_fg")
	if err != nil || !found {
		t.Fatalf("get title op: ok=%v err=%v", found, err)
	}
	if titleOp.Status != PlanningOperationStatusSucceeded {
		t.Fatalf("expected title op succeeded, got %q", titleOp.Status)
	}

	replyOp, found, err := store.GetOperation(ctx, "po_reply_fg")
	if err != nil || !found {
		t.Fatalf("get reply op: ok=%v err=%v", found, err)
	}
	if replyOp.Status != PlanningOperationStatusRunning {
		t.Fatalf("expected reply op to remain running, got %q", replyOp.Status)
	}

	session, found, err := store.GetSession(ctx, "ps_title_fg")
	if err != nil || !found {
		t.Fatalf("get session: ok=%v err=%v", found, err)
	}
	if session.CurrentOperationID == nil || *session.CurrentOperationID != "po_reply_fg" {
		t.Fatalf("expected interactive slot preserved, got %+v", session.CurrentOperationID)
	}
	if session.CurrentTitleOperationID != nil {
		t.Fatalf("expected title slot cleared after completion, got %+v", session.CurrentTitleOperationID)
	}
	if session.ConversationState != PlanningSessionConversationStateProcessing {
		t.Fatalf("expected conversation_state preserved as processing, got %q", session.ConversationState)
	}
	if session.Title != "Foreground-safe title" || session.TitleState != "generated" {
		t.Fatalf("expected generated title to settle on title lane, got %+v", session)
	}
}

func TestFinalizeGenerateTitleAbortClearsTitleSlot(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewPlanningStore(db)
	ctx := context.Background()

	if _, err := store.CreateSession(ctx, CreatePlanningSessionRequest{
		PlanningSessionID:       "ps_title_abort",
		Title:                   "placeholder",
		TitleState:              "placeholder",
		LifecycleState:          PlanningSessionLifecycleStateActive,
		ConversationState:       PlanningSessionConversationStateReady,
		CurrentTitleOperationID: "po_title_abort",
		ProjectRoot:             "/workspace/project",
		ModelSelectionJSON:      `{"model":"m"}`,
		CreatedAt:               "2026-04-26T14:00:00Z",
		UpdatedAt:               "2026-04-26T14:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	bindTestPlanningContext(t, db, "ps_title_abort")
	if _, err := store.CreateOperation(ctx, CreatePlanningOperationRequest{
		OperationID:        "po_title_abort",
		PlanningSessionID:  "ps_title_abort",
		Type:               PlanningOperationTypeGenerateTitle,
		Status:             PlanningOperationStatusRunning,
		RequestPayloadJSON: `{}`,
		LeaseOwner:         "title-worker",
		LeaseExpiresAt:     "2026-04-26T15:00:00Z",
		LeaseVersion:       2,
		CreatedAt:          "2026-04-26T14:00:01Z",
		StartedAt:          "2026-04-26T14:00:02Z",
	}); err != nil {
		t.Fatalf("create title op: %v", err)
	}

	if err := store.FinalizeGenerateTitleAbort(ctx, FinalizeGenerateTitleAbortRequest{
		SessionID:    "ps_title_abort",
		OperationID:  "po_title_abort",
		LeaseOwner:   "title-worker",
		LeaseVersion: 2,
		Status:       PlanningOperationStatusCancelled,
		UpdatedAt:    "2026-04-26T14:00:30Z",
		FinishedAt:   "2026-04-26T14:00:30Z",
		ErrorMessage: "cancelled during shutdown",
		TerminalEvent: &AppendPlanningOperationEventRequest{
			EventID:     "po_title_abort:1",
			OperationID: "po_title_abort",
			Seq:         1,
			EventType:   "run.cancelled",
			PayloadJSON: `{"reason":"cancelled during shutdown"}`,
			CreatedAt:   "2026-04-26T14:00:30Z",
		},
	}); err != nil {
		t.Fatalf("finalize generate title abort: %v", err)
	}

	session, found, err := store.GetSession(ctx, "ps_title_abort")
	if err != nil || !found {
		t.Fatalf("get session: ok=%v err=%v", found, err)
	}
	if session.CurrentTitleOperationID != nil {
		t.Fatalf("expected title slot cleared after abort, got %+v", session.CurrentTitleOperationID)
	}

	op, found, err := store.GetOperation(ctx, "po_title_abort")
	if err != nil || !found {
		t.Fatalf("get operation: ok=%v err=%v", found, err)
	}
	if op.Status != PlanningOperationStatusCancelled {
		t.Fatalf("expected cancelled title operation, got %q", op.Status)
	}
}

func TestCancelQueuedGenerateTitleOperationClearsTitleSlot(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewPlanningStore(db)
	ctx := context.Background()

	if _, err := store.CreateSession(ctx, CreatePlanningSessionRequest{
		PlanningSessionID:       "ps_title_cancel_queued",
		Title:                   "placeholder",
		TitleState:              "placeholder",
		LifecycleState:          PlanningSessionLifecycleStateActive,
		ConversationState:       PlanningSessionConversationStateReady,
		CurrentTitleOperationID: "po_title_cancel_queued",
		ProjectRoot:             "/workspace/project",
		ModelSelectionJSON:      `{"model":"m"}`,
		CreatedAt:               "2026-04-27T16:00:00Z",
		UpdatedAt:               "2026-04-27T16:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	bindTestPlanningContext(t, db, "ps_title_cancel_queued")
	if _, err := store.CreateOperation(ctx, CreatePlanningOperationRequest{
		OperationID:        "po_title_cancel_queued",
		PlanningSessionID:  "ps_title_cancel_queued",
		Type:               PlanningOperationTypeGenerateTitle,
		Status:             PlanningOperationStatusQueued,
		RequestPayloadJSON: `{}`,
		CreatedAt:          "2026-04-27T16:00:01Z",
	}); err != nil {
		t.Fatalf("create operation: %v", err)
	}

	if err := store.CancelQueuedOperation(ctx, CancelQueuedPlanningOperationRequest{
		OperationID:  "po_title_cancel_queued",
		ErrorMessage: "cancelled before claim",
		UpdatedAt:    "2026-04-27T16:00:30Z",
		FinishedAt:   "2026-04-27T16:00:30Z",
		TerminalEvent: &AppendPlanningOperationEventRequest{
			EventID:     "po_title_cancel_queued:1",
			OperationID: "po_title_cancel_queued",
			Seq:         1,
			EventType:   "run.cancelled",
			PayloadJSON: `{"reason":"cancelled before claim"}`,
			CreatedAt:   "2026-04-27T16:00:30Z",
		},
	}); err != nil {
		t.Fatalf("cancel queued operation: %v", err)
	}

	session, found, err := store.GetSession(ctx, "ps_title_cancel_queued")
	if err != nil || !found {
		t.Fatalf("get session: ok=%v err=%v", found, err)
	}
	if session.CurrentTitleOperationID != nil {
		t.Fatalf("expected queued title cancel to clear title slot, got %+v", session.CurrentTitleOperationID)
	}
	op, found, err := store.GetOperation(ctx, "po_title_cancel_queued")
	if err != nil || !found {
		t.Fatalf("get operation: ok=%v err=%v", found, err)
	}
	if op.Status != PlanningOperationStatusCancelled {
		t.Fatalf("expected cancelled title op, got %q", op.Status)
	}
}

func TestCancelQueuedRegenerateRestoresPreviousActiveLeaf(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewPlanningStore(db)
	ctx := context.Background()

	if _, err := store.CreateSession(ctx, CreatePlanningSessionRequest{
		PlanningSessionID:   "ps_regen_cancel_queued",
		Title:               "regen-cancel-queued",
		TitleState:          "custom",
		LifecycleState:      PlanningSessionLifecycleStateActive,
		ConversationState:   PlanningSessionConversationStateProcessing,
		ActiveLeafMessageID: "pm_target_parent",
		CurrentOperationID:  "po_regen_cancel_queued",
		ProjectRoot:         "/workspace/project",
		ModelSelectionJSON:  `{"model":"m"}`,
		CreatedAt:           "2026-04-27T16:30:00Z",
		UpdatedAt:           "2026-04-27T16:30:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := store.AppendMessage(ctx, AppendPlanningMessageRequest{
		PlanningMessageID:   "pm_target_parent",
		PlanningSessionID:   "ps_regen_cancel_queued",
		Role:                PlanningMessageRoleUser,
		ContentMarkdown:     "Original prompt",
		QuestionPayloadJSON: `{}`,
		AnswerPayloadJSON:   `{}`,
		CreatedAt:           "2026-04-27T16:30:00Z",
	}); err != nil {
		t.Fatalf("append target parent: %v", err)
	}
	if _, err := store.AppendMessage(ctx, AppendPlanningMessageRequest{
		PlanningMessageID:   "pm_target",
		PlanningSessionID:   "ps_regen_cancel_queued",
		ParentMessageID:     "pm_target_parent",
		Role:                PlanningMessageRoleAssistant,
		ContentMarkdown:     "Original answer",
		QuestionPayloadJSON: `{}`,
		AnswerPayloadJSON:   `{}`,
		CreatedAt:           "2026-04-27T16:30:01Z",
	}); err != nil {
		t.Fatalf("append target assistant: %v", err)
	}
	if _, err := store.AppendMessage(ctx, AppendPlanningMessageRequest{
		PlanningMessageID:   "pm_followup",
		PlanningSessionID:   "ps_regen_cancel_queued",
		ParentMessageID:     "pm_target",
		Role:                PlanningMessageRoleUser,
		ContentMarkdown:     "Follow-up question",
		QuestionPayloadJSON: `{}`,
		AnswerPayloadJSON:   `{}`,
		CreatedAt:           "2026-04-27T16:30:02Z",
	}); err != nil {
		t.Fatalf("append follow-up user: %v", err)
	}
	bindTestPlanningContext(t, db, "ps_regen_cancel_queued")
	if _, err := store.CreateOperation(ctx, CreatePlanningOperationRequest{
		OperationID:        "po_regen_cancel_queued",
		PlanningSessionID:  "ps_regen_cancel_queued",
		Type:               PlanningOperationTypeRegenerate,
		Status:             PlanningOperationStatusQueued,
		RequestPayloadJSON: `{"target_message_id":"pm_target","parent_message_id":"pm_target_parent","previous_active_leaf_message_id":"pm_followup"}`,
		CreatedAt:          "2026-04-27T16:30:03Z",
	}); err != nil {
		t.Fatalf("create operation: %v", err)
	}

	if err := store.CancelQueuedOperation(ctx, CancelQueuedPlanningOperationRequest{
		OperationID:  "po_regen_cancel_queued",
		ErrorMessage: "cancelled before claim",
		UpdatedAt:    "2026-04-27T16:30:30Z",
		FinishedAt:   "2026-04-27T16:30:30Z",
		TerminalEvent: &AppendPlanningOperationEventRequest{
			EventID:     "po_regen_cancel_queued:1",
			OperationID: "po_regen_cancel_queued",
			Seq:         1,
			EventType:   "run.cancelled",
			PayloadJSON: `{"reason":"cancelled before claim"}`,
			CreatedAt:   "2026-04-27T16:30:30Z",
		},
	}); err != nil {
		t.Fatalf("cancel queued operation: %v", err)
	}

	session, found, err := store.GetSession(ctx, "ps_regen_cancel_queued")
	if err != nil || !found {
		t.Fatalf("get session: ok=%v err=%v", found, err)
	}
	if session.ActiveLeafMessageID == nil || *session.ActiveLeafMessageID != "pm_followup" {
		t.Fatalf("expected queued regenerate cancel to restore active leaf, got %+v", session.ActiveLeafMessageID)
	}
	if session.CurrentOperationID == nil || *session.CurrentOperationID != "po_regen_cancel_queued" {
		t.Fatalf("expected cancelled regenerate lineage retained, got %+v", session.CurrentOperationID)
	}
}

// TestSetActiveBranchRequiresMatchingSession verifies that branch switching is
// scoped by both session id and message id.
func TestSetActiveBranchRequiresMatchingSession(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewPlanningStore(db)
	ctx := context.Background()

	for _, sessionID := range []string{"ps_a", "ps_b"} {
		if _, err := store.CreateSession(ctx, CreatePlanningSessionRequest{
			PlanningSessionID:  sessionID,
			Title:              sessionID,
			TitleState:         "custom",
			LifecycleState:     PlanningSessionLifecycleStateActive,
			ConversationState:  PlanningSessionConversationStateReady,
			ProjectRoot:        "/workspace/project",
			ModelSelectionJSON: `{"model":"m"}`,
			CreatedAt:          "2026-04-25T16:40:00Z",
			UpdatedAt:          "2026-04-25T16:40:00Z",
		}); err != nil {
			t.Fatalf("create session %s: %v", sessionID, err)
		}
	}
	if _, err := store.AppendMessage(ctx, AppendPlanningMessageRequest{
		PlanningMessageID:   "pm_a",
		PlanningSessionID:   "ps_a",
		Role:                PlanningMessageRoleAssistant,
		ContentMarkdown:     "answer in A",
		QuestionPayloadJSON: `{}`,
		AnswerPayloadJSON:   `{}`,
		CreatedAt:           "2026-04-25T16:40:10Z",
	}); err != nil {
		t.Fatalf("append message: %v", err)
	}

	// pm_a belongs to ps_a; switching to it via ps_b must return Not Found.
	if _, err := store.SetActiveBranch(ctx, "ps_b", "pm_a"); !errors.Is(err, ErrPlanningMessageNotFound) {
		t.Fatalf("expected ErrPlanningMessageNotFound, got %v", err)
	}
	if _, err := store.GetMessageInSession(ctx, "ps_b", "pm_a"); !errors.Is(err, ErrPlanningMessageNotFound) {
		t.Fatalf("expected ErrPlanningMessageNotFound for cross-session GetMessageInSession, got %v", err)
	}
	// And the legitimate (ps_a, pm_a) lookup still works.
	if _, err := store.GetMessageInSession(ctx, "ps_a", "pm_a"); err != nil {
		t.Fatalf("unexpected error fetching pm_a in ps_a: %v", err)
	}
}

// TestPublishContextSnapshotPreservesNewer guards the bootstrap-vs-refresh
// race: a slow bootstrap finishing after a manual /context/refresh must
// not roll the session back to the older snapshot. PublishContextSnapshot
// races on the session's adopted slot — once the newer snapshot is in,
// the older publish's UPDATE evaluates the ELSE branch and the column
// stays put.
func TestPublishContextSnapshotPreservesNewer(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewPlanningStore(db)
	ctx := context.Background()

	if _, err := store.CreateSession(ctx, CreatePlanningSessionRequest{
		PlanningSessionID:  "ps_pub",
		Title:              "pub",
		TitleState:         "custom",
		LifecycleState:     PlanningSessionLifecycleStateActive,
		ConversationState:  PlanningSessionConversationStateReady,
		ProjectRoot:        "/workspace/project",
		ModelSelectionJSON: `{"model":"m"}`,
		CreatedAt:          "2026-04-25T16:40:00Z",
		UpdatedAt:          "2026-04-25T16:40:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	if _, err := store.CreateContextSnapshot(ctx, CreatePlanningContextSnapshotRequest{
		PlanningContextSnapshotID: "snap_old",
		PlanningSessionID:         "ps_pub",
		SummaryMarkdown:           "old",
		IncludedFilesJSON:         `[]`,
		ExcludedFilesJSON:         `[]`,
		CreatedAt:                 "2026-04-25T16:40:01Z",
	}); err != nil {
		t.Fatalf("create old snapshot: %v", err)
	}
	if _, err := store.CreateContextSnapshot(ctx, CreatePlanningContextSnapshotRequest{
		PlanningContextSnapshotID: "snap_new",
		PlanningSessionID:         "ps_pub",
		SummaryMarkdown:           "new",
		IncludedFilesJSON:         `[]`,
		ExcludedFilesJSON:         `[]`,
		CreatedAt:                 "2026-04-25T16:42:00Z",
	}); err != nil {
		t.Fatalf("create new snapshot: %v", err)
	}

	// /context/refresh wins by publishing the newer snapshot first.
	adopted, err := store.PublishContextSnapshot(ctx, "ps_pub", "snap_new", "2026-04-25T16:42:01Z")
	if err != nil {
		t.Fatalf("publish newer: %v", err)
	}
	if adopted != "snap_new" {
		t.Fatalf("expected adopted=snap_new, got %q", adopted)
	}
	// Slow bootstrap finishes and tries to publish the older snapshot.
	// The publish must succeed (no error) but report the still-adopted
	// newer snapshot id so the bootstrap's terminal event can carry the
	// truth, not the candidate.
	adopted, err = store.PublishContextSnapshot(ctx, "ps_pub", "snap_old", "2026-04-25T16:42:02Z")
	if err != nil {
		t.Fatalf("publish older (expected silent demotion-rejection): %v", err)
	}
	if adopted != "snap_new" {
		t.Fatalf("expected adopted=snap_new (newer wins), got %q", adopted)
	}

	session, ok, err := store.GetSession(ctx, "ps_pub")
	if err != nil || !ok {
		t.Fatalf("get session: ok=%v err=%v", ok, err)
	}
	if session.ContextSnapshotID == nil || *session.ContextSnapshotID != "snap_new" {
		t.Fatalf("expected newer snapshot to remain published, got %+v", session.ContextSnapshotID)
	}
}

// TestPublishContextSnapshotFreshSessionAdoptsNewerOnSequentialPublish
// pins the regression that motivated the adopted-slot rewrite: when two
// snapshots already exist in the table for a session whose adopted slot
// is still NULL, publishing the older one first must succeed (set the
// slot) and publishing the newer one second must succeed (replace the
// slot). The previous shape compared against the snapshot table and so
// silently no-op'd the first publish whenever the second snapshot row
// pre-existed in the table — losing the very first adoption on a fresh
// session under any concurrent refresh that beat the bootstrap into the
// table.
func TestPublishContextSnapshotFreshSessionAdoptsNewerOnSequentialPublish(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewPlanningStore(db)
	ctx := context.Background()

	if _, err := store.CreateSession(ctx, CreatePlanningSessionRequest{
		PlanningSessionID:  "ps_fresh",
		Title:              "fresh",
		TitleState:         "custom",
		LifecycleState:     PlanningSessionLifecycleStateActive,
		ConversationState:  PlanningSessionConversationStateReady,
		ProjectRoot:        "/workspace/project",
		ModelSelectionJSON: `{"model":"m"}`,
		CreatedAt:          "2026-04-26T11:00:00Z",
		UpdatedAt:          "2026-04-26T11:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}

	// Pre-create both snapshot rows before either is published. This
	// reproduces the racing-publishers shape where two refreshes both
	// inserted snapshot rows before either won the slot.
	if _, err := store.CreateContextSnapshot(ctx, CreatePlanningContextSnapshotRequest{
		PlanningContextSnapshotID: "snap_a",
		PlanningSessionID:         "ps_fresh",
		SummaryMarkdown:           "a",
		IncludedFilesJSON:         `[]`,
		ExcludedFilesJSON:         `[]`,
		CreatedAt:                 "2026-04-26T11:00:01Z",
	}); err != nil {
		t.Fatalf("create snap_a: %v", err)
	}
	if _, err := store.CreateContextSnapshot(ctx, CreatePlanningContextSnapshotRequest{
		PlanningContextSnapshotID: "snap_b",
		PlanningSessionID:         "ps_fresh",
		SummaryMarkdown:           "b",
		IncludedFilesJSON:         `[]`,
		ExcludedFilesJSON:         `[]`,
		CreatedAt:                 "2026-04-26T11:00:02Z",
	}); err != nil {
		t.Fatalf("create snap_b: %v", err)
	}

	// Publishing snap_a first must adopt it — the slot is empty (NULL),
	// so the candidate always wins regardless of what else exists in the
	// snapshot table. The previous shape regressed exactly here.
	adopted, err := store.PublishContextSnapshot(ctx, "ps_fresh", "snap_a", "2026-04-26T11:00:03Z")
	if err != nil {
		t.Fatalf("publish snap_a: %v", err)
	}
	if adopted != "snap_a" {
		t.Fatalf("expected adopted=snap_a after first publish, got %q", adopted)
	}

	// Publishing snap_b must promote it — strictly newer than snap_a.
	adopted, err = store.PublishContextSnapshot(ctx, "ps_fresh", "snap_b", "2026-04-26T11:00:04Z")
	if err != nil {
		t.Fatalf("publish snap_b: %v", err)
	}
	if adopted != "snap_b" {
		t.Fatalf("expected adopted=snap_b after second publish, got %q", adopted)
	}

	session, ok, err := store.GetSession(ctx, "ps_fresh")
	if err != nil || !ok {
		t.Fatalf("get session: ok=%v err=%v", ok, err)
	}
	if session.ContextSnapshotID == nil || *session.ContextSnapshotID != "snap_b" {
		t.Fatalf("expected session adopted=snap_b, got %+v", session.ContextSnapshotID)
	}
}

// TestPublishContextSnapshotSameSecondTiebreaks asserts that two snapshots
// produced in the same RFC3339 second still have a deterministic winner —
// the tuple (created_at, planning_context_snapshot_id) gives a total order
// matching the read order, so the lexicographically-greater id wins.
func TestPublishContextSnapshotSameSecondTiebreaks(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewPlanningStore(db)
	ctx := context.Background()

	if _, err := store.CreateSession(ctx, CreatePlanningSessionRequest{
		PlanningSessionID:  "ps_tie",
		Title:              "tie",
		TitleState:         "custom",
		LifecycleState:     PlanningSessionLifecycleStateActive,
		ConversationState:  PlanningSessionConversationStateReady,
		ProjectRoot:        "/workspace/project",
		ModelSelectionJSON: `{"model":"m"}`,
		CreatedAt:          "2026-04-26T10:00:00Z",
		UpdatedAt:          "2026-04-26T10:00:00Z",
	}); err != nil {
		t.Fatalf("create session: %v", err)
	}
	const sameTS = "2026-04-26T10:00:01Z"
	for _, id := range []string{"snap_a", "snap_b"} {
		if _, err := store.CreateContextSnapshot(ctx, CreatePlanningContextSnapshotRequest{
			PlanningContextSnapshotID: id,
			PlanningSessionID:         "ps_tie",
			SummaryMarkdown:           id,
			IncludedFilesJSON:         `[]`,
			ExcludedFilesJSON:         `[]`,
			CreatedAt:                 sameTS,
		}); err != nil {
			t.Fatalf("create snapshot %s: %v", id, err)
		}
	}

	// Publishing snap_b (lex-greater) first sets the pointer.
	if _, err := store.PublishContextSnapshot(ctx, "ps_tie", "snap_b", sameTS); err != nil {
		t.Fatalf("publish snap_b: %v", err)
	}
	// Publishing snap_a after must NOT demote — same created_at, smaller
	// id, so the tuple ordering treats snap_b as the strictly-newer entry.
	adopted, err := store.PublishContextSnapshot(ctx, "ps_tie", "snap_a", sameTS)
	if err != nil {
		t.Fatalf("publish snap_a: %v", err)
	}
	if adopted != "snap_b" {
		t.Fatalf("expected adopted=snap_b (lex-greater wins on equal timestamps), got %q", adopted)
	}
}

// TestPublishContextSnapshotRejectsCrossSession guards against a caller
// that hands a snapshot id from a different session. Looking up by id
// alone would let a foreign snapshot land on this session's pointer.
func TestPublishContextSnapshotRejectsCrossSession(t *testing.T) {
	t.Parallel()

	db := openTestDB(t)
	applyMigration(t, db, "0001_initial_schema.up.sql")
	store := NewPlanningStore(db)
	ctx := context.Background()

	for _, sessionID := range []string{"ps_x", "ps_y"} {
		if _, err := store.CreateSession(ctx, CreatePlanningSessionRequest{
			PlanningSessionID:  sessionID,
			Title:              sessionID,
			TitleState:         "custom",
			LifecycleState:     PlanningSessionLifecycleStateActive,
			ConversationState:  PlanningSessionConversationStateReady,
			ProjectRoot:        "/workspace/project",
			ModelSelectionJSON: `{"model":"m"}`,
			CreatedAt:          "2026-04-26T10:00:00Z",
			UpdatedAt:          "2026-04-26T10:00:00Z",
		}); err != nil {
			t.Fatalf("create session %s: %v", sessionID, err)
		}
	}
	if _, err := store.CreateContextSnapshot(ctx, CreatePlanningContextSnapshotRequest{
		PlanningContextSnapshotID: "snap_x",
		PlanningSessionID:         "ps_x",
		SummaryMarkdown:           "x",
		IncludedFilesJSON:         `[]`,
		ExcludedFilesJSON:         `[]`,
		CreatedAt:                 "2026-04-26T10:00:01Z",
	}); err != nil {
		t.Fatalf("create snapshot for ps_x: %v", err)
	}

	// snap_x belongs to ps_x; publishing it under ps_y must fail.
	if _, err := store.PublishContextSnapshot(ctx, "ps_y", "snap_x", "2026-04-26T10:00:02Z"); !errors.Is(err, ErrPlanningSnapshotNotInSession) {
		t.Fatalf("expected ErrPlanningSnapshotNotInSession, got %v", err)
	}
}

// (Counter-based stuck-progression tests removed: the integer counter
// was replaced with a timestamp-derived predicate so observer count cannot
// bias the failure decision. See TestIsStuckOperation* and
// TestFinaliseIfStuck* in service_test.go for the new coverage.)
