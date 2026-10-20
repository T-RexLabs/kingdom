package state

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"kingdom/internal/dbmigrations"
)

func TestAgentConfigurationSnapshotsStayPinnedForRunsAndPlanningSessions(t *testing.T) {
	db := openTestDB(t)
	applyAllMigrationsForRunCollectionEventTest(t, db)
	ctx := context.Background()
	if err := EnsureCanonicalRoles(ctx, db, time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	initialConfiguration, err := GetAgentConfiguration(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	catalogDigest := initialConfiguration.PromptCatalogDigest
	staleCatalogDigest := "0" + catalogDigest[1:]
	if staleCatalogDigest == catalogDigest {
		staleCatalogDigest = "1" + catalogDigest[1:]
	}

	voice := AgentVoice{
		Source:      AgentVoiceSource{Kind: AgentVoiceKindTheme, ThemeID: "star-wars", ThemeDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		Instruction: "Use calm, cinematic language.",
	}
	workspace, err := UpdateWorkspaceAgentVoice(ctx, db, 1, voice, "2026-10-02T12:00:00.000000000Z")
	if err != nil || workspace.RowVersion != 2 {
		t.Fatalf("update workspace voice: %#v %v", workspace, err)
	}
	if _, err := UpdateWorkspaceAgentVoice(ctx, db, 1, NeutralAgentVoice(), "2026-10-02T12:00:01.000000000Z"); !errors.Is(err, ErrAgentConfigurationConflict) {
		t.Fatalf("stale workspace write should conflict, got %v", err)
	}

	neutralOverride := NeutralAgentVoice()
	builderGuidance := "Keep implementation notes concise."
	if _, err := UpdateAgentRoleConfiguration(ctx, db, "builder", &builderGuidance, nil, 0, staleCatalogDigest, "2026-10-02T12:00:01.500000000Z"); !errors.Is(err, ErrAgentPromptCatalogConflict) {
		t.Fatalf("stale role guidance catalog should conflict, got %v", err)
	}
	role, err := UpdateAgentRoleConfiguration(ctx, db, "builder", &builderGuidance, &neutralOverride, 0, catalogDigest, "2026-10-02T12:00:02.000000000Z")
	if err != nil || role.RowVersion != 1 || role.VoiceOverride == nil {
		t.Fatalf("create role settings: %#v %v", role, err)
	}
	if _, err := ResetAgentRoleGuidance(ctx, db, "builder", role.RowVersion, staleCatalogDigest, "2026-10-02T12:00:02.500000000Z"); !errors.Is(err, ErrAgentPromptCatalogConflict) {
		t.Fatalf("stale reset catalog should conflict, got %v", err)
	}
	if _, err := UpdateAgentRoleConfiguration(ctx, db, "builder", nil, nil, 0, catalogDigest, "2026-10-02T12:00:03.000000000Z"); !errors.Is(err, ErrAgentConfigurationConflict) {
		t.Fatalf("duplicate initial role write should conflict, got %v", err)
	}
	planAuthorGuidance := "Ask one focused question at a time."
	planAuthor, err := UpdateAgentRoleConfiguration(ctx, db, "plan-author", &planAuthorGuidance, nil, 0, catalogDigest, "2026-10-02T12:00:03.500000000Z")
	if err != nil || planAuthor.RowVersion != 1 {
		t.Fatalf("create Plan Author settings: %#v %v", planAuthor, err)
	}

	if _, err := db.ExecContext(ctx, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work-agent-config', 'Settings', 'Test', '{}', '2026-10-02T12:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('workflow-agent-config', 'test', 'v1', '{}', '2026-10-02T12:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run-agent-config', NULL, 'work-agent-config', 'workflow-agent-config', 'pending', 'draft', 1, '2026-10-02T12:00:00Z', '2026-10-02T12:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := CaptureRunAgentConfigurationSnapshotTx(ctx, tx, "run-agent-config", "2026-10-02T12:00:04.000000000Z"); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	planningStore := NewPlanningStore(db)
	_, err = planningStore.CreateSessionBundle(ctx, CreatePlanningSessionBundleRequest{
		Session: CreatePlanningSessionRequest{
			PlanningSessionID: "planning-agent-config", Title: "Voice snapshot", TitleState: "custom",
			LifecycleState: PlanningSessionLifecycleStateActive, ConversationState: PlanningSessionConversationStateReady,
			ProjectRoot: "/workspace", ModelSelectionJSON: `{}`, CreatedAt: "2026-10-02T12:00:05Z", UpdatedAt: "2026-10-02T12:00:05Z",
		},
		OpeningMessage: AppendPlanningMessageRequest{
			PlanningMessageID: "message-agent-config", PlanningSessionID: "planning-agent-config", Role: PlanningMessageRoleAssistant,
			ContentMarkdown: "What should we plan?", QuestionPayloadJSON: `{}`, AnswerPayloadJSON: `{}`, CreatedAt: "2026-10-02T12:00:05Z",
		},
		ContextSnapshot: CreatePlanningContextSnapshotRequest{
			PlanningContextSnapshotID: "context-agent-config", PlanningSessionID: "planning-agent-config", SummaryMarkdown: "test",
			IncludedFilesJSON: `[]`, ExcludedFilesJSON: `[]`, CreatedAt: "2026-10-02T12:00:05Z",
		},
	})
	if err != nil {
		t.Fatalf("create planning session bundle: %v", err)
	}

	runSnapshot, err := LoadRunAgentConfigurationSnapshot(ctx, db, "run-agent-config")
	if err != nil {
		t.Fatal(err)
	}
	if got := runSnapshot.ForRole("reviewer").Voice; got != voice {
		t.Fatalf("workspace voice not captured for role without override: %#v", got)
	}
	if got := runSnapshot.ForRole("builder"); got.Voice.Source.Kind != AgentVoiceKindNeutral || got.GuidanceText != "Keep implementation notes concise." {
		t.Fatalf("role configuration not captured: %#v", got)
	}
	planningConfiguration, err := planningStore.LoadAgentConfiguration(ctx, "planning-agent-config")
	if err != nil || planningConfiguration.Role.Voice != voice || planningConfiguration.Role.GuidanceText != planAuthorGuidance {
		t.Fatalf("planning Plan Author snapshot: %#v %v", planningConfiguration, err)
	}
	directSession, err := planningStore.CreateSession(ctx, CreatePlanningSessionRequest{
		PlanningSessionID: "planning-agent-config-direct", Title: "Direct session", TitleState: "custom",
		LifecycleState: PlanningSessionLifecycleStateActive, ConversationState: PlanningSessionConversationStateReady,
		ProjectRoot: "/workspace", ModelSelectionJSON: `{}`, CreatedAt: "2026-10-02T12:00:05Z", UpdatedAt: "2026-10-02T12:00:05Z",
	})
	if err != nil {
		t.Fatalf("create direct planning session: %v", err)
	}
	directConfiguration, err := planningStore.LoadAgentConfiguration(ctx, directSession.PlanningSessionID)
	if err != nil || !reflect.DeepEqual(directConfiguration, planningConfiguration) {
		t.Fatalf("direct planning session settings snapshot: %#v %v", directConfiguration, err)
	}
	planAuthorOverride := NeutralAgentVoice()
	planAuthor, err = UpdateAgentRoleConfiguration(ctx, db, "plan-author", planAuthor.GuidanceOverride, &planAuthorOverride, planAuthor.RowVersion, catalogDigest, "2026-10-02T12:00:05.500000000Z")
	if err != nil {
		t.Fatalf("set Plan Author voice override: %v", err)
	}
	overrideSession, err := planningStore.CreateSession(ctx, CreatePlanningSessionRequest{
		PlanningSessionID: "planning-agent-config-override", Title: "Override session", TitleState: "custom",
		LifecycleState: PlanningSessionLifecycleStateActive, ConversationState: PlanningSessionConversationStateReady,
		ProjectRoot: "/workspace", ModelSelectionJSON: `{}`, CreatedAt: "2026-10-02T12:00:05Z", UpdatedAt: "2026-10-02T12:00:05Z",
	})
	if err != nil {
		t.Fatalf("create Plan Author override session: %v", err)
	}
	overrideConfiguration, err := planningStore.LoadAgentConfiguration(ctx, overrideSession.PlanningSessionID)
	if err != nil || overrideConfiguration.Role.Voice != planAuthorOverride || overrideConfiguration.Role.GuidanceText != planningConfiguration.Role.GuidanceText {
		t.Fatalf("Plan Author override was not captured: %#v %v", overrideConfiguration, err)
	}

	if _, err := UpdateWorkspaceAgentVoice(ctx, db, workspace.RowVersion, NeutralAgentVoice(), "2026-10-02T12:00:06.000000000Z"); err != nil {
		t.Fatal(err)
	}
	runSnapshotAfterEdit, err := LoadRunAgentConfigurationSnapshot(ctx, db, "run-agent-config")
	if err != nil || runSnapshotAfterEdit.ForRole("reviewer").Voice != voice {
		t.Fatalf("run snapshot changed after workspace edit: %#v %v", runSnapshotAfterEdit, err)
	}
	briefGuidance := "Keep planning decisions brief."
	if _, err := UpdateAgentRoleConfiguration(ctx, db, "plan-author", &briefGuidance, nil, planAuthor.RowVersion, catalogDigest, "2026-10-02T12:00:07.000000000Z"); err != nil {
		t.Fatal(err)
	}
	planningConfigurationAfterEdit, err := planningStore.LoadAgentConfiguration(ctx, "planning-agent-config")
	if err != nil || !reflect.DeepEqual(planningConfigurationAfterEdit, planningConfiguration) {
		t.Fatalf("planning snapshot changed after settings edits: %#v %v", planningConfigurationAfterEdit, err)
	}
	if _, err := db.ExecContext(ctx, `UPDATE run_agent_configuration_snapshots SET snapshot_json='{}' WHERE run_id='run-agent-config'`); err == nil {
		t.Fatal("run configuration snapshot must be immutable")
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM planning_agent_configuration_snapshots WHERE planning_session_id='planning-agent-config'`); err == nil {
		t.Fatal("planning session configuration snapshot must be immutable while its session exists")
	}
	if deleted, err := planningStore.DeleteSession(ctx, "planning-agent-config"); err != nil || !deleted {
		t.Fatalf("delete planning session with cascading voice snapshot: deleted=%v err=%v", deleted, err)
	}
	var remainingSnapshots int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM planning_agent_configuration_snapshots WHERE planning_session_id='planning-agent-config'`).Scan(&remainingSnapshots); err != nil || remainingSnapshots != 0 {
		t.Fatalf("planning session deletion left a voice snapshot: count=%d err=%v", remainingSnapshots, err)
	}
}

func TestAgentConfigurationMigrationDoesNotFabricatePromptSnapshots(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	names, err := dbmigrations.List()
	if err != nil {
		t.Fatal(err)
	}
	var beforeAgentSettings []SQLMigration
	for _, name := range names[:len(names)-1] {
		raw, err := dbmigrations.Read(name)
		if err != nil {
			t.Fatal(err)
		}
		beforeAgentSettings = append(beforeAgentSettings, SQLMigration{Name: name, SQL: raw})
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE schema_migrations (name TEXT PRIMARY KEY, sha256 TEXT NOT NULL CHECK (length(sha256) = 64 AND sha256 NOT GLOB '*[^0-9a-f]*'), applied_at TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	for _, migration := range beforeAgentSettings {
		if err := applyMigrationTx(ctx, db, migration, time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)); err != nil {
			t.Fatal(err)
		}
	}
	if err := EnsureCanonicalRoles(ctx, db, time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('old-work', 'Old', 'Test', '{}', '2026-10-02T12:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('old-workflow', 'test', 'v1', '{}', '2026-10-02T12:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('old-run', NULL, 'old-work', 'old-workflow', 'pending', 'draft', 1, '2026-10-02T12:00:00Z', '2026-10-02T12:00:00Z')`); err != nil {
		t.Fatal(err)
	}
	planningStore := NewPlanningStore(db)
	if _, err := insertPlanningSession(ctx, db, CreatePlanningSessionRequest{
		PlanningSessionID: "old-planning", Title: "Old", TitleState: "custom", LifecycleState: PlanningSessionLifecycleStateActive,
		ConversationState: PlanningSessionConversationStateReady, ProjectRoot: "/workspace", ModelSelectionJSON: `{}`,
		CreatedAt: "2026-10-02T12:00:00Z", UpdatedAt: "2026-10-02T12:00:00Z",
	}); err != nil {
		t.Fatal(err)
	}
	allMigrations := append([]SQLMigration(nil), beforeAgentSettings...)
	newMigration, err := dbmigrations.Read(names[len(names)-1])
	if err != nil {
		t.Fatal(err)
	}
	allMigrations = append(allMigrations, SQLMigration{Name: names[len(names)-1], SQL: newMigration})
	if err := ApplySQLMigrations(ctx, db, allMigrations, time.Date(2026, 10, 2, 12, 0, 1, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	var runSnapshots, planningSnapshots int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM run_agent_configuration_snapshots WHERE run_id='old-run'`).Scan(&runSnapshots); err != nil || runSnapshots != 0 {
		t.Fatalf("migration fabricated historical run snapshots: count=%d err=%v", runSnapshots, err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM planning_agent_configuration_snapshots WHERE planning_session_id='old-planning'`).Scan(&planningSnapshots); err != nil || planningSnapshots != 0 {
		t.Fatalf("migration fabricated historical planning snapshots: count=%d err=%v", planningSnapshots, err)
	}
	if _, err := LoadRunAgentConfigurationSnapshot(ctx, db, "old-run"); err == nil {
		t.Fatal("historical run without a captured prompt catalog must not be silently assigned one")
	}
	if _, err := planningStore.LoadAgentConfiguration(ctx, "old-planning"); err == nil {
		t.Fatal("historical planning session without a captured prompt catalog must not be silently assigned one")
	}
}

func TestAgentRoleGuidanceDistinguishesInheritanceFromExplicitEmpty(t *testing.T) {
	db := openTestDB(t)
	applyAllMigrationsForRunCollectionEventTest(t, db)
	ctx := context.Background()
	if err := EnsureCanonicalRoles(ctx, db, time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	configuration, err := GetAgentConfiguration(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	var builder AgentRoleConfiguration
	for _, role := range configuration.Roles {
		if role.RoleID == "builder" {
			builder = role
		}
	}
	if builder.GuidanceOverride != nil || builder.EffectiveGuidance != builder.DefaultGuidance {
		t.Fatalf("builder should inherit its canonical guidance: %#v", builder)
	}
	empty := ""
	updated, err := UpdateAgentRoleConfiguration(ctx, db, "builder", &empty, nil, builder.RowVersion, configuration.PromptCatalogDigest, "2026-10-02T12:00:00.000000000Z")
	if err != nil {
		t.Fatal(err)
	}
	if updated.GuidanceOverride == nil || *updated.GuidanceOverride != "" || updated.EffectiveGuidance != "" {
		t.Fatalf("explicit empty guidance override was collapsed into inheritance: %#v", updated)
	}
}

func TestInheritedAgentVoiceRejectsStoredOverrideFields(t *testing.T) {
	db := openTestDB(t)
	applyAllMigrationsForRunCollectionEventTest(t, db)
	ctx := context.Background()
	if err := EnsureCanonicalRoles(ctx, db, time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO agent_role_configuration (role_id, voice_kind, voice_theme_id, voice_theme_digest, voice_instruction, row_version, updated_at) VALUES ('builder', NULL, '', '', '', 1, '2026-10-02T12:00:00Z')`); err != nil {
		t.Fatalf("valid inherited voice fields should be accepted: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO agent_role_configuration (role_id, voice_kind, voice_theme_id, voice_theme_digest, voice_instruction, row_version, updated_at) VALUES ('reviewer', NULL, '', '', 'inconsistent', 1, '2026-10-02T12:00:00Z')`); err == nil {
		t.Fatal("inherited voice must reject a persisted instruction override")
	}
}

func TestResetAgentRoleGuidanceReturnsVersionFromItsCompareAndSwap(t *testing.T) {
	db := openTestDB(t)
	applyAllMigrationsForRunCollectionEventTest(t, db)
	ctx := context.Background()
	if err := EnsureCanonicalRoles(ctx, db, time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	configuration, err := GetAgentConfiguration(ctx, db)
	if err != nil {
		t.Fatal(err)
	}

	resetWithoutStoredRow, err := ResetAgentRoleGuidance(ctx, db, "reviewer", 0, configuration.PromptCatalogDigest, "2026-10-02T12:00:00Z")
	if err != nil || resetWithoutStoredRow.RowVersion != 0 || resetWithoutStoredRow.GuidanceOverride != nil {
		t.Fatalf("reset without stored row should return the default row version: %#v %v", resetWithoutStoredRow, err)
	}

	concurrentGuidance := "A concurrent browser edit."
	concurrent, err := UpdateAgentRoleConfiguration(ctx, db, "reviewer", &concurrentGuidance, nil, resetWithoutStoredRow.RowVersion, configuration.PromptCatalogDigest, "2026-10-02T12:00:01Z")
	if err != nil || concurrent.RowVersion != 1 {
		t.Fatalf("create concurrent edit: %#v %v", concurrent, err)
	}
	if _, err := UpdateAgentRoleConfiguration(ctx, db, "reviewer", nil, nil, resetWithoutStoredRow.RowVersion, configuration.PromptCatalogDigest, "2026-10-02T12:00:02Z"); !errors.Is(err, ErrAgentConfigurationConflict) {
		t.Fatalf("stale editor must not overwrite a row created after its reset response: %v", err)
	}

	reset, err := ResetAgentRoleGuidance(ctx, db, "reviewer", concurrent.RowVersion, configuration.PromptCatalogDigest, "2026-10-02T12:00:03Z")
	if err != nil || reset.RowVersion != concurrent.RowVersion+1 || reset.GuidanceOverride != nil || reset.EffectiveGuidance != reset.DefaultGuidance {
		t.Fatalf("reset must return the committed reset version and default guidance: %#v %v", reset, err)
	}

	otherBrowserGuidance := "Another browser edit after reset."
	otherBrowser, err := UpdateAgentRoleConfiguration(ctx, db, "reviewer", &otherBrowserGuidance, nil, reset.RowVersion, configuration.PromptCatalogDigest, "2026-10-02T12:00:04Z")
	if err != nil || otherBrowser.RowVersion != reset.RowVersion+1 {
		t.Fatalf("update from returned reset version: %#v %v", otherBrowser, err)
	}
	staleDraft := "A stale unsaved draft."
	if _, err := UpdateAgentRoleConfiguration(ctx, db, "reviewer", &staleDraft, nil, reset.RowVersion, configuration.PromptCatalogDigest, "2026-10-02T12:00:05Z"); !errors.Is(err, ErrAgentConfigurationConflict) {
		t.Fatalf("stale reset draft must conflict after another browser writes: %v", err)
	}
}

func TestAgentConfigurationSnapshotsAreRequiredForPersistedWork(t *testing.T) {
	db := openTestDB(t)
	applyAllMigrationsForRunCollectionEventTest(t, db)
	ctx := context.Background()
	if _, err := LoadRunAgentConfigurationSnapshot(ctx, db, "missing-run"); err == nil {
		t.Fatal("a run without its required settings snapshot must be rejected")
	}
	if _, err := LoadPlanningAgentConfiguration(ctx, db, "missing-session"); err == nil {
		t.Fatal("a planning session without its required settings snapshot must be rejected")
	}
}
