package state

import (
	"context"
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"

	"kingdom/internal/dbmigrations"
)

func TestTokenUsageInsertAndSumForRun(t *testing.T) {
	t.Parallel()

	db := openTokenUsageTestDB(t)
	applyTokenUsageMigration(t, db)
	seedTokenUsagePrereqs(t, db)

	store := NewTokenUsageStore(db)
	ctx := context.Background()

	rec1, err := store.Insert(ctx, InsertTokenUsageRequest{
		RunID:            "run_tok_1",
		NodeRunID:        "run_tok_1_nr_1",
		AttemptID:        "attempt_1",
		RequiredRole:     "governance",
		Model:            "claude-opus-4",
		InputTokens:      1000,
		OutputTokens:     200,
		TotalTokens:      1200,
		ReasoningTokens:  50,
		CacheReadTokens:  100,
		CacheWriteTokens: 10,
		CreatedAt:        "2026-03-01T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("insert token usage 1: %v", err)
	}
	if rec1.UsageID == "" {
		t.Fatal("expected non-empty usage_id")
	}
	if rec1.TotalTokens != 1200 {
		t.Fatalf("expected total_tokens=1200, got %d", rec1.TotalTokens)
	}

	_, err = store.Insert(ctx, InsertTokenUsageRequest{
		RunID:            "run_tok_1",
		NodeRunID:        "run_tok_1_nr_1",
		AttemptID:        "attempt_2",
		RequiredRole:     "knight",
		Model:            "claude-opus-4",
		InputTokens:      500,
		OutputTokens:     100,
		TotalTokens:      600,
		ReasoningTokens:  20,
		CacheReadTokens:  50,
		CacheWriteTokens: 5,
		CreatedAt:        "2026-03-01T12:01:00Z",
	})
	if err != nil {
		t.Fatalf("insert token usage 2: %v", err)
	}

	total, err := store.SumForRun(ctx, "run_tok_1")
	if err != nil {
		t.Fatalf("sum for run: %v", err)
	}
	if total != 1800 {
		t.Fatalf("expected sum=1800, got %d", total)
	}

	// SumForRun for unknown run returns 0
	zeroTotal, err := store.SumForRun(ctx, "run_nonexistent")
	if err != nil {
		t.Fatalf("sum for nonexistent run: %v", err)
	}
	if zeroTotal != 0 {
		t.Fatalf("expected sum=0 for nonexistent run, got %d", zeroTotal)
	}
}

func TestTokenUsageSummaryByTimeWindow(t *testing.T) {
	t.Parallel()

	db := openTokenUsageTestDB(t)
	applyTokenUsageMigration(t, db)
	seedTokenUsagePrereqs(t, db)

	store := NewTokenUsageStore(db)
	ctx := context.Background()

	_, _ = store.Insert(ctx, InsertTokenUsageRequest{
		RunID:            "run_tok_3",
		NodeRunID:        "run_tok_3_nr_1",
		AttemptID:        "attempt_1",
		RequiredRole:     "governance",
		InputTokens:      1000,
		OutputTokens:     200,
		TotalTokens:      1200,
		ReasoningTokens:  50,
		CacheReadTokens:  100,
		CacheWriteTokens: 10,
		CreatedAt:        "2026-03-01T12:00:00Z",
	})
	_, _ = store.Insert(ctx, InsertTokenUsageRequest{
		RunID:            "run_tok_3",
		NodeRunID:        "run_tok_3_nr_1",
		AttemptID:        "attempt_2",
		RequiredRole:     "knight",
		InputTokens:      500,
		OutputTokens:     100,
		TotalTokens:      600,
		ReasoningTokens:  20,
		CacheReadTokens:  50,
		CacheWriteTokens: 5,
		CreatedAt:        "2026-03-01T13:00:00Z",
	})

	// Window that captures both records
	summary, err := store.SummaryByTimeWindow(ctx, "2026-03-01T11:00:00Z")
	if err != nil {
		t.Fatalf("summary by time window: %v", err)
	}
	if summary.TotalTokens != 1800 {
		t.Fatalf("expected total=1800, got %d", summary.TotalTokens)
	}
	if summary.InputTokens != 1500 {
		t.Fatalf("expected input=1500, got %d", summary.InputTokens)
	}
	if summary.RecordCount != 2 {
		t.Fatalf("expected record_count=2, got %d", summary.RecordCount)
	}

	// Window that captures only the second record
	summary2, err := store.SummaryByTimeWindow(ctx, "2026-03-01T12:30:00Z")
	if err != nil {
		t.Fatalf("summary by narrow window: %v", err)
	}
	if summary2.TotalTokens != 600 {
		t.Fatalf("expected total=600 in narrow window, got %d", summary2.TotalTokens)
	}
	if summary2.RecordCount != 1 {
		t.Fatalf("expected record_count=1 in narrow window, got %d", summary2.RecordCount)
	}

	// Window that captures nothing
	summary3, err := store.SummaryByTimeWindow(ctx, "2026-03-02T00:00:00Z")
	if err != nil {
		t.Fatalf("summary by future window: %v", err)
	}
	if summary3.TotalTokens != 0 {
		t.Fatalf("expected total=0 in future window, got %d", summary3.TotalTokens)
	}
}

func TestTokenUsageByRole(t *testing.T) {
	t.Parallel()

	db := openTokenUsageTestDB(t)
	applyTokenUsageMigration(t, db)
	seedTokenUsagePrereqs(t, db)

	store := NewTokenUsageStore(db)
	ctx := context.Background()

	_, _ = store.Insert(ctx, InsertTokenUsageRequest{
		RunID:        "run_tok_4",
		NodeRunID:    "run_tok_4_nr_1",
		AttemptID:    "attempt_1",
		RequiredRole: "governance",
		TotalTokens:  1000,
		InputTokens:  800,
		OutputTokens: 200,
		CreatedAt:    "2026-03-01T12:00:00Z",
	})
	_, _ = store.Insert(ctx, InsertTokenUsageRequest{
		RunID:        "run_tok_4",
		NodeRunID:    "run_tok_4_nr_1",
		AttemptID:    "attempt_2",
		RequiredRole: "knight",
		TotalTokens:  500,
		InputTokens:  400,
		OutputTokens: 100,
		CreatedAt:    "2026-03-01T12:01:00Z",
	})
	_, _ = store.Insert(ctx, InsertTokenUsageRequest{
		RunID:        "run_tok_4",
		NodeRunID:    "run_tok_4_nr_1",
		AttemptID:    "attempt_3",
		RequiredRole: "governance",
		TotalTokens:  300,
		InputTokens:  250,
		OutputTokens: 50,
		CreatedAt:    "2026-03-01T12:02:00Z",
	})

	byRole, err := store.ByRole(ctx, "2026-03-01T11:00:00Z")
	if err != nil {
		t.Fatalf("by role: %v", err)
	}
	if len(byRole) != 2 {
		t.Fatalf("expected 2 role groups, got %d", len(byRole))
	}
	// Should be sorted by total_tokens DESC (governance=1300, knight=500)
	if byRole[0].RequiredRole != "governance" || byRole[0].TotalTokens != 1300 {
		t.Fatalf("expected governance with 1300 tokens first, got %q with %d", byRole[0].RequiredRole, byRole[0].TotalTokens)
	}
	if byRole[1].RequiredRole != "knight" || byRole[1].TotalTokens != 500 {
		t.Fatalf("expected knight with 500 tokens second, got %q with %d", byRole[1].RequiredRole, byRole[1].TotalTokens)
	}
}

func TestTokenUsageByRoleForRun(t *testing.T) {
	t.Parallel()

	db := openTokenUsageTestDB(t)
	applyTokenUsageMigration(t, db)
	seedTokenUsagePrereqs(t, db)

	store := NewTokenUsageStore(db)
	ctx := context.Background()

	_, _ = store.Insert(ctx, InsertTokenUsageRequest{
		RunID:           "run_tok_4",
		NodeRunID:       "run_tok_4_nr_1",
		AttemptID:       "attempt_1",
		RequiredRole:    "governance",
		TotalTokens:     1000,
		InputTokens:     700,
		OutputTokens:    200,
		ReasoningTokens: 100,
		CreatedAt:       "2026-03-01T12:00:00Z",
	})
	_, _ = store.Insert(ctx, InsertTokenUsageRequest{
		RunID:           "run_tok_4",
		NodeRunID:       "run_tok_4_nr_1",
		AttemptID:       "attempt_2",
		RequiredRole:    "knight",
		TotalTokens:     500,
		InputTokens:     400,
		OutputTokens:    80,
		ReasoningTokens: 20,
		CreatedAt:       "2026-03-01T12:01:00Z",
	})
	_, _ = store.Insert(ctx, InsertTokenUsageRequest{
		RunID:           "run_tok_5a",
		NodeRunID:       "run_tok_5a_nr_1",
		AttemptID:       "attempt_1",
		RequiredRole:    "governance",
		TotalTokens:     200,
		InputTokens:     150,
		OutputTokens:    40,
		ReasoningTokens: 10,
		CreatedAt:       "2026-03-01T12:02:00Z",
	})

	byRole, err := store.ByRoleForRun(ctx, "run_tok_4")
	if err != nil {
		t.Fatalf("by role for run: %v", err)
	}
	if len(byRole) != 2 {
		t.Fatalf("expected 2 role groups, got %d", len(byRole))
	}
	if byRole[0].RequiredRole != "governance" || byRole[0].TotalTokens != 1000 {
		t.Fatalf("expected governance with 1000 provider tokens first, got %q with %d", byRole[0].RequiredRole, byRole[0].TotalTokens)
	}
	if byRole[1].RequiredRole != "knight" || byRole[1].TotalTokens != 500 {
		t.Fatalf("expected knight with 500 provider tokens second, got %q with %d", byRole[1].RequiredRole, byRole[1].TotalTokens)
	}
}

func TestTokenUsageByRoleIncludesExactModelBreakdown(t *testing.T) {
	t.Parallel()

	db := openTokenUsageTestDB(t)
	applyTokenUsageMigration(t, db)
	seedTokenUsagePrereqs(t, db)

	store := NewTokenUsageStore(db)
	ctx := context.Background()

	_, _ = store.Insert(ctx, InsertTokenUsageRequest{
		RunID:           "run_tok_4",
		NodeRunID:       "run_tok_4_nr_1",
		AttemptID:       "attempt_1",
		RequiredRole:    "governance",
		Model:           "github-copilot/gpt-4.1",
		Variant:         "medium",
		TotalTokens:     900,
		InputTokens:     600,
		OutputTokens:    200,
		ReasoningTokens: 100,
		CreatedAt:       "2026-03-01T12:00:00Z",
	})
	_, _ = store.Insert(ctx, InsertTokenUsageRequest{
		RunID:           "run_tok_4",
		NodeRunID:       "run_tok_4_nr_1",
		AttemptID:       "attempt_2",
		RequiredRole:    "governance",
		Model:           "anthropic/opus-4.6",
		Variant:         "high",
		TotalTokens:     700,
		InputTokens:     500,
		OutputTokens:    150,
		ReasoningTokens: 50,
		CreatedAt:       "2026-03-01T12:01:00Z",
	})
	_, _ = store.Insert(ctx, InsertTokenUsageRequest{
		RunID:           "run_tok_5a",
		NodeRunID:       "run_tok_5a_nr_1",
		AttemptID:       "attempt_1",
		RequiredRole:    "governance",
		Model:           "github-copilot/gpt-4.1",
		Variant:         "medium",
		TotalTokens:     300,
		InputTokens:     200,
		OutputTokens:    80,
		ReasoningTokens: 20,
		CreatedAt:       "2026-03-01T12:02:00Z",
	})

	byRole, err := store.ByRole(ctx, "2026-03-01T11:00:00Z")
	if err != nil {
		t.Fatalf("by role: %v", err)
	}
	if len(byRole) == 0 {
		t.Fatal("expected at least one role group")
	}
	if byRole[0].RequiredRole != "governance" {
		t.Fatalf("expected governance role group first, got %q", byRole[0].RequiredRole)
	}
	if len(byRole[0].ByModel) != 2 {
		t.Fatalf("expected 2 model breakdown rows, got %d", len(byRole[0].ByModel))
	}

	first := byRole[0].ByModel[0]
	if first.Model != "github-copilot/gpt-4.1" {
		t.Fatalf("expected top model github-copilot/gpt-4.1, got %q", first.Model)
	}
	if first.TotalTokens != 1200 {
		t.Fatalf("expected top model provider total 1200, got %d", first.TotalTokens)
	}
	if first.RunCount != 2 {
		t.Fatalf("expected top model run_count=2, got %d", first.RunCount)
	}
	if first.RecordCount != 2 {
		t.Fatalf("expected top model record_count=2, got %d", first.RecordCount)
	}

	second := byRole[0].ByModel[1]
	if second.Model != "anthropic/opus-4.6" {
		t.Fatalf("expected second model anthropic/opus-4.6, got %q", second.Model)
	}
	if second.TotalTokens != 700 {
		t.Fatalf("expected second model provider total 700, got %d", second.TotalTokens)
	}
	if second.RunCount != 1 {
		t.Fatalf("expected second model run_count=1, got %d", second.RunCount)
	}
}

func TestTokenUsageByRoleOrdersByDerivedTotals(t *testing.T) {
	t.Parallel()

	db := openTokenUsageTestDB(t)
	applyTokenUsageMigration(t, db)
	seedTokenUsagePrereqs(t, db)

	store := NewTokenUsageStore(db)
	ctx := context.Background()

	_, _ = store.Insert(ctx, InsertTokenUsageRequest{
		RunID:           "run_tok_5a",
		NodeRunID:       "run_tok_5a_nr_1",
		AttemptID:       "attempt_1",
		RequiredRole:    "governance",
		Model:           "github-copilot/gpt-4.1",
		TotalTokens:     1000,
		InputTokens:     200,
		OutputTokens:    50,
		ReasoningTokens: 0,
		CreatedAt:       "2026-03-01T12:00:00Z",
	})
	_, _ = store.Insert(ctx, InsertTokenUsageRequest{
		RunID:           "run_tok_5a",
		NodeRunID:       "run_tok_5a_nr_1",
		AttemptID:       "attempt_2",
		RequiredRole:    "knight",
		Model:           "anthropic/opus-4.6",
		TotalTokens:     700,
		InputTokens:     300,
		OutputTokens:    250,
		ReasoningTokens: 100,
		CreatedAt:       "2026-03-01T12:01:00Z",
	})

	byRole, err := store.ByRole(ctx, "2026-03-01T11:00:00Z")
	if err != nil {
		t.Fatalf("by role: %v", err)
	}
	if len(byRole) != 2 {
		t.Fatalf("expected 2 role groups, got %d", len(byRole))
	}

	if byRole[0].RequiredRole != "knight" {
		t.Fatalf("expected knight first because derived total is higher, got %q", byRole[0].RequiredRole)
	}
	if derived := byRole[0].InputTokens + byRole[0].OutputTokens + byRole[0].ReasoningTokens; derived != 650 {
		t.Fatalf("expected knight derived total 650, got %d", derived)
	}
	if byRole[1].RequiredRole != "governance" {
		t.Fatalf("expected governance second because derived total is lower, got %q", byRole[1].RequiredRole)
	}

	if len(byRole[0].ByModel) != 1 {
		t.Fatalf("expected one model row for knight, got %d", len(byRole[0].ByModel))
	}
	if byRole[0].ByModel[0].Model != "anthropic/opus-4.6" {
		t.Fatalf("expected knight model row first, got %q", byRole[0].ByModel[0].Model)
	}
}

func TestTokenUsageTopRunsByTokens(t *testing.T) {
	t.Parallel()

	db := openTokenUsageTestDB(t)
	applyTokenUsageMigration(t, db)
	seedTokenUsagePrereqs(t, db)

	store := NewTokenUsageStore(db)
	ctx := context.Background()

	// Seed run_tok_5a with more tokens
	_, _ = store.Insert(ctx, InsertTokenUsageRequest{
		RunID:        "run_tok_5a",
		NodeRunID:    "run_tok_5a_nr_1",
		AttemptID:    "attempt_1",
		RequiredRole: "governance",
		TotalTokens:  2000,
		CreatedAt:    "2026-03-01T12:00:00Z",
	})
	// Seed run_tok_5b with fewer tokens
	_, _ = store.Insert(ctx, InsertTokenUsageRequest{
		RunID:        "run_tok_5b",
		NodeRunID:    "run_tok_5b_nr_1",
		AttemptID:    "attempt_1",
		RequiredRole: "governance",
		TotalTokens:  500,
		CreatedAt:    "2026-03-01T12:01:00Z",
	})

	topRuns, err := store.TopRunsByTokens(ctx, "2026-03-01T11:00:00Z", 10)
	if err != nil {
		t.Fatalf("top runs: %v", err)
	}
	if len(topRuns) != 2 {
		t.Fatalf("expected 2 runs, got %d", len(topRuns))
	}
	// Should be sorted by total_tokens DESC
	if topRuns[0].RunID != "run_tok_5a" || topRuns[0].TotalTokens != 2000 {
		t.Fatalf("expected run_tok_5a with 2000 tokens first, got %q with %d", topRuns[0].RunID, topRuns[0].TotalTokens)
	}
	if topRuns[1].RunID != "run_tok_5b" || topRuns[1].TotalTokens != 500 {
		t.Fatalf("expected run_tok_5b with 500 tokens second, got %q with %d", topRuns[1].RunID, topRuns[1].TotalTokens)
	}

	// Test limit
	topRunsLimited, err := store.TopRunsByTokens(ctx, "2026-03-01T11:00:00Z", 1)
	if err != nil {
		t.Fatalf("top runs limited: %v", err)
	}
	if len(topRunsLimited) != 1 {
		t.Fatalf("expected 1 run with limit=1, got %d", len(topRunsLimited))
	}
}

func TestTokenUsageInsertIsIdempotentPerAttempt(t *testing.T) {
	t.Parallel()

	db := openTokenUsageTestDB(t)
	applyTokenUsageMigration(t, db)
	seedTokenUsagePrereqs(t, db)

	store := NewTokenUsageStore(db)
	ctx := context.Background()

	_, err := store.Insert(ctx, InsertTokenUsageRequest{
		RunID:            "run_tok_1",
		NodeRunID:        "run_tok_1_nr_1",
		AttemptID:        "attempt_dedupe_1",
		RequiredRole:     "governance",
		Model:            "claude-opus-4",
		InputTokens:      100,
		OutputTokens:     20,
		TotalTokens:      120,
		ReasoningTokens:  5,
		CacheReadTokens:  0,
		CacheWriteTokens: 0,
		CreatedAt:        "2026-03-01T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("insert token usage first time: %v", err)
	}

	_, err = store.Insert(ctx, InsertTokenUsageRequest{
		RunID:            "run_tok_1",
		NodeRunID:        "run_tok_1_nr_1",
		AttemptID:        "attempt_dedupe_1",
		RequiredRole:     "governance",
		Model:            "claude-opus-4",
		InputTokens:      100,
		OutputTokens:     20,
		TotalTokens:      120,
		ReasoningTokens:  5,
		CacheReadTokens:  0,
		CacheWriteTokens: 0,
		CreatedAt:        "2026-03-01T12:00:01Z",
	})
	if err != nil {
		t.Fatalf("insert token usage duplicate attempt: %v", err)
	}

	total, err := store.SumForRun(ctx, "run_tok_1")
	if err != nil {
		t.Fatalf("sum for run after duplicate insert: %v", err)
	}
	if total != 120 {
		t.Fatalf("expected deduplicated total=120, got %d", total)
	}
}

// --- test helpers ---

func openTokenUsageTestDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatalf("enable foreign keys: %v", err)
	}
	return db
}

func applyTokenUsageMigration(t *testing.T, db *sql.DB) {
	t.Helper()

	raw, err := dbmigrations.Read("0001_initial_schema.up.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	if _, err := db.Exec(string(raw)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}
}

func seedTokenUsagePrereqs(t *testing.T, db *sql.DB) {
	t.Helper()

	mustTokenExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('governance', 'Governance', 1, '2026-03-01T12:00:00Z')`)
	mustTokenExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('knight', 'Knight', 0, '2026-03-01T12:00:00Z')`)
	mustTokenExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_governance', 'governance', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustTokenExec(t, db, `INSERT INTO work_orders (work_order_id, work_order_title, work_order_text, metadata_json, created_at) VALUES ('work_order_1', 'D1', 'text', '{}', '2026-03-01T12:00:00Z')`)
	mustTokenExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_1', 'work-order', 'v1', '{}', '2026-03-01T12:00:00Z')`)

	runs := []string{"run_tok_1", "run_tok_2", "run_tok_3", "run_tok_4", "run_tok_5a", "run_tok_5b"}
	for _, runID := range runs {
		mustTokenExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('`+runID+`', NULL, 'work_order_1', 'wfdef_1', 'running', 'draft', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
		mustTokenExec(t, db, `INSERT INTO node_runs (node_run_id, run_id, node_id, attempt, wave, node_status, required_role, agent_id, row_version, started_at, finished_at) VALUES ('`+runID+`_nr_1', '`+runID+`', 'node_1', 1,1, 'running', 'governance', 'agent_governance', 1, '2026-03-01T12:00:00Z', NULL)`)
	}
}

func mustTokenExec(t *testing.T, db *sql.DB, query string) {
	t.Helper()

	if _, err := db.Exec(query); err != nil {
		t.Fatalf("exec failed: %v\nquery: %s", err, query)
	}
}
