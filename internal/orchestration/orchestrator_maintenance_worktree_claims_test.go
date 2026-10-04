package orchestration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"kingdom/internal/processguardian"
	"kingdom/internal/workspace"
)

func TestMaintenanceWorktreeClaimRecoveryIsBoundedAndCyclesPastLiveOwners(t *testing.T) {
	db := openMigratedTestDB(t)
	seedMinimumGraph(t, db)
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	identity, err := processguardian.ProcessIdentityToken(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	addClaim := func(index int, token string) string {
		t.Helper()
		runID := fmt.Sprintf("claimed_%03d", index)
		if _, err := db.Exec(`INSERT INTO workflow_runs
			(run_id, work_order_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at)
			SELECT ?, work_order_id, workflow_definition_id, 'failed', plan_state, 1, created_at, updated_at FROM workflow_runs WHERE run_id = 'run_1'`, runID); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO run_worktrees
			(run_id, repo_common_dir, main_worktree_root, project_relpath, worktree_name, seed_commit_oid, state,
			 created_at, last_used_at, cleanup_owner, cleanup_owner_pid, cleanup_owner_identity_token, cleanup_fence, cleanup_started, cleanup_cursor)
			VALUES (?, '/repo/.git', '/repo', '.', ?, 'seed', 'cleanup_pending', ?, ?, ?, ?, ?, 7, 1, 'saved-progress')`,
			runID, runID, now.Format(time.RFC3339), now.Format(time.RFC3339), "owner_"+runID, os.Getpid(), token); err != nil {
			t.Fatal(err)
		}
		return runID
	}
	for i := 1; i <= runWorktreeCleanupBatchSize; i++ {
		addClaim(i, identity)
	}
	abandoned := addClaim(runWorktreeCleanupBatchSize+1, "previous-process-generation")
	store := workspace.NewRunWorktreeStore(db)
	loop := orchestrationLoop{db: db, runWorktrees: store, now: func() time.Time { return now }}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pass := func() {
		t.Helper()
		if admitted, err := loop.scheduleWorktreeClaimRecovery(ctx); err != nil || admitted != 1 {
			t.Fatalf("schedule claim inspection: admitted=%d err=%v", admitted, err)
		}
		if err := loop.waitForMaintenance(ctx); err != nil {
			t.Fatal(err)
		}
	}
	read := func(runID string) workspace.RunWorktreeRecord {
		t.Helper()
		record, err := store.GetByRun(ctx, runID)
		if err != nil {
			t.Fatal(err)
		}
		return record
	}
	pass()
	if got := read(abandoned); got.CleanupOwner == "" {
		t.Fatal("one inspection batch visited more than its configured limit")
	}
	loop.execMu.Lock()
	cursor := loop.maintenance.worktreeClaims
	loop.execMu.Unlock()
	if cursor.AfterRunID != fmt.Sprintf("claimed_%03d", runWorktreeCleanupBatchSize) || cursor.ThroughRunID != abandoned {
		t.Fatalf("first batch did not advance past live owners with a fixed upper boundary: %#v", cursor)
	}
	newClaim := addClaim(runWorktreeCleanupBatchSize+2, "previous-process-generation")
	pass()
	got := read(abandoned)
	if got.CleanupOwner != "" || got.CleanupFence != 8 || !got.CleanupStarted || got.CleanupCursor != "saved-progress" || got.CleanupAfter.String != now.Format(time.RFC3339) {
		t.Fatalf("abandoned owner was not fenced without losing deletion progress: %#v", got)
	}
	if got := read(newClaim); got.CleanupOwner == "" {
		t.Fatal("claim beyond the captured upper boundary entered the current cycle")
	}
	pass()
	pass()
	if got := read(newClaim); got.CleanupOwner != "" {
		t.Fatal("next inspection cycle did not reach the new abandoned owner")
	}
	if got := read("claimed_001"); got.CleanupOwner != "owner_claimed_001" || got.CleanupFence != 7 {
		t.Fatalf("periodic inspection changed a live process's claim: %#v", got)
	}

	rows, err := db.Query(`EXPLAIN QUERY PLAN SELECT run_id, cleanup_owner, cleanup_fence, cleanup_owner_pid, cleanup_owner_identity_token
		FROM run_worktrees WHERE cleanup_owner <> '' AND run_id > ? AND run_id <= ? ORDER BY run_id LIMIT ?`, "claimed_001", newClaim, 20)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan.String(), "idx_run_worktrees_claimed_run (run_id>? AND run_id<?)") || strings.Contains(plan.String(), "TEMP B-TREE") {
		t.Fatalf("claim inspection must seek only claimed rows without sorting: %s", plan.String())
	}
}
