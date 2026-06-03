package workspace

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"

	"kingdom/internal/dbmigrations"
)

func TestWorkspaceManagerCreateAndRelease(t *testing.T) {
	t.Parallel()

	db := openWorkspaceTestDB(t)
	applyWorkspaceMigration(t, db)
	seedWorkspaceRun(t, db)

	root := t.TempDir()
	manager, err := NewManager(db, root)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	workspace, err := manager.CreateWorkspace(context.Background(), CreateWorkspaceRequest{
		RunID:       "run_1",
		WorkspaceID: "ws_test_1",
		CreatedAt:   "2026-03-01T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if workspace.Status != "active" {
		t.Fatalf("expected active workspace status, got %q", workspace.Status)
	}
	if _, err := os.Stat(workspace.WorkspacePath); err != nil {
		t.Fatalf("expected workspace directory to exist: %v", err)
	}

	if err := manager.ReleaseWorkspace(context.Background(), ReleaseWorkspaceRequest{
		WorkspaceID: workspace.WorkspaceID,
		ReleasedAt:  "2026-03-01T12:01:00Z",
		Cleanup:     true,
	}); err != nil {
		t.Fatalf("release workspace: %v", err)
	}

	if _, err := os.Stat(workspace.WorkspacePath); !os.IsNotExist(err) {
		t.Fatalf("expected workspace directory cleanup, stat err=%v", err)
	}

	var status string
	if err := db.QueryRow(`SELECT status FROM workspaces WHERE workspace_id = ?`, workspace.WorkspaceID).Scan(&status); err != nil {
		t.Fatalf("query workspace status: %v", err)
	}
	if status != "released" {
		t.Fatalf("expected released status, got %q", status)
	}
}

func TestWorkspaceManagerCreateWorkspaceResetsExistingDirectory(t *testing.T) {
	t.Parallel()

	db := openWorkspaceTestDB(t)
	applyWorkspaceMigration(t, db)
	seedWorkspaceRun(t, db)

	root := t.TempDir()
	staleDir := filepath.Join(root, "ws_reset_1")
	if err := os.MkdirAll(staleDir, 0o755); err != nil {
		t.Fatalf("create stale workspace dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(staleDir, "stale.txt"), []byte("old"), 0o644); err != nil {
		t.Fatalf("write stale artifact: %v", err)
	}

	manager, err := NewManager(db, root)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	workspace, err := manager.CreateWorkspace(context.Background(), CreateWorkspaceRequest{
		RunID:       "run_1",
		WorkspaceID: "ws_reset_1",
		CreatedAt:   "2026-03-01T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if _, err := os.Stat(filepath.Join(workspace.WorkspacePath, "stale.txt")); !os.IsNotExist(err) {
		t.Fatalf("expected stale file to be removed when workspace is recreated, stat err=%v", err)
	}
}

func TestWorkspaceManagerCreateWorkspaceDuplicateIDDoesNotResetExistingDirectory(t *testing.T) {
	t.Parallel()

	db := openWorkspaceTestDB(t)
	applyWorkspaceMigration(t, db)
	seedWorkspaceRun(t, db)

	root := t.TempDir()
	manager, err := NewManager(db, root)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	workspace, err := manager.CreateWorkspace(context.Background(), CreateWorkspaceRequest{
		RunID:       "run_1",
		WorkspaceID: "ws_duplicate_1",
		CreatedAt:   "2026-03-01T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	sentinelPath := filepath.Join(workspace.WorkspacePath, "sentinel.txt")
	if err := os.WriteFile(sentinelPath, []byte("keep me"), 0o644); err != nil {
		t.Fatalf("write sentinel file: %v", err)
	}

	_, err = manager.CreateWorkspace(context.Background(), CreateWorkspaceRequest{
		RunID:       "run_1",
		WorkspaceID: "ws_duplicate_1",
		CreatedAt:   "2026-03-01T12:01:00Z",
	})
	if err == nil {
		t.Fatal("expected duplicate workspace create to fail")
	}

	if _, err := os.Stat(sentinelPath); err != nil {
		t.Fatalf("expected duplicate create failure to preserve existing workspace contents: %v", err)
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(1) FROM workspaces WHERE workspace_id = 'ws_duplicate_1'`).Scan(&count); err != nil {
		t.Fatalf("query workspace row count: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected one workspace row after duplicate create failure, got %d", count)
	}
}

func TestWorkspaceManagerReleaseWorkspaceCleanupFailureDoesNotMarkReleased(t *testing.T) {
	t.Parallel()

	db := openWorkspaceTestDB(t)
	applyWorkspaceMigration(t, db)
	seedWorkspaceRun(t, db)

	root := t.TempDir()
	manager, err := NewManager(db, root)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	workspace, err := manager.CreateWorkspace(context.Background(), CreateWorkspaceRequest{
		RunID:       "run_1",
		WorkspaceID: "ws_cleanup_fail_1",
		CreatedAt:   "2026-03-01T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	rootInfo, err := os.Stat(root)
	if err != nil {
		t.Fatalf("stat workspace root: %v", err)
	}
	if err := os.Chmod(root, 0o555); err != nil {
		t.Fatalf("make workspace root read-only: %v", err)
	}
	defer func() {
		_ = os.Chmod(root, rootInfo.Mode().Perm())
	}()

	err = manager.ReleaseWorkspace(context.Background(), ReleaseWorkspaceRequest{
		WorkspaceID: workspace.WorkspaceID,
		ReleasedAt:  "2026-03-01T12:01:00Z",
		Cleanup:     true,
	})
	if err == nil {
		t.Fatal("expected cleanup failure when workspace root is read-only")
	}

	var status string
	if err := db.QueryRow(`SELECT status FROM workspaces WHERE workspace_id = ?`, workspace.WorkspaceID).Scan(&status); err != nil {
		t.Fatalf("query workspace status: %v", err)
	}
	if status != "active" {
		t.Fatalf("expected workspace status to remain active after cleanup failure, got %q", status)
	}

	if _, err := os.Stat(workspace.WorkspacePath); err != nil {
		t.Fatalf("expected workspace directory to remain after cleanup failure: %v", err)
	}
}

func TestWorkspaceManagerReleaseWorkspaceWithoutCleanupPreservesDirectory(t *testing.T) {
	t.Parallel()

	db := openWorkspaceTestDB(t)
	applyWorkspaceMigration(t, db)
	seedWorkspaceRun(t, db)

	root := t.TempDir()
	manager, err := NewManager(db, root)
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	workspace, err := manager.CreateWorkspace(context.Background(), CreateWorkspaceRequest{
		RunID:       "run_1",
		WorkspaceID: "ws_preserve_1",
		CreatedAt:   "2026-03-01T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}

	if err := manager.ReleaseWorkspace(context.Background(), ReleaseWorkspaceRequest{
		WorkspaceID: workspace.WorkspaceID,
		ReleasedAt:  "2026-03-01T12:01:00Z",
		Cleanup:     false,
	}); err != nil {
		t.Fatalf("release workspace without cleanup: %v", err)
	}

	if _, err := os.Stat(workspace.WorkspacePath); err != nil {
		t.Fatalf("expected workspace directory to remain after non-cleanup release: %v", err)
	}

	var status string
	if err := db.QueryRow(`SELECT status FROM workspaces WHERE workspace_id = ?`, workspace.WorkspaceID).Scan(&status); err != nil {
		t.Fatalf("query workspace status: %v", err)
	}
	if status != "released" {
		t.Fatalf("expected released status without cleanup, got %q", status)
	}
}

func openWorkspaceTestDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatalf("enable fk: %v", err)
	}

	return db
}

func applyWorkspaceMigration(t *testing.T, db *sql.DB) {
	t.Helper()

	raw, err := dbmigrations.Read("0001_initial_schema.up.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	if _, err := db.Exec(string(raw)); err != nil {
		t.Fatalf("apply migration: %v", err)
	}
}

func seedWorkspaceRun(t *testing.T, db *sql.DB) {
	t.Helper()

	mustExec(t, db, `INSERT INTO roles (role_id, display_name, can_spawn, created_at) VALUES ('king', 'King', 1, '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO agents (agent_id, role_id, status, parent_agent_id, spawn_depth, created_at, updated_at) VALUES ('agent_king', 'king', 'idle', NULL, 0, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_1', 'd', 'd', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_1', 'wf', 'v1', '{}', '2026-03-01T12:00:00Z')`)
	mustExec(t, db, `INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('run_1', NULL, 'decree_1', 'wfdef_1', 'running', 'draft', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`)
}

func mustExec(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("exec failed: %v\nquery: %s", err, query)
	}
}
