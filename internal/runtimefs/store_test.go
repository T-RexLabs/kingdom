package runtimefs

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"kingdom/internal/dbmigrations"
	"kingdom/internal/state"

	_ "modernc.org/sqlite"
)

func TestCaptureRestoreAndDiffSnapshots(t *testing.T) {
	t.Parallel()

	db := openRuntimeFSTestDB(t)
	applyRuntimeFSMigrations(t, db)
	store, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	seedRuntimeFSRun(t, db, "run_1")

	root := t.TempDir()
	writeTestFile(t, root, "src/app.js", "console.log('v1')")
	writeTestFile(t, root, "docs/readme.md", "hello")

	base, err := store.CaptureSnapshot(context.Background(), CaptureSnapshotRequest{
		SnapshotID:   "snap_base",
		RunID:        "run_1",
		SnapshotKind: "baseline",
		RootPath:     root,
		CreatedAt:    "2026-03-01T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("capture base snapshot: %v", err)
	}

	writeTestFile(t, root, "src/app.js", "console.log('v2')")
	if err := os.Remove(filepath.Join(root, "docs", "readme.md")); err != nil {
		t.Fatalf("remove tracked file: %v", err)
	}
	writeTestFile(t, root, "src/new.js", "export const value = 1")

	result, err := store.CaptureSnapshot(context.Background(), CaptureSnapshotRequest{
		SnapshotID:       "snap_result",
		RunID:            "run_1",
		ParentSnapshotID: base.SnapshotID,
		SnapshotKind:     "attempt_result",
		RootPath:         root,
		CreatedAt:        "2026-03-01T12:01:00Z",
	})
	if err != nil {
		t.Fatalf("capture result snapshot: %v", err)
	}

	diffs, err := store.DiffSnapshots(context.Background(), base.SnapshotID, result.SnapshotID)
	if err != nil {
		t.Fatalf("diff snapshots: %v", err)
	}
	if len(diffs) != 3 {
		t.Fatalf("expected 3 diff entries, got %d", len(diffs))
	}
	if diffs[0].Path != "docs/readme.md" || diffs[0].EntryType != "delete" {
		t.Fatalf("unexpected delete diff %#v", diffs[0])
	}
	if diffs[1].Path != "src/app.js" || diffs[1].EntryType != "file" {
		t.Fatalf("unexpected update diff %#v", diffs[1])
	}
	if diffs[2].Path != "src/new.js" || diffs[2].EntryType != "file" {
		t.Fatalf("unexpected create diff %#v", diffs[2])
	}

	restoreRoot := t.TempDir()
	if err := store.RestoreSnapshot(context.Background(), result.SnapshotID, restoreRoot); err != nil {
		t.Fatalf("restore snapshot: %v", err)
	}
	assertTestFile(t, restoreRoot, "src/app.js", "console.log('v2')")
	assertTestFile(t, restoreRoot, "src/new.js", "export const value = 1")
	if _, err := os.Stat(filepath.Join(restoreRoot, "docs", "readme.md")); !os.IsNotExist(err) {
		t.Fatalf("expected deleted file to stay deleted, got err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(restoreRoot, ".kingdom")); !os.IsNotExist(err) {
		t.Fatalf("expected protected directory to remain untouched in restore target, got err=%v", err)
	}
}

func TestResolveSnapshotManifestDropsNewlyProtectedHistoricalPaths(t *testing.T) {
	t.Parallel()

	db := openRuntimeFSTestDB(t)
	applyRuntimeFSMigrations(t, db)
	store, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	seedRuntimeFSRun(t, db, "run_protected_history")
	mustRuntimeFSExec(t, db, `INSERT INTO filesystem_snapshots (snapshot_id, run_id, snapshot_kind, created_at) VALUES ('snap_protected_history', 'run_protected_history', 'baseline', '2026-03-01T12:00:00Z')`)
	mustRuntimeFSExec(t, db, `INSERT INTO filesystem_blobs (blob_id, size_bytes, created_at) VALUES ('missing_protected_blob', 10, '2026-03-01T12:00:00Z')`)
	mustRuntimeFSExec(t, db, `INSERT INTO filesystem_snapshot_entries (snapshot_id, path, entry_type, blob_id, size_bytes, mode_bits, mtime_ns) VALUES ('snap_protected_history', '.playwright-mcp/page.yml', 'file', 'missing_protected_blob', 10, 420, 0)`)

	manifest, err := store.ResolveSnapshotManifest(context.Background(), "snap_protected_history")
	if err != nil {
		t.Fatalf("resolve protected historical manifest: %v", err)
	}
	if len(manifest) != 0 {
		t.Fatalf("expected protected historical path to be removed, got %#v", manifest)
	}
	if err := store.RestoreSnapshot(context.Background(), "snap_protected_history", t.TempDir()); err != nil {
		t.Fatalf("restore should not read a protected historical blob: %v", err)
	}
}

func TestReadBlobFallsBackToLegacyStore(t *testing.T) {
	t.Parallel()

	db := openRuntimeFSTestDB(t)
	applyRuntimeFSMigrations(t, db)
	legacyRoot := t.TempDir()
	legacyStore, err := NewStore(db, legacyRoot)
	if err != nil {
		t.Fatalf("new legacy runtime fs store: %v", err)
	}
	seedRuntimeFSRun(t, db, "run_legacy_blob")
	sourceRoot := t.TempDir()
	writeTestFile(t, sourceRoot, "src/app.txt", "legacy snapshot content")
	if _, err := legacyStore.CaptureSnapshot(context.Background(), CaptureSnapshotRequest{
		SnapshotID:   "snap_legacy_blob",
		RunID:        "run_legacy_blob",
		SnapshotKind: "baseline",
		RootPath:     sourceRoot,
		CreatedAt:    "2026-03-01T12:00:00Z",
	}); err != nil {
		t.Fatalf("capture legacy snapshot: %v", err)
	}

	durableStore, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new durable runtime fs store: %v", err)
	}
	if err := durableStore.AddBlobFallbackRoot(legacyRoot); err != nil {
		t.Fatalf("add legacy blob fallback: %v", err)
	}
	restoreRoot := t.TempDir()
	if err := durableStore.RestoreSnapshot(context.Background(), "snap_legacy_blob", restoreRoot); err != nil {
		t.Fatalf("restore snapshot through legacy blob fallback: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(restoreRoot, "src", "app.txt"))
	if err != nil {
		t.Fatalf("read restored legacy file: %v", err)
	}
	if string(raw) != "legacy snapshot content" {
		t.Fatalf("unexpected restored legacy content %q", string(raw))
	}
}

func TestCountChangesAgainstSnapshotIgnoresProtectedRoots(t *testing.T) {
	t.Parallel()

	db := openRuntimeFSTestDB(t)
	applyRuntimeFSMigrations(t, db)
	store, err := NewStore(db, t.TempDir())
	if err != nil {
		t.Fatalf("new runtime fs store: %v", err)
	}
	seedRuntimeFSRun(t, db, "run_1")

	root := t.TempDir()
	writeTestFile(t, root, "src/app.js", "console.log('v1')")
	base, err := store.CaptureSnapshot(context.Background(), CaptureSnapshotRequest{
		SnapshotID:   "snap_base",
		RunID:        "run_1",
		SnapshotKind: "baseline",
		RootPath:     root,
		CreatedAt:    "2026-03-01T12:00:00Z",
	})
	if err != nil {
		t.Fatalf("capture base snapshot: %v", err)
	}
	writeTestFile(t, root, ".kingdom/provider-completion/receipts/x/receipt.json", "ignored")
	changes, err := store.CountChangesAgainstSnapshot(context.Background(), root, base.SnapshotID)
	if err != nil {
		t.Fatalf("count changes: %v", err)
	}
	if changes != 0 {
		t.Fatalf("expected protected runtime paths to be ignored, got %d changes", changes)
	}
}

func openRuntimeFSTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("enable foreign keys: %v", err)
	}
	return db
}

func applyRuntimeFSMigrations(t *testing.T, db *sql.DB) {
	t.Helper()
	entries, err := dbmigrations.List()
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}
	migrations := make([]state.SQLMigration, 0, len(entries))
	for _, name := range entries {
		if name < "0001_initial_schema.up.sql" || filepath.Ext(name) != ".sql" {
			continue
		}
		raw, err := dbmigrations.Read(name)
		if err != nil {
			t.Fatalf("read migration %s: %v", name, err)
		}
		migrations = append(migrations, state.SQLMigration{Name: name, SQL: raw})
	}
	if err := state.ApplySQLMigrations(context.Background(), db, migrations, time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}
}

func mustRuntimeFSExec(t *testing.T, db *sql.DB, statement string) {
	t.Helper()
	if _, err := db.Exec(statement); err != nil {
		t.Fatalf("execute runtime fs test statement: %v", err)
	}
}

func writeTestFile(t *testing.T, root, relativePath, body string) {
	t.Helper()
	resolved := filepath.Join(root, filepath.FromSlash(relativePath))
	if err := os.MkdirAll(filepath.Dir(resolved), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", relativePath, err)
	}
	if err := os.WriteFile(resolved, []byte(body), 0o644); err != nil {
		t.Fatalf("write %s: %v", relativePath, err)
	}
}

func assertTestFile(t *testing.T, root, relativePath, want string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(relativePath)))
	if err != nil {
		t.Fatalf("read %s: %v", relativePath, err)
	}
	if string(raw) != want {
		t.Fatalf("%s = %q, want %q", relativePath, string(raw), want)
	}
}

func seedRuntimeFSRun(t *testing.T, db *sql.DB, runID string) {
	t.Helper()
	stmts := []string{
		`INSERT INTO decrees (decree_id, decree_title, decree_text, metadata_json, created_at) VALUES ('decree_runtimefs', 'Runtime FS', 'text', '{}', '2026-03-01T12:00:00Z')`,
		`INSERT INTO workflow_definitions (workflow_definition_id, workflow_name, workflow_version, definition_json, created_at) VALUES ('wfdef_runtimefs', 'royal-decree-v1', '1', '{}', '2026-03-01T12:00:00Z')`,
		`INSERT INTO workflow_runs (run_id, campaign_id, decree_id, workflow_definition_id, run_status, plan_state, row_version, created_at, updated_at) VALUES ('` + runID + `', NULL, 'decree_runtimefs', 'wfdef_runtimefs', 'running', 'approved', 1, '2026-03-01T12:00:00Z', '2026-03-01T12:00:00Z')`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("seed runtime fs run: %v", err)
		}
	}
}
