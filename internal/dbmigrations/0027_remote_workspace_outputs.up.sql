CREATE TABLE IF NOT EXISTS remote_workspace_output_intents (
 operation_id TEXT PRIMARY KEY REFERENCES remote_workspace_downloads(operation_id),
 allocation INTEGER NOT NULL REFERENCES remote_runtimes(allocation),
 purpose TEXT NOT NULL CHECK(purpose IN ('run','planning')),
 archive_sha256 TEXT NOT NULL,
 manifest_sha256 TEXT NOT NULL,
 snapshot_id TEXT NOT NULL UNIQUE,
 run_id TEXT NOT NULL,
 created_at TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS remote_workspace_output_intent_immutable BEFORE UPDATE ON remote_workspace_output_intents
BEGIN SELECT RAISE(ABORT,'remote output intent is immutable'); END;
CREATE TRIGGER IF NOT EXISTS remote_workspace_output_intent_retained BEFORE DELETE ON remote_workspace_output_intents
BEGIN SELECT RAISE(ABORT,'remote output intent must be retained'); END;
CREATE TABLE IF NOT EXISTS remote_workspace_output_applications (
 operation_id TEXT PRIMARY KEY REFERENCES remote_workspace_output_intents(operation_id),
 snapshot_id TEXT REFERENCES filesystem_snapshots(snapshot_id),
 applied_at TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS remote_workspace_output_application_immutable BEFORE UPDATE ON remote_workspace_output_applications
BEGIN SELECT RAISE(ABORT,'remote output application is immutable'); END;
CREATE TRIGGER IF NOT EXISTS remote_workspace_output_application_retained BEFORE DELETE ON remote_workspace_output_applications
BEGIN SELECT RAISE(ABORT,'remote output application must be retained'); END;
