CREATE TABLE IF NOT EXISTS remote_workspace_imports (
 operation_id TEXT PRIMARY KEY REFERENCES managed_execution_receipts(operation_id),
 archive_sha256 TEXT NOT NULL CHECK(length(archive_sha256)=64),
 archive_bytes INTEGER NOT NULL CHECK(archive_bytes>0),
 manifest_sha256 TEXT NOT NULL CHECK(length(manifest_sha256)=64),
 receipt_sha256 BLOB NOT NULL CHECK(length(receipt_sha256)=32)
);
CREATE TRIGGER IF NOT EXISTS remote_workspace_import_immutable BEFORE UPDATE ON remote_workspace_imports BEGIN SELECT RAISE(ABORT,'workspace import verification is immutable'); END;
CREATE TRIGGER IF NOT EXISTS remote_workspace_import_retained BEFORE DELETE ON remote_workspace_imports BEGIN SELECT RAISE(ABORT,'workspace import verification must be retained'); END;
