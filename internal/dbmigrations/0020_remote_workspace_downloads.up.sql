CREATE TABLE IF NOT EXISTS remote_workspace_downloads (
 operation_id TEXT PRIMARY KEY REFERENCES managed_execution_receipts(operation_id),
 purpose TEXT NOT NULL CHECK(purpose IN ('workspace_export','native_receipts')),
 spool_name TEXT NOT NULL UNIQUE,
 archive_sha256 TEXT NOT NULL CHECK(length(archive_sha256)=64),
 archive_bytes INTEGER NOT NULL CHECK(archive_bytes>0),
 manifest_sha256 TEXT NOT NULL CHECK(length(manifest_sha256)=64),
 receipt_sha256 BLOB NOT NULL CHECK(length(receipt_sha256)=32)
);
CREATE TRIGGER IF NOT EXISTS remote_workspace_download_immutable BEFORE UPDATE ON remote_workspace_downloads BEGIN SELECT RAISE(ABORT,'workspace download is immutable'); END;
CREATE TRIGGER IF NOT EXISTS remote_workspace_download_retained BEFORE DELETE ON remote_workspace_downloads BEGIN SELECT RAISE(ABORT,'workspace download must be retained'); END;
