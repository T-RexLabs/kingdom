-- Rebuild the purpose constraint while preserving all retained evidence.
CREATE TEMP TABLE receipts_saved_managed_executions AS SELECT * FROM managed_executions;
CREATE TEMP TABLE receipts_saved_managed_execution_identities AS SELECT * FROM managed_execution_identities;
CREATE TEMP TABLE receipts_saved_managed_execution_output AS SELECT * FROM managed_execution_output;
CREATE TEMP TABLE receipts_saved_managed_execution_receipts AS SELECT * FROM managed_execution_receipts;
CREATE TEMP TABLE receipts_saved_imports AS SELECT * FROM remote_workspace_imports;
DROP TABLE remote_workspace_imports;
DROP TABLE managed_execution_receipts;
DROP TABLE managed_execution_output;
DROP TABLE managed_execution_identities;
DROP TABLE managed_executions;
-- Request pins contain digests and identity metadata, never argv, input, or credentials.
CREATE TABLE IF NOT EXISTS managed_executions (
 operation_id TEXT PRIMARY KEY REFERENCES remote_operations(operation_id),
 purpose TEXT NOT NULL CHECK(purpose IN ('provider_server','tool','workspace_import','workspace_export','native_receipts')),
 subject_id TEXT NOT NULL,
 output_limit INTEGER NOT NULL CHECK(output_limit>0 AND output_limit<=16777216),
 runtime BLOB NOT NULL,
 request_sha256 BLOB NOT NULL CHECK(length(request_sha256)=32),
 effective_sha256 BLOB NOT NULL CHECK(length(effective_sha256)=32)
);
CREATE TABLE IF NOT EXISTS managed_execution_identities (
 operation_id TEXT PRIMARY KEY REFERENCES managed_executions(operation_id),
 identity BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS managed_execution_output (
 operation_id TEXT NOT NULL REFERENCES managed_execution_identities(operation_id),
 sequence INTEGER NOT NULL CHECK(sequence>0 AND sequence<=4096),
 chunk BLOB NOT NULL,
 data_bytes INTEGER NOT NULL CHECK(data_bytes>0 AND data_bytes<=65536),
 PRIMARY KEY(operation_id,sequence)
);
CREATE TABLE IF NOT EXISTS managed_execution_receipts (
 operation_id TEXT PRIMARY KEY REFERENCES managed_execution_identities(operation_id),
 receipt BLOB NOT NULL
);
CREATE TRIGGER IF NOT EXISTS managed_execution_pins_immutable BEFORE UPDATE ON managed_executions BEGIN SELECT RAISE(ABORT,'managed execution pins are immutable'); END;
CREATE TRIGGER IF NOT EXISTS managed_execution_pins_retained BEFORE DELETE ON managed_executions BEGIN SELECT RAISE(ABORT,'managed execution pins must be retained'); END;
CREATE TRIGGER IF NOT EXISTS managed_identity_immutable BEFORE UPDATE ON managed_execution_identities BEGIN SELECT RAISE(ABORT,'managed identity is immutable'); END;
CREATE TRIGGER IF NOT EXISTS managed_identity_retained BEFORE DELETE ON managed_execution_identities BEGIN SELECT RAISE(ABORT,'managed identity must be retained'); END;
CREATE TRIGGER IF NOT EXISTS managed_output_immutable BEFORE UPDATE ON managed_execution_output BEGIN SELECT RAISE(ABORT,'managed output is immutable'); END;
CREATE TRIGGER IF NOT EXISTS managed_output_retained BEFORE DELETE ON managed_execution_output BEGIN SELECT RAISE(ABORT,'managed output must be retained'); END;
CREATE TRIGGER IF NOT EXISTS managed_receipt_immutable BEFORE UPDATE ON managed_execution_receipts BEGIN SELECT RAISE(ABORT,'managed receipt is immutable'); END;
CREATE TRIGGER IF NOT EXISTS managed_receipt_retained BEFORE DELETE ON managed_execution_receipts BEGIN SELECT RAISE(ABORT,'managed receipt must be retained'); END;
INSERT INTO managed_executions SELECT * FROM receipts_saved_managed_executions;
DROP TABLE receipts_saved_managed_executions;
INSERT INTO managed_execution_identities SELECT * FROM receipts_saved_managed_execution_identities;
DROP TABLE receipts_saved_managed_execution_identities;
INSERT INTO managed_execution_output SELECT * FROM receipts_saved_managed_execution_output;
DROP TABLE receipts_saved_managed_execution_output;
INSERT INTO managed_execution_receipts SELECT * FROM receipts_saved_managed_execution_receipts;
DROP TABLE receipts_saved_managed_execution_receipts;

CREATE TABLE IF NOT EXISTS remote_workspace_imports (
 operation_id TEXT PRIMARY KEY REFERENCES managed_execution_receipts(operation_id),
 archive_sha256 TEXT NOT NULL CHECK(length(archive_sha256)=64),
 archive_bytes INTEGER NOT NULL CHECK(archive_bytes>0),
 manifest_sha256 TEXT NOT NULL CHECK(length(manifest_sha256)=64),
 receipt_sha256 BLOB NOT NULL CHECK(length(receipt_sha256)=32)
);
CREATE TRIGGER IF NOT EXISTS remote_workspace_import_immutable BEFORE UPDATE ON remote_workspace_imports BEGIN SELECT RAISE(ABORT,'workspace import verification is immutable'); END;
CREATE TRIGGER IF NOT EXISTS remote_workspace_import_retained BEFORE DELETE ON remote_workspace_imports BEGIN SELECT RAISE(ABORT,'workspace import verification must be retained'); END;

INSERT INTO remote_workspace_imports SELECT * FROM receipts_saved_imports;
DROP TABLE receipts_saved_imports;

-- A durable one-shot capability issuance intent precedes both broker calls.
-- An incomplete intent cannot issue again after restart; revoke its exact binding.
CREATE TABLE IF NOT EXISTS remote_provider_launches (
 allocation INTEGER PRIMARY KEY REFERENCES remote_runtimes(allocation),
 operation_id TEXT NOT NULL UNIQUE,
 model_lease_id TEXT NOT NULL DEFAULT '',
 plugin_lease_id TEXT NOT NULL DEFAULT ''
);
CREATE TRIGGER IF NOT EXISTS remote_provider_launch_identity_immutable BEFORE UPDATE ON remote_provider_launches
WHEN NEW.allocation IS NOT OLD.allocation OR NEW.operation_id IS NOT OLD.operation_id OR
 (OLD.model_lease_id<>'' AND NEW.model_lease_id IS NOT OLD.model_lease_id) OR
 (OLD.plugin_lease_id<>'' AND NEW.plugin_lease_id IS NOT OLD.plugin_lease_id)
BEGIN SELECT RAISE(ABORT,'remote provider launch identity is immutable'); END;
CREATE TRIGGER IF NOT EXISTS remote_provider_launch_retained BEFORE DELETE ON remote_provider_launches BEGIN SELECT RAISE(ABORT,'remote provider launch intent must be retained'); END;

-- This is a local proof of no dispatch, never a synthetic native receipt.
CREATE TABLE IF NOT EXISTS remote_unstarted_executions (
 operation_id TEXT PRIMARY KEY REFERENCES remote_operations(operation_id)
);
CREATE TRIGGER IF NOT EXISTS remote_unstarted_immutable BEFORE UPDATE ON remote_unstarted_executions BEGIN SELECT RAISE(ABORT,'no-dispatch proof is immutable'); END;
CREATE TRIGGER IF NOT EXISTS remote_unstarted_retained BEFORE DELETE ON remote_unstarted_executions BEGIN SELECT RAISE(ABORT,'no-dispatch proof must be retained'); END;
CREATE TRIGGER IF NOT EXISTS remote_unstarted_dispatch_denied BEFORE UPDATE OF delivery ON remote_operations
WHEN EXISTS(SELECT 1 FROM remote_unstarted_executions WHERE operation_id=OLD.operation_id) AND NEW.delivery IS NOT OLD.delivery
BEGIN SELECT RAISE(ABORT,'execution is proven not dispatched'); END;
