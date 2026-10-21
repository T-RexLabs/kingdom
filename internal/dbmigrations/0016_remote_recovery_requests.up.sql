-- Rebuild the purpose constraint while preserving all retained evidence.
CREATE TEMP TABLE recovery_saved_managed_executions AS SELECT * FROM managed_executions;
CREATE TEMP TABLE recovery_saved_managed_execution_identities AS SELECT * FROM managed_execution_identities;
CREATE TEMP TABLE recovery_saved_managed_execution_output AS SELECT * FROM managed_execution_output;
CREATE TEMP TABLE recovery_saved_managed_execution_receipts AS SELECT * FROM managed_execution_receipts;
DROP TABLE managed_execution_receipts;
DROP TABLE managed_execution_output;
DROP TABLE managed_execution_identities;
DROP TABLE managed_executions;
-- Request pins contain digests and identity metadata, never argv, input, or credentials.
CREATE TABLE IF NOT EXISTS managed_executions (
 operation_id TEXT PRIMARY KEY REFERENCES remote_operations(operation_id),
 purpose TEXT NOT NULL CHECK(purpose IN ('provider_server','tool','workspace_import','workspace_export')),
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
INSERT INTO managed_executions SELECT * FROM recovery_saved_managed_executions;
DROP TABLE recovery_saved_managed_executions;
INSERT INTO managed_execution_identities SELECT * FROM recovery_saved_managed_execution_identities;
DROP TABLE recovery_saved_managed_execution_identities;
INSERT INTO managed_execution_output SELECT * FROM recovery_saved_managed_execution_output;
DROP TABLE recovery_saved_managed_execution_output;
INSERT INTO managed_execution_receipts SELECT * FROM recovery_saved_managed_execution_receipts;
DROP TABLE recovery_saved_managed_execution_receipts;

-- Full replay requests are authenticated ciphertext. Keys are operator-owned.
CREATE TABLE IF NOT EXISTS remote_recovery_requests (
 operation_id TEXT PRIMARY KEY REFERENCES remote_operations(operation_id),
 schema_version INTEGER NOT NULL CHECK(schema_version=1),
 ciphertext BLOB NOT NULL CHECK(length(ciphertext)>28 AND length(ciphertext)<=2097180)
);
CREATE TRIGGER IF NOT EXISTS remote_recovery_request_immutable BEFORE UPDATE ON remote_recovery_requests BEGIN SELECT RAISE(ABORT,'remote recovery request is immutable'); END;
CREATE TRIGGER IF NOT EXISTS remote_recovery_request_retained BEFORE DELETE ON remote_recovery_requests BEGIN SELECT RAISE(ABORT,'remote recovery request must be retained'); END;

-- Allocate before deriving a native name or canonical request digest.
CREATE TABLE IF NOT EXISTS remote_allocations (allocation INTEGER PRIMARY KEY AUTOINCREMENT);
INSERT OR IGNORE INTO remote_allocations(allocation) SELECT allocation FROM remote_runtimes;
INSERT OR IGNORE INTO remote_allocations(allocation)
 SELECT seq FROM sqlite_sequence WHERE name='remote_runtimes' AND seq>0;
CREATE TRIGGER IF NOT EXISTS remote_allocation_immutable BEFORE UPDATE ON remote_allocations BEGIN SELECT RAISE(ABORT,'remote allocations are immutable'); END;
CREATE TRIGGER IF NOT EXISTS remote_allocation_retained BEFORE DELETE ON remote_allocations BEGIN SELECT RAISE(ABORT,'remote allocations must be retained'); END;
