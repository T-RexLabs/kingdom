-- Request pins contain digests and identity metadata, never argv, input, or credentials.
CREATE TABLE IF NOT EXISTS managed_executions (
 operation_id TEXT PRIMARY KEY REFERENCES remote_operations(operation_id),
 purpose TEXT NOT NULL CHECK(purpose IN ('provider_server','tool','workspace_export')),
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
