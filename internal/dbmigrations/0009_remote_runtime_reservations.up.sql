-- Remote dispatch records are resource debt, never closure evidence.
CREATE TABLE IF NOT EXISTS remote_runtimes (
 allocation INTEGER PRIMARY KEY AUTOINCREMENT,
 creation_id TEXT NOT NULL UNIQUE,
 owner_kind TEXT NOT NULL CHECK(owner_kind IN ('run','planning')),
 invocation_id TEXT NOT NULL UNIQUE,
 binding_id TEXT NOT NULL UNIQUE,
 lease_owner TEXT NOT NULL,
 lease_version INTEGER NOT NULL CHECK(lease_version>0),
 worker_id TEXT NOT NULL,
 gateway_id TEXT NOT NULL,
 workspace_id TEXT NOT NULL,
 spec_digest TEXT NOT NULL,
 sandbox_id TEXT NOT NULL DEFAULT '',
 generation TEXT NOT NULL DEFAULT '',
 main_instance_id TEXT NOT NULL DEFAULT '',
 admission_closed INTEGER NOT NULL DEFAULT 0 CHECK(admission_closed IN (0,1)),
 CHECK((sandbox_id='' AND generation='' AND main_instance_id='') OR
       (sandbox_id<>'' AND generation<>'' AND main_instance_id<>''))
 );
 CREATE UNIQUE INDEX IF NOT EXISTS remote_runtime_sandbox ON remote_runtimes(gateway_id,workspace_id,sandbox_id) WHERE sandbox_id<>'';
 CREATE TABLE IF NOT EXISTS remote_operations (
 operation_id TEXT PRIMARY KEY,
 allocation INTEGER NOT NULL REFERENCES remote_runtimes(allocation),
 kind TEXT NOT NULL CHECK(kind IN ('create','execute','stop')),
 payload_digest TEXT NOT NULL,
 delivery TEXT NOT NULL DEFAULT 'prepared' CHECK(delivery IN ('prepared','delivery_uncertain','admitted')),
 admission_digest TEXT NOT NULL DEFAULT '',
 CHECK((delivery='admitted' AND admission_digest<>'') OR (delivery<>'admitted' AND admission_digest=''))
 );
 CREATE UNIQUE INDEX IF NOT EXISTS remote_runtime_stop ON remote_operations(allocation) WHERE kind='stop';
 CREATE TABLE IF NOT EXISTS remote_commands (
 command_id TEXT PRIMARY KEY REFERENCES remote_operations(operation_id),
 allocation INTEGER NOT NULL REFERENCES remote_runtimes(allocation),
 binding_id TEXT NOT NULL,
 claim_id TEXT NOT NULL UNIQUE,
 payload_digest TEXT NOT NULL,
 sequence INTEGER NOT NULL CHECK(sequence>0),
 UNIQUE(binding_id,sequence)
 );

CREATE TRIGGER IF NOT EXISTS remote_runtime_identity_immutable
BEFORE UPDATE OF allocation,creation_id,owner_kind,invocation_id,binding_id,lease_owner,lease_version,worker_id,gateway_id,workspace_id,spec_digest ON remote_runtimes
BEGIN SELECT RAISE(ABORT, 'remote runtime identity is immutable'); END;
CREATE TRIGGER IF NOT EXISTS remote_runtime_generation_immutable
BEFORE UPDATE OF sandbox_id,generation,main_instance_id ON remote_runtimes
WHEN OLD.sandbox_id<>'' AND (NEW.sandbox_id IS NOT OLD.sandbox_id OR NEW.generation IS NOT OLD.generation OR NEW.main_instance_id IS NOT OLD.main_instance_id)
BEGIN SELECT RAISE(ABORT, 'remote runtime generation is immutable'); END;
CREATE TRIGGER IF NOT EXISTS remote_runtime_admission_monotonic
BEFORE UPDATE OF admission_closed ON remote_runtimes
WHEN OLD.admission_closed=1 AND NEW.admission_closed<>1
BEGIN SELECT RAISE(ABORT, 'remote runtime admission cannot reopen'); END;
CREATE TRIGGER IF NOT EXISTS remote_operation_identity_immutable
BEFORE UPDATE OF operation_id,allocation,kind,payload_digest ON remote_operations
BEGIN SELECT RAISE(ABORT, 'remote operation identity is immutable'); END;
CREATE TRIGGER IF NOT EXISTS remote_operation_delivery_monotonic
BEFORE UPDATE OF delivery,admission_digest ON remote_operations
WHEN (OLD.delivery='delivery_uncertain' AND NEW.delivery='prepared') OR
 (OLD.delivery='admitted' AND (NEW.delivery IS NOT OLD.delivery OR NEW.admission_digest IS NOT OLD.admission_digest))
BEGIN SELECT RAISE(ABORT, 'remote delivery facts cannot be replaced'); END;
CREATE TRIGGER IF NOT EXISTS remote_command_identity_immutable
BEFORE UPDATE ON remote_commands
BEGIN SELECT RAISE(ABORT, 'remote command identity is immutable'); END;
CREATE TRIGGER IF NOT EXISTS remote_runtime_retained BEFORE DELETE ON remote_runtimes
BEGIN SELECT RAISE(ABORT, 'remote runtime debt must be retained'); END;
CREATE TRIGGER IF NOT EXISTS remote_operation_retained BEFORE DELETE ON remote_operations
BEGIN SELECT RAISE(ABORT, 'remote operation facts must be retained'); END;
CREATE TRIGGER IF NOT EXISTS remote_command_retained BEFORE DELETE ON remote_commands
BEGIN SELECT RAISE(ABORT, 'remote command identity must be retained'); END;
