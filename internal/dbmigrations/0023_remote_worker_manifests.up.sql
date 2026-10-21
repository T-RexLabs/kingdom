CREATE TABLE IF NOT EXISTS remote_runtime_owners (
 allocation INTEGER PRIMARY KEY REFERENCES remote_runtimes(allocation),
 descriptor BLOB NOT NULL,
 digest TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS remote_runtime_owner_immutable BEFORE UPDATE ON remote_runtime_owners
BEGIN SELECT RAISE(ABORT, 'remote runtime owner is immutable'); END;
CREATE TRIGGER IF NOT EXISTS remote_runtime_owner_retained BEFORE DELETE ON remote_runtime_owners
BEGIN SELECT RAISE(ABORT, 'remote runtime owner must be retained'); END;
CREATE TABLE IF NOT EXISTS remote_worker_manifests (
 invocation_id TEXT PRIMARY KEY,
 owner_kind TEXT NOT NULL CHECK(owner_kind IN ('run','planning')),
 binding_id TEXT NOT NULL UNIQUE,
 lease_owner TEXT NOT NULL,
 lease_version INTEGER NOT NULL CHECK(lease_version>0),
 resource_id TEXT NOT NULL,
 creation_id TEXT NOT NULL UNIQUE,
 import_id TEXT NOT NULL UNIQUE,
 export_id TEXT NOT NULL UNIQUE,
 source_json BLOB NOT NULL,
 pins_json BLOB NOT NULL,
 allocation INTEGER UNIQUE REFERENCES remote_runtimes(allocation)
);
CREATE TRIGGER IF NOT EXISTS remote_worker_manifest_immutable
BEFORE UPDATE OF invocation_id,owner_kind,binding_id,lease_owner,lease_version,resource_id,creation_id,import_id,export_id,source_json,pins_json ON remote_worker_manifests
BEGIN SELECT RAISE(ABORT, 'remote worker manifest is immutable'); END;
CREATE TRIGGER IF NOT EXISTS remote_worker_manifest_allocation_immutable
BEFORE UPDATE OF allocation ON remote_worker_manifests WHEN OLD.allocation IS NOT NULL AND NEW.allocation IS NOT OLD.allocation
BEGIN SELECT RAISE(ABORT, 'remote worker allocation is immutable'); END;
CREATE TRIGGER IF NOT EXISTS remote_worker_manifest_retained BEFORE DELETE ON remote_worker_manifests
BEGIN SELECT RAISE(ABORT, 'remote worker manifest must be retained'); END;
