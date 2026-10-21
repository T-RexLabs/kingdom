CREATE TABLE IF NOT EXISTS remote_capability_registry (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 registry_id TEXT NOT NULL UNIQUE
);
CREATE TRIGGER IF NOT EXISTS remote_capability_registry_immutable BEFORE UPDATE ON remote_capability_registry BEGIN SELECT RAISE(ABORT,'capability registry binding is immutable'); END;
CREATE TRIGGER IF NOT EXISTS remote_capability_registry_retained BEFORE DELETE ON remote_capability_registry BEGIN SELECT RAISE(ABORT,'capability registry binding must be retained'); END;
CREATE TABLE IF NOT EXISTS remote_capability_closures (
 allocation INTEGER NOT NULL REFERENCES remote_runtimes(allocation),
 purpose TEXT NOT NULL CHECK(purpose IN ('model','plugin')),
 registry_id TEXT NOT NULL REFERENCES remote_capability_registry(registry_id),
 certificate_id TEXT NOT NULL UNIQUE,
 certificate BLOB NOT NULL,
 certificate_sha256 TEXT NOT NULL CHECK(length(certificate_sha256)=64),
 PRIMARY KEY(allocation,purpose)
);
CREATE TRIGGER IF NOT EXISTS remote_capability_closure_immutable BEFORE UPDATE ON remote_capability_closures BEGIN SELECT RAISE(ABORT,'capability closure is immutable'); END;
CREATE TRIGGER IF NOT EXISTS remote_capability_closure_retained BEFORE DELETE ON remote_capability_closures BEGIN SELECT RAISE(ABORT,'capability closure must be retained'); END;
