CREATE TABLE IF NOT EXISTS remote_runtime_disposal_intents (
 allocation INTEGER PRIMARY KEY REFERENCES remote_runtimes(allocation),
 inventory BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS remote_runtime_archives (
 allocation INTEGER PRIMARY KEY REFERENCES remote_runtime_disposal_intents(allocation),
 archive BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS remote_runtime_delete_intents (
 allocation INTEGER PRIMARY KEY REFERENCES remote_runtime_archives(allocation),
 selector BLOB NOT NULL,
 sandbox BLOB NOT NULL,
 request_id TEXT NOT NULL UNIQUE,
 observed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS remote_runtime_absence (
 allocation INTEGER PRIMARY KEY REFERENCES remote_runtime_delete_intents(allocation),
 observed_at TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS remote_disposal_intent_immutable BEFORE UPDATE ON remote_runtime_disposal_intents BEGIN SELECT RAISE(ABORT,'disposal intent is immutable'); END;
CREATE TRIGGER IF NOT EXISTS remote_disposal_intent_retained BEFORE DELETE ON remote_runtime_disposal_intents BEGIN SELECT RAISE(ABORT,'disposal intent must be retained'); END;
CREATE TRIGGER IF NOT EXISTS remote_archive_immutable BEFORE UPDATE ON remote_runtime_archives BEGIN SELECT RAISE(ABORT,'runtime archive is immutable'); END;
CREATE TRIGGER IF NOT EXISTS remote_archive_retained BEFORE DELETE ON remote_runtime_archives BEGIN SELECT RAISE(ABORT,'runtime archive must be retained'); END;
CREATE TRIGGER IF NOT EXISTS remote_delete_intent_immutable BEFORE UPDATE ON remote_runtime_delete_intents BEGIN SELECT RAISE(ABORT,'delete intent is immutable'); END;
CREATE TRIGGER IF NOT EXISTS remote_delete_intent_retained BEFORE DELETE ON remote_runtime_delete_intents BEGIN SELECT RAISE(ABORT,'delete intent must be retained'); END;
CREATE TRIGGER IF NOT EXISTS remote_absence_immutable BEFORE UPDATE ON remote_runtime_absence BEGIN SELECT RAISE(ABORT,'native absence is immutable'); END;
CREATE TRIGGER IF NOT EXISTS remote_absence_retained BEFORE DELETE ON remote_runtime_absence BEGIN SELECT RAISE(ABORT,'native absence must be retained'); END;
