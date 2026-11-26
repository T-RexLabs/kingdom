-- 0019 native name-observation rows remain immutable historical evidence.
-- New closure requires the gateway's exact-ID finalized deletion protocol.
CREATE TABLE IF NOT EXISTS remote_runtime_deletions (
 allocation INTEGER PRIMARY KEY REFERENCES remote_runtime_archives(allocation),
 receipt BLOB NOT NULL
);
CREATE TRIGGER IF NOT EXISTS remote_runtime_deletion_immutable BEFORE UPDATE ON remote_runtime_deletions BEGIN SELECT RAISE(ABORT,'runtime deletion receipt is immutable'); END;
CREATE TRIGGER IF NOT EXISTS remote_runtime_deletion_retained BEFORE DELETE ON remote_runtime_deletions BEGIN SELECT RAISE(ABORT,'runtime deletion receipt must be retained'); END;
