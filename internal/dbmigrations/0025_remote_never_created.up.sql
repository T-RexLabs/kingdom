CREATE TABLE IF NOT EXISTS remote_runtimes_never_created (
 allocation INTEGER PRIMARY KEY REFERENCES remote_runtimes(allocation),
 creation_id TEXT NOT NULL UNIQUE REFERENCES remote_operations(operation_id)
);
CREATE TRIGGER IF NOT EXISTS remote_never_created_immutable BEFORE UPDATE ON remote_runtimes_never_created BEGIN SELECT RAISE(ABORT,'never-created proof is immutable'); END;
CREATE TRIGGER IF NOT EXISTS remote_never_created_retained BEFORE DELETE ON remote_runtimes_never_created BEGIN SELECT RAISE(ABORT,'never-created proof must be retained'); END;
CREATE TRIGGER IF NOT EXISTS remote_never_created_delivery BEFORE UPDATE OF delivery ON remote_operations WHEN EXISTS(SELECT 1 FROM remote_runtimes_never_created n WHERE n.creation_id=OLD.operation_id) BEGIN SELECT RAISE(ABORT,'never-created runtime cannot dispatch'); END;
