-- Backend pins belong to their canonical source. Canonical source deletion
-- already checks provider/resource debt; direct pin deletion must remain denied.
CREATE TEMP TABLE saved_run_backends_0031 AS SELECT * FROM execution_run_backends;
CREATE TEMP TABLE saved_planning_backends_0031 AS SELECT * FROM execution_planning_backends;
DROP TABLE execution_run_backends;
DROP TABLE execution_planning_backends;
CREATE TABLE execution_run_backends(run_id TEXT PRIMARY KEY REFERENCES workflow_runs(run_id) ON DELETE CASCADE,backend TEXT NOT NULL CHECK(backend IN('local','openshell')));
CREATE TABLE execution_planning_backends(operation_id TEXT PRIMARY KEY REFERENCES planning_operations(operation_id) ON DELETE CASCADE,backend TEXT NOT NULL CHECK(backend IN('local','openshell')));
INSERT INTO execution_run_backends SELECT * FROM saved_run_backends_0031;
INSERT INTO execution_planning_backends SELECT * FROM saved_planning_backends_0031;
DROP TABLE saved_run_backends_0031;
DROP TABLE saved_planning_backends_0031;
CREATE TRIGGER execution_run_backend_immutable BEFORE UPDATE ON execution_run_backends BEGIN SELECT RAISE(ABORT,'run backend is immutable'); END;
CREATE TRIGGER execution_run_backend_retained BEFORE DELETE ON execution_run_backends WHEN EXISTS(SELECT 1 FROM workflow_runs WHERE run_id=OLD.run_id) BEGIN SELECT RAISE(ABORT,'run backend belongs to its source'); END;
CREATE TRIGGER execution_planning_backend_immutable BEFORE UPDATE ON execution_planning_backends BEGIN SELECT RAISE(ABORT,'planning backend is immutable'); END;
CREATE TRIGGER execution_planning_backend_retained BEFORE DELETE ON execution_planning_backends WHEN EXISTS(SELECT 1 FROM planning_operations WHERE operation_id=OLD.operation_id) BEGIN SELECT RAISE(ABORT,'planning backend belongs to its source'); END;
