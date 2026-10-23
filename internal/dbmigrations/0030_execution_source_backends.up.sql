CREATE TABLE execution_project_identity(slot INTEGER PRIMARY KEY CHECK(slot=1),id TEXT NOT NULL UNIQUE);
INSERT INTO execution_project_identity VALUES(1,lower(hex(randomblob(4)))||'-'||lower(hex(randomblob(2)))||'-4'||substr(lower(hex(randomblob(2))),2)||'-8'||substr(lower(hex(randomblob(2))),2)||'-'||lower(hex(randomblob(6))));
CREATE TRIGGER execution_project_identity_immutable BEFORE UPDATE ON execution_project_identity BEGIN SELECT RAISE(ABORT,'project incarnation is immutable'); END;
CREATE TRIGGER execution_project_identity_retained BEFORE DELETE ON execution_project_identity BEGIN SELECT RAISE(ABORT,'project incarnation must be retained'); END;
CREATE TABLE execution_run_backends(run_id TEXT PRIMARY KEY REFERENCES workflow_runs(run_id),backend TEXT NOT NULL CHECK(backend IN('local','openshell')));
CREATE TABLE execution_planning_backends(operation_id TEXT PRIMARY KEY REFERENCES planning_operations(operation_id),backend TEXT NOT NULL CHECK(backend IN('local','openshell')));
-- Derive historical pins only from retained execution facts. Conflicting
-- backends fail the CHECK rather than selecting a new deployment default.
WITH facts AS (
 SELECT run_id,'openshell' AS backend FROM execution_access_runs
 UNION ALL SELECT i.run_id,r.execution_backend FROM provider_requests r JOIN provider_invocations i ON i.provider_invocation_id=r.run_invocation_id
 UNION ALL SELECT i.run_id,'openshell' FROM remote_runtimes r JOIN provider_invocations i ON r.owner_kind='run' AND i.provider_invocation_id=r.invocation_id
)
INSERT INTO execution_run_backends SELECT run_id,COALESCE((SELECT CASE WHEN count(DISTINCT backend)>1 THEN 'conflict' ELSE min(backend) END FROM facts f WHERE f.run_id=r.run_id),'local') FROM workflow_runs r;
WITH facts AS (
 SELECT operation_id,'openshell' AS backend FROM execution_access_planning
 UNION ALL SELECT i.planning_operation_id,r.execution_backend FROM provider_requests r JOIN planning_provider_invocations i ON i.planning_provider_invocation_id=r.planning_invocation_id
 UNION ALL SELECT i.planning_operation_id,'openshell' FROM remote_runtimes r JOIN planning_provider_invocations i ON r.owner_kind='planning' AND i.planning_provider_invocation_id=r.invocation_id
)
INSERT INTO execution_planning_backends SELECT operation_id,COALESCE((SELECT CASE WHEN count(DISTINCT backend)>1 THEN 'conflict' ELSE min(backend) END FROM facts f WHERE f.operation_id=o.operation_id),'local') FROM planning_operations o;
CREATE TRIGGER execution_run_backend_immutable BEFORE UPDATE ON execution_run_backends BEGIN SELECT RAISE(ABORT,'run backend is immutable'); END;
CREATE TRIGGER execution_run_backend_retained BEFORE DELETE ON execution_run_backends BEGIN SELECT RAISE(ABORT,'run backend must be retained'); END;
CREATE TRIGGER execution_planning_backend_immutable BEFORE UPDATE ON execution_planning_backends BEGIN SELECT RAISE(ABORT,'planning backend is immutable'); END;
CREATE TRIGGER execution_planning_backend_retained BEFORE DELETE ON execution_planning_backends BEGIN SELECT RAISE(ABORT,'planning backend must be retained'); END;
