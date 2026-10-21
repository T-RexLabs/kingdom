CREATE TABLE execution_access_project (
 slot INTEGER PRIMARY KEY CHECK(slot=1),
 project_id TEXT NOT NULL CHECK(project_id<>'')
);
CREATE TRIGGER execution_access_project_immutable BEFORE UPDATE ON execution_access_project BEGIN SELECT RAISE(ABORT,'execution project is immutable'); END;
CREATE TRIGGER execution_access_project_retained BEFORE DELETE ON execution_access_project BEGIN SELECT RAISE(ABORT,'execution project must be retained'); END;

CREATE TABLE execution_access_runs (
 run_id TEXT PRIMARY KEY REFERENCES workflow_runs(run_id),
 snapshot TEXT NOT NULL CHECK(json_valid(snapshot))
);
CREATE TRIGGER execution_access_run_immutable BEFORE UPDATE ON execution_access_runs BEGIN SELECT RAISE(ABORT,'execution access is immutable'); END;
CREATE TRIGGER execution_access_run_retained BEFORE DELETE ON execution_access_runs BEGIN SELECT RAISE(ABORT,'execution access must be retained'); END;

CREATE TABLE execution_access_planning (
 operation_id TEXT PRIMARY KEY REFERENCES planning_operations(operation_id),
 snapshot TEXT NOT NULL CHECK(json_valid(snapshot))
);
CREATE TRIGGER execution_access_planning_immutable BEFORE UPDATE ON execution_access_planning BEGIN SELECT RAISE(ABORT,'execution access is immutable'); END;
CREATE TRIGGER execution_access_planning_retained BEFORE DELETE ON execution_access_planning BEGIN SELECT RAISE(ABORT,'execution access must be retained'); END;

CREATE TABLE execution_access_invocations (
 owner_kind TEXT NOT NULL CHECK(owner_kind IN ('run','planning')),
 invocation_id TEXT NOT NULL,
 run_id TEXT REFERENCES execution_access_runs(run_id),
 operation_id TEXT REFERENCES execution_access_planning(operation_id),
 snapshot TEXT NOT NULL CHECK(json_valid(snapshot)),
 PRIMARY KEY(owner_kind,invocation_id),
 FOREIGN KEY(owner_kind,invocation_id) REFERENCES provider_preparations(owner_kind,invocation_id),
 CHECK((owner_kind='run' AND run_id IS NOT NULL AND operation_id IS NULL) OR
       (owner_kind='planning' AND operation_id IS NOT NULL AND run_id IS NULL))
);
CREATE TRIGGER execution_access_invocation_immutable BEFORE UPDATE ON execution_access_invocations BEGIN SELECT RAISE(ABORT,'invocation access is immutable'); END;
CREATE TRIGGER execution_access_invocation_retained BEFORE DELETE ON execution_access_invocations BEGIN SELECT RAISE(ABORT,'invocation access must be retained'); END;
