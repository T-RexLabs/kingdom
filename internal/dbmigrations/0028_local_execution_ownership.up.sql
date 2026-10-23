-- Local ownership is distinct from central membership. No retained hosted
-- snapshot or completed local history is rewritten by this declaration.
CREATE TABLE execution_access_local_owner (
 slot INTEGER PRIMARY KEY CHECK(slot=1) REFERENCES execution_access_project(slot),
 owner_id TEXT NOT NULL CHECK(owner_id<>''),
 domain TEXT NOT NULL CHECK(domain='local_owner')
);
CREATE TRIGGER execution_access_local_owner_immutable BEFORE UPDATE ON execution_access_local_owner BEGIN SELECT RAISE(ABORT,'local execution owner is immutable'); END;
CREATE TRIGGER execution_access_local_owner_retained BEFORE DELETE ON execution_access_local_owner BEGIN SELECT RAISE(ABORT,'local execution owner must be retained'); END;
