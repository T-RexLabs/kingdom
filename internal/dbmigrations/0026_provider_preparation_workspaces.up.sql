CREATE TABLE IF NOT EXISTS provider_preparation_workspaces (
 owner_kind TEXT NOT NULL CHECK(owner_kind IN ('run','planning')),
 invocation_id TEXT NOT NULL,
 lease_owner TEXT NOT NULL,
 lease_version INTEGER NOT NULL CHECK(lease_version>0),
 resource_id TEXT NOT NULL CHECK(resource_id<>''),
 workspace_path TEXT NOT NULL CHECK(workspace_path<>''),
 device TEXT NOT NULL,
 inode TEXT NOT NULL,
 PRIMARY KEY(owner_kind,invocation_id)
);
CREATE TRIGGER IF NOT EXISTS provider_preparation_workspace_immutable BEFORE UPDATE ON provider_preparation_workspaces
BEGIN SELECT RAISE(ABORT,'preparation workspace is immutable'); END;
CREATE TRIGGER IF NOT EXISTS provider_preparation_workspace_retained BEFORE DELETE ON provider_preparation_workspaces
BEGIN SELECT RAISE(ABORT,'preparation workspace must be retained'); END;
