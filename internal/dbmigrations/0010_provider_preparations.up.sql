CREATE TABLE provider_preparations (
 owner_kind TEXT NOT NULL CHECK(owner_kind IN ('run','planning')),
 invocation_id TEXT NOT NULL,
 lease_owner TEXT NOT NULL CHECK(lease_owner<>''),
 lease_version INTEGER NOT NULL CHECK(lease_version>0),
 admission_closed INTEGER NOT NULL DEFAULT 0 CHECK(admission_closed IN (0,1)),
 PRIMARY KEY(owner_kind,invocation_id)
);
CREATE TRIGGER provider_preparation_identity_immutable BEFORE UPDATE OF owner_kind,invocation_id,lease_owner,lease_version ON provider_preparations
BEGIN SELECT RAISE(ABORT,'preparation identity is immutable'); END;
CREATE TRIGGER provider_preparation_retained BEFORE DELETE ON provider_preparations
BEGIN SELECT RAISE(ABORT,'preparation identity must be retained'); END;
CREATE TRIGGER provider_preparation_admission_monotonic BEFORE UPDATE OF admission_closed ON provider_preparations
WHEN OLD.admission_closed=1 AND NEW.admission_closed<>1
BEGIN SELECT RAISE(ABORT,'preparation admission cannot reopen'); END;
