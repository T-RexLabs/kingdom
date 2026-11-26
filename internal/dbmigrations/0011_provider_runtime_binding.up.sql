-- Runtime provenance is immutable and cannot borrow local process evidence.
ALTER TABLE provider_requests ADD COLUMN execution_backend TEXT NOT NULL DEFAULT 'local'
 CHECK(execution_backend IN ('local','openshell'));
ALTER TABLE provider_requests ADD COLUMN remote_allocation INTEGER REFERENCES remote_runtimes(allocation)
 CHECK((execution_backend='local' AND remote_allocation IS NULL) OR
       (execution_backend='openshell' AND remote_allocation IS NOT NULL));
CREATE UNIQUE INDEX provider_request_remote_allocation ON provider_requests(remote_allocation) WHERE remote_allocation IS NOT NULL;
CREATE TRIGGER provider_request_runtime_immutable BEFORE UPDATE OF execution_backend,remote_allocation ON provider_requests
BEGIN SELECT RAISE(ABORT,'provider runtime binding is immutable'); END;
