-- A request belongs to exactly one existing canonical invocation. Provider
-- identity is separate from workflow identity and from command process guards.
CREATE TABLE provider_requests (
  binding_id TEXT PRIMARY KEY CHECK (TRIM(binding_id) <> ''),
  run_invocation_id TEXT UNIQUE REFERENCES provider_invocations(provider_invocation_id),
  planning_invocation_id TEXT UNIQUE REFERENCES planning_provider_invocations(planning_provider_invocation_id) ON DELETE CASCADE,
  instance_id TEXT NOT NULL CHECK (TRIM(instance_id) <> ''),
  generation TEXT NOT NULL CHECK (TRIM(generation) <> ''),
  location TEXT NOT NULL CHECK (TRIM(location) <> ''),
  provider_session_id TEXT NOT NULL CHECK (TRIM(provider_session_id) <> ''),
  profile_id TEXT NOT NULL CHECK (TRIM(profile_id) <> ''),
  request_id TEXT NOT NULL CHECK (TRIM(request_id) <> ''),
  logical_session_id TEXT NOT NULL CHECK (TRIM(logical_session_id) <> ''),
  lease_owner TEXT NOT NULL CHECK (TRIM(lease_owner) <> ''),
  lease_version INTEGER NOT NULL CHECK (lease_version > 0),
  dispatch_issued_at TEXT,
  admission_closed_at TEXT,
  -- References existing invocation-scoped immutable provider content. A
  -- reference is evidence transport; it does not assert terminal validity.
  terminal_sha256 TEXT,
  terminal_size_bytes INTEGER,
  created_at TEXT NOT NULL,
  CHECK ((run_invocation_id IS NOT NULL) <> (planning_invocation_id IS NOT NULL)),
  CHECK ((terminal_sha256 IS NULL AND terminal_size_bytes IS NULL) OR
    (terminal_sha256 IS NOT NULL AND terminal_size_bytes IS NOT NULL AND length(terminal_sha256) = 64 AND terminal_sha256 NOT GLOB '*[^0-9a-f]*' AND terminal_size_bytes >= 0)),
  UNIQUE (instance_id, generation, provider_session_id, request_id),
  UNIQUE (instance_id, generation, profile_id)
);
CREATE INDEX idx_provider_requests_session ON provider_requests(provider_session_id);
CREATE TRIGGER provider_requests_identity_immutable
BEFORE UPDATE OF binding_id,run_invocation_id,planning_invocation_id,instance_id,generation,location,
  provider_session_id,profile_id,request_id,logical_session_id,lease_owner,lease_version,created_at ON provider_requests
BEGIN SELECT RAISE(ABORT, 'provider request identity is immutable'); END;
CREATE TRIGGER provider_requests_facts_monotonic
BEFORE UPDATE OF dispatch_issued_at,admission_closed_at,terminal_sha256,terminal_size_bytes ON provider_requests
WHEN (OLD.dispatch_issued_at IS NOT NULL AND NEW.dispatch_issued_at IS NOT OLD.dispatch_issued_at)
  OR (OLD.admission_closed_at IS NOT NULL AND NEW.admission_closed_at IS NOT OLD.admission_closed_at)
  OR (OLD.terminal_sha256 IS NOT NULL AND (NEW.terminal_sha256 IS NOT OLD.terminal_sha256 OR NEW.terminal_size_bytes IS NOT OLD.terminal_size_bytes))
BEGIN SELECT RAISE(ABORT, 'provider request facts cannot be replaced'); END;

CREATE TABLE provider_native_grants (
  grant_id TEXT PRIMARY KEY CHECK (TRIM(grant_id) <> ''),
  binding_id TEXT NOT NULL REFERENCES provider_requests(binding_id) ON DELETE CASCADE,
  message_id TEXT NOT NULL CHECK (TRIM(message_id) <> ''),
  call_id TEXT NOT NULL CHECK (TRIM(call_id) <> ''),
  tool TEXT NOT NULL CHECK (tool IN ('write','edit','patch','read','glob','grep')),
  input_json TEXT NOT NULL CHECK (json_valid(input_json)),
  identity_digest TEXT NOT NULL CHECK (length(identity_digest) = 64 AND identity_digest NOT GLOB '*[^0-9a-f]*'),
  outcome TEXT CHECK (outcome IN ('completed','failed','aborted')),
  native_started INTEGER CHECK (native_started IN (0,1)),
  created_at TEXT NOT NULL,
  completed_at TEXT,
  CHECK ((outcome IS NULL AND native_started IS NULL AND completed_at IS NULL) OR
    (outcome IS NOT NULL AND native_started IS NOT NULL AND completed_at IS NOT NULL AND ((outcome IN ('completed','failed') AND native_started = 1) OR (outcome = 'aborted' AND native_started = 0)))),
  UNIQUE (binding_id,message_id,call_id)
);
CREATE TRIGGER provider_native_identity_immutable BEFORE UPDATE OF grant_id,binding_id,message_id,call_id,tool,input_json,identity_digest,created_at ON provider_native_grants
BEGIN SELECT RAISE(ABORT, 'native grant identity is immutable'); END;
CREATE TRIGGER provider_native_receipt_immutable BEFORE UPDATE OF outcome,native_started,completed_at ON provider_native_grants
WHEN OLD.outcome IS NOT NULL AND (NEW.outcome IS NOT OLD.outcome OR NEW.native_started IS NOT OLD.native_started OR NEW.completed_at IS NOT OLD.completed_at)
BEGIN SELECT RAISE(ABORT, 'native receipt is immutable'); END;

-- A claim precedes guardian acknowledgement. A crash between them leaves an
-- unresolved claim rather than granting permission to run the command again.
CREATE TABLE provider_command_calls (
  claim_id TEXT PRIMARY KEY CHECK (TRIM(claim_id) <> ''),
  binding_id TEXT NOT NULL REFERENCES provider_requests(binding_id) ON DELETE CASCADE,
  message_id TEXT NOT NULL CHECK (TRIM(message_id) <> ''),
  call_id TEXT NOT NULL CHECK (TRIM(call_id) <> ''),
  tool TEXT NOT NULL CHECK (tool = 'shell'),
  input_json TEXT NOT NULL CHECK (json_valid(input_json)),
  identity_digest TEXT NOT NULL CHECK (length(identity_digest) = 64 AND identity_digest NOT GLOB '*[^0-9a-f]*'),
  command_ordinal INTEGER NOT NULL CHECK (command_ordinal > 0),
  guard_id TEXT UNIQUE REFERENCES provider_process_guards(guard_id),
  created_at TEXT NOT NULL,
  completed_at TEXT,
  CHECK ((guard_id IS NULL AND completed_at IS NULL) OR (guard_id IS NOT NULL AND completed_at IS NOT NULL)),
  UNIQUE (binding_id,message_id,call_id),
  UNIQUE (binding_id,command_ordinal)
);
CREATE TRIGGER provider_command_identity_immutable BEFORE UPDATE OF claim_id,binding_id,message_id,call_id,tool,input_json,identity_digest,command_ordinal,created_at ON provider_command_calls
BEGIN SELECT RAISE(ABORT, 'command claim identity is immutable'); END;
CREATE TRIGGER provider_command_completion_immutable BEFORE UPDATE OF guard_id,completed_at ON provider_command_calls
WHEN OLD.completed_at IS NOT NULL AND (NEW.guard_id IS NOT OLD.guard_id OR NEW.completed_at IS NOT OLD.completed_at)
BEGIN SELECT RAISE(ABORT, 'command completion is immutable'); END;
