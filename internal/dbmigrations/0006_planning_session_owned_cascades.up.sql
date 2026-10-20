-- A planning session is the lifecycle owner of its captured context and
-- provider activity. Removing the session must remove those child records,
-- while immutable-record guards continue to reject edits during its lifetime.

CREATE TABLE planning_context_entries_rebuild (
  snapshot_id TEXT NOT NULL,
  ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
  kind TEXT NOT NULL CHECK (kind IN ('included', 'excluded')),
  path TEXT NOT NULL CHECK (length(path) > 0),
  reason TEXT NOT NULL,
  size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
  PRIMARY KEY (snapshot_id, ordinal),
  FOREIGN KEY (snapshot_id) REFERENCES planning_context_snapshots(planning_context_snapshot_id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED
);
INSERT INTO planning_context_entries_rebuild (snapshot_id, ordinal, kind, path, reason, size_bytes)
SELECT snapshot_id, ordinal, kind, path, reason, size_bytes FROM planning_context_entries;
DROP TABLE planning_context_entries;
ALTER TABLE planning_context_entries_rebuild RENAME TO planning_context_entries;
CREATE INDEX planning_context_entries_kind ON planning_context_entries(snapshot_id, kind, ordinal);

CREATE TRIGGER planning_context_entries_sealed BEFORE INSERT ON planning_context_entries
WHEN EXISTS (SELECT 1 FROM planning_context_snapshots WHERE planning_context_snapshot_id=NEW.snapshot_id)
BEGIN SELECT RAISE(ABORT, 'planning context snapshot is sealed'); END;
CREATE TRIGGER planning_context_entries_no_update BEFORE UPDATE ON planning_context_entries
BEGIN SELECT RAISE(ABORT, 'planning context entries are immutable'); END;
CREATE TRIGGER planning_context_entries_no_delete BEFORE DELETE ON planning_context_entries
WHEN EXISTS (
  SELECT 1 FROM planning_context_snapshots snapshot
  JOIN planning_sessions session ON session.planning_session_id=snapshot.planning_session_id
  WHERE snapshot.planning_context_snapshot_id=OLD.snapshot_id
)
BEGIN SELECT RAISE(ABORT, 'planning context entries are immutable'); END;

-- Provider content manifests and chunks are rebuilt together because their
-- composite foreign key must cascade as one owned content unit.
CREATE TABLE planning_provider_contents_rebuild AS
SELECT planning_provider_invocation_id, sha256, size_bytes FROM planning_provider_contents;
CREATE TABLE planning_provider_content_chunks_rebuild AS
SELECT planning_provider_invocation_id, sha256, ordinal, content FROM planning_provider_content_chunks;
DROP TABLE planning_provider_content_chunks;
DROP TABLE planning_provider_contents;

CREATE TABLE planning_provider_contents (
  planning_provider_invocation_id TEXT NOT NULL REFERENCES planning_provider_invocations(planning_provider_invocation_id) ON DELETE CASCADE,
  sha256 TEXT NOT NULL,
  size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
  PRIMARY KEY (planning_provider_invocation_id, sha256)
);
CREATE TABLE planning_provider_content_chunks (
  planning_provider_invocation_id TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
  content BLOB NOT NULL CHECK (length(content) > 0 AND length(content) <= 65536),
  PRIMARY KEY (planning_provider_invocation_id, sha256, ordinal),
  FOREIGN KEY (planning_provider_invocation_id, sha256)
    REFERENCES planning_provider_contents(planning_provider_invocation_id, sha256) ON DELETE CASCADE
);
INSERT INTO planning_provider_contents (planning_provider_invocation_id, sha256, size_bytes)
SELECT planning_provider_invocation_id, sha256, size_bytes FROM planning_provider_contents_rebuild;
INSERT INTO planning_provider_content_chunks (planning_provider_invocation_id, sha256, ordinal, content)
SELECT planning_provider_invocation_id, sha256, ordinal, content FROM planning_provider_content_chunks_rebuild;
DROP TABLE planning_provider_content_chunks_rebuild;
DROP TABLE planning_provider_contents_rebuild;

CREATE TRIGGER planning_provider_contents_no_update BEFORE UPDATE ON planning_provider_contents
BEGIN SELECT RAISE(ABORT, 'planning provider content is immutable'); END;
CREATE TRIGGER planning_provider_contents_no_delete BEFORE DELETE ON planning_provider_contents
WHEN EXISTS (
  SELECT 1 FROM planning_provider_invocations invocation
  JOIN planning_sessions session ON session.planning_session_id=invocation.planning_session_id
  WHERE invocation.planning_provider_invocation_id=OLD.planning_provider_invocation_id
)
BEGIN SELECT RAISE(ABORT, 'planning provider content is immutable'); END;
CREATE TRIGGER planning_provider_chunks_no_update BEFORE UPDATE ON planning_provider_content_chunks
BEGIN SELECT RAISE(ABORT, 'planning provider content is immutable'); END;
CREATE TRIGGER planning_provider_chunks_no_delete BEFORE DELETE ON planning_provider_content_chunks
WHEN EXISTS (
  SELECT 1 FROM planning_provider_invocations invocation
  JOIN planning_sessions session ON session.planning_session_id=invocation.planning_session_id
  WHERE invocation.planning_provider_invocation_id=OLD.planning_provider_invocation_id
)
BEGIN SELECT RAISE(ABORT, 'planning provider content is immutable'); END;

-- Activity details and exact text references belong to their planning event.
CREATE TABLE activity_event_contents_rebuild AS
SELECT content_id, run_event_id, planning_event_id, field FROM activity_event_contents;
DROP TABLE activity_event_contents;
CREATE TABLE activity_event_contents (
  content_id TEXT PRIMARY KEY REFERENCES request_contents(content_id),
  run_event_id TEXT REFERENCES events(event_id) DEFERRABLE INITIALLY DEFERRED,
  planning_event_id TEXT REFERENCES planning_operation_events(event_id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
  field TEXT NOT NULL CHECK (field IN ('input', 'output', 'reasoning', 'message')),
  CHECK ((run_event_id IS NULL) <> (planning_event_id IS NULL)),
  UNIQUE (run_event_id, field),
  UNIQUE (planning_event_id, field)
);
INSERT INTO activity_event_contents (content_id, run_event_id, planning_event_id, field)
SELECT content_id, run_event_id, planning_event_id, field FROM activity_event_contents_rebuild;
DROP TABLE activity_event_contents_rebuild;
CREATE TRIGGER activity_event_contents_no_update BEFORE UPDATE ON activity_event_contents
BEGIN SELECT RAISE(ABORT, 'activity content references are immutable'); END;
CREATE TRIGGER activity_event_contents_no_delete BEFORE DELETE ON activity_event_contents
WHEN OLD.run_event_id IS NOT NULL OR EXISTS (
  SELECT 1 FROM planning_operation_events event
  JOIN planning_operations operation ON operation.operation_id=event.operation_id
  JOIN planning_sessions session ON session.planning_session_id=operation.planning_session_id
  WHERE event.event_id=OLD.planning_event_id
)
BEGIN SELECT RAISE(ABORT, 'activity content references are immutable'); END;

CREATE TABLE planning_event_text_refs_rebuild AS
SELECT event_id, field, content_id FROM planning_event_text_refs;
DROP TABLE planning_event_text_refs;
CREATE TABLE planning_event_text_refs (
  event_id TEXT NOT NULL REFERENCES planning_operation_events(event_id) ON DELETE CASCADE,
  field TEXT NOT NULL,
  content_id TEXT NOT NULL REFERENCES request_contents(content_id),
  PRIMARY KEY (event_id, field)
);
INSERT INTO planning_event_text_refs (event_id, field, content_id)
SELECT event_id, field, content_id FROM planning_event_text_refs_rebuild;
DROP TABLE planning_event_text_refs_rebuild;
CREATE TRIGGER planning_event_text_refs_no_update BEFORE UPDATE ON planning_event_text_refs
BEGIN SELECT RAISE(ABORT, 'planning event text references are immutable'); END;
CREATE TRIGGER planning_event_text_refs_no_delete BEFORE DELETE ON planning_event_text_refs
WHEN EXISTS (
  SELECT 1 FROM planning_operation_events event
  JOIN planning_operations operation ON operation.operation_id=event.operation_id
  JOIN planning_sessions session ON session.planning_session_id=operation.planning_session_id
  WHERE event.event_id=OLD.event_id
)
BEGIN SELECT RAISE(ABORT, 'planning event text references are immutable'); END;

-- Canonical text blobs have one lifecycle owner. Planner-authored content is
-- removed with its session; other content remains immutable and restrictive.
ALTER TABLE request_contents ADD COLUMN planning_session_id TEXT
  REFERENCES planning_sessions(planning_session_id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED;
CREATE INDEX idx_request_contents_planning_session
  ON request_contents(planning_session_id) WHERE planning_session_id IS NOT NULL;

DROP TRIGGER request_contents_no_update;
WITH planning_content_owners AS (
  SELECT title_content_id AS content_id, planning_session_id
  FROM planning_sessions WHERE title_content_id IS NOT NULL
  UNION
  SELECT content_content_id, planning_session_id
  FROM planning_messages WHERE content_content_id IS NOT NULL
  UNION
  SELECT answer_text_content_id, planning_session_id
  FROM planning_messages WHERE answer_text_content_id IS NOT NULL
  UNION
  SELECT work_order_text_content_id, planning_session_id
  FROM planning_drafts WHERE work_order_text_content_id IS NOT NULL
  UNION
  SELECT contents.content_id, operations.planning_session_id
  FROM activity_event_contents contents
  JOIN planning_operation_events events ON events.event_id=contents.planning_event_id
  JOIN planning_operations operations ON operations.operation_id=events.operation_id
  UNION
  SELECT refs.content_id, operations.planning_session_id
  FROM planning_event_text_refs refs
  JOIN planning_operation_events events ON events.event_id=refs.event_id
  JOIN planning_operations operations ON operations.operation_id=events.operation_id
), unique_planning_content_owners AS (
  SELECT content_id, MIN(planning_session_id) AS planning_session_id
  FROM planning_content_owners
  GROUP BY content_id
  HAVING COUNT(DISTINCT planning_session_id)=1
)
UPDATE request_contents
SET planning_session_id=(
  SELECT owners.planning_session_id
  FROM unique_planning_content_owners owners
  WHERE owners.content_id=request_contents.content_id
)
WHERE planning_session_id IS NULL
  AND content_id IN (SELECT content_id FROM unique_planning_content_owners);
CREATE TRIGGER request_contents_no_update BEFORE UPDATE ON request_contents
BEGIN SELECT RAISE(ABORT, 'request content manifests are immutable'); END;
CREATE TRIGGER request_contents_no_delete BEFORE DELETE ON request_contents
WHEN OLD.planning_session_id IS NULL OR EXISTS (
  SELECT 1 FROM planning_sessions
  WHERE planning_session_id=OLD.planning_session_id
)
BEGIN SELECT RAISE(ABORT, 'request content manifests are immutable'); END;

-- Planner-owned manifests cascade through bytes and activity integrity digests.
-- The delete guards below allow only the owner-driven cascade after the session
-- row has been removed. Direct deletion remains blocked during a session's life.
CREATE TABLE request_content_chunks_rebuild AS
SELECT content_id, ordinal, content FROM request_content_chunks;
CREATE TABLE activity_content_chunk_digests_rebuild AS
SELECT content_id, ordinal, sha256 FROM activity_content_chunk_digests;
DROP TABLE activity_content_chunk_digests;
DROP TABLE request_content_chunks;
CREATE TABLE request_content_chunks (
  content_id TEXT NOT NULL REFERENCES request_contents(content_id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
  ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
  content BLOB NOT NULL CHECK (length(content) > 0 AND length(content) <= 65536),
  PRIMARY KEY (content_id, ordinal)
) WITHOUT ROWID;
INSERT INTO request_content_chunks (content_id, ordinal, content)
SELECT content_id, ordinal, content FROM request_content_chunks_rebuild;
DROP TABLE request_content_chunks_rebuild;
CREATE TABLE activity_content_chunk_digests (
  content_id TEXT NOT NULL,
  ordinal INTEGER NOT NULL,
  sha256 TEXT NOT NULL CHECK (length(sha256)=64),
  PRIMARY KEY (content_id, ordinal),
  FOREIGN KEY (content_id, ordinal)
    REFERENCES request_content_chunks(content_id, ordinal) ON DELETE CASCADE
) WITHOUT ROWID;
INSERT INTO activity_content_chunk_digests (content_id, ordinal, sha256)
SELECT content_id, ordinal, sha256 FROM activity_content_chunk_digests_rebuild;
DROP TABLE activity_content_chunk_digests_rebuild;

CREATE TRIGGER request_content_chunks_sealed BEFORE INSERT ON request_content_chunks
WHEN EXISTS (SELECT 1 FROM request_contents WHERE content_id=NEW.content_id)
BEGIN SELECT RAISE(ABORT, 'request content is sealed'); END;
CREATE TRIGGER request_content_chunks_no_update BEFORE UPDATE ON request_content_chunks
BEGIN SELECT RAISE(ABORT, 'request content chunks are immutable'); END;
CREATE TRIGGER request_content_chunks_no_delete BEFORE DELETE ON request_content_chunks
WHEN EXISTS (
  SELECT 1 FROM request_contents content
  LEFT JOIN planning_sessions session ON session.planning_session_id=content.planning_session_id
  WHERE content.content_id=OLD.content_id
    AND (content.planning_session_id IS NULL OR session.planning_session_id IS NOT NULL)
)
BEGIN SELECT RAISE(ABORT, 'request content chunks are immutable'); END;
CREATE TRIGGER activity_content_chunk_digests_no_update BEFORE UPDATE ON activity_content_chunk_digests
BEGIN SELECT RAISE(ABORT, 'activity content chunk digests are immutable'); END;
CREATE TRIGGER activity_content_chunk_digests_no_delete BEFORE DELETE ON activity_content_chunk_digests
WHEN EXISTS (
  SELECT 1 FROM request_contents content
  LEFT JOIN planning_sessions session ON session.planning_session_id=content.planning_session_id
  WHERE content.content_id=OLD.content_id
    AND (content.planning_session_id IS NULL OR session.planning_session_id IS NOT NULL)
)
BEGIN SELECT RAISE(ABORT, 'activity content chunk digests are immutable'); END;
