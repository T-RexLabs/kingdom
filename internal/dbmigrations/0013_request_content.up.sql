CREATE TABLE request_contents (
  content_id TEXT PRIMARY KEY,
  sha256 TEXT NOT NULL CHECK (length(sha256) = 64),
  size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
  chunk_count INTEGER NOT NULL CHECK (chunk_count >= 0),
  created_at TEXT NOT NULL
);
CREATE TABLE request_content_chunks (
  content_id TEXT NOT NULL REFERENCES request_contents(content_id) DEFERRABLE INITIALLY DEFERRED,
  ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
  content BLOB NOT NULL CHECK (length(content) > 0 AND length(content) <= 65536),
  PRIMARY KEY (content_id, ordinal)
) WITHOUT ROWID;
CREATE TABLE work_order_text_refs (
  work_order_id TEXT PRIMARY KEY REFERENCES work_orders(work_order_id),
  content_id TEXT NOT NULL UNIQUE REFERENCES request_contents(content_id)
);
ALTER TABLE work_orders ADD COLUMN text_storage TEXT NOT NULL DEFAULT 'inline' CHECK (text_storage IN ('inline', 'chunked'));
CREATE TRIGGER request_content_chunks_sealed BEFORE INSERT ON request_content_chunks
WHEN EXISTS (SELECT 1 FROM request_contents WHERE content_id = NEW.content_id)
BEGIN SELECT RAISE(ABORT, 'request content is sealed'); END;
CREATE TRIGGER request_content_chunks_no_update BEFORE UPDATE ON request_content_chunks
BEGIN SELECT RAISE(ABORT, 'request content chunks are immutable'); END;
CREATE TRIGGER request_content_chunks_no_delete BEFORE DELETE ON request_content_chunks
BEGIN SELECT RAISE(ABORT, 'request content chunks are immutable'); END;
CREATE TRIGGER request_contents_no_update BEFORE UPDATE ON request_contents
BEGIN SELECT RAISE(ABORT, 'request content manifests are immutable'); END;
CREATE TRIGGER work_order_text_refs_no_update BEFORE UPDATE ON work_order_text_refs
BEGIN SELECT RAISE(ABORT, 'work_order text references are immutable'); END;
CREATE TRIGGER work_order_text_refs_no_delete BEFORE DELETE ON work_order_text_refs
BEGIN SELECT RAISE(ABORT, 'work_order text references are immutable'); END;

CREATE TABLE activity_event_contents (
 content_id TEXT PRIMARY KEY REFERENCES request_contents(content_id),
 run_event_id TEXT REFERENCES events(event_id) DEFERRABLE INITIALLY DEFERRED,
 planning_event_id TEXT REFERENCES planning_operation_events(event_id) DEFERRABLE INITIALLY DEFERRED,
 field TEXT NOT NULL CHECK(field IN ('input','output','reasoning','message')),
 CHECK((run_event_id IS NULL) <> (planning_event_id IS NULL)),
 UNIQUE(run_event_id,field),
 UNIQUE(planning_event_id,field)
);
CREATE TABLE activity_content_chunk_digests (
 content_id TEXT NOT NULL,
 ordinal INTEGER NOT NULL,
 sha256 TEXT NOT NULL CHECK(length(sha256)=64),
 PRIMARY KEY(content_id,ordinal),
 FOREIGN KEY(content_id,ordinal) REFERENCES request_content_chunks(content_id,ordinal)
) WITHOUT ROWID;
CREATE TRIGGER activity_event_contents_no_update BEFORE UPDATE ON activity_event_contents
BEGIN SELECT RAISE(ABORT, 'activity content references are immutable'); END;
CREATE TRIGGER activity_event_contents_no_delete BEFORE DELETE ON activity_event_contents
BEGIN SELECT RAISE(ABORT, 'activity content references are immutable'); END;
CREATE TRIGGER activity_content_chunk_digests_no_update BEFORE UPDATE ON activity_content_chunk_digests
BEGIN SELECT RAISE(ABORT, 'activity content chunk digests are immutable'); END;
CREATE TRIGGER activity_content_chunk_digests_no_delete BEFORE DELETE ON activity_content_chunk_digests
BEGIN SELECT RAISE(ABORT, 'activity content chunk digests are immutable'); END;

CREATE INDEX idx_planning_activity_identity ON planning_operation_events(operation_id,json_extract(payload_json,'$.activity_id'),seq DESC);
