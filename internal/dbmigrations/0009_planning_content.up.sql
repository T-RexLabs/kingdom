CREATE TABLE planning_context_entries (
  snapshot_id TEXT NOT NULL,
  ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
  kind TEXT NOT NULL CHECK (kind IN ('included', 'excluded')),
  path TEXT NOT NULL CHECK (length(path) > 0),
  reason TEXT NOT NULL,
  size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
  PRIMARY KEY (snapshot_id, ordinal),
  FOREIGN KEY (snapshot_id) REFERENCES planning_context_snapshots(planning_context_snapshot_id) DEFERRABLE INITIALLY DEFERRED
);

INSERT INTO planning_context_entries
SELECT snapshot.planning_context_snapshot_id, CAST(entry.key AS INTEGER), 'included',
       json_extract(entry.value, '$.path'), COALESCE(json_extract(entry.value, '$.reason'), ''), COALESCE(json_extract(entry.value, '$.size_bytes'), 0)
FROM planning_context_snapshots snapshot, json_each(snapshot.included_files_json) entry;

INSERT INTO planning_context_entries
SELECT snapshot.planning_context_snapshot_id,
       json_array_length(snapshot.included_files_json) + CAST(entry.key AS INTEGER), 'excluded',
       json_extract(entry.value, '$.path'), COALESCE(json_extract(entry.value, '$.reason'), ''), COALESCE(json_extract(entry.value, '$.size_bytes'), 0)
FROM planning_context_snapshots snapshot, json_each(snapshot.excluded_files_json) entry;

CREATE INDEX planning_context_entries_kind ON planning_context_entries(snapshot_id, kind, ordinal);

-- Publish the parent only after all entries have arrived in its transaction.
CREATE TRIGGER planning_context_entries_sealed BEFORE INSERT ON planning_context_entries
WHEN EXISTS (SELECT 1 FROM planning_context_snapshots WHERE planning_context_snapshot_id=NEW.snapshot_id)
BEGIN SELECT RAISE(ABORT, 'planning context snapshot is sealed'); END;
CREATE TRIGGER planning_context_entries_no_update BEFORE UPDATE ON planning_context_entries
BEGIN SELECT RAISE(ABORT, 'planning context entries are immutable'); END;
CREATE TRIGGER planning_context_entries_no_delete BEFORE DELETE ON planning_context_entries
BEGIN SELECT RAISE(ABORT, 'planning context entries are immutable'); END;

CREATE TABLE planning_provider_contents (
  planning_provider_invocation_id TEXT NOT NULL REFERENCES planning_provider_invocations(planning_provider_invocation_id),
  sha256 TEXT NOT NULL,
  size_bytes INTEGER NOT NULL CHECK(size_bytes >= 0),
  PRIMARY KEY(planning_provider_invocation_id,sha256)
);
CREATE TABLE planning_provider_content_chunks (
  planning_provider_invocation_id TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  ordinal INTEGER NOT NULL CHECK(ordinal >= 0),
  content BLOB NOT NULL CHECK(length(content) > 0 AND length(content) <= 65536),
  PRIMARY KEY(planning_provider_invocation_id,sha256,ordinal),
  FOREIGN KEY(planning_provider_invocation_id,sha256) REFERENCES planning_provider_contents(planning_provider_invocation_id,sha256)
);
CREATE TRIGGER planning_provider_contents_no_update BEFORE UPDATE ON planning_provider_contents
BEGIN SELECT RAISE(ABORT, 'planning provider content is immutable'); END;
CREATE TRIGGER planning_provider_contents_no_delete BEFORE DELETE ON planning_provider_contents
BEGIN SELECT RAISE(ABORT, 'planning provider content is immutable'); END;
CREATE TRIGGER planning_provider_chunks_no_update BEFORE UPDATE ON planning_provider_content_chunks
BEGIN SELECT RAISE(ABORT, 'planning provider content is immutable'); END;
CREATE TRIGGER planning_provider_chunks_no_delete BEFORE DELETE ON planning_provider_content_chunks
BEGIN SELECT RAISE(ABORT, 'planning provider content is immutable'); END;

ALTER TABLE planning_activity_reconciliations ADD COLUMN delivered_activity_count INTEGER NOT NULL DEFAULT 0 CHECK(delivered_activity_count >= 0);
