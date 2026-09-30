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
CREATE TABLE decree_text_refs (
  decree_id TEXT PRIMARY KEY REFERENCES decrees(decree_id),
  content_id TEXT NOT NULL UNIQUE REFERENCES request_contents(content_id)
);
ALTER TABLE decrees ADD COLUMN text_storage TEXT NOT NULL DEFAULT 'inline' CHECK (text_storage IN ('inline', 'chunked'));
CREATE TRIGGER request_content_chunks_sealed BEFORE INSERT ON request_content_chunks
WHEN EXISTS (SELECT 1 FROM request_contents WHERE content_id = NEW.content_id)
BEGIN SELECT RAISE(ABORT, 'request content is sealed'); END;
CREATE TRIGGER request_content_chunks_no_update BEFORE UPDATE ON request_content_chunks
BEGIN SELECT RAISE(ABORT, 'request content chunks are immutable'); END;
CREATE TRIGGER request_content_chunks_no_delete BEFORE DELETE ON request_content_chunks
BEGIN SELECT RAISE(ABORT, 'request content chunks are immutable'); END;
CREATE TRIGGER request_contents_no_update BEFORE UPDATE ON request_contents
BEGIN SELECT RAISE(ABORT, 'request content manifests are immutable'); END;
CREATE TRIGGER decree_text_refs_no_update BEFORE UPDATE ON decree_text_refs
BEGIN SELECT RAISE(ABORT, 'decree text references are immutable'); END;
CREATE TRIGGER decree_text_refs_no_delete BEFORE DELETE ON decree_text_refs
BEGIN SELECT RAISE(ABORT, 'decree text references are immutable'); END;
