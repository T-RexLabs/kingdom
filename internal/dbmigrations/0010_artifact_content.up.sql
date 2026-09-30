-- Content is published with its metadata in the same transaction. Fixed-size
-- rows avoid SQLite's single-value size ceiling and bound Go/driver allocations.
CREATE TABLE artifact_content_chunks (
    artifact_id TEXT NOT NULL REFERENCES artifacts(artifact_id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL CHECK (chunk_index >= 0),
    content BLOB NOT NULL CHECK (length(content) > 0 AND length(content) <= 1048576),
    PRIMARY KEY (artifact_id, chunk_index)
) WITHOUT ROWID;

WITH RECURSIVE chunks(artifact_id, offset, size) AS (
    SELECT artifact_id, 1, length(content) FROM artifact_contents WHERE length(content) > 0
    UNION ALL
    SELECT artifact_id, offset + 1048576, size FROM chunks WHERE offset + 1048576 <= size
)
INSERT INTO artifact_content_chunks(artifact_id, chunk_index, content)
SELECT c.artifact_id, (c.offset - 1) / 1048576, substr(a.content, c.offset, 1048576)
FROM chunks c JOIN artifact_contents a ON a.artifact_id = c.artifact_id;

-- Retain the existing manifest table and replace its obsolete monolithic value.
ALTER TABLE artifact_contents DROP COLUMN content;

CREATE INDEX idx_artifacts_attempt_identity ON artifacts(attempt_id, artifact_id);
CREATE INDEX idx_artifacts_attempt_chronology ON artifacts(attempt_id, created_at DESC, artifact_id);
