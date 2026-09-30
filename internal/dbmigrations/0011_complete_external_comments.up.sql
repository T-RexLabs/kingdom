-- kingdom: foreign_keys=off

CREATE TABLE external_source_snapshots_next (
  snapshot_id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL,
  predecessor_snapshot_id TEXT,
  normalized_issue_json TEXT NOT NULL CHECK (json_valid(normalized_issue_json)),
  import_profile_json TEXT NOT NULL CHECK (json_valid(import_profile_json)),
  import_profile_digest TEXT NOT NULL,
  provider_updated_at TEXT NOT NULL,
  fetched_at TEXT NOT NULL,
  content_digest TEXT NOT NULL,
  etag TEXT NOT NULL DEFAULT '',
  comments_etag TEXT NOT NULL DEFAULT '',
  last_modified TEXT NOT NULL DEFAULT '',
  provider_comment_count INTEGER NOT NULL DEFAULT 0 CHECK (provider_comment_count >= 0),
  fetched_comment_count INTEGER NOT NULL DEFAULT 0 CHECK (fetched_comment_count >= 0),
  fetched_comment_pages INTEGER NOT NULL DEFAULT 0 CHECK (fetched_comment_pages >= 0),
  omitted_comment_count INTEGER NOT NULL DEFAULT 0 CHECK (omitted_comment_count >= 0),
  omitted_comment_pages INTEGER NOT NULL DEFAULT 0 CHECK (omitted_comment_pages >= 0),
  retained_comment_count INTEGER NOT NULL DEFAULT 0 CHECK (retained_comment_count >= 0),
  retained_comment_bytes INTEGER NOT NULL DEFAULT 0 CHECK (retained_comment_bytes >= 0),
  comments_truncated INTEGER NOT NULL DEFAULT 0 CHECK (comments_truncated IN (0, 1)),
  truncation_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(truncation_json)),
  created_at TEXT NOT NULL,
  created_at_unix_nano INTEGER NOT NULL,
  FOREIGN KEY (source_id) REFERENCES external_sources(source_id),
  FOREIGN KEY (predecessor_snapshot_id) REFERENCES external_source_snapshots(snapshot_id)
);
INSERT INTO external_source_snapshots_next SELECT * FROM external_source_snapshots;
DROP TABLE external_source_snapshots;
ALTER TABLE external_source_snapshots_next RENAME TO external_source_snapshots;
CREATE INDEX idx_external_snapshots_source_created_unix_nano ON external_source_snapshots (source_id, created_at_unix_nano DESC, snapshot_id);
CREATE UNIQUE INDEX idx_external_snapshots_version_fence ON external_source_snapshots (source_id, COALESCE(predecessor_snapshot_id, ''), import_profile_digest, content_digest);
CREATE TRIGGER external_snapshots_no_update BEFORE UPDATE ON external_source_snapshots BEGIN SELECT RAISE(ABORT, 'external snapshots are immutable'); END;
CREATE TRIGGER external_snapshots_no_delete BEFORE DELETE ON external_source_snapshots BEGIN SELECT RAISE(ABORT, 'external snapshots are immutable'); END;

CREATE TABLE external_source_snapshot_comments_next (
  snapshot_id TEXT NOT NULL,
  provider_comment_id TEXT NOT NULL,
  ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
  author_display_identifier TEXT NOT NULL,
  body TEXT NOT NULL,
  canonical_url TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  original_bytes INTEGER NOT NULL CHECK (original_bytes >= 0),
  retained_bytes INTEGER NOT NULL CHECK (retained_bytes >= 0),
  truncation_reason TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (snapshot_id, provider_comment_id),
  UNIQUE (snapshot_id, ordinal),
  FOREIGN KEY (snapshot_id) REFERENCES external_source_snapshots(snapshot_id)
);
INSERT INTO external_source_snapshot_comments_next SELECT * FROM external_source_snapshot_comments;
DROP TABLE external_source_snapshot_comments;
ALTER TABLE external_source_snapshot_comments_next RENAME TO external_source_snapshot_comments;
CREATE INDEX idx_external_snapshot_comments_order ON external_source_snapshot_comments (snapshot_id, ordinal, provider_comment_id);
CREATE TRIGGER external_snapshot_comments_no_update BEFORE UPDATE ON external_source_snapshot_comments BEGIN SELECT RAISE(ABORT, 'external snapshot comments are immutable'); END;
CREATE TRIGGER external_snapshot_comments_no_delete BEFORE DELETE ON external_source_snapshot_comments BEGIN SELECT RAISE(ABORT, 'external snapshot comments are immutable'); END;
CREATE TRIGGER external_snapshot_comments_complete BEFORE INSERT ON external_source_snapshot_comments
WHEN NOT EXISTS (SELECT 1 FROM external_source_snapshots WHERE snapshot_id=NEW.snapshot_id AND NEW.ordinal<retained_comment_count)
BEGIN SELECT RAISE(ABORT, 'external snapshot comment ordinal is outside its immutable capture'); END;
