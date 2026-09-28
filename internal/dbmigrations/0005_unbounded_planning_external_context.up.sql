DROP TRIGGER planning_operation_external_no_update;
DROP TRIGGER planning_operation_external_no_delete;

CREATE TABLE planning_operation_external_snapshots_next (
  operation_id TEXT NOT NULL,
  snapshot_id TEXT NOT NULL,
  assembler_version TEXT NOT NULL,
  context_digest TEXT NOT NULL,
  issue_bytes INTEGER NOT NULL CHECK (issue_bytes >= 0),
  comment_bytes INTEGER NOT NULL CHECK (comment_bytes >= 0),
  bound_at TEXT NOT NULL,
  PRIMARY KEY (operation_id, snapshot_id),
  FOREIGN KEY (snapshot_id) REFERENCES external_source_snapshots(snapshot_id)
);

INSERT INTO planning_operation_external_snapshots_next (
  operation_id, snapshot_id, assembler_version, context_digest, issue_bytes, comment_bytes, bound_at
)
SELECT operation_id, snapshot_id, assembler_version, context_digest, issue_bytes, comment_bytes, bound_at
FROM planning_operation_external_snapshots;

DROP TABLE planning_operation_external_snapshots;
ALTER TABLE planning_operation_external_snapshots_next RENAME TO planning_operation_external_snapshots;

CREATE TRIGGER planning_operation_external_no_update
BEFORE UPDATE ON planning_operation_external_snapshots
BEGIN SELECT RAISE(ABORT, 'planning operation source bindings are immutable'); END;

CREATE TRIGGER planning_operation_external_no_delete
BEFORE DELETE ON planning_operation_external_snapshots
BEGIN SELECT RAISE(ABORT, 'planning operation source bindings are immutable'); END;
