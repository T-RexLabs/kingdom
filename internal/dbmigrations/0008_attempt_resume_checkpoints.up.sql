-- A checkpoint retains interrupted work without promoting its workspace to
-- trusted output. Its attempt retains the original trusted base and pinned
-- prompts; the provider request retains the conversation's ownership.
CREATE TABLE attempt_resume_checkpoints (
  source_attempt_id TEXT PRIMARY KEY REFERENCES attempt_executions(attempt_id),
  binding_id TEXT REFERENCES provider_requests(binding_id),
  workspace_snapshot_id TEXT REFERENCES filesystem_snapshots(snapshot_id),
  conversation_content_id TEXT REFERENCES request_contents(content_id),
  unavailable_reason TEXT NOT NULL DEFAULT '',
  captured_at TEXT NOT NULL CHECK (TRIM(captured_at) <> ''),
  CHECK (
    (unavailable_reason = '' AND binding_id IS NOT NULL AND workspace_snapshot_id IS NOT NULL AND conversation_content_id IS NOT NULL)
    OR
    (TRIM(unavailable_reason) <> '' AND workspace_snapshot_id IS NULL AND conversation_content_id IS NULL)
  )
);

CREATE TRIGGER attempt_resume_checkpoints_immutable
BEFORE UPDATE ON attempt_resume_checkpoints
BEGIN SELECT RAISE(ABORT, 'attempt resume checkpoints are immutable'); END;

CREATE TRIGGER attempt_resume_checkpoints_retained
BEFORE DELETE ON attempt_resume_checkpoints
WHEN EXISTS (SELECT 1 FROM attempt_executions WHERE attempt_id=OLD.source_attempt_id)
BEGIN SELECT RAISE(ABORT, 'referenced attempt resume checkpoints cannot be deleted'); END;

-- Recovery provenance pins the exact immutable checkpoint consumed by a new
-- attempt. Historical Retry/Rerun records retain their original meaning.
ALTER TABLE node_attempt_recoveries RENAME TO node_attempt_recoveries_previous;
CREATE TABLE node_attempt_recoveries (
  target_node_run_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  node_id TEXT NOT NULL,
  source_attempt_id TEXT NOT NULL,
  source_node_run_id TEXT NOT NULL,
  trigger TEXT NOT NULL CHECK (trigger IN ('operator_retry', 'operator_rerun', 'operator_resume')),
  reason_code TEXT NOT NULL,
  scheduled_at TEXT NOT NULL,
  checkpoint_source_attempt_id TEXT REFERENCES attempt_resume_checkpoints(source_attempt_id),
  CHECK ((trigger = 'operator_resume' AND checkpoint_source_attempt_id IS NOT NULL AND checkpoint_source_attempt_id = source_attempt_id)
    OR (trigger <> 'operator_resume' AND checkpoint_source_attempt_id IS NULL)),
  FOREIGN KEY (target_node_run_id, run_id) REFERENCES node_runs(node_run_id, run_id),
  FOREIGN KEY (source_attempt_id, run_id, source_node_run_id) REFERENCES attempt_executions(attempt_id, run_id, node_run_id)
);
INSERT INTO node_attempt_recoveries(target_node_run_id,run_id,node_id,source_attempt_id,source_node_run_id,trigger,reason_code,scheduled_at)
SELECT target_node_run_id,run_id,node_id,source_attempt_id,source_node_run_id,trigger,reason_code,scheduled_at FROM node_attempt_recoveries_previous;
DROP TABLE node_attempt_recoveries_previous;
-- A checkpoint is pinned again only when every earlier pin never reached the
-- provider; the replay transaction enforces that rule.
CREATE INDEX idx_node_attempt_recoveries_checkpoint
ON node_attempt_recoveries(checkpoint_source_attempt_id) WHERE checkpoint_source_attempt_id IS NOT NULL;
CREATE INDEX idx_node_attempt_recoveries_run_node_scheduled
ON node_attempt_recoveries(run_id,node_id,scheduled_at DESC,target_node_run_id DESC);
CREATE TRIGGER node_attempt_recoveries_immutable BEFORE UPDATE ON node_attempt_recoveries
BEGIN SELECT RAISE(ABORT, 'node attempt recovery provenance is immutable'); END;
