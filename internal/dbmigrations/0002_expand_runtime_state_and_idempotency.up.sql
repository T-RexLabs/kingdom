-- kingdom: foreign_keys=off

DROP TRIGGER run_planning_outputs_validate_lineage;
DROP INDEX idx_node_runs_running_started;
DROP INDEX idx_node_runs_run_status_node_attempt;

ALTER TABLE attempt_executions
ADD COLUMN launch_started_at TEXT;

UPDATE attempt_executions
SET launch_started_at = started_at
WHERE launch_started_at IS NULL
  AND started_at IS NOT NULL;

CREATE INDEX idx_attempt_executions_launch_started
ON attempt_executions (launch_started_at, state, updated_at)
WHERE launch_started_at IS NOT NULL;

CREATE TABLE node_runs_new (
  node_run_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  node_id TEXT NOT NULL,
  attempt INTEGER NOT NULL CHECK (attempt >= 1),
  wave INTEGER NOT NULL CHECK (wave >= 1),
  node_status TEXT NOT NULL CHECK (node_status IN ('pending', 'ready', 'starting', 'running', 'succeeded', 'failed', 'blocked', 'cancelled', 'skipped')),
  required_role TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  row_version INTEGER NOT NULL DEFAULT 1 CHECK (row_version >= 1),
  started_at TEXT,
  finished_at TEXT,
  superseded_at TEXT,
  FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id),
  FOREIGN KEY (required_role) REFERENCES roles(role_id),
  FOREIGN KEY (agent_id) REFERENCES agents(agent_id),
  UNIQUE (run_id, node_id, attempt)
);

INSERT INTO node_runs_new (
  node_run_id,
  run_id,
  node_id,
  attempt,
  wave,
  node_status,
  required_role,
  agent_id,
  row_version,
  started_at,
  finished_at,
  superseded_at
)
SELECT
  node_run_id,
  run_id,
  node_id,
  attempt,
  wave,
  node_status,
  required_role,
  agent_id,
  row_version,
  started_at,
  finished_at,
  NULL AS superseded_at
FROM node_runs;

DROP INDEX idx_node_runs_run_node_wave;
DROP TABLE node_runs;
ALTER TABLE node_runs_new RENAME TO node_runs;

CREATE INDEX idx_node_runs_run_node_wave
ON node_runs(run_id, node_id, wave, attempt);

CREATE INDEX idx_node_runs_running_started
ON node_runs(started_at, run_id, node_id, attempt, agent_id)
WHERE node_status = 'running' AND started_at IS NOT NULL;

CREATE INDEX idx_node_runs_run_status_node_attempt
ON node_runs(run_id, node_status, node_id, attempt, agent_id);

CREATE TRIGGER run_planning_outputs_validate_lineage
BEFORE INSERT ON run_planning_outputs
BEGIN
  SELECT CASE
    WHEN NOT EXISTS (
      SELECT 1
      FROM node_runs nr
      JOIN workflow_runs wr ON wr.run_id = nr.run_id
      JOIN workflow_nodes wn ON wn.workflow_definition_id = wr.workflow_definition_id AND wn.node_id = nr.node_id
      JOIN attempt_executions ae ON ae.attempt_id = NEW.attempt_id AND ae.run_id = NEW.run_id AND ae.node_run_id = NEW.node_run_id
      WHERE nr.node_run_id = NEW.node_run_id
        AND nr.run_id = NEW.run_id
        AND nr.node_id = NEW.node_id
        AND nr.agent_id = NEW.agent_id
        AND (
          (NEW.kind = 'plan_revision' AND wn.node_class = 'plan_drafting') OR
          (NEW.kind = 'advisory_guidance' AND wn.node_class = 'advisory_synthesis')
        )
    ) THEN RAISE(ABORT, 'invalid run planning output lineage')
  END;

  SELECT CASE
    WHEN NEW.source_message_id != '' AND NOT EXISTS (
      SELECT 1
      FROM mailbox_messages mm
      WHERE mm.message_id = NEW.source_message_id
        AND mm.run_id = NEW.run_id
    ) THEN RAISE(ABORT, 'invalid run planning output source message')
  END;
END;

ALTER TABLE idempotency_keys
ADD COLUMN status TEXT NOT NULL DEFAULT 'completed'
CHECK (status IN ('pending', 'completed'));

ALTER TABLE idempotency_keys
ADD COLUMN updated_at TEXT NOT NULL DEFAULT '';

UPDATE idempotency_keys
SET updated_at = created_at
WHERE TRIM(updated_at) = '';
