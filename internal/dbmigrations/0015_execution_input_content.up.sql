ALTER TABLE run_planning_outputs ADD COLUMN content_ref_json TEXT NOT NULL DEFAULT '';

CREATE TABLE execution_prompt_refs (
    attempt_id TEXT PRIMARY KEY REFERENCES attempt_executions(attempt_id),
    run_id TEXT NOT NULL REFERENCES workflow_runs(run_id),
    node_run_id TEXT NOT NULL REFERENCES node_runs(node_run_id),
    content_id TEXT NOT NULL REFERENCES request_contents(content_id),
    created_at TEXT NOT NULL,
    FOREIGN KEY(attempt_id,run_id,node_run_id) REFERENCES attempt_executions(attempt_id,run_id,node_run_id)
);
CREATE TRIGGER execution_prompt_refs_immutable BEFORE UPDATE ON execution_prompt_refs
BEGIN SELECT RAISE(ABORT,'execution prompt references are immutable'); END;
