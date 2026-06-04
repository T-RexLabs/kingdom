ALTER TABLE attempt_executions
ADD COLUMN progress_event_type TEXT NOT NULL DEFAULT '';

ALTER TABLE attempt_executions
ADD COLUMN progress_reason_code TEXT NOT NULL DEFAULT '';

ALTER TABLE attempt_executions
ADD COLUMN progress_at TEXT;
