-- Remote commands never borrow a local guardian identity. Retain their exact
-- verified managed execution receipt link independently of workflow outcome.
CREATE TABLE provider_remote_command_completions (
 claim_id TEXT PRIMARY KEY REFERENCES provider_command_calls(claim_id),
 operation_id TEXT NOT NULL UNIQUE REFERENCES remote_commands(command_id),
 completed_at TEXT NOT NULL
);
CREATE TRIGGER provider_remote_command_completion_immutable BEFORE UPDATE ON provider_remote_command_completions
BEGIN SELECT RAISE(ABORT, 'remote command completion is immutable'); END;
CREATE TRIGGER provider_remote_command_completion_retained BEFORE DELETE ON provider_remote_command_completions
BEGIN SELECT RAISE(ABORT, 'remote command completion must be retained'); END;
