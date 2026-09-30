ALTER TABLE attempt_executions
ADD COLUMN response_only_workspace_cleanup_pending_at TEXT;

ALTER TABLE provider_invocations
ADD COLUMN terminal_handoff_pending_lease_owner TEXT NOT NULL DEFAULT '';

ALTER TABLE provider_invocations
ADD COLUMN terminal_handoff_pending_lease_version INTEGER NOT NULL DEFAULT 0 CHECK (terminal_handoff_pending_lease_version >= 0);

ALTER TABLE provider_invocations
ADD COLUMN terminal_handoff_pending_process_quiescent INTEGER NOT NULL DEFAULT 0 CHECK (terminal_handoff_pending_process_quiescent IN (0, 1));

ALTER TABLE provider_invocations
ADD COLUMN terminal_recovery_required_lease_owner TEXT NOT NULL DEFAULT '';

ALTER TABLE provider_invocations
ADD COLUMN terminal_recovery_required_lease_version INTEGER NOT NULL DEFAULT 0 CHECK (terminal_recovery_required_lease_version >= 0);

ALTER TABLE provider_invocations
ADD COLUMN terminal_recovery_required_process_quiescent INTEGER NOT NULL DEFAULT 0 CHECK (terminal_recovery_required_process_quiescent IN (0, 1));
