ALTER TABLE run_execution_targets
ADD COLUMN cleanup_cursor TEXT NOT NULL DEFAULT '';

ALTER TABLE run_worktrees
ADD COLUMN cleanup_cursor TEXT NOT NULL DEFAULT '';

-- The owner/fence serializes bounded cleanup batches. cleanup_started is the
-- irreversible boundary used by terminal Retry/Rerun transactions.
ALTER TABLE run_worktrees
ADD COLUMN cleanup_owner TEXT NOT NULL DEFAULT '';

ALTER TABLE run_worktrees
ADD COLUMN cleanup_owner_pid INTEGER NOT NULL DEFAULT 0 CHECK (cleanup_owner_pid >= 0);

ALTER TABLE run_worktrees
ADD COLUMN cleanup_owner_identity_token TEXT NOT NULL DEFAULT '';

ALTER TABLE run_worktrees
ADD COLUMN cleanup_fence INTEGER NOT NULL DEFAULT 0 CHECK (cleanup_fence >= 0);

ALTER TABLE run_worktrees
ADD COLUMN cleanup_started INTEGER NOT NULL DEFAULT 0 CHECK (cleanup_started IN (0, 1));

CREATE INDEX idx_run_worktrees_claimed_run
ON run_worktrees (run_id) WHERE cleanup_owner <> '';
