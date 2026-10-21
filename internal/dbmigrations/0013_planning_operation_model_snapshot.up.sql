-- Older operations did not retain their model at enqueue. The session value at
-- migration is the best available backfill; prior session edits cannot be recovered.
ALTER TABLE planning_operations ADD COLUMN model_selection_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(model_selection_json));
ALTER TABLE planning_operations ADD COLUMN model_selection_source_operation_id TEXT NOT NULL DEFAULT '';
UPDATE planning_operations SET model_selection_json=(SELECT model_selection_json FROM planning_sessions WHERE planning_sessions.planning_session_id=planning_operations.planning_session_id);
CREATE TRIGGER planning_operation_model_snapshot_immutable BEFORE UPDATE OF model_selection_json,model_selection_source_operation_id ON planning_operations
BEGIN SELECT RAISE(ABORT,'planning operation model snapshot is immutable'); END;
