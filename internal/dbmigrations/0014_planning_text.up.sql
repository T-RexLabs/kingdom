ALTER TABLE planning_messages ADD COLUMN content_content_id TEXT REFERENCES request_contents(content_id);
ALTER TABLE planning_messages ADD COLUMN answer_text_content_id TEXT REFERENCES request_contents(content_id);
ALTER TABLE planning_sessions ADD COLUMN title_content_id TEXT REFERENCES request_contents(content_id);
ALTER TABLE planning_drafts ADD COLUMN work_order_text_content_id TEXT REFERENCES request_contents(content_id);
ALTER TABLE planning_provider_completion_settlements ADD COLUMN final_text_ref_json TEXT NOT NULL DEFAULT '';
ALTER TABLE planning_operations ADD COLUMN observed_output_ref_json TEXT NOT NULL DEFAULT '';

CREATE TABLE planning_event_text_refs (
  event_id TEXT NOT NULL REFERENCES planning_operation_events(event_id),
  field TEXT NOT NULL,
  content_id TEXT NOT NULL REFERENCES request_contents(content_id),
  PRIMARY KEY(event_id,field)
);
CREATE TRIGGER planning_event_text_refs_no_update BEFORE UPDATE ON planning_event_text_refs
BEGIN SELECT RAISE(ABORT, 'planning event text references are immutable'); END;
CREATE TRIGGER planning_event_text_refs_no_delete BEFORE DELETE ON planning_event_text_refs
BEGIN SELECT RAISE(ABORT, 'planning event text references are immutable'); END;
CREATE TRIGGER planning_message_text_refs_no_update BEFORE UPDATE OF content_content_id,answer_text_content_id ON planning_messages
BEGIN SELECT RAISE(ABORT, 'planning message text references are immutable'); END;
CREATE TRIGGER planning_draft_text_ref_no_update BEFORE UPDATE OF work_order_text_content_id ON planning_drafts
BEGIN SELECT RAISE(ABORT, 'planning draft text reference is immutable'); END;
