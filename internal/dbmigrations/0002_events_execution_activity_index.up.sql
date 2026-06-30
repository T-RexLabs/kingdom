CREATE INDEX idx_events_node_run_type_created_activity
ON events (
  node_run_id,
  event_type,
  julianday(created_at) DESC,
  run_event_seq DESC,
  seq DESC,
  event_id DESC
);
