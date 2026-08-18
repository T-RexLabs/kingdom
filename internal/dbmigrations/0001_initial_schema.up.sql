PRAGMA foreign_keys = ON;

CREATE TABLE roles (
  role_id TEXT PRIMARY KEY,
  display_name TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  can_spawn INTEGER NOT NULL CHECK (can_spawn IN (0, 1)),
  agent_model_selector_visible INTEGER NOT NULL DEFAULT 0 CHECK (agent_model_selector_visible IN (0, 1)),
  agent_model_selector_section_id TEXT NOT NULL DEFAULT '',
  agent_model_selector_section_label TEXT NOT NULL DEFAULT '',
  agent_model_selector_section_order INTEGER NOT NULL DEFAULT 0,
  agent_model_selector_role_order INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL
);

CREATE TABLE agents (
  agent_id TEXT PRIMARY KEY,
  role_id TEXT NOT NULL,
  display_name TEXT,
  status TEXT NOT NULL CHECK (status IN ('idle', 'busy', 'blocked', 'offline', 'failed')),
  parent_agent_id TEXT,
  campaign_id TEXT,
  session_id TEXT,
  capability_profile_id TEXT,
  spawn_depth INTEGER NOT NULL CHECK (spawn_depth >= 0),
  last_heartbeat_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (role_id) REFERENCES roles(role_id),
  FOREIGN KEY (parent_agent_id) REFERENCES agents(agent_id),
  FOREIGN KEY (campaign_id) REFERENCES campaigns(campaign_id)
);

CREATE TABLE run_participants (
  run_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  role_id TEXT NOT NULL,
  display_name TEXT NOT NULL,
  parent_agent_id TEXT,
  spawn_depth INTEGER NOT NULL CHECK (spawn_depth >= 0),
  first_seen_at TEXT NOT NULL,
  PRIMARY KEY (run_id, agent_id),
  FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id),
  FOREIGN KEY (agent_id) REFERENCES agents(agent_id),
  FOREIGN KEY (role_id) REFERENCES roles(role_id),
  FOREIGN KEY (parent_agent_id) REFERENCES agents(agent_id)
);

CREATE TABLE decrees (
  decree_id TEXT PRIMARY KEY,
  decree_title TEXT NOT NULL,
  decree_text TEXT NOT NULL,
  metadata_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE campaigns (
  campaign_id TEXT PRIMARY KEY,
  decree_id TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('active', 'paused', 'closed')),
  created_at TEXT NOT NULL,
  FOREIGN KEY (decree_id) REFERENCES decrees(decree_id)
);

CREATE TABLE workflow_definitions (
  workflow_definition_id TEXT PRIMARY KEY,
  workflow_name TEXT NOT NULL,
  workflow_version TEXT NOT NULL,
  definition_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE (workflow_name, workflow_version)
);

CREATE TABLE workflow_nodes (
  workflow_node_id TEXT PRIMARY KEY,
  workflow_definition_id TEXT NOT NULL,
  node_id TEXT NOT NULL,
  required_role TEXT NOT NULL,
  node_class TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (workflow_definition_id) REFERENCES workflow_definitions(workflow_definition_id),
  FOREIGN KEY (required_role) REFERENCES roles(role_id),
  UNIQUE (workflow_definition_id, node_id)
);

CREATE TABLE workflow_edges (
  workflow_edge_id TEXT PRIMARY KEY,
  workflow_definition_id TEXT NOT NULL,
  from_node_id TEXT NOT NULL,
  to_node_id TEXT NOT NULL,
  on_condition TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (workflow_definition_id) REFERENCES workflow_definitions(workflow_definition_id),
  FOREIGN KEY (workflow_definition_id, from_node_id) REFERENCES workflow_nodes(workflow_definition_id, node_id),
  FOREIGN KEY (workflow_definition_id, to_node_id) REFERENCES workflow_nodes(workflow_definition_id, node_id),
  UNIQUE (workflow_definition_id, from_node_id, to_node_id, on_condition)
);

CREATE TABLE workflow_runs (
  run_id TEXT PRIMARY KEY,
  campaign_id TEXT,
  decree_id TEXT NOT NULL,
  workflow_definition_id TEXT NOT NULL,
  run_status TEXT NOT NULL CHECK (run_status IN ('pending', 'running', 'awaiting_human_confirmation', 'succeeded', 'failed', 'cancelled')),
  plan_state TEXT NOT NULL CHECK (plan_state IN ('draft', 'in_council_review', 'awaiting_king_decision', 'approved', 'needs_rework')),
  rework_cycle_count INTEGER NOT NULL DEFAULT 0 CHECK (rework_cycle_count >= 0),
  autonomous_approvals INTEGER NOT NULL DEFAULT 1 CHECK (autonomous_approvals IN (0, 1)),
  pending_approval_key TEXT NOT NULL DEFAULT '',
  model_selection_json TEXT NOT NULL DEFAULT '{}',
  run_until_all_approve INTEGER NOT NULL DEFAULT 0 CHECK (run_until_all_approve IN (0, 1)),
  max_execution_review_cycles INTEGER NOT NULL DEFAULT 0 CHECK (max_execution_review_cycles >= 0),
  max_validation_rework_cycles INTEGER NOT NULL DEFAULT 0 CHECK (max_validation_rework_cycles >= 0),
  row_version INTEGER NOT NULL DEFAULT 1 CHECK (row_version >= 1),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  publish_mode TEXT NOT NULL DEFAULT 'direct' CHECK (publish_mode IN ('direct', 'git_worktree')),
  FOREIGN KEY (campaign_id) REFERENCES campaigns(campaign_id),
  FOREIGN KEY (decree_id) REFERENCES decrees(decree_id),
  FOREIGN KEY (workflow_definition_id) REFERENCES workflow_definitions(workflow_definition_id)
);

CREATE TABLE run_activity (
  run_id TEXT PRIMARY KEY,
  last_activity_at TEXT NOT NULL,
  last_activity_event_id TEXT NOT NULL DEFAULT '',
  last_activity_run_event_seq INTEGER NOT NULL DEFAULT 0 CHECK (last_activity_run_event_seq >= 0),
  FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id)
);

CREATE TABLE run_search_documents (
  run_id TEXT PRIMARY KEY,
  decree_id TEXT NOT NULL,
  run_id_search TEXT NOT NULL,
  decree_title_search TEXT NOT NULL,
  FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id),
  FOREIGN KEY (decree_id) REFERENCES decrees(decree_id)
);

CREATE VIRTUAL TABLE run_search_fts USING fts5(
  run_id_search,
  decree_title_search,
  content='run_search_documents',
  content_rowid='rowid',
  tokenize = "unicode61 remove_diacritics 2 tokenchars '-_'",
  prefix='2 3 4'
);

CREATE TABLE run_event_sequences (
  run_id TEXT PRIMARY KEY,
  last_seq INTEGER NOT NULL CHECK (last_seq >= 0),
  FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id)
);

CREATE TABLE node_runs (
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

CREATE INDEX idx_node_runs_run_node_wave
ON node_runs(run_id, node_id, wave, attempt);

CREATE INDEX idx_workflow_runs_active_created
ON workflow_runs(created_at, run_id)
WHERE run_status IN ('pending', 'running');

CREATE INDEX idx_workflow_runs_terminal_created_updated
ON workflow_runs(created_at, updated_at DESC, run_id)
WHERE run_status IN ('succeeded', 'failed', 'cancelled');

CREATE INDEX idx_workflow_runs_manual_decision_updated
ON workflow_runs(updated_at DESC, run_id DESC)
WHERE autonomous_approvals = 0
  AND ((run_status = 'running' AND plan_state = 'awaiting_king_decision')
    OR run_status = 'awaiting_human_confirmation');

CREATE INDEX idx_workflow_runs_created_at
ON workflow_runs(created_at, run_id);

CREATE TRIGGER workflow_runs_run_activity_ai
AFTER INSERT ON workflow_runs
FOR EACH ROW
BEGIN
  INSERT INTO run_activity (run_id, last_activity_at, last_activity_event_id, last_activity_run_event_seq)
  VALUES (NEW.run_id, NEW.updated_at, '', 0);
END;

CREATE TRIGGER workflow_runs_run_activity_au
AFTER UPDATE OF updated_at ON workflow_runs
FOR EACH ROW
BEGIN
  UPDATE run_activity
  SET last_activity_at = CASE WHEN last_activity_at < NEW.updated_at THEN NEW.updated_at ELSE last_activity_at END
  WHERE run_id = NEW.run_id;
END;

CREATE UNIQUE INDEX idx_run_search_documents_run_id_search
ON run_search_documents(run_id_search);

CREATE INDEX idx_run_search_documents_decree_id
ON run_search_documents(decree_id);

CREATE INDEX idx_node_runs_running_started
ON node_runs(started_at, run_id, node_id, attempt, agent_id)
WHERE node_status = 'running' AND started_at IS NOT NULL;

CREATE INDEX idx_node_runs_run_status_node_attempt
ON node_runs(run_id, node_status, node_id, attempt, agent_id);

CREATE TRIGGER run_search_documents_ai
AFTER INSERT ON run_search_documents
BEGIN
  INSERT INTO run_search_fts(rowid, run_id_search, decree_title_search)
  VALUES (new.rowid, new.run_id_search, new.decree_title_search);
END;

CREATE TRIGGER run_search_documents_au
AFTER UPDATE ON run_search_documents
BEGIN
  INSERT INTO run_search_fts(run_search_fts, rowid, run_id_search, decree_title_search)
  VALUES ('delete', old.rowid, old.run_id_search, old.decree_title_search);
  INSERT INTO run_search_fts(rowid, run_id_search, decree_title_search)
  VALUES (new.rowid, new.run_id_search, new.decree_title_search);
END;

CREATE TRIGGER run_search_documents_ad
AFTER DELETE ON run_search_documents
BEGIN
  INSERT INTO run_search_fts(run_search_fts, rowid, run_id_search, decree_title_search)
  VALUES ('delete', old.rowid, old.run_id_search, old.decree_title_search);
END;

CREATE TRIGGER workflow_runs_run_search_ai
AFTER INSERT ON workflow_runs
BEGIN
  INSERT INTO run_search_documents (run_id, decree_id, run_id_search, decree_title_search)
  SELECT new.run_id, new.decree_id, lower(new.run_id), lower(d.decree_title)
  FROM decrees d
  WHERE d.decree_id = new.decree_id;
END;

CREATE TRIGGER workflow_runs_run_search_au
AFTER UPDATE OF run_id, decree_id ON workflow_runs
BEGIN
  UPDATE run_search_documents
  SET run_id = new.run_id,
      decree_id = new.decree_id,
      run_id_search = lower(new.run_id),
      decree_title_search = lower((SELECT d.decree_title FROM decrees d WHERE d.decree_id = new.decree_id))
  WHERE run_id = old.run_id;
END;

CREATE TRIGGER workflow_runs_run_search_ad
AFTER DELETE ON workflow_runs
BEGIN
  DELETE FROM run_search_documents WHERE run_id = old.run_id;
END;

CREATE TRIGGER decrees_run_search_title_au
AFTER UPDATE OF decree_title ON decrees
BEGIN
  UPDATE run_search_documents
  SET decree_title_search = lower(new.decree_title)
  WHERE decree_id = new.decree_id;
END;

CREATE TABLE plan_documents (
  plan_document_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  version INTEGER NOT NULL CHECK (version >= 1),
  state TEXT NOT NULL CHECK (state IN ('draft', 'in_council_review', 'awaiting_king_decision', 'approved', 'needs_rework')),
  content_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id),
  UNIQUE (run_id, version)
);

CREATE TABLE approvals (
  approval_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  decision TEXT NOT NULL CHECK (decision IN ('approve', 'needs_rework')),
  actor_id TEXT NOT NULL,
  role_id TEXT NOT NULL,
  reason_text TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id),
  FOREIGN KEY (role_id) REFERENCES roles(role_id)
);

CREATE TABLE run_planning_outputs (
  planning_output_seq INTEGER PRIMARY KEY AUTOINCREMENT,
  planning_output_id TEXT NOT NULL UNIQUE,
  run_id TEXT NOT NULL,
  node_id TEXT NOT NULL,
  node_run_id TEXT NOT NULL,
  attempt_id TEXT NOT NULL,
  agent_id TEXT NOT NULL,
  source_message_id TEXT NOT NULL DEFAULT '',
  kind TEXT NOT NULL CHECK (kind IN ('advisory_guidance', 'plan_revision')),
  content_markdown TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id),
  FOREIGN KEY (node_run_id) REFERENCES node_runs(node_run_id),
  FOREIGN KEY (attempt_id, node_run_id) REFERENCES attempt_executions(attempt_id, node_run_id),
  FOREIGN KEY (agent_id) REFERENCES agents(agent_id)
);

CREATE INDEX idx_run_planning_outputs_run_kind_seq
ON run_planning_outputs (run_id, kind, planning_output_seq DESC);

CREATE INDEX idx_run_planning_outputs_run_kind_agent_seq
ON run_planning_outputs (run_id, kind, agent_id, planning_output_seq DESC);

CREATE TABLE run_plan_revisions (
  plan_revision_seq INTEGER PRIMARY KEY AUTOINCREMENT,
  plan_revision_id TEXT NOT NULL UNIQUE,
  run_id TEXT NOT NULL,
  content_markdown TEXT NOT NULL,
  components_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id)
);

CREATE INDEX idx_run_plan_revisions_run_seq
ON run_plan_revisions (run_id, plan_revision_seq DESC);

CREATE TABLE run_plan_reviews (
  plan_review_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  reviewed_plan_revision_id TEXT NOT NULL,
  decision TEXT NOT NULL CHECK (decision IN ('approved', 'needs_rework')),
  feedback_markdown TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  role_id TEXT NOT NULL,
  queued_advisory_attempt INTEGER CHECK (queued_advisory_attempt IS NULL OR queued_advisory_attempt >= 1),
  queued_plan_attempt INTEGER CHECK (queued_plan_attempt IS NULL OR queued_plan_attempt >= 1),
  created_at TEXT NOT NULL,
  FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id),
  FOREIGN KEY (reviewed_plan_revision_id) REFERENCES run_plan_revisions(plan_revision_id),
  FOREIGN KEY (role_id) REFERENCES roles(role_id)
);

CREATE INDEX idx_run_plan_reviews_run_created
ON run_plan_reviews (run_id, created_at DESC, plan_review_id DESC);

CREATE INDEX idx_run_plan_reviews_run_decision_created
ON run_plan_reviews (run_id, decision, created_at DESC, plan_review_id DESC);

CREATE TABLE run_final_reviews (
  final_review_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  decision TEXT NOT NULL CHECK (decision IN ('confirmed', 'needs_rework')),
  feedback_markdown TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  role_id TEXT NOT NULL,
  queued_plan_attempt INTEGER CHECK (queued_plan_attempt IS NULL OR queued_plan_attempt >= 1),
  created_at TEXT NOT NULL,
  FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id),
  FOREIGN KEY (role_id) REFERENCES roles(role_id)
);

CREATE INDEX idx_run_final_reviews_run_created
ON run_final_reviews (run_id, created_at DESC, final_review_id DESC);

CREATE INDEX idx_run_final_reviews_run_decision_created
ON run_final_reviews (run_id, decision, created_at DESC, final_review_id DESC);

CREATE TABLE transitions (
  transition_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  node_run_id TEXT,
  entity_type TEXT NOT NULL CHECK (entity_type IN ('run', 'node', 'plan')),
  from_state TEXT NOT NULL,
  to_state TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  role_id TEXT NOT NULL,
  authority_source TEXT NOT NULL,
  reason_code TEXT NOT NULL,
  reason_text TEXT NOT NULL,
  correlation_id TEXT NOT NULL,
  causation_id TEXT NOT NULL,
  prev_hash TEXT NOT NULL,
  entry_hash TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id),
  FOREIGN KEY (node_run_id) REFERENCES node_runs(node_run_id),
  FOREIGN KEY (role_id) REFERENCES roles(role_id)
);

CREATE TABLE events (
  event_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  run_event_seq INTEGER NOT NULL CHECK (run_event_seq >= 1),
  node_run_id TEXT,
  attempt_id TEXT NOT NULL,
  schema_version TEXT NOT NULL,
  event_type TEXT NOT NULL,
  seq INTEGER NOT NULL CHECK (seq >= 1),
  payload_json TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  role_id TEXT NOT NULL,
  authority_source TEXT NOT NULL,
  reason_code TEXT NOT NULL,
  reason_text TEXT NOT NULL,
  correlation_id TEXT NOT NULL,
  causation_id TEXT NOT NULL,
  prev_hash TEXT NOT NULL,
  entry_hash TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id),
  FOREIGN KEY (node_run_id) REFERENCES node_runs(node_run_id),
  FOREIGN KEY (role_id) REFERENCES roles(role_id),
  UNIQUE (run_id, run_event_seq),
  UNIQUE (attempt_id, seq)
);

CREATE INDEX idx_events_type_created_event_id
ON events (event_type, created_at ASC, event_id ASC);

CREATE INDEX idx_events_run_event_seq
ON events (run_id, run_event_seq ASC);

CREATE INDEX idx_events_run_reason_code
ON events (run_id, reason_code);

CREATE TABLE attempt_executions (
  attempt_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  node_run_id TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('queued', 'starting', 'running', 'terminal_observed', 'committed_success', 'committed_failure', 'committed_cancelled')),
  execution_mode TEXT NOT NULL DEFAULT 'provider_backed' CHECK (execution_mode IN ('provider_backed', 'runtime_owned')),
  provider_timeout_ms INTEGER NOT NULL DEFAULT 0 CHECK (provider_timeout_ms >= 0),
  lease_owner TEXT NOT NULL DEFAULT '',
  lease_expires_at TEXT,
  lease_version INTEGER NOT NULL DEFAULT 0 CHECK (lease_version >= 0),
  session_ref TEXT NOT NULL DEFAULT '',
  provider_session_id TEXT NOT NULL DEFAULT '',
  active_provider_invocation_id TEXT,
  launch_started_at TEXT,
  started_at TEXT,
  terminal_observed_at TEXT,
  committed_at TEXT,
  failure_code TEXT NOT NULL DEFAULT '',
  failure_text TEXT NOT NULL DEFAULT '',
  canonical_settlement_id TEXT,
  progress_event_type TEXT NOT NULL DEFAULT '',
  progress_reason_code TEXT NOT NULL DEFAULT '',
  progress_at TEXT,
  base_snapshot_id TEXT NOT NULL DEFAULT '',
  result_snapshot_id TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id),
  FOREIGN KEY (node_run_id) REFERENCES node_runs(node_run_id),
  FOREIGN KEY (active_provider_invocation_id) REFERENCES provider_invocations(provider_invocation_id),
  FOREIGN KEY (canonical_settlement_id) REFERENCES provider_completion_settlements(settlement_id),
  UNIQUE (attempt_id, node_run_id)
);

CREATE INDEX idx_attempt_executions_state_lease
ON attempt_executions (state, lease_expires_at, updated_at);

CREATE INDEX idx_attempt_executions_launch_started
ON attempt_executions (launch_started_at, state, updated_at)
WHERE launch_started_at IS NOT NULL;

CREATE INDEX idx_attempt_executions_node_run
ON attempt_executions (node_run_id);

CREATE INDEX idx_attempt_executions_active_provider_invocation
ON attempt_executions (active_provider_invocation_id);

CREATE INDEX idx_attempt_executions_canonical_settlement
ON attempt_executions (canonical_settlement_id);

CREATE TABLE provider_invocations (
  provider_invocation_id TEXT PRIMARY KEY,
  attempt_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  node_run_id TEXT NOT NULL,
  logical_session_id TEXT NOT NULL DEFAULT '',
  provider_session_id TEXT NOT NULL DEFAULT '',
  invocation_state TEXT NOT NULL CHECK (invocation_state IN ('issued', 'open', 'receipt_persisted', 'settled_success', 'settled_failed', 'settled_cancelled', 'superseded', 'invalid_handshake')),
  completion_capability_hash TEXT NOT NULL DEFAULT '',
  completion_capability_issued_at TEXT NOT NULL,
  completion_capability_used_at TEXT,
  completion_capability_revoked_at TEXT,
  opened_at TEXT,
  receipt_seen_count INTEGER NOT NULL DEFAULT 0 CHECK (receipt_seen_count >= 0),
  first_receipt_seen_at TEXT,
  last_receipt_seen_at TEXT,
  last_receipt_disposition TEXT NOT NULL DEFAULT '' CHECK (last_receipt_disposition IN ('', 'accepted', 'rejected', 'invalidated', 'idempotent_duplicate', 'error')),
  receipt_persisted_at TEXT,
  settled_at TEXT,
  superseded_by_invocation_id TEXT,
  invalid_reason TEXT NOT NULL DEFAULT '',
  failure_reason_code TEXT NOT NULL DEFAULT '',
  failure_reason_text TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (attempt_id) REFERENCES attempt_executions(attempt_id),
  FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id),
  FOREIGN KEY (node_run_id) REFERENCES node_runs(node_run_id),
  FOREIGN KEY (superseded_by_invocation_id) REFERENCES provider_invocations(provider_invocation_id)
);

CREATE INDEX idx_provider_invocations_attempt_created
ON provider_invocations (attempt_id, created_at DESC, provider_invocation_id);

CREATE INDEX idx_provider_invocations_state_updated
ON provider_invocations (invocation_state, updated_at DESC, provider_invocation_id);

CREATE INDEX idx_provider_invocations_run_node_created
ON provider_invocations (run_id, node_run_id, created_at DESC, provider_invocation_id);

CREATE TABLE provider_completion_evidence (
  provider_evidence_seq INTEGER PRIMARY KEY AUTOINCREMENT,
  evidence_id TEXT NOT NULL UNIQUE,
  provider_invocation_id TEXT NOT NULL,
  attempt_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  node_run_id TEXT NOT NULL,
  evidence_kind TEXT NOT NULL CHECK (evidence_kind IN ('completion_receipt', 'completion_receipt_attempt', 'stream_observation', 'export_diagnostic', 'repair_diagnostic')),
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (provider_invocation_id) REFERENCES provider_invocations(provider_invocation_id),
  FOREIGN KEY (attempt_id) REFERENCES attempt_executions(attempt_id),
  FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id),
  FOREIGN KEY (node_run_id) REFERENCES node_runs(node_run_id)
);

CREATE INDEX idx_provider_completion_evidence_invocation_seq
ON provider_completion_evidence (provider_invocation_id, provider_evidence_seq ASC);

CREATE INDEX idx_provider_completion_evidence_attempt_kind_created
ON provider_completion_evidence (attempt_id, evidence_kind, created_at DESC, evidence_id);

CREATE TABLE provider_completion_settlements (
  settlement_id TEXT PRIMARY KEY,
  provider_invocation_id TEXT NOT NULL UNIQUE,
  attempt_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  node_run_id TEXT NOT NULL,
  outcome TEXT NOT NULL CHECK (outcome IN ('success', 'failed', 'cancelled')),
  primary_evidence_id TEXT NOT NULL UNIQUE,
  final_text TEXT NOT NULL DEFAULT '',
  structured_payload_json TEXT NOT NULL DEFAULT '{}',
  provider_diagnostics_json TEXT NOT NULL DEFAULT '{}',
  failure_reason_code TEXT NOT NULL DEFAULT '',
  failure_reason_text TEXT NOT NULL DEFAULT '',
  settled_at TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (provider_invocation_id) REFERENCES provider_invocations(provider_invocation_id),
  FOREIGN KEY (attempt_id) REFERENCES attempt_executions(attempt_id),
  FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id),
  FOREIGN KEY (node_run_id) REFERENCES node_runs(node_run_id),
  FOREIGN KEY (primary_evidence_id) REFERENCES provider_completion_evidence(evidence_id)
);

CREATE INDEX idx_provider_completion_settlements_attempt_settled
ON provider_completion_settlements (attempt_id, settled_at DESC, settlement_id);

CREATE INDEX idx_provider_completion_settlements_outcome_settled
ON provider_completion_settlements (outcome, settled_at DESC, settlement_id);

CREATE TABLE attempt_event_log (
  attempt_event_id TEXT PRIMARY KEY,
  attempt_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  node_run_id TEXT NOT NULL,
  seq INTEGER NOT NULL CHECK (seq >= 1),
  event_type TEXT NOT NULL,
  reason_code TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (attempt_id) REFERENCES attempt_executions(attempt_id),
  FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id),
  FOREIGN KEY (node_run_id) REFERENCES node_runs(node_run_id),
  UNIQUE (attempt_id, seq)
);

CREATE INDEX idx_attempt_event_log_attempt_created
ON attempt_event_log (attempt_id, created_at, seq);

CREATE TABLE attempt_outbox (
  outbox_id TEXT PRIMARY KEY,
  attempt_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  node_run_id TEXT NOT NULL,
  terminal_seq INTEGER NOT NULL CHECK (terminal_seq >= 1),
  terminal_state TEXT NOT NULL CHECK (terminal_state IN ('committed_success', 'committed_failure', 'committed_cancelled')),
  payload_json TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('pending', 'published', 'failed')),
  publish_attempts INTEGER NOT NULL DEFAULT 0 CHECK (publish_attempts >= 0),
  last_error TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  published_at TEXT,
  FOREIGN KEY (attempt_id) REFERENCES attempt_executions(attempt_id),
  FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id),
  FOREIGN KEY (node_run_id) REFERENCES node_runs(node_run_id),
  UNIQUE (attempt_id, terminal_seq)
);

CREATE INDEX idx_attempt_outbox_status_created
ON attempt_outbox (status, created_at);

CREATE TABLE attempt_cancel_outbox (
  cancel_outbox_id TEXT PRIMARY KEY,
  attempt_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  node_run_id TEXT NOT NULL,
  session_id TEXT NOT NULL DEFAULT '',
  reason_code TEXT NOT NULL DEFAULT '',
  reason_text TEXT NOT NULL DEFAULT '',
  requested_at TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('pending', 'published')),
  publish_attempts INTEGER NOT NULL DEFAULT 0 CHECK (publish_attempts >= 0),
  last_error TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  published_at TEXT,
  FOREIGN KEY (attempt_id) REFERENCES attempt_executions(attempt_id),
  FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id),
  FOREIGN KEY (node_run_id) REFERENCES node_runs(node_run_id),
  UNIQUE (attempt_id)
);

CREATE INDEX idx_attempt_cancel_outbox_status_created
ON attempt_cancel_outbox (status, created_at);

CREATE TABLE artifacts (
  artifact_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  node_run_id TEXT NOT NULL,
  attempt_id TEXT NOT NULL,
  artifact_kind TEXT NOT NULL DEFAULT 'orchestration_meta' CHECK (artifact_kind IN ('deliverable', 'verification', 'orchestration_meta')),
  artifact_action TEXT NOT NULL DEFAULT 'write' CHECK (artifact_action IN ('write', 'delete')),
  name TEXT NOT NULL,
  mime TEXT NOT NULL,
  path TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
  producer_role TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id),
  FOREIGN KEY (node_run_id) REFERENCES node_runs(node_run_id),
  FOREIGN KEY (producer_role) REFERENCES roles(role_id)
);

CREATE TABLE artifact_contents (
  artifact_id TEXT PRIMARY KEY,
  content BLOB NOT NULL,
  sha256 TEXT NOT NULL,
  size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
  created_at TEXT NOT NULL,
  FOREIGN KEY (artifact_id) REFERENCES artifacts(artifact_id)
);

CREATE TABLE idempotency_keys (
  idempotency_key_id TEXT PRIMARY KEY,
  route TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  client_key TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'completed' CHECK (status IN ('pending', 'completed')),
  response_status INTEGER NOT NULL,
  response_headers_json TEXT NOT NULL,
  response_body_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT '',
  expires_at TEXT,
  UNIQUE (route, actor_id, client_key)
);

CREATE TABLE session_leases (
  session_id TEXT PRIMARY KEY,
  agent_id TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('active', 'expired', 'closed')),
  lease_expires_at TEXT NOT NULL,
  last_heartbeat_at TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (agent_id) REFERENCES agents(agent_id)
);

CREATE TABLE workspaces (
  workspace_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  node_run_id TEXT,
  workspace_path TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('active', 'released', 'failed')),
  created_at TEXT NOT NULL,
  released_at TEXT,
  FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id),
  FOREIGN KEY (node_run_id) REFERENCES node_runs(node_run_id)
);

CREATE TABLE run_worktrees (
  run_id TEXT PRIMARY KEY,
  repo_common_dir TEXT NOT NULL,
  main_worktree_root TEXT NOT NULL,
  project_relpath TEXT NOT NULL,
  worktree_name TEXT NOT NULL,
  seed_commit_oid TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('active', 'cleanup_pending', 'cleanup_failed', 'cleaned')),
  created_at TEXT NOT NULL,
  last_used_at TEXT NOT NULL,
  cleanup_after TEXT,
  cleaned_at TEXT,
  failure_reason TEXT NOT NULL DEFAULT '',
  FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id),
  UNIQUE (main_worktree_root, worktree_name)
);

CREATE INDEX idx_run_worktrees_state_cleanup_after
ON run_worktrees (state, cleanup_after, run_id);

CREATE TABLE run_execution_targets (
  run_id TEXT PRIMARY KEY,
  target_mode TEXT NOT NULL CHECK (target_mode IN ('direct', 'git_worktree')),
  source_root TEXT NOT NULL,
  execution_root TEXT NOT NULL,
  publish_root TEXT NOT NULL,
  source_snapshot_id TEXT NOT NULL DEFAULT '',
  trusted_snapshot_id TEXT NOT NULL DEFAULT '',
  published_snapshot_id TEXT NOT NULL DEFAULT '',
  state TEXT NOT NULL CHECK (state IN ('trusted', 'attempt_dirty', 'unknown')),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id)
);

CREATE TABLE filesystem_snapshots (
  snapshot_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  parent_snapshot_id TEXT,
  snapshot_kind TEXT NOT NULL CHECK (snapshot_kind IN ('baseline', 'attempt_result', 'trusted_promotion')),
  created_at TEXT NOT NULL,
  FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id),
  FOREIGN KEY (parent_snapshot_id) REFERENCES filesystem_snapshots(snapshot_id)
);

CREATE TABLE filesystem_snapshot_entries (
  snapshot_id TEXT NOT NULL,
  path TEXT NOT NULL,
  entry_type TEXT NOT NULL CHECK (entry_type IN ('file', 'delete')),
  blob_id TEXT NOT NULL DEFAULT '',
  size_bytes INTEGER NOT NULL DEFAULT 0 CHECK (size_bytes >= 0),
  mode_bits INTEGER NOT NULL DEFAULT 0 CHECK (mode_bits >= 0),
  mtime_ns INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (snapshot_id, path),
  FOREIGN KEY (snapshot_id) REFERENCES filesystem_snapshots(snapshot_id)
);

CREATE TABLE filesystem_blobs (
  blob_id TEXT PRIMARY KEY,
  size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
  created_at TEXT NOT NULL
);

CREATE INDEX idx_filesystem_snapshots_run_created
ON filesystem_snapshots (run_id, created_at DESC, snapshot_id);

CREATE INDEX idx_filesystem_snapshot_entries_path
ON filesystem_snapshot_entries (path, snapshot_id);

CREATE TABLE run_publication_batches (
  publication_batch_id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL,
  attempt_id TEXT NOT NULL,
  target_mode TEXT NOT NULL CHECK (target_mode IN ('direct', 'git_worktree')),
  status TEXT NOT NULL CHECK (status IN ('succeeded', 'partial_conflict', 'partial_failure', 'failed')),
  published_count INTEGER NOT NULL DEFAULT 0 CHECK (published_count >= 0),
  conflicted_count INTEGER NOT NULL DEFAULT 0 CHECK (conflicted_count >= 0),
  failed_count INTEGER NOT NULL DEFAULT 0 CHECK (failed_count >= 0),
  created_at TEXT NOT NULL,
  FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id),
  FOREIGN KEY (attempt_id) REFERENCES attempt_executions(attempt_id)
);

CREATE TABLE run_publication_entries (
  publication_entry_id TEXT PRIMARY KEY,
  publication_batch_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  attempt_id TEXT NOT NULL,
  path TEXT NOT NULL,
  action TEXT NOT NULL CHECK (action IN ('write', 'delete')),
  status TEXT NOT NULL CHECK (status IN ('published', 'conflicted', 'failed')),
  reason_code TEXT NOT NULL DEFAULT '',
  reason_text TEXT NOT NULL DEFAULT '',
  blob_id TEXT NOT NULL DEFAULT '',
  expected_sha256 TEXT NOT NULL DEFAULT '',
  observed_sha256 TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  FOREIGN KEY (publication_batch_id) REFERENCES run_publication_batches(publication_batch_id),
  FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id),
  FOREIGN KEY (attempt_id) REFERENCES attempt_executions(attempt_id)
);

CREATE INDEX idx_run_publication_entries_attempt_created
ON run_publication_entries (attempt_id, created_at DESC, publication_entry_id);

CREATE TABLE run_publication_paths (
  run_id TEXT NOT NULL,
  path TEXT NOT NULL,
  artifact_action TEXT NOT NULL CHECK (artifact_action IN ('write', 'delete')),
  blob_id TEXT NOT NULL DEFAULT '',
  sha256 TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL,
  PRIMARY KEY (run_id, path),
  FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id)
);

CREATE TABLE mailbox_messages (
  message_id TEXT PRIMARY KEY,
  message_seq INTEGER UNIQUE,
  run_id TEXT NOT NULL,
  from_agent_id TEXT NOT NULL,
  to_agent_id TEXT NOT NULL,
  thread_id TEXT NOT NULL,
  message_type TEXT NOT NULL CHECK (message_type IN ('clarification', 'escalation', 'decision_request', 'status_update')),
  priority TEXT NOT NULL CHECK (priority IN ('low', 'normal', 'high', 'critical')),
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  read_at TEXT,
  FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id),
  FOREIGN KEY (from_agent_id) REFERENCES agents(agent_id),
  FOREIGN KEY (to_agent_id) REFERENCES agents(agent_id)
);

CREATE TRIGGER mailbox_messages_assign_message_seq
AFTER INSERT ON mailbox_messages
FOR EACH ROW
WHEN NEW.message_seq IS NULL
BEGIN
  UPDATE mailbox_messages
  SET message_seq = NEW.rowid
  WHERE rowid = NEW.rowid;
END;

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

CREATE TRIGGER run_planning_outputs_append_only_no_update
BEFORE UPDATE ON run_planning_outputs
BEGIN
  SELECT RAISE(ABORT, 'append-only table: run_planning_outputs cannot be updated');
END;

CREATE TRIGGER run_planning_outputs_append_only_no_delete
BEFORE DELETE ON run_planning_outputs
BEGIN
  SELECT RAISE(ABORT, 'append-only table: run_planning_outputs cannot be deleted');
END;

CREATE INDEX idx_mailbox_messages_run_seq
ON mailbox_messages (run_id, message_seq DESC);

CREATE INDEX idx_mailbox_messages_run_to_agent_seq
ON mailbox_messages (run_id, to_agent_id, message_seq DESC);

CREATE INDEX idx_mailbox_messages_run_thread_seq
ON mailbox_messages (run_id, thread_id, message_seq DESC);

CREATE TABLE run_collection_events (
  collection_event_seq INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL UNIQUE,
  run_id TEXT NOT NULL,
  event_type TEXT NOT NULL CHECK (
    event_type = 'approval.recorded'
    OR event_type GLOB 'run.*'
  ),
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id)
);

CREATE INDEX idx_run_collection_events_run_seq
ON run_collection_events (run_id, collection_event_seq ASC);

CREATE INDEX idx_run_collection_events_type_seq
ON run_collection_events (event_type, collection_event_seq ASC);

CREATE TABLE kv_store (
  kv_key TEXT PRIMARY KEY,
  kv_value TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TRIGGER transitions_append_only_no_update
BEFORE UPDATE ON transitions
BEGIN
  SELECT RAISE(ABORT, 'append-only table: transitions cannot be updated');
END;

CREATE TRIGGER transitions_append_only_no_delete
BEFORE DELETE ON transitions
BEGIN
  SELECT RAISE(ABORT, 'append-only table: transitions cannot be deleted');
END;

CREATE TRIGGER events_append_only_no_update
BEFORE UPDATE ON events
BEGIN
  SELECT RAISE(ABORT, 'append-only table: events cannot be updated');
END;

CREATE TRIGGER events_append_only_no_delete
BEFORE DELETE ON events
BEGIN
  SELECT RAISE(ABORT, 'append-only table: events cannot be deleted');
END;

CREATE TRIGGER run_plan_revisions_append_only_no_update
BEFORE UPDATE ON run_plan_revisions
BEGIN
  SELECT RAISE(ABORT, 'append-only table: run_plan_revisions cannot be updated');
END;

CREATE TRIGGER run_plan_revisions_append_only_no_delete
BEFORE DELETE ON run_plan_revisions
BEGIN
  SELECT RAISE(ABORT, 'append-only table: run_plan_revisions cannot be deleted');
END;

CREATE TRIGGER run_plan_reviews_append_only_no_update
BEFORE UPDATE ON run_plan_reviews
BEGIN
  SELECT RAISE(ABORT, 'append-only table: run_plan_reviews cannot be updated');
END;

CREATE TRIGGER run_plan_reviews_append_only_no_delete
BEFORE DELETE ON run_plan_reviews
BEGIN
  SELECT RAISE(ABORT, 'append-only table: run_plan_reviews cannot be deleted');
END;

CREATE TRIGGER run_collection_events_append_only_no_update
BEFORE UPDATE ON run_collection_events
BEGIN
  SELECT RAISE(ABORT, 'append-only table: run_collection_events cannot be updated');
END;

CREATE TRIGGER run_collection_events_append_only_no_delete
BEFORE DELETE ON run_collection_events
BEGIN
  SELECT RAISE(ABORT, 'append-only table: run_collection_events cannot be deleted');
END;

CREATE TABLE token_usage (
  usage_id           TEXT PRIMARY KEY,
  run_id             TEXT NOT NULL REFERENCES workflow_runs(run_id),
  node_run_id        TEXT NOT NULL REFERENCES node_runs(node_run_id),
  attempt_id         TEXT NOT NULL,
  required_role      TEXT NOT NULL,
  model              TEXT NOT NULL DEFAULT '',
  variant            TEXT NOT NULL DEFAULT '',
  input_tokens       INTEGER NOT NULL DEFAULT 0,
  output_tokens      INTEGER NOT NULL DEFAULT 0,
  total_tokens       INTEGER NOT NULL DEFAULT 0,
  reasoning_tokens   INTEGER NOT NULL DEFAULT 0,
  cache_read_tokens  INTEGER NOT NULL DEFAULT 0,
  cache_write_tokens INTEGER NOT NULL DEFAULT 0,
  created_at         TEXT NOT NULL
);

CREATE INDEX idx_token_usage_run ON token_usage(run_id);
CREATE INDEX idx_token_usage_created ON token_usage(created_at);
CREATE UNIQUE INDEX idx_token_usage_attempt_unique ON token_usage(run_id, node_run_id, attempt_id);

CREATE TABLE planning_sessions (
  planning_session_id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  title_state TEXT NOT NULL CHECK (title_state IN ('placeholder', 'generated', 'custom')),
  lifecycle_state TEXT NOT NULL CHECK (lifecycle_state IN ('active', 'archived')),
  conversation_state TEXT NOT NULL CHECK (conversation_state IN ('ready', 'processing')),
  active_leaf_message_id TEXT,
  current_operation_id TEXT,
  current_title_operation_id TEXT,
  current_draft_id TEXT,
  context_snapshot_id TEXT,
  project_root TEXT NOT NULL,
  model_selection_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX idx_planning_sessions_updated_at
ON planning_sessions (updated_at DESC, planning_session_id);

CREATE INDEX idx_planning_sessions_lifecycle_updated_at
ON planning_sessions (lifecycle_state, updated_at DESC, planning_session_id);

-- Planner messages form a persistent tree: each message points at its parent
-- (NULL = conversation root). Regenerate/edit append sibling branches and the
-- session's active_leaf_message_id selects the visible root->leaf path. Nothing
-- is ever deleted, so every branch and its downstream stay recoverable.
CREATE TABLE planning_messages (
  planning_message_id TEXT PRIMARY KEY,
  planning_session_id TEXT NOT NULL,
  parent_message_id TEXT,
  role TEXT NOT NULL CHECK (role IN ('user', 'assistant')),
  content_markdown TEXT NOT NULL,
  question_payload_json TEXT NOT NULL,
  answer_payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (planning_session_id) REFERENCES planning_sessions(planning_session_id) ON DELETE CASCADE,
  FOREIGN KEY (parent_message_id) REFERENCES planning_messages(planning_message_id) ON DELETE CASCADE
);

CREATE INDEX idx_planning_messages_session_created
ON planning_messages (planning_session_id, created_at, planning_message_id);

-- Sibling/child lookups (branch metadata, active-path descent) query by parent.
CREATE INDEX idx_planning_messages_parent
ON planning_messages (planning_session_id, parent_message_id, created_at);

CREATE TABLE planning_drafts (
  planning_draft_id TEXT PRIMARY KEY,
  planning_session_id TEXT NOT NULL,
  decree_title TEXT NOT NULL,
  decree_text TEXT NOT NULL,
  summary_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (planning_session_id) REFERENCES planning_sessions(planning_session_id) ON DELETE CASCADE
);

CREATE INDEX idx_planning_drafts_session_created
ON planning_drafts (planning_session_id, created_at DESC, planning_draft_id);

CREATE TABLE planning_context_snapshots (
  planning_context_snapshot_id TEXT PRIMARY KEY,
  planning_session_id TEXT NOT NULL,
  summary_markdown TEXT NOT NULL,
  included_files_json TEXT NOT NULL,
  excluded_files_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (planning_session_id) REFERENCES planning_sessions(planning_session_id) ON DELETE CASCADE
);

CREATE INDEX idx_planning_context_snapshots_session_created
ON planning_context_snapshots (planning_session_id, created_at DESC, planning_context_snapshot_id);

CREATE TABLE planning_operations (
  operation_id TEXT PRIMARY KEY,
  acp_run_id TEXT NOT NULL,
  planning_session_id TEXT NOT NULL,
  planning_context_snapshot_id TEXT,
  type TEXT NOT NULL CHECK (type IN ('reply', 'formalize', 'regenerate', 'generate_title')),
  status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
  request_payload_json TEXT NOT NULL,
  lease_owner TEXT NOT NULL DEFAULT '',
  lease_expires_at TEXT,
  lease_version INTEGER NOT NULL DEFAULT 0,
  last_heartbeat_at TEXT,
  provider_session_id TEXT,
  active_provider_invocation_id TEXT,
  observed_output_text TEXT NOT NULL DEFAULT '',
  error TEXT,
  result_ref TEXT,
  canonical_settlement_id TEXT,
  created_at TEXT NOT NULL,
  started_at TEXT,
  finished_at TEXT,
  FOREIGN KEY (planning_session_id) REFERENCES planning_sessions(planning_session_id) ON DELETE CASCADE,
  FOREIGN KEY (planning_context_snapshot_id) REFERENCES planning_context_snapshots(planning_context_snapshot_id),
  FOREIGN KEY (active_provider_invocation_id) REFERENCES planning_provider_invocations(planning_provider_invocation_id),
  FOREIGN KEY (canonical_settlement_id) REFERENCES planning_provider_completion_settlements(settlement_id)
);

CREATE INDEX idx_planning_operations_session_created
ON planning_operations (planning_session_id, created_at DESC, operation_id);

CREATE UNIQUE INDEX idx_planning_operations_acp_run_id
ON planning_operations (acp_run_id);

CREATE INDEX idx_planning_operations_status_created
ON planning_operations (status, created_at ASC, operation_id);

CREATE INDEX idx_planning_operations_active_provider_invocation
ON planning_operations (active_provider_invocation_id);

CREATE INDEX idx_planning_operations_canonical_settlement
ON planning_operations (canonical_settlement_id);

CREATE INDEX idx_planning_operations_context_snapshot
ON planning_operations (planning_context_snapshot_id);

CREATE TRIGGER planning_operation_context_binding_immutable
BEFORE UPDATE OF planning_context_snapshot_id ON planning_operations
WHEN NEW.planning_context_snapshot_id IS NOT OLD.planning_context_snapshot_id
BEGIN SELECT RAISE(ABORT, 'planning operation context binding is immutable'); END;

CREATE UNIQUE INDEX idx_planning_operations_single_active_interactive
ON planning_operations (planning_session_id)
WHERE type IN ('reply', 'formalize', 'regenerate') AND status IN ('queued', 'running');

CREATE UNIQUE INDEX idx_planning_operations_single_active_title
ON planning_operations (planning_session_id)
WHERE type = 'generate_title' AND status IN ('queued', 'running');

CREATE TABLE planning_provider_invocations (
  planning_provider_invocation_id TEXT PRIMARY KEY,
  planning_operation_id TEXT NOT NULL,
  planning_session_id TEXT NOT NULL,
  logical_session_id TEXT NOT NULL DEFAULT '',
  provider_session_id TEXT NOT NULL DEFAULT '',
  invocation_state TEXT NOT NULL CHECK (invocation_state IN ('issued', 'open', 'receipt_persisted', 'settled_success', 'settled_failed', 'settled_cancelled', 'superseded', 'invalid_handshake')),
  completion_capability_hash TEXT NOT NULL DEFAULT '',
  completion_capability_issued_at TEXT NOT NULL,
  completion_capability_used_at TEXT,
  opened_at TEXT,
  receipt_persisted_at TEXT,
  settled_at TEXT,
  invalid_reason TEXT NOT NULL DEFAULT '',
  failure_reason_code TEXT NOT NULL DEFAULT '',
  failure_reason_text TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (planning_operation_id) REFERENCES planning_operations(operation_id) ON DELETE CASCADE,
  FOREIGN KEY (planning_session_id) REFERENCES planning_sessions(planning_session_id) ON DELETE CASCADE
);

CREATE INDEX idx_planning_provider_invocations_operation_created
ON planning_provider_invocations (planning_operation_id, created_at DESC, planning_provider_invocation_id);

CREATE INDEX idx_planning_provider_invocations_state_updated
ON planning_provider_invocations (invocation_state, updated_at DESC, planning_provider_invocation_id);

CREATE TABLE planning_provider_completion_evidence (
  planning_provider_evidence_seq INTEGER PRIMARY KEY AUTOINCREMENT,
  evidence_id TEXT NOT NULL UNIQUE,
  planning_provider_invocation_id TEXT NOT NULL,
  planning_operation_id TEXT NOT NULL,
  planning_session_id TEXT NOT NULL,
  evidence_kind TEXT NOT NULL CHECK (evidence_kind IN ('completion_receipt', 'stream_observation', 'export_diagnostic', 'repair_diagnostic')),
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (planning_provider_invocation_id) REFERENCES planning_provider_invocations(planning_provider_invocation_id),
  FOREIGN KEY (planning_operation_id) REFERENCES planning_operations(operation_id) ON DELETE CASCADE,
  FOREIGN KEY (planning_session_id) REFERENCES planning_sessions(planning_session_id) ON DELETE CASCADE
);

CREATE INDEX idx_planning_provider_completion_evidence_invocation_seq
ON planning_provider_completion_evidence (planning_provider_invocation_id, planning_provider_evidence_seq ASC);

CREATE INDEX idx_planning_provider_completion_evidence_operation_kind_created
ON planning_provider_completion_evidence (planning_operation_id, evidence_kind, created_at DESC, evidence_id);

CREATE UNIQUE INDEX idx_planning_provider_completion_receipt_invocation
ON planning_provider_completion_evidence (planning_provider_invocation_id)
WHERE evidence_kind = 'completion_receipt';

CREATE TABLE planning_provider_completion_settlements (
  settlement_id TEXT PRIMARY KEY,
  planning_provider_invocation_id TEXT NOT NULL UNIQUE,
  planning_operation_id TEXT NOT NULL,
  planning_session_id TEXT NOT NULL,
  outcome TEXT NOT NULL CHECK (outcome IN ('success', 'failed', 'cancelled')),
  primary_evidence_id TEXT NOT NULL UNIQUE,
  final_text TEXT NOT NULL DEFAULT '',
  structured_payload_json TEXT NOT NULL DEFAULT '{}',
  failure_reason_code TEXT NOT NULL DEFAULT '',
  failure_reason_text TEXT NOT NULL DEFAULT '',
  settled_at TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (planning_provider_invocation_id) REFERENCES planning_provider_invocations(planning_provider_invocation_id),
  FOREIGN KEY (planning_operation_id) REFERENCES planning_operations(operation_id) ON DELETE CASCADE,
  FOREIGN KEY (planning_session_id) REFERENCES planning_sessions(planning_session_id) ON DELETE CASCADE,
  FOREIGN KEY (primary_evidence_id) REFERENCES planning_provider_completion_evidence(evidence_id)
);

CREATE INDEX idx_planning_provider_completion_settlements_operation_settled
ON planning_provider_completion_settlements (planning_operation_id, settled_at DESC, settlement_id);

CREATE INDEX idx_planning_provider_completion_settlements_outcome_settled
ON planning_provider_completion_settlements (outcome, settled_at DESC, settlement_id);

CREATE TABLE planning_operation_events (
  event_id TEXT PRIMARY KEY,
  operation_id TEXT NOT NULL,
  seq INTEGER NOT NULL CHECK (seq >= 1),
  event_type TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (operation_id) REFERENCES planning_operations(operation_id) ON DELETE CASCADE,
  UNIQUE (operation_id, seq)
);

CREATE INDEX idx_planning_operation_events_operation_seq
ON planning_operation_events (operation_id, seq ASC, created_at ASC);
