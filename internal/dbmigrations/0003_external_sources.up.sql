-- Provider-neutral external intake and immutable provenance.
CREATE TABLE external_connections (
  connection_id TEXT PRIMARY KEY,
  provider TEXT NOT NULL CHECK (provider IN ('github')),
  credential_strategy TEXT NOT NULL CHECK (credential_strategy IN ('app_private_key', 'user_access_token')),
  display_name TEXT NOT NULL,
  app_id TEXT NOT NULL,
  client_id TEXT NOT NULL,
  app_slug TEXT NOT NULL,
  installation_id TEXT NOT NULL,
  installation_account_id TEXT NOT NULL DEFAULT '',
  installation_account_login TEXT NOT NULL DEFAULT '',
  credential_handle TEXT NOT NULL,
  lifecycle TEXT NOT NULL CHECK (lifecycle IN ('enabled', 'disabled', 'removal_pending', 'removed')),
  health_class TEXT NOT NULL CHECK (health_class IN ('missing_credential', 'never_checked', 'healthy', 'permission_mismatch', 'revoked', 'suspended', 'rate_limited', 'unavailable', 'disabled', 'removal_pending', 'removed')),
  failure_class TEXT NOT NULL DEFAULT '',
  last_checked_at TEXT,
  last_success_at TEXT,
  row_version INTEGER NOT NULL DEFAULT 1 CHECK (row_version > 0),
  credential_generation INTEGER NOT NULL DEFAULT 0 CHECK (credential_generation >= 0),
  validated_generation INTEGER NOT NULL DEFAULT 0 CHECK (validated_generation >= 0),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  CHECK ((installation_account_id = '') = (installation_account_login = '')),
  CHECK (validated_generation <= credential_generation),
  CHECK (credential_strategy <> 'user_access_token' OR length(trim(app_slug)) > 0),
  CHECK (health_class <> 'healthy' OR (length(trim(app_slug)) > 0 AND length(trim(installation_account_id)) > 0 AND length(trim(installation_account_login)) > 0))
);

CREATE UNIQUE INDEX idx_external_connections_active_installation
ON external_connections (provider, installation_id)
WHERE lifecycle <> 'removed';

CREATE TRIGGER external_connection_identity_pinned
BEFORE UPDATE OF installation_account_id ON external_connections
WHEN OLD.installation_account_id <> ''
  AND NEW.installation_account_id <> OLD.installation_account_id
BEGIN SELECT RAISE(ABORT, 'external installation account identity is immutable'); END;

CREATE TRIGGER external_connection_app_identity_pinned
BEFORE UPDATE OF provider, credential_strategy, app_id, client_id, installation_id, credential_handle ON external_connections
WHEN NEW.provider <> OLD.provider
  OR NEW.credential_strategy <> OLD.credential_strategy
  OR NEW.app_id <> OLD.app_id
  OR NEW.client_id <> OLD.client_id
  OR NEW.installation_id <> OLD.installation_id
  OR NEW.credential_handle <> OLD.credential_handle
BEGIN SELECT RAISE(ABORT, 'external App and installation identity is immutable'); END;

CREATE TABLE external_sources (
  source_id TEXT PRIMARY KEY,
  connection_id TEXT NOT NULL,
  provider TEXT NOT NULL CHECK (provider IN ('github')),
  source_kind TEXT NOT NULL CHECK (source_kind IN ('issue')),
  provider_native_id TEXT NOT NULL,
  repository_native_id TEXT NOT NULL,
  repository_locator TEXT NOT NULL,
  issue_number INTEGER NOT NULL CHECK (issue_number > 0),
  canonical_url TEXT NOT NULL,
  row_version INTEGER NOT NULL DEFAULT 1 CHECK (row_version > 0),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (connection_id) REFERENCES external_connections(connection_id),
  UNIQUE (connection_id, source_kind, provider_native_id)
);

CREATE INDEX idx_external_sources_connection ON external_sources (connection_id, source_kind);

CREATE TABLE external_source_aliases (
  alias_id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL,
  repository_native_id TEXT NOT NULL,
  repository_locator TEXT NOT NULL,
  issue_number INTEGER NOT NULL CHECK (issue_number > 0),
  canonical_url TEXT NOT NULL,
  valid_from TEXT NOT NULL,
  valid_until TEXT,
  FOREIGN KEY (source_id) REFERENCES external_sources(source_id),
  UNIQUE (source_id, repository_native_id, issue_number, valid_from)
);

CREATE TABLE external_source_snapshots (
  snapshot_id TEXT PRIMARY KEY,
  source_id TEXT NOT NULL,
  predecessor_snapshot_id TEXT,
  normalized_issue_json TEXT NOT NULL CHECK (json_valid(normalized_issue_json)),
  import_profile_json TEXT NOT NULL CHECK (json_valid(import_profile_json)),
  import_profile_digest TEXT NOT NULL,
  provider_updated_at TEXT NOT NULL,
  fetched_at TEXT NOT NULL,
  content_digest TEXT NOT NULL,
  etag TEXT NOT NULL DEFAULT '',
  comments_etag TEXT NOT NULL DEFAULT '',
  last_modified TEXT NOT NULL DEFAULT '',
  provider_comment_count INTEGER NOT NULL DEFAULT 0 CHECK (provider_comment_count >= 0),
  fetched_comment_count INTEGER NOT NULL DEFAULT 0 CHECK (fetched_comment_count >= 0),
  fetched_comment_pages INTEGER NOT NULL DEFAULT 0 CHECK (fetched_comment_pages >= 0),
  omitted_comment_count INTEGER NOT NULL DEFAULT 0 CHECK (omitted_comment_count >= 0),
  omitted_comment_pages INTEGER NOT NULL DEFAULT 0 CHECK (omitted_comment_pages >= 0),
  retained_comment_count INTEGER NOT NULL DEFAULT 0 CHECK (retained_comment_count BETWEEN 0 AND 100),
  retained_comment_bytes INTEGER NOT NULL DEFAULT 0 CHECK (retained_comment_bytes BETWEEN 0 AND 131072),
  comments_truncated INTEGER NOT NULL DEFAULT 0 CHECK (comments_truncated IN (0, 1)),
  truncation_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(truncation_json)),
  created_at TEXT NOT NULL,
  FOREIGN KEY (source_id) REFERENCES external_sources(source_id),
  FOREIGN KEY (predecessor_snapshot_id) REFERENCES external_source_snapshots(snapshot_id)
);

CREATE INDEX idx_external_snapshots_source_created
ON external_source_snapshots (source_id, created_at DESC, snapshot_id);
CREATE UNIQUE INDEX idx_external_snapshots_version_fence
ON external_source_snapshots (source_id, COALESCE(predecessor_snapshot_id, ''), import_profile_digest, content_digest);

CREATE TABLE external_source_snapshot_comments (
  snapshot_id TEXT NOT NULL,
  provider_comment_id TEXT NOT NULL,
  ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
  author_display_identifier TEXT NOT NULL,
  body TEXT NOT NULL,
  canonical_url TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  original_bytes INTEGER NOT NULL CHECK (original_bytes >= 0),
  retained_bytes INTEGER NOT NULL CHECK (retained_bytes BETWEEN 0 AND 32768),
  truncation_reason TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (snapshot_id, provider_comment_id),
  FOREIGN KEY (snapshot_id) REFERENCES external_source_snapshots(snapshot_id)
);

CREATE INDEX idx_external_snapshot_comments_order
ON external_source_snapshot_comments (snapshot_id, ordinal, provider_comment_id);

-- target_id is deliberately polymorphic and detached. Creation validates the
-- target transactionally, while immutable provenance can outlive that target.
CREATE TABLE external_source_links (
  link_id TEXT PRIMARY KEY,
  target_kind TEXT NOT NULL CHECK (target_kind IN ('planning_session', 'decree')),
  target_id TEXT NOT NULL,
  source_id TEXT NOT NULL,
  used_snapshot_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  FOREIGN KEY (source_id) REFERENCES external_sources(source_id),
  FOREIGN KEY (used_snapshot_id) REFERENCES external_source_snapshots(snapshot_id),
  UNIQUE (target_kind, target_id, source_id)
);

CREATE INDEX idx_external_links_target ON external_source_links (target_kind, target_id);

CREATE TABLE external_source_link_heads (
  link_id TEXT PRIMARY KEY,
  latest_snapshot_id TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('never_checked', 'unchanged', 'changed', 'inaccessible_or_missing', 'deleted', 'rate_limited', 'connection_disabled', 'service_unavailable', 'revoked', 'permission_mismatch', 'timeout', 'malformed_response', 'oversized_response')),
  checked_at TEXT,
  row_version INTEGER NOT NULL DEFAULT 1 CHECK (row_version > 0),
  FOREIGN KEY (link_id) REFERENCES external_source_links(link_id),
  FOREIGN KEY (latest_snapshot_id) REFERENCES external_source_snapshots(snapshot_id)
);

CREATE TABLE external_source_observations (
  observation_id TEXT PRIMARY KEY,
  link_id TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  prior_snapshot_id TEXT,
  result_snapshot_id TEXT,
  outcome_class TEXT NOT NULL CHECK (outcome_class IN ('unchanged', 'changed', 'superseded', 'inaccessible_or_missing', 'deleted', 'rate_limited', 'connection_disabled', 'service_unavailable', 'revoked', 'permission_mismatch', 'timeout', 'malformed_response', 'oversized_response')),
  transfer_json TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(transfer_json)),
  started_at TEXT NOT NULL,
  finished_at TEXT NOT NULL,
  retry_at TEXT,
  FOREIGN KEY (link_id) REFERENCES external_source_links(link_id),
  FOREIGN KEY (prior_snapshot_id) REFERENCES external_source_snapshots(snapshot_id),
  FOREIGN KEY (result_snapshot_id) REFERENCES external_source_snapshots(snapshot_id)
);

CREATE INDEX idx_external_observations_link_finished
ON external_source_observations (link_id, finished_at DESC, observation_id);

-- planning_session_id is detached so adoption history survives session removal.
CREATE TABLE planning_external_source_adoptions (
  adoption_id TEXT PRIMARY KEY,
  link_id TEXT NOT NULL,
  planning_session_id TEXT NOT NULL,
  snapshot_id TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  adopted_at TEXT NOT NULL,
  FOREIGN KEY (link_id) REFERENCES external_source_links(link_id),
  FOREIGN KEY (snapshot_id) REFERENCES external_source_snapshots(snapshot_id)
);

CREATE INDEX idx_planning_external_adoptions_active
ON planning_external_source_adoptions (planning_session_id, adopted_at DESC, adoption_id DESC);

-- operation_id is detached so the exact immutable binding remains historical
-- evidence after operation lifecycle cleanup.
CREATE TABLE planning_operation_external_snapshots (
  operation_id TEXT NOT NULL,
  snapshot_id TEXT NOT NULL,
  assembler_version TEXT NOT NULL,
  context_digest TEXT NOT NULL,
  issue_bytes INTEGER NOT NULL CHECK (issue_bytes BETWEEN 0 AND 131072),
  comment_bytes INTEGER NOT NULL CHECK (comment_bytes BETWEEN 0 AND 65536),
  bound_at TEXT NOT NULL,
  PRIMARY KEY (operation_id, snapshot_id),
  FOREIGN KEY (snapshot_id) REFERENCES external_source_snapshots(snapshot_id)
);

CREATE TABLE external_integration_audit_events (
  audit_event_id TEXT PRIMARY KEY,
  event_sequence INTEGER NOT NULL UNIQUE CHECK (event_sequence > 0),
  hash_version TEXT NOT NULL CHECK (hash_version IN ('v1')),
  connection_id TEXT,
  source_id TEXT,
  event_type TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  safe_payload_json TEXT NOT NULL CHECK (json_valid(safe_payload_json)),
  previous_hash TEXT NOT NULL CHECK (length(previous_hash) = 64 AND previous_hash = lower(previous_hash) AND previous_hash NOT GLOB '*[^0-9a-f]*'),
  event_hash TEXT NOT NULL UNIQUE CHECK (length(event_hash) = 64 AND event_hash = lower(event_hash) AND event_hash NOT GLOB '*[^0-9a-f]*'),
  created_at TEXT NOT NULL,
  FOREIGN KEY (connection_id) REFERENCES external_connections(connection_id),
  FOREIGN KEY (source_id) REFERENCES external_sources(source_id)
);

CREATE TABLE external_integration_audit_head (
  head_id INTEGER PRIMARY KEY CHECK (head_id = 1),
  last_sequence INTEGER NOT NULL CHECK (last_sequence >= 0),
  last_hash TEXT NOT NULL CHECK (length(last_hash) = 64 AND last_hash = lower(last_hash) AND last_hash NOT GLOB '*[^0-9a-f]*')
);

INSERT INTO external_integration_audit_head (head_id, last_sequence, last_hash)
VALUES (1, 0, '0000000000000000000000000000000000000000000000000000000000000000');

-- Source versions, aliases, links, observations, adoptions, operation bindings,
-- and audit events are immutable. Link heads and the fenced audit head are the
-- only mutable external provenance projections.
CREATE TRIGGER external_aliases_no_update BEFORE UPDATE ON external_source_aliases BEGIN SELECT RAISE(ABORT, 'external source aliases are immutable'); END;
CREATE TRIGGER external_aliases_no_delete BEFORE DELETE ON external_source_aliases BEGIN SELECT RAISE(ABORT, 'external source aliases are immutable'); END;
CREATE TRIGGER external_snapshots_no_update BEFORE UPDATE ON external_source_snapshots BEGIN SELECT RAISE(ABORT, 'external snapshots are immutable'); END;
CREATE TRIGGER external_snapshots_no_delete BEFORE DELETE ON external_source_snapshots BEGIN SELECT RAISE(ABORT, 'external snapshots are immutable'); END;
CREATE TRIGGER external_snapshot_comments_no_update BEFORE UPDATE ON external_source_snapshot_comments BEGIN SELECT RAISE(ABORT, 'external snapshot comments are immutable'); END;
CREATE TRIGGER external_snapshot_comments_no_delete BEFORE DELETE ON external_source_snapshot_comments BEGIN SELECT RAISE(ABORT, 'external snapshot comments are immutable'); END;
CREATE TRIGGER external_links_no_update BEFORE UPDATE ON external_source_links BEGIN SELECT RAISE(ABORT, 'external source links are immutable'); END;
CREATE TRIGGER external_links_no_delete BEFORE DELETE ON external_source_links BEGIN SELECT RAISE(ABORT, 'external source links are immutable'); END;
CREATE TRIGGER external_observations_no_update BEFORE UPDATE ON external_source_observations BEGIN SELECT RAISE(ABORT, 'external observations are immutable'); END;
CREATE TRIGGER external_observations_no_delete BEFORE DELETE ON external_source_observations BEGIN SELECT RAISE(ABORT, 'external observations are immutable'); END;
CREATE TRIGGER planning_external_adoptions_no_update BEFORE UPDATE ON planning_external_source_adoptions BEGIN SELECT RAISE(ABORT, 'external source adoptions are immutable'); END;
CREATE TRIGGER planning_external_adoptions_no_delete BEFORE DELETE ON planning_external_source_adoptions BEGIN SELECT RAISE(ABORT, 'external source adoptions are immutable'); END;
CREATE TRIGGER planning_operation_external_no_update BEFORE UPDATE ON planning_operation_external_snapshots BEGIN SELECT RAISE(ABORT, 'planning operation source bindings are immutable'); END;
CREATE TRIGGER planning_operation_external_no_delete BEFORE DELETE ON planning_operation_external_snapshots BEGIN SELECT RAISE(ABORT, 'planning operation source bindings are immutable'); END;
CREATE TRIGGER external_audit_no_update BEFORE UPDATE ON external_integration_audit_events BEGIN SELECT RAISE(ABORT, 'external integration audit is append-only'); END;
CREATE TRIGGER external_audit_no_delete BEFORE DELETE ON external_integration_audit_events BEGIN SELECT RAISE(ABORT, 'external integration audit is append-only'); END;
CREATE TRIGGER external_audit_head_no_insert BEFORE INSERT ON external_integration_audit_head BEGIN SELECT RAISE(ABORT, 'external integration audit head already exists'); END;
CREATE TRIGGER external_audit_head_no_delete BEFORE DELETE ON external_integration_audit_head BEGIN SELECT RAISE(ABORT, 'external integration audit head cannot be deleted'); END;
CREATE TRIGGER external_audit_insert_fenced
BEFORE INSERT ON external_integration_audit_events
WHEN NOT EXISTS (SELECT 1 FROM external_integration_audit_head WHERE head_id = 1)
  OR NEW.event_sequence <> (SELECT last_sequence + 1 FROM external_integration_audit_head WHERE head_id = 1)
  OR NEW.previous_hash <> (SELECT last_hash FROM external_integration_audit_head WHERE head_id = 1)
BEGIN SELECT RAISE(ABORT, 'external integration audit append is not based on the current head'); END;
CREATE TRIGGER external_audit_head_fenced
BEFORE UPDATE ON external_integration_audit_head
WHEN NEW.head_id <> 1
  OR NEW.last_sequence <> OLD.last_sequence + 1
  OR NOT EXISTS (
    SELECT 1 FROM external_integration_audit_events
    WHERE event_sequence = NEW.last_sequence AND event_hash = NEW.last_hash
  )
BEGIN SELECT RAISE(ABORT, 'external integration audit head update is invalid'); END;
