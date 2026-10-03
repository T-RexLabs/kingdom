CREATE TABLE agent_workspace_configuration (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  voice_kind TEXT NOT NULL CHECK (voice_kind IN ('neutral-v1', 'theme', 'custom')),
  voice_theme_id TEXT NOT NULL DEFAULT '',
  voice_theme_digest TEXT NOT NULL DEFAULT '',
  voice_instruction TEXT NOT NULL DEFAULT '' CHECK (length(voice_instruction) <= 2000),
  row_version INTEGER NOT NULL CHECK (row_version >= 1),
  updated_at TEXT NOT NULL,
  CHECK (
    (voice_kind = 'neutral-v1' AND voice_theme_id = '' AND voice_theme_digest = '' AND voice_instruction = '')
    OR
    (voice_kind = 'theme' AND voice_theme_id <> '' AND length(voice_theme_digest) = 64 AND length(trim(voice_instruction)) > 0)
    OR
    (voice_kind = 'custom' AND voice_theme_id = '' AND voice_theme_digest = '' AND length(trim(voice_instruction)) > 0)
  )
);

INSERT INTO agent_workspace_configuration (
  singleton, voice_kind, voice_theme_id, voice_theme_digest, voice_instruction, row_version, updated_at
) VALUES (1, 'neutral-v1', '', '', '', 1, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));

CREATE TABLE agent_role_configuration (
  role_id TEXT PRIMARY KEY REFERENCES roles(role_id),
  guidance_override TEXT CHECK (guidance_override IS NULL OR length(guidance_override) <= 8000),
  voice_kind TEXT CHECK (voice_kind IS NULL OR voice_kind IN ('neutral-v1', 'theme', 'custom')),
  voice_theme_id TEXT NOT NULL DEFAULT '',
  voice_theme_digest TEXT NOT NULL DEFAULT '',
  voice_instruction TEXT NOT NULL DEFAULT '' CHECK (length(voice_instruction) <= 2000),
  row_version INTEGER NOT NULL CHECK (row_version >= 1),
  updated_at TEXT NOT NULL,
  CHECK (voice_kind IS NOT NULL OR (voice_theme_id = '' AND voice_theme_digest = '' AND voice_instruction = '')),
  CHECK (
    (voice_kind IS NULL AND voice_theme_id = '' AND voice_theme_digest = '' AND voice_instruction = '')
    OR
    (voice_kind = 'neutral-v1' AND voice_theme_id = '' AND voice_theme_digest = '' AND voice_instruction = '')
    OR
    (voice_kind = 'theme' AND voice_theme_id <> '' AND length(voice_theme_digest) = 64 AND length(trim(voice_instruction)) > 0)
    OR
    (voice_kind = 'custom' AND voice_theme_id = '' AND voice_theme_digest = '' AND length(trim(voice_instruction)) > 0)
  )
);

CREATE TABLE prompt_catalog_revisions (
  prompt_catalog_digest TEXT PRIMARY KEY CHECK (length(prompt_catalog_digest) = 64),
  catalog_json TEXT NOT NULL CHECK (json_valid(catalog_json)),
  created_at TEXT NOT NULL
);

CREATE TRIGGER prompt_catalog_revisions_immutable_update
BEFORE UPDATE ON prompt_catalog_revisions
BEGIN SELECT RAISE(ABORT, 'prompt catalog revisions are immutable'); END;

CREATE TRIGGER prompt_catalog_revisions_immutable_delete
BEFORE DELETE ON prompt_catalog_revisions
BEGIN SELECT RAISE(ABORT, 'prompt catalog revisions are immutable'); END;

CREATE TABLE run_agent_configuration_snapshots (
  run_id TEXT PRIMARY KEY REFERENCES workflow_runs(run_id) ON DELETE CASCADE,
  snapshot_version INTEGER NOT NULL CHECK (snapshot_version = 1),
  snapshot_json TEXT NOT NULL CHECK (json_valid(snapshot_json)),
  prompt_catalog_digest TEXT NOT NULL REFERENCES prompt_catalog_revisions(prompt_catalog_digest),
  created_at TEXT NOT NULL
);

CREATE TRIGGER run_agent_configuration_snapshots_immutable_update
BEFORE UPDATE ON run_agent_configuration_snapshots
BEGIN SELECT RAISE(ABORT, 'run agent configuration snapshots are immutable'); END;

CREATE TRIGGER run_agent_configuration_snapshots_immutable_delete
BEFORE DELETE ON run_agent_configuration_snapshots
WHEN EXISTS (SELECT 1 FROM workflow_runs WHERE run_id = OLD.run_id)
BEGIN SELECT RAISE(ABORT, 'run agent configuration snapshots are immutable'); END;

CREATE TABLE planning_agent_configuration_snapshots (
  planning_session_id TEXT PRIMARY KEY REFERENCES planning_sessions(planning_session_id) ON DELETE CASCADE,
  snapshot_version INTEGER NOT NULL CHECK (snapshot_version = 1),
  snapshot_json TEXT NOT NULL CHECK (json_valid(snapshot_json)),
  prompt_catalog_digest TEXT NOT NULL REFERENCES prompt_catalog_revisions(prompt_catalog_digest),
  created_at TEXT NOT NULL
);

CREATE TRIGGER planning_agent_configuration_snapshots_immutable_update
BEFORE UPDATE ON planning_agent_configuration_snapshots
BEGIN SELECT RAISE(ABORT, 'planning agent configuration snapshots are immutable'); END;

CREATE TRIGGER planning_agent_configuration_snapshots_immutable_delete
BEFORE DELETE ON planning_agent_configuration_snapshots
WHEN EXISTS (SELECT 1 FROM planning_sessions WHERE planning_session_id = OLD.planning_session_id)
BEGIN SELECT RAISE(ABORT, 'planning agent configuration snapshots are immutable'); END;
