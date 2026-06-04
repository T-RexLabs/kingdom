# Config Reference

This document explains the fields in `.kingdom/config.yaml`.

It is intentionally limited to the actual config file shape used by local installed-mode Kingdom.

## What This File Controls

- local runtime listen address
- API base path
- preview session TTL
- local operator identity defaults
- provider startup and binary settings
- publish mode defaults
- worktree retention behavior
- default local approval mode

This file does not define workflow structure, retry policy, role permissions, or per-run model selection rules.

## File Shape

The current config file shape is:

```yaml
version: <string>
kingdom_home: <path>
db_path: <path>

governance:
  autonomous_approvals: <bool>

publish:
  mode: <direct|git_worktree>
  worktree_retention: <duration|never>

server:
  listen: <host:port>
  base_path: <path>
  preview_session_ttl: <duration>

operator:
  actor_id: <string>
  role_id: <string>

provider:
  engine: opencode
  strict: <bool>
  governance_mode: autonomous
  binary_path: <path>
  model: <string>
  agent: <string>
  attach_url: <url>
  skip_permissions: <bool>
```

## Field Reference

### Top-level fields

- `version`
  - config version string written by Kingdom
- `kingdom_home`
  - path to the local `.kingdom/` directory
- `db_path`
  - path to the SQLite database file

### `governance`

- `autonomous_approvals`
  - default: `true`
  - when `true`, Kingdom auto-approves plan and final closure
  - when `false`, Kingdom pauses for allfather approval at both gates

### `publish`

- `mode`
  - supported values: `direct`, `git_worktree`
  - default local behavior is `direct`
  - `git_worktree` requires a valid non-bare git worktree

- `worktree_retention`
  - default: `1h`
  - accepts a positive duration like `30m` or `2h`, or `never`
  - controls how long terminal run worktrees are kept before cleanup

### `server`

- `listen`
  - required
  - default scaffold value is `127.0.0.1:8080`

- `base_path`
  - required
  - default scaffold value is `/api/v1`
  - normalized on read, so values like `v1` become `/v1`

- `preview_session_ttl`
  - default: `30m`
  - controls preview token lifetime for artifact previews

### `operator`

- `actor_id`
  - default: `summon-system`
  - used as the local runtime's operator identity

- `role_id`
  - default: `allfather`
  - used as the local runtime's operator role

### `provider`

- `engine`
  - required runtime value: `opencode`
  - no other provider engine is supported

- `strict`
  - default: `true`
  - when `true`, summon validates provider startup and agent resolution before readiness

- `governance_mode`
  - required value: `autonomous`
  - `manual` is not implemented

- `binary_path`
  - optional path to the `opencode` binary

- `model`
  - optional summon-time provider default
  - does not remove the requirement for explicit per-run global model selection

- `agent`
  - optional summon-time provider default agent

- `attach_url`
  - optional OpenCode attach URL

- `skip_permissions`
  - default: `true`
  - keeps local execution non-interactive so permission prompts do not block runs

## Practical Guidance

- Change `publish.mode` to `git_worktree` when you want run output isolated from the main working tree.
- Keep `provider.strict=true` unless you are deliberately relaxing startup validation during local debugging.
- Set `provider.binary_path` only when `opencode` is not already available on your `PATH`.
- Treat `provider.model` and `provider.agent` as summon-time defaults only, not as substitutes for per-run choices.
- Change `server.base_path` only if you are intentionally serving the API from a different path prefix.

## Related Sources

- `internal/cli/install_mode.go`: local config parsing, defaults, and validation
- `kd --help`: CLI entrypoint help
- `docs/system-architecture.md`: runtime invariants and behavior not controlled by config
