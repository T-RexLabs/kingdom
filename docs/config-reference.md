# Config Reference

This document explains the fields in `.kingdom/config.yaml`.

It is intentionally limited to the actual config file shape used by local installed-mode Kingdom.
Unknown fields are rejected so retired or misspelled settings cannot be silently ignored.

## What This File Controls

- local runtime listen address
- API base path
- no credentials or operator authority; local authentication lives under `.kingdom/secrets/`
- provider startup and binary settings
- publish mode defaults
- worktree retention behavior
- default local approval mode

This file does not define workflow structure, retry policy, role permissions, or per-run model selection rules.

It also never contains local operator credentials, GitHub private keys, App/client secrets, OAuth tokens, or installation tokens. Operator auth lives at `.kingdom/secrets/operator-token`; private App keys use `.kingdom/secrets/github-apps/`, and shared user tokens use `.kingdom/secrets/github-user-tokens/`. SQLite stores only opaque credential handles.

GitHub provider origins are not configurable. Production registration, OAuth, and device flow use `https://github.com`; API exchanges use `https://api.github.com` with REST version `2026-03-10`. Manifest callback URLs come only from the authenticated browser request's direct origin. Shared device flow is browser-session/origin bound but callback-free.

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

provider:
  engine: opencode
  strict: <bool>
  governance_mode: autonomous
  binary_path: <path>
  model: <string>
  agent: <string>
  skip_permissions: <bool>

github:
  shared_app:
    slug: kingdom-issue-importer
    client_id: Iv23li1tLBTzhr70LA1J
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
  - `kd summon --listen`, `--host`, and `--port` can override this value for a single summon process without rewriting the config file

- `base_path`
  - required
  - default scaffold value is `/api/v1`
  - normalized on read, so values like `v1` become `/v1`
  - cannot be `/`, malformed, or collide with a built-in web route or static-asset prefix
  - injected into the embedded web UI so browser API and SSE requests use the same configured path

### `provider`

- `engine`
  - required runtime value: `opencode`
  - no other provider engine is supported

- `strict`
  - default: `true`
  - when `true`, summon validates managed OpenCode assets, binary availability, and agent resolution before readiness

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

- `skip_permissions`
  - default: `true`
  - keeps local execution non-interactive so permission prompts do not block runs

### `github.shared_app`

- `slug` and `client_id` are public identifiers and must both be set or both be blank.
- `kd init` defaults to Kingdom's official public App: `kingdom-issue-importer` / `Iv23li1tLBTzhr70LA1J`.
- both blank disable the branded shared setup and leave private Manifest setup primary.
- exactly one set is a startup error.
- never configure a client secret, App private key, or owner credential. The App must be public/installable, have device flow and expiring user tokens enabled, request exactly metadata read plus Issues read, and subscribe to no events.
- standalone `kingdomd` uses `--github-shared-app-slug` / `KINGDOM_GITHUB_SHARED_APP_SLUG` and `--github-shared-app-client-id` / `KINGDOM_GITHUB_SHARED_APP_CLIENT_ID` with the same two-field rule.

## Practical Guidance

- Change `publish.mode` to `git_worktree` when you want run output isolated from the main working tree.
- Keep `provider.strict=true` unless you are deliberately relaxing startup validation during local debugging.
- Set `provider.binary_path` only when `opencode` is not already available on your `PATH`.
- Treat `provider.model` and `provider.agent` as summon-time defaults only, not as substitutes for per-run choices.
- Change `server.base_path` only if you are intentionally serving the API from a different path prefix.

## Content Handling and Policy Boundaries

There are no config fields for a run-wide attempt budget, provider output-byte ceiling, tracked workspace file-size/aggregate-size/entry-count/directory-depth ceiling, or external-source/comment retention cap. The runtime uses disk staging, chunked canonical content, and paginated reads for those workloads; individual scalar and read-response allocations can still grow with content.

Per-error automatic retry allowances remain in `policies/retry_policy.yaml`. Review and validation cycle caps, ready-node auto-success, and `run_until_all_approve` remain workflow/per-run controls. Active provider timeouts remain in `policies/opencode_provider_policy.yaml`: strict startup, first event, stream inactivity, cancellation grace/force, and activity-gated node wall-clock budgets. Guardian metadata, snapshot lineage, recovery batches, and GitHub setup/discovery boundaries are still enforced. Removing content ceilings does not disable those controls or enlarge a model's context window.

## Related Sources

- `internal/cli/install_mode.go`: local config parsing, defaults, and validation
- `kd --help`: CLI entrypoint help
- `docs/system-architecture.md`: runtime invariants and behavior not controlled by config
