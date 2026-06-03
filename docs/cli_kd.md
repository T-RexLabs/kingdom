# `kd` CLI

This document describes the installed-mode CLI contract that exists today.

## Install

```bash
make install
which kd
```

`make install` activates the pinned `pnpm` version through Corepack, installs the web dependencies, builds the embedded web UI, and installs `kd`.

If needed, add `$(go env GOPATH)/bin` to your `PATH`.

## Installed-Mode Flow

The normal local flow is:

```bash
kd init
kd summon
```

`kd init` writes:

- `.kingdom/config.yaml`
- `.kingdom/kingdom.db`
- `.kingdom/policies/`
- `.kingdom/schemas/`
- `.kingdom/workflows/`
- `.kingdom/migrations/`
- `.kingdom/logs/`
- `.kingdom/run/`
- `.kingdom/opencode/agents/`
- `.kingdom/opencode/tools/`

`kd summon` starts the runtime and serves the web UI plus API.

Default endpoint:

- web UI: `http://127.0.0.1:8080`
- API: `http://127.0.0.1:8080/api/v1`

## Key Commands

### Workspace and Runtime

- `kd init [--path <project-root>] [--force] [--publish-mode <direct|git_worktree>] [--autonomous-approvals <bool>] [--provider-engine <opencode>] [--provider-strict <bool>] [--listen <addr>] [--base-path <path>]`
- `kd clean [--path <project-root>]`
- `kd summon [--config <path>] [--ready-timeout <duration>] [--detach]`
- `kd summon status [--config <path>]`
- `kd summon stop [--config <path>]`

`kd clean` stops the local summoned runtime when needed and removes `.kingdom/`.

### Decrees and Runs

- `kd create decree --title <title> --text <text> [--metadata-json <json>] [--skip-reviews-and-validations] [--autonomous-approvals <inherit|true|false>] [--run-until-all-approve] [--max-execution-review-cycles <n>] [--max-validation-rework-cycles <n>] [--publish-mode <direct|git_worktree>] --global-model <provider/model> [--agent-model-overrides <role=model,...>]`
- `kd run start --decree-id <id> [--skip-reviews-and-validations] [--autonomous-approvals <inherit|true|false>] [--run-until-all-approve] [--max-execution-review-cycles <n>] [--max-validation-rework-cycles <n>] [--publish-mode <direct|git_worktree>] --global-model <provider/model> [--agent-model-overrides <role=model,...>]`

For both commands, `--max-execution-review-cycles` and `--max-validation-rework-cycles` accept `0` or greater when provided.
`0` defers to the selected workflow default.
When `--skip-reviews-and-validations` is set, those loop controls are still accepted, but the alternate workflow omits review and validation and therefore defaults both caps to `0`.
- `kd run status --run-id <id>`
- `kd graph show --run-id <id> [--ascii|--mermaid]`
- `kd node retry --run-id <id> --node-key <node> --reason <text>`
- `kd node rerun-from --run-id <id> --node-key <node> --reason <text>`
- `kd timeline follow --run-id <id>`

### Artifacts and Approvals

- `kd run artifacts export --run-id <id> [--output <path>]`
- `kd approvals list --run-id <id> [--limit <n>] [--cursor <cursor>]`
- `kd approvals plan-decide --run-id <id> --action <confirm|rework> [--reason <text>]`
- `kd approvals decide --run-id <id> --action <confirm|rework> [--reason <text>]`

For both approval commands, `--reason` is optional when `--action confirm` and required when `--action rework`.

- `kd approvals plan-decide --action rework` queues a new planning wave.
- `kd approvals decide --action rework` reopens the decree from `plan_drafting`, and the revised plan must be approved again before execution resumes.

## Global Flags

The API-driving command families accept:

- `--server` API base URL
- `--actor-id` actor header value
- `--role-id` role header value
- `--json` JSON output
- `--timeout` request timeout for non-streaming commands

Important installed-mode behavior:

- when `--server` is omitted, installed-mode commands derive the URL from local `.kingdom/config.yaml`
- when you are not targeting the local summoned runtime, pass `--server` explicitly

## `.kingdom/config.yaml` Reference

`kd summon` reads these fields.

### Top-level paths

- `version`
- `kingdom_home`
- `db_path`

### `server`

- `listen`
  - default: `127.0.0.1:8080`
- `base_path`
  - default: `/api/v1`
  - normalized on read so values such as `v1` are canonicalized to `/v1`
- `preview_session_ttl`
  - default: `30m`
  - controls artifact preview token lifetime

### `operator`

- `actor_id`
  - default: `summon-system`
- `role_id`
  - default: `allfather`

### `provider`

- `engine`
  - default: `opencode`
  - only supported value today: `opencode`
- `strict`
  - default: `true`
  - when `true`, summon validates provider startup and agent resolution before becoming ready
- `governance_mode`
  - required value today: `autonomous`
- `binary_path`
  - optional path to the `opencode` binary
- `model`
  - optional summon-time provider default passed into the OpenCode session adapter
  - does not remove the requirement for explicit per-run global model selection
- `agent`
  - optional summon-time provider default agent passed into the session adapter
- `attach_url`
  - optional OpenCode attach URL
- `skip_permissions`
  - default: `true`
  - enables non-interactive execution without permission prompts

### `governance`

- `autonomous_approvals`
  - default: `true`

### `publish`

- `mode`
  - `direct` (default)
  - `git_worktree`
  - provides the installed-mode default for new runs when CLI callers omit `--publish-mode`
- `worktree_retention`
  - default: `1h`
  - accepts a positive duration like `30m` or `2h`, or `never`
  - terminal run worktrees are kept for this long before Kingdom cleans them up automatically

## Runtime Notes

- strict OpenCode mode expands the default summon readiness wait so startup covers the provider strict probe budget plus buffer
- runs using `publish_mode=git_worktree` require `git` and a valid non-bare repo
- Kingdom creates linked run worktrees as detached checkouts in `../kingdom-worktrees/<repo>/<decree-title-slug>-<run_id>/` next to the repository's main worktree root, then scopes execution back to the configured project root inside that checkout
- review and validation defaults come from the built-in workflow Kingdom selects for the run; standard runs use `royal-decree-v1`, while `--skip-reviews-and-validations` selects `royal-decree-skip-review-validation-v1`, which omits those stages and therefore defaults both caps to `0`
- allfather plan rework is uncapped
- manual approval mode pauses at both the plan gate and the final gate

## Useful API Endpoints Behind the CLI

The CLI commonly drives these API routes:

- `POST /api/v1/decrees`
- `POST /api/v1/runs`
- `GET /api/v1/runs/{id}`
- `GET /api/v1/runs/{id}/graph`
- `GET /api/v1/runs/{id}/timeline`
- `GET /api/v1/runs/{id}/artifacts`
- `GET /api/v1/events/stream`
- `POST /api/v1/runs/{id}/plan-approval`
- `POST /api/v1/runs/{id}/final-approval`

## Troubleshooting

- `summon already running with pid=<pid>`
  - use `kd summon status`, then `kd summon stop`
- `summon readiness timeout`
  - inspect `.kingdom/logs/summon.log`
- `initialize opencode session adapter`
  - install `opencode` or set `provider.binary_path`
- `model_selection.global.model is required`
  - choose a model from the web UI or `GET /api/v1/provider/models`, then rerun with `--global-model`
- `GET /api/v1/health` returns `401`
  - include `X-Actor-Id` and `X-Role-Id`
- `git_worktree` workspace preparation failure
  - ensure `git` is installed and the project root is a valid non-bare worktree
