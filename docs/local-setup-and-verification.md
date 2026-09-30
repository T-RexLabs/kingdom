# Local Setup and Verification

This document explains how to install Kingdom locally, verify it, and run a realistic local development loop.

## Toolchain Setup

```bash
make install
export PATH="$PATH:$(go env GOPATH)/bin"
```

`make install`:

- enables Corepack
- prepares the pinned pnpm version
- installs the frontend and JavaScript tooling dependencies under `web/`
- builds the embedded web UI
- installs `kd`

`make install` is the only setup command required for a brand new local install.

Run `make` or `make help` to list the supported public make targets.

Targets that need JavaScript tooling, such as `make openapi-lint`, reuse the dependencies installed under `web/`.

## Main Verification Commands

| Command | What It Runs |
| ------- | ------------ |
| `make test` | `go test ./...` after building web assets |
| `make test-web` | frontend unit tests |
| `make lint-web` | frontend ESLint with zero warnings allowed |
| `make test-race` | race-enabled tests for `internal/state/...`, `internal/planning/...`, and `internal/api/...` |
| `make vet` | `go vet ./...` |
| `make coverage-core` | core coverage gate via `scripts/check_core_coverage.sh` |
| `make contracts-check` | regenerate OpenAPI types and verify no checked-in diff |
| `make verify` | `test`, `test-web`, `lint-web`, `test-race`, `vet`, `coverage-core`, and `contracts-check` |

## Recommended Local Runtime Verification

When you need to verify the real installed-mode runtime:

```bash
make install

mkdir -p kd-sandbox/local-runtime-check
cd kd-sandbox/local-runtime-check

kd init
kd summon
```

To fully reset a local install before re-initializing:

```bash
kd clean
kd init
```

Then open `http://127.0.0.1:8080` and walk the product through a real decree flow.

Recommended manual verification:

1. Open `Decrees`.
2. Choose a global model.
3. Create a planning session or issue a decree directly.
4. Follow the run under `Runs`.
5. Inspect graph, timeline, agents, and mailbox.
6. If manual approvals are enabled, confirm the plan and final gate.

## Recommended Repository Development Loop

When you are iterating on the checked-out source tree, use the existing split workflow instead of treating `kd summon` as a hot-reload server.

Terminal A, from a disposable workspace under `kd-sandbox/`:

```bash
mkdir -p kd-sandbox/local-dev
cd kd-sandbox/local-dev

kd init
kd summon
```

Terminal B, from the repository root:

```bash
make dev-web
```

Use `http://127.0.0.1:5173` while iterating on the frontend. Vite provides frontend hot reload and proxies API plus health traffic to the summoned runtime on `127.0.0.1:8080`.

Expected behavior:

- edits under `web/` should reload through Vite
- edits under `cmd/`, `internal/`, `.kingdom/config.yaml`, `.kingdom/policies/`, or `.kingdom/schemas/` require restarting `kd summon`
- final runtime verification should always happen against `http://127.0.0.1:8080` so you exercise the embedded UI path

## Local Server Contract

`kd init` writes a local `.kingdom/` workspace and managed OpenCode profiles under `.kingdom/opencode/agents/` in the project root.

`kd summon`:

- ensures the SQLite schema exists
- loads policies, schemas, workflows, and config from `.kingdom/`
- seeds canonical roles
- validates managed OpenCode assets and agent resolution in strict mode when enabled
- starts the API at `http://127.0.0.1:8080/api/v1`
- serves the web UI from the same process
- accepts runtime-only address overrides via `--listen <host:port>` or symmetric `--host <host>` and `--port <port>` flags

## Installed-Mode Notes

- `publish.mode=direct` publishes builder deliverables into the project root.
- `publish.mode=git_worktree` publishes into a detached linked worktree under `../kingdom-worktrees/<repo>/<decree-title-slug>-<run_id>/` next to the repository's main worktree root, then scopes back to the configured project root inside that checkout.
- `governance.autonomous_approvals=true` auto-approves plan and final closure.
- `governance.autonomous_approvals=false` pauses at both approval gates for allfather decisions.
- OpenCode runs still require an explicit per-run global model selection even if summon config contains provider defaults.

## Troubleshooting

- `listen tcp 127.0.0.1:8080: bind: address already in use`
  - if another summon is using the port, run `kd summon status` and `kd summon stop`
  - if another local service is using the port, stop it or change `server.listen`
- `summon already running with pid=<pid>`
  - use `kd summon status`, then `kd summon stop`
- `summon readiness timeout`
  - run `kd summon logs --follow`
  - inspect `.kingdom/logs/summon.log`
  - verify `http://127.0.0.1:8080/healthz`
  - verify authenticated `GET /api/v1/health`
- changes visible at `:8080` but not `:5173`
  - confirm `make dev-web` is running from the repo root
  - use `http://127.0.0.1:5173` for Vite-based frontend iteration
- changes under Go or `.kingdom/` paths are not taking effect
  - stop and restart `kd summon`; backend, config, policy, and schema changes are not hot-reloaded in-process
- `initialize opencode session adapter`
  - install `opencode` or set `provider.binary_path`
- `git_worktree` workspace preparation failure
  - ensure `git` is installed and the project root is a valid non-bare worktree
- missing publish output in the project root
  - confirm the run reached `succeeded`
  - inspect `publish.mode`
  - inspect `GET /api/v1/runs/{id}/artifacts`
