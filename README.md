# Kingdom

Kingdom is a single-host orchestration runtime for work that should move like a governed realm instead of a pile of disconnected prompts.

Plan with counsel. Issue a decree. Route execution through fixed authority, durable state, review, validation, and royal closure.

The contract is concrete:

- Go runtime
- embedded React web UI
- `kd` CLI
- authenticated `/api/v1` API
- SQLite as the durable source of truth
- OpenCode as the only supported provider engine

## Why Kingdom

Kingdom is built for cases where AI work should be inspectable, recoverable, and governed.

- Planning sessions are durable and separate from execution.
- Decrees run through stored workflows, not ad-hoc agent chains.
- Role authority is fixed in the runtime, not left to prompt convention.
- Review, validation, approvals, artifacts, and mailbox traffic are stored for audit and recovery.
- Operators can work through the web UI, the `kd` CLI, or the API.

## What Kingdom Is Today

- Single-host runtime with an embedded web UI and local API server.
- Execution is host-launched against prepared workspaces.
- OpenCode is currently the only supported provider engine today.
- Builder output can publish directly into the project root or into a per-run git worktree.
- SQLite stores workflow, planner, run, mailbox, artifact, and execution state.

## OpenCode Integration

Kingdom uses [OpenCode](https://opencode.ai/docs/) as its provider engine. OpenCode's source lives at [anomalyco/opencode](https://github.com/anomalyco/opencode).

What that means in practice:

- Kingdom discovers selectable models from your local OpenCode installation.
  - If you have hosted or self-hosted providers configured in OpenCode, and OpenCode exposes them in its model catalog, those models will be available for selection in Kingdom.
- `kd init` installs Kingdom-managed OpenCode profiles and tools into `.kingdom/opencode/agents/` and `.kingdom/opencode/tools/`.
- Kingdom launches OpenCode with that overlay via `OPENCODE_CONFIG_DIR`, so Kingdom sees its managed profiles while your normal repo/global OpenCode setup still applies and standalone `opencode` sessions are not polluted by Kingdom assets.

Under the hood, Kingdom asks OpenCode for its model catalog and exposes that list through the product surfaces. Hence, Kingdom does not manage providers directly; it relies on OpenCode's configured environment.

## Quickstart Guide

### Prerequisites

- Go `1.26.3`
- Node `26.2.0`
- `opencode` installed and authenticated for the providers and models you want to use

### 1. Install Kingdom

Install Kingdom and build the embedded UI:

```bash
make install
export PATH="$PATH:$(go env GOPATH)/bin"
```

### 2. Go to the project folder you want Kingdom to work in

Kingdom is meant to run inside the project it will govern.

Use any project folder:

```bash
cd /path/to/your/project
```

Kingdom supports two publishing styles:

- `direct`: builder output publishes back into the current working directory; this is the default
- `git_worktree`: builder output publishes into a per-run git worktree when the target project is a valid non-bare git working tree

### 3. Initialize Kingdom in that project

Run `kd init` from the root of the target project:

```bash
kd init
```

This creates Kingdom's local workspace under `.kingdom/` and installs Kingdom-managed OpenCode profiles and tools under `.kingdom/opencode/agents/` and `.kingdom/opencode/tools/` in that project. Runtime startup validates those managed assets read-only and fails fast on drift instead of rewriting them.

### 4. Start the runtime

```bash
kd summon
```

Open `http://127.0.0.1:8080`.

### 5. Use Kingdom

1. Open the web UI.
2. Go to `Decrees`.
3. Choose a global model from the models OpenCode exposes.
4. Start with `Planning Sessions` or issue a decree directly.
5. Follow the run under `Runs`.
6. Inspect the graph, timeline, agents, mailbox, and published output.

> [!NOTE]
> Every new run requires an explicit global model selection, even if OpenCode has defaults configured.
> 
> If `kd` is not found after `make install`, make sure `$(go env GOPATH)/bin` is on your `PATH`.
> 
> If OpenCode is missing or unavailable, Kingdom will fail to initialize the provider path or fail to load the model catalog.

## How Work Moves Through Kingdom

The mental model is simple:

1. A planning session explores the work.
2. A decree captures what should be done.
3. A run executes the decree through a stored workflow.
4. Roles act in order, under fixed authority.
5. Builder output is reviewed and validated.
6. The King closes the decree.

The default workflow moves through these stages:

1. decree intake
2. advisor consultation
3. advisory synthesis
4. plan drafting
5. king plan decision
6. execution kickoff
7. builder execution
8. execution review
9. validation gates
10. royal closure

Important boundaries:

- `decree intake`, `king plan decision`, `execution kickoff`, and `royal closure` are runtime-owned deterministic steps; provider-backed run nodes are host-launched through OpenCode sessions, not containers.
- SQLite is the system of record. 
- Publication is done into the project root or a git worktree if toggled on and in a Git repo.
- Only builder output is treated as implementation deliverable content.
- Non-builder roles are response-first and publish markdown snapshots.

More detail lives in [`docs/runtime_flow.md`](docs/runtime_flow.md) and [`docs/architecture_rfc.md`](docs/architecture_rfc.md).

## Cast of the Realm

Kingdom has a canonical role catalog in the runtime, and a smaller set of managed OpenCode profiles that those roles map onto.

### Canonical Roles

| Role | Purpose |
| --- | --- |
| :cloud: `allfather` | Human operator who issues decrees and makes plan and final approval decisions when manual approvals are enabled. |
| :crown: `king` | Supreme governance authority that approves plans, requests rework when needed, and closes decrees. |
| :scroll: `chancellor` | Chief coordinator that synthesizes advisor input, resolves ambiguity, preserves scope coherence, and delegates clear downstream direction. |
| :compass: `council-member` | Strategic planner that turns decree intent into an execution plan and kicks off implementation. |
| :classical_building: `noble-delegate` | Advisor that focuses on functional fit, architecture fit, contract fit, precedent, and long-term system coherence. |
| :church: `clergy-delegate` | Advisor that focuses on security, trust boundaries, resilience, and blast-radius risk. |
| :hammer_and_pick: `burgher-delegate` | Advisor that focuses on performance, scalability, resource efficiency, and maintainability. |
| :ear_of_rice: `peasant-delegate` | Advisor that focuses on usability, operability, support burden, and day-2 practicality. |
| :axe: `builder-berserker` | Implementation specialist and the only role whose output is treated as implementation deliverable content. |
| :shield: `reviewer-shieldmaiden` | Reviews builder output against the decree and approved plan, checks scope and correctness before validation begins. |
| :test_tube: `intelligence-qa-scout` | Validates functional behavior, test evidence, and requirement coverage before royal closure. |
| :candle: `intelligence-nightraven` | Validates security posture, security findings, and risk concerns before royal closure. |
| :cyclone: `intelligence-loki-chaos`* | Resilience validator for chaos, recovery, and failure-mode concerns. |

\* Present in the canonical role catalog, but not used in the workflow today.

### Managed OpenCode Profiles

`kd init` installs these managed profiles under `.kingdom/opencode/agents/`:

| Profile | Used For |
| --- | --- |
| `king` | Strategic governance voice for `king`; provider policy also maps `allfather` to this profile when needed. |
| `chancellor` | Orchestration and delegation guidance for `chancellor`. |
| `council` | Planning and coordination work for `council-member`. |
| `advisor` | Shared advisor profile for the four delegate roles. |
| `builder` | Implementation profile for `builder-berserker`. |
| `reviewer` | Review profile for `reviewer-shieldmaiden`. |
| `qa_scout` | Functional validation profile for `intelligence-qa-scout`. |
| `nightraven` | Security validation profile for `intelligence-nightraven`. |
| `tester` | Resilience validation profile for `intelligence-loki-chaos`. |
| `planner` | Interactive planning profile used for planning sessions, separate from decree role execution. |

In the UI, think in terms of roles. In OpenCode, Kingdom resolves those roles onto managed profiles.

## Operator Surfaces

The realm is split into distinct operator surfaces:

- `Throne Room`: runtime overview, health, metrics, recent runs
- `Decrees`: planning sessions and decree creation
- `Runs`: run list, graph, timeline, agents, mailbox, approvals
- `Agents`: canonical agent registry and lineage view
- `Realm Analytics`: runtime and run analytics

Artifact browsing is still primarily CLI and API first. There is not yet a dedicated full web artifact browser.

## Contributing to Kingdom

Kingdom should feel good to use and serious to maintain. The quickest path to contributing is:

```bash
make install
export PATH="$PATH:$(go env GOPATH)/bin"
make verify
```

`make verify` runs the main verification sweep:

- `make test`
- `make test-race`
- `make vet`
- `make coverage-core`
- `make contracts-check`

Run `make` or `make help` to see the full public task list.

### Installed-Mode Verification

When you want to verify the real product path, use a fresh workspace under `kd-sandbox/`:

```bash
make install

mkdir -p kd-sandbox/local-runtime-check
cd kd-sandbox/local-runtime-check

kd init
kd summon
```

Then open `http://127.0.0.1:8080` and walk through a real decree flow.

### Frontend Development Loop

For frontend iteration, run the local runtime in one terminal and Vite in another.

Terminal A, from a disposable workspace:

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

Use `http://127.0.0.1:5173` during this loop.

Expected behavior:

- edits under `web/` should hot reload through Vite
- edits under `cmd/`, `internal/`, `.kingdom/config.yaml`, `.kingdom/policies/`, or `.kingdom/schemas/` require restarting `kd summon`
- final verification should still happen against `http://127.0.0.1:8080` so you exercise the embedded UI path

### What `kd init` and `kd summon` Do

`kd init` creates a local Kingdom workspace under `.kingdom/` and installs managed OpenCode profiles and tools under `.kingdom/opencode/agents/` and `.kingdom/opencode/tools/`.

That scaffold includes:

- runtime config
- SQLite state
- policies
- schemas
- workflows
- migrations
- logs
- runtime control files

`kd summon` starts the local runtime, API server, and embedded web UI. By default it listens on `127.0.0.1:8080` and serves the API under `/api/v1`. Startup validates managed `.kingdom/opencode/` assets read-only; if they are missing or stale, rerun `kd init --force`.

Use `kd clean` to remove the local `.kingdom/` scaffold before re-initializing from a clean state.

When the migration baseline changes, `kd clean` followed by `kd init` is the supported reset path for local environments.

## Common Commands

### `make`

| Command | Purpose |
| --- | --- |
| `make install` | Prepare pinned pnpm, build the embedded UI, and install `kd` |
| `make build` | Build the local `kd` binary into `bin/` |
| `make dev-web` | Start the web UI in Vite dev mode |
| `make test` | Run `go test ./...` |
| `make test-race` | Run targeted race-enabled tests |
| `make vet` | Run `go vet ./...` |
| `make verify` | Run the main verification sweep |
| `make help` | List supported public make targets |

### `kd`

| Command | Purpose |
| --- | --- |
| `kd init` | Scaffold `.kingdom/` and managed `.kingdom/opencode/agents/` profiles plus `.kingdom/opencode/tools/` |
| `kd clean` | Remove local Kingdom state and managed profiles |
| `kd summon` | Start the local runtime and web/API server |
| `kd summon status` | Inspect the local summoned runtime |
| `kd summon stop` | Stop the local summoned runtime |
| `kd create decree` | Create a decree |
| `kd run start` | Start a run from an existing decree |
| `kd run status` | Inspect run state |
| `kd timeline follow` | Follow run events over SSE |
| `kd run artifacts export` | Export run artifacts |

For deeper CLI detail, use [`docs/cli_kd.md`](docs/cli_kd.md).

## Repository Map

| Path | Purpose |
| --- | --- |
| `cmd/kd/` | CLI entrypoint |
| `cmd/kingdomd/` | standalone runtime entrypoint |
| `internal/api/` | HTTP handlers, validation, SSE, metrics, artifacts |
| `internal/cli/` | installed-mode bootstrap and CLI dispatch |
| `internal/orchestration/` | runtime bootstrap, scheduler, orchestration loop |
| `internal/state/` | persistence, transitions, audit, planner state, mailbox |
| `internal/bootstrapassets/` | seeded policies, schemas, workflows, profiles |
| `contracts/openapi/` | OpenAPI source of truth |
| `contracts/schemas/` | JSON schema contracts |
| `web/` | embedded React UI |
| `tests/` | integration and end-to-end suites |
| `docs/` | architecture, references, and runbooks |

## Further Reading

Start here when you want more detail:

- [`docs/architecture_rfc.md`](docs/architecture_rfc.md): accepted architecture and invariants
- [`docs/bootstrap.md`](docs/bootstrap.md): local setup and verification workflow
- [`docs/cli_kd.md`](docs/cli_kd.md): installed-mode CLI reference
- [`docs/operator_surfaces.md`](docs/operator_surfaces.md): UI, API, health, and metrics surfaces
- [`docs/opencode_provider_contract.md`](docs/opencode_provider_contract.md): provider integration contract

## Troubleshooting

| Symptom | What to Check |
| --- | --- |
| `kd: command not found` | Ensure `$(go env GOPATH)/bin` is on your `PATH` after `make install` |
| `summon already running` | Run `kd summon status`, then `kd summon stop` |
| `address already in use` | Free port `8080` or change `server.listen` |
| `initialize opencode session adapter` | Install `opencode` or set `provider.binary_path` |
| `model_selection.global.model is required` | Choose a model in the UI or pass `--global-model` |
| `GET /api/v1/health` returns `401` | Include `X-Actor-Id` and `X-Role-Id` |
| `git_worktree` publish failure | Confirm `git` exists and the project root is a valid non-bare worktree |
