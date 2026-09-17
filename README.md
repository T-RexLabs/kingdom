<p align="center">
  <picture>
    <source srcset="web/public/t-rex-white.svg" media="(prefers-color-scheme: dark)">
    <source srcset="web/public/t-rex-black.svg" media="(prefers-color-scheme: light)">
    <img src="web/public/t-rex-black.svg" alt="Kingdom logo" width="220">
  </picture>
</p>
<p align="center"><strong>Self-hosted orchestration for governed AI work.</strong></p>

# Kingdom

In a world of rising token costs, raising your own Kingdom can help protect the treasury. Kingdom is open source, self-hosted, and runs on your own hardware so planning, execution, and audit stay under your control.

If you want to spend a few gold nuggets on hosted models, Kingdom supports that too through your local OpenCode setup.

Kingdom is built for governed AI work: plan with counsel, issue a decree, and route execution through fixed authority, durable state, review, validation, and royal closure.

Kingdom consists of:

- Go runtime
- embedded React web UI
- `kd` CLI
- authenticated `/api/v1` API
- SQLite as the durable source of truth
- OpenCode as the only supported provider engine

## Why Kingdom

Kingdom is built for peopl/teams that want AI work to stay under their control without giving up structure, visibility, or rigor.

- Self-hosted by default, so you control the runtime, data, and operating boundary.
- Open source, so the system can be inspected, adapted, and trusted on its merits.
- Use planning sessions to turn rough intent into execution-ready decrees.
- Route decrees through stored workflows instead of ad-hoc agent chains.
- Enforce role authority in the runtime instead of relying on prompt convention.
- Preserve review, validation, approvals, artifacts, and mailbox history for audit and recovery.
- Operate through the web UI, the `kd` CLI, or the API.
- Use hosted models through your local OpenCode setup when you want them.

## OpenCode Integration

Kingdom uses [OpenCode](https://opencode.ai/docs/) as its provider engine. OpenCode's source lives at [anomalyco/opencode](https://github.com/anomalyco/opencode).

What that means in practice:

- Kingdom discovers selectable models from your local OpenCode installation.
  - If you have hosted or self-hosted providers configured in OpenCode, and OpenCode exposes them in its model catalog, those models will be available for selection in Kingdom.
- `kd init` installs Kingdom-managed OpenCode profiles and tools into `.kingdom/opencode/agents/` and `.kingdom/opencode/tools/`.
- Kingdom launches OpenCode with that overlay via `OPENCODE_CONFIG_DIR`, so Kingdom sees its managed profiles while your normal repo and global OpenCode setup still apply and standalone `opencode` sessions are not polluted by Kingdom assets.

Under the hood, Kingdom asks OpenCode for its model catalog and exposes that list through the product surfaces. Kingdom does not manage providers directly; it relies on OpenCode's configured environment.

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

This creates Kingdom's local workspace under `.kingdom/`, a protected operator credential at `.kingdom/secrets/operator-token`, and Kingdom-managed OpenCode profiles and tools under `.kingdom/opencode/agents/` and `.kingdom/opencode/tools/`. Runtime startup validates those managed assets and secret permissions read-only and fails fast on drift instead of rewriting them. The credential value is never written to config, SQLite, logs, URLs, or browser storage.

### 4. Start the runtime

```bash
kd summon
```

Open `http://localhost:8080` or `http://127.0.0.1:8080`. A browser connecting directly through loopback establishes its local session automatically; there is no login or pairing step. The session is restart-scoped, expires after eight hours, uses an origin-scoped `HttpOnly`, `SameSite=Strict` cookie, and keeps its CSRF token in memory only. Session expiry closes active browser requests, including SSE streams, and the UI recovers transparently. Bearer-authenticated CLI and API calls are not bounded by browser-session expiry.

Browser access is deliberately host-local. A browser reaching Kingdom through a LAN address or forwarding proxy cannot establish a session. Programmatic non-loopback access requires an HTTPS API URL and an explicitly supplied `--credential-file`; Kingdom never auto-discovers a credential for a remote server.

Database backups do not include `.kingdom/secrets`; retain that directory separately or reinitialize/reprovision local credentials after restore. `kd clean` deletes the complete `.kingdom` tree, including local credentials, and reports that consequence explicitly.

### 5. Use Kingdom

1. Open the web UI.
2. Go to `Decrees`.
3. Choose a global model from the models OpenCode exposes.
4. Start with `Planning Sessions` or issue a decree directly.
5. Follow the run under `Runs`.
6. Inspect the graph, Activity, agents, messages, and published output.

> [!NOTE]
> Every new run requires an explicit global model selection, even if OpenCode has defaults configured.
> 
> If `kd` is not found after `make install`, make sure `$(go env GOPATH)/bin` is on your `PATH`.
> 
> If OpenCode is missing or unavailable, Kingdom will fail to initialize the provider path or fail to load the model catalog.

## GitHub Issue Connections

Kingdom can start planning, attach additional issues to an active planning session, or prefill direct decree issuance from a GitHub App installation through three local setup paths.

1. Fresh `kd init` configuration enables Kingdom's official public GitHub App automatically. Select **Connect GitHub**, enter the displayed device code on GitHub, and tick every account the connection should cover — one connection covers them all and reads every repository they grant. Authorizing is not installing: the App can be installed on additional accounts at any point in the flow, and a saved connection links straight to its repository selection on GitHub. Closing setup preserves it for **Resume Setup**; **Discard Setup** clears it. **Reconnect Connection** always reauthorizes the complete saved installation set, while **Change** is the only set editor. The App uses device flow and expiring user tokens with exactly **Metadata: read-only** and **Issues: read-only**, and no events.
2. Select **Create a private GitHub App** to use the preserved Manifest path and own a dedicated private App under a personal account or organization.
3. Select **Use manual setup** for an existing private App.

For manual setup, save display name, App ID, client ID, and installation ID, then provision the key from the initialized workspace host. Kingdom discovers and pins the slug during the first successful health check:

   ```bash
   kd connections github key provision --connection-id <connection-id> --from ./private-key.pem
   ```

Run the connection health check after manual provisioning or any key rotation. The check requires the App and installation to grant exactly **Metadata: read-only** and **Issues: read-only**, with zero subscribed events and no additional App permissions. Healthy connections become available in the shared issue picker on planning and direct-decree screens.

Private keys remain in generation files at `.kingdom/secrets/github-apps/<connection>/private-key.<generation>.pem`; shared user tokens remain in `.kingdom/secrets/github-user-tokens/<connection>/tokens.<generation>.json`. SQLite stores only the active generation pointer, never credential bytes. Tokens never enter logs, API/audit/idempotency payloads, or browser storage. GitHub API, OAuth, and device-flow traffic originates from the local Kingdom runtime without a Kingdom cloud broker; only private Manifest registration is posted by the browser directly to GitHub. Shared access is the intersection of App installation access and the authorizing user; long-idle refresh credentials can expire after six months and require **Reconnect Connection**. The picker paginates repositories/issues and renders selected bounded comment bodies through restricted Markdown. See [the GitHub Connections runbook](docs/github-connections-runbook.md).

## How Work Moves Through Kingdom

The mental model is simple:

1. Planning sessions turn rough intent into execution-ready decrees.
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
- The builder alone receives a completion receipt capability. Other provider-backed nodes settle from the server-observed terminal response; output, artifacts, mailbox messages, and `opencode export` do not independently authorize success.

More detail lives in [`docs/how-runs-work.md`](docs/how-runs-work.md) and [`docs/system-architecture.md`](docs/system-architecture.md).

## Cast of the Realm

Kingdom has a canonical role catalog in the runtime, and a smaller set of managed OpenCode profiles that those roles map onto.

### Canonical Roles

| Role | Purpose |
| --- | --- |
| :cloud: `allfather` | Human operator who issues decrees and makes plan and final approval decisions when manual approvals are enabled. |
| :crown: `king` | Supreme governance authority that approves plans, requests rework when needed, and closes decrees. |
| :scroll: `chancellor` | Chief coordinator that synthesizes advisor input, resolves ambiguity, preserves scope coherence, and delegates clear downstream direction. |
| :compass: `council-member` | Strategic planner that turns decree intent into an execution-ready plan, breaks work into implementation slices, and refines plan deltas during rework. |
| :classical_building: `noble-delegate` | Advisor that focuses on functional fit, architecture fit, contract fit, precedent, and long-term system coherence. |
| :church: `clergy-delegate` | Advisor that focuses on security, trust boundaries, resilience, and blast-radius risk. |
| :hammer_and_pick: `burgher-delegate` | Advisor that focuses on performance, scalability, resource efficiency, and maintainability. |
| :ear_of_rice: `peasant-delegate` | Advisor that focuses on usability, operability, support burden, and day-2 practicality. |
| :axe: `builder-berserker` | Implementation specialist that turns approved plans into scoped, verified, maintainable code changes and deliverable outputs. |
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
- `make test-web`
- `make lint-web`
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

Use `http://localhost:5173` (`http://127.0.0.1:5173`) during this loop.

Expected behavior:

- edits under `web/` should hot reload through Vite
- edits under `cmd/`, `internal/`, `.kingdom/config.yaml`, `.kingdom/policies/`, or `.kingdom/schemas/` require restarting `kd summon`
- final verification should still happen against `http://localhost:8080` so you exercise the embedded UI path

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

`kd summon` starts the local runtime, API server, and embedded web UI attached to the current terminal. Use `kd summon -d` or `kd summon --detach` to start it in the background. Use `kd summon --listen <host:port>` for a full runtime-only address override, or `kd summon --host <host>` and `kd summon --port <port>` for symmetric partial overrides. By default it listens on `127.0.0.1:8080` and serves the API under `/api/v1`. Standalone and installed servers bound request headers and bodies with 5-second header, 15-second read, and 60-second idle timeouts; response writes remain unbounded so authenticated SSE streams stay connected. Startup validates managed `.kingdom/opencode/` assets read-only; if they are missing or stale, rerun `kd init --force`.

Summons are also recorded machine-wide in `$XDG_CACHE_HOME/kingdom/summon_registry.json` (`~/Library/Caches/kingdom/` on macOS), so `kd` can discover a running instance from any project directory. The file is a cache of live processes, not a source of truth: entries are validated against running PIDs on every read, writes are atomic and serialized across processes, and a registry Kingdom cannot decode is rebuilt on the next summon rather than failing the command. Per-project state stays authoritative in `.kingdom/run/`, so a rebuilt registry never makes a running summon undiscoverable from its own workspace.

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
| `make test-web` | Run the frontend unit tests |
| `make lint-web` | Run frontend ESLint with zero warnings allowed |
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

Use `kd --help` and `kd help <command>` for CLI usage details.

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

- [`docs/README.md`](docs/README.md): docs map and source-of-truth guide
- [`docs/system-architecture.md`](docs/system-architecture.md): accepted architecture and invariants
- [`docs/local-setup-and-verification.md`](docs/local-setup-and-verification.md): local setup and verification workflow
- `kd --help`: installed-mode CLI reference
- [`docs/interfaces-overview.md`](docs/interfaces-overview.md): UI, CLI, API, health, and metrics overview

## Troubleshooting

| Symptom | What to Check |
| --- | --- |
| `kd: command not found` | Ensure `$(go env GOPATH)/bin` is on your `PATH` after `make install` |
| `summon already running` | Run `kd summon status`, then `kd summon stop` |
| `kd summon status` reports stopped while a summon is running | Only affects discovery from another project directory; run `kd summon status` from the summon's own workspace, or restart it to re-register in the machine-wide registry |
| `address already in use` | Free port `8080` or change `server.listen` |
| `initialize opencode session adapter` | Install `opencode` or set `provider.binary_path` |
| `model_selection.global.model is required` | Choose a model in the UI or pass `--global-model` |
| `GET /api/v1/health` returns `401` | Run `kd` from the initialized workspace so it discovers the bearer credential, or pass `--credential-file` explicitly; actor/role headers are not supported |
| GitHub request returns `424` with `provider_failure_class=revoked` | Reconnect the GitHub connection, then run its health check; the local browser session remains valid |
| `git_worktree` publish failure | Confirm `git` exists and the project root is a valid non-bare worktree |
