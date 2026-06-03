# Operator Surfaces

This document describes the operator-facing web, CLI, API, health, metrics, and artifact access surfaces that exist today.

## Authentication Model

All `/api/v1` routes require `X-Actor-Id` and `X-Role-Id` unless explicitly documented otherwise.

The main exception is:

- `GET /healthz`, which is an unauthenticated liveness probe

Current web client behavior:

- default API headers are `X-Actor-Id: allfather` and `X-Role-Id: king`
- manual approval mutations override the role header to `allfather`

## Web UI Surfaces

### `Throne Room` (`/`)

Purpose:

- top-level runtime overview
- health indicator
- recent runs
- runtime and token metrics cards

What it does not do:

- it is not the decree composer
- it is not the planning workspace
- it is not a web artifact browser

### `Decrees` (`/decrees`)

This area has two working modes.

#### Planning mode

Routes:

- `/decrees/planning`
- `/decrees/planning/{sessionId}`

Capabilities:

- list sessions
- inspect a session transcript
- submit planner replies
- regenerate assistant responses
- view and activate variants
- formalize the current plan into a draft

#### Issue mode

Route:

- `/decrees/new`

Capabilities:

- create a decree
- choose workflow and approval settings
- set review and validation loop controls
- choose a global model
- optionally set per-role model overrides
- issue a decree directly or from a planning draft

### `Runs` (`/runs` and `/runs/{runId}`)

The run-detail surface currently exposes these tabs:

- `graph`
- `executions`
- `timeline`
- `agents`
- `mailbox`

The run surface is also where operators handle manual approvals when approvals are not autonomous.

Execution tab semantics:

- the `executions` tab is the canonical operator surface for durable attempt state
- primary execution status is intentionally simple: queued, starting, running, verifying, succeeded, failed, or cancelled
- execution detail carries the nuanced settlement and output qualifiers operators need for trust and debugging
- provisional output can be shown as observed output while settlement is still pending, but that detail never implies success by itself
- canonical output and settled review/validation verdicts come from durable runtime-owned settlement, not mailbox text, stream text, or export diagnostics
- runs remain blocked until invocation-scoped runtime settlement is durably recorded from an accepted `kingdom_complete_attempt` receipt; captured text alone does not advance the DAG

Important limitation:

- there is no dedicated web artifact browser yet

### `Agents` (`/agents`)

Capabilities:

- inspect the canonical agent registry
- inspect lineage and role assignment

### `Realm Analytics` (`/realm-analytics`)

Capabilities:

- inspect runtime and run analytics backed by `/api/v1/realm-analytics/*`

## CLI Surfaces

The `kd` CLI is the supported installed-mode operator CLI.

### Workspace and Runtime Lifecycle

- `kd init`
- `kd clean`
- `kd summon`
- `kd summon status`
- `kd summon stop`

### Decree and Run Control

- `kd create decree`
- `kd run start`
- `kd run status`
- `kd graph show`
- `kd node retry`
- `kd node rerun-from`
- `kd timeline follow`

### Artifacts and Approvals

- `kd run artifacts export`
- `kd approvals list`
- `kd approvals plan-decide`
- `kd approvals decide`

See [`docs/cli_kd.md`](cli_kd.md) for the full installed-mode contract.

## API Surfaces

### Health and Identity

- `GET /healthz`
- `GET /api/v1/health`
- `GET /api/v1/roles`
- `GET /api/v1/agents`
- `GET /api/v1/provider/models`

### Planning

- `POST /api/v1/planning-sessions`
- `GET /api/v1/planning-sessions`
- `GET /api/v1/planning-sessions/{id}`
- `PATCH /api/v1/planning-sessions/{id}`
- `DELETE /api/v1/planning-sessions/{id}`
- `GET /api/v1/planning-sessions/{id}/messages`
- `GET /api/v1/planning-sessions/{id}/messages/{messageId}/variants`
- `PUT /api/v1/planning-sessions/{id}/messages/{messageId}/activate`
- `POST /api/v1/planning-sessions/{id}/operations`
- `GET /api/v1/planning-sessions/{id}/plan`
- `GET /api/v1/planning-operations/{id}`
- `GET /api/v1/planning-operations/{id}/events`
- `POST /api/v1/planning-sessions/{id}/context/refresh`
- `POST /api/v1/planning-sessions/{id}/generate-title`

### Decrees, Workflows, and Runs

- `POST /api/v1/decrees`
- `GET /api/v1/decrees/{id}`
- `POST /api/v1/workflows/publish`
- `POST /api/v1/runs`
- `GET /api/v1/runs`
- `GET /api/v1/runs/{id}`
- `POST /api/v1/runs/{id}/cancel`
- `GET /api/v1/runs/{id}/plan`
- `GET /api/v1/runs/{id}/graph`
- `GET /api/v1/runs/{id}/agents`
- `GET /api/v1/runs/{id}/timeline`
- `POST /api/v1/runs/{id}/nodes/{nodeKey}/retry`
- `POST /api/v1/runs/{id}/rerun-from/{nodeKey}`
- `POST /api/v1/runs/{id}/plan-approval`
- `POST /api/v1/runs/{id}/final-approval`
- `GET /api/v1/runs/{id}/governance-decisions`
- `GET /api/v1/runs/{id}/executions`
- `GET /api/v1/runs/{id}/executions/{attemptId}`

### Mailbox

- `GET /api/v1/runs/{id}/mailbox/messages`
- `GET /api/v1/runs/{id}/mailbox/messages/{messageId}`
- `POST /api/v1/runs/{id}/mailbox/messages/{messageId}/read`

Mailbox semantics:

- mailbox is a communication and audit surface, not settlement authority
- response text in mailbox can help operators inspect context, but it does not determine canonical success or settled review/validation outcomes

### Artifacts and Preview

- `GET /api/v1/runs/{id}/artifacts`
- `GET /api/v1/runs/{id}/artifacts/archive`
- `GET /api/v1/artifacts/{id}`
- `GET /api/v1/artifacts/{id}/content`
- `POST /api/v1/artifacts/{id}/preview-session`
- `GET /api/v1/preview/{token}`
- `GET /api/v1/preview/{token}/{assetPath...}`

### Metrics

- `GET /api/v1/metrics/runtime`
- `GET /api/v1/dashboard/snapshot`
- `GET /api/v1/realm-analytics/snapshot`
- `GET /api/v1/realm-analytics/execution-detail`

Notes:

- `/api/v1/dashboard/snapshot` separates current runtime state (`runtime`) from window-scoped dashboard metrics (`overview`, `tokens`).
- `/api/v1/realm-analytics/snapshot` remains the fully window-scoped analytics surface.
- `/api/v1/realm-analytics/execution-detail` returns aggregate execution detail for `window_minutes`, or the exact selected-run detail when `run_id` is provided, and returns `404` for an unknown `run_id`.

## Streaming Surfaces

### Run Events

Endpoint:

- `GET /api/v1/events/stream`

Use this for:

- following run progress
- replaying from `Last-Event-ID`
- SSE-based operator or test tooling

### Planner Events

Endpoint:

- `GET /api/v1/planning-operations/{id}/events`

Use this for:

- planner reply/regenerate/formalize progress
- replaying from `Last-Event-ID`
- reconnecting after refresh or disconnect

Current planner stream behavior to know:

- unknown planning operations return `404` before streaming starts
- invalid `Last-Event-ID` values return `400`
- keepalive frames are sent while the operation is still active

## Health and Metrics Usage

Typical checks:

```bash
curl http://127.0.0.1:8080/healthz

curl -H 'X-Actor-Id: allfather' -H 'X-Role-Id: king' \
  http://127.0.0.1:8080/api/v1/health

curl -H 'X-Actor-Id: allfather' -H 'X-Role-Id: king' \
  http://127.0.0.1:8080/api/v1/metrics/runtime
```

## Known UI/API Gaps

- Artifact browsing is CLI/API-first today; the web UI does not yet have a dedicated artifact browser.
- Planning context refresh exists in the API but is not exposed as a dedicated web-planner button.

## Related Docs

- [`docs/runtime_flow.md`](runtime_flow.md)
- [`docs/cli_kd.md`](cli_kd.md)
- [`docs/runbooks/planner_operations.md`](runbooks/planner_operations.md)
