# Interfaces Overview

This document gives a human-readable map of Kingdom's main interfaces: the web app, CLI, API, health endpoints, and streaming endpoints.

It is an overview, not the canonical API reference. Route-level API truth lives in `contracts/openapi/openapi.yaml`.

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
- generate a session title
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

Use it for:

- local setup and runtime lifecycle
- decree creation and run control
- graph and timeline inspection
- artifact export and approval decisions

Use `kd --help` and `kd help <command>` for the CLI source of truth.

## API Surfaces

Use this section as a guide to the major API areas, not as a complete route inventory.

### Health and Identity

- health and runtime health checks
- roles and agents
- provider model discovery

### Planning

- planning sessions
- planning messages and variants
- planning operations and their event streams
- plan drafts and context refresh
- background title generation

### Decrees, Workflows, and Runs

- decree creation
- workflow publishing
- run creation, inspection, graph, timeline, and cancellation
- retry and rerun-from controls
- plan and final approval decisions
- durable execution records

### Mailbox

- run mailbox listing and detail
- read markers for mailbox messages

Mailbox semantics:

- mailbox is a communication and audit surface, not settlement authority
- response text in mailbox can help operators inspect context, but it does not determine canonical success or settled review/validation outcomes

### Artifacts and Preview

- artifact listing and download
- archive export
- preview-session creation and preview serving

### Metrics

- runtime metrics
- dashboard snapshot data
- realm analytics snapshot and execution detail

Notes:

- `/api/v1/dashboard/snapshot` separates current runtime state (`runtime`) from window-scoped dashboard metrics (`overview`, `tokens`).
- `/api/v1/realm-analytics/snapshot` remains the fully window-scoped analytics surface.
- `/api/v1/realm-analytics/execution-detail` returns aggregate execution detail for `window_minutes`, or the exact selected-run detail when `run_id` is provided, and returns `404` for an unknown `run_id`.

## Streaming Surfaces

### Run Events

Endpoint family:

- run event streaming

Use this for:

- following run progress
- replaying from `Last-Event-ID`
- SSE-based operator or test tooling

### Planner Events

Endpoint family:

- planning operation event streaming

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

- [`docs/how-runs-work.md`](how-runs-work.md)
