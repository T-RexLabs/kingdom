# Interfaces Overview

This document gives a human-readable map of Kingdom's main interfaces: the web app, CLI, API, health endpoints, and streaming endpoints.

It is an overview, not the canonical API reference. Route-level API truth lives in `contracts/openapi/openapi.yaml`.

## Authentication Model

All `/api/v1` routes require the local operator principal unless explicitly documented otherwise. CLI clients send `Authorization: Bearer` using the protected credential file created by `kd init`. Raw bearer values are not accepted as CLI flags.

The exceptions are:

- `GET /healthz`, which is an unauthenticated liveness probe
- `POST /api/v1/auth/session`, which creates or reuses a session only for a direct same-origin loopback browser
- the three GitHub Manifest/setup/OAuth callback GETs, which consume one-time state authority and redirect to the initiating exact-origin Connections page without requiring the browser cookie

Browser flow:

- open the runtime through `localhost` or a loopback IP on the host machine
- the UI transparently creates or reuses an origin-bound, restart-scoped session
- Kingdom returns an origin-scoped `HttpOnly`, `SameSite=Strict` cookie and an in-memory CSRF token
- cookie-authenticated requests remain loopback-only, and mutations require the exact session origin plus `X-CSRF-Token`
- a non-loopback browser receives a stable local-only explanation rather than a login prompt

Actor and authority headers do not exist in the public contract. Identity and fixed allfather authority come only from the verified local principal.

Most API request bodies have a 1 MiB transport limit; `POST /decrees` allows up to 16 MiB so complete, storage-bounded external issue text can be issued without clipping. Requests beyond their route's limit return the canonical error envelope with HTTP `413`. Request reads and idle connections are time-bounded; SSE response writes are not.

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

Both creation modes can open the reusable, paginated GitHub issue picker. The picker performs a profile-aware preview, renders selected bounded comment bodies through restricted Markdown, and cancels stale searches. Planning intake can attach the first source at creation and additional unique issues from the active planning page. Each attach fetches before a short atomic capture/link/adoption transaction, affects future operations only, and fabricates no transcript message or context-snapshot movement. Direct issuance first captures an immutable snapshot, prefills editable title/body fields, and links that exact selected snapshot atomically when issued. Pre-issuance refresh exposes a candidate with explicit apply-title, apply-body, and use-snapshot actions; issued decrees provide refresh/field comparison without mutable text.

### `Connections` (`/connections`)

- makes **Connect GitHub** the primary guided device flow when shared App config is enabled; otherwise private Manifest creation remains primary
- offers private Manifest creation as the clear secondary path and existing-App manual metadata as the tertiary modal
- resumes restart-scoped setup stages after no-query callback redirects without putting provider secrets or callback values in browser storage
- renders private Manifest start, conversion, installation, authorization, and completion failures inside the private-App setup surface; ambiguous transport retries reuse the request key, while known terminal failures restart with a new flow and key
- keeps manual fields to display name, App ID, client ID, and installation ID; verified health discovers the slug
- treats App/client/installation identity as immutable after creation; PATCH changes only display name or enabled state
- returns explicit `app_private_key|user_access_token` strategy, App identity, and pinned account identity; shared cards offer **Reconnect** instead of PEM commands
- explains the exact metadata-read and Issues-read permission allowlist
- shows redacted credential and health states
- filters and paginates installation-granted repositories, then discovers issue-only results only after the current key generation passes health validation
- validates, disables, and removes local connections without deleting historical provenance

External snapshot reads return issue and retention metadata only. Comment bodies use `/external-source-snapshots/{id}/comments` with opaque pagination; link history is likewise paginated. Link coordinates always describe the target's immutable decree-used or planning-active snapshot, even when the provider's current issue locator has moved. Refresh responses are compact, typed summaries with field-change flags and optional safe retry timestamps.

Canonical API errors may include optional `retry_at` date-time metadata for provider rate limits. Connection and picker error states display that reset time without discarding the operator's current selection or form input. Only failed Kingdom local authentication returns `401`; revoked GitHub authorization returns a dependency error with safe `revoked` classification and reconnect guidance, leaving the local browser session signed in.

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
- choose the planner model and thinking effort
- attach or remove planning sources
- generate and view a plan draft

Planning context refresh is available through the API, but the web planner does not currently expose a dedicated refresh control.

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

- `DAG`
- `Activity`
- `Agents`
- `Messages`

The run surface is also where operators handle manual approvals when approvals are not autonomous.

Activity semantics:

- Activity presents one canonical logical workflow-node projection, then lets the operator select its paged physical attempt history
- a `starting`, `running`, or `blocked` predecessor remains selected while a replacement is only queued, so a stale-process handoff is visible rather than hidden by the queued retry
- primary execution status is intentionally simple: queued, starting, running, verifying, succeeded, failed, or cancelled
- attempt detail carries the nuanced settlement and output qualifiers operators need for trust and debugging
- the Outcome panel hides provider output and verdicts until canonical settlement commits; the Activity timeline may show explicitly labelled provisional stream text, which never implies success or canonical output
- canonical output and settled review/validation verdicts come from durable runtime-owned settlement, not mailbox text, stream text, or export diagnostics
- all provider-backed decree attempts settle from a valid server-observed terminal response and node-specific output validation
- the Activity surface includes a collapsible event timeline rather than a separate Timeline tab

When a stale provider process cannot be proven quiescent, Activity shows the attempt as paused and exposes **Retry Stop** only for its addressable guardian. That action requests one further bounded stop; it does not bypass quiescence, workspace restoration, or the cancellation handoff.

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
- run creation, inspection, graph, Activity, timeline events, and cancellation
- retry and rerun-from controls
- plan and final approval decisions
- logical-node execution projections at `GET /runs/{id}/executions`
- paged physical attempt history at `GET /runs/{id}/nodes/{nodeId}/attempts`

### Mailbox

- run mailbox listing and detail
- read markers for mailbox messages

Mailbox semantics:

- mailbox is a communication and audit surface, not settlement authority
- response text in mailbox can help operators inspect context, but it does not determine canonical success or settled review/validation outcomes

### Artifacts

- artifact listing and download
- archive export
- artifact content is always delivered as an attachment and cannot execute under the authenticated Kingdom origin

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

# Use the authenticated kd CLI for protected operator routes; it reads the
# credential file without exposing a raw-token command-line flag.
kd summon status
```

## Known UI/API Gaps

- Artifact browsing is CLI/API-first today; the web UI does not yet have a dedicated artifact browser.
- Planning context refresh exists in the API but is not exposed as a dedicated web-planner button.

## Related Docs

- [`docs/how-runs-work.md`](how-runs-work.md)
- [`docs/github-connections-runbook.md`](github-connections-runbook.md)
