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

Actor and authority headers do not exist in the public contract. Identity and fixed operator authority come only from the verified local principal.

API request bodies are privately spooled for schema validation, exact-byte idempotency hashing, and handler replay without a fixed route-level aggregate byte limit. Designated work order and planning text fields stream into canonical SQLite content storage. Authentication and schema constraints still apply; request reads and idle connections remain time-bounded, while SSE response writes are not. Individual scalar fields and some read responses still materialize data in memory.

## Web UI Surfaces

### `Overview` (`/`)

Overview prioritizes pending operator decisions and current work using server-filtered run queues. It also shows recent planning sessions and a 24-hour outcome/token summary. Each queue links to its complete filtered collection; loaded cards are not a total count.

### Planning and New Work Order

Planning sessions live at `/work-orders/planning`; direct issuance is a separate workspace at `/work-orders/new`. Both can use the reusable paginated GitHub issue picker. The picker performs a profile-aware preview, renders opted-in comment bodies through paginated restricted Markdown, and cancels stale searches. Comment import captures every page and complete bodies; preview pagination is not a retention cap. Planning sources attach to an existing session. Each attach fetches before a short atomic capture/link/adoption transaction, affects future operations only, and fabricates no transcript message or filesystem context-snapshot movement. Direct issuance captures an immutable snapshot, prefills editable title/body fields, and links that exact selected snapshot when issued. Pre-issuance refresh offers explicit title, body, and snapshot adoption; refreshing an issued work order never rewrites its immutable text.

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

External snapshot reads return issue and retention metadata only. Comment bodies use `/external-source-snapshots/{id}/comments` with opaque pagination; link history is likewise paginated. Link coordinates always describe the target's immutable work-order-used or planning-active snapshot, even when the provider's current issue locator has moved. Refresh responses are compact, typed summaries with field-change flags and optional safe retry timestamps.

Canonical API errors may include optional `retry_at` date-time metadata for provider rate limits. Connection and picker error states display that reset time without discarding the operator's current selection or form input. Only failed Kingdom local authentication returns `401`; revoked GitHub authorization returns a dependency error with safe `revoked` classification and reconnect guidance, leaving the local browser session signed in.

### `Planning` (`/work-orders/planning`)

Routes:

- `/work-orders/planning`
- `/work-orders/planning/{sessionId}`

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

Planning keeps **Chat** and **Plan** as its primary views. **Attach Source** opens source selection directly, and **Sources** opens linked-source management when sources exist. The **Project Context** dialog exposes **Refresh Context** and paginated **Included Files** and **Excluded Files** inspection. Refresh creates a new filesystem context snapshot without fabricating a planner operation.

Planner user messages align to the right. **Show Activity** opens recorded provider thinking, tools, checklist snapshots, and observed message inspection above the canonical assistant response. Short activity trails fit their content, while long histories retain bounded scrolling and earlier-page access. Raw event identifiers sit behind **Event Details**.

Planning-session lists expose continuation through **Load More Sessions** in the planning rail and **Load Older Sessions** beside **From Plan**. Context snapshot responses contain summaries rather than file arrays; `GET /planning-sessions/{id}/context/{snapshotId}/entries` returns included or excluded metadata in ordinal pages. Planner operations bind every active source snapshot when enqueued, so later source changes affect future operations only.

### `New Work Order` (`/work-orders/new`)

Route:

- `/work-orders/new`

Capabilities:

- create a work order
- choose workflow and approval settings
- set review and validation loop controls in **Review Options**
- choose a global model
- optionally expand **Customize Role Models** to set per-role model overrides
- issue a work order directly or from a planning draft

### `Runs` (`/runs` and `/runs/{runId}`)

Run detail starts in **Workflow**, which displays the stored workflow DAG. The two primary views are **Workflow** and **Agent Activity**. Quiet contextual actions keep supporting material available without competing with the conversation:

- **Workflow**: actual topology, live execution state, and node selection.
- **Agent Activity**: the selected agent and attempt's read-only conversation, provider reasoning, compact tool actions, and canonical final response. **Instructions** reads the complete immutable prompt body prepared for the selected attempt, without reconstructing it from the issued brief. **Final Response** appears when a canonical settled response exists and also remains inline in the conversation.
- **View Plan**: the plan and its review history, with current approval/rework controls where applicable.
- **Run Files**: artifact downloads and publication results.
- **Run Details**: the full issued work order, source context, settings, and audit history.

The run surface is also where operators handle manual approvals when approvals are not autonomous.

Activity semantics:

- Activity presents one canonical logical workflow-node projection, then lets the operator select its paged physical attempt history
- Agent Activity shares the planner’s Show Activity disclosure for actual reasoning, tools, reported checklists, and observed messages; it has no user composer or display filters
- provider message observations retain exact content, invocation/part identity, and complete content pages; they never settle an attempt. The conversation renders the canonical final response for that same selected attempt separately from settlement diagnostics
- a `starting`, `running`, or `blocked` predecessor remains selected while a replacement is only queued, so a stale-process handoff is visible rather than hidden by the queued retry
- primary execution status is intentionally simple: queued, starting, running, verifying, succeeded, failed, or cancelled
- a collapsed **Attempt Record** disclosure under **Run Details → Audit** carries selected-attempt failure, settlement, identity, and retry evidence without duplicating its conversation response
- The conversation keeps canonical settled responses distinct from observed or provisional evidence; neither activity nor an observed response implies success
- canonical output and settled review/validation verdicts come from durable runtime-owned settlement, not mailbox text, stream text, or export diagnostics
- all provider-backed work order attempts settle from a valid server-observed terminal response and node-specific output validation
- Audit exposes the event timeline separately from provider activity

When a stale provider process cannot be proven quiescent, Activity shows the attempt as paused and exposes **Retry Stop** only for its addressable guardian. That action requests one further bounded stop; it does not bypass quiescence, workspace restoration, or the cancellation handoff.

**Run Files** includes bounded artifact provenance pages with canonical deliverable, audit evidence, and deletion distinctions. File downloads and **Download All** return complete stored content; the UI does not execute or preview arbitrary artifact bytes inline. Publication entry pages preserve structured path actions, results, and conflict reasons independently of execution settlement.

### `Agents` (`/agents`)

Capabilities:

- inspect themed names, descriptions, lore, and canonical role responsibilities
- inspect availability aggregated across the registered instances of each role
- preserve explicit loading, unknown, mixed, blocked, or offline states instead of inventing availability or selecting an arbitrary run

### `Analytics` (`/analytics`)

Capabilities:

- inspect runtime and run analytics backed by `/api/v1/analytics/*`

### `Appearance` (`/appearance` and `/appearance/create`)

Choose a personally pinned theme revision, inspect shared packages and editable source files, or create a theme with synchronized visual and JSON editors. Private app preview, source saving, immutable publication, import/export, unpublishing, and editable-source deletion are separate operations. See [Themes and Appearance](themes.md).

## CLI Surfaces

The `kd` CLI is the supported installed-mode operator CLI.

Use it for:

- local setup and runtime lifecycle
- work order creation and run control
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

### Work Orders, Workflows, and Runs

- work order creation
- workflow publishing
- run creation, inspection, graph, Activity, timeline events, and cancellation
- retry and rerun-from controls
- plan and final approval decisions
- logical-node execution projections at `GET /runs/{id}/executions`
- paged physical attempt history at `GET /runs/{id}/nodes/{nodeId}/attempts`
- paged plan-review history at `GET /runs/{id}/plan-reviews`

### Mailbox API

The backend mailbox contract remains operational; the web UI has no Messages panel or mailbox controls.

- run mailbox listing and detail
- read markers for mailbox messages

Mailbox semantics:

- mailbox is a communication and audit surface, not settlement authority
- response text in mailbox can help operators inspect context, but it does not determine canonical success or settled review/validation outcomes
- mailbox history uses signed continuation cursors bound to the run and filters
- referenced instructions and responses remain available through the authenticated, run/message-scoped content endpoint. Content identity and integrity are verified before serving complete bytes; list responses contain previews.

Planning-session, plan-review, mailbox, and context-entry page sizes are read boundaries rather than retention ceilings. Continue with the returned `next_cursor` and the same filter scope.

### Artifacts

- bounded artifact pages with exact node/attempt/producer lineage and signed continuation cursors
- separately paged publication results, pinned to one run/attempt/batch revision
- complete authenticated downloads and archive export
- artifact content is always delivered as an attachment and cannot execute under the authenticated Kingdom origin
- downloads and archive exports stream canonical SQLite content chunks without fixed file-size, aggregate-byte, or archive-entry workload ceilings

Artifact pages return `snapshot_id` and optional `next_cursor`; publication pages return `entries_revision` and optional `next_cursor`. Load every page for a complete listing. A changed lineage or publication revision returns HTTP 409 with `refresh_required`; restart from the first page while keeping the current run/attempt selection.

### Metrics

- runtime metrics
- dashboard snapshot data
- analytics snapshot and execution detail

Notes:

- `/api/v1/dashboard/snapshot` separates current runtime state (`runtime`) from window-scoped dashboard metrics (`overview`, `tokens`).
- `/api/v1/analytics/snapshot` remains the fully window-scoped analytics surface.
- `/api/v1/analytics/execution-detail` returns aggregate execution detail for `window_minutes`, or the exact selected-run detail when `run_id` is provided, and returns `404` for an unknown `run_id`.

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

## Related Docs

- [`docs/how-runs-work.md`](how-runs-work.md)
- [`docs/github-connections-runbook.md`](github-connections-runbook.md)
