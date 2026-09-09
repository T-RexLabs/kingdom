# How Runs Work

This document explains how work moves through Kingdom today, from optional planning through decree closure.

## Two Separate Lifecycles

Kingdom has two related but separate lifecycles:

1. Planning sessions
2. Decree execution runs

Planning sessions are optional. A decree can be issued directly without using the planner first.

## Planning Session Lifecycle

Planning domain resources live under `/planning-sessions`; durable provider activity is exposed through `/api/v1/acp`.

### What a Planning Session Stores

- `lifecycle_state`
- `conversation_state`
- `active_leaf_message_id`
- `current_operation_id`
- `current_title_operation_id`
- `current_draft_id`
- `context_snapshot_id`
- canonical planning messages
- planning drafts

### Planner Operations

Planner work is not request-scoped. It runs as a durable planning operation with stored events and status.

Current operation types:

- `reply`
- `regenerate`
- `formalize`
- `generate_title`

Important behavior:

- `reply` appends the next assistant answer to the transcript.
- `regenerate` creates an alternate assistant answer for an existing turn.
- assistant answers can have multiple variants.
- one assistant variant can be activated as the canonical transcript entry.
- `formalize` produces the current planning draft.
- `generate_title` runs as a durable planner operation through `POST /api/v1/acp/runs`, uses the session's dedicated title-operation lane, and updates the session title only after server-owned settlement succeeds.
- `generate_title` does not claim the session's interactive `current_operation_id` slot or change `conversation_state`, so title generation can run in the background while interactive planning continues.
- context refresh exists as an explicit API call on the session.
- planner stream text is provisional operator feedback; canonical planner state comes from persisted message variants, drafts, and session fields rather than from transient stream output.
- provider-backed planner work settles through planner-specific runtime-owned invocation, evidence, and settlement records before canonical planning tables are updated.
- planner success uses the provider's adjudicated terminal success plus non-empty runtime-assembled output. Planning never uses a `kingdom_complete_attempt` receipt; process exit, provisional stream text, and export output are not success authority.
- planner operations persist canonical state through planning message variants, drafts, and session fields; planner settlement records authorize those writes but do not replace the planning tables as transcript/draft/session truth.

### Planner Streaming and Recovery

Planning operation events stream from:

- `GET /api/v1/acp/runs/{runId}/events`

The stream supports:

- SSE `id:` frames
- `Last-Event-ID` replay/resume
- keepalive frames
- recovery after runtime restart or page refresh

If the client exhausts SSE retries, the web planner falls back to polling the planning-operation row until the operation reaches a terminal state.

### Issuing a Decree from a Plan

When the operator issues a decree from a planning session, the client supplies:

- `planning_session_id`
- `planning_draft_id`

Kingdom derives the following immutable lineage from the draft's unique successful formalize operation and attaches all three values to decree metadata:

- `planning_context_snapshot_id`

That lineage is attached only at decree issuance time. Planning itself does not mutate a decree run.

## Run Creation Lifecycle

Decree execution starts when a decree is created and a run is started for it.

A run carries:

- decree id
- workflow id
- approval mode
- review and validation loop controls
- model selection

Current model-selection contract:

- a global model is required for OpenCode runs
- role overrides are optional
- `/roles` defines which roles appear in the override selector

## Canonical Workflow: `royal-decree-v1`

The seeded workflow definition contains these nodes.

| Node ID | Required Role | Purpose |
| ------- | ------------- | ------- |
| `decree_intake` | `king` | Record royal intake and open advisory consultation |
| `advisor_consultation_noble` | `noble-delegate` | Functional-fit, architecture-fit, and precedent counsel |
| `advisor_consultation_clergy` | `clergy-delegate` | Security, trust-boundary, and resilience counsel |
| `advisor_consultation_burgher` | `burgher-delegate` | Performance, scalability, efficiency, and maintainability counsel |
| `advisor_consultation_peasant` | `peasant-delegate` | Usability, operability, and day-2 practicality counsel |
| `advisory_synthesis` | `chancellor` | Synthesize advisory counsel into clear downstream direction |
| `plan_drafting` | `council-member` | Draft the execution-ready plan and rework deltas |
| `king_plan_decision` | `king` | Approve the plan or request plan rework |
| `execution_kickoff` | `council-member` | Record the runtime-owned execution handoff after plan approval |
| `delegated_execution_berserker` | `builder-berserker` | Perform implementation |
| `execution_review_shieldmaiden` | `reviewer-shieldmaiden` | Perform execution review |
| `validation_gates_qa_scout` | `intelligence-qa-scout` | Functional validation |
| `validation_gates_nightraven` | `intelligence-nightraven` | Security validation |
| `royal_closure` | `king` | Close the decree |

Notes:

- `decree_intake` is a runtime-owned deterministic node. It records ceremonial intake, persists the opening response snapshot, and advances the workflow without invoking OpenCode.
- `king_plan_decision` is a runtime-owned deterministic governance node. It does not invoke OpenCode; it stays pending at the plan approval gate until approval or rework is recorded, then persists a deterministic response snapshot.
- `execution_kickoff` is a runtime-owned deterministic handoff node. It records that approved work has moved into implementation and opens the builder/review/validation phase without invoking OpenCode.
- `royal_closure` is a runtime-owned deterministic ceremonial node. It does not invoke OpenCode; it persists the final ceremonial summary before the run either auto-succeeds or pauses for allfather final approval.
- The canonical role catalog also includes `intelligence-loki-chaos`, but the seeded workflow does not currently include a Loki node.
- The default workflow metadata sets `max_execution_review_cycles=5` and `max_validation_rework_cycles=5`.

## Bundled Alternate Workflow: `royal-decree-skip-review-validation-v1`

Kingdom also ships a built-in alternate workflow for runs that intentionally bypass quality gates.

It keeps the same planning path through `execution_kickoff`, runs `delegated_execution_berserker`, and then routes directly to `royal_closure`.

Notes:

- `execution_review` and `validation_gates` nodes are absent from this workflow by design.
- review and validation caps default to `0` for runs created from this workflow.
- the web decree form and `kd` CLI expose this mode as `Skip Reviews and Validations` and disable or reject review-loop controls when selected.

## Governance and Rework

### Plan Approval

Plan drafting must complete before execution begins.

- If `governance.autonomous_approvals=true`, the runtime auto-approves the plan and then completes `king_plan_decision` without a provider call.
- If `governance.autonomous_approvals=false`, the run pauses for an allfather plan decision while `king_plan_decision` remains pending.

Plan rework behavior:

- allfather plan rework is uncapped
- a rework decision records `needs_rework` plan state, persists feedback in the plan review record, and reopens `advisory_synthesis` plus `plan_drafting`
- plan state can move from `needs_rework` back to `in_council_review` for another council pass

### Execution Review and Validation

After builder execution:

1. `reviewer-shieldmaiden` emits exactly one `VERDICT: PASS` or `VERDICT: REWORK` line as the first non-empty line of the response
2. validation roles inspect the result and emit exactly one `VERDICT: PASS` or `VERDICT: REWORK` line as the first non-empty line of the response

Rework behavior:

- review rework uses `max_execution_review_cycles`
- validation rework uses `max_validation_rework_cycles`
- review rework is wave-settled across all review nodes for a given persisted wave lineage before runtime queues another builder/review cycle
- `run_until_all_approve=true` causes the runtime to keep reworking until review and validation pass instead of stopping at the normal cap
- a boundary `REWORK` still queues the next builder/review or builder/review/validation wave
- once a queued review or validation node becomes ready at or beyond the configured cap, runtime auto-succeeds that node without provider dispatch
- explicit stored cap `0` means the first ready review/validation attempt auto-succeeds immediately
- review attempts and settled validation-wave outcomes remain as executed; cap auto-success applies only to queued ready review/validation nodes skipped instead of dispatching another provider-backed loop
- validation rework is counted by settled wave, not by individual validator node
- workflows that omit `execution_review` and `validation_gates` skip this phase entirely and proceed directly to final closure

### Final Approval

Successful workflow progression is not enough for terminal success on its own.

- `royal_closure` completes before the final governance gate and does not invoke OpenCode.
- If `governance.autonomous_approvals=true`, final acceptance is automatic.
- If `governance.autonomous_approvals=false`, the run pauses at `awaiting_human_confirmation` until allfather records `confirm` or `rework`.
- Final `confirm` transitions the decree to `succeeded`.
- Final `rework` records durable allfather feedback, reopens the decree from `plan_drafting`, and requires the revised plan to return to allfather plan approval before execution resumes.

## Workspace Lifecycle

Before execution, the runtime resolves one shared execution target for the run.

### `publish.mode=direct`

- the current project root is the source root and publish target
- Kingdom captures tracked source files into one runtime-owned run-scoped execution target
- retries and non-canonical side effects are reset from trusted runtime-owned snapshot state instead of from per-attempt seeded copies

### `publish.mode=git_worktree`

- the project root must be a valid non-bare git working tree
- Kingdom creates or reuses a detached sibling worktree under `../kingdom-worktrees/<repo>/<decree-title-slug>-<run_id>/` next to the repository's main worktree root
- if the configured project root is nested inside the repository, execution is scoped back to that same relative subdirectory inside the linked worktree
- that scoped project directory becomes the shared execution and publish target for the run

### Snapshot Tracking

Kingdom captures runtime-owned filesystem snapshots for tracked files in the execution target.

Those snapshots record the trusted builder baseline and later attempt deltas so the runtime can restore, diff, and publish without per-attempt seeded copies.

Tracked snapshot capture ignores:

- `.git`
- `.hg`
- `.svn`
- `.jj`
- `.kingdom`
- `kingdom-artifacts`
- symlinks
- non-regular filesystem entries

## Execution Lifecycle

Kingdom launches OpenCode directly on the host against the prepared workspace for provider-backed decree execution nodes.

Important runtime behavior:

- `decree_intake`, `execution_kickoff`, `king_plan_decision`, and `royal_closure` are runtime-owned and deterministic; they do not open OpenCode sessions.
- execution is session-based via `opencode run --format json`
- runtime creates a provider invocation record before dispatching every provider-backed decree execution attempt; only builder invocations receive a single-use completion capability
- strict mode validates managed OpenCode assets, binary availability, and agent resolution
- runtime continues across non-terminal `step_finish` reasons such as `tool-calls` and `unknown`
- builder sessions must call `kingdom_complete_attempt` to request terminal success settlement; all other provider-backed nodes settle from the server-observed terminal response
- runtime validates active attempt and invocation lineage for every settlement, plus the completion capability and required node-specific fields for builder receipts
- canonical settled response text is invocation-owned and single-turn; runtime must not concatenate multiple final answers or competing receipts into one settled response body
- durable decree execution state is stored in `provider_invocations`, `provider_completion_evidence`, `provider_completion_settlements`, `attempt_executions`, `attempt_event_log`, and `attempt_outbox`
- clean builder execution exit without a valid receipt is terminal failure; response-first nodes require a structurally valid terminal server response
- `opencode export` is diagnostic only for provider-backed decree execution; it can add operator-facing context for the active invocation but cannot create or repair settlement, while the complete live stream remains canonical evidence for response-first completion
- `attempt_outbox` replay is failure-isolated: retryable projection failures stay `pending` with failure bookkeeping, while unrecoverable replay payload problems are dead-lettered to `failed` so one poison row does not block newer durable completions
- mailbox rows are latest-state projections for operator views; append-only run events remain the durable timeline/audit source

## Artifact Lifecycle

Artifact persistence and publication are separate steps.

### Persistence

Artifact metadata is written to `artifacts`.

Artifact bytes are written to `artifact_contents`.

### Builder Output

- builder output is derived from runtime-owned snapshot diffs
- only files changed or deleted relative to the trusted snapshot baseline are treated as canonical builder artifacts
- noisy dependency, cache, and package-management files are excluded from reviewer-facing deliverable manifests
- review and validation nodes inspect the shared execution target directly; runtime no longer materializes a separate `hydrated/` tree

### Non-Builder Output

- non-builder roles are response-first
- runtime writes a markdown snapshot of the role response into the workspace
- that snapshot becomes the persisted artifact for the attempt

### Publication

After persistence, Kingdom may publish artifacts into the operator-visible output root.

For `publish_mode=direct`, publication is per-path and best-effort. If a destination path changed outside Kingdom during the run, Kingdom records a path conflict with a simple reason and leaves that external change untouched while still publishing non-conflicting paths.

- builder deliverables publish into the project root in `direct` mode
- builder deliverables publish into the run worktree in `git_worktree` mode
- non-builder outputs publish under `kingdom-artifacts/<decree-slug>-<run_id>/...`

## Event Streaming

Kingdom exposes two important SSE channels:

- `GET /api/v1/events/stream` for run events
- `GET /api/v1/acp/runs/{runId}/events` for planner operation events

Both are intended to support replay/resume with `Last-Event-ID`.

## Terminal States

The important run outcomes are:

- `succeeded`
- `failed`
- `cancelled`
- `awaiting_human_confirmation`

Key rule:

- `succeeded` requires legal workflow progression and final authority acceptance
- `cancelled` runs retain their attempt history and can be reopened by explicit Retry or Rerun only after cancellation and completion work has fully settled

Artifact existence does not decide whether the run succeeds.

## Related Docs

- [`docs/system-architecture.md`](system-architecture.md)
- [`docs/interfaces-overview.md`](interfaces-overview.md)
