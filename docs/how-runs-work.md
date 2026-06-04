# How Runs Work

This document explains how work moves through Kingdom today, from optional planning through decree closure.

## Two Separate Lifecycles

Kingdom has two related but separate lifecycles:

1. Planning sessions
2. Decree execution runs

Planning sessions are optional. A decree can be issued directly without using the planner first.

## Planning Session Lifecycle

Planning lives under `/planning-sessions` and `/planning-operations`.

### What a Planning Session Stores

- `lifecycle_state`
- `conversation_state`
- `current_question_message_id`
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
- `generate_title` runs as a durable planner operation behind `POST /planning-sessions/{id}/generate-title`, uses the session's dedicated `current_title_operation_id` lane, and updates the session title only after receipt-backed settlement succeeds.
- `generate_title` does not claim the session's interactive `current_operation_id` slot or change `conversation_state`, so title generation can run in the background while interactive planning continues.
- context refresh exists as an explicit API call on the session.
- planner stream text is provisional operator feedback; canonical planner state comes from persisted message variants, drafts, and session fields rather than from transient stream output.
- provider-backed planner work settles through planner-specific runtime-owned invocation, evidence, and settlement records before canonical planning tables are updated.
- planner operations persist canonical state through planning message variants, drafts, and session fields; planner settlement records authorize those writes but do not replace the planning tables as transcript/draft/session truth.

### Planner Streaming and Recovery

Planning operation events stream from:

- `GET /api/v1/planning-operations/{id}/events`

The stream supports:

- SSE `id:` frames
- `Last-Event-ID` replay/resume
- keepalive frames
- recovery after runtime restart or page refresh

If the client exhausts SSE retries, the web planner falls back to polling the planning-operation row until the operation reaches a terminal state.

### Issuing a Decree from a Plan

When the operator issues a decree from a planning session, Kingdom attaches lineage metadata to the decree:

- `planning_session_id`
- `planning_draft_id`
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

Before execution, the runtime resolves an execution workspace.

### `publish.mode=direct`

- the current project root is the seed root
- if the project root is empty, the workspace starts empty
- if the project root is not empty, regular files are copied into the execution workspace and fingerprinted as the baseline

### `publish.mode=git_worktree`

- the project root must be a valid non-bare git working tree
- Kingdom creates or reuses a detached sibling worktree under `../kingdom-worktrees/<repo>/<decree-title-slug>-<run_id>/` next to the repository's main worktree root
- if the configured project root is nested inside the repository, execution is scoped back to that same relative subdirectory inside the linked worktree
- that scoped project directory becomes both the execution seed root and publish target

### Baseline Tracking

Kingdom writes `.kingdom/workspace-baseline.json` into the execution workspace.

That baseline records the seed files and fingerprints so the runtime can later identify which builder files are newly created or modified.

The seeding process ignores:

- `.git`
- `.kingdom`
- symlinks
- non-regular filesystem entries

## Execution Lifecycle

Kingdom launches OpenCode directly on the host against the prepared workspace for provider-backed decree execution nodes.

Important runtime behavior:

- `decree_intake`, `execution_kickoff`, `king_plan_decision`, and `royal_closure` are runtime-owned and deterministic; they do not open OpenCode sessions.
- execution is session-based via `opencode run --format json`
- runtime creates a provider invocation record and single-use completion capability before dispatching any provider-backed decree execution attempt
- strict mode validates provider startup and agent resolution
- runtime continues across non-terminal `step_finish` reasons such as `tool-calls` and `unknown`
- every provider-backed decree execution session must call `kingdom_complete_attempt` to request terminal success settlement
- runtime settles provider-backed decree execution success only after validating the active attempt, invocation, completion capability, and any required node-specific fields from that tool call
- canonical settled response text is invocation-owned and single-turn; runtime must not concatenate multiple final answers or competing receipts into one settled response body
- durable decree execution state is stored in `provider_invocations`, `provider_completion_evidence`, `provider_completion_settlements`, `attempt_executions`, `attempt_event_log`, and `attempt_outbox`
- clean provider-backed decree execution exit without a valid receipt is terminal failure for the provider-backed decree execution attempt
- stream evidence and `opencode export` remain diagnostic and repair inputs only for provider-backed decree execution; they can add operator-facing context for the active invocation but cannot create canonical success, alter receipt-driven settled output, or become a second authority path
- `attempt_outbox` replay is failure-isolated: retryable projection failures stay `pending` with failure bookkeeping, while unrecoverable replay payload problems are dead-lettered to `failed` so one poison row does not block newer durable completions
- mailbox rows are latest-state projections for operator views; append-only run events remain the durable timeline/audit source

## Artifact Lifecycle

Artifact persistence and publication are separate steps.

### Persistence

Artifact metadata is written to `artifacts`.

Artifact bytes are written to `artifact_contents`.

### Builder Output

- builder output is discovered from the execution workspace
- only files changed relative to the baseline are treated as `deliverable` artifacts
- noisy dependency, cache, and package-management files are excluded from reviewer-facing deliverable manifests
- review and validation nodes still inspect a self-contained `hydrated/` snapshot materialized from the current prepared workspace, so unchanged baseline assets remain visible even though durable builder persistence is delta-only

### Non-Builder Output

- non-builder roles are response-first
- runtime writes a markdown snapshot of the role response into the workspace
- that snapshot becomes the persisted artifact for the attempt

### Publication

After persistence, Kingdom may publish artifacts into the operator-visible output root.

- builder deliverables publish into the project root in `direct` mode
- builder deliverables publish into the run worktree in `git_worktree` mode
- non-builder outputs publish under `kingdom-artifacts/<decree-slug>-<run_id>/...`

## Event Streaming

Kingdom exposes two important SSE channels:

- `GET /api/v1/events/stream` for run events
- `GET /api/v1/planning-operations/{id}/events` for planner operation events

Both are intended to support replay/resume with `Last-Event-ID`.

## Terminal States

The important run outcomes are:

- `succeeded`
- `failed`
- `cancelled`
- `awaiting_human_confirmation`

Key rule:

- `succeeded` requires legal workflow progression and final authority acceptance

Artifact existence does not decide whether the run succeeds.

## Related Docs

- [`docs/system-architecture.md`](system-architecture.md)
- [`docs/interfaces-overview.md`](interfaces-overview.md)
