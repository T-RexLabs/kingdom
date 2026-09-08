# Execution Reliability Plan

## Purpose

This plan defines the implementation sequence for making Kingdom's provider execution, completion settlement, stale-worker recovery, retries, and operator experience reliable under timeouts, process crashes, SQLite contention, and restart recovery.

The work must produce one coherent execution model, not a collection of incident-specific patches.

## Engineering Standard

Every phase must satisfy these standards:

- Fix root causes in the canonical layer.
- Prefer the smallest complete design over tactical patches or speculative frameworks.
- Keep SQLite as the authoritative control-plane store.
- Keep state ownership explicit and enforce invariants transactionally.
- Make provider-controlled input and output bounded.
- Preserve complete audit evidence without making internal mechanics dominate the primary UX.
- Keep APIs, persistence, runtime behavior, and UI projections consistent.
- Delete superseded code once its replacement is proven.
- Do not retain compatibility aliases, dual behavior, legacy branches, dead fields, dead helpers, or unused recovery paths.
- Do not infer authority from optional callbacks, filenames, process exit, untrusted payload identifiers, or UI state.
- Require deterministic tests for races and crash boundaries. Do not use timing sleeps as proof.
- Keep the UI clean, responsive, accessible, and understandable to non-technical operators.

Kingdom has not been deployed and there are no existing databases to preserve. Schema changes should modify the canonical bootstrap schema directly. Do not add data backfills, compatibility migrations, old-state readers, or dual-schema support for databases that do not exist.

## Target Invariants

The final implementation must enforce all of the following:

1. Only the active provider invocation may settle an attempt.
2. Every provider invocation has one immutable completion mode.
3. Provider process exit alone can never authorize success.
4. `opencode export` is diagnostic evidence only and can never authorize or repair success.
5. A response-first node succeeds only from an invocation-bound, structurally complete terminal response that satisfies its output contract.
6. A builder succeeds only from one accepted invocation-scoped completion receipt and a valid terminal settlement.
7. Receipt directory ownership and SQLite lineage determine routing; receipt payload identifiers are claims, never routing authority.
8. Receipt publication and ingestion are bounded, no-follow, crash-safe, and idempotent.
9. An OpenCode process cannot outlive Kingdom without an independent guardian terminating it.
10. A stale replacement cannot dispatch until its predecessor is proven quiescent and the workspace is restored to trusted state.
11. Completion and stale revocation compete through one durable authority boundary; exactly one wins.
12. Automatic retries are at-least-once execution with exactly-once canonical settlement.
13. Automatic retry schedules, selected jitter, retry counts, and causes survive restart without recalculation.
14. No automatic model retry occurs after meaningful model work unless stale recovery first proves quiescence and restores trusted state.
15. Failed invocation token usage remains visible and counts toward budget decisions.
16. SQLite contention retries replay complete database transactions, never external side effects.
17. The primary UI groups attempts under one logical node while always showing the current error and retry reason.
18. Every physical attempt remains available in audit history.

## Locked Architectural Decisions

### Provider Completion Modes

Provider-backed nodes use exactly one of these modes:

| Node Class | Completion Mode |
| --- | --- |
| `advisor_consultation` | `server_response` |
| `advisory_synthesis` | `server_response` |
| `plan_drafting` | `server_response` |
| `execution_review` | `server_response` |
| `validation_gates` | `server_response` |
| `delegated_execution` | `receipt_required` |

Reviewers and validators use `server_response` because their canonical deliverable is response text with a strict verdict contract. Their workspace and tool permissions are separate from completion authority.

Only builders publish canonical filesystem deliverables and retain access to `kingdom_complete_attempt`.

Runtime-owned nodes do not have a provider completion mode.

### Completion Mode Ownership

Completion mode is a server-owned property of canonical node behavior. Workflow authors do not configure it independently.

The canonical node class resolves completion mode, and the resolved mode is persisted on the attempt and provider invocation before dispatch. Runtime execution reads the persisted mode and never infers it from an agent name, callback, capability, prompt, or receipt directory.

### Process Quiescence

Kingdom uses guardian-owned provider process trees and bounded cancellation.

If process quiescence cannot be proven, Kingdom visibly pauses the retry. It does not silently wait forever and does not dispatch a replacement into a potentially writable workspace.

Per-attempt isolated workspace generations are not part of this change. They remain a future option only if guardian-based ownership proves insufficient in production evidence.

### Retry Presentation

Attempts are grouped structurally, not hidden.

- One primary item represents one logical workflow node.
- The current error type and retry reason are visible without expanding details.
- Retry count, retry limit, and next retry time are visible when applicable.
- Every retry transition appears in the timeline.
- Every physical attempt remains available in paginated attempt history.
- Rate limiting, hard provider usage exhaustion, Kingdom budget exhaustion, timeout, and unsafe process state remain distinct.

### Token-Conscious Retry Policy

- No automatic retry for an empty or incomplete response.
- No automatic retry after meaningful model progress.
- Temporary failures before meaningful progress may retry within a strict policy budget.
- Stale attempts may retry only after process quiescence and trusted workspace restoration.
- Failed invocation usage is persisted before another retry is considered.
- The Kingdom budget guard runs before retry scheduling and again before dispatch.

## Implementation Protocol

Every implementation phase follows this local sequence:

1. Reproduce the relevant defect or missing invariant with a failing deterministic test where practical.
2. Implement the smallest complete canonical change.
3. Remove the superseded path in the same phase once the replacement is covered.
4. Run focused tests for the touched packages.
5. Run adjacent integration or contract tests affected by the change.
6. Inspect the phase diff for accidental scope growth, dead code, compatibility shims, and duplicated ownership.
7. Do not begin the next phase while the current phase's required tests are failing.

## Phase 1: Canonical Completion Authority

### Goal

Make provider completion behavior explicit, immutable, and fail-closed.

### Domain Changes

Add `ProviderCompletionMode` with exactly:

```text
server_response
receipt_required
```

Add completion mode to `workflow.NodeBehavior` in `internal/core/workflow/node_behavior.go`.

Resolve mode only from canonical node class. Do not add a second workflow-authored completion-mode field that can contradict node class.

Add one canonical `(node_class, required_role)` contract matrix. Workflow compilation and publication must reject invalid combinations before persistence, including builder roles on advisory classes and response-only roles on delegated execution.

### Persistence Changes

Modify `internal/dbmigrations/0001_initial_schema.up.sql` directly.

Add completion mode to:

- `attempt_executions`, allowing `not_applicable`, `server_response`, and `receipt_required`.
- `provider_invocations`, allowing `server_response` and `receipt_required`.
- `planning_provider_invocations`, fixed to `server_response`.

Persist mode in this order:

```text
canonical node behavior
-> attempt execution
-> provider invocation
-> terminal settlement and outbox payload
```

After attempt creation, runtime code must use the persisted mode and reject mismatches between attempt and invocation.

### Planning Schema Cleanup

Planning already uses server-owned settlement. Remove receipt-only planning schema and implementation that can never be valid:

- Completion capability fields.
- Receipt timestamps.
- `receipt_persisted` planning invocation state.
- Planning `completion_receipt` evidence kind.
- Receipt-oriented planning helpers and tests.

Because no databases exist, define only the desired final planning tables in the initial schema. Do not write migration or compatibility code for old planning rows.

### Managed Provider Profiles

Allow `kingdom_complete_attempt` only in the builder profile.

Deny it in advisor, chancellor, council, reviewer, QA, Nightraven, tester, king, and planner profiles.

Server-response invocations must receive:

- No completion capability.
- No receipt directory.
- No receipt callback.
- No completion-capability environment variable.
- No prompt instruction to call the completion tool.

Receipt-required builder invocations must reject launch if any required completion field is absent.

### Code Removal

Remove:

- Completion-mode inference based on optional request fields.
- Planner-agent name checks controlling empty-output behavior.
- Any fallback that derives mode from callback presence.
- Dead planning receipt states and helpers.

### Tests

Cover:

- Every valid node-class and role tuple.
- Every invalid tuple category.
- Persisted completion-mode agreement across attempt and invocation.
- Server-response requests rejecting receipt fields.
- Receipt-required requests rejecting missing receipt fields.
- Tool denial for every non-builder managed profile.
- Mode-specific prompt rendering.
- Runtime-owned nodes remaining provider-free.

### Phase Exit Criteria

- Completion mode is explicit at every durable execution boundary.
- Invalid workflow authority combinations fail before dispatch.
- Only builders can access the receipt tool.
- No implicit completion-mode branch remains.
- Focused workflow, schema, bootstrap asset, provider request, and orchestration tests pass.

## Phase 2: Atomic Provider Settlement

### Goal

Create one authoritative transaction for provider evidence, settlement, execution terminal state, token usage, attempt events, and projection handoff.

### Strict Stream Observation

Refactor OpenCode stream adjudication into one invocation-level observation containing:

- Provider session ID.
- Final turn and step IDs.
- Final stream sequence.
- Raw and normalized finish reason.
- Canonical terminal-step text.
- Observed tool calls and their terminal states.
- Pending tool count.
- Token usage.
- Process exit result.
- stdout and stderr drain completion.
- Timeout and cancellation flags.
- Provider error details.
- Malformed protocol count.
- Conflicting terminal evidence flag.
- Meaningful progress classification.

For `server_response`, success requires all of the following:

1. Attempt, invocation, lease, and provider session lineage match.
2. The invocation is still active and unsuperseded.
3. The authoritative final step has exact normalized reason `stop`.
4. Canonical text comes only from that terminal step.
5. Canonical text is non-empty after trimming.
6. Every observed tool call is terminal and the pending-tool set is empty.
7. No later assistant, tool, or step event exists.
8. The process exits successfully and both streams are drained.
9. No timeout, cancellation, provider error, malformed protocol event, or settlement conflict occurred.
10. Node-specific output validation succeeds.
11. Evidence comes from the live invocation stream, never export reconstruction.

Reject `length`, `content-filter`, `other`, `unknown`, `error`, blank, missing, and unrecognized finish reasons for server-owned success.

### Shared Output Validation

Create one pure node-output validator used by both completion modes.

It must enforce:

- Non-empty canonical final text.
- Exactly one valid review or validation verdict where required.
- Verdict placement as the first non-empty control line.
- Rejection of duplicate or conflicting verdicts.
- Canonical structured-payload generation.
- Builder receipt text as authoritative over incidental assistant prose.

A valid `REWORK` response is a structurally successful provider response that produces the existing workflow-level rework outcome.

### Atomic Settlement Operation

Add one composite settlement operation behind `TransitionService`, using focused execution-store transaction helpers.

Within one SQLite transaction:

1. Reload attempt, invocation, lease, node run, and completion mode.
2. Verify active invocation authority.
3. Insert or idempotently verify primary evidence.
4. Apply the mode-specific success predicate.
5. Validate and canonicalize output.
6. Insert or idempotently verify the provider settlement.
7. Revoke any remaining completion capability.
8. Move the provider invocation to its terminal state.
9. Link the attempt to its canonical settlement.
10. Commit the attempt execution terminal state and clear its lease.
11. Persist invocation token usage, including failures.
12. Append the terminal attempt event.
13. Enqueue the terminal projection outbox.
14. Commit.

Exact replay returns the existing result. Any changed lineage, mode, evidence, output, structured payload, or outcome is a settlement conflict.

### Projection Boundary

After this phase, `attempt_outbox` is projection-only.

It may project node state, workflow progression, mailbox responses, response artifacts, and builder artifacts from already committed truth.

It may not:

- Infer provider success.
- Search for or demand a receipt.
- Create or rewrite settlements.
- Replace canonical output.
- Retry empty output.
- Repair success from export data.

### Export Policy

Keep `opencode export` for diagnostics only.

Delete paths allowing export to:

- Promote an invocation to success.
- Replace terminal stream text.
- Merge assistant answers.
- Repair empty output.
- Override stream or receipt settlement.

Delete unused deferred and late-repair export phases.

### Tests

Cover:

- Clean `stop` server response.
- Empty terminal response.
- Tool-call continuation followed by `stop`.
- Unresolved tools.
- Every rejected finish reason.
- Malformed stream input.
- Non-zero process exit after apparent `stop`.
- Provider error plus apparent `stop`.
- Cancellation versus completion race.
- Supersession versus completion race.
- Exact idempotent replay.
- Contradictory replay conflict.
- SQL failure injection at each write stage.
- Review and validation PASS, REWORK, missing, duplicate, and conflicting verdicts.
- Export being unable to create success.
- Failed-invocation token persistence.

### Phase Exit Criteria

- One transaction owns canonical provider terminal truth.
- Projection consumers cannot adjudicate or repair outcomes.
- Server-response nodes complete without model-issued receipts.
- Export is diagnostic only in code and tests.
- No generic empty-output retry remains in settlement.

## Phase 3: Secure Durable Builder Receipt Spool

### Goal

Make builder receipt transport invocation-scoped, bounded, secure, and recoverable independently of execution workspaces.

### Canonical Location

Store receipts under:

```text
<WorkspaceRoot>/provider-completion/receipts/<sha256(invocation-id)>/
```

The SQLite invocation record and hashed directory determine ownership. Payload identifiers are claims to validate, never routing authority.

This location must be independent of direct publish roots, Git worktrees, workspace restoration, and worktree deletion.

### Receipt Contract

Add one versioned JSON Schema requiring:

- Exact protocol version.
- Attempt ID.
- Invocation ID.
- Completion capability.
- Non-empty final text.
- Object-valued structured payload.
- No additional properties.

Use a central 1 MiB receipt wire limit.

Bound runtime ingestion to:

- 16 receipt entries per invocation batch.
- 128 invocation inboxes per runtime pass.
- Regular `.json` files only.
- No symlinks, FIFOs, sockets, devices, or nested directories.

### Secure Filesystem Primitives

Extend `internal/securefs` and `internal/runtimefs` with focused operations for:

- Descriptor-relative descendant directory creation.
- Bounded directory enumeration.
- Bounded regular-file reads.
- No-follow path traversal.
- Empty-directory-only removal.
- Directory fsync after publication and deletion.

Do not expose arbitrary path operations to orchestration. Receipt APIs accept invocation IDs and derive paths internally.

### Atomic Tool Publication

The runtime creates the inbox before provider launch.

The completion tool:

1. Validates and serializes its payload.
2. Enforces the byte limit.
3. Creates a random temporary file exclusively with mode `0600`.
4. Writes, fsyncs, and closes the temporary file.
5. Publishes a unique final filename atomically without replacement.
6. Fsyncs the inbox directory.
7. Returns success only after durable publication.

The tool must not recursively create directories.

### Orchestration-Owned Ingestion

Move receipt ingestion out of the OpenCode adapter.

For each receipt, orchestration must:

1. Read through secure runtime filesystem APIs.
2. Enforce file type, count, and byte limits.
3. Validate the canonical schema.
4. Validate attempt, invocation, capability, mode, and node contract.
5. Record every disposition as `completion_receipt_attempt` evidence.
6. Record accepted receipts as `completion_receipt` evidence.
7. Delete the file only after its disposition is durable.
8. Treat deletion failure as cleanup debt, not settlement failure.

Receipt acceptance records candidate evidence. Final attempt settlement still runs through the atomic settlement operation from Phase 2.

### Startup Recovery

Startup must replay receipt files before finalizing missing-receipt failure.

Recovery must:

- Converge accepted receipt evidence through atomic settlement.
- Treat exact duplicate receipts idempotently.
- Reject contradictory receipts.
- Preserve malformed receipt evidence without trusting payload lineage.
- Remove only terminal empty inboxes.
- Never recursively delete unknown contents.

### Code Removal

Remove adapter-owned:

- Receipt directory reads.
- Receipt parsing.
- Receipt file deletion.
- Receipt disposition types.
- Workspace-relative receipt paths.
- Receipt callbacks.

### Tests

Cover:

- Cross-invocation payload claims.
- Crash before atomic publication.
- Crash after publication but before SQLite acceptance.
- Crash after acceptance but before deletion.
- Crash after receipt persistence but before settlement.
- Exact duplicate and conflicting receipts.
- Symlinked parent and leaf paths.
- FIFOs and non-regular entries.
- Exact byte boundary and one-byte overflow.
- Entry-count overflow.
- Directory replacement races.
- Direct and worktree modes resolving the same spool.
- Deleted execution worktree during replay.

### Phase Exit Criteria

- Builder receipt transport is independent of provider workspaces.
- Receipt routing cannot cross invocation boundaries.
- All provider-controlled receipt I/O is bounded and no-follow.
- Every crash window replays deterministically.
- No adapter receipt filesystem code remains.

## Phase 4: Guardian-Owned Provider Processes

### Goal

Ensure an OpenCode process cannot continue unowned after Kingdom exits, crashes, or cancels an invocation.

### Process Runner

Add a focused `internal/processguardian` package and hidden self-exec guardian mode to both `kd` and `kingdomd`.

Use one guardian per OpenCode command. This keeps ownership and failure scope local and avoids a second long-lived daemon.

The launch protocol is:

```text
Kingdom creates guard identity and private control pipes
-> guardian starts
-> guardian creates the child process group or Job Object
-> guardian reports child ownership
-> Kingdom durably records launch
-> Kingdom releases the launch gate
-> guardian supervises until complete or cancelled
```

OpenCode cannot pass the launch gate before Kingdom knows and durably records its process identity.

If Kingdom dies, control-pipe EOF instructs the guardian to terminate the complete child tree.

If the guardian dies while Kingdom remains alive, Kingdom uses the already acknowledged process identity to terminate the child tree. A guardian failure before acknowledgement closes the launch gate and prevents OpenCode execution.

### Platform Behavior

Darwin and Linux use:

- A dedicated child process group.
- Graceful `SIGTERM`.
- A bounded grace period.
- Forced `SIGKILL`.
- Process-group absence as the quiescence check.

Windows uses:

- Suspended process creation.
- Job Object assignment before resume.
- `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`.
- Zero active Job processes as the quiescence check.

Unsupported platforms fail clearly before provider dispatch.

### Durable Guard State

Modify the canonical initial schema to add one focused `provider_process_guards` table.

Persist:

- Guard ID.
- Stateful owner kind and owner ID.
- Provider invocation ID.
- Lifecycle state.
- Guardian and child process identities.
- Process-group or Job identity.
- Quiescence capability hash.
- Start, cancellation, exit, and quiescence timestamps.
- Exit result and operator-safe failure text.

The guardian must not write SQLite. It publishes a capability-bound atomic quiescence record under the runtime root, and Kingdom validates and ingests it.

Decree and planning invocations require durable guard records. Short maintenance commands use the same guarded runner but do not need durable recovery rows.

### Adapter Contract

Replace direct `exec.Cmd` ownership and PID-only liveness checks with a guardian process handle.

Cancellation succeeds only when:

- The complete process tree is absent.
- Guardian quiescence evidence is valid.
- stdout and stderr are drained.
- Adapter post-processing has exited.

`AttemptNotFoundError` must not count as successful cancellation when a launched process lacks durable quiescence evidence.

### Command Coverage

Route every OpenCode command through the guardian runner:

- Decree execution.
- Tool-call continuation.
- Planning execution.
- Export diagnostics.
- Strict managed-agent validation.
- Provider warming.
- Model catalog calls.

### Tests

Cover:

- Kingdom exit while OpenCode continuously writes.
- Descendant process cleanup.
- Guardian crash before and after ownership acknowledgement.
- Launch persistence failure.
- Graceful cancellation and forced termination.
- Process exit while a descendant remains alive.
- Invalid or replayed quiescence evidence.
- Missing durable ownership evidence.
- Graceful and forced Kingdom shutdown.
- Darwin/Linux behavior and Windows cross-compilation.

### Phase Exit Criteria

- No production OpenCode launch bypasses the guardian runner.
- Provider launch cannot begin before ownership is durable.
- Cancellation means proven tree quiescence, not signal delivery.
- Missing quiescence evidence fails closed.
- Direct command ownership code is removed from the adapter.

## Phase 5: Stale-Attempt Fencing And Workspace Handoff

### Goal

Make stale recovery an atomic authority transition followed by a proven process and workspace handoff.

### Atomic Stale Transition

Replace `ReassignStaleAttemptForRetry` with a general fenced retry-scheduling transition.

Within one transaction:

1. Reload the canonical attempt, execution, invocation, and lease.
2. Stop if terminal completion already committed.
3. Move the stale node to `blocked`.
4. Commit the stale attempt execution as cancelled.
5. Revoke or supersede its active provider invocation.
6. Queue `attempt_cancel_outbox` when launch handoff occurred.
7. Close its session lease.
8. Create the replacement node attempt as `pending`.
9. Persist the retry reason and handoff dependency.
10. Append blocked and retry-scheduled events.
11. Commit.

The replacement may become visible immediately but cannot dispatch.

### Cancellation Outbox As Handoff Barrier

Use the existing cancellation outbox and dispatch barrier. Do not add a parallel handoff subsystem.

A cancellation row becomes `published` only after:

- Process quiescence is proven.
- Adapter post-processing has stopped.
- A keyed per-run workspace lock is held.
- The attempt's trusted base snapshot is restored where required.
- Restoration verification succeeds.

The existing run terminal-handoff barrier continues to prevent dispatch while completion or cancellation outboxes remain pending.

Response-first nodes without a mutable execution workspace require quiescence but no workspace restoration.

### Workspace Serialization

Use one keyed per-run lock around:

- Workspace preparation and restoration.
- Result snapshot capture.
- Builder artifact calculation.
- Promotion and rollback.
- Stale-attempt cleanup.

This lock coordinates in-process filesystem mutations. Guardian quiescence protects against processes from an earlier runtime.

### Unproven Quiescence

After bounded graceful and forced cancellation windows:

- Keep the cancellation outbox pending.
- Keep the stale attempt `blocked`.
- Keep the replacement `pending`.
- Record the safe technical reason.
- Expose recovery phase `paused`.
- Permit an explicit stop retry when the guardian remains addressable.

Never mark cancellation successful merely to unblock dispatch.

### Completion Race

- Completion committed before stale revocation wins.
- Stale revocation committed first rejects later completion.
- Accepted but unsettled receipt evidence does not override cancellation.
- A stale attempt cannot snapshot, restore, publish artifacts, publish canonical mailbox output, or advance workflow state.

### Startup Ordering

Before planning recovery or decree dispatch:

1. Validate the canonical schema and runtime configuration.
2. Ingest durable guardian quiescence records.
3. Ingest durable receipt records.
4. Converge eligible terminal settlements.
5. Drain completion and cancellation outboxes.
6. Reconcile stale execution and planning state.
7. Release due retries only after their handoff barriers are clear.
8. Start ordinary dispatch.

### Tests

Cover:

- Stale transition atomically revoking invocation authority and queuing cancellation.
- Replacement blocked while cancellation is pending.
- `AttemptNotFoundError` not releasing the barrier.
- Completion winning before stale revocation.
- Revocation winning before late completion.
- Process exit without descendant exit.
- Workspace restoration before replacement dispatch.
- Stale completion unable to publish artifacts or mailbox output.
- Crash during each handoff boundary.
- Planning recovery waiting for process reconciliation.

### Phase Exit Criteria

- Replacement dispatch is impossible before process and workspace quiescence.
- Stale completion has no canonical side effects.
- The cancellation outbox is the only durable handoff barrier.
- The unsafe stale reassignment path is deleted.
- Startup follows one documented recovery order.

## Phase 6: Durable Cost-Aware Retry Scheduling

### Goal

Replace hard-coded retry loops with one durable policy decision model shared by decree and planning execution.

### Retry Persistence

Modify the canonical initial schema to add one `retry_decisions` table.

Persist:

- Decision ID.
- Subject kind: decree attempt or planning invocation.
- Run and node identity or planning operation identity.
- Source attempt or planning invocation.
- Target node run where applicable.
- Policy ID and retry class.
- Canonical error code and operator-safe text.
- Meaningful-progress classification.
- Decision: retry, fail fast, or exhausted.
- Retry index and maximum.
- Policy delay and provider retry time.
- Final absolute due time.
- Dispatch state and timestamps.
- Escalation target and resolution timestamp.

Use partial unique indexes to guarantee:

- One decision per source execution.
- One pending automatic retry per run and node.
- One pending automatic retry per planning operation.
- One target node run per retry decision.

Do not add a separate retry-series table. Run/node and planning-operation identities already provide the logical series.

### Failure Classification

Add a typed provider failure classifier that prefers structured data:

- Provider error code.
- HTTP status.
- `Retry-After` or reset time.
- Provider failure category.
- Sanitized operator-safe message.

Use these canonical distinctions:

| Error Code | Behavior |
| --- | --- |
| `llm_rate_limited` | Retry before meaningful progress |
| `llm_usage_limit_reached` | Fail fast |
| `llm_model_unavailable` | Retry before meaningful progress |
| `transient_transport` | Retry before meaningful progress |
| `timeout` | Retry only before meaningful progress |
| `llm_context_overflow` | Fail fast |
| `llm_invalid_response` | Fail fast |
| `provider_incomplete_turn` | Fail fast |
| `provider_empty_output` | Fail fast |
| `provider_cancelled` | Never retry |
| `budget_exceeded` | Fail fast |
| `agent_unresponsive` | Retry only after quiescence and restoration |

Stop emitting new `provider_timeout` failures. Use canonical `timeout`.

Message substring matching may only produce a conservative fail-fast classification when structured information is unavailable. It must not manufacture retryability.

### Meaningful Progress

Meaningful progress begins after any of:

- Non-empty assistant text.
- Reasoning output or reasoning tokens.
- Output tokens.
- Tool start or completion.
- Workspace delta.
- Provider activity proving work passed admission.

Transport setup and a provider rejection before generation do not count as meaningful progress.

### Retry Policy

| Failure | Maximum Automatic Retries |
| --- | ---: |
| Temporary rate limit before progress | 3 |
| Model unavailable before progress | 3 |
| Connection failure before progress | 1 |
| Timeout before progress | 1 |
| Failure after meaningful progress | 0 |
| Hard provider usage limit | 0 |
| Empty or incomplete response | 0 |
| Invalid output or verdict | 0 |
| Context overflow | 0 |
| Stale agent after quiescence and restoration | 1 |

Persist due time once:

```text
due_at = max(policy_jittered_time, provider_retry_time)
```

Recovery must never redraw jitter or shorten an explicit provider reset time.

### Decree Retry Lifecycle

- Settle the source invocation and persist its token usage.
- Ask `retryengine.Engine` for one policy decision.
- Persist the decision and target pending node run atomically.
- Promote the target to `ready` only when `due_at` has passed and handoff barriers are clear.
- Dispatch through the ordinary orchestration path.
- Resolve the decision as recovered on success.
- Persist exhaustion and escalation exactly once when the budget is exhausted.
- Keep the retry in the same workflow wave.

Every automatic decree retry gets a fresh node attempt, provider invocation, and provider session.

Physical attempt ordinal remains lineage and must not be used as automatic retry count.

Manual Retry and Rerun remain uncapped and do not increment or reopen an automatic retry sequence.

### Planning Retry Lifecycle

- Keep the same planning operation and ACP run.
- Set the operation from `running` back to `queued` in the failure transaction.
- Clear its lease and active invocation.
- Persist the retry decision and due time.
- Keep the planning session in processing state.
- Claim the operation only after `due_at`.
- Create a fresh provider invocation and session.
- Never duplicate messages, drafts, titles, or formalization output.
- Allow only final successful settlement to mutate canonical planning state.

### Token Accounting

- Persist usage from successful, failed, cancelled, and retried invocations.
- Run the Kingdom budget guard before scheduling and before dispatch.
- Count every dispatched retry against policy even if the provider reports zero tokens.
- Never erase or replace failed-attempt usage in analytics.

### Code Removal

Remove:

- `emptyOutputRetryBackoffs`.
- `retryExecutionOnEmptyOutput`.
- `SuppressEmptyOutputRetry`.
- Empty-output-specific retry events.
- Attempt-ordinal retry accounting.
- Immediate stale reassignment.
- Provider failure branches that bypass `retryengine.Engine`.

Keep short SQLite contention retries separate from semantic provider retries and name them accordingly.

### Tests

Cover:

- Structured rate limit versus hard usage-limit classification.
- Persisted jitter and provider retry hints.
- No dispatch before due time.
- Concurrent due-dispatch claims.
- Restart after scheduling and before dispatch.
- Restart after dispatch claim and before provider launch.
- No retry after meaningful progress.
- Failed usage counted before budget checks.
- Retry exhaustion and exactly-once escalation.
- Manual Retry and Rerun isolation from automatic counts.
- Planner retry without duplicate messages or drafts.
- Planner lease fencing across retries.
- Cancellation of a scheduled retry.

### Phase Exit Criteria

- Every semantic retry decision is durable and policy-driven.
- Retry timing survives restart exactly.
- Empty and incomplete outputs fail without another model call.
- No automatic retry path bypasses policy or budget checks.
- Manual and automatic recovery remain semantically distinct.

## Phase 7: SQLite And Resource-Boundary Hardening

### Goal

Prevent local database contention and unbounded provider data from becoming correctness failures.

### SQLite Transaction Retry

Create one transaction-level busy-retry helper.

It must:

- Retry the complete transaction body.
- Recognize typed `SQLITE_BUSY` and `SQLITE_LOCKED` errors.
- Use bounded jittered delays.
- Respect context cancellation.
- Require replay-safe transaction bodies.
- Record retry count, contention duration, and terminal error.

Apply it to short critical transactions for:

- Provider settlement.
- Stale transition and retry scheduling.
- Cancellation outbox publication.
- Retry due dispatch.
- Planning operation claim and finalization.
- Planning title operation claim and finalization.
- Other audited state transitions currently treating one busy response as terminal.

Never wrap provider calls, filesystem publication, or other external side effects in a database retry.

Do not upgrade the SQLite driver without evidence of a driver defect.

### Resource Limits

Centralize provider runtime limits for:

- Final output bytes.
- Provisional output bytes.
- stderr diagnostic tail.
- Provider event-line bytes.
- Receipt entries and bytes.
- Prompt bytes.
- Workspace traversal entries and bytes.
- Startup replay batch sizes.

Store a bounded stderr tail rather than unbounded stderr.

Reject oversized canonical output with a typed non-retryable error.

Validate prompt size before process launch. Do not rely on operating-system argument limits.

### Tests

Cover:

- Busy at transaction begin, statement execution, and commit.
- Full transaction replay without duplicate effects.
- Immediate return for non-busy errors.
- Context cancellation during contention.
- Exact resource boundaries and one-byte or one-entry overflow.
- Large malformed provider events.
- Bounded stderr behavior.
- Oversized prompt rejection before provider launch.

### Phase Exit Criteria

- Critical SQLite contention is retried at the transaction boundary.
- No external side effect can replay with a busy transaction.
- Every provider-controlled accumulation has one tested upper bound.
- Resource-limit errors are typed and operator-safe.

## Phase 8: Canonical Logical-Node API

### Goal

Give every client one canonical logical execution projection instead of requiring frontend reconstruction of attempts and retries.

### API Types

Replace `RuntimeRetryStatus` and `runtime_retries` with:

- `RunExecutionFailure`.
- `RunRecoveryState`.
- `RunNodeExecution`.

`RunExecutionFailure` contains:

- Canonical error code.
- Error classification.
- Error category.
- Retryability.
- Durable operator-safe text.

`RunRecoveryState` contains:

- Phase: `scheduled`, `paused`, `running`, `recovered`, or `exhausted`.
- Trigger: automatic retry, operator retry, or operator rerun.
- Reason code.
- Source and target attempt identities.
- Retry index and maximum.
- Absolute scheduled time.
- Last update time.

`RunNodeExecution` contains:

- Logical node identity.
- Required role and execution ownership.
- Current physical execution.
- Attempt count.
- Latest logical activity time.
- Current failure.
- Current recovery state.

Make this a direct contract replacement. Do not retain legacy fields beside the new representation.

### Attempt History Endpoint

Add:

```text
GET /runs/{runId}/nodes/{nodeId}/attempts
```

The endpoint returns:

- Current attempt identity.
- Newest-first physical attempts.
- Failed, cancelled, superseded, and successful history.
- A run-and-node-bound pagination cursor.
- Retry relationships and token usage where available.

Do not return unbounded attempt history in the main execution response.

### Shared Backend Projection

Build one store query and projection used by:

- Run graph.
- Execution surface.
- Run summary.
- Timeline enrichment.

Selection rules must preserve these invariants:

- An older active or blocked attempt remains current over a newer pending retry.
- A replacement becomes current only at `starting` or `running`.
- Superseded attempts never become canonical.
- Retry activity updates logical activity without replacing the current attempt.
- Graph and execution endpoints select the same current attempt.

Avoid per-node query loops.

### Event Contract

Standardize retry event payloads with:

- Recovery trigger.
- Retry index and maximum.
- Source and target attempt IDs.
- Canonical error code.
- Absolute scheduled time.
- Recovery phase.

Manual Retry and Rerun use dedicated reason codes rather than `internal_error`.

SSE remains a replayable notification and optimistic-display channel. Canonical API refetch remains authoritative.

### Code Removal

Remove:

- Event-JSON reconstruction of active retries.
- `RuntimeRetryStatus`.
- `fetchActiveRuntimeRetries`.
- Frontend-owned current-attempt priority logic once the API projection is in use.
- Legacy execution response fields replaced by typed failure and recovery objects.

### Tests

Cover:

- Multiple physical attempts forming one logical item.
- Blocked predecessor remaining current over queued replacement.
- Replacement becoming current when starting.
- Paused, scheduled, running, recovered, and exhausted phases.
- Rate-limit, usage-limit, timeout, budget, and generic failure classification.
- Superseded attempts in paginated history.
- Cursor scope enforcement.
- Identical graph and execution current-attempt selection.
- Stable projections under malformed historical event payloads.

### Phase Exit Criteria

- Clients receive one typed logical execution model.
- Current failure and recovery state require no event parsing.
- Attempt history remains complete and bounded.
- No old retry projection or duplicate selection algorithm remains.

## Phase 9: Professional Retry And Failure UX

### Goal

Present recovery clearly to non-technical users without hiding operational facts.

### Primary Node Presentation

Render one card or graph item per logical workflow node.

The primary item always shows:

- Step title.
- Current status.
- Exact user-safe error classification.
- Whether recovery is automatic.
- Retry count and maximum.
- Retry time or pause reason.
- Attempt count when greater than one.
- Required user action when recovery is not automatic.

The current error must never require opening **Attempt History**.

### Operator Copy

| Situation | Heading | Description |
| --- | --- | --- |
| Temporary rate limit | **Retry Scheduled** | The provider rate limit was reached. Kingdom will retry this step at {time}. |
| Timeout before progress | **Retry Scheduled** | This step timed out before work began. Kingdom will retry it at {time}. |
| Waiting for old process | **Retry Paused** | Kingdom is waiting for the previous attempt to stop before starting the retry. |
| Retry running | **Retry Running** | Kingdom is trying this step again now. |
| Retry recovered | **Retry Recovered** | The retry succeeded, and this step resumed. |
| Retries exhausted | **Retries Exhausted** | Kingdom could not complete this step after {count} automatic retries. |
| Provider usage exhausted | **Usage Limit Reached** | Restore provider capacity or select another model, then retry this step. |
| Kingdom budget exhausted | **Budget Exceeded** | This run reached its configured Kingdom budget. |
| Unsafe process state | **Step Paused** | Kingdom could not confirm that the previous agent stopped. Resolve the process before retrying. |

### Attempt History

Add **Attempt History (N)** as progressive disclosure.

History is collapsed by default but immediately accessible. Each attempt shows:

- Attempt number.
- Start and finish time.
- Outcome.
- Exact error.
- Retry relationship.
- Supersession status.
- Token usage where available.
- Provider invocation ID under technical details.

When viewing old evidence, show **Historical Attempt** and a **Return to Current Attempt** action.

### Timeline

Every durable retry transition receives a visible concise milestone:

- **Retry Scheduled - Rate Limit**
- **Retry Paused - Previous Attempt Still Running**
- **Retry Started - Attempt 2**
- **Retry Recovered - Attempt 2**
- **Retries Exhausted - Timeout**
- **Usage Limit Reached**

Keep operational retries visually distinct from review rework, validation rework, operator Retry, and operator Rerun.

### Responsive Design

- Keep the graph on suitable desktop widths.
- Render the same presentation model as a vertical workflow list on small screens.
- Do not shrink graph nodes below readable or tappable sizes.
- Use at least 44px interactive targets.
- Avoid horizontal page scrolling.
- Preserve one page scroll owner.
- Lazy-load attempt history.
- Preserve selected historical attempts across SSE refreshes.
- Respect reduced-motion preferences.

### Accessibility

- Retry progress uses `role="status"` and polite announcements.
- Exhausted and non-retryable failures use `role="alert"`.
- History disclosure uses `aria-expanded` and `aria-controls`.
- Current and historical attempts are identified textually, not by color alone.
- Countdown changes do not continuously announce to screen readers.
- Absolute retry time remains available in accessible text and tooltips.

### Frontend Simplification

Delete duplicated retry labels, tones, and attempt-selection logic. Keep one typed mapping from API failure and recovery state to:

- Display copy.
- Status tone.
- Iconography.
- Accessible text.
- Timeline label.

### Tests

Cover:

- One logical item for multiple physical attempts.
- Error type and retry reason visible before expanding history.
- Distinct rate-limit and usage-limit copy and actions.
- Queued retry not displacing a draining predecessor.
- Starting replacement becoming current.
- Attempt-history lazy loading and selection.
- Selection surviving canonical refresh.
- Timeline labels for every recovery phase.
- Accessible names including retry phase and attempt count.
- Keyboard behavior and focus restoration.
- Mobile layout without clipped controls or nested page scrolling.

### Phase Exit Criteria

- Retry grouping never hides the current error or reason.
- The default experience is understandable without provider or programming knowledge.
- Technical evidence remains available without dominating the primary surface.
- Desktop and mobile layouts are clean, responsive, and accessible.
- No duplicate frontend retry interpretation remains.

## Phase 10: Remove Superseded Surfaces And Align Documentation

### Goal

Finish with one architecture, one vocabulary, and no abandoned implementation paths.

### Dead-Code Sweep

Search for and remove all remaining references to:

- Adapter-owned receipt scanning.
- Workspace-relative receipt routing.
- Implicit completion-mode detection.
- Non-builder completion capabilities.
- Planning receipt states.
- Export-derived success.
- Same-attempt empty-output retry.
- Old stale reassignment.
- Attempt-ordinal retry counting.
- Event-derived current retry state.
- Legacy OpenAPI fields and generated types.
- Duplicate retry copy and status mapping.
- Unused constants, structs, fixtures, and compatibility helpers.

Do not preserve aliases or deprecated wrappers when all callers are in this repository.

### Documentation

Update `docs/system-architecture.md` to describe only implemented behavior:

- Completion-mode ownership.
- Server-response success predicate.
- Builder receipt authority.
- Runtime-owned receipt spool.
- Atomic provider settlement.
- Guardian process ownership.
- Quiescence-before-retry invariant.
- Durable retry decisions.
- Logical-node API and UI projection.

Update `docs/errors-and-retries.md` with:

- Canonical provider errors.
- Meaningful-progress rules.
- Retry budgets.
- Rate limit versus usage limit.
- Retry exhaustion and escalation.
- Manual versus automatic recovery.

Update relevant operational documentation only for actual operator-visible behavior, including process-pause recovery and provider usage exhaustion.

Regenerate OpenAPI Go and TypeScript types. Do not manually patch generated files.

### Tests

Run repository searches for every removed symbol and old error code.

Run contract generation and drift checks.

Verify managed bootstrap copies exactly match canonical schemas and policies.

### Phase Exit Criteria

- No old implementation path or compatibility layer remains.
- Documentation matches implemented runtime behavior.
- Generated contracts are current.
- Canonical and bootstrap-managed policies and schemas match.
- Focused and adjacent tests pass.

## Phase 11: Independent Review Gauntlet

### Goal

Review the complete diff to `main` without anchoring reviewers to this plan, implementation rationale, or previous reviewer conclusions.

This phase occurs once, after all implementation phases and before full verification and runtime evidence.

### Review Sequence

Reserve five independent review slots and execute them sequentially, never in parallel.

Each review must use a fresh agent session. Do not resume a previous reviewer. Do not provide this plan, implementation notes, prior findings, or prior remediation decisions in the reviewer prompt. The reviewer may read repository files, but must independently derive expected behavior from the accepted architecture, contracts, code, tests, and diff.

Use the following prompt, adjusted only for repository location and available verification commands:

```text
Independently review the complete current worktree diff against main.

Do not rely on implementation plans, implementation rationale, prior reviews, or assumptions about what the changes intended to do. Derive the expected behavior independently from the accepted architecture, public contracts, surrounding code, and tests.

Check whether the changes live up to Kingdom's engineering bar and follow best practices. There must be no duct-tape solutions or lazy implementations. Prefer scalable, maintainable, reliable, clean code and long-term strategic fixes. Simple is better than over-engineered, but simplicity must not omit required correctness. This project has not been deployed and has no existing databases, so backward-compatibility code, legacy paths, deprecated aliases, dual behavior, dead code, and migration shims are defects rather than requirements.

Prioritize correctness bugs, security or trust-boundary failures, race conditions, crash-recovery gaps, broken invariants, behavioral regressions, resource-bound risks, API and persistence inconsistencies, poor error handling, missing tests, and UX or accessibility regressions. Check that state ownership and transaction boundaries are clear and that the implementation fixes root causes in the canonical layer.

Before reporting a finding, validate it by tracing the relevant code and tests. Reproduce it or identify the concrete failing path where practical. Do not state speculative issues merely to produce findings. If evidence is insufficient, identify it as a question rather than a finding.

Return findings first, ordered by severity, with exact file and line references and a concrete explanation of impact. Include open questions or residual testing risks afterward. If there are no valid findings, state that explicitly. Do not modify files.
```

### Exit And Remediation Rules

Apply these rules after each reviewer returns:

1. Validate every reported finding independently before changing code.
2. Reject speculative or invalid findings with a recorded technical reason.
3. For valid findings, create a concise remediation plan that follows the same engineering standard as this document.
4. Fix valid findings in the canonical layer.
5. Remove any superseded code introduced by the fix.
6. Run focused tests and the affected implementation phase's complete test gate.
7. Inspect the resulting diff before starting the next reviewer.
8. Never tell the next reviewer what earlier reviewers found or how those findings were fixed.

The review gauntlet stops early and passes immediately when any reviewer returns no valid findings.

If reviewers one through four return valid findings, address and verify them before starting the next review.

If reviewer five returns valid findings, address and verify all of them. No sixth review is required. The gauntlet then passes only if the fixes and affected phase gates are green.

If any valid finding cannot be resolved, the gauntlet remains blocked and full verification must not begin.

### Phase Exit Criteria

- At least one reviewer returned no valid findings, or all valid findings from the fifth reviewer were fixed and verified.
- Every accepted finding has a corresponding tested remediation.
- Every rejected finding has a concrete technical reason.
- No reviewer was primed with prior findings or implementation rationale.
- The final diff has no unresolved review finding.

## Phase 12: Full Verification And Runtime Evidence

### Goal

Prove the complete system works through repository checks, race testing, installation, restart recovery, and browser-visible behavior.

### Repository Verification

Run:

```bash
make contracts-generate
make contracts-check
make test
make test-web
make lint-web
make test-race
make verify
make test-integration
make test-e2e
```

Cross-compile the Windows guardian implementation:

```bash
GOOS=windows GOARCH=amd64 go test -c ./internal/processguardian
```

Do not treat compilation alone as proof of Darwin/Linux process-tree behavior. Run native process-group integration tests as part of verification.

### Fresh Runtime Verification

Run:

```bash
make install
```

Then:

1. Create a fresh directory under `kd-sandbox/`.
2. Run `kd init` in that directory.
3. Run `kd summon` on an available port other than `8080`.
4. Use `GitHub Copilot - GPT-5.4 Mini` with `Low` thinking if model selection appears.
5. Verify a normal server-response node.
6. Verify a builder receipt completion.
7. Verify a temporary rate-limit retry.
8. Verify a hard usage-limit failure.
9. Verify retry pause while predecessor cancellation is pending.
10. Verify successful retry recovery.
11. Verify retry exhaustion.
12. Verify restart recovery from pending receipt ingestion.
13. Verify restart recovery from committed settlement with pending projection.
14. Verify restart recovery from pending cancellation.
15. Verify restart recovery from a scheduled retry.
16. Verify attempt history and timeline evidence.
17. Verify desktop and mobile layouts.
18. Verify keyboard and accessible status behavior.

Capture command output, logs, screenshots, and relevant artifacts in the fresh `kd-sandbox/` verification directory.

### Final Acceptance Criteria

The implementation is complete only when:

- No stale process can overlap a replacement against the same workspace.
- No receipt can settle a different invocation.
- No provider success can be fabricated from process exit or export.
- Server-response nodes complete without model-issued receipts.
- Only builders retain the completion tool.
- Settlement survives every tested crash boundary.
- Retries survive restart without duplicate dispatch or recalculated timing.
- Token-expensive retries are prevented after meaningful work.
- Failed usage remains in budget and analytics totals.
- Rate limits and hard usage limits are visibly distinct.
- Retry causes remain prominent when attempts are grouped.
- Every physical attempt remains auditable.
- SQLite contention does not duplicate state or side effects.
- Every provider-controlled input and output is bounded.
- Desktop and mobile UX are responsive, accessible, and professional.
- No superseded implementation or compatibility path remains.
- The independent review gauntlet has passed.
- All repository, integration, end-to-end, and runtime verification gates pass.

## Explicit Non-Goals

This work does not add:

- Containers.
- Distributed scheduling.
- A second database.
- Per-attempt isolated workspace generations.
- Automatic merge semantics.
- Generic multi-provider abstractions.
- Model-based error classification.
- Automatic incomplete-turn repair prompts.
- Compatibility APIs or feature-flagged dual behavior.
- A second workflow engine or generic durable-execution framework.

These exclusions keep the implementation focused on the smallest architecture that fully enforces the required invariants.
