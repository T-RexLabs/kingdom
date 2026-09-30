# Errors and Retries

This document explains the main runtime error categories and how retry policy treats them.

Source references:

- `policies/retry_policy.yaml`
- `contracts/fixtures/error_codes/error_matrix.v1.json`

Current runtime note:

- Kingdom currently executes host-launched OpenCode sessions against prepared workspaces.

## Canonical Codes by Category

### Core

- `transient_transport`
- `timeout`
- `invalid_output_schema`
- `policy_violation`
- `dependency_blocked`
- `resource_limit_exceeded`
- `internal_error`

### Provider

- `llm_rate_limited`
- `llm_model_unavailable`
- `llm_usage_limit_reached`
- `llm_context_overflow`
- `llm_invalid_response`
- `provider_incomplete_turn`
- `provider_empty_output`
- `provider_unknown_failure`
- `provider_cancelled`

Provider completion note:

- Structured provider error metadata is classified before transport status: quota, usage, credit, billing, and hard-limit identifiers become `llm_usage_limit_reached`, even with HTTP `429`. A rate-limit status or rate-limit identifier without those usage-limit identifiers becomes `llm_rate_limited`. Provider prose is not a control channel and unclassified provider failures are `provider_unknown_failure`.
- `provider_incomplete_turn` covers an incomplete or contradictory terminal provider turn, including a non-`stop` finish, missing final turn/step identity or sequence, unfinished tools, later stream events, or conflicting terminal evidence.
- `provider_empty_output` covers a purported terminal success with no canonical terminal text. It applies to work order execution and planner terminal-success output.
- Process exit, provisional output, artifacts, mailbox projections, and export diagnostics cannot independently authorize provider-backed work order or planning success.
- Work Order completion handoffs preserve terminal observations for restart recovery; they do not independently authorize success. The server settles the active invocation from the strict terminal response and node-specific output validation.
- Provider-backed planner operations have a separate server-owned operation lifecycle. The server settles the active planner invocation from the provider's adjudicated terminal outcome and non-empty runtime-assembled output; process exit, provisional stream text, and export diagnostics cannot independently authorize success.
- Planner canonical persistence is still defined by planning messages, variants, drafts, and session fields. Those writes happen only after the invocation-scoped `stream_observation` evidence and server-owned settlement are recorded atomically.

### Runtime

- `agent_unresponsive`

### Control Plane

- `event_sequence_gap`
- `idempotency_conflict`
- `idempotency_key_expired`
- `idempotency_pending`

## Retry Mapping

- Rate limit or model unavailable: `llm_rate_limited`, `llm_model_unavailable`; up to 3 automatic retries.
- Transient or timeout: `transient_transport`, `timeout`; up to 1 automatic retry.
- Conditional stale-agent recovery: `agent_unresponsive`; up to 1 automatic retry, but only after the cancellation handoff proves guardian quiescence and any required trusted-workspace restoration completes. `idempotency_pending` is also conditional and has a one-retry policy.
- Internal runtime: `internal_error`; up to 1 automatic retry.
- All other canonical failures, including `llm_context_overflow` and `llm_usage_limit_reached`, have 0 automatic retries and fail fast.

The policy applies full jitter to retry backoff. Kingdom uses the later of its policy due time and a valid provider retry hint when one is available.

An `idempotency_key_expired` response means the current request was not admitted under that key; use a new key only after receiving this response. This applies when there is no retained pending reservation. For transport failures without a response, retain the original key so a retry can recover the original result without duplicating the mutation.

An `idempotency_pending` response means the original mutation is unresolved: it may still be executing, or the server could not establish whether its side effect committed. Retry only with the same key; never replace it with a fresh key. Pending reservations are retained regardless of key age, so an expired key with an unresolved reservation continues to return `idempotency_pending`; a fresh key could duplicate a side effect that already committed.

The web UI retains pending and transport-ambiguous requests in memory and exposes only same-key recovery. `kd` retries automatically; if its retry budget is exhausted, it reports the exact key. Resume the corresponding leaf mutation with global `--idempotency-key <reported-key>` rather than rerunning with a fresh key.

Automatic retry policy is recorded as one durable `retry_decisions` row with the canonical failure code, operator-safe reason, meaningful-progress result, retry index, policy delay, provider retry hint, absolute due time, source/target lineage, dispatch state, and resolution. A restart reloads that row; work order retries release only when due, and planning owns one bounded due-time wake in addition to periodic recovery. Durable claims remain the authority, so a wake cannot launch early or duplicate a recovered retry.

Meaningful progress is any observed tool call, reasoning event, non-empty text event, reasoning-token use, or output-token use. It disables automatic retry, including automatic exhaustion escalation. Planner stale recovery checks its durable ACP operation events as well as provider stream metadata, so progress remains visible to retry policy after a worker disappears. A failed dispatched retry target resolves its predecessor as `failed` while its successor or terminal policy decision is recorded. Cancelling a queued retry resolves its decision as `cancelled` and makes it ineligible for dispatch. Without meaningful progress, exhaustion resolves as `escalated`: work order exhaustion delivers one durable escalation mailbox message to the configured target role (normally `coordinator`), while planning exhaustion delivers the same target, canonical reason, and operator-safe text through its durable ACP operation event.

There is no run-wide attempt-count budget. Automatic successors remain subject to the per-error retry policy, meaningful-progress rule, durable due time, and recovery barriers. Node attempt ordinals record lineage rather than impose a separate ceiling.

Retry limits govern automatic recovery, not workflow review/validation rework. Review and validation retain their configured cycle caps and ready-node auto-success behavior; `run_until_all_approve` bypasses those loop caps. Explicit operator Retry and Rerun are uncapped, but can reopen a cancelled run only after provider cancellation, completion projection, attempt settlement, and session leases are quiescent. If managed worktree deletion has started, Retry and Rerun return `409 dependency_blocked` until cleanup finishes; cleanup that has not begun deleting files is cancelled atomically when the run reopens. After a cleanup conflict, submit a new action with a new idempotency key because the original 409 response is replayable.

`resource_limit_exceeded` remains non-retryable for retained control-plane resource boundaries, such as snapshot lineage depth. It is not a workload-size policy: provider output, diagnostics, artifact content, and tracked workspace files are no longer rejected by fixed byte/count/directory-depth ceilings. Streaming, disk-backed staging, and chunked persistence replace those limits. Display previews remain bounded; complete canonical text and invocation diagnostics are stored separately. Guardian metadata, compact handoff envelopes, startup/recovery batching, cancellation barriers, and active timeouts remain bounded.

Kingdom does not impose an aggregate provider-prompt byte cap or reject prompt size before launch: complete first-party work order and planner content reaches OpenCode over stdin. An actual provider context-window rejection is `llm_context_overflow` and remains non-retryable. Queued planner operations bound with an older linked-source assembler version fail before provider invocation, and any pending automatic retry is cancelled; stale running operations use the normal process-quiescence barrier before failing. The immutable old binding is not rewritten and no legacy assembler is loaded, so submit a new planner operation to use the current context. Untrusted or unrestorable execution targets remain quarantined until trusted restoration succeeds; cleanup remains resumable and batched.

## Paused Stop Recovery

When Kingdom cannot prove that a stale provider process stopped, it keeps the stale node `blocked`, leaves the cancellation handoff pending, and does not release its replacement. This does not automatically send repeated stop requests. The paused execution exposes **Retry Stop** only for an `agent_unresponsive` attempt with a pending handoff and an addressable guardian. The action is idempotent and requests one further bounded stop attempt; it never bypasses guardian quiescence, trusted workspace restoration, or the cancellation handoff barrier. Investigate the guardian and cancellation handoff first; use Retry or Rerun only after the run is eligible to reopen.

Once guardian evidence proves the process stopped, Kingdom automatically resumes cancellation after its local worker and any shared-workspace consumers finish. This wait survives restart and needs no further **Retry Stop** action. Workspace restoration and terminal projection must still succeed before a replacement is released.
