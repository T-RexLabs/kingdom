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
- `budget_exceeded`
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
- `provider_incomplete_turn` covers an incomplete or contradictory terminal provider turn. For response-first nodes this includes a non-`stop` finish, missing final turn/step identity or sequence, unfinished tools, later stream events, or conflicting terminal evidence. For builder execution, a missing or invalid required receipt also fails rather than authorizing success.
- `provider_empty_output` covers a purported terminal success with no canonical terminal text. It applies to response-first execution, planner terminal-success output, and receipt-backed output validation.
- Process exit, provisional output, artifacts, mailbox projections, and export diagnostics cannot independently authorize provider-backed decree or planning success.
- Provider-backed planner operations do not use a `kingdom_complete_attempt` receipt. The server settles the active planner invocation from the provider's adjudicated terminal outcome and non-empty runtime-assembled output; process exit, provisional stream text, and export diagnostics cannot independently authorize success.
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

Automatic retry policy is recorded as one durable `retry_decisions` row with the canonical failure code, operator-safe reason, meaningful-progress result, retry index, policy delay, provider retry hint, absolute due time, source/target lineage, dispatch state, and resolution. A restart reloads that row; decree retries release only when due, and planning owns one bounded due-time wake in addition to periodic recovery. Durable claims remain the authority, so a wake cannot launch early or duplicate a recovered retry.

Meaningful progress is any observed tool call, reasoning event, non-empty text event, reasoning-token use, or output-token use. It disables automatic retry, including automatic exhaustion escalation. Planner stale recovery checks its durable ACP operation events as well as provider stream metadata, so progress remains visible to retry policy after a worker disappears. A failed dispatched retry target resolves its predecessor as `failed` while its successor or terminal policy decision is recorded. Cancelling a queued retry resolves its decision as `cancelled` and makes it ineligible for dispatch. Without meaningful progress, exhaustion resolves as `escalated`: decree exhaustion delivers one durable escalation mailbox message to the configured target role (normally `chancellor`), while planning exhaustion delivers the same target, canonical reason, and operator-safe text through its durable ACP operation event.

The run attempt budget is a run-wide count, not a node attempt ordinal. The automatic successor reservation checks the prospective count in the same transaction that would create the target: 60 existing attempts remain valid, but a 61st automatic successor is not created. The retry decision becomes a `budget_exceeded` fail-fast disposition and the existing run terminalization path fails the run while retaining the source failure evidence.

These limits govern automatic recovery, not workflow review/validation rework. Node attempt ordinals have no separate generic cap. Explicit operator Retry and Rerun are uncapped, but can reopen a cancelled run only after provider cancellation, completion projection, attempt settlement, and session leases are quiescent. If managed worktree deletion has started, Retry and Rerun return `409 dependency_blocked` until cleanup finishes; cleanup that has not begun deleting files is cancelled atomically when the run reopens. After a cleanup conflict, submit a new action with a new idempotency key because the original 409 response is replayable.

`resource_limit_exceeded` is terminal and requires operator action. Kingdom continues to bound provider event and output accumulation, diagnostics, receipt and terminal-handoff transport, workspace traversal, and startup replay. It does not impose an aggregate provider-prompt byte cap or reject prompt size before launch: complete first-party decree and planner content reaches OpenCode over stdin. An actual provider context-window rejection is `llm_context_overflow` and remains non-retryable. Queued planner operations bound with an older linked-source assembler version fail before provider invocation, and any pending automatic retry is cancelled; stale running operations use the normal process-quiescence barrier before failing. The immutable old binding is not rewritten and no legacy assembler is loaded, so submit a new planner operation to use the current context. If a provider-created workspace exceeds its safe capture boundary, Kingdom records the failure and quarantines that run's execution target instead of attempting an unbounded cleanup or dispatching another attempt into untrusted files.

## Paused Stop Recovery

When Kingdom cannot prove that a stale provider process stopped, it keeps the stale node `blocked`, leaves the cancellation handoff pending, and does not release its replacement. This does not automatically send repeated stop requests. The paused execution exposes **Retry Stop** only for an `agent_unresponsive` attempt with a pending handoff and an addressable guardian. The action is idempotent and requests one further bounded stop attempt; it never bypasses guardian quiescence, trusted workspace restoration, or the cancellation handoff barrier. Investigate the guardian and cancellation handoff first; use Retry or Rerun only after the run is eligible to reopen.
