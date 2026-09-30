# Errors and Retries

This document explains the main runtime error categories and how retry policy treats them.

Source references:

- `policies/retry_policy.yaml`
- `contracts/fixtures/error_codes/error_matrix.v1.json`

Current runtime note:

- Kingdom currently executes host-launched OpenCode sessions against prepared workspaces.
- Container-specific runtime codes remain part of the canonical error matrix, but they do not imply that the current happy-path execution transport is container-based.

## Canonical Codes by Category

### Core

- `transient_transport`
- `timeout`
- `invalid_output_schema`
- `policy_violation`
- `dependency_blocked`
- `budget_exceeded`
- `internal_error`

### Provider

- `llm_rate_limited`
- `llm_model_unavailable`
- `llm_context_overflow`
- `llm_invalid_response`
- `provider_incomplete_turn`
- `provider_cancelled`

Provider completion note:

- `provider_incomplete_turn` is the terminal provider-backed decree execution failure code for an invocation that does not produce one valid runtime-owned completion receipt the runtime can accept for success.
- For provider-backed decree execution, missing or invalid receipt remains failure even if output text, artifacts, mailbox projections, or later export diagnostics exist.
- Provider-backed planner operations do not use a `kingdom_complete_attempt` receipt. The server settles the active planner invocation from the provider's adjudicated terminal outcome and non-empty runtime-assembled output; process exit, provisional stream text, and export diagnostics cannot independently authorize success.
- Planner canonical persistence is still defined by planning messages, variants, drafts, and session fields. Those writes happen only after the invocation-scoped `stream_observation` evidence and server-owned settlement are recorded atomically.

### Runtime

- `agent_unresponsive`
- `session_lease_expired`
- `tool_execution_failed`
- `container_start_failed`
- `container_oom_killed`

### Control Plane

- `event_sequence_gap`
- `idempotency_conflict`
- `idempotency_key_expired`
- `idempotency_pending`

## Retry Mapping

- `transient_transport_provider_session` (retryable): `transient_transport`, `timeout`, `llm_rate_limited`, `llm_model_unavailable`, `session_lease_expired`, `container_start_failed`
- `conditional_limited` (retryable, capped): `agent_unresponsive`, `container_oom_killed`, `tool_execution_failed`, `idempotency_pending`
- `internal_runtime` (retryable, minimal): `internal_error`
- `non_retryable` (fail-fast): `invalid_output_schema`, `policy_violation`, `idempotency_conflict`, `idempotency_key_expired`, `dependency_blocked`, `llm_invalid_response`, `llm_context_overflow`, `provider_incomplete_turn`, `provider_cancelled`, `budget_exceeded`, `event_sequence_gap`

An `idempotency_key_expired` response is definitive: the server did not admit the mutation. Create a new key only after receiving this response. For transport failures without a response, retain the original key so a retry can recover the original result without duplicating the mutation.

An `idempotency_pending` response means the original mutation may still be executing. Retry only with the same key; never replace it with a fresh key.

The web UI retains pending and transport-ambiguous requests in memory and exposes only same-key recovery. `kd` retries automatically; if its retry budget is exhausted, it reports the exact key. Resume the corresponding leaf mutation with global `--idempotency-key <reported-key>` rather than rerunning with a fresh key.

Retry exhaustion escalates to `chancellor` via dead-letter flow.

These limits govern runtime-initiated recovery and workflow rework. Node attempt ordinals have no separate generic cap, and explicit operator Retry and Rerun actions on failed or fully settled cancelled runs are uncapped.
