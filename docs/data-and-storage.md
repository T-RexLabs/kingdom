# Data and Storage

This document explains what Kingdom stores durably and how that storage is used.

The current control plane uses SQLite with explicit startup pragmas.

## Required Pragmas

- `PRAGMA foreign_keys = ON;`
- `PRAGMA journal_mode = WAL;`
- `PRAGMA synchronous = NORMAL;`
- `PRAGMA busy_timeout = 5000;`

## Why

- `foreign_keys=ON` enforces relational integrity checks expected by section 10.2 constraints.
- `journal_mode=WAL` improves concurrent read/write behavior for API and scheduler workloads.
- `synchronous=NORMAL` balances durability and performance for WAL mode.
- `busy_timeout=5000` reduces transient lock-failure churn under bursty writes.

## Durable Execution Tables

- `provider_invocations`: runtime-owned invocation identity, provider session linkage, supersession state, lease-generation-fenced terminal-handoff publication/recovery markers, and replayable handoff-file cleanup debt for each provider-backed decree execution attempt dispatch.
- `provider_completion_evidence`: append-only stream-observation and diagnostic evidence for provider-backed decree execution attempts.
- `provider_completion_settlements`: canonical invocation-scoped settlement truth for provider-backed decree execution attempts (`success`, `failed`, or `cancelled`) with evidence linkage.
- `provider_terminal_handoffs`: durable lease-fenced terminal observations for provider-backed decree invocations, written after the pending marker and before settlement; file-only observations are replayed before lost-handoff recovery.
- `provider_process_guards`: durable provider-command guardian ownership, lease lineage, and acknowledged/quiescent lifecycle. Quiescence evidence is file-backed under the runtime root before ingestion, then retained in SQLite for recovery.
- `attempt_executions`: durable attempt lifecycle and latest execution projection consumed by orchestration and UI (`queued` -> `starting` -> `running` -> `terminal_observed` -> `committed_*`). For provider-backed decree execution attempts, it references the active invocation and canonical runtime settlement; it does not own settlement truth itself. It also carries replayable cleanup debt for attempt-scoped response-only workspaces, which are isolated copies rather than execution targets or snapshots.
- `attempt_event_log`: canonical per-attempt execution event stream with monotonic `seq`.
- `attempt_outbox`: durable completion handoff queue consumed by orchestration for terminal projection.
  - `pending`: awaiting replay/projection.
  - `published`: successfully projected.
  - `failed`: dead-lettered after unrecoverable replay payload/invariant failure so poison rows stop blocking later completions.
- `node_runs`: durable per-node retry lineage (`attempt`) plus explicit cross-node review/validation wave lineage (`wave`).
- `workspaces`: execution workspace ownership and lifecycle (`active` and `released`) for each node run.
- `attempt_cancel_outbox`: one durable provider-stop handoff per attempt, including requested reason, dispatch/awaiting-quiescence/workspace-restoration state, retry count, and completion publication.
- `retry_decisions`: canonical automatic retry or fail-fast decision for a decree attempt or planning invocation, with policy identity, failure code, operator-safe text, meaningful-progress result, budget, provider hint, due time, source/target lineage, dispatch state, escalation target, and final resolution.

For provider-backed decree execution, the atomic settlement boundary writes stream evidence, canonical settlement, terminal attempt state, token usage, and the completion outbox record. The outbox is a projection handoff, not independent settlement authority.

## Artifact Tables

- `artifacts`: artifact metadata (`artifact_kind`, producer role, path, checksum, size, provenance).
- `artifact_contents`: immutable artifact bytes keyed by `artifact_id`.

Artifact content persistence is the system of record. Publishing files into the project root or a git worktree is a separate projection step.

## Planning Tables

- planning sessions, messages, drafts, context snapshots, operations, and operation events are all persisted in SQLite.
- `planning_provider_invocations`, `planning_provider_completion_evidence`, and `planning_provider_completion_settlements` store runtime-owned invocation-scoped settlement for provider-backed planner work.
- `planning_operations` links each provider-backed planner operation to its active invocation, provider session, and canonical settlement.
- planner canonical state still persists through planning sessions, messages, drafts, and session fields; the planner settlement tables authorize those writes but do not replace the planning tables as transcript or draft truth.
- planner replay and recovery depend on these tables rather than on request-local or browser-local state.

## Local Authentication and Idempotency

- `.kingdom/secrets/operator-token` is local secret state, not SQLite data. Ordinary database backups do not contain it; restoring a workspace requires retaining or reprovisioning local secrets.
- Browser sessions, CSRF tokens, and GitHub Manifest/device setup flows are memory-only and disappear on restart. Browser sessions are fixed at eight hours, bounded to 128 live entries, and bound to exact loopback origins. Each setup-flow store retains at most 32 entries, never outlives its browser session, preserves active flows, and evicts expired flows before the oldest terminal status under pressure while removing or zeroing transient provider codes, PEM, client secret, PKCE verifier, and unfinalized user-token material where practical.
- `idempotency_keys` reserves each authenticated actor/method/route/client-key scope as `pending` before handler execution. The request hash also includes the authentication binding, preventing cookie replay across sessions and origins. Ordinary mutations store a compact completed response. Setup mutations store only reservation, request-hash, and completion metadata; up to 128 sensitive responses remain replayable in restart-scoped memory no later than one hour, setup-flow expiry, or browser-session expiry and are then deterministically evicted and cleared. Versioned keys expire after their declared bounded TTL and remain permanently inadmissible; completed rows are pruned at expiry. Pending reservations are never pruned solely by age because their side-effect outcome may be unknown: they continue returning `idempotency_pending` for the same key even after its encoded TTL, avoiding a fresh key that could duplicate a committed mutation. An unresolved reservation can therefore leave one small SQLite row per pending request indefinitely. `idempotency_retention_clock` persists the greatest observed admission time so host clock rollback cannot revive an expired key. Cache eviction, key expiry, and restart never make a durable pending reservation executable again.

## External Source Tables

- `external_installations`: immutable numeric account identity per provider installation; a verified account-login rename updates only its login projection.
- `external_connection_installations`: the installations a connection currently covers, keyed by installation ID so at most one live connection holds each; rows are released on tombstone while the installation identity remains.
- `external_connections`: immutable non-secret provider/App identity, `app_private_key|user_access_token` strategy, App slug, editable display/lifecycle metadata, opaque credential handle, and credential/validated generation health projection. No token value or expiry is stored here.
- `external_sources` and `external_source_aliases`: canonical immutable provider identity plus the mutable current repository locator used for provider fetches. Public historical coordinates are never read from this projection.
- `external_source_snapshots` and `external_source_snapshot_comments`: immutable normalized versions and bounded retained comments.
- `external_source_links`: immutable target and exact used snapshot; `external_source_link_heads` is the latest-observed projection. Decree summaries decode the used snapshot, while planning summaries decode the latest explicit adoption.
- `external_source_observations`: body-free refresh attempt history.
- `planning_external_source_adoptions`: append-only active-version movement for future planning operations.
- `planning_operation_external_snapshots`: exact operation bindings, assembler version, digest, and byte counts.
- `external_integration_audit_events`: hash-chained, append-only, safe lifecycle audit.

Setup state is never a table. Manifest and shared finalization first stage and fsync generation-addressed credential files, then publish the generation through the connection/audit transaction. Private keys are `.kingdom/secrets/github-apps/<connection>/private-key.<generation>.pem`; shared tokens are `.kingdom/secrets/github-user-tokens/<connection>/tokens.<generation>.json`. Credential directories are current-user-owned `0700`; credential files are current-user-owned, one-link, non-symlink regular files at `0600`, accessed descriptor-relatively under a lifecycle lock. `external_connections.credential_generation` is the only active pointer, startup removes stale and unowned generations, and connection removal deletes every generation. Ordinary token refresh atomically replaces only the selected token generation. Token files contain access/refresh tokens and both expiries. Ordinary SQLite backups retain provenance but exclude all local credentials.

## Operator Checks

- Outbox backlog check:
  - `SELECT status, COUNT(1) FROM attempt_outbox GROUP BY status;`
- Long-pending completion handoff:
  - `SELECT outbox_id, attempt_id, run_id, status, publish_attempts, updated_at FROM attempt_outbox WHERE status='pending' ORDER BY updated_at ASC;`
- Dead-lettered completion handoff:
  - `SELECT outbox_id, attempt_id, run_id, status, publish_attempts, last_error, updated_at FROM attempt_outbox WHERE status='failed' ORDER BY updated_at ASC;`
- Lease health snapshot:
  - `SELECT attempt_id, state, lease_owner, lease_expires_at, updated_at FROM attempt_executions WHERE state IN ('starting','running') ORDER BY updated_at ASC;`
- Pending guardian quiescence:
  - `SELECT guard_id, attempt_id, run_id, lifecycle, started_at, acknowledged_at, updated_at FROM provider_process_guards WHERE owner_kind='decree_attempt' AND lifecycle <> 'quiescent' ORDER BY updated_at ASC;`
- Pending cancellation handoff:
  - `SELECT cancel_outbox_id, attempt_id, run_id, reason_code, handoff_state, publish_attempts, last_error, updated_at FROM attempt_cancel_outbox WHERE status='pending' ORDER BY updated_at ASC;`
- Retry decision state:
  - `SELECT retry_decision_id, subject_kind, run_id, node_id, error_code, meaningful_progress, decision, retry_index, max_retries, due_at, dispatch_state, resolution FROM retry_decisions ORDER BY created_at DESC;`
