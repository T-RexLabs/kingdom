# SQLite Storage Notes

The MVP control plane uses SQLite with explicit startup pragmas.

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

- `provider_invocations`: runtime-owned invocation identity, completion capability lifecycle, provider session linkage, and supersession state for each provider-backed decree execution attempt dispatch.
- `provider_completion_evidence`: append-only settlement evidence for provider-backed decree execution attempts, including accepted receipts and diagnostic observations.
- `provider_completion_settlements`: canonical invocation-scoped settlement truth for provider-backed decree execution attempts (`success`, `failed`, or `cancelled`) with evidence linkage.
- `attempt_executions`: durable attempt lifecycle and latest execution projection consumed by orchestration and UI (`queued` -> `starting` -> `running` -> `terminal_observed` -> `committed_*`). For provider-backed decree execution attempts, it references the active invocation and canonical runtime settlement; it does not own settlement truth itself.
- `attempt_event_log`: canonical per-attempt execution event stream with monotonic `seq`.
- `attempt_outbox`: durable completion handoff queue consumed by orchestration for terminal projection.
  - `pending`: awaiting replay/projection.
  - `published`: successfully projected.
  - `failed`: dead-lettered after unrecoverable replay payload/invariant failure so poison rows stop blocking later completions.
- `node_runs`: durable per-node retry lineage (`attempt`) plus explicit cross-node review/validation wave lineage (`wave`).
- `workspaces`: execution workspace ownership and lifecycle (`active` and `released`) for each node run.

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

## Operator Checks

- Outbox backlog check:
  - `SELECT status, COUNT(1) FROM attempt_outbox GROUP BY status;`
- Long-pending completion handoff:
  - `SELECT outbox_id, attempt_id, run_id, status, publish_attempts, updated_at FROM attempt_outbox WHERE status='pending' ORDER BY updated_at ASC;`
- Dead-lettered completion handoff:
  - `SELECT outbox_id, attempt_id, run_id, status, publish_attempts, last_error, updated_at FROM attempt_outbox WHERE status='failed' ORDER BY updated_at ASC;`
- Lease health snapshot:
  - `SELECT attempt_id, state, lease_owner, lease_expires_at, updated_at FROM attempt_executions WHERE state IN ('starting','running') ORDER BY updated_at ASC;`
