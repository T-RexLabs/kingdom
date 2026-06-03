# Incident Staging Drill Runbook

Last updated: 2026-03-01

## Purpose

Validate the on-call response flow against production-like incident paths before release.

## Preconditions

- Latest schema and OpenAPI migrations applied.
- Local verification gates are green (`make verify`).

## Drill Scenarios

1. **SSE reconnect/regression incident**
	- Command: `go test ./tests/e2e -run TestE2EReconnectFlowUsesLastEventID`
	- Pass criteria: stream resumes from `Last-Event-ID` without replaying old events.
2. **Security validation gate incident**
	- Command: `go test ./internal/state -run TestTransitionService`
	- Pass criteria: closure is blocked when validation gates fail and no override path bypasses gate enforcement.
3. **Stale-attempt recovery incident**
	- Command: `go test ./internal/state -run TestReconciler`
	- Pass criteria: stale attempts and leases recover deterministically with audit events, and per-node retries inside review/validation fanout preserve the original wave lineage instead of creating phantom extra waves.

## Escalation and Ownership

- Primary owner: Chancellor on-call.
- Governance escalation: Chancellor -> King/Allfather for explicit rework or final approval decisions.
- Runtime escalation: Chancellor -> Builder/Intelligence owners for execution-path regressions.

## Evidence Recording

- Record command output and pass/fail in `artifacts/release/staging_drill_YYYY-MM-DD.md`.
- Link the artifact from the active release change note or pull request, and keep raw outputs with the verification evidence captured for that release.
