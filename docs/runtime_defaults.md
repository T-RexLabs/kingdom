# Runtime Defaults and Safe Overrides

## Default Values

- Worker heartbeat interval: `10s`
- Stale heartbeat threshold: `35s`
- Reassign candidate threshold: `60s`
- Queue aging boost: `30s`
- Ready escalation threshold: `5m`
- `max_spawn_depth`: `3`
- `max_active_agents_per_run`: `12`
- `intelligence-nightraven` concurrency cap: `1`
- `max_attempts_per_run`: `60`
- `max_attempts_per_node`: `5`
- `max_tokens_per_run`: `1_500_000`
- `max_wall_time_per_run`: `2h`
- provider engine: `opencode`
- provider strict startup: `true`
- strict probe attempt timeout: `30s`
- strict startup total timeout budget: `90s`
- strict probe startup backoff schedule: `1s`, `5s`, `10s` (bounded by total startup timeout budget)
- provider skip-permissions: `true`
- Kingdom-managed OpenCode sharing: `disabled`
- Kingdom-managed OpenCode mDNS discovery: `false`
- planner OpenCode inline permissions: `external_directory=deny`
- execution OpenCode inline permissions: `external_directory=deny`, `question=deny`
- governance mode: `autonomous` (MVP-supported)
- autonomous approvals: `true`
- default workflow id: `royal-decree-v1`
- bundled alternate workflow id: `royal-decree-skip-review-validation-v1`
- default operator role: `allfather`
- seeded workflow execution review cap: `5`
- seeded workflow validation rework cap: `5`
- bundled skip-review workflow execution review cap: `0`
- bundled skip-review workflow validation rework cap: `0`
- per-run `run_until_all_approve`: `false`
- delegated empty-output retry backoff (delegated execution only): `1s`, `10s`, `20s`

## Enforcement Points

- Session stale threshold defaults: `internal/state/session_manager.go`
- Reassign threshold defaults: `internal/state/reconciler.go`
- Scheduler defaults (`queue aging`, `ready escalation`): `internal/core/workflow/scheduler.go`
- Delegation defaults (`spawn depth`, `active agents`) and nightraven cap: `internal/state/delegation_policy.go`
- Run budget guardrails (`attempts/tokens/wall-time`): `internal/state/budget_guard.go`
- Node attempt cap (`max_attempts_per_node`): `internal/api/server.go`
- Provider engine and strict startup defaults: `cmd/kingdomd/main.go`, `internal/orchestration/runtime.go`
- Provider skip-permissions default and runtime wiring: `internal/cli/install_mode.go`, `internal/providers/opencode/session_adapter.go`
- Planner/execution OpenCode inline config wiring (`share`, `server.mdns`, and permissions): `internal/api/server.go`, `internal/orchestration/runtime.go`, `internal/providers/opencode/session_policy.go`
- Governance mode default/behavior: `internal/cli/install_mode.go`, `cmd/kingdomd/main.go`, `internal/orchestration/orchestrator.go`
- Autonomous approvals default/behavior: `internal/cli/install_mode.go`, `cmd/kingdomd/main.go`, `internal/orchestration/runtime.go`, `internal/orchestration/orchestrator.go`, `internal/api/server.go`
- Workflow default id config: `internal/cli/install_mode.go`, `internal/cli/dispatch.go`, `web/src/features/decrees/decrees-page.tsx`
- Seeded workflow rework defaults: `internal/bootstrapassets/files/workflows/royal-decree-v1.json`, `internal/bootstrapassets/files/workflows/royal-decree-skip-review-validation-v1.json`
- Per-run approval and rework override fields: `internal/api/server_mutations.go`, `internal/orchestration/orchestrator_rework.go`

## Safe Override Guidance

- Heartbeat and stale thresholds should preserve `stale > heartbeat` by at least `3x` to avoid false positives.
- Reassign threshold should stay above stale threshold to avoid premature reassignment.
- Queue aging and ready escalation thresholds should change together to avoid aggressive escalations.
- `max_spawn_depth` and `max_active_agents_per_run` must remain bounded to prevent runaway delegation.
- `intelligence-nightraven` cap should remain `1` unless explicit security-governance decision is logged.
- `max_attempts_per_node` should remain low single digits to prevent retry storms.
- Use `provider.engine=opencode` for runtime operation.
- Keep `provider.strict=true` for production-like startup guarantees; set `false` only to skip strict startup probes and agent-resolution checks while still requiring OpenCode initialization.
- Keep `provider.skip_permissions=true` for autonomous headless runs so tool permissions do not block on interactive prompts.
- Keep planner and execution inline OpenCode config aligned with the runtime contract: all Kingdom-managed sessions keep `share=disabled` and `server.mdns=false`, both session types remain workspace-bound via `external_directory=deny`, and execution-only sessions keep `question=deny` so autonomous runs never block on interactive input.
- In `governance.autonomous_approvals=false`, runtime pauses at `plan_state=awaiting_king_decision` and requires `allfather` `confirm|rework` before execution can continue.
- In `governance.autonomous_approvals=false`, `allfather` plan `rework` reopens the planning wave without a hard cap.
- In `governance.autonomous_approvals=false`, runtime also pauses at `run_status=awaiting_human_confirmation`; `allfather` `confirm` reaches terminal success, while `allfather` `rework` reopens the decree from `plan_drafting` without a hard cap.
- Per-run `autonomous_approvals` override on run creation supersedes runtime default for that run (`inherit` keeps runtime default).
- Execution review and validation loop caps default from the workflow when the run request omits them.
- `run_until_all_approve=true` causes review and validation rework to continue until all gates pass instead of stopping at the configured caps.
- Artifact persistence remains enabled for auditability and operator visibility; artifact checks do not gate run success.

## Retry Lifecycle Notes

- Empty provider output retries apply only to delegated execution nodes.
- Retry timeline reason codes:
  - `provider_empty_output_retry`: runtime is retrying after empty output.
  - `provider_output_recovered`: provider output recovered; retry status should be cleared in UI.
- Recovery is event-driven (`attempt.retry_recovered`) and can occur before node terminal completion.
