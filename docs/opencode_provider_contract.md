# OpenCode Provider Contract Defaults

Date: 2026-05-28

## Goal

Freeze explicit provider-facing defaults for timeout budgets, fallback policy, and retry boundaries before runtime implementation.

This contract now also locks the runtime integration shape for the `opencode` engine path.

## Locked Defaults

### 1) Timeout budgets

- Strict probe attempt timeout: `30s`
- Strict startup total timeout budget: `90s`
- Strict probe startup retry backoff schedule: `1s`, `5s`, `10s` (timeout/cancel transients only, bounded by total startup timeout)
- Foreground export diagnostic fetch timeout: `20s`
- Foreground export diagnostic backoff schedule: `250ms`, `500ms`, `1s`, `2s`, `3s`, `5s`
- Dispatch acknowledge timeout: `5s`
- First event timeout: `60s`
- Stream inactivity timeout: `10m`
- Cancel grace timeout: `5s`
- Cancel force timeout: `10s`

Node wall-clock budgets (default by `royal-decree-v1` node):

- Governance and advisory nodes: `5m` to `10m`
- `plan_drafting`: `15m`
- `delegated_execution`: `20m`
- `validation_gates`: `20m`

Runtime-owned nodes (`decree_intake`, `king_plan_decision`, `execution_kickoff`, and `royal_closure`) do not consume provider wall-clock budgets because they do not invoke OpenCode.

Policy source: `policies/opencode_provider_policy.yaml`

### 2) Model selection contract

- Fallback is disabled (`fallback_enabled: false`).
- Provider policy does not define runtime model defaults or per-role preferred models.
- `provider.engine=opencode` run creation requires an explicit global model selection (`model_selection.global.model`).
- The web UI must require a chosen global model before decree submission, and the CLI must require `--global-model <provider/model>` for OpenCode run creation.
- Any future fallback or implicit-default behavior requires an explicit contract update and deterministic tests.

### 2.1) Runtime provider engine

- `provider.engine=opencode` is the default production engine.
- OpenCode execution transport is session-based via `opencode run --format json`.
- Runtime-owned workflow nodes bypass the provider transport entirely and complete through deterministic runtime logic.
- OpenCode agent selection is role-aware via provider policy `agent_policy` (`default_agent` + `role_agents`).
- Kingdom-managed OpenCode role profiles and managed tools are installed under project `.kingdom/opencode/agents/` and `.kingdom/opencode/tools/`.
- Kingdom launches OpenCode with `OPENCODE_CONFIG_DIR` pointing at the Kingdom-managed overlay for planner and execution sessions so `opencode run --agent <name>` and managed completion tooling resolve deterministically while still inheriting user/global project OpenCode config.
- Kingdom also launches OpenCode with `OPENCODE_CONFIG_CONTENT` session overrides so planner and execution sessions both deny `external_directory` access above the active workdir, and execution sessions additionally deny the `question` tool while planner sessions keep question support.
- Startup validates the managed `.kingdom/opencode/` assets read-only and fails fast on drift or missing files; operators repair that state with `kd init --force`, not by relying on API/runtime startup to rewrite project files.
- Non-builder decree roles map to explicit role-type agent profiles: `king`, `chancellor`, `council`, `advisor`, `reviewer`, and `tester`. The `planner` profile is separate and serves planning sessions, not decree role mapping.
- Advisor, council, and planner profiles have read-only research tooling enabled (`list`, `glob`, `grep`, `webfetch`); council additionally owns planning checklist updates via `todowrite`.
- Planner canonical state persists through planning messages, variants, drafts, and session fields.
- Provider-backed planner work uses planner-specific runtime-owned settlement tables (`planning_provider_invocations`, `planning_provider_completion_evidence`, and `planning_provider_completion_settlements`) before canonical planning tables are updated.
- Tester profile is validation-oriented with command execution enabled (`bash`) plus repository inspection (`list`, `glob`, `grep`).
- OpenCode run creation fails before dispatch when the global model selection is missing.
- Tests should inject adapter doubles directly; there is no supported public `provider.engine=mock_engine` runtime path.

### 2.2) Session identity, invocation, and lease policy

- Runtime logical session id is deterministic per run+node (`session_<run_id>_<node_id>`).
- Every provider-backed decree execution attempt also gets a Kingdom-owned invocation identity before provider dispatch.
- Provider-backed decree execution uses three durable identities:
  - logical session id: Kingdom-owned orchestration and lease identity
  - provider invocation id: Kingdom-owned invocation-scoped settlement identity
  - provider session id: OpenCode-owned `ses_*` identity required by `opencode run --session <id>` and available to diagnostic reads such as `opencode export <id>`
- Session identity is canonical only on execution-scoped read surfaces; roster-style agent surfaces must not be used as the canonical source for logical or provider session inspection.
- Adapter must capture the provider-native OpenCode session id returned by the stream and persist it separately from the logical session id.
- Runtime mints a single-use opaque completion capability per decree-execution provider invocation and validates it server-side.
- Adapter retries and diagnostic reads for the same decree-execution node must reuse the provider session id, not the logical session id.
- Parallel nodes do not share a provider session; this avoids cross-node contention during advisor fanout.
- Session lease lifecycle in state must be exercised for real execution (`create|renew|close`).

### 2.3) Provider settlement and output contract for decree fulfillment

- This settlement contract applies to provider-backed decree execution sessions only.
- Provider-backed planner work follows the same receipt-backed authority model through planner-specific settlement tables, while planner canonical persistence still lands in planning messages, drafts, variants, and session fields.
- Adapter output is persisted into worker artifacts and execution records, but output text is not canonical success authority.
- Adapter token usage is treated as model-token usage (not character counts) from OpenCode `step_finish.part.tokens` fields (`input`, `output`, `total`, `reasoning`, `cache.read`, `cache.write`) and persisted into runtime token metrics.
- Provider caveat: when upstream providers omit specific usage fields, missing subfields may be zero in runtime metrics.
- Runtime exposes one mandatory completion tool to every provider-backed decree execution session: `kingdom_complete_attempt`.
- The tool payload is invocation-scoped and includes:
  - `protocol_version`
  - `attempt_id`
  - `invocation_id`
  - `completion_capability`
  - `final_text`
  - optional node-specific structured fields such as `verdict`
- The tool payload is model-authored only for `final_text` and node-specific structured fields; runtime derives file lineage, artifacts, and settlement facts from runtime state.
- Provider settlement authority for decree fulfillment is runtime-owned. `provider_invocations` and `provider_completion_evidence` store invocation lifecycle and supporting evidence, `provider_completion_settlements` stores the single canonical settlement, and `attempt_executions` projects that canonical truth for orchestration and UI.
- Adapter completion contract is strict and every provider-backed decree execution attempt must resolve to exactly one canonical settlement outcome:
  - `success`
  - `failed`
  - `cancelled`
- `provider_incomplete_turn` is a terminal failure reason code within the `failed` path when an invocation does not produce one valid runtime-owned completion receipt the runtime can accept for success.
- `success` requires exactly one valid runtime-owned completion receipt for the active decree-execution provider invocation.
- A completion receipt is necessary but not sufficient; runtime must validate the active decree-execution attempt, invocation, completion capability, and any node-specific structured contract before settling success.
- Clean provider-backed decree execution exit without a valid completion receipt is terminal failure.
- Adapter completion adjudication remains finish-reason aware for diagnostics, continuation, and failure handling, but not as canonical success authority.
- For diagnostics and continuation control only, stream terminal turn-boundary observations are limited to `step_finish.reason` in `stop`, `length`, `content-filter`, or `other`.
- Stream terminal failure evidence is limited to explicit upstream error events, `step_finish.reason=error`, non-zero exit, signal exit, or watchdog timeout.
- Continuation-only evidence is limited to `tool-calls` and `unknown`.
- Ambiguous evidence includes blank reasons, whitespace reasons, missing reasons, unrecognized reasons, conflicting terminal evidence, or clean EOF without strong terminal stream evidence.
- Blank, whitespace, missing, and unrecognized finish reasons must remain distinct during adjudication; they must not be silently collapsed into `unknown`.
- Adapter continues execution across non-terminal turn boundaries (`tool-calls`/`unknown`) within the same provider session using a bounded continuation loop; terminal success is required for completion.
- Adapter must not infer success from process exit.
- Adapter must not treat clean EOF after ambiguous evidence as success.
- Before launching an explicit continuation turn for a non-terminal boundary, the adapter may inspect persisted provider session state for the current invocation when a provider session id is available; that inspection is diagnostic only and must not create canonical success without a valid completion receipt.
- `opencode export` and other persisted provider-session reads are diagnostic and repair evidence only; they may add observability context for the current invocation, but they must never create canonical success, modify receipt-driven settled output, or become a second settlement authority.
- Invocation ownership for any diagnostic export parsing must still be proven from runtime-owned invocation metadata for the current invocation; runtime must never borrow output from an older turn in the same provider session.
- Malformed trailing export bytes must not invalidate already captured diagnostic evidence for the current invocation, but they must not create canonical success or erase a clean receipt-driven settlement.
- Canonical settled output is invocation-owned and single-turn; runtime must not concatenate multiple final assistant answers or accept multiple competing completion receipts into one settled response body.
- Export-backed reads and repair are allowed only for diagnostics, recovery of non-canonical provider-session transcript snapshots for operator inspection, or post-settlement observability. They must not determine canonical settlement, convert a missing-receipt failure into success, or replace receipt-driven settled output.
- Continuation-turn failures must preserve any already-observed continuation output and token usage in the final failed response and diagnostics.
- Adapter cancellation semantics are deterministic: cancellation emits a single terminal `attempt.cancelled` event (never a competing `attempt.failed` terminal for the same attempt).
- Execution-context deadline exhaustion is treated as terminal failure with `provider_timeout`, not as cancellation.
- Runtime uses non-interactive provider execution by default (`--dangerously-skip-permissions`) to prevent permission prompts from stalling headless decree execution.
- Adapter does not force terminalization after `step_finish`; canonical success is determined only by accepted runtime receipt validation, while provider stream state and runtime watchdog timeouts remain diagnostic, continuation, and failure inputs.
- Runtime enforces role authority on persisted artifacts: `builder-berserker` emits implementation files; all other roles are response-first and runtime materializes canonical markdown snapshots from response text.
- Execution topology is single-builder plus review: `reviewer-shieldmaiden` acts as execution reviewer and emits `VERDICT: PASS|REWORK` for bounded pre-validation loops.
- Runtime prompt contract is metadata-first: execution metadata + decree text + delegator role id + approved plan snapshot + delegator instruction body (verbatim when present; empty allowed).
- Delegator instruction retrieval is scoped to latest delegator `instruction_dispatched` message targeting the delegate agent + node.
- Advisor fanout (`advisor_consultation_*`) does not fall back to latest king response text; when scoped instruction is absent runtime injects an advisor-safe advice-only fallback brief.
- Role drift detection for king/advisor is non-blocking: runtime emits warning reason codes (`king_instruction_drift`, `advisor_scope_drift`) to timeline (`attempt.progress`) and mailbox communication previews without failing the run.
- Validation nodes must include exactly one strict verdict line as the first non-empty line: `VERDICT: PASS` or `VERDICT: REWORK`.
- Execution review nodes inspect `hydrated/`, a self-contained inspection root materialized from the current prepared workspace after the latest successful builder publication, and must include exactly one strict verdict line as the first non-empty line: `VERDICT: PASS` or `VERDICT: REWORK`.
- Duplicate, conflicting, or too-late verdict lines are invalid for both review and validation nodes.
- Structured reviewer/validator sections are recommended for readability and downstream rework quality:
  - reviewer rework responses should prefer `REWORK_ITEMS:`
  - validator rework responses should prefer `BLOCKING_GATES:` and `FIX_ITEMS:`
  - these sections remain advisory and are not strict runtime validation requirements
- Non-builder role outputs are published under `kingdom-artifacts/<decree-title-slug>-<run_id>/...`; only builder `deliverable` artifacts publish into project root.
- Validation nodes materialize `hydrated/` from the current prepared workspace before execution so unchanged baseline assets remain inspectable alongside the latest builder changes.
- Validation verdict handling is strict: a validation wave is keyed by explicit persisted wave lineage across the validation nodes that actually participated in that wave. `VERDICT: REWORK` contributes to one failed validation wave only after the full participating wave settles, and only while the failed validation-wave count remains less than or equal to `max_validation_rework_cycles` for the run unless `run_until_all_approve=true`; a boundary `REWORK` still queues the next downstream wave, but any queued ready validation node at or beyond the configured cap auto-succeeds instead of dispatching another provider-backed validation turn. Explicit stored cap `0` means the first ready validation wave auto-succeeds immediately. Per-node stale retries stay inside the same validation wave and must not create phantom extra waves. Executed wave outcomes are not rewritten. Missing/malformed verdict yields `invalid_validation_verdict` and fails the node.
- Execution review verdict handling is strict: an execution review wave is keyed by explicit persisted wave lineage across the execution-review nodes that actually participated in that wave. `VERDICT: REWORK` contributes to one failed review wave only after the full participating wave settles, and only while failed review-wave count remains less than or equal to `max_execution_review_cycles` for the run unless `run_until_all_approve=true`; a boundary `REWORK` still queues the next downstream wave, but any queued ready review node at or beyond the configured cap auto-succeeds instead of dispatching another provider-backed review turn. Explicit stored cap `0` means the first ready review wave auto-succeeds immediately. Per-node stale retries stay inside the same review wave and must not create phantom extra waves. Executed review outcomes are not rewritten. Missing/malformed verdict yields `invalid_execution_review_verdict`.
- Builder evidence manifest fields are explicit and stable:
  - `DeliverableArtifactCount`: count of unique non-noisy deliverable paths.
  - `TruncatedDeliverableFileList`: bounded preview sample list for readability (not a full inventory).
  - Noise filtering excludes dependency/build/cache/package metadata and virtualenv artifacts from the preview/count set.
- Builder persistence remains delta-based: only changed files relative to the seeded workspace baseline are persisted as builder `deliverable` artifacts.
- Builder evidence manifests are generated from the prepared workspace snapshot exposed under `hydrated/`, while canonical persisted deliverable lineage still comes only from the latest successful builder attempt per delegated-execution node; failed builder attempts may still persist changed files for audit, but those audit artifacts are not treated as canonical downstream implementation evidence.
- Artifact metadata and immutable content persistence are retained for auditability, but artifact presence does not gate run success.
- Plan and final approval authority are controlled by `governance.autonomous_approvals`:
  - `true` (default): runtime auto-approves plan and can transition directly to `succeeded`.
- `false`: runtime pauses at `plan_state=awaiting_king_decision` and `run_status=awaiting_human_confirmation`; uncapped allfather plan `rework` reopens the planning wave, and uncapped final `rework` reopens the decree from `plan_drafting` before execution can continue.

### 3) Retry boundaries

- Retry behavior is delegated to `policies/retry_policy.yaml`.
- Class mapping follows the master plan:
  - transient/provider/session startup: max `3`
  - internal runtime: max `1`
  - schema/policy/idempotency/dependency violations: max `0`
- Hard-fail list includes:
  - `invalid_output_schema`
  - `policy_violation`
  - `idempotency_conflict`
  - `dependency_blocked`
  - `llm_invalid_response`
  - `llm_context_overflow`

### 4) Escalation behavior

- Retry exhaustion emits dead-letter and escalates to `chancellor`.
- Strategic unresolved provider failure can escalate from `chancellor` to `king`.

## Research Outcomes and Rationale

- Determinism-first execution is more important than opportunistic completion for MVP.
- Disabling automatic fallback reduces non-deterministic output and debugging complexity.
- Timeout layers separate startup, dispatch, stream liveness, and cancellation to improve failure diagnosis.
- Hard-fail policy/schema/idempotency errors prevents expensive retry loops on non-retryable causes.

## Required Verification Hooks

- Startup fails when provider policy or retry policy is invalid.
- Timeout tests cover startup timeout, first-event timeout, stream inactivity timeout, and cancellation deadlines.
- Runtime/orchestrator tests assert node wall-clock diagnostics: warning event at 80% elapsed and timeout failure at 100% elapsed.
- Retry tests assert per-code retry counts and dead-letter escalation.
- Contract test proves fallback remains disabled and legacy `preferred_models` policy fields are rejected.
- Runtime tests prove OpenCode session adapter bootstrap behavior in strict/non-strict modes and reject unsupported provider-engine selections.
- Dispatch/runtime test proves run-scoped session lease records are created and closed.
- Installed-mode stream test proves `/api/v1/events/stream?follow=1` works under summon access-log wrapper (no `streaming not supported` regression).
- Runtime tests prove missing contract artifacts do not block terminal success.
- E2E decree test proves autonomous run can materialize and serve a generated homepage artifact.

## Open Follow-up

- Add per-role model override tests once role profiles are implemented.
