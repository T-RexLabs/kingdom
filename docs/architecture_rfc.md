# Kingdom Orchestrator Architecture RFC

Status: accepted
Date: 2026-04-30
Owners: core team (you + assistant)

## Objective

Describe the accepted current-state architecture for Kingdom as it is implemented in this repository today.

This RFC is intentionally not aspirational. If the code does not do it, this document should not claim that it does.

## Product Summary

Kingdom is a single-host, role-governed orchestration runtime.

- Operators can plan work in durable planning sessions before issuing a decree.
- Decrees execute through a deterministic workflow DAG with fixed role authority.
- Runtime execution uses host-launched OpenCode sessions against prepared workspaces.
- The ceremonial and governance-transition `decree_intake`, `king_plan_decision`, `execution_kickoff`, and `royal_closure` nodes are runtime-owned and deterministic; they do not invoke OpenCode.
- State transitions, approvals, artifacts, mailbox traffic, and execution events are stored durably for audit and recovery.
- The primary operator surfaces are the embedded web UI, the `kd` CLI, and the authenticated `/api/v1` API.

## Locked Decisions (Current Contract)

- Core runtime is Go.
- Embedded operator UI is a TypeScript/React SPA served by the Go runtime.
- The only supported provider engine is `opencode`.
- OpenCode provider fallback is disabled.
- New run creation requires an explicit global model selection.
- Optional per-role model overrides are allowed, and `/roles` is the source of truth for selector visibility, grouping, and ordering.
- Runtime execution is host-launched and session-based via `opencode run --format json`; Kingdom does not currently execute attempts inside containers.
- Every provider-backed decree execution attempt gets a runtime-owned invocation identity and single-use completion capability before provider dispatch.
- Provider-backed decree execution success requires a valid `kingdom_complete_attempt` receipt for the active invocation.
- Clean provider-backed decree execution exit without a valid completion receipt is terminal failure.
- `opencode export` is diagnostic and repair evidence only for provider-backed decree execution; it is not canonical success authority.
- `kd init` installs Kingdom-managed OpenCode role profiles and managed tools into project `.kingdom/opencode/agents/` and `.kingdom/opencode/tools/`; runtime/API startup validates those managed assets read-only and fails fast on drift, and Kingdom launches OpenCode with that overlay so agent resolution remains deterministic without leaking Kingdom assets into normal user-run `opencode` sessions.
- Runtime defaults to headless OpenCode execution with skip-permissions enabled so provider prompts do not block autonomous runs.
- Kingdom launches OpenCode with session-scoped inline permissions that deny `external_directory` access outside the active workdir for both planner and execution sessions; execution sessions also deny the `question` tool while planner sessions remain question-capable.
- `decree_intake` is a runtime-owned deterministic node that records ceremonial intake and opens advisory consultation without a provider call.
- `king_plan_decision` is a runtime-owned deterministic governance node; it waits at the plan approval gate and completes only after approval or rework is recorded.
- `execution_kickoff` is a runtime-owned deterministic execution-handoff node; it records the post-approval handoff into implementation without invoking OpenCode.
- `royal_closure` is a runtime-owned deterministic ceremonial node; final authority still comes from the autonomous or allfather run-level decision path.
- Workflow definitions are schema-validated and stored as canonical JSON.
- The seeded default workflow is `royal-decree-v1`.
- Kingdom also ships the built-in alternate workflow `royal-decree-skip-review-validation-v1`, which omits execution review and validation and routes builder output directly to royal closure.
- `TransitionService` is the sole write path for run and node state mutations.
- Mutating APIs require idempotency scoped to `(route, actor, client_key)`.
- Live updates use SSE with event ids and `Last-Event-ID` replay/resume semantics.
- SQLite is the authoritative persistence layer for control-plane state, planner state, durable execution tables, mailbox records, and artifacts.
- Artifact metadata is stored in `artifacts`, and artifact bytes are stored in `artifact_contents`.
- Artifact publication into the project root or a git worktree is a projection step, not the system of record.
- Governance mode is `autonomous` only.
- `governance.autonomous_approvals=true` auto-approves plan and final closure.
- `governance.autonomous_approvals=false` pauses for explicit allfather plan and final decisions.
- Allfather plan rework is uncapped and reopens the planning wave from `advisory_synthesis` through `plan_drafting`.
- Final allfather rework is uncapped and reopens the decree from `plan_drafting`; the revised plan must return to allfather plan approval before execution resumes.
- Execution review and validation rework are governed separately by `max_execution_review_cycles` and `max_validation_rework_cycles`.
- `run_until_all_approve=true` disables the practical cap on review and validation loops for that run.
- Delegation policy is fixed:
  - `king -> chancellor + council-member + advisors`
  - `chancellor -> council-member`
  - `council-member -> builder + reviewer + validators`
  - advisors, reviewer, builder, and validators do not spawn
- Advisory and execution escalation targets route first to `chancellor`.
- Workspace execution is runtime-resolved before dispatch from the run's selected publish mode:
  - `publish_mode=direct` seeds execution from the current project root
  - `publish_mode=git_worktree` requires a valid non-bare git working tree, creates a per-run sibling worktree named from the decree title slug plus `run_id` next to the repository's main worktree root, and scopes execution/publication back to the configured project root inside that worktree
- Workspace seeding ignores `.git`, `.kingdom`, symlinks, and other non-regular files when copying the baseline.
- Builder deliverables are delta-based: only files created or changed relative to the seeded workspace baseline are persisted as `deliverable` artifacts.
- Non-builder roles are response-first; runtime materializes markdown response snapshots for them.
- Durable decree execution completion flows through `provider_invocations`, `provider_completion_evidence`, `provider_completion_settlements`, `attempt_event_log`, `attempt_executions`, and `attempt_outbox`.
- Planning is a first-class subsystem under `/planning-sessions` and `/planning-operations`, separate from decree execution runs.
- Provider-backed planning operations (`reply`, `regenerate`, `formalize`, and generated title work) get runtime-owned invocation identity and single-use completion capability before provider dispatch.
- Provider-backed planning success requires one valid `kingdom_complete_attempt` receipt accepted for the active planner invocation; clean provider exit without a valid receipt is terminal planner-operation failure.
- Provider-backed planning settlement is runtime-owned and invocation-scoped; `opencode export` is diagnostic and repair evidence only for planning just as it is for decree execution.
- Planning operations are durable and recoverable, and support reply, regenerate, formalize, generated-title, and assistant-message-variant flows.
- Planning lineage (`planning_session_id`, `planning_draft_id`, `planning_context_snapshot_id`) is attached to a decree only when the operator explicitly issues a decree from a plan.

## Non-Goals

- Multi-host or distributed scheduling.
- Enterprise auth or multi-tenant RBAC.
- Provider-engine abstraction beyond OpenCode for current production use.
- Container isolation guarantees in the current runtime.
- Artifact presence as a gate for decree success.

## Architectural Shape

### Operator Surfaces

- Web UI for overview, planning, decree issuance, run inspection, approvals, agent inspection, and analytics.
- `kd` CLI for workspace bootstrap, runtime lifecycle, decree/run control, artifact export, timeline streaming, and approvals.
- REST API under `/api/v1` for all product capabilities.
- Unauthenticated liveness probe at `/healthz`.

### Control Plane

- API handlers validate requests against OpenAPI, enforce idempotency for mutating routes, and expose the authenticated operator surface.
- `TransitionService` enforces legal run/node transitions and governance gates.
- Scheduler and orchestration loop drive deterministic node dispatch.
- Reconciler recovers stale execution state and planner state at startup and during runtime.

### Planning Surface

- Planning sessions persist independently from decrees and runs.
- Planning state has explicit lifecycle and conversation fields.
- Planning messages are the canonical transcript.
- Reply, regenerate, and formalize are durable planning operations, not request-scoped streaming handlers.
- Planning operation events stream over SSE with replay/resume.
- Assistant responses can have multiple variants, and one variant can be activated as canonical for the session.
- Context snapshots are durable and refreshable through the API.
- Planner canonical state persists through planning messages, variants, drafts, and session fields.
- Provider-backed planner canonicalization is runtime-settled through planner-specific invocation, evidence, and settlement records before the canonical planning tables are updated.

### Workflow and Orchestration Plane

- Workflow progression is driven by stored workflow definitions and legal edge transitions.
- The canonical seeded workflow is `royal-decree-v1`.
- Bundled alternate workflows may omit review and validation node classes entirely; the current built-in alternate is `royal-decree-skip-review-validation-v1`.
- Governance is enforced through fixed role authority, not prompt convention alone.
- Mailbox records handoff communication and observability context but does not mutate run state.

### Execution Plane

- Runtime resolves a prepared execution workspace for each attempt.
- OpenCode sessions run on the host against that workspace for provider-backed decree execution nodes.
- Session identity is stable per run/node lineage and reused where the runtime contract requires it.
- Runtime issues one invocation-scoped completion capability per provider-backed decree execution attempt and validates receipt usage server-side.
- Strict startup validates provider readiness and agent resolution when `provider.strict=true`.
- Runtime continues across non-terminal `step_finish` reasons such as `tool-calls` and `unknown` until terminal success or failure.
- All provider-backed decree execution sessions must call `kingdom_complete_attempt` to request terminal success settlement.
- Canonical provider settlement for a provider-backed decree execution attempt is runtime-owned and invocation-scoped; success requires one valid receipt accepted for the active invocation, and runtime must not merge multiple final assistant answers or competing receipts into one settled output.
- Stream output, `step_finish.reason`, mailbox projections, artifact presence, and provider export reads can inform diagnostics and repair for provider-backed decree execution, but none of them can create canonical success without a valid runtime receipt.

### State and Audit Plane

- SQLite stores workflow, planner, run, agent, mailbox, artifact, and execution-lifecycle state.
- Audit history is append-oriented and tamper-evident.
- Planner operations, attempt events, and final projections are stored durably enough to support replay and recovery.

## Canonical Workflow: `royal-decree-v1`

The default workflow executes in this order:

1. `decree_intake` (`king`)
2. `advisor_consultation_noble` (`noble-delegate`)
3. `advisor_consultation_clergy` (`clergy-delegate`)
4. `advisor_consultation_burgher` (`burgher-delegate`)
5. `advisor_consultation_peasant` (`peasant-delegate`)
6. `advisory_synthesis` (`chancellor`)
7. `plan_drafting` (`council-member`)
8. `king_plan_decision` (`king`)
9. `execution_kickoff` (`council-member`) on approval
10. `delegated_execution_berserker` (`builder-berserker`)
11. `execution_review_shieldmaiden` (`reviewer-shieldmaiden`)
12. `validation_gates_qa_scout` (`intelligence-qa-scout`)
13. `validation_gates_nightraven` (`intelligence-nightraven`)
14. `royal_closure` (`king`)

Important clarifications:

- The canonical role catalog also includes `intelligence-loki-chaos`, but the seeded `royal-decree-v1` workflow does not currently place a Loki node in the DAG.
- Plan rework persists allfather feedback in review records and reopens `advisory_synthesis` through `plan_drafting`; it does not route through a dedicated feedback node.
- Review and validation can drive execution rework.
- Final success requires legal workflow progression plus final authority acceptance.

## Bundled Alternate Workflow: `royal-decree-skip-review-validation-v1`

The built-in alternate workflow preserves the same plan and execution kickoff phases, runs `delegated_execution_berserker`, and then transitions directly to `royal_closure`.

Important clarifications:

- `execution_review` and `validation_gates` are intentionally absent from this workflow.
- review and validation loop caps default to `0` when the selected workflow omits those node classes.
- final success still requires legal workflow progression plus final authority acceptance.

## Invariants (Must Not Break)

- Only legal transitions are accepted.
- `TransitionService` remains the exclusive mutation authority for run and node state.
- Execution cannot begin before plan approval.
- Mailbox records cannot mutate run state.
- Mailbox rows are latest-state projections only; append-only mailbox/timeline events remain the durable audit history.
- Every attempt and artifact has accountable role/agent lineage.
- `provider.engine` accepts only `opencode` for supported runtime use.
- Missing global model selection blocks new OpenCode runs.
- Node `running` means setup is complete and provider dispatch can begin immediately.
- Setup failures terminate from setup states and must not emit fake execution-start telemetry.
- Worker outputs must satisfy the runtime output schema before persistence.
- Only builder output is treated as implementation deliverable content.
- Non-builder roles remain response-first and publish markdown snapshots rather than arbitrary deliverables.
- Builder `deliverable` artifacts represent changed output only relative to the seeded workspace baseline.
- Failed builder attempts may still persist changed files as audit artifacts, but only the latest successful builder attempt per delegated-execution node contributes canonical persisted deliverable lineage for downstream publication.
- Review and validation inspect `hydrated/` as a self-contained prepared-workspace snapshot; unchanged baseline files remain visible there even though durable builder deliverables are persisted delta-only.
- A settled review or validation `REWORK` at the configured cap boundary still queues the next downstream execution wave.
- Once a queued review or validation node becomes `ready` at or beyond the configured cap, runtime auto-succeeds that node from `ready` without provider dispatch; explicit stored cap `0` means the first ready review/validation attempt auto-succeeds immediately.
- Review attempts and settled validation-wave outcomes remain as executed; review and validation rework are keyed by explicit persisted wave lineage across the relevant node class, and loop-cap auto-success applies only to queued ready review/validation nodes that are skipped instead of dispatching another provider-backed loop, never by rewriting the current executed outcome.
- Validation rework accounting is wave-based: a settled validation attempt counts as one rework wave only when at least one validator returns `VERDICT: REWORK` and no validator in that wave fails for a harder reason.
- Review and validation nodes require exactly one valid `VERDICT:` control line as the first non-empty line of the response; duplicate, conflicting, or too-late verdict lines are invalid.
- A valid completion receipt is necessary but not sufficient for provider-backed decree execution success; runtime must validate the active attempt, invocation, capability, and any node-specific structured contract before settling success.
- Provider-backed decree execution success must never be inferred from process exit, `step_finish.reason`, non-empty output text, mailbox projections, artifact presence, or export output.
- Provider-backed planning success must never be inferred from process exit, stream text, non-empty output text, or export output without accepted runtime receipt settlement for the active planner invocation.
- Latest node state must converge to durable `attempt_executions` terminal truth; a durably terminal attempt cannot leave its latest node run live.
- `run_status=succeeded` requires final authority acceptance:
  - automatically when `governance.autonomous_approvals=true`
  - explicit allfather confirmation when `governance.autonomous_approvals=false`
- Artifact presence supports auditability and observability but does not determine terminal success.
- Run and planning SSE streams must preserve replay semantics through stable event ids and `Last-Event-ID`.
- Planning transcript truth comes from persisted planning messages, not transient client state.
- Planning operations must be durable enough to recover after refresh, disconnect, or restart.

## Deferred Decisions and Explicitly Not Implemented

- Container isolation and `ContainerRunner` remain future work, not the current execution contract.
- Alternative provider engines remain unsupported for real runtime use.
- Distributed scheduling remains out of scope.
- DB backends beyond SQLite remain deferred.
- The web UI does not yet expose a dedicated artifact browser; artifact access is currently CLI/API-first.
- The API supports planning context refresh, but the web planner does not yet expose a dedicated refresh control.

## Acceptance and Sign-off

- Accepted by: core team (you + assistant)
- Sign-off date: 2026-04-30
- Condition: tracked docs and implementation should stay aligned with this RFC.
