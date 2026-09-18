# System Architecture
## What This Doc Is For

This is the main architecture document for Kingdom.

It describes the accepted current-state architecture as it exists in this repository today.

It is intentionally not aspirational. If the code does not do it, this document should not claim that it does.

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
- Runtime execution is host-launched and session-based via `opencode run --format json`; Kingdom does not currently execute attempts inside containers. Darwin and Linux guardians own one dedicated managed provider process group per command. This is lifecycle containment for Kingdom-managed processes, not a security boundary against a deliberately detached hostile subprocess; hard isolation remains future container or VM work.
- Every OpenCode command runs behind a one-command guardian. Darwin and Linux use a dedicated managed process group; Windows assigns a suspended process to a kill-on-close Job Object before resuming it. Durable decree and planning launches persist invocation, lease, process identity, and quiescence capability lineage before the provider launch gate opens. Guardian evidence is atomically file-backed under the runtime root and ingested before receipt, settlement, cancellation, or stale recovery can claim quiescence.
- Every provider-backed decree execution attempt gets a runtime-owned invocation identity before provider dispatch; only builders get a single-use completion capability.
- Every new provider-backed decree execution dispatch first acquires a unique, nonempty execution lease owner and positive monotonic lease version while the attempt remains queued. While provider execution is active, the owning worker renews only that exact unexpired lease generation and cancels itself if renewal loses authority; normal worker settlement requires a final renewal of that same lineage, while a timeout that already claimed settlement remains the sole owner of its terminal path. Atomic settlement clears the owner and expiry while retaining the version. Runtime-owned attempts never acquire execution lease lineage.
- Completion mode is runtime-owned by canonical node class, not workflow-authored: runtime-owned nodes are `not_applicable`, builder delegated execution is `receipt_required`, and provider-backed advisor, synthesis, plan, review, and validation nodes are `server_response`. Builder success requires a valid `kingdom_complete_attempt` receipt for the active invocation; response-first nodes settle from server-observed terminal responses.
- Builder receipts are published into a runtime-owned, invocation-hashed spool under `<runtime workspace root>/provider-completion/receipts/`, independent of direct publish roots and Git worktrees. Orchestration ingests that spool through bounded, no-follow filesystem operations; filenames and payload claims never select invocation authority.
- Receipt-required terminal process observations first enter an invocation-hashed, orchestration-written trusted handoff file under `<runtime workspace root>/provider-completion/handoffs/`, then cross a durable lease-fenced SQLite handoff before receipt ingestion. Startup replays receipt dispositions and eligible handoffs before completion projection or stale reconciliation, while receipt evidence without terminal process evidence remains non-authoritative.
- Clean builder execution exit without a valid completion receipt is terminal failure.
- Response-first success requires a normalized `stop` finish, final turn/step identity and positive sequence, non-empty canonical terminal text, no unresolved or nonterminal tool call, no later stream event, clean process exit with stdout and stderr drained, and no timeout, cancellation, provider error, malformed protocol, or conflicting terminal evidence; node-specific output validation still applies.
- Canonical provider settlement atomically records evidence, settlement, terminal attempt state, token usage, and the completion outbox handoff. Guardian quiescence is required before that boundary; outbox replay projects the settled result but cannot create it.
- `opencode export` is diagnostic only for provider-backed decree execution; it cannot authorize or repair completion.
- `kd init` installs Kingdom-managed OpenCode role profiles and managed tools into project `.kingdom/opencode/agents/` and `.kingdom/opencode/tools/`; runtime/API startup validates those managed assets read-only and fails fast on drift, and Kingdom launches OpenCode with that overlay plus a Kingdom-owned OpenCode database under `.kingdom/opencode/` so agent resolution remains deterministic without leaking Kingdom-managed sessions or assets into normal user-run `opencode` sessions.
- Runtime defaults to headless OpenCode execution with skip-permissions enabled so provider prompts do not block autonomous runs.
- Kingdom launches OpenCode with session-scoped inline OpenCode config that disables conversation sharing (`share=disabled`) and mDNS discovery (`server.mdns=false`) for all Kingdom-managed planner and execution sessions, while permissions still deny `external_directory` access outside the active workdir for both session types and additionally deny the `question` tool for execution sessions.
- Provider-backed decree execution node wall-clock budgets are activity-gated: exceeding `node_wall_clock` is terminal only after real execution activity or active completion-settlement observation has been absent for at least `stream_inactivity_timeout`, and active nodes may continue indefinitely.
- `decree_intake` is a runtime-owned deterministic node that records ceremonial intake and opens advisory consultation without a provider call.
- `king_plan_decision` is a runtime-owned deterministic governance node; it waits at the plan approval gate and completes only after approval or rework is recorded.
- `execution_kickoff` is a runtime-owned deterministic execution-handoff node; it records the post-approval handoff into implementation without invoking OpenCode.
- `royal_closure` is a runtime-owned deterministic ceremonial node; final authority still comes from the autonomous or allfather run-level decision path.
- Workflow definitions are schema-validated and stored as canonical JSON.
- The seeded default workflow is `royal-decree-v1`.
- Kingdom also ships the built-in alternate workflow `royal-decree-skip-review-validation-v1`, which omits execution review and validation and routes builder output directly to royal closure.
- `TransitionService` is the sole write path for run and node state mutations.
- Mutating APIs require versioned, cryptographically random idempotency keys scoped to `(route, actor, client_key)` and hash the concrete request target, body, and authentication binding; cookie mutations cannot replay across browser sessions or origins. Bundled clients issue 24-hour keys, the protocol caps TTL at seven days, and a durable high-water clock prevents wall-clock rollback from making expired keys admissible again.
- `kd init` creates a random local operator bearer credential at `.kingdom/secrets/operator-token`; the secrets directory is mode `0700`, the credential is mode `0600`, and the credential value is never stored in config, SQLite, logs, URLs, or browser storage.
- API authority comes only from the verified local operator principal (`local-operator`, fixed `allfather` authority). Caller-supplied actor and role headers are ignored and cannot grant authority.
- A direct loopback browser creates or reuses a bounded, restart-scoped eight-hour session through `POST /api/v1/auth/session` without user ceremony. Bootstrap requires a loopback TCP peer, an exact `localhost` or loopback-literal Host, exact Origin, no forwarding headers, and a browser bootstrap header; loopback never grants ambient authority to ordinary API routes. Sessions are bound to the exact origin, use deterministic origin/base-path-scoped `HttpOnly`, `SameSite=Strict` cookie names whose lifetime never exceeds absolute session expiry, and retain CSRF only in server and browser memory. Same-origin tabs coordinate bootstrap, and a canonical CSRF rejection triggers session recovery without replaying the failed mutation. Cookie-authenticated request contexts, including SSE streams, end at session expiry without shortening bearer calls.
- The local host is the browser trust boundary: any process that can connect through loopback can emulate browser bootstrap. Non-loopback browsers are denied, direct remote bearer authentication requires TLS, and `kd` permits a non-loopback server only over HTTPS with an explicitly supplied credential file. CLI bearer credentials are bound to the configured API origin and are never forwarded across an origin change or HTTPS downgrade.
- Installed `kd summon` binds loopback only. Non-loopback serving is rejected until Kingdom has a direct TLS listener and certificate lifecycle; it never exposes the local bearer surface over plaintext.
- The embedded UI receives the normalized runtime API base path in server-rendered metadata; browser API, bootstrap, and SSE clients never assume `/api/v1` when a custom base path is configured. Root, malformed, static-asset, liveness, and SPA-route-colliding API base paths are rejected before runtime startup.
- HTTP `401` is reserved for Kingdom local-authentication failure. Upstream GitHub credential rejection remains classified as `revoked` but is returned as a dependency failure with reconnect guidance, so it cannot sign the operator out of the local browser session.
- Durable-clock advancement, expiry validation, pruning, and reservation are one serialized transaction before a mutation executes. Identical concurrent requests wait and replay the completed response; conflicting hashes fail, and crash-interrupted pending reservations return retryable `idempotency_pending` without automatic re-execution or replacement-key guidance.
- Idempotency route scope uses the matched route template; the concrete escaped path, canonical query, and request body remain in the request hash.
- Setup mutation responses are replayable only from bounded, restart-scoped memory layered over durable reservation and request-hash metadata. Cache lifetime is capped by one hour, the setup-flow expiry, and the browser-session expiry. SQLite never stores setup codes, states, user codes, URLs, credentials, or response bodies; after cache eviction, expiry, or restart the reserved mutation cannot re-execute and its response is unavailable.
- GitHub.com Issues is the only external-source provider in v1. Production API egress is fixed to `https://api.github.com` with REST version `2026-03-10`, and Manifest, OAuth, and device-flow exchange is fixed to `https://github.com`; there is no PAT, active webhook, background issue polling, write-back, paste-link, GitHub Enterprise, or Kingdom cloud broker path.
- Fresh `kd init` and standalone defaults use Kingdom's official public App (`kingdom-issue-importer`, client ID `Iv23li1tLBTzhr70LA1J`). When `github.shared_app.slug` and `client_id` are both configured, the primary onboarding path uses that installable GitHub App with device flow, expiring user tokens, exactly repository metadata read and Issues read, and no events. Both values blank disable this path; a partial pair is a startup error. Kingdom never receives a shared App secret or private key.
- The primary connection onboarding path creates a private, user-owned GitHub App through GitHub's manifest flow with exactly repository metadata read and Issues read, no events, and no webhook at all: the manifest declares no `hook_attributes`, because GitHub validates a hook URL for public reachability even when the hook is inactive and would otherwise reject every localhost origin. Post-install temporary OAuth with PKCE is explicit. Personal and organization App owners are supported, and the operator can edit the App name on GitHub.
- Manifest setup is restart-scoped, memory-only, bounded, expiring no later than either one hour or the initiating browser session, stage-checked, and bound to that exact session and origin. One unfinished manifest intent owns that browser binding: starting the same personal or exact-organization intent resumes it, while a different intent conflicts until completion, discard, or expiry. Provider and durable completion work is deadline-bound to flow expiry; in-flight records remain pinned until completion or rollback so concurrent pruning cannot orphan a committed connection. Public callback GETs use one-time state authority without requiring the `SameSite=Strict` cookie, consume state once, and redirect with `303` to a stored exact-origin chromeless return page without callback query data. Every GitHub hop is operator-initiated into one dedicated setup tab; the initiating tab never navigates away, owns the flow, and observes stage changes by polling. An unfinished flow is surfaced as resumable state and can be discarded explicitly; it never opens a modal on its own.
- A saved GitHub connection represents one App **credential** and covers one or more installations of that App, recording `credential_strategy=app_private_key|user_access_token`, App identity, and per-installation pinned numeric account identity. The credential is what makes it one row: a device authorization returns a single rotating token pair for the whole App and GitHub invalidates the old refresh token on every rotation, so installations of one authorization cannot be split across connections without the copies invalidating each other. At most one live connection may hold a given installation; tombstoning releases its installations for reuse while the audit stream retains the removal. Manual App slug may be empty only until verified `/app` health populates it. Login rename is allowed; App ID, client ID, installation ID, and account ID are immutable.
- Shared device flows are restart-scoped, bounded, expire no later than the provider or initiating browser session, cryptographically identified, exact browser-session/origin bound, server-paced, resumable, explicitly discardable, and memory-only. One unfinished intent owns a browser binding: starting the same fresh-connect or exact-connection reconnect intent resumes it without another provider authorization, while a different intent conflicts until completion, discard, or expiry. Capacity and binding are reserved before provider authorization, and that `starting_authorization` reservation is safely observable without provider codes or tokens. Poll, discovery, and durable finalization work is owned by opaque operation-specific leases, runs under the server lifecycle rather than request cancellation, is deadline-bound to flow expiry and a bounded operation timeout, and stays pinned against concurrent pruning and discard until completion or rollback. Safe status exposes only the reconnect connection ID, never provider device codes or tokens; provider device codes and successful-but-unfinalized tokens never enter SQLite, logs, API responses, audit payloads, idempotency rows, or browser storage.
- GitHub credentials are generation-addressed protected files: private keys use `.kingdom/secrets/github-apps/<connection>/private-key.<generation>.pem`, and shared tokens use `.kingdom/secrets/github-user-tokens/<connection>/tokens.<generation>.json`. `external_connections.credential_generation` is the sole active publication pointer. A new generation is staged and fsynced before the generation and audit transaction commits; current files are never overwritten before commit, and provider paths load only the exact DB-selected generation. Startup removes stale and unowned generations under each connection's lifecycle lock, and tombstoning removes every generation.
- Shared refresh is secretless, proactive by five minutes, and atomically replaces only the same DB-selected token generation under in-process and cross-process serialization; a refresh using a stale generation can never overwrite a newer generation. Refresh does not advance connection generation and retries one safe GET after a GitHub `401`. The one auth-refresh retry and the normal safe-GET transport retry budget are independent. Reauthorization stages and publishes a new generation.
- Manifest finalization first proves the claimed installation through the authorizing user's paginated `/user/installations`, then independently validates App and installation identity, exact permissions, suspension state, installation token, and repositories with the returned private key. Durable generation staging precedes the transaction that publishes one healthy generation-validated connection row, pinned account identity, and audit append.
- Shared finalization re-fetches eligible `/user/installations`, accepts an exact selected set for a new connection, durably stages its token generation, and publishes the healthy generation-validated connection in the DB transaction. Reconnect derives the complete saved installation set, rejects client subsets or additions, and requires every saved installation to remain visible with its pinned numeric account identity before a replacement token generation is installed; only **Change** edits the saved set. Provider access is always the intersection of the App installation and the authorizing user's access. The shared App owner can centrally revoke or delete the App; GitHub refresh tokens generally require reconnect after six months of disuse.
- External sources deduplicate by immutable provider issue identity within a connection. Normalized snapshots, comments, target links, link attachment changes, refresh observations, planning adoptions, operation bindings, and integration audit events are immutable or append-only.
- External links distinguish immutable **used**, explicitly adopted planning **active**, and refreshable **latest observed** snapshots. Public decree coordinates are decoded from the used snapshot, and public planning coordinates are decoded from the active snapshot; the mutable source locator is internal refresh state only. Refresh never changes transcript messages, generated drafts, filesystem context, or issued decree text. Removing a planning source is an append-only participation change, not a deletion: it stops the source feeding future planner operations while its provenance, history, and already-bound operations stay exactly as they were, and re-adding the same issue reuses its immutable link and adopts freshly captured content. Only planning sources are removable; an issued decree keeps its exact source.
- Provider-backed planning operations bind the exact active planning context snapshot and up to eight external snapshot IDs atomically with enqueue; when more sources are active, the eight most recently adopted are selected deterministically. Execution and recovery only load those immutable bindings. External text is length-framed, explicitly untrusted, budgeted across the complete rendered prompt including framing and separators, absent from transcript rows, and executes under the standard planner session policy. A linked issue is the subject the operator wants to work through, not a hostile payload, so a turn carrying one keeps the workspace-inspection and research tools that understanding it requires. Containment comes from the planner agent profile — which grants no shell, no writes, and no subagents in any session — and from the framing that tells the model never to follow instructions inside external text or use tools because it asks.
- The provider model catalog is served stale-while-revalidate: a populated cache answers immediately and an aged one revalidates in the background, so only a process's first read can block. A failed revalidation is reported as a stale snapshot on the next read. An explicit refresh is synchronous.
- Live updates use SSE with event ids and `Last-Event-ID` replay/resume semantics.
- SQLite is the authoritative persistence layer for control-plane state, planner state, durable execution tables, mailbox records, and artifacts.
- Artifact metadata is stored in `artifacts`, and artifact bytes are stored in `artifact_contents`.
- Artifact write bytes are served only as authenticated downloads with attachment disposition and MIME sniffing disabled; provider-produced active content never executes under Kingdom's control-plane origin. All artifact actions use portable workspace-relative paths, and archive entries reject absolute, traversal, drive-qualified, and backslash forms. Deletion artifacts expose their action and path as metadata but never advertise checksum or download content that does not exist.
- Artifact publication into the project root or a git worktree is a projection step, not the system of record.
- Governance mode is `autonomous` only.
- `governance.autonomous_approvals=true` auto-approves plan and final closure.
- `governance.autonomous_approvals=false` pauses for explicit allfather plan and final decisions.
- Allfather plan rework is uncapped and reopens the planning wave from `advisory_synthesis` through `plan_drafting`.
- Final allfather rework is uncapped and reopens the decree from `plan_drafting`; the revised plan must return to allfather plan approval before execution resumes.
- Node attempt numbers are durable lineage, not a retry budget, and have no generic ceiling. Automatic stale-attempt recovery and review/validation rework remain governed by their configured retry policies and loop limits.
- Automatic provider retry decisions are durable, canonical records that retain the canonical error code, operator-safe reason, meaningful-progress result, retry index, policy/provider delay, absolute due time, source and target lineage, dispatch state, and final resolution. Restart recovery and planning's bounded service-owned due-time wake both use the persisted due time; lease-fenced claims remain the only launch authority.
- Meaningful provider progress disables automatic retry. A failed dispatched retry target resolves its predecessor as `failed` in the same transaction that records its successor or terminal policy decision. Cancellation resolves a pending retry decision as `cancelled`; retry exhaustion resolves it as `escalated` and delivers exactly one durable escalation to the configured target role. Decree delivery uses the run mailbox; planning delivery uses the durable ACP operation event stream because planning sessions are independent of decree runs.
- Provider-controlled prompts, event streams, output, receipt transport, diagnostics, workspace traversal, and startup replay are centrally bounded before they can become unbounded runtime state. A breached workspace boundary is a non-retryable `resource_limit_exceeded` failure, and its run execution target is quarantined rather than reused without a trusted restoration.
- The run attempt budget is reserved at automatic decree successor creation, not inferred from a node attempt ordinal. A prospective successor over the 60-attempt run budget is never inserted; its decision is recorded as the `budget_exceeded` fail-fast disposition and normal run terminalization preserves the source failure evidence.
- Explicit Retry and Rerun actions reopen failed or cancelled runs in the same durable lineage. Cancelled runs may reopen only after provider cancellation, completion projection, attempt settlement, and session leases are quiescent.
- Execution review and validation rework are governed separately by `max_execution_review_cycles` and `max_validation_rework_cycles`.
- `run_until_all_approve=true` disables the practical cap on review and validation loops for that run.
- Delegation policy is fixed:
  - `king -> chancellor + council-member + advisors`
  - `chancellor -> council-member`
  - `council-member -> builder + reviewer + validators`
  - advisors, reviewer, builder, and validators do not spawn
- Advisory and execution escalation targets route first to `chancellor`.
- Workspace execution is runtime-resolved before dispatch from the run's selected publish mode:
  - `publish_mode=direct` captures the configured project root into one runtime-owned run-scoped execution target and publishes changes back to the configured project root as a projection step
  - `publish_mode=git_worktree` requires a valid non-bare git working tree, creates a per-run sibling worktree named from the decree title slug plus `run_id` next to the repository's main worktree root, and uses that scoped worktree project root as the shared execution/publication target for the run
- Runtime-owned filesystem snapshots live under `.kingdom/runtime`, identify regular-file content only by digest, load only canonical bytes that match that digest, and ignore VCS metadata, Kingdom/tool state, package dependencies, sandbox/temp output, symlinks, and other non-regular files. Restoration verifies destination content and uses descriptor-relative no-follow traversal to replace provider-created symlink and non-directory conflicts safely without touching their targets or recursively deleting real directories.
- Builder deliverables are delta-based: only files created, changed, or deleted relative to the trusted runtime-owned snapshot are persisted as canonical builder artifacts.
- Non-builder roles are response-first; runtime materializes markdown response snapshots for them.
- Durable decree execution completion flows through `provider_invocations`, `provider_completion_evidence`, `provider_completion_settlements`, `attempt_event_log`, `attempt_executions`, and `attempt_outbox`.
- Planning is a first-class subsystem under `/planning-sessions` for domain resources and `/api/v1/acp` for planner activity, separate from decree execution runs.
- Provider-backed planning operations (`reply`, `regenerate`, `formalize`, and generated title work) get runtime-owned invocation identity before provider dispatch (the completion capability persists as an invocation-identity anchor only).
- Provider-backed planning success is settled server-side from the provider's adjudicated terminal outcome plus runtime-assembled text from the terminal-success assistant step; earlier nonterminal-step prose remains provisional and cannot become canonical fallback. A planner turn that exits success with no terminal-step output text is terminal planner-operation failure. Unlike decree execution, the planner does not use a `kingdom_complete_attempt` receipt — an interactive single-turn chat ends on the provider's structural stop signal, and its managed planner profile explicitly denies that execution-only tool.
- Provider-backed planning settlement is runtime-owned and invocation-scoped, recorded as a `stream_observation` evidence row and a settlement row written atomically when the operation finalizes; `opencode export` is diagnostic only for planning just as it is for decree execution.
- Successful provider-backed planning settlement atomically records a pending, independently lease-fenced diagnostic-activity reconciliation with `run.completed`. Startup admits one 750 ms-bounded inspection before listener and dispatch readiness; the planning-service lifecycle serially continues eligible pending or expired reconciliations in the background. Reconciliation can append only validated and de-duplicated real provider activity, and atomically finishes by writing `run.activity.reconciled`; export timeout or failure writes the marker without activity and cannot affect canonical settlement.
- Planning operations are durable and recoverable, and support reply, regenerate, formalize, generated-title, and branch-navigation (active-leaf selection) flows.
- Clients identify planned decree lineage with `planning_session_id` and `planning_draft_id`; decree issuance derives and persists `planning_context_snapshot_id` from the draft's unique successful formalize operation.
- ACP is the single canonical protocol for planner execution and activity semantics: planner runs, statuses, event streams, cancellation, message parts, and provider activity all flow through `/api/v1/acp`.
- Kingdom REST remains the single canonical API for planner domain resources: planning sessions, planning messages, drafts, context snapshots, and decree issuance remain under the Kingdom resource model.
- Provider-backed planner ACP runs cover `reply`, `regenerate`, `formalize`, and `generate_title`. These are the only planner operation types; context capture is not an operation.
- Planner activity must not expose a second public lifecycle protocol beside ACP, and the planner must not fabricate tool, reasoning, or progress activity that the provider did not actually emit.

## Non-Goals

- Multi-host or distributed scheduling.
- Enterprise auth or multi-tenant RBAC.
- OS-user identity, physical-presence authentication, and protection from hostile processes already running on the trusted local host.
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
- Planning messages form a persistent tree: each message has a `parent_message_id` (null at the conversation root), and the session's `active_leaf_message_id` selects the visible root-to-leaf path that is the canonical transcript.
- Provider-backed planner activity is exposed as ACP runs and ACP events, not as a parallel planner-specific lifecycle surface.
- Reply, regenerate, formalize, and generate-title work are durable planner operations projected as ACP runs, not request-scoped streaming handlers.
- Planner ACP events stream over SSE with replay/resume.
- Regenerate and user-message edits are non-destructive: they append a new sibling branch under the relevant parent and move the active leaf onto it; the prior branch and its entire downstream are preserved and re-selectable. Nothing is deleted or truncated.
- Branches at any message are operator-navigable; an `activate` sets the active leaf to a chosen branch's most recent leaf.
- A planning session and its first context snapshot are created in one transaction, so a session always has an active context snapshot from birth. Snapshots are durable and refreshable through the API. A context snapshot is a bounded enumeration of project file paths and sizes — never file contents — taken from `git ls-files` when the project root is a work tree, so the repository's own ignore rules decide what counts as project content, and from a directory walk with a skip list otherwise. Every enumerated path counts towards the project shape the planner prompt receives; only a bounded prefix is captured with sizes.
- Planner canonical state persists through the planning message tree, the session active-leaf pointer, drafts, and session fields.
- Provider-backed planner canonicalization is runtime-settled through planner-specific invocation, evidence, and settlement records before the canonical planning tables are updated.
- Linked GitHub issues are a separate external-context domain. They are attached, refreshed, adopted, and removed inside an existing planning session; sessions are never created from an issue. They do not create messages and do not move `planning_context_snapshots`; explicit adoption and removal affect future operations only.

### External Intake Surface

- Connections management stores non-secret App and installation metadata, reports redacted credential health, and supports validate, disable, and tombstone/remove operations. Removal tombstones are retained durably for provenance and installation re-use, but they are not listed on the operator connections surface; a connection whose removal did not finish stays listed until reconciliation completes it.
- Repository and issue discovery starts from the union of the connection's installation-granted repository IDs, paged in a stable installation order so a walk never repeats or skips a grant. One request returns an aggregated Kingdom page rather than one provider page: accumulation crosses installations until it reaches a page size or a provider-request budget, so a connection covering several accounts that each grant a few repositories returns them together and a search filter applies across the whole accumulated page. Each repository is authenticated for through the installation that grants it. Health requires every covered installation to validate, and the account-ownership assertion stays per installation so one installation can never vouch for another's repositories. An actual saved-set change atomically invalidates health, and discovery and intake remain blocked until **Check** validates the complete updated set; submitting the same normalized set does not advance connection state or audit. GitHub issue-list pull-request objects are filtered before presentation.
- The normalized issue core has exactly title, body, state, repository, issue number, canonical URL, created timestamp, and updated timestamp. Null bodies normalize to empty text.
- The comment count is always captured; it arrives on the provider's issue payload and costs no extra request. Comment bodies are the only import choice and default off. Storage retains at most 100 comments, 32 KiB each and 128 KiB aggregate. Planner assembly allocates at most 4 KiB to comments and 6 KiB to total external text within its bounded provider prompt. Decree text prefilled from a snapshot is the issue body plus the retained comment bodies when the import selected them, rendered runtime-side, attributed and blockquoted as external evidence; the operator reviews and may edit that text before issuing, and the issued decree text is what execution reads.
- Planning intake fetches before a short transaction. Intake validates the active target and connection row version while atomically capturing/deduplicating the snapshot and creating or re-activating its unique link, head, adoption, and audit event. It never fabricates transcript content or moves the planning context snapshot. Decree issuance atomically inserts the immutable decree and exact selected snapshot link.
- Issue and comment validators are independent. A conditionally unchanged issue still checks comments for comment-enabled profiles. Comment-page validators may reuse immutable snapshot comments only when their complete page set exactly matches the current retained page window; boundary changes rebuild that window without validators. Transfer is accepted only when immutable issue identity matches and the destination repository is still granted by one of the connection's installations, so an issue may move between connected accounts but never outside the connection's own grant.

### Workflow and Orchestration Plane

- Workflow progression is driven by stored workflow definitions and legal edge transitions.
- The canonical seeded workflow is `royal-decree-v1`.
- Bundled alternate workflows may omit review and validation node classes entirely; the current built-in alternate is `royal-decree-skip-review-validation-v1`.
- Governance is enforced through fixed role authority, not prompt convention alone.
- Mailbox records handoff communication and observability context but does not mutate run state.

### Execution Plane

- Runtime resolves one shared execution target per run and restores trusted runtime-owned snapshot state before mutating attempts.
- OpenCode sessions run on the host against that workspace for provider-backed decree execution nodes.
- Session identity is stable per run/node lineage and reused where the runtime contract requires it.
- Runtime issues one invocation-scoped completion capability only for builder attempts and validates receipt usage server-side.
- Strict startup validates managed OpenCode assets, binary availability, and agent resolution when `provider.strict=true`. Agent validation runs against an isolated database copy so it can never write to Kingdom's own.
- After the listener is up, `kd summon` warms the provider surfaces an operator would otherwise wait on: the OpenCode model catalog, and the planner agent resolved against the real managed OpenCode database so that database is created and migrated before the first planner turn. Warming never gates readiness and never fails startup; a failure leaves the cost to be paid lazily and is reported through the model catalog notice.
- Runtime continues across non-terminal `step_finish` reasons such as `tool-calls` and `unknown` until terminal success or failure.
- Provider-backed decree execution wall-clock budgets gate silence recovery, not active work: real provider activity and active completion-settlement observation suppress terminal timeout until the inactivity window is exceeded.
- Only builder execution sessions may call `kingdom_complete_attempt` to request terminal success settlement.
- Canonical provider settlement is runtime-owned and invocation-scoped: builders require one valid receipt accepted for the active invocation, while response-first nodes require server-observed terminal output. Runtime must not merge competing outputs into one settled response.
- Stream output and `step_finish.reason` are canonical only for response-first settlement; mailbox projections, artifact presence, and provider export reads remain diagnostic and cannot repair settlement.

### State and Audit Plane

- SQLite stores workflow, planner, run, agent, mailbox, artifact, and execution-lifecycle state.
- Audit history is append-oriented and tamper-evident.
- External-integration audit events use a dense sequence and a transactionally fenced durable hash head; integrity verification covers this stream alongside transitions and runtime events.
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
- Node `starting` means runtime setup is complete and provider launch is in progress but not yet observed.
- A lease-fenced canonical provider settlement may project a `starting` node directly to its terminal state when the asynchronous launch observation has not persisted; it must not fabricate an intermediate `running` observation.
- Runtime-owned nodes do not use the provider launch phase; they transition directly from `ready` to `running`.
- Node `running` means provider launch has been observed and execution is actively in flight.
- Provider-backed node wall-clock timeout enforcement must ignore runtime-authored observation polling and may terminate only after the node is both over its wall-clock budget and inactive past the configured inactivity window.
- Node `blocked` is the canonical stale-retry quarantine state for a live attempt that runtime has stopped treating as actively executing.
- A stale cancellation handoff is the sole gate between a blocked attempt and its replacement. Kingdom sends one bounded stop request, then pauses on unproven quiescence; only an authenticated, idempotent **Retry Stop** action for that exact attempt can authorize one further stop request. If process identity absence is durably proven while guardian evidence is missing or corrupt, Kingdom records a non-quiescent evidence-loss quarantine; only that same action can authorize cancellation recovery from the quarantine, and it never changes the guard's quiescence authority. Workspace restoration and terminal projection must still succeed before the handoff releases.
- Operator-visible node surfaces keep a `starting`, `running`, or `blocked` lineage visible until a replacement attempt actually reaches `starting` or `running`; queued retries do not hide the older live lineage.
- Setup failures terminate from setup states and must not emit fake execution-start telemetry.
- A queued execution lease claim does not imply provider launch and must not populate `launch_started_at` or `started_at`; those remain provider-handoff and provider-observation timestamps respectively.
- `attempt_executions.launch_started_at` records the durable start of the provider launch handoff, while `started_at` records only observed `running` execution.
- Worker outputs must satisfy the runtime output schema before persistence.
- Only builder output is treated as implementation deliverable content.
- Non-builder roles remain response-first and publish markdown snapshots rather than arbitrary deliverables.
- Builder `deliverable` artifacts represent changed output only relative to the trusted runtime-owned snapshot baseline, and deletion actions are first-class.
- Failed builder attempts may still persist changed files as audit artifacts, but only the latest successful builder attempt per delegated-execution node contributes canonical persisted deliverable lineage for downstream publication.
- Review and validation inspect the shared execution target directly; runtime no longer materializes a separate `hydrated/` tree.
- A settled review or validation `REWORK` at the configured cap boundary still queues the next downstream execution wave.
- Once a queued review or validation node becomes `ready` at or beyond the configured cap, runtime auto-succeeds that node from `ready` without provider dispatch; explicit stored cap `0` means the first ready review/validation attempt auto-succeeds immediately.
- Review attempts and settled validation-wave outcomes remain as executed; review and validation rework are keyed by explicit persisted wave lineage across the relevant node class, and loop-cap auto-success applies only to queued ready review/validation nodes that are skipped instead of dispatching another provider-backed loop, never by rewriting the current executed outcome.
- Validation rework accounting is wave-based: a settled validation attempt counts as one rework wave only when at least one validator returns `VERDICT: REWORK` and no validator in that wave fails for a harder reason.
- Review and validation nodes require exactly one valid `VERDICT:` control line as the first non-empty line of the response; duplicate, conflicting, or too-late verdict lines are invalid.
- A valid completion receipt is necessary but not sufficient for provider-backed decree execution success; runtime must validate the active attempt, invocation, capability, and any node-specific structured contract before settling success.
- Receipt-required terminal process handoffs are durably file-backed before SQLite ingestion. Replay authority retains the exact active invocation, attempt, stream/session, lease owner/version, and pre-receipt payload lineage, and is valid only when its fixed observation time precedes the durable lease expiry; replay wall-clock expiry alone cannot erase already-durable process evidence.
- Provider-backed decree execution success must never be inferred from process exit, `step_finish.reason`, non-empty output text, mailbox projections, artifact presence, or export output.
- Provider-backed planning success must never be inferred from process exit, provisional stream text, or export output. It requires server-owned settlement for the active planner invocation from the provider's adjudicated terminal success plus non-empty output from the terminal-success assistant step; no earlier nonterminal step may supply fallback output, and no `kingdom_complete_attempt` receipt exists on the planning path.
- Empty terminal provider output is `provider_empty_output`; an unclassified provider failure is `provider_unknown_failure`. Both are canonical fail-fast codes, not text-derived retry signals.
- Latest node state must converge to durable `attempt_executions` terminal truth; a durably terminal attempt cannot leave its latest node run live.
- Requested rework is durably a node failure, because the workflow engine routes the loop back on failure. The `rework` value in `RunGraphNode.state` is an operator-facing read projection derived from the `execution_review_rework` and `validation_gate_rework` failure codes; it is never written to `node_runs.node_status`.
- Superseded attempts remain audit evidence only; they must not publish canonical planning output, mailbox responses, or canonical response artifacts.
- Superseded provider-backed attempts that already have a launch/session handoff must still flow through durable cancel dispatch and session-lease closure before they disappear from live execution.
- If an older attempt settles successfully before a queued retry ever launches, runtime cancels that queued retry as superseded-before-launch and excludes it from canonical latest-attempt lineage while preserving it in audit history.
- Graceful shutdown must wait for durable async completion handoff and drain pending execution outbox projection before returning.
- `run_status=succeeded` requires final authority acceptance:
  - automatically when `governance.autonomous_approvals=true`
  - explicit allfather confirmation when `governance.autonomous_approvals=false`
- Artifact presence supports auditability and observability but does not determine terminal success.
- Run and planning SSE streams must preserve replay semantics through stable event ids and `Last-Event-ID`.
- Successful planning SSE streams remain open after `run.completed` until the durable `run.activity.reconciled` marker; failed and cancelled planning streams close at their terminal event.
- Every planning session has a resolvable active context snapshot; creation is one transaction, so a session that cannot bind a snapshot cannot exist.
- Planning transcript truth comes from the persisted message tree resolved through the session active-leaf pointer, not transient client state; planner operations are append-only and never delete or truncate prior branches.
- Planning operations must be durable enough to recover after refresh, disconnect, or restart.
- ACP run state is the canonical planner activity lifecycle visible to operators; Kingdom planner session fields may point to the active ACP run, but they do not define an alternate public lifecycle protocol.
- Planner tool, reasoning, and progress activity may only be shown when projected from real provider observations.

## Deferred Decisions and Explicitly Not Implemented

- Container isolation and `ContainerRunner` remain future work, not the current execution contract.
- Alternative provider engines remain unsupported for real runtime use.
- Distributed scheduling remains out of scope.
- DB backends beyond SQLite remain deferred.
- The web UI does not yet expose a dedicated artifact browser; artifact access is currently CLI/API-first.
- The API supports planning context refresh, but the web planner does not yet expose a dedicated refresh control.
