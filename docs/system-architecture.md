# System Architecture
## What This Doc Is For

This is the main architecture document for Kingdom.

It describes the accepted current-state architecture as it exists in this repository today.

It is intentionally not aspirational. If the code does not do it, this document should not claim that it does.

## Product Summary

Kingdom is a single-host, role-governed orchestration runtime.

- Operators can plan work in durable planning sessions before issuing a work order.
- Work Orders execute through a deterministic workflow DAG with fixed role authority.
- Runtime execution uses host-launched OpenCode sessions against prepared workspaces.
- The ceremonial and governance-transition `work_intake`, `plan_decision`, `execution_kickoff`, and `completion` nodes are runtime-owned and deterministic; they do not invoke OpenCode.
- State transitions, approvals, artifacts, mailbox traffic, and execution events are stored durably for audit and recovery.
- The primary operator surfaces are the embedded web UI, the `kd` CLI, and the authenticated `/api/v1` API.

## Locked Decisions (Current Contract)

- Core runtime is Go.
- Embedded operator UI is a TypeScript/React SPA served by the Go runtime.
- The only supported provider engine is `opencode`.
- OpenCode provider fallback is disabled.
- New run creation requires an explicit global model selection.
- Optional per-role model overrides are allowed, and `/roles` is the source of truth for selector visibility, grouping, and ordering.
- Runtime execution is host-launched and session-based via `opencode run --format json`; complete first-party work order and planner content is sent to OpenCode over stdin, not as a command-line prompt argument, and Kingdom imposes no fixed aggregate provider-prompt byte maximum. Kingdom does not currently execute attempts inside containers. Darwin and Linux guardians own one dedicated managed provider process group per command. This is lifecycle containment for Kingdom-managed processes, not a security boundary against a deliberately detached hostile subprocess; hard isolation remains future container or VM work.
- Every OpenCode command runs behind a one-command guardian. Darwin and Linux use a dedicated managed process group; Windows assigns a suspended process to a kill-on-close Job Object before resuming it. Durable work order and planning launches persist invocation, lease, process identity, and quiescence capability lineage before the provider launch gate opens. Guardian evidence is atomically file-backed under the runtime root and ingested before settlement, cancellation, or stale recovery can claim quiescence.
- Every provider-backed work order execution attempt gets a runtime-owned invocation identity before provider dispatch.
- Every new provider-backed work order execution dispatch first acquires a unique, nonempty execution lease owner and positive monotonic lease version while the attempt remains queued. While provider execution is active, the owning worker renews only that exact unexpired lease generation and cancels itself if renewal loses authority; normal worker settlement requires a final renewal of that same lineage, while a timeout that already claimed settlement remains the sole owner of its terminal path. Atomic settlement clears the owner and expiry while retaining the version. Runtime-owned attempts never acquire execution lease lineage.
- Completion mode is runtime-owned by canonical node class, not workflow-authored: runtime-owned nodes are `not_applicable`, and every provider-backed work order node—including builder delegated execution—is `server_response`.
- Every provider-backed work order terminal observation first commits a lease-fenced SQLite handoff-pending marker, then enters an invocation-hashed, orchestration-written trusted handoff file under `<runtime workspace root>/provider-completion/handoffs/`, then crosses a durable lease-fenced SQLite handoff before settlement. If pending or file publication fails, a recovery-required marker records the exact active lease generation and is written only after guardian quiescence, provider-reported quiescence, or an unlaunched starting state proves there is no live process to preserve. Startup replays file-only and SQLite handoffs after guardian evidence and before lost-handoff recovery, completion projection, or stale reconciliation. A handoff preserves terminal evidence for replay but does not authorize success by itself.
- Response-first success requires a normalized `stop` finish, final turn/step identity and positive sequence, non-empty canonical terminal text, no unresolved or nonterminal tool call, no later stream event, clean process exit with stdout and stderr drained, and no timeout, cancellation, provider error, malformed protocol, or conflicting terminal evidence; node-specific output validation still applies.
- Canonical provider settlement atomically records evidence, settlement, terminal attempt state, token usage, and the completion outbox handoff. Guardian quiescence is required before that boundary; outbox replay projects the settled result but cannot create it.
- `opencode export` is diagnostic only for provider-backed work order execution; it cannot authorize or repair completion.
- `kd init` installs Kingdom-managed OpenCode role profiles into project `.kingdom/opencode/agents/`; runtime/API startup validates those managed assets read-only and fails fast on drift, and Kingdom launches OpenCode with that overlay plus a Kingdom-owned OpenCode database under `.kingdom/opencode/` so agent resolution remains deterministic without leaking Kingdom-managed sessions or assets into normal user-run `opencode` sessions.
- Runtime defaults to headless OpenCode execution with skip-permissions enabled so provider prompts do not block autonomous runs.
- Kingdom launches OpenCode with session-scoped inline OpenCode config that disables conversation sharing (`share=disabled`) and mDNS discovery (`server.mdns=false`) for all Kingdom-managed planner and execution sessions, while permissions deny `external_directory` access outside the active workdir for both session types and additionally deny the `question` tool for execution sessions. Workspace-backed execution sessions establish an exact-root `.git` project boundary because OpenCode discovers project roots by walking the filesystem before invoking Git; direct targets use an inert marker, nested scopes in managed Git worktrees use a `.git` file pointing to the linked worktree's existing Git metadata, and worktree-root scopes retain their linked-worktree marker. The nested marker preserves parent-worktree Git status and diff behavior. `GIT_CEILING_DIRECTORIES` above that root is defense in depth. OpenCode's built-in file-tool boundary uses lexical path checks and does not confine symlink targets or arbitrary shell commands; this is not OS-level confinement.
- Provider-backed work order execution node wall-clock budgets are activity-gated: exceeding `node_wall_clock` is terminal only after real execution activity or active completion-settlement observation has been absent for at least `stream_inactivity_timeout`, and active nodes may continue indefinitely.
- `work_intake` is a runtime-owned deterministic node that records ceremonial intake and opens advisory consultation without a provider call.
- `plan_decision` is a runtime-owned deterministic governance node; it waits at the plan approval gate and completes only after approval or rework is recorded.
- `execution_kickoff` is a runtime-owned deterministic execution-handoff node; it records the post-approval handoff into implementation without invoking OpenCode.
- `completion` is a runtime-owned deterministic ceremonial node; final authority still comes from the autonomous or operator run-level decision path.
- Workflow definitions are schema-validated and stored as canonical JSON.
- The seeded default workflow is `work-order-v1`.
- Kingdom also ships the built-in alternate workflow `work-order-skip-review-validation-v1`, which omits execution review and validation and routes builder output directly to royal closure.
- `TransitionService` is the sole write path for run and node state mutations.
- Mutating APIs require versioned, cryptographically random idempotency keys scoped to `(route, actor, client_key)` and hash the concrete request target, body, and authentication binding; cookie mutations cannot replay across browser sessions or origins. Bundled clients issue 24-hour keys, the protocol caps TTL at seven days, and a durable high-water clock prevents wall-clock rollback from making expired keys admissible again.
- `kd init` creates a random local operator bearer credential at `.kingdom/secrets/operator-token`; the secrets directory is mode `0700`, the credential is mode `0600`, and the credential value is never stored in config, SQLite, logs, URLs, or browser storage.
- API authority comes only from the verified local operator principal (`local-operator`, fixed `operator` authority). Caller-supplied actor and role headers are ignored and cannot grant authority.
- A direct loopback browser creates or reuses a bounded, restart-scoped eight-hour session through `POST /api/v1/auth/session` without user ceremony. Bootstrap requires a loopback TCP peer, an exact `localhost` or loopback-literal Host, exact Origin, no forwarding headers, and a browser bootstrap header; loopback never grants ambient authority to ordinary API routes. Sessions are bound to the exact origin, use deterministic origin/base-path-scoped `HttpOnly`, `SameSite=Strict` cookie names whose lifetime never exceeds absolute session expiry, and retain CSRF only in server and browser memory. Same-origin tabs coordinate bootstrap, and a canonical CSRF rejection triggers session recovery without replaying the failed mutation. Cookie-authenticated request contexts, including SSE streams, end at session expiry without shortening bearer calls.
- The local host is the browser trust boundary: any process that can connect through loopback can emulate browser bootstrap. Non-loopback browsers are denied, direct remote bearer authentication requires TLS, and `kd` permits a non-loopback server only over HTTPS with an explicitly supplied credential file. CLI bearer credentials are bound to the configured API origin and are never forwarded across an origin change or HTTPS downgrade.
- Installed `kd summon` binds loopback only. Non-loopback serving is rejected until Kingdom has a direct TLS listener and certificate lifecycle; it never exposes the local bearer surface over plaintext.
- The embedded UI receives the normalized runtime API base path in server-rendered metadata; browser API, bootstrap, and SSE clients never assume `/api/v1` when a custom base path is configured. Root, malformed, static-asset, liveness, and SPA-route-colliding API base paths are rejected before runtime startup.
- HTTP `401` is reserved for Kingdom local-authentication failure. Upstream GitHub credential rejection remains classified as `revoked` but is returned as a dependency failure with reconnect guidance, so it cannot sign the operator out of the local browser session.
- Durable-clock advancement, expiry validation, pruning, and reservation are one serialized transaction before a mutation executes. Identical concurrent requests wait and replay the completed response; conflicting hashes fail, and reservations remain pending when a mutation outcome cannot be established (including a process crash or ambiguous commit). Pending reservations are never pruned solely by age because their side-effect outcome is unknown; same-key requests continue returning retryable `idempotency_pending` even after the key's encoded TTL, without automatic re-execution or replacement-key guidance. Completed responses are pruned at expiry.
- Authenticated mutation-outcome reads recover retained, allowlisted planning, work-order, run, and theme results for the original local operator and request key, including after browser-session replacement. They never replay a mutation, return credentials or response headers, or infer completion from matching current state. Completed empty responses remain distinguishable from unknown/pending outcomes; an unknown outcome does not authorize a new request.
- Idempotency route scope uses the matched route template; the concrete escaped path, canonical query, and request body remain in the request hash.
- Setup mutation responses are replayable only from bounded, restart-scoped memory layered over durable reservation and request-hash metadata. Cache lifetime is capped by one hour, the setup-flow expiry, and the browser-session expiry. SQLite never stores setup codes, states, user codes, URLs, credentials, or response bodies; after cache eviction, expiry, or restart the reserved mutation cannot re-execute and its response is unavailable.
- GitHub.com Issues is the only external-source provider in v1. Production API egress is fixed to `https://api.github.com` with REST version `2026-03-10`, and Manifest, OAuth, and device-flow exchange is fixed to `https://github.com`; there is no PAT, active webhook, background issue polling, write-back, paste-link, GitHub Enterprise, or Kingdom cloud broker path.
- Fresh `kd init` and standalone defaults use Kingdom's official public App (`kingdom-issue-importer`, client ID `Iv23li1tLBTzhr70LA1J`). When `github.shared_app.slug` and `client_id` are both configured, the primary onboarding path uses that installable GitHub App with device flow, expiring user tokens, exactly repository metadata read and Issues read, and no events. Both values blank disable this path; a partial pair is a startup error. Kingdom never receives a shared App secret or private key.
- The primary connection onboarding path creates a private, user-owned GitHub App through GitHub's manifest flow with exactly repository metadata read and Issues read, no events, and no webhook at all: the manifest declares no `hook_attributes`, because GitHub validates a hook URL for public reachability even when the hook is inactive and would otherwise reject every localhost origin. Post-install temporary OAuth with PKCE is explicit. Personal and organization App owners are supported, and the operator can edit the App name on GitHub.
- Manifest setup is restart-scoped, memory-only, bounded, expiring no later than either one hour or the initiating browser session, stage-checked, and bound to that exact session and origin. One unfinished manifest intent owns that browser binding: starting the same personal or exact-organization intent resumes it, while a different intent conflicts until completion, discard, or expiry. Provider and durable completion work is deadline-bound to flow expiry; in-flight records remain pinned until completion or rollback so concurrent pruning cannot orphan a committed connection. Public callback GETs use one-time state authority without requiring the `SameSite=Strict` cookie, consume state once, and redirect with `303` to a stored exact-origin chromeless return page without callback query data. Every GitHub hop is operator-initiated into one dedicated setup tab; the initiating tab never navigates away, owns the flow, and observes stage changes by polling. An unfinished flow is surfaced as resumable state and can be discarded explicitly; it never opens a modal on its own.
- A saved GitHub connection represents one App **credential** and covers one or more installations of that App, recording `credential_strategy=app_private_key|user_access_token`, App identity, and per-installation pinned numeric account identity. The credential is what makes it one row: a device authorization returns a single rotating token pair for the whole App and GitHub invalidates the old refresh token on every rotation, so installations of one authorization cannot be split across connections without the copies invalidating each other. At most one live connection may hold a given installation; tombstoning releases its installations for reuse while the audit stream retains the removal. Manual App slug may be empty only until verified `/app` health populates it. Login rename is allowed; App ID, client ID, installation ID, and account ID are immutable.
- Shared device flows are restart-scoped, bounded, expire no later than the provider or initiating browser session, cryptographically identified, exact browser-session/origin bound, server-paced, resumable, explicitly discardable, and memory-only. One unfinished intent owns a browser binding: starting the same fresh-connect or exact-connection reconnect intent resumes it without another provider authorization, while a different intent conflicts until completion, discard, or expiry. Capacity and binding are reserved before provider authorization, and that `starting_authorization` reservation is safely observable without provider codes or tokens. Poll, discovery, and durable finalization work is owned by opaque operation-specific leases, runs under the server lifecycle rather than request cancellation, is deadline-bound to flow expiry and a bounded operation timeout, and stays pinned against concurrent pruning and discard until completion or rollback. Safe status exposes only the reconnect connection ID, never provider device codes or tokens; provider device codes and successful-but-unfinalized tokens never enter SQLite, logs, API responses, audit payloads, idempotency rows, or browser storage.
- GitHub credentials are generation-addressed protected files: private keys use `.kingdom/secrets/github-apps/<connection>/private-key.<generation>.pem`, and shared tokens use `.kingdom/secrets/github-user-tokens/<connection>/tokens.<generation>.json`. `external_connections.credential_generation` is the sole active publication pointer. A new generation is staged and fsynced before the generation and audit transaction commits; current files are never overwritten before commit, and provider paths load only the exact DB-selected generation. Startup removes stale and unowned generations under each connection's lifecycle lock, and tombstoning removes every generation.
- Shared refresh is secretless, proactive by five minutes, and atomically replaces only the same DB-selected token generation under in-process and cross-process serialization; a refresh using a stale generation can never overwrite a newer generation. Refresh does not advance connection generation and retries one safe GET after a GitHub `401`. The one auth-refresh retry and the normal safe-GET transport retry budget are independent. Reauthorization stages and publishes a new generation. Removal selects the strategy-specific credential store from the durable connection row and holds that store's cross-process connection lock through `removal_pending`, credential-generation deletion, and tombstoning. Shared refresh uses the same user-token lock, so no refresh write can outlive a tombstone.
- Manifest finalization first proves the claimed installation through the authorizing user's paginated `/user/installations`, then independently validates App and installation identity, exact permissions, suspension state, installation token, and repositories with the returned private key. Durable generation staging precedes the transaction that publishes one healthy generation-validated connection row, pinned account identity, and audit append.
- Shared finalization re-fetches eligible `/user/installations`, accepts an exact selected set for a new connection, durably stages its token generation, and publishes the healthy generation-validated connection in the DB transaction. Reconnect derives the complete saved installation set, rejects client subsets or additions, and requires every saved installation to remain visible with its pinned numeric account identity before a replacement token generation is installed; only **Change** edits the saved set. Provider access is always the intersection of the App installation and the authorizing user's access. The shared App owner can centrally revoke or delete the App; GitHub refresh tokens generally require reconnect after six months of disuse.
- External sources deduplicate by immutable provider issue identity within a connection. Normalized snapshots, comments, target links, link attachment changes, refresh observations, planning adoptions, operation bindings, and integration audit events are immutable or append-only.
- Manual connection creation, installation-set changes, planning-source attachment, and planning-source detachment recover a reported commit error only from exact generated-ID durable readback and matching audit; confirmed absence is a definitive server failure, while mismatched or unreadable state leaves the idempotency reservation pending and returns `idempotency_pending`. Unique installation and target/source constraints reject duplicate inserts but do not prove that an earlier operation committed.
- External links distinguish immutable **used**, explicitly adopted planning **active**, and refreshable **latest observed** snapshots. Public work order coordinates are decoded from the used snapshot, and public planning coordinates are decoded from the active snapshot; the mutable source locator is internal refresh state only. Refresh never changes transcript messages, generated drafts, filesystem context, or issued work order text. Removing a planning source is an append-only participation change, not a deletion: it stops the source feeding future planner operations while its provenance, history, and already-bound operations stay exactly as they were, and re-adding the same issue reuses its immutable link and adopts freshly captured content. Only planning sources are removable; an issued work order keeps its exact source.
- Provider-backed planning operations bind the exact active planning context snapshot and every active external-source snapshot atomically with enqueue, in deterministic snapshot order. Execution and recovery only load those immutable bindings. `external-context-v3` length-frames the selected snapshots as explicitly untrusted evidence; planner assembly has no separate external-evidence byte budget, and the assembled evidence is absent from transcript rows. Queued operations bound with an older assembler version fail before provider invocation and cancel any pending retry; stale running operations first pass normal provider-quiescence recovery. Neither path rebinds historical inputs or uses legacy assembly, so the operator submits a new operation. A linked issue is the subject the operator wants to work through, not a hostile payload, so a turn carrying one keeps the workspace-inspection and research tools that understanding it requires. Containment comes from the planner agent profile — which grants no shell, no writes, and no subagents in any session — and from the framing that tells the model never to follow instructions inside external text or use tools because it asks.
- The provider model catalog is served stale-while-revalidate: a populated cache answers immediately and an aged one revalidates in the background, so only a process's first read can block. A failed revalidation is reported as a stale snapshot on the next read. An explicit refresh is synchronous.
- Live updates use SSE with event ids and `Last-Event-ID` replay/resume semantics.
- SQLite is the authoritative persistence layer for control-plane state, planner state, durable execution tables, mailbox records, and artifacts.
- Artifact metadata and stable insertion identity are stored in `artifacts`, content manifests in `artifact_contents`, and canonical bytes in ordered `artifact_content_chunks`. Downloads, archive export, and publication read those chunks.
- Artifact write bytes are served only as authenticated downloads with attachment disposition and MIME sniffing disabled; provider-produced active content never executes under Kingdom's control-plane origin. All artifact actions use portable workspace-relative paths, and archive entries reject absolute, traversal, drive-qualified, and backslash forms. Deletion artifacts expose their action and path as metadata but never advertise checksum or download content that does not exist.
- Artifact publication into the project root or a git worktree is a projection step, not the system of record.
- Governance mode is `autonomous` only.
- `governance.autonomous_approvals=true` auto-approves plan and final closure.
- `governance.autonomous_approvals=false` pauses for explicit operator plan and final decisions.
- Operator plan rework is uncapped and reopens the planning wave from `advisory_synthesis` through `plan_drafting`.
- Final operator rework is uncapped and reopens the work order from `plan_drafting`; the revised plan must return to operator plan approval before execution resumes.
- Node attempt numbers are durable lineage, not a retry budget, and have no generic ceiling. Automatic stale-attempt recovery and review/validation rework remain governed by their configured retry policies and loop limits.
- Automatic provider retry decisions are durable, canonical records that retain the canonical error code, operator-safe reason, meaningful-progress result, retry index, policy/provider delay, absolute due time, source and target lineage, dispatch state, and final resolution. Restart recovery and planning's bounded service-owned due-time wake both use the persisted due time; lease-fenced claims remain the only launch authority.
- Meaningful provider progress disables automatic retry. A failed dispatched retry target resolves its predecessor as `failed` in the same transaction that records its successor or terminal policy decision. Cancellation resolves a pending retry decision as `cancelled`; retry exhaustion resolves it as `escalated` and delivers exactly one durable escalation to the configured target role. Work Order delivery uses the run mailbox; planning delivery uses the durable ACP operation event stream because planning sessions are independent of work order runs.
- Provider content and workspace processing use disk-backed staging, chunked content, and incremental traversal rather than fixed output-byte, file-size, aggregate-size, entry-count, or directory-depth ceilings. Complete invocation stdout/stderr diagnostics are retained separately from bounded display previews. Guardian metadata, compact terminal-handoff envelopes, snapshot lineage depth, and startup/recovery batches retain their control-plane bounds.
- Automatic work order successor creation has no run-wide attempt-count budget. Per-error retry policies, meaningful-progress rules, and review/validation loop controls still govern whether a successor is eligible.
- Explicit Retry and Rerun actions reopen failed or cancelled runs in the same durable lineage. Cancelled runs may reopen only after provider cancellation, completion projection, attempt settlement, and session leases are quiescent.
- Execution review and validation rework are governed separately by `max_execution_review_cycles` and `max_validation_rework_cycles`.
- `run_until_all_approve=true` disables the practical cap on review and validation loops for that run.
- Delegation policy is fixed:
  - `governance -> coordinator + plan-author + advisors`
  - `coordinator -> plan-author`
  - `plan-author -> builder + reviewer + validators`
  - advisors, reviewer, builder, and validators do not spawn
- Advisory and execution escalation targets route first to `coordinator`.
- Workspace execution is runtime-resolved before dispatch from the run's selected publish mode:
  - `publish_mode=direct` captures the configured project root into one runtime-owned run-scoped execution target at the first workspace-owning attempt and publishes changes back to the configured project root as a projection step; response-only provider nodes use attempt-scoped isolated copies without registering execution-target or snapshot rows
  - `publish_mode=git_worktree` requires a valid non-bare git working tree, creates a per-run sibling worktree named from the work order title slug plus `run_id` next to the repository's main worktree root, and uses that scoped worktree project root as the shared execution/publication target for the run
- Runtime-owned filesystem snapshots live under `.kingdom/runtime`, identify regular-file content only by digest, load only canonical bytes that match that digest, and ignore VCS metadata, Kingdom/tool state, package dependencies, sandbox/temp output, symlinks, and other non-regular files. Restoration verifies destination content and uses descriptor/root-relative no-follow traversal to replace provider-created symlink and non-directory conflicts safely without touching their targets or recursively deleting real directories. Windows captures identities for symlink reparse points and claims, verifies, and removes only the link. Quarantined execution targets and managed Git worktrees are deleted through bounded, resumable batches with durable cursors; targets remain unknown until full removal and trusted snapshot restoration, and worktree records remain uncleaned until provider content is removed and Git registration is pruned. Terminal worktree cleanup records a durable deletion-start boundary and persists its owner process identity with the owner/fence for every batch; startup reclaims a claim only after that exact process generation is absent. Retry/Rerun cancels only before deletion starts and conflicts after start until cleanup completes, after which dispatch recreates a clean checkout. Explicit quarantined-worktree recovery remains a pre-dispatch restoration path.
- Builder deliverables are delta-based: only files created, changed, or deleted relative to the trusted runtime-owned snapshot are persisted as canonical builder artifacts.
- Every provider-backed work order node is response-first; builder deliverables still come from workspace snapshot deltas, while non-builder responses are materialized as markdown snapshots.
- Durable work order execution completion flows through `provider_invocations`, `provider_completion_evidence`, `provider_completion_settlements`, `attempt_event_log`, `attempt_executions`, and `attempt_outbox`.
- Planning is a first-class subsystem under `/planning-sessions` for domain resources and `/api/v1/acp` for planner activity, separate from work order execution runs.
- Provider-backed planning operations (`reply`, `regenerate`, `formalize`, and generated title work) get runtime-owned invocation identity before provider dispatch.
- Provider-backed planning success is settled server-side from the provider's adjudicated terminal outcome plus runtime-assembled text from the terminal-success assistant step; earlier nonterminal-step prose remains provisional and cannot become canonical fallback. A planner turn that exits success with no terminal-step output text is terminal planner-operation failure. Planning keeps its separate operation lifecycle and does not share work order terminal handoff storage.
- Provider-backed planning settlement is runtime-owned and invocation-scoped, recorded as a `stream_observation` evidence row and a settlement row written atomically when the operation finalizes; `opencode export` is diagnostic only for planning just as it is for work order execution.
- Planner `message.completed` events commit atomically with the canonical message, draft, or title, provider settlement, and `run.completed`; streamed `message.part` events are provisional and cannot stand in for that finalization.
- Planning stale recovery derives retry meaningful-progress from durable operation events as well as provider metadata. Progress prevents automatic provider re-execution but never authorizes canonical success without the provider settlement transaction.
- Successful provider-backed planning settlement atomically records a pending, independently lease-fenced diagnostic-activity reconciliation with `run.completed`. Startup admits one 750 ms-bounded inspection before listener and dispatch readiness; the planning-service lifecycle serially continues eligible pending or expired reconciliations in the background. Reconciliation can append only validated and de-duplicated real provider activity, and atomically finishes by writing `run.activity.reconciled`; export timeout or failure writes the marker without activity and cannot affect canonical settlement.
- Planning operations are durable and recoverable, and support reply, regenerate, formalize, generated-title, and branch-navigation (active-leaf selection) flows.
- Clients identify planned work order lineage with `planning_session_id` and `planning_draft_id`; work order issuance derives and persists `planning_context_snapshot_id` from the draft's unique successful formalize operation.
- ACP is the single canonical protocol for planner execution and activity semantics: planner runs, statuses, event streams, cancellation, message parts, and provider activity all flow through `/api/v1/acp`.
- Kingdom REST remains the single canonical API for planner domain resources: planning sessions, planning messages, drafts, context snapshots, and work order issuance remain under the Kingdom resource model.
- Planning-session, run-mailbox, and plan-review lists expose bounded pages with signed continuation cursors, not retention ceilings. Session and review pages retain timestamp/ID order; mailbox pages retain sequence order and bind every filter into the cursor scope. Context snapshot summaries omit file arrays; immutable context entries are read in ordinal order through a session-, snapshot-, and kind-scoped paginated endpoint. Loaded item counts are not presented as complete totals while continuation remains available.
- Provider-backed planner ACP runs cover `reply`, `regenerate`, `formalize`, and `generate_title`. These are the only planner operation types; context capture is not an operation.
- Planner activity must not expose a second public lifecycle protocol beside ACP, and the planner must not fabricate tool, reasoning, or progress activity that the provider did not actually emit.
- Provider activity observations retain invocation, provider part/tool-call identity, immutable event identity, and monotonic sequence. Exact input, output, provider-exposed reasoning, and provider message bytes commit atomically with bounded descriptors; available empty content is distinct from content not supplied or unavailable. Scoped content pages preserve UTF-8 boundaries and verify immutable chunk digests. Activity never supplies terminal settlement authority. Provider-emitted message parts share the same invocation/part identity, immutable content pages, and replay sequence as tools and reasoning. They are conversational observations; the single canonical settled response remains in the selected attempt’s Output. Planner messages remain authoritative from the persisted message tree, with matching provider message observations available only through explicit activity inspection. Routine progress stays available through View Options while the default run conversation emphasizes messages, reasoning, and tools.
- Run-attempt and planner activity initially return the latest bounded window in ascending sequence order. Signed history cursors traverse earlier pages inside a pinned snapshot; independent resume cursors read forward deltas after its high-water mark. Covered sequence ranges and remaining event counts expose unloaded gaps while history and live deltas merge. SSE replay remains bounded per read, and later revisions cannot be replaced by an older replay.
- Provider occurrence time and server recording time remain separate. Meaningful run activity advances from the observed occurrence time when supplied, otherwise recording time; later ingestion of older diagnostic activity does not make the run appear freshly active.

## Non-Goals

- Multi-host or distributed scheduling.
- Enterprise auth or multi-tenant RBAC.
- OS-user identity, physical-presence authentication, and protection from hostile processes already running on the trusted local host.
- Provider-engine abstraction beyond OpenCode for current production use.
- Container isolation guarantees in the current runtime.
- Artifact presence as a gate for work order success.

## Architectural Shape

### Operator Surfaces

- Web UI for overview, planning, work order issuance, run inspection, approvals, agent inspection, and analytics.
- `kd` CLI for workspace bootstrap, runtime lifecycle, work order/run control, artifact export, timeline streaming, and approvals.
- REST API under `/api/v1` for all product capabilities.
- Unauthenticated liveness probe at `/healthz`.

### Presentation and Theme Packages

- Presentation is a shared React layer backed by the data-only versioned schema in `contracts/theme/theme.schema.json` and the canonical Go resolver in `internal/theme`. OpenAPI and browser types are generated from that contract; the browser does not duplicate palette derivation.
- A validated package supplies semantic color/status/DAG tokens, typography, surfaces, fixed plain-text UI copy, icons, and role names/descriptions/lore. Canonical role IDs and responsibilities, status meanings, routes, permissions, model selection, and execution behavior remain application-owned. Each role icon is resolved from the single `role.<canonical-role-id>` slot.
- Generated replies remain neutral. Theme packages accept no prompt fragments, provider settings, executable code, or generated-voice configuration. A future voice feature requires its own versioned execution contract and explicit run/session snapshot; presentation selection cannot mutate live or historical execution inputs.
- Shared editable files live under `.kingdom/themes/<id>/`. Validated publication stores immutable authored and resolved snapshots, with separate source and resolved digests. ID/version ownership survives unpublishing. Invalid source edits leave known-good revisions usable; save and delete use source digest preconditions and publication never silently changes personal selection.
- Personal selection pins a package ID and resolved digest in browser storage scoped by the stable workspace presentation ID. Startup validates the selected revision behind a loading boundary; missing or invalid revisions produce a visible fallback without deleting the preference. Private preview is reversible and does not publish or persist a new selection.
- Assets are validated package-relative files served by immutable revision and asset identity. No remote imports, arbitrary CSS, HTML, scripts, path traversal, or symlinks are allowed. ZIP expansion, asset count/size, image dimensions, static SVG complexity, and font container budgets are bounded; browser decoding remains a second boundary. Built-in fonts are self-hosted. Reduced-motion presentation retains visible state without animation.

### Control Plane

- API handlers validate requests against OpenAPI, enforce idempotency for mutating routes, and expose the authenticated operator surface.
- `TransitionService` enforces legal run/node transitions and governance gates.
- Scheduler and orchestration loop drive deterministic node dispatch.
- Reconciler recovers stale execution state and planner state at startup and during runtime.

### Planning Surface

- Planning sessions persist independently from work orders and runs.
- Planning state has explicit lifecycle and conversation fields.
- Planning messages form a persistent tree: each message has a `parent_message_id` (null at the conversation root), and the session's `active_leaf_message_id` selects the visible root-to-leaf path that is the canonical transcript.
- Provider-backed planner activity is exposed as ACP runs and ACP events, not as a parallel planner-specific lifecycle surface.
- Reply, regenerate, formalize, and generate-title work are durable planner operations projected as ACP runs, not request-scoped streaming handlers.
- Planner ACP events stream over SSE with replay/resume.
- Regenerate and user-message edits are non-destructive: they append a new sibling branch under the relevant parent and move the active leaf onto it; the prior branch and its entire downstream are preserved and re-selectable. Nothing is deleted or truncated.
- Branches at any message are operator-navigable; an `activate` sets the active leaf to a chosen branch's most recent leaf.
- A planning session and its first context snapshot are created in one transaction, so a session always has an active context snapshot from birth. Snapshots are durable and refreshable through the API. Context capture enumerates project file paths and sizes — never file contents — using `git ls-files` when the project root is a work tree, so the repository's own ignore rules decide what counts as project content, and a directory walk with a skip list otherwise. Included and excluded metadata is spooled to disk and persisted as immutable `planning_context_entries`, without prefix truncation. Only the project-shape summary enters the planner prompt; entry details are paginated reads.
- Planner canonical state persists through the planning message tree, the session active-leaf pointer, drafts, and session fields.
- Provider-backed planner canonicalization is runtime-settled through planner-specific invocation, evidence, and settlement records before the canonical planning tables are updated.
- Linked GitHub issues are a separate external-context domain. They are attached, refreshed, adopted, and removed inside an existing planning session; sessions are never created from an issue. They do not create messages and do not move `planning_context_snapshots`; explicit adoption and removal affect future operations only.

### External Intake Surface

- Connections management stores non-secret App and installation metadata, reports redacted credential health, and supports validate, disable, and tombstone/remove operations. Removal tombstones are retained durably for provenance and installation re-use, but they are not listed on the operator connections surface; a connection whose removal did not finish stays listed until reconciliation completes it.
- Repository and issue discovery starts from the union of the connection's installation-granted repository IDs, paged in a stable installation order so a walk never repeats or skips a grant. One request returns an aggregated Kingdom page rather than one provider page: accumulation crosses installations until it reaches a page size or a provider-request budget, so a connection covering several accounts that each grant a few repositories returns them together and a search filter applies across the whole accumulated page. Each repository is authenticated for through the installation that grants it. Health requires every covered installation to validate, and the account-ownership assertion stays per installation so one installation can never vouch for another's repositories. An actual saved-set change atomically invalidates health, and discovery and intake remain blocked until **Check** validates the complete updated set; submitting the same normalized set does not advance connection state or audit. GitHub issue-list pull-request objects are filtered before presentation.
- The normalized issue core has exactly title, body, state, repository, issue number, canonical URL, created timestamp, and updated timestamp. Issue title/body capture has no Kingdom byte ceiling; text and identity validation still apply, and null bodies normalize to empty text.
- The comment count is always captured; it arrives on the provider's issue payload and costs no extra request. Comment bodies are the only import choice and default off. When enabled, intake walks every comment page and retains complete bodies without a count, per-comment, or aggregate-byte cap. Pages are spooled and replayed into immutable comment rows, and public comment reads remain paginated. Work Order text prefilled from a snapshot is the issue body plus all captured comment bodies when selected, rendered runtime-side, attributed and blockquoted as external evidence; the operator reviews and may edit that text before issuing, and the issued work order text is what execution reads.
- Planning intake fetches before a short transaction. Intake validates the active target and connection row version while atomically capturing/deduplicating the snapshot and creating or re-activating its unique link, head, adoption, and audit event. It never fabricates transcript content or moves the planning context snapshot. Work Order issuance atomically inserts the immutable work order and exact selected snapshot link.
- Issue and comment validators are independent. A conditionally unchanged issue still checks comments for comment-enabled profiles. Comment-page validators may reuse immutable snapshot comments only when their complete page set matches the current full comment-page window; boundary changes rebuild the capture without validators. Transfer is accepted only when immutable issue identity matches and the destination repository is still granted by one of the connection's installations, so an issue may move between connected accounts but never outside the connection's own grant.
- Content intake does not relax GitHub setup/discovery controls: installation selections allow at most 500 IDs, installation discovery remains limited to ten 100-item pages, and issue search retains its 1,000-result/ten-page boundary. Provider deadlines and safe-GET retry policies remain in force.

### Workflow and Orchestration Plane

- Workflow progression is driven by stored workflow definitions and legal edge transitions.
- The canonical seeded workflow is `work-order-v1`.
- Bundled alternate workflows may omit review and validation node classes entirely; the current built-in alternate is `work-order-skip-review-validation-v1`.
- Governance is enforced through fixed role authority, not prompt convention alone.
- Mailbox records handoff communication and observability context but does not mutate run state.

### Execution Plane

- Runtime resolves one shared execution target per run and restores trusted runtime-owned snapshot state before mutating attempts.
- OpenCode sessions run on the host against that workspace for provider-backed work order execution nodes.
- Session identity is stable per run/node lineage and reused where the runtime contract requires it.
- Runtime binds every provider response to its invocation, attempt, lease, process guardian, and node contract before settlement.
- Strict startup validates managed OpenCode assets, binary availability, and agent resolution when `provider.strict=true`. Agent validation runs against an isolated database copy so it can never write to Kingdom's own.
- After the listener is up, `kd summon` warms the provider surfaces an operator would otherwise wait on: the OpenCode model catalog, and the planner agent resolved against the real managed OpenCode database so that database is created and migrated before the first planner turn. Warming never gates readiness and never fails startup; a failure leaves the cost to be paid lazily and is reported through the model catalog notice.
- Runtime continues across non-terminal `step_finish` reasons such as `tool-calls` and `unknown` until terminal success or failure.
- Provider-backed work order execution wall-clock budgets gate silence recovery, not active work: real provider activity and active completion-settlement observation suppress terminal timeout until the inactivity window is exceeded.
- Canonical provider settlement is runtime-owned and invocation-scoped: every provider-backed work order node requires the strict server-observed terminal response and then applies its node-specific output contract. Runtime must not merge competing outputs into one settled response.
- Stream output and `step_finish.reason` are authoritative only through the strict server-response predicate; mailbox projections, artifact presence, and provider export reads remain diagnostic and cannot repair settlement.

### State and Audit Plane

- SQLite stores workflow, planner, run, agent, mailbox, artifact, and execution-lifecycle state.
- Audit history is append-oriented and tamper-evident.
- External-integration audit events use a dense sequence and a transactionally fenced durable hash head; integrity verification covers this stream alongside transitions and runtime events.
- Planner operations, attempt events, and final projections are stored durably enough to support replay and recovery.

### Content Storage and Transport

- SQLite is canonical for provider terminal text and invocation diagnostics, work order/planning text, persisted execution prompts, artifact content, and filesystem snapshot manifests. Content manifests identify ordered chunks by size and digest; transient spool paths are transport, never durable authority.
- Work Order terminal handoffs use a compact `v2` envelope with invocation-scoped SHA-256/size references. Referenced content is published under the runtime-owned `provider-completion/content/` tree before the envelope, verified during lease-fenced SQLite ingestion, and retained in canonical chunk tables. File cleanup follows ingestion; envelope presence or a content hash alone cannot authorize settlement. Planning retains its separate invocation and settlement lifecycle.
- Provider streams and helper-command diagnostics are captured in full on disk. Activity text, tool input, stderr tails, and settlement previews are display projections, not canonical content limits or success evidence. `opencode export` remains time-bounded and diagnostic only.
- Request bodies are privately spooled for schema validation, exact-byte idempotency hashing, and handler replay without a route-level aggregate byte ceiling. Designated work order/planning text fields stream into canonical content storage. Existing authentication, schema constraints, HTTP read deadlines, and guardian metadata boundaries remain enforced.
- Streaming and pagination avoid whole-workload accumulation on the refactored paths, but do not guarantee bounded memory for every projection: individual scalar values, metadata collections, and some read responses still materialize data. Host resources and provider context windows remain practical constraints.

## Canonical Workflow: `work-order-v1`

The default workflow executes in this order:

1. `work_intake` (`governance`)
2. `architecture_consultation` (`architecture-advisor`)
3. `risk_consultation` (`risk-advisor`)
4. `systems_consultation` (`systems-advisor`)
5. `usability_consultation` (`usability-advisor`)
6. `advisory_synthesis` (`coordinator`)
7. `plan_drafting` (`plan-author`)
8. `plan_decision` (`governance`)
9. `execution_kickoff` (`plan-author`) on approval
10. `implementation` (`builder`)
11. `execution_review` (`reviewer`)
12. `quality_validation` (`qa-validator`)
13. `security_validation` (`security-validator`)
14. `completion` (`governance`)

Important clarifications:

- The canonical role catalog also includes `resilience-validator`, but the seeded `work-order-v1` workflow does not currently place a Loki node in the DAG.
- Plan rework persists operator feedback in review records and reopens `advisory_synthesis` through `plan_drafting`; it does not route through a dedicated feedback node.
- Review and validation can drive execution rework.
- Final success requires legal workflow progression plus final authority acceptance.

## Bundled Alternate Workflow: `work-order-skip-review-validation-v1`

The built-in alternate workflow preserves the same plan and execution kickoff phases, runs `implementation`, and then transitions directly to `completion`.

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
- Run summaries project `started_at` from the first observed provider-backed attempt, including advisory and superseded attempts, so retry/rework cannot reset it. `start_state` distinguishes `observed`, `not_started`, and `unrecorded`: the last requires canonical stream settlement plus process evidence without a persisted running observation. Creation, lease claim, launch handoff, terminal status, and reused provider-session identity cannot substitute for a start observation. `last_activity_at` remains the separate meaningful-activity timestamp.
- Run artifact listings expose node/attempt/producer lineage in bounded signed-cursor pages. Only deliverables from the latest non-superseded successfully settled builder attempt per node are canonical; other artifacts remain audit evidence. A cursor binds the exact run and optional attempt filter, pins the immutable artifact insertion sequence, and fingerprints canonical winners. Concurrent inserts remain outside that window; changed lineage returns a refresh-required conflict instead of mixing classifications. Complete artifact bytes remain available through authenticated downloads and full archive export.
- Execution detail exposes the latest durable publication batch summary independently of provider settlement or final run approval. Its path actions, results, and conflict reasons use a separate bounded run/attempt/batch-scoped endpoint. Each entry write advances the batch revision in the same transaction; continuation rejects a changed revision. Read failures are errors, never an absent-successful projection, and a page size is never a complete collection count.
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
- Provider-backed work order execution success requires a valid server-observed terminal response plus active attempt, invocation, lease, guardian, and node-specific output validation; process exit, finish reason, or handoff presence alone is not sufficient.
- Terminal process handoffs are durably file-backed before SQLite ingestion. Replay authority retains the exact active invocation, attempt, stream/session, lease owner/version, and execution payload lineage, and is valid only when its fixed observation time precedes the durable lease expiry; replay wall-clock expiry alone cannot erase already-durable process evidence.
- Provider-backed work order execution success must never be inferred from process exit, `step_finish.reason`, non-empty output text, mailbox projections, artifact presence, or export output.
- Provider-backed planning success must never be inferred from process exit, provisional stream text, or export output. It requires server-owned settlement for the active planner invocation from the provider's adjudicated terminal success plus non-empty output from the terminal-success assistant step; no earlier nonterminal step may supply fallback output.
- Empty terminal provider output is `provider_empty_output`; an unclassified provider failure is `provider_unknown_failure`. Both are canonical fail-fast codes, not text-derived retry signals.
- Latest node state must converge to durable `attempt_executions` terminal truth; a durably terminal attempt cannot leave its latest node run live.
- Requested rework is durably a node failure, because the workflow engine routes the loop back on failure. The `rework` value in `RunGraphNode.state` is an operator-facing read projection derived from the `execution_review_rework` and `validation_gate_rework` failure codes; it is never written to `node_runs.node_status`.
- Superseded attempts remain audit evidence only; they must not publish canonical planning output, mailbox responses, or canonical response artifacts.
- Superseded provider-backed attempts that already have a launch/session handoff must still flow through durable cancel dispatch and session-lease closure before they disappear from live execution.
- If an older attempt settles successfully before a queued retry ever launches, runtime cancels that queued retry as superseded-before-launch and excludes it from canonical latest-attempt lineage while preserving it in audit history.
- Graceful shutdown must wait for durable async completion handoff and drain pending execution outbox projection before returning.
- `run_status=succeeded` requires final authority acceptance:
  - automatically when `governance.autonomous_approvals=true`
  - explicit operator confirmation when `governance.autonomous_approvals=false`
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
