# Prompt Authoring

`internal/prompts/catalog/` is the single source for authored role instructions, default Agent Guidance, node contracts, planning-mode contracts, communication-style framing, and mailbox templates. The Go package embeds these files and validates the catalog against Kingdom's canonical role IDs at startup. Do not keep prompt prose in provider profiles, orchestration literals, a second policy file, or a theme package.

## Source Map

| File | Owns |
| --- | --- |
| `roles.yaml` | Protected role instructions and system-owned mailbox templates |
| `default_guidance.yaml` | Substantive editable working defaults for each role |
| `node_contracts.yaml` | Shared authority/evidence, and node-class planning, path, inspection, response, and final-check requirements |
| `planning_modes.yaml` | Planning invocation context, shared safety, and the `reply`, `formalize`, and `generate_title` contracts and final checks |
| `communication_styles.yaml` | Built-in Neutral style and protected Communication Style framing |

`policies/role_policy.yaml` remains the authority for tools, commands, paths, and execution permissions. The prompt catalog guides model behavior; it does not grant capabilities or authorize workflow transitions.

## Protected And Editable Content

Protected Role Instructions contain role identity, distinct discipline, and the minimum useful duty that must survive a completely replaced guidance draft. Reviewer and validator role instructions also state the exact first-line verdict requirement; node contracts retain its detailed protocol and Final Checks repeat it after context. Kingdom Contracts own accepted requirement/architecture/rework baselines, authority, evidence integrity, workspace restrictions, node obligations, exact response formats, and final checks. Users cannot edit either source in the Agents page.

Working methods belong in Default Agent Guidance: diagnostic questions, engineering practices, research and test selection, decomposition, emphasis, and optional report organization. Split mixed clauses into their mandatory duty and editable method. Preserve every unique requirement when moving it, and consolidate repeated clauses only when their meaning is retained. Do not protect a working preference merely because it was previously part of a role prompt.

Default Agent Guidance is the substantive, prefilled working guidance shown for a role. User guidance is inserted without a scope wrapper. Conflicting guidance may produce poor model results; runtime permissions, workflow transitions, and response validation remain enforced. A saved role override replaces that guidance for new work; resetting removes the override and restores the catalog default. Communication Style can be selected for the workspace or overridden for one role. It may affect phrasing only. Workspace Custom and role Custom text are stored independently. Style and guidance supplied by a user or theme are inserted as literal text, never evaluated as templates.

Planning sessions use one Plan Author identity with protected invocation context. The same captured Plan Author Communication Style and Agent Guidance apply to discussion, formalization, and title generation. Defaults are phase-aware; editable research/discovery/decomposition methods apply when relevant to the selected task. The selected mode and exact output contract take precedence over guidance and style.

Operator is a human role and is excluded from editable agent settings. Governance work intake, plan decision, and completion, plus Plan Author execution kickoff, are runtime-owned transitions. Governance guidance does not control approvals or closure, and this catalog does not introduce model sessions for those transitions. Mailbox templates describe recorded runtime events; they are system-owned and never editable guidance.

## Runtime Composition And Snapshots

For execution, Kingdom resolves the catalog and effective role settings from the run's immutable snapshot. The provider system prompt is composed from Protected Role Instruction, Communication Style, Agent Guidance, and the applicable Kingdom Contract, including the required response format. Assignment, work order, approved plan, upstream guidance, and evidence are streamed as Run Context at their existing workflow stage. Kingdom appends the snapshot-resolved Final Checks after that context as the final section immediately before the provider responds. This keeps the concise checks visible after potentially large run context without making them editable or removing the higher-priority response contract from the system prompt.

For planning, the selected mode contract is composed with the captured Plan Author settings in the system prompt. Transcript and project/external context are streamed at the planning workflow stage, followed by the pinned mode-specific Final Checks as the final section. Reply and regenerate share one contract; formalize and title generation have distinct protected output and reminder lists. Do not put run-node settlement language in planning checks or buffer the full transcript to append them.

Run and planning-session creation persist the effective settings and reference one immutable, content-addressed catalog revision in the same transaction. Retries, resumes, and process restarts load that referenced revision. They must fail clearly if the revision is missing or its digest does not match; they must never substitute the latest embedded catalog. Existing records without a captured snapshot remain explicitly unsnapshotted.

Content changes produce a new catalog digest. Keep `Catalog.Version` aligned with the serialized catalog shape and validation rules. Never update or delete persisted catalog revisions or snapshots.

## Changing The Catalog

1. Inventory every source clause affected and record its protected/default destination or explicit equivalent before editing.
2. Edit the owning YAML file. Preserve each role's unique responsibilities and each node's required protocol.
3. Extend catalog, planning, provider-config, snapshot, or prompt tests to assert the specific clauses and boundaries affected.
4. Regenerate Go and TypeScript OpenAPI types only when the public API contract changes.
5. Run focused Go and web checks, then `make verify` for a full change.
6. When prompt behavior changes, compare a representative before/after provider prompt and confirm role capabilities remain intact.

## Inspecting Dispatched Instructions

The Agent Activity Instructions tab shows Kingdom's captured input in two Markdown sections: System Instructions and User Instructions. The system section is the exact invocation-scoped agent prompt, including protected role instructions, communication style, Agent Guidance, and Kingdom Contract. The user section is the complete captured Run Context and Final Checks, exactly as stored. Both prompt channels are published atomically under the attempt's queued lease and verified against their immutable content hashes before the API publishes bytes. Historical attempts never use current settings to rebuild their instructions. OpenCode's own environment and tool prompts are outside Kingdom's visibility.
