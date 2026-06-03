## Repository Navigation

### Source of truth and intent

- `docs/architecture_rfc.md`: accepted architecture, invariants, and design boundaries.

### Operations and evidence

- `README.md`: onboarding, workflow map, quickstart, and core commands.

## Session Start Checklist

1. Read `docs/architecture_rfc.md` for constraints and invariants.
2. Review current code/test state (`git status`, targeted tests) before implementation.

## Update Matrix (Where changes go)

- Architecture or invariant changes: update `docs/architecture_rfc.md`.
- Runtime/operator guidance changes: update `README.md` and relevant runbooks in `docs/`.
- Verification evidence: include command output and artifacts in pull request notes and `kd-sandbox/` directories.

## Engineering Bar

- No duct-tape solutions.
- Think long term.
- Do not be lazy. No lazy implementations.
- Prefer scalable, maintainable, reliable, and clean code.
- Do not preserve backward compatibility unless the user explicitly asks for it.
- Leave no legacy code or dead code behind.
- Think in strategic coding standards, not tactical patches.
- UX and UI must feel clean, responsive, and professional.

## Execution Rules

- Preserve the accepted architecture and invariants from `docs/architecture_rfc.md`.
- For correctness bugs, reproduce with a failing test first when practical.
- Prefer fixing problems in the canonical layer instead of patching around the edges.
- Delete unsupported or dead surfaces instead of keeping them around.
- Do not start god-file refactors until earlier invariant fixes are protected by tests.
- For runtime and UX verification, run `make install`, create a fresh directory under `kd-sandbox/`, run `kd init` in that directory, run `kd summon`, and verify in the browser at `http://localhost:8080`.
- If model selection appears, always use `GitHub Copilot - GPT-5.4 Mini` with `Low` thinking.
- When creating commits, use semantic naming, keep them frequent, and scope each commit to the current batch only.
- Keep edits scoped to the current batch only.
- Update tracked repo docs (`README.md`, `docs/`, OpenAPI, policies, schemas) only when the actual product, runtime, or public contract changes. Do not churn tracked docs for local planning notes.

## Workflow Orchestration

### 1. Plan Mode Default

- Enter plan mode for any non-trivial task (3+ steps or architectural decisions).
- Plan mode is read-only: inspect, analyze, search, and delegate read-only exploration only.
- During plan mode, do not edit files, change system state, or use write-side shell commands.
- Ask clarifying questions when tradeoffs matter; do not make large assumptions about user intent.
- If something goes sideways, stop and re-plan immediately. Do not keep pushing.
- Use plan mode for verification steps, not just implementation.
- Write detailed specs upfront to reduce ambiguity.

### 2. Subagent Strategy

- Use subagents liberally to keep the main context window clean.
- Offload research, exploration, and parallel analysis to subagents.
- For complex problems, throw more compute at it via subagents.
- Keep one task per subagent for focused execution.

### 3. Self-Improvement Loop

- After any correction from the user, encode the lesson as a regression test or a clarified doc note in the touched area.
- Write rules that prevent repeating the same mistake.
- Ruthlessly iterate on these lessons until mistake rate drops.
- Review recent failures/tests before coding when context is uncertain.

### 4. Verification Before Done

- Never mark a task complete without proving it works.
- Diff behavior between main and your changes when relevant.
- Ask yourself: "Would a staff engineer approve this?"
- Run tests, check logs, and demonstrate correctness.

### 5. Demand Elegance (Balanced)

- For non-trivial changes, pause and ask: "Is there a more elegant way?"
- If a fix feels hacky, re-implement the elegant solution with current understanding.
- Prefer simple, obvious fixes over over-engineering.
- Challenge your own work before presenting it.

### 6. Autonomous Bug Fixing

- When a bug is reported, do not start by changing implementation. Reproduce it with a failing test first when practical.
- Point at logs, errors, and failing tests.
- Have subagents attempt fixes and prove them with a passing test.

## Task Management

1. **Plan First**: write a concise, checkable execution plan in-session before major implementation.
2. **Verify Plan**: check in before starting implementation.
3. **Track Progress**: mark items complete as you go.
4. **Explain Changes**: provide high-level summary at each step.
5. **Document Results**: cite exact commands, outputs, and artifact paths used as evidence.
6. **Capture Lessons**: add durable guardrails via tests or narrowly scoped documentation updates.

## Core Principles

- **Simplicity First**: make every change as simple as possible; minimize code impact.
- **No Laziness**: find root causes; avoid temporary fixes; hold senior-engineer standards.
- **Minimal Impact**: touch only what is necessary and avoid introducing regressions.
