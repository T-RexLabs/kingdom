# Repository Instructions

## Start Here

- Read `docs/system-architecture.md` for runtime behavior, design changes, invariant work, or a broad repository review. Use focused references for small documentation or copy edits. Preserve accepted architecture, invariants, and trust boundaries unless the task explicitly changes them.
- Check `git status` and inspect the relevant code and tests before implementation. Preserve work outside the current task.
- Use `docs/local-setup-and-verification.md` for development commands and test selection, `tests/README.md` for evidence limits, and `README.md` for operator setup.
- Use the architecture source map to locate the canonical layer. Read focused docs, policies, and contracts when the change affects them.

## Plan and Authority

- For work with several steps or design decisions, state a concise, checkable plan in the conversation. Identify the outcome, affected boundaries, and verification. Adjust the plan when evidence changes.
- Read-only reviews stay read-only. Do not start implementation when the user asked only for inspection or advice.
- Once implementation is authorized, proceed with scoped edits, targeted checks, dependency setup, disposable fixtures, and local test processes. These routine actions do not need another approval.
- Ask when an unresolved requirement changes scope or design, or when an action can destroy retained data, change an external account, publish, deploy, or send a message. Existing explicit authorization still applies.
- Do not stop or use unrelated services. Do not use port 8080 for testing or evaluation.
- Use focused subagents for independent research, review, or verification when useful. Give each agent one clear task and an ownership boundary. Do not create overlapping edits.

## Implementation

- Keep each batch scoped to its requirement. Prefer simple, maintainable changes in the canonical layer; fix the root cause.
- For correctness bugs, reproduce the defect with a failing test first when practical. Protect invariants before larger refactors.
- Do not preserve backward compatibility unless the user explicitly requires it. Remove dead code and unsupported surfaces within the authorized scope.
- When a user correction reveals a repeatable defect or product invariant, add a regression test or a narrow clarification in the relevant docs. Do not add tests or tracked notes for conversational preferences alone.
- Keep the UX clean, responsive, and professional. Review the final change for unnecessary complexity and regression risk.
- If commits are requested, use semantic names and keep each commit scoped to a coherent batch.

## Verification

- Select checks by the changed behavior. The test selection table in `docs/local-setup-and-verification.md` gives starting points; add checks for affected consumers and failure paths.
- Use `make verify` for the broader deterministic gate. It includes Go and web tests, lint, race checks, vet, core coverage, generated contracts, OpenAPI lint, script checks, and shipped asset checks.
- Keep heavy checks within the shared CPU budget. Make defaults to `KINGDOM_TEST_CPUS=2`; for direct Go checks use `GOMAXPROCS=2 go test -p=1 -parallel=2` before the package arguments unless another budget is selected. Do not run independent heavy checks at the same time.
- For runtime or UX changes, run `make install`, create a fresh directory under `kd-sandbox/`, run `kd init`, and run `kd summon` on an available port other than 8080. Verify the embedded app at the reported URL. Stop that sandbox runtime after the check.
- For live runtime verification, use `OpenAI - GPT-5.6 Luna` with `None` thinking effort when model selection appears.
- For frontend iteration, set `KINGDOM_DEV_BACKEND_URL` to the fresh sandbox runtime's HTTP loopback URL before `make dev-web`. The development server rejects port 8080 and remote backends.
- Live checks are separate from deterministic checks. The GitHub smoke test permanently uninstalls its configured test App installation; follow its setup and authorization guidance before running it.
- Before reporting completion, inspect the diff and report exact commands, results, skipped checks, and remaining limits. Preserve useful verification artifacts under the task's `kd-sandbox/` directory and cite their paths in the result or PR notes. A passing unit or fixture test does not prove browser behavior or live provider compatibility.

## Contracts and Documentation

- Update `docs/system-architecture.md` when accepted architecture or invariants change.
- Update `README.md` and focused runbooks when operator behavior or runtime guidance changes. Update developer guides when the development workflow changes.
- Edit canonical policies, schemas, and OpenAPI sources. Regenerate shipped copies and derived contracts with the supported Make targets; do not patch generated files directly.
- Keep docs, policies, schemas, and contracts aligned with actual behavior. Do not use tracked docs for local planning notes.
- `CLAUDE.md` references this file; keep one repository instruction source.

## UI Text Casing

- **Title Case** for headings, modal titles and captions, field and toggle labels, buttons, status values, and metadata term names. Capitalize every word except articles, coordinating conjunctions, and prepositions of four letters or fewer, unless first or last.
- **Sentence case** for descriptions, help text, empty states, errors, loading text, placeholders, and aria-labels that describe an action rather than name a control.
- Preserve product spellings exactly: `GitHub`, `Kingdom`, `App`, `ID`.

## Communication

- Use direct, formal language and ASD-STE100 Simplified Technical English when explaining technical topics.
- Do not use emojis unless requested. Do not give time estimates unless asked.
