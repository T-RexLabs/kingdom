# Test Suites

Run commands from the repository root. Use the [development check table](../docs/local-setup-and-verification.md#select-checks-by-the-change) to select checks for a change.

## Deterministic Checks

| Command | Evidence |
| --- | --- |
| `go test ./internal/<package>/...` | Behavior of the selected Go package and its fixtures. |
| `make test-integration` | Cross-component state and workflow behavior in `tests/integration/` (the H2 suite). |
| `make test-e2e` | API and orchestration flows in `tests/e2e/` (the H3 suite), with controlled provider fixtures. |
| `make test-web` | Frontend data and configuration tests under Node, and component behavior under Vitest and jsdom. |
| `make test-race` | Race detector checks for state, planning, API, orchestration, process supervision, OpenCode, and execution storage. |
| `make scripts-check` | Shell syntax, Python harness and build-workflow checks, and Node asset-generator checks. |
| `make assets-check` | Complete file-set and byte equality for canonical and shipped assets. |
| `make contracts-check openapi-lint` | Generated contract consistency and OpenAPI validation. |
| `make verify` | The broader deterministic gate, including the above checks, web lint, vet, and core coverage. |

`make test` runs all Go packages. The Make test targets prepare the embedded web build when needed; a direct Go command can need an existing `web/dist/` build in a fresh checkout. Run `make build` first if the selected packages embed web assets.

A deterministic end-to-end pass does not prove browser rendering, accessibility, or live provider compatibility. Frontend component tests use jsdom rather than a full browser. Go tests can use temporary files, managed fixture processes, and local test servers; these are routine test resources and do not need external service credentials.

Database behavior fixtures can use `internal/testutil/sqlitefixture` to restore an empty canonical schema into a separate database for each test. Only the immutable schema image is shared. Keep migration-runner tests on the real migration path, and keep file-backed or fault-injection fixtures on their original connections. The fixture guards check schema equivalence, foreign keys, triggers, and isolation.

`web/test-projects.ts` lists tests whose runtime imports do not need browser APIs. These tests run in an isolated Node environment without DOM setup. All other frontend tests use the existing jsdom storage environment and component setup. Keep a test in the DOM project if its imports need `document`, storage, or other browser APIs. Both projects use the shared worker budget and isolated worker threads. Each test file gets a fresh worker; module state is not shared between files.

The OpenCode suite replays recorded 1.18.32 JSONL streams, including compaction and intermediate stop events. The fixtures prove handling of those streams. They do not certify every installed OpenCode version.

## Browser and Live Checks

For runtime and UX changes, follow the fresh installation, sandbox, browser, model selection, and cleanup steps in the [local verification guide](../docs/local-setup-and-verification.md#check-an-installed-runtime). Use an available port other than 8080.

Live OpenCode and GitHub tests are opt-in. Read their setup in the [controlled live checks guide](../docs/local-setup-and-verification.md#controlled-live-checks) and report skipped tests. The GitHub smoke permanently uninstalls its configured test App installation and requires explicit authorization. The Python work-order scenarios under `scripts/e2e/` are also live checks; they are not run by `make test-e2e`.

## Completion Evidence

Report exact commands, passed and failed checks, skips, and remaining limits. Save useful logs and reports in the task's fresh `kd-sandbox/` directory. A test that skipped due to missing credentials or a missing model has not verified that live scenario.
