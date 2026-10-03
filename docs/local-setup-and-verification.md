# Local Setup and Verification

Use this guide when changing Kingdom itself. To run Kingdom on a project, start with the [root README](../README.md).

## Install the Tools

Use the Go, Node, pnpm, and Python versions in `.tool-versions`. The Go module and web package record the same required Go and pnpm versions. Install pnpm as a separate tool and make `go`, `node`, `pnpm`, and `python3` available on PATH. Python runs tool preflight, script checks, and coverage reporting. `make tool-versions` prints the expected versions, and `make tools-check` checks the active tools before builds and verification. You also need OpenCode installed and signed in for live provider checks.

From the repository root, run:

```bash
make install
export PATH="$PATH:$(go env GOPATH)/bin"
```

This builds the embedded web app and installs `kd`. Add the Go bin folder to your shell's PATH if needed. A build does not need an OpenCode account or a running backend.

The provider regression suite includes recorded OpenCode 1.18.32 JSONL streams under `internal/providers/opencode/testdata/`. These are evidence for the recorded stream behavior, not a certification of every feature or a requirement that a newly installed version works without verification. Record `opencode --version` with each live check.

## Select Checks by the Change

Run commands from the repository root. Start with the affected package or test, then check the relevant consumers and failure paths. `make help` lists the supported targets.

| Changed Area | Starting Checks | Additional Evidence |
| --- | --- | --- |
| Go package behavior | `go test ./internal/<package>/...` | Include affected consumers; reproduce correctness bugs with a failing test when practical. |
| State, transitions, scheduling, or recovery | `go test ./internal/state/... ./internal/orchestration/... ./tests/integration/... ./tests/e2e/...` | `make test-race`; fresh runtime restart/recovery checks for changed operator behavior. |
| Planning or provider execution | `go test ./internal/planning/... ./internal/providers/opencode/... ./internal/processguardian/...` | `make test-race`; a controlled live check when provider compatibility changes. |
| Web UI or frontend configuration | `make test-web lint-web build` | Fresh installed runtime and browser check of the changed flow. |
| HTTP contract or error shapes | `make contracts-check openapi-lint` and `go test ./internal/api/...` | Update canonical OpenAPI; generate derived types with `make contracts-generate`. |
| Policies, schemas, or scaffolded assets | `make assets-check` and `go test ./internal/bootstrapassets/... ./internal/core/...` | Use `make assets-generate` after canonical edits; check affected consumers and fresh `kd init`. |
| Development or end-to-end scripts | `make scripts-check` | Controlled fixture checks for lifecycle, cleanup, and error behavior; live runs only when needed. |
| Broader changes or final delivery | `make verify` | Report any skipped live or browser checks separately. |

`make verify` runs Go and web tests, web lint, race checks for state, planning, API, orchestration, process supervision, OpenCode, and execution storage, vet, core coverage, generated contract checks, OpenAPI lint, script checks, and shipped asset checks. The [Test Suites guide](../tests/README.md) explains what each layer proves.

Keep useful logs and reports under a fresh task directory in `kd-sandbox/`. Report commands, results, skips, and artifact paths. A passing deterministic test does not prove live provider behavior or browser rendering.

## Check an Installed Runtime

Use this when a change needs a real Kingdom process and browser session. Rebuild with `make install`, then make a fresh directory under `kd-sandbox/` for each check:

```bash
mkdir kd-sandbox/my-runtime-check
cd kd-sandbox/my-runtime-check
kd init
kd summon --port 18080
```

Use a new directory name. Check that the selected port is free, and use another port if needed. Never use 8080 for testing or evaluation. Open the UI URL reported by Kingdom.

Check the changed flow and its failure paths. For workflow changes, open **Planning**, create a Work Order, and inspect its workflow, execution, results, and approvals under **Runs**. Use **OpenAI - GPT-5.6 Luna** with **None** thinking effort when model selection appears. If approval behavior changed, test both affected approval gates.

The fresh directory keeps test state separate from the repository and other projects. When the check is complete, stop its process with `kd summon stop` from that directory. Do not use `kd clean` unless removal of that directory's entire `.kingdom/` state is intended and authorized.

## Develop the Web App

Start a fresh sandbox runtime as described above. In a second terminal, from the repository root, set its reported UI URL as the development backend:

```bash
KINGDOM_DEV_BACKEND_URL=http://127.0.0.1:18080 make dev-web
```

Replace the URL with the sandbox's reported URL. Vite requires an explicit HTTP URL on `localhost`, `127.0.0.1`, or `[::1]`, with a port other than 8080. It rejects remote hosts, credentials, paths other than `/`, query strings, and fragments. There is no backend default.

Open the frontend URL reported by Vite, normally `http://127.0.0.1:5173`. Browser API calls stay on the frontend origin and Vite proxies `/api` and `/healthz` to the selected sandbox without changing the request origin. The existing authentication flow still applies. The backend setting is not embedded in production assets and is not required for builds, previews, or unit tests.

Vite reloads changes under `web/`. Rebuild and restart the sandbox for backend or scaffold changes. For final runtime and UX verification, rebuild with `make install` and use Kingdom's embedded UI at its reported URL.

## Controlled Live Checks

Live tests need separate setup and do not run as part of the deterministic gate. Read the relevant test before configuring credentials. Never include secrets in logs or reports.

- `make test-opencode-security-smoke` runs the controlled planner containment and workspace capability tests in `internal/planning/opencode_agent_live_test.go`. It needs an authenticated OpenCode model. The default model is `openai/gpt-5.6-luna`; record the selected model, thinking effort, OpenCode version, and any skips.
- `make test-github-smoke` runs `internal/external/github/client_live_test.go`. It **permanently uninstalls the configured GitHub App installation**. Use an operator-owned disposable test App installation and obtain explicit authorization for this external account change. Set the required `KINGDOM_GITHUB_SMOKE_*` values listed in that file, including the private key file path, controlled repository and issue IDs, and `KINGDOM_GITHUB_SMOKE_ALLOW_UNINSTALL=1`. Missing prerequisites produce a skip; a skip is not a pass of the live scenario.
- `scripts/run_in_existing_repo_e2e.sh` and `scripts/run_rework_analysis_e2e.sh` drive live work-order scenarios. Read their `--help` output, select the required model explicitly, and retain the output directory under `kd-sandbox/`. They are separate from `make test-e2e`.

## Runtime Boundaries

`kd init` creates `.kingdom/` with configuration, runtime state, and managed OpenCode profiles. Do not edit managed profiles directly; change their repository source and verify a fresh scaffold. Kingdom checks managed profiles at startup.

The installed runtime listens on loopback. A port flag changes its local port and does not enable remote access. Configuration lives in `.kingdom/config.yaml`; see the [Config Reference](config-reference.md). The [System Architecture](system-architecture.md) defines authority and recovery boundaries.
