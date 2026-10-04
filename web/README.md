# Kingdom Web App

The `web/` application is the primary local interface for operating Kingdom.

## Current Capabilities

- Create work orders in Planning, review plans, and start runs.
- Set `autonomous_approvals` and bounded review and validation cycle controls for a work order.
- View run status, the workflow graph, timeline, execution output, and artifacts.
- Handle approvals and run controls from the run detail view.

Run Details provides workflow and execution views. Mailbox messages remain available through the API and related audit events; there is no mailbox panel in the web app.

## Local Development

Install `kd` with `make install`, then start a fresh sandbox runtime on a free port other than 8080. Follow the [local verification guide](../docs/local-setup-and-verification.md#check-an-installed-runtime) for setup and cleanup.

From the repository root in a second terminal, start the frontend server with the sandbox's reported UI URL:

```bash
KINGDOM_DEV_BACKEND_URL=http://127.0.0.1:18080 make dev-web
```

Replace the backend URL with the sandbox's reported URL. The setting requires HTTP loopback (`localhost`, `127.0.0.1`, or `[::1]`) and an explicit port other than 8080. Remote hosts, credentials, non-root paths, query strings, and fragments are rejected. There is no backend default.

Open the frontend URL reported by Vite, normally `http://127.0.0.1:5173`. API and health requests stay on the frontend origin and proxy to the selected sandbox without changing the request origin. The existing authentication flow still applies. Production builds, previews, and unit tests do not require this setting.

Changes under `web/` use Vite hot reload. Backend and scaffold changes require rebuilding and restarting the runtime. For final runtime and UX verification, run `make install` and inspect the embedded app at Kingdom's reported URL in a fresh sandbox. Use **OpenAI - GPT-5.6 Luna** with **None** thinking effort when model selection appears. Stop both test processes after the check.

## Build and Checks

From the repository root:

```bash
make build
make test-web
make lint-web
```

The production web build is embedded in the `kd` binary and in locally built `kingdomd` binaries.

Vitest checks frontend logic and components with jsdom. It does not prove browser rendering or live provider behavior. Use `make verify` for the broader deterministic gate and report browser or live checks separately.

Frontend tests default to `Europe/Stockholm` when `TZ` is unset. This lets the daylight saving time gap tests run in local and CI checks. Set `TZ` explicitly to check another timezone, for example `TZ=UTC pnpm --dir web test`. Report any timezone-dependent skips. This default applies only to Vitest.

## Notes

- The work order form defaults to five review cycles and five validation cycles, following the seeded workflow. These limits can be changed for each run.
- API routes follow runtime policy. Direct clients use `Authorization: Bearer` with the initialized workspace credential. Caller supplied actor and role headers do not grant authority.
