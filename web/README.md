# Kingdom Web App

The `web/` application is the primary local interface for operating Kingdom.

## Current Capabilities

- Create work orders in Planning, review plans, and start runs.
- Set `autonomous_approvals` and bounded review and validation cycle controls for a work order.
- View run status, the workflow graph, timeline, execution output, and artifacts.
- Handle approvals and run controls from the run detail view.

Run Details provides workflow and execution views. Mailbox messages remain available through the API and related audit events; there is no mailbox panel in the web app.

## Local Development

From the repository root, start the frontend development server:

```bash
make dev-web
```

Open `http://127.0.0.1:5173` for isolated frontend iteration. The Vite proxy sends `/api` and `/healthz` requests to `localhost:8080`, as configured in `web/vite.config.ts`. Do not use this proxy for runtime evaluation.

First install `kd` with `make install` from the repository root. Add the Go bin folder to your `PATH` if the command is not found. Then evaluate the embedded app in a fresh workspace and on a port other than 8080:

```bash
mkdir -p kd-sandbox/evaluation
cd kd-sandbox/evaluation
kd init
kd summon --port 18081
```

If port 18081 is occupied, choose another available port. Open the URL reported by Kingdom.

Changes under `web/` use Vite hot reload. Changes to backend code, configuration, policies, or schemas require rebuilding and restarting the runtime.

## Build and Checks

From the repository root:

```bash
make build
make test-web
make lint-web
```

The production web build is embedded in the `kd` binary and in locally built `kingdomd` binaries.

## Notes

- The work order form defaults to five review cycles and five validation cycles, following the seeded workflow. These limits can be changed for each run.
- API routes follow runtime policy. Direct clients use `Authorization: Bearer` with the initialized workspace credential. Caller supplied actor and role headers do not grant authority.
