# Kingdom Web UI

The `web/` app is the primary operator interface for Kingdom runtime.

## What it covers

- Create decrees and start runs in one flow.
- Set run governance behavior (`autonomous_approvals`) and cycle controls.
- Monitor run status, timeline, mailbox handoffs, node graph, and artifacts.
- Perform approvals and run control actions from the run detail experience.

## Local development

From the repository root, start the runtime first:

```bash
mkdir -p kd-sandbox/local-dev
cd kd-sandbox/local-dev

kd init
kd summon
```

Then start the frontend dev server from `web/` or via `make dev-web` at the repo root:

```bash
pnpm install
pnpm dev
```

Use `http://127.0.0.1:5173` for local frontend iteration. The Vite dev server proxies `/api` and `/healthz` to the summoned runtime at `http://127.0.0.1:8080`.

Development boundary:

- edits in `web/` use Vite hot reload
- backend, config, policy, and schema edits require restarting `kd summon`
- final verification still belongs on the embedded runtime UI at `http://127.0.0.1:8080`

## Build

- `pnpm build`

The production build is embedded into the `kd` binary and any locally built `kingdomd` binary.

## Notes

- Web defaults for bounded review/validation cycles follow seeded workflow expectations (`5` each in the decree form) and can be overridden per run.
- API routes are governed by runtime policy; when testing endpoints directly, include required actor/role headers.
