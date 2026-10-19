# Local Setup and Verification

Use this guide when changing Kingdom itself. If you only want to run it on a project, start with the [root README](../README.md).

## Install the tools

Kingdom pins its Go, Node, and pnpm versions in the Makefile. Check them with `make tool-versions`. You also need OpenCode V2 installed and signed in to the providers you plan to use.

From the repository root, run:

```bash
make install
export PATH="$PATH:$(go env GOPATH)/bin"
```

Corepack uses the pinned pnpm version to install and build the web app. The command also installs `kd`. It does not change your shell settings, so add the Go bin folder to your PATH again in a new terminal if needed.

## Check your changes

`make help` lists the supported targets. The main checks are:

| Command | What it checks |
| --- | --- |
| `make verify` | Go and web checks, race checks, vet, coverage, and generated contracts. |
| `make test-web` | Web app unit tests. |
| `make lint-web` | Web app lint rules. |
| `make contracts-check` | Generated API types and theme contracts. |

These commands can take time and may use local dependencies. Choose a check that matches the change, or run `make verify` before sharing a larger change.

## Check an installed runtime

Use this when a change needs a real Kingdom process and browser session. Make a fresh directory under `kd-sandbox/` for every check:

```bash
mkdir -p kd-sandbox/my-runtime-check
cd kd-sandbox/my-runtime-check

kd init
kd summon --port 18080
```

Pick a free port other than 8080 and replace `18080` if it is already in use. Open the URL printed by Kingdom.

In the browser, open **Planning**, select a model, and start a planning session or create a Work Order. Follow its run under **Runs** and inspect the workflow, execution, result, and approval state. If manual approvals are on, try both approval gates.

The fresh directory keeps this check separate from the repository and from your other projects. When you are done, stop Kingdom in that directory with `kd summon stop`. Do not use `kd clean` unless you intend to remove that directory’s entire `.kingdom/` state.

## Run the web app while developing

`make dev-web` starts Vite at `http://localhost:5173`. The proxy in `web/vite.config.ts` currently sends API and health requests to `localhost:8080`. For an installed runtime check, use the embedded web app at the URL printed by `kd summon --port <free port>` instead.

Kingdom serves the built web app from the Go process. Rebuild it with `make install` when you need to check the embedded version.

## Useful boundaries

`kd init` creates a local `.kingdom/` directory with config and runtime state.

The installed runtime listens on loopback. A port flag changes the local port for that process. It does not enable remote access.

Configuration lives in `.kingdom/config.yaml`. The [Config Reference](config-reference.md) explains its fields. The [System Architecture](system-architecture.md) explains runtime boundaries and recovery.
