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

Kingdom uses port 8080 by default, so that port usually belongs to the Kingdom you use day to day. Give every check its own free port, such as `18080`, so it never talks to or changes your own Kingdom. Open the URL printed by Kingdom.

In the browser, open **Planning**, select any model you have access to, and start a planning session or create a Work Order. Follow its run under **Runs** and inspect the workflow, execution, result, and approval state. If manual approvals are on, try both approval gates.

The fresh directory keeps this check separate from the repository and from your other projects. When you are done, stop Kingdom in that directory with `kd summon stop`. Do not use `kd clean` unless you intend to remove that directory’s entire `.kingdom/` state.

## Run the web app while developing

`make dev-web` starts Vite at `http://localhost:5173` with live reload. It sends API requests to the Kingdom on the default port, 8080, so start one with `kd summon` first. This is for working on the web app. To check a change the way users will see it, use a sandbox on its own port as described above.

Kingdom serves the built web app from the Go process. Rebuild it with `make install` when you need to check the embedded version.

## Useful boundaries

`kd init` creates a local `.kingdom/` directory with config and runtime state.

The installed runtime listens on loopback. A port flag changes the local port for that process. It does not enable remote access.

Configuration lives in `.kingdom/config.yaml`. The [Config Reference](config-reference.md) explains its fields. The [System Architecture](system-architecture.md) explains runtime boundaries and recovery.
