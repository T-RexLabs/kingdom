# Configuration

Kingdom reads its local settings from `.kingdom/config.yaml` when `kd summon` starts. `kd init` creates this file with values that work for a local setup. Most operators can leave it as it is.

Edit the file when you want to change where runs publish their work, whether Kingdom pauses for approval, or how it finds OpenCode. A setting in this file is a default for the local runtime. Changing it does not rewrite choices already saved on a run.

Unknown fields are rejected. Keep the field names and nesting shown below.

## Example

This shows the supported shape. `kd init` writes the local paths for your project and includes the official public GitHub App identifiers by default.

```yaml
version: v1
kingdom_home: /path/to/project/.kingdom
db_path: /path/to/project/.kingdom/kingdom.db

governance:
  autonomous_approvals: true

publish:
  mode: direct
  worktree_retention: 1h

server:
  listen: 127.0.0.1:8080
  base_path: /api/v1

provider:
  engine: opencode
  governance_mode: autonomous
  binary_path: ""

github:
  shared_app:
    slug: kingdom-issue-importer
    client_id: Iv23li1tLBTzhr70LA1J
```

## Settings

### Local files

`version` is written as `v1`. `kingdom_home` points to the local `.kingdom` directory, and `db_path` points to the SQLite database. These paths decide which local Kingdom data the runtime opens, so leave them as written unless you are deliberately moving that data.

### Approvals

`governance.autonomous_approvals` defaults to `true`. Kingdom moves through its plan and final approval gates without pausing. Set it to `false` to have Kingdom wait for you at both gates. This is the default for new runs; it does not change runs that already exist.

### Where run changes go

`publish.mode` is `direct` by default. Use `git_worktree` to keep each run's work in its own Git worktree. It requires a Git repository with a working tree. Bare repositories cannot use this mode.

`publish.worktree_retention` defaults to `1h`. Set a positive duration such as `30m` or `2h` to choose how long to keep each run worktree after its run ends. Set it to `never` to turn off automatic cleanup. This setting only applies when `publish.mode` is `git_worktree`.

### Local web address

`server.listen` defaults to `127.0.0.1:8080`. Installed Kingdom listens on loopback only. Keep it on a loopback address for local use. The `--listen`, `--host`, and `--port` options to `kd summon` can override the address for that process without changing this file.

`server.base_path` defaults to `/api/v1` and must be a valid path prefix. Kingdom adds a leading slash to values such as `v1`, but rejects the root path, malformed paths, and paths that conflict with Kingdom's web routes or static assets. Kingdom uses this path for API and browser requests. Keep the default unless you have a specific reason to serve the API under another path.

### OpenCode

`provider.engine` must be `opencode`. There is no other supported engine.

`provider.governance_mode` must be `autonomous`. Manual provider governance is not implemented.

`provider.binary_path` is optional. Set it when the `opencode` binary is not available on your `PATH`.

Kingdom requires OpenCode V2 (2.x). It runs one local OpenCode server on loopback and uses it to list models and to run every planning and execution request. Configure provider sign-ins, API keys, and custom model endpoints in OpenCode. Kingdom does not copy or refresh those credentials, and it writes no OpenCode agent profiles or configuration into your project. The model selector supports custom provider and model IDs; a listed model can still fail if its provider is unavailable or access has changed. Refreshing the model list reads the catalog without restarting active OpenCode locations.

Kingdom gives each request its own temporary agent profile and installs a small plugin in its own state directory so it can admit and record the file and shell tools a request uses. Shell commands run under Kingdom's process supervision. Requests that call other tools are refused.

Kingdom disables automatic formatting in the server. If administrator configuration overrides this setting, Kingdom reports a configuration error and does not start work. It also reports an error when the Kingdom plugin does not load or when the server's event stream is interrupted, because it can no longer prove what a request did. Interrupted work fails once nothing it started can still run. Until then the run stays blocked; see [Paused and blocked steps](how-runs-work.md#paused-and-blocked-steps). Explicit formatter commands remain separate from automatic formatting.

#### Unsupported OpenCode Changes

Kingdom runs its own OpenCode server beside any OpenCode you use yourself, but both use your OpenCode data directory and database. Sessions Kingdom creates appear there. Do not edit, delete, or continue those sessions, and do not change Kingdom's temporary agent profiles, from OpenCode's own interface or API: Kingdom proves what each request did from the events of its own session, and an outside change makes that proof impossible. Your own OpenCode plugins and configuration load in Kingdom's server too. A plugin that changes tool behavior, or a configuration that enables automatic formatting, is unsupported.

#### Restarts

Restarting Kingdom stops its OpenCode server and every step that was running. [When Kingdom restarts](how-runs-work.md#when-kingdom-restarts) explains what happens to those runs and how to continue them.

### GitHub App identifiers

`github.shared_app.slug` and `github.shared_app.client_id` identify the public App used by Kingdom's shared setup. `kd init` fills in Kingdom's official App. To turn off shared setup, leave both values blank. Set both or leave both blank; setting only one prevents startup.

Do not put secrets in this file. GitHub private keys, OAuth tokens, installation tokens, and the local operator token are stored under `.kingdom/secrets/`. The database stores opaque credential handles.

## Related Guides

For commands and the operator workflow, see the [README](../README.md). For GitHub setup and credential handling, see the [GitHub connections guide](github-connections-runbook.md). The [system architecture](system-architecture.md) explains behavior that configuration does not change.

Implementation defaults and validation live in `internal/cli/install_mode.go`.
