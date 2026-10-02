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
  strict: true
  governance_mode: autonomous
  binary_path: ""
  model: ""
  agent: ""
  skip_permissions: true

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

`provider.strict` is set to `true` by `kd init`. At startup, this checks the managed OpenCode files, the binary, and role resolution before Kingdom reports ready. Set it to `false` only when debugging a local setup.

`provider.governance_mode` must be `autonomous`. Manual provider governance is not implemented.

`provider.binary_path` is optional. Set it when the `opencode` binary is not available on your `PATH`.

`provider.model` and `provider.agent` are optional defaults for OpenCode. A run still needs an explicit global model choice.

`provider.skip_permissions` defaults to `true` and keeps OpenCode runs from stopping for interactive permission prompts.

### GitHub App identifiers

`github.shared_app.slug` and `github.shared_app.client_id` identify the public App used by Kingdom's shared setup. `kd init` fills in Kingdom's official App. To turn off shared setup, leave both values blank. Set both or leave both blank; setting only one prevents startup.

Do not put secrets in this file. GitHub private keys, OAuth tokens, installation tokens, and the local operator token are stored under `.kingdom/secrets/`. The database stores opaque credential handles.

## Related Guides

For commands and the operator workflow, see the [README](../README.md). For GitHub setup and credential handling, see the [GitHub connections guide](github-connections-runbook.md). The [system architecture](system-architecture.md) explains behavior that configuration does not change.

Implementation defaults and validation live in `internal/cli/install_mode.go`.
