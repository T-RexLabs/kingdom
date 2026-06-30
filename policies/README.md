# Policy Pack

These files are the canonical runtime policy baseline for deny-by-default
execution.

- `role_policy.yaml`: role-scoped tool, command, and path constraints.
- `network_policy.yaml`: role-scoped domain egress allowlists.
- `retry_policy.yaml`: canonical retry classes and code mapping.
- `opencode_provider_policy.yaml`: OpenCode provider timeout, agent-mapping, and retry-boundary defaults; runtime model selection stays per-run.
- `role_communication_policy.yaml`: per-canonical-role system prompt and mailbox communication templates.

## Editing principles

- Start from least privilege. New roles, tools, command classes, paths, and domains should begin denied and be opened only when the runtime contract genuinely requires them.
- Change the canonical policy file, not generated or embedded copies. `kd init` scaffolding and bootstrap assets derive from this directory.
- Keep policy changes aligned across layers. A role or node-class change often needs matching updates in schemas, bootstrap assets, provider-policy role mapping, and tests.
- Prefer deleting unsupported policy surface over leaving inert config behind.

## Which file to edit

- Edit `role_policy.yaml` when a role's allowed tool classes, command classes, path scopes, execution authority, or forbidden command classes change.
- Edit `network_policy.yaml` when a role or node class needs outbound network access to a specific domain.
- Edit `retry_policy.yaml` when a runtime error code should map to a different retry class, backoff, or escalation path.
- Edit `opencode_provider_policy.yaml` when strict startup validation, dispatch timeouts, stream inactivity windows, or canonical role-to-agent mappings change.
- Edit `role_communication_policy.yaml` when the shipped role prompts or mailbox templates change.

Timeout note:

- `timeouts.node_wall_clock` is an activity-gated budget for provider-backed decree execution nodes, not an unconditional kill switch; terminal timeout still requires inactivity beyond `timeouts.stream_inactivity_timeout`, ignoring synthetic runtime polling while honoring real execution activity and active completion-settlement observation.

## Typical changes

### Add a command/tool capability to a role

If `builder-berserker` needs a new command class, extend only that role in
`role_policy.yaml` and keep the scope narrow:

```yaml
roles:
  builder-berserker:
    allowed_command_classes:
      - git_safe
      - go_toolchain
      - node_toolchain
      - test_runner
```

Do not add the same capability to advisory or governance roles unless the
architecture explicitly changes.

### Allow network access for one node class

If dependency installation needs one additional registry, add it to the exact
role and node-class override in `network_policy.yaml` rather than widening the
default domain list:

```yaml
roles:
  builder-berserker:
    node_class_overrides:
      dependency_install:
        - proxy.golang.org
        - sum.golang.org
        - registry.npmjs.org
```

Keep `global_denies` intact unless the accepted security baseline changes.

### Adjust retry behavior

When a new runtime error code is introduced, map it in `retry_policy.yaml`
instead of hardcoding retry rules in Go:

```yaml
error_code_map:
  timeout: transient_transport_provider_session
  invalid_output_schema: non_retryable
```

Retry behavior is a policy concern. The runtime should read these classes, not
fork independent retry semantics in code.

## Validation and startup guarantees

Policy loading requirements:

- Unknown fields must be rejected.
- Invalid policy files must fail startup.
- Policy decisions must be logged with authority and reason.
- Any policy change must include allow/deny and failure-path tests.

In practice this means a broken policy change should fail during bootstrap or
runtime startup, not later on a request path.

## Verification expectations

- Run targeted Go tests for the layer that consumes the changed policy.
- Run `make install` when the change affects scaffolded runtime assets or the
  fresh-install path.
- If the public contract or shipped assets move with the policy change, run the
  relevant contract checks as well.
- For operator-facing behavior changes, verify the flow in a fresh local runtime
  under `kd-sandbox/` using the remediation playbook.

Examples:

- Role or network policy changes usually need `go test ./internal/core/... ./internal/orchestration ./internal/api`.
- Retry policy changes usually need `go test ./internal/core/... ./internal/state ./internal/orchestration`.
- Role communication changes usually need `go test ./internal/bootstrapassets ./internal/core/... ./internal/orchestration` and a fresh `kd init` if scaffolded prompts are affected.
