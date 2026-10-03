# Policy Pack

These files define Kingdom's canonical runtime policy. Execution starts with no permissions, and policies grant only the capabilities required by the runtime contract.

- `role_policy.yaml`: tool, command, and path limits by role.
- `network_policy.yaml`: outbound domain allow lists by role and node class.
- `retry_policy.yaml`: retry classes, error code mapping, backoff, and escalation.
- `opencode_provider_policy.yaml`: OpenCode provider timeouts, agent mapping, and retry boundaries. Model selection stays per run.
- Agent instructions and mailbox templates live in the embedded catalog under `internal/prompts/catalog/`.

## Editing Principles

- Start with least privilege. New roles, tools, command classes, paths, and domains should remain denied until the runtime contract requires them.
- Change the canonical policy file, not generated or embedded copies. `kd init` scaffolding and bootstrap assets derive from this directory.
- Keep policy changes aligned across layers. A role or node class change may also require updates to schemas, bootstrap assets, provider policy role mapping, and tests.
- Remove unsupported policy options instead of leaving inactive configuration behind.

## Which File to Edit

- Edit `role_policy.yaml` when a role's allowed tool classes, command classes, path scopes, execution authority, or forbidden command classes change.
- Edit `network_policy.yaml` when a role or node class needs outbound access to a specific domain.
- Edit `retry_policy.yaml` when an error code needs a different retry class, backoff, or escalation path.
- Edit `opencode_provider_policy.yaml` when startup validation, dispatch timeouts, stream inactivity windows, or role to agent mappings change.
- Edit `internal/prompts/catalog/` when shipped role instructions, default guidance, planning contracts, node contracts, communication styles, or mailbox templates change.

## Timeout Behavior

`timeouts.node_wall_clock` is an activity gated budget for provider backed work order execution nodes. It is not an unconditional kill switch. Exceeding the wall clock budget becomes terminal only after a full `timeouts.stream_inactivity_timeout` without real execution activity or active completion settlement observation. Synthetic runtime polling does not count as real execution activity.

## Typical Changes

### Add a Command or Tool Capability to a Role

If `builder` needs a new command class, extend only that role in `role_policy.yaml` and keep the scope narrow:

```yaml
roles:
  builder:
    allowed_command_classes:
      - git_safe
      - go_toolchain
      - node_toolchain
      - test_runner
```

Do not add the same capability to advisory or governance roles unless the accepted architecture changes.

### Allow Network Access for One Node Class

If dependency installation needs one additional registry, add it to the exact role and node class override in `network_policy.yaml` rather than widening the default domain list:

```yaml
roles:
  builder:
    node_class_overrides:
      dependency_install:
        - proxy.golang.org
        - sum.golang.org
        - registry.npmjs.org
```

Keep `global_denies` intact unless the accepted security baseline changes.

### Adjust Retry Behavior

Map runtime error codes in `retry_policy.yaml` instead of hardcoding retry rules in Go. For example, the current policy maps timeouts to `transient_transport` and invalid output schemas to `non_retryable`:

```yaml
error_code_map:
  timeout: transient_transport
  invalid_output_schema: non_retryable
```

The runtime reads these classes; it should not fork retry semantics in code.

## Validation and Startup Guarantees

Policy loading requirements:

- Reject unknown fields.
- Fail startup when a policy file is invalid.
- Log policy decisions with their authority and reason.
- Include allow, deny, and failure path tests with policy changes.

A broken policy change should fail during bootstrap or runtime startup, not later on a request path.

## Verification Expectations

- Run targeted Go tests for the layer that consumes the changed policy.
- Run `make install` when a change affects scaffolded runtime assets or the fresh install path.
- Run the relevant contract checks when public contracts or shipped assets change with the policy.
- For operator facing changes, verify a fresh local runtime. From the repository root, run:

  ```bash
  make install
  mkdir -p kd-sandbox/policy-check
  cd kd-sandbox/policy-check
  kd init
  kd summon --port 18081
  ```

  Open the URL reported by Kingdom. Use another available port if 18081 is occupied, and do not use 8080 for evaluation.

Examples:

- Role or network policy changes usually need `go test ./internal/core/... ./internal/orchestration ./internal/api`.
- Retry policy changes usually need `go test ./internal/core/... ./internal/state ./internal/orchestration`.
- Role communication changes usually need `go test ./internal/bootstrapassets ./internal/core/... ./internal/orchestration` and a fresh `kd init` when scaffolded prompts are affected.
