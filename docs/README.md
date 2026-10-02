# Kingdom Docs

The root [README](../README.md) is the best place to meet Kingdom and get it running. These guides answer the next questions, whether you run Kingdom or help build it.

## I run Kingdom

1. [How Runs Work](how-runs-work.md) explains Planning, Work Orders, approvals, results, and recovery.
2. [Config Reference](config-reference.md) explains the settings in `.kingdom/config.yaml`.
3. [GitHub Connections](github-connections-runbook.md) covers setup, access, and removal.
4. [Themes and Appearance](themes.md) shows how to choose or make a theme.

## Contribute to Kingdom

1. [Local Setup and Verification](local-setup-and-verification.md) explains how to install, change, and check the code.
2. [System Architecture](system-architecture.md) explains how the runtime fits together and the rules it must preserve.
3. [OpenAPI](../contracts/openapi/openapi.yaml) defines the HTTP contract.
4. [Policies](../policies/) define role authority, network access, and automatic retry behavior.

## Commands

For command options, run `kd help` or `kd help <command>`. The CLI help follows the installed version.

For policy defaults, read the files in `policies/`. For workflow steps, read the definitions in `internal/bootstrapassets/files/workflows/`. For API routes and request shapes, use OpenAPI.

For the meaning of runtime error codes, see the [error code matrix](../contracts/fixtures/error_codes/error_matrix.v1.json).

The architecture guide is the place for accepted design boundaries. It points to these more specific sources rather than copying every setting or route into another page.
