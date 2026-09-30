# Docs Guide

  Each document should answer one clear question. If a file starts duplicating OpenAPI, policy files, workflow JSON, or low-level code constants, it should usually be trimmed rather than expanded.

## Read These First

- [`system-architecture.md`](system-architecture.md): the accepted architecture, invariants, and design boundaries
- [`how-runs-work.md`](how-runs-work.md): how planning, decrees, runs, reviews, and closure work end to end
- [`local-setup-and-verification.md`](local-setup-and-verification.md): how to install, run, and verify Kingdom locally

## Reference Docs

- [`github-connections-runbook.md`](github-connections-runbook.md): GitHub App setup, key lifecycle, recovery, and operator disclosures.

- [`interfaces-overview.md`](interfaces-overview.md): the main web, CLI, API, metrics, and streaming interfaces
- [`data-and-storage.md`](data-and-storage.md): durable data model and storage notes
- [`errors-and-retries.md`](errors-and-retries.md): runtime error categories and retry behavior
- [`config-reference.md`](config-reference.md): important operator-facing defaults and override guidance

Use `kd --help` and `kd help <command>` for the CLI source of truth.

## Canonical Sources Of Truth

- API routes and schemas: `contracts/openapi/openapi.yaml`
- Runtime policies: `policies/*.yaml`
- Seeded workflows: `internal/bootstrapassets/files/workflows/*.json`
- Product invariants: [`system-architecture.md`](system-architecture.md)

## Naming Rule

Prefer plain-language file names that tell the reader what question the document answers.

Examples:

- `how-runs-work.md` is better than `runtime_flow.md`
- `data-and-storage.md` is better than `state_storage.md`
