# Test Suites

This directory contains Kingdom's Go integration and end to end suites. Run commands from the repository root.

## Integration Tests

The H2 integration suite is in `tests/integration/`:

```bash
go test ./tests/integration/...
```

You can also run it with `make test-integration`.

## End to End Tests

The H3 suite is in `tests/e2e/`:

```bash
go test ./tests/e2e/...
```

You can also run it with `make test-e2e`.

For all Go test packages, run `make test`. For web app unit tests, run `make test-web`.
