# API Contract

[openapi.yaml](openapi.yaml) defines the public API. Run `make contracts-generate` after changing it and `make contracts-check` to check the generated Go and TypeScript types.

## Hosted Model Selection

Planning model selections and each run model selection entry can include an exact connection revision:

```json
{
  "model": "openai/gpt-model",
  "thinking_effort": "medium",
  "connection": {
    "id": "2d53972c-7a93-46f4-9ef1-8d23f4ca760b",
    "revision": 3
  }
}
```

This field requires a project-bound hosted API with a trusted selection resolver. The example model is illustrative; the exact model and thinking effort must exist in the cached catalog for that connection revision. The API derives the actor from authentication and records only credential references. A new planning action must use a connection available to the requesting user. Automatic titles keep their causal reply's selection. A saved snapshot does not bypass later access checks.

Local API requests reject the `connection` field. The hosted execution backend is not yet enabled; this contract does not provide a hosted startup command or a working remote execution path.
