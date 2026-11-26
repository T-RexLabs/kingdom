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

Local API requests reject the `connection` field. Hosted execution uses the separate `kingdom-central` entrypoint and the verified managed OpenShell profile. See the [central service guide](../../docs/central-service.md) for startup and operator requirements. Default local startup remains independent of the central service and OpenShell.

[central-models.yaml](central-models.yaml) defines the separate central connection model discovery routes. See [Central Model Catalog](../../docs/central-model-catalog.md) for authorization, cache lifetime, and operator profile requirements.

[central-access.yaml](central-access.yaml) defines configured-provider account admission, the team-owner directory, trusted current project roles, and project membership management. Project ownership and team ownership are separate authorities.
