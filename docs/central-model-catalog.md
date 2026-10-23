# Central Model Catalog

The central service can discover models before it starts a project runtime. Discovery requires an authenticated project member and access to the selected personal or team connection revision. It supports API-key connections with an OpenAI-compatible `GET /models` endpoint. OAuth discovery is not implemented.

The service makes one bounded HTTPS request to the configured endpoint. The operator must allow its exact origin. Private addresses require an explicit address policy. Redirects are denied. Credentials remain in the trusted service. Responses contain model IDs, display names, supported thinking efforts, and cache timestamps; they do not contain credentials or provider endpoints.

## Routes

- `GET /api/central/v1/projects/{project}/connections/{connection}/revisions/{revision}/models` reads a fresh cached catalog.
- `POST /api/central/v1/projects/{project}/connections/{connection}/revisions/{revision}/models/refresh` discovers and replaces that exact revision's cache. The session's same-origin and CSRF checks apply.

These routes use the authenticated session identity. A request cannot supply an actor. Missing authorization, an unsupported profile, failed discovery, or an expired cache returns no catalog. Discovery does not make an inference request or certify that a later model request will succeed.

## Model Profiles

A provider's model list does not specify supported thinking efforts. The default OpenAI profile uses explicit capability metadata from the pinned OpenCode 2.0.30 registry. Its source URL and Git blob hash are retained in `internal/central/modelcatalog/openai_profiles.json`. Discovery intersects accessible provider model IDs with this trusted profile.

Custom endpoints require an explicit `modelcatalog.Config.Profiles` entry. For example, the equivalent profile JSON is:

```json
{
  "provider": "custom",
  "endpoint": "https://models.example.com/v1",
  "models": [
    {"id": "example-model", "name": "Example Model", "efforts": ["medium", "high"]}
  ]
}
```

This is an internal composition setting, not a new CLI configuration file. The endpoint must also appear in `Config.Origins`. An empty effort list allows only the provider default. Kingdom does not infer effort levels from model names.

Caches bind the complete immutable connection metadata and revision. The default lifetime is ten minutes. A new credential revision requires its own discovery. Selection checks use memory only and deny expired or absent entries. Project startup without an authenticated principal cannot discover models or load host credentials. The application owns `Service.Close`, which cancels and joins active discovery requests before the connection store closes.

## Connection Selection

The central portal provides personal API-key connections and team connections managed by team owners. Key replacement creates a new revision. The browser retains uncertain mutation requests in memory for exact retries and does not store keys in browser storage.

Hosted model pickers keep each connection and revision as a distinct choice, including when two connections expose the same model. Planning creation, planning autosave, run defaults, and role overrides send the plain provider/model ID with the exact connection reference. UI selection keys are not sent as model IDs. Local model pickers keep the local provider catalog and do not attach connection references.
