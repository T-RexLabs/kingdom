# Optional Local OpenShell

Normal `kd init` and `kd summon` use local OpenCode. They do not require Docker, an OpenShell gateway, or managed credentials. OpenShell is an explicit project setting:

```yaml
execution:
  backend: openshell
  managed_config: /absolute/private/path/operator.json
```

The operator file must be a regular file with mode `0600`. It cannot be a symlink. Paths in this file are absolute. Start Kingdom with `kd summon` as usual after you set up the gateway and the trusted worker image described in [Central Service](central-service.md). The browser remains on the local loopback address. The worker callback listener uses a separate HTTPS address that the sandbox can reach.

## Operator File

The following example shows the API-key configuration. Replace the identities, paths, image digest, and all SHA-256 values with the exact deployed profile. The master key file contains a hex-encoded 32-byte random key. The API key and private key files must have mode `0600`.

```json
{
  "version": 1,
  "state_root": "/private/kingdom-managed",
  "owner_id": "00000000-0000-4000-8000-000000000001",
  "gateway_id": "my-gateway",
  "worker_id": "local-worker",
  "gateway": {
    "address": "gateway.example:443",
    "ca_file": "/private/tls/ca.crt",
    "certificate_file": "/private/tls/client.crt",
    "key_file": "/private/tls/client.key"
  },
  "image": "registry.example/kingdom-worker@sha256:<image-digest>",
  "profile_sha256": "<profile-sha256>",
  "helper_sha256": "<workspace-helper-sha256>",
  "file_tool_sha256": "<file-tool-helper-sha256>",
  "launcher_sha256": "<launcher-sha256>",
  "opencode_sha256": "<opencode-sha256>",
  "context_limit": 128000,
  "output_limit": 16000,
  "lifetime_seconds": 3600,
  "master_key_file": "/private/kingdom-managed/master.key",
  "callback": {
    "listen": "0.0.0.0:9443",
    "origin": "https://worker-host.example:9443",
    "certificate_file": "/private/tls/worker.crt",
    "key_file": "/private/tls/worker.key",
    "ca_file": "/private/tls/ca.crt",
    "allowed_ips": ["192.0.2.10"]
  },
  "model": {
    "auth_kind": "api_key",
    "connection_id": "00000000-0000-4000-8000-000000000002",
    "api_key_file": "/private/kingdom-managed/model.key",
    "definition": {
      "name": "OpenAI",
      "provider": "openai",
      "endpoint": "https://api.openai.com/v1",
      "models": [{"id": "gpt-6-luna", "name": "GPT-6 Luna", "efforts": ["medium"]}]
    }
  }
}
```

The callback certificate must match its advertised hostname and configured CA. The gateway and worker must trust the relevant certificate chains. Do not disable TLS checks. `allowed_ips` lists the exact worker callback destinations. A model endpoint on a private network also requires explicit `model.allowed_private_addresses` CIDRs; `model.ca_file` can supply its private CA.

The local owner vault stores encrypted credential revisions. It does not read OpenCode accounts or grant central membership. Model credentials stay in the local proxy. Each sandbox receives a scoped capability that is revoked before export. Changes to model metadata or credentials create a new revision; already queued work retains its exact selection.

For a Kingdom-owned local ChatGPT registration, use the [ChatGPT connection guide](chatgpt-local-connection.md) and replace the model section with its registration reference. Personal ChatGPT credentials cannot become shared central credentials.

## Return to Local Execution

Wait for managed work and typed resource cleanup to finish, stop Kingdom, then set `execution.backend: local` and remove `execution.managed_config`. A restart refuses the switch if active managed work or unresolved remote resources remain. Keep the managed configuration available to complete that cleanup. Completed historical runs do not prevent the switch.

New work uses the selected backend. Existing run and planning records retain their backend. An old remote request cannot be sent to local OpenCode. Remote checkpoints do not become resumable local checkpoints after a switch.

Retain the private managed state directory and master key until all remote resources are closed. They contain the exact identities and encrypted requests needed for recovery. HTTP success alone does not prove a command stopped or that a workspace was safely exported.
