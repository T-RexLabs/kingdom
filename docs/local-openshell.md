# Optional Local OpenShell

Normal `kd init` and `kd summon` use local OpenCode. They do not require Docker, an OpenShell gateway, or managed credentials. Use the supported setup commands below to opt in. OpenShell is an explicit project setting:

```yaml
execution:
  backend: openshell
  managed_config: /absolute/private/path/operator.json
```

The operator file must be a regular file with mode `0600`. It cannot be a symlink. Paths in this file are absolute. Start Kingdom with `kd summon` as usual after you set up the gateway and the trusted worker image described in [Central Service](central-service.md). The browser remains on the local loopback address. The worker callback listener uses a separate HTTPS address that the sandbox can reach.

## Configure an Existing Gateway

This setup uses an existing reviewed ARM64 gateway and worker. It does not build,
install, provision, start, or stop a gateway. The operator first exports a private
version 1 deployment bundle from the exact local images. The Docker context is
mandatory; the command does not change the global context or pull images.

Create a mode `0600` environment file with these fields. All file paths must be
absolute, regular paths without symbolic links. Private keys must have mode
`0600`. Use actual worker-reachable callback addresses and matching certificates.

```json
{
  "gateway": {
    "id": "my-gateway",
    "address": "gateway.example:443",
    "ca_file": "/private/tls/ca.crt",
    "certificate_file": "/private/tls/operator.crt",
    "key_file": "/private/tls/operator.key"
  },
  "worker_id": "local-worker",
  "callback": {
    "listen": "0.0.0.0:9443",
    "origin": "https://worker-host.example:9443",
    "certificate_file": "/private/tls/worker.crt",
    "key_file": "/private/tls/worker.key",
    "ca_file": "/private/tls/ca.crt",
    "allowed_ips": ["192.0.2.10"]
  },
  "limits": {
    "context_limit": 128000,
    "output_limit": 16000,
    "lifetime_seconds": 3600
  }
}
```

Use the reviewed deployment profile JSON that matches the gateway, supervisor,
and runtime build. Export with immutable image references:

```sh
kd openshell export --docker-context CONTEXT \
  --gateway-image REPOSITORY@sha256:DIGEST \
  --worker-image REPOSITORY@sha256:DIGEST \
  --profile /private/profile.json \
  --environment /private/environment.json \
  --out /private/deployment.json
```

Export checks image identities and ARM64 Linux architecture. It reads fixed
binary paths through temporary stopped containers, verifies the gateway and
runtime hashes against the profile, and records the actual worker hashes.
Those containers are never started. Cleanup uses only their exact retained IDs.
The output has mode `0600`; it contains TLS file references, never private keys
or model credentials. Keep it private because it describes operator paths.

In a project initialized with `kd init`, stop the runtime and configure an API key:

```sh
kd openshell configure --bundle /private/deployment.json \
  --api-key-file /private/model.key \
  --model gpt-6-luna --thinking-effort medium
kd openshell check
kd summon
```

The common model form uses `openai` and `https://api.openai.com/v1`. The model and
thinking effort are explicit; setup does not discover or infer model support.
Use only a model and effort supported by the selected endpoint.

Configuration creates `.kingdom/managed/` with mode `0700`, a mode `0600`
hex-encoded master key, and a private operator file. It generates one local owner
and connection identity and updates only the project's execution settings after
validation. Use `--state-root /absolute/private/path` to select another existing
parent directory. Do not put model credentials in command arguments.

An exact retry preserves the master key, owner, and connection. Different inputs
are rejected instead of silently replacing a retained setup. If project-file
publication is interrupted, rerun the same command to finish it from the retained
private setup record. Configuration refuses an active runtime or unfinished work
that still requires its current backend. Do not remove the setup record or state
directory to bypass this check.

For another OpenAI-compatible endpoint or multiple models, use
`--model-profile /private/models.json` instead of `--model` and
`--thinking-effort`. This metadata file uses the existing definition shape:

```json
{
  "name": "Private Provider",
  "provider": "openai",
  "endpoint": "https://model.example/v1",
  "models": [{"id": "my-model", "name": "My Model", "efforts": ["medium"]}]
}
```

A private endpoint requires explicit trust configuration. Add
`--model-ca-file /private/model-ca.crt` when needed, and repeat
`--model-allowed-private-address CIDR` for its allowed private address ranges.
There are no implicit private-network allowances.

An existing Kingdom ChatGPT registration can be selected without a new sign-in:

```sh
kd openshell configure --bundle /private/deployment.json \
  --state-root /private/existing-chatgpt-state \
  --owner-id EXISTING_OWNER_UUID \
  --master-key-file /private/existing-master.key \
  --chatgpt-registration EXISTING_REGISTRATION_ID
```

The existing registration must be ready, owned by that local owner, and not
fenced for transfer. Setup reads its local metadata and key proof only. It does
not read the host OpenCode account store, refresh a token, or contact ChatGPT.
Use the [ChatGPT guide](chatgpt-local-connection.md) to create a registration first.

`check` verifies the retained bundle, private files, certificate validity, callback
hostname, and dedicated gateway client identity. It authenticates the gateway
with mutual TLS and calls only `Health` and `GetGatewayInfo`. Its JSON separates
`bundle_verified`, `gateway_authenticated`, and `docker_driver_available` from
`execution_attested`, which remains `false`. The existing metadata APIs cannot
attest remote profile or worker bytes. Normal managed allocation must still
verify those exact native pins before a prompt can run. A passing check does not
create a workspace, sandbox, provider process, or model request.

## Advanced Manual Operator File

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
