# Central Service

`kingdom-central` is the separate entrypoint for shared projects. It serves the
login page, project portal, project APIs, and invocation-scoped worker routes.
The local `kd init` and `kd summon` commands do not require this service,
Docker, or OpenShell.

The current deployment target is one team on one host. Each project has a
private database, workspace, and runtime. Authentication, the encrypted model
connection store, and the worker capability registry belong to the central
process. Project access is checked on each request. Project runtimes are shared
between authorized users, but user identity remains request-scoped.

## Build and Start

```sh
make build-central
bin/kingdom-central --config /etc/kingdom/service.json
```

Use `make install-central` to install the command with Go. The web app is embedded
in the command. A separate web development server is not required.
For the container build and single-host Compose example, see the
[container guide](../deploy/central/README.md).

The service requires HTTPS, a 32-byte master key, and at least one login
provider. The master key is raw binary, not hexadecimal text. Keep a protected
backup of the key with the state backup. A lost key makes stored credentials
unreadable. A different key cannot replace an existing vault.

Secret files must be regular files with no group or other access. Symbolic links
are rejected. The state directory must have mode `0700`. Relative paths in the
configuration are resolved from the configuration file directory.

## Configuration

The following example shows the required fields. Replace each value in angle
brackets with the actual deployment value. Use immutable image and binary pins
from the matching build; do not use moving image tags.

```json
{
  "version": 1,
  "listen": "127.0.0.1:9090",
  "public_origin": "https://kingdom.example",
  "data_root": "/var/lib/kingdom",
  "master_key_file": "secrets/master.key",
  "tls_certificate_file": "tls/server.crt",
  "tls_key_file": "tls/server.key",
  "initial_owners": [
    {"issuer": "https://github.com", "subject": "<numeric GitHub user ID>"}
  ],
  "github": {
    "client_id": "<OAuth App client ID>",
    "client_secret_file": "secrets/github-client"
  },
  "openshell": {
    "address": "gateway.example:8443",
    "gateway_id": "<gateway identity>",
    "worker_id": "kingdom-central",
    "ca_file": "tls/gateway-ca.crt",
    "certificate_file": "tls/operator.crt",
    "key_file": "tls/operator.key",
    "worker_image": "<worker repository>@sha256:<image digest>",
    "profile_sha256": "<deployment profile digest>",
    "launcher_sha256": "<launcher digest>",
    "opencode_sha256": "<OpenCode digest>",
    "workspace_helper_sha256": "<workspace helper digest>",
    "file_tool_sha256": "<file tool digest>",
    "server_lifetime": "1h",
    "context_limit": 100000,
    "output_limit": 4096
  }
}
```

Register `https://kingdom.example/auth/github/callback` as the GitHub OAuth App
callback. Login requests no repository access. Use the stable numeric GitHub
user ID for an initial owner, not a login name or email address.

For OpenID Connect, replace or supplement `github` with:

```json
{
  "oidc": {
    "issuer": "https://identity.example",
    "client_id": "kingdom",
    "client_secret_file": "secrets/oidc-client",
    "additional_endpoint_origins": []
  },
  "initial_owners": [
    {"issuer": "https://identity.example", "subject": "<stable subject>"}
  ]
}
```

Register `/auth/oidc/callback` on the public origin. The provider must advertise
PKCE S256 and a supported confidential client authentication method. Token and
key endpoints must use the issuer origin or an explicitly allowed additional
origin. Login verifies the issuer, signature, audience, nonce, expiry, and the
browser-bound transaction. Email addresses do not grant membership.

`worker_origin` optionally sets a separate HTTPS origin for worker callbacks. It
defaults to `public_origin`. Use this when a Docker VM reaches the host through
a different DNS name from the browser. Both origins need valid certificates.
`openshell.allowed_service_ips` can pin the private host addresses that the
workload may contact. The worker routes use invocation capabilities; browser
session cookies cannot grant worker authority.

## OpenShell Profile

The managed worker uses the Kingdom extension to OpenShell **0.1.5**, with
OpenCode **2.0.30**. Stock OpenShell does not provide the managed execution
contract. Installing a newer CLI does not install the gateway extension. See
the [managed profile build guide](../experiments/openshell-managed-commands/README.md)
for the pinned source, images, and verification requirements.

The service and worker must use the same build. The provider launcher uses job protocol version 2 to pin the model API. Drain active work and finish typed resource cleanup before changing those pins; retained launch requests are never rewritten for a new worker.

The service refuses a missing or incompatible managed profile. It does not
fall back to host execution. The worker receives scoped proxy tokens, not user
API keys. Imports must be verified before provider startup. Success requires
verified tool closure, a clean provider stop, closed capabilities, a sealed
export, and verified output application. Sandbox disposal remains separate
durable work after settlement.

## Model Connections

Users can add personal API keys. A team owner can add team keys. The portal
does not store keys in browser storage or return them after creation. Replacing
a key creates an immutable revision. Planning and runs retain the exact revision
they selected, and dispatch checks current access again.

The default endpoint policy allows `https://api.openai.com`. Add custom HTTPS
origins with `model_origins`; private addresses require explicit CIDR allowances.
Use `provider_ca_file` to append a private CA to the system trust store for model
requests. Custom providers also require exact `model_profiles`; Kingdom does
not infer thinking effort from a model name. See the
[model catalog contract](central-model-catalog.md).

## Initial Project Files

Create a project, then select it in Initial Project Files before you open it.
Add a ZIP archive, one file, or an operator-approved GitHub repository. Opening a project permanently closes initial
uploads. See [initial project files](central-project-input.md) for limits and
retry rules.

Explicit GitHub result publication uses a separate repository App and per-project grant. It sends verified file changes from a successful run to an operation-specific branch and pull request. See [repository setup and publication](central-repositories.md).

## Shutdown and Recovery

Send SIGINT or SIGTERM for an orderly stop. The service stops new requests,
cancels streams, and joins project work before it closes shared credentials and
databases. Unresolved cleanup returns an error and retains the durable state
for recovery. Do not delete a sandbox or project directory to bypass a pending
closure check.

OpenShell execution does not support Resume. Saved checkpoint details remain
visible. Use Rerun to start fresh work.

Controlled checks cover hosted planning and workflows, file and ZIP inputs,
container startup and restart, and optional local OpenShell execution. Repository
protocol tests cover import, result deltas, and uncertain publication recovery.
These checks use synthetic providers; they do not qualify real account consent,
a live repository App, or a production deployment. The optional local
[ChatGPT connection](chatgpt-local-connection.md) remains a local-owner preview.
