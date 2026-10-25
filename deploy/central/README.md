# Central Service Containers

The central image includes the Go service, web app, and pinned OpenCode 2.0.30
account owner. It does not need Go, Node, or a shell at runtime. It runs as
UID/GID `65532:65532`. The Compose example uses a read-only root filesystem,
drops Linux capabilities, and mounts a separate private state volume. It does
not mount the Docker socket.

Personal GitHub Copilot sign-in uses a temporary OpenCode process outside the
workload sandbox. Its private native account store is on the `/run/kingdom`
`tmpfs` mount. The mount has mode `0700`, owner `65532:65532`, and a size limit
of `128m`. It is not executable. Credentials retained by Kingdom are encrypted
in the durable central store. Native account files are removed only after the
owned process has stopped. Do not replace this mount with a durable shared
folder or a host account directory.

The managed gateway and worker run separately on the same Docker host. Follow
the [managed OpenShell build guide](../../experiments/openshell-managed-commands/README.md)
to prepare the required extension to stable OpenShell 0.1.5. Stock OpenShell
cannot satisfy the managed execution contract.

## Build

From the repository root:

```sh
docker build --platform linux/arm64 -f deploy/central/Dockerfile -t kingdom-central:local .
docker build --platform linux/arm64 -f deploy/central/Dockerfile.worker \
  -t kingdom-provider-worker:local .
```

The base images and pnpm distribution use exact digests. Go modules and web
dependencies use the committed lockfiles. The build context is an allowlist;
local workspaces, databases, account stores, and test evidence are excluded.
Dependency downloads are not a claim of byte-for-byte reproducible output.
Record the actual built image and executable digests for deployment.

The worker contains OpenCode 2.0.30, the Kingdom plugin, the fixed provider
launcher, and the confined file/workspace helpers. The central account owner
uses the same pinned OpenCode ELF and its three required musl runtime libraries.
The central image does not include the base image's shell or package manager.
The reviewed managed boundary and this OpenCode base are ARM64 artifacts. Both
builds reject other target architectures. Other architectures require matching
artifacts and their own live acceptance.

Use `kd openshell export` with an explicit Docker context, immutable gateway and
worker image references, and the reviewed profile to export the actual binary
digests. See the [deployment bundle procedure](../../docs/local-openshell.md#configure-an-existing-gateway).
Copy the matching worker and profile pins into `service.json`; the central
service still uses its own configuration. A binary built with different build
flags can have a different digest. Do not copy pins from a different image.

## Configure

Create `deploy/central/config/service.json` and the referenced private files.
This directory is excluded from Git. Follow the
[central configuration guide](../../docs/central-service.md). Use these paths
and listen settings inside the container:

```json
{
  "listen": "0.0.0.0:9090",
  "data_root": "/var/lib/kingdom",
  "master_key_file": "/etc/kingdom/secrets/master.key",
  "tls_certificate_file": "/etc/kingdom/tls/server.crt",
  "tls_key_file": "/etc/kingdom/tls/server.key",
  "personal_accounts": {
    "opencode_binary": "/usr/local/bin/opencode",
    "runtime_root": "/run/kingdom/accounts"
  }
}
```

The `personal_accounts` values shown are the container defaults. Keep the
runtime root on the private volatile mount. For a direct host launch, set an
absolute OpenCode V2 binary path and a private volatile runtime location.

Merge those fields into the complete version 1 configuration, including login,
initial owner, and worker pins. The master key must contain exactly 32 random
bytes. Secret files need mode `0600` and must be readable by UID `65532` inside
the container. Use the same owner for their private parent directories. The
image initializes a new state volume with owner `65532:65532` and mode `0700`.
An existing state directory with public permissions is rejected.

Set `public_origin` to the browser's exact HTTPS origin. Set `worker_origin`
when a workload needs another HTTPS name to reach the service. Resolve both
names and provide matching certificates. The gateway address must be reachable
from the central container. A container's `127.0.0.1` is not the host or another
container. The Compose port is bound to host loopback; use an explicit trusted
ingress for access from other computers.

## Start and Stop

```sh
docker compose -f deploy/central/compose.yaml up --build -d
docker compose -f deploy/central/compose.yaml logs -f central
docker compose -f deploy/central/compose.yaml stop
```

The service uses HTTPS on port `9090` in this example. It stops admission,
cancels streams, and joins project consumers before it releases shared state.
Compose allows the configured shutdown interval. A failed cleanup remains
durable recovery work; do not remove the state volume or force-delete native
sandboxes to clear it.

Back up the private state volume and master key as one protected deployment.
Keep unresolved invocation evidence and workspace exports. This single-host
package is not an active-active deployment; the capability registry enforces a
single process owner.

## Verification Scope

Check HTTPS startup, embedded portal routing, unauthenticated session denial,
restart with retained state, and clean exit under the Compose restrictions.
Also start the pinned private account process and check its model/integration
APIs under those restrictions. Check that its native store is on the private
volatile mount and is removed after process closure. These package checks do
not qualify real provider consent, a production deployment, or another
architecture. Worker images require live execution, confinement, closure,
and disposal checks with their exact built pins.
