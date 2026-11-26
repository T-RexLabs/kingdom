# OpenShell Managed Execution Profile

This opt-in profile extends NVIDIA OpenShell v0.1.5, revision `bdb956f2cd65`. It is a Kingdom extension, not an upstream stable API. Stock OpenShell does not advertise the managed capability. The central service and explicit local OpenShell mode use this contract for worker execution, settlement, and workspace transfer. Controlled ARM64 checks cover planning and complete workflows; they do not qualify a production deployment or another architecture. Default local Kingdom does not require OpenShell.

Before extending this profile, check NVIDIA's latest stable release with `gh api repos/NVIDIA/OpenShell/releases/latest --jq .tag_name`. Compare it with `upstream_version` in `manifest.json`. Check the installed CLI separately with `openshell --version`. Port and verify against a newer stable release before continuing; do not use moving `latest` image tags.

## Source and Artifact Pins

`manifest.json` pins the upstream tree, patch, and reviewed resulting tree. `prepare.py` refuses altered source, an altered patch, or an existing output directory. The custom gateway, supervisor, and sandbox runtime must all come from that tree. Updating the installed CLI does not install this extension.

The deployment profile binds the gateway binary hash, supervisor image digest, sandbox runtime image digest, and runtime binary hash. The Docker driver checks these pins before admission, including the extracted runtime and actual created image identities. A workload cannot enable the capability by supplying a profile name. The boundary must run as PID 1 and validate its own executable before it advertises the capability.

## Admission and Closure

The public protocol is in [contracts/openshell-managed](../../contracts/openshell-managed/README.md). The gateway derives native user or operator identity, immutable workspace identity, and runtime identity. Unauthenticated local-development identity cannot use managed execution. Gateway-created authority crosses an authenticated internal supervisor relay; public callers cannot supply it.

The gateway retains command intent before its first dispatch. The boundary retains protected intent before native execution. Replays never repeat an uncertain start. All identities include gateway, workspace, subject, request, runtime generation, main process instance, and deployment profile. Command specifications bind explicit arguments, environment overrides, fixed input, workdir, and limits. Provider credentials require a separate immutable invocation binding.

The v2 capability supports a fixed `PROJECT_TOOL` scope: the project directory is writable, system paths are read-only, private runtime paths and `/proc` are unavailable, and the environment is fixed. It intersects the sandbox policy and requires enforced Landlock. Caller arguments cannot widen it. Project and temporary directory descriptors are opened without following links, checked for inode changes before and after rule preparation, and passed directly to Landlock. The temporary root and system parent must be root-owned.

The pinned OpenCode server, its configuration and plugin, and fixed transfer helpers are trusted components. Model-driven effects use `PROJECT_TOOL`; they cannot rename its parent directory. Imports must finish before provider startup, and exports require writer closure. This profile does not isolate a compromised trusted provider with full workspace authority from other workspace contents.

The boundary assigns process groups before applying the workload filter. Workloads cannot use `setsid` or `setpgid` to escape. The trusted owner holds the launch/reaper registry lock while it reaps the group and proves kernel closure. `/proc` absence is not closure evidence. Requested termination or forced descendant cleanup stays non-clean even when the leader exits zero.

Input completion, raw exit status, group closure, forced cleanup, and output completeness are separate receipt facts. Output is retained as sequenced, typed frames with a digest. Missing evidence remains unresolved. Neither transport closure nor a receipt alone settles a Kingdom workflow.

The managed journal is protected from workload reads and writes. Use strict Landlock, `filesystem.include_workdir=false`, and explicit workspace/scratch grants. The native executor and workload share an execution identity; file permissions alone do not protect journal data. Never put real user credentials in workload-readable files or environment variables.

Each runtime retains at most 64 commands. Each command accepts a control request below 1 MiB and retains at most 16 MiB of output in 4,096 chunks. Large workspace transfers require a separately bounded stream; they must not be copied into command arguments or an unbounded journal.

## Finalization and Recovery

Authenticated discovery registers exact runtime ownership and reserves native lifecycle retention before any command. Native stop, restart, delete, recovery, and controller cleanup must respect it. PostgreSQL uses the same advisory lock for admission and destructive lifecycle calls; SQLite uses a single gateway process.

Finalization moves retention from Active to Finalizing, which prevents new starts while preserving recovery. The gateway independently obtains every command's closed receipt and exact retained output. It validates the full framed digest before publishing immutable archive data. Only a verified, content-addressed manifest permits Released and native teardown. A failed transfer or unknown command leaves resumable Finalizing state. No caller-supplied boolean can release retention.

Manifest schema 2 binds the authenticated subject even for a runtime with no commands. `DeleteFinalizedRuntime` uses the retained stable sandbox ID under the native lifecycle lock, returns Pending while removal remains incomplete, and durably retains its exact archive-bound absence receipt before reporting Absent. Reusing the old sandbox name cannot redirect deletion.

Archive reads work after the sandbox is gone and still enforce native identity and access. Failed or incomplete results remain failed or incomplete. Kingdom must separately retain a verified workspace export before requesting finalization. Out-of-band host or container destruction cannot establish managed closure.

## Build

Requirements: ARM64 Linux or an ARM64 Docker VM, Python 3, Git, and Docker. The builder pins Rust 1.95.0 and Cargo dependencies. Package-manager downloads are not a claim of byte-for-byte reproducible binaries; retain and use the actual built artifact digests.

```sh
mkdir -p kd-sandbox/managed-build
python3 experiments/openshell-managed-commands/prepare.py \
  --source /path/to/exact/OpenShell-v0.1.5-source \
  --output "$PWD/kd-sandbox/managed-build/source"
docker build -f experiments/openshell-managed-commands/Dockerfile.builder \
  -t kingdom-openshell-managed-builder:local .
mkdir -p kd-sandbox/managed-build/bin
docker run --rm --cpus 2 --memory 5g --memory-swap 6g \
  --mount type=bind,src="$PWD/kd-sandbox/managed-build/source",dst=/src,readonly \
  --mount type=bind,src="$PWD/kd-sandbox/managed-build/bin",dst=/out \
  --mount type=bind,src="$PWD/experiments/openshell-managed-commands/build-binaries.sh",dst=/build-binaries.sh,readonly \
  --mount type=volume,src=kingdom-managed-cargo,dst=/usr/local/cargo/registry \
  --mount type=volume,src=kingdom-managed-target,dst=/target \
  kingdom-openshell-managed-builder:local sh /build-binaries.sh
docker build -f experiments/openshell-managed-commands/Dockerfile.gateway \
  -t kingdom-managed-gateway:local kd-sandbox/managed-build
docker build -f experiments/openshell-managed-commands/Dockerfile.supervisor \
  -t kingdom-managed-supervisor:local kd-sandbox/managed-build
docker build -f experiments/openshell-managed-commands/Dockerfile.runtime \
  -t kingdom-managed-runtime:local kd-sandbox/managed-build
```

Create the output parent before preparation. Use immutable `repository@sha256:...` image references from the resulting image store in the Docker driver and managed profile. The runtime uses ARM64 musl; gateway and supervisor use ARM64 GNU. The supervisor image includes the GNU support library required by the built binary; verify each image starts before sandbox acceptance. Test builds use GNU because the upstream musl test fixtures have an unrelated `msghdr` compile error.

Use a separate disposable gateway with TLS and native authenticated user/operator access. Do not enable unauthenticated development access. Do not attach personal credentials during protocol acceptance. Never use port 8080 for Kingdom verification.

## Verification Status

The v5 source passed 19 managed gateway tests, 341 native compute tests (three PostgreSQL tests require a disposable database), 21 managed backend tests, 18 default boundary tests, four scoped-tool tests, and 13 managed owner tests. The directory-substitution regression failed before descriptor pinning and passed after the fix, including a real child denied access after the root pathname changed. All three ARM64 binaries were rebuilt from the reproduced source tree. Gateway and supervisor images passed executable startup checks; the isolated TLS gateway passed its health check.

The earlier v4 source passed 15 managed gateway tests and 252 native lifecycle tests. Backend, Docker driver, default boundary, managed sandbox, and focused owner tests also passed. Two existing Docker-live driver tests were ignored. The gateway tests include actual native authentication routing, membership revocation, non-admin finalization denial, uncertain dispatch, interrupted archive recovery, and deletion after archive. Rust and Go agree on canonical request, receipt, output, and archive vectors.

Live ARM64 acceptance passed with the exact v4 images: mutual TLS operator authentication, single-effect command replay, changed-request rejection, typed timeout and cancellation, blocked native stop/delete before finalization, and verified archive reads and replay after native deletion. A separate live transfer passed a 2 MiB incompressible project through the authenticated fixed-port tunnel and strict Landlock, with identical imported/exported manifests and clean helper receipts. The native capability negotiation regression exercises the actual descriptor validator and runtime gRPC service.

Evidence is retained under `kd-sandbox/central-openshell-20261021-managed-live/acceptance-06`, `kd-sandbox/workspace-transfer/acceptance-02`, and the build directory `kd-sandbox/central-openshell-20261021-managed-protocol/`. The v5 rebuild and gateway startup checks passed; live scope/deletion verification is pending; prior evidence does not qualify the new profile. The manifest records image digests only after the matching build; these images are not published. These checks do not establish PostgreSQL concurrency, end-to-end hosted OpenCode execution, or production deployment readiness. The earlier native cancellation probe used an older profile and does not qualify v4.
