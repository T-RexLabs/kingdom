# OpenShell Managed Command Prototype

This opt-in OpenShell runtime profile tests trusted command supervision for Kingdom. It is not connected to `kd summon`, workflow settlement, or a hosted service. Normal local execution does not require this profile or OpenShell.

The patch targets OpenShell v0.1.5, revision `bdb956f2cd65`. `manifest.json` pins the complete source tree, patch, and resulting tree. `prepare.py` refuses changed source, a changed patch, or an existing output directory.

Before extending this profile, check NVIDIA's latest stable release with `gh api repos/NVIDIA/OpenShell/releases/latest --jq .tag_name` and compare it with `upstream_version` in `manifest.json`. Check the installed CLI separately with `openshell --version`; its version can differ from the pinned SDK and runtime. If a newer stable release exists, port and verify the profile against that release before continuing. Keep exact source and image pins after the update; do not use a moving `latest` tag for verification.

## Execution Contract

The Cargo feature `kingdom-managed-commands` is off by default. A custom sandbox runtime binary must explicitly enable it. Native API cancellation also requires the patched gateway described below. The released network supervisor remains unchanged.

The trusted boundary assigns each command a process group before applying the workload filter. The profile then denies workload `setsid` and `setpgid`, including in the main workload. Commands cannot move themselves or their children out of their assigned group. The production profile requires the boundary to run as PID 1 in its private PID namespace.

After the command leader exits, the boundary holds the shared launch/reaper registry lock, reaps the leader, and uses `waitpid` with `__WALL` to account for adopted children in that group. Other trusted launches cannot reuse the group during this operation. The kernel's `ECHILD` result establishes that no child chain remains. A scan of `/proc` is not used as proof of absence.

Surviving children trigger forced cleanup and a non-clean result. Requested termination also produces a non-clean result, including when a command catches the signal and exits zero. Incomplete observations and cleanup deadlines remain errors. These checks do not change Kingdom's success rules.

The patched gateway keeps the SSH operation alive after a native execution timeout so it can send `KILL` and drain the remote channel. Client response-stream cancellation uses the same cleanup path. Cleanup covers blocked input and output, has a fixed deadline, and does not depend on the client reading more output. A synthetic timeout still leaves the gateway's durable request unresolved, even if cleanup receives exit zero. SSH channel closure alone does not prove that the process group is closed.

## Durable Admission

The boundary writes and synchronizes a logical admission record before invoking the executor. The record binds the request ID, payload digest, sandbox ID, runtime generation, and logical process ID. The journal is under `/.openshell/channel/sandbox/kingdom-exec-journal`, on the protected channel volume. The mandatory Landlock baseline excludes that path from workload access. File permissions alone would not be sufficient because the boundary and workload use the same UID.

Records use exclusive creation and file/directory synchronization. A repeated request with lost in-memory state cannot launch again. A conflicting payload is rejected. Clean terminal records are bound to the original admission and synchronized before reporting clean completion. Wait and storage failures remain unresolved.

This is a logical admission gate, not an acknowledgement gate for the exact child PID. A crash between admission and retained terminal evidence requires whole-sandbox shutdown before replacement. The journal survives boundary restarts while its channel volume remains; it does not survive sandbox deletion. There is no authenticated journal export or output-stream replay API in this prototype.

## Build and Test

Requirements: Docker on Linux ARM64 or a macOS ARM64 Docker VM, Python 3, and Git. The builder uses Rust 1.95.0 and locked Cargo dependencies. Its base image is pinned, but package-manager downloads are not a claim of byte-for-byte reproducible builds. Pin the resulting image digest when using it for a test.

From the Kingdom repository root, prepare an unmodified OpenShell v0.1.5 source checkout or extracted archive:

```sh
python3 experiments/openshell-managed-commands/prepare.py \
  --source /path/to/unmodified/openshell \
  --output /tmp/kingdom-openshell-managed-source
docker build -f experiments/openshell-managed-commands/Dockerfile.builder \
  -t kingdom-openshell-managed-builder:local .
mkdir -p kd-sandbox/openshell-managed-build/bin
docker run --rm \
  --mount type=bind,src=/tmp/kingdom-openshell-managed-source,dst=/src,readonly \
  --mount type=bind,src="$PWD/kd-sandbox/openshell-managed-build/bin",dst=/out \
  --mount type=volume,src=kingdom-openshell-managed-target,dst=/target \
  kingdom-openshell-managed-builder:local sh -c '
    cargo test --locked -p openshell-sandbox --features kingdom-managed-commands --lib kingdom_ -- --test-threads=1 &&
    cargo test --locked -p openshell-sandbox --features kingdom-managed-commands --lib boundary_exec::tests::managed_ -- --test-threads=1 &&
    cargo build --locked -p openshell-sandbox --features kingdom-managed-commands --bin openshell-sandbox --target aarch64-unknown-linux-musl &&
    cp /target/aarch64-unknown-linux-musl/debug/openshell-sandbox /out/openshell-sandbox'
docker build -f experiments/openshell-managed-commands/Dockerfile.runtime \
  -t kingdom-openshell-managed:local kd-sandbox/openshell-managed-build
```

The tests use the GNU target. The deployable binary uses musl. Upstream v0.1.5 has an unrelated musl test-build error in its `msghdr` test fixtures; the production binary builds with musl.

Configure a separate disposable gateway with `sandbox_runtime_image = "kingdom-openshell-managed:local"` in its Docker driver configuration. This replaces the trusted sandbox runtime, not the outer network supervisor. Use strict Landlock, `include_workdir: false`, and explicit workspace write grants. Do not attach provider credentials for these tests. Use non-PTY commands and `--no-login-shell`.

For native API cancellation, also build the patched gateway. The GNU build needs Z3, included in the builder above. Use Rust's bundled ELF linker to reduce peak memory during gateway test linking:

```sh
docker run --rm \
  --mount type=bind,src=/tmp/kingdom-openshell-managed-source,dst=/src,readonly \
  --mount type=bind,src="$PWD/kd-sandbox/openshell-managed-build/bin",dst=/out \
  --mount type=volume,src=kingdom-openshell-managed-target,dst=/target \
  kingdom-openshell-managed-builder:local sh -c '
    export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=clang
    export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_RUSTFLAGS="-C link-arg=-fuse-ld=$(rustc --print sysroot)/lib/rustlib/aarch64-unknown-linux-gnu/bin/gcc-ld/ld.lld"
    cargo test --locked -p openshell-server --lib interactive_exec_tests &&
    cargo test --locked -p openshell-server --lib mutation_replay::streaming::tests &&
    cargo build --locked -p openshell-gateway --bin openshell-gateway --no-default-features --features compute-driver-docker,telemetry &&
    cp /target/debug/openshell-gateway /out/openshell-gateway &&
    cp /usr/lib/aarch64-linux-gnu/libz3.so.4 /out/libz3.so.4'
docker build -f experiments/openshell-managed-commands/Dockerfile.gateway \
  -t kingdom-openshell-gateway:local kd-sandbox/openshell-managed-build
```

Use this gateway image for the disposable gateway service. This is a custom Docker-only build from stable v0.1.5 source, not an upstream release binary. The existing local CLI installation is separate; upgrading it does not install this patch.

## Limits Before Kingdom Integration

- The profile can reject job control, daemon startup, and tools that create sessions or groups. General support for those tools needs a stronger per-command boundary, such as delegated cgroups.
- The production proof depends on private PID-namespace ownership and on all trusted group creation using the same registry lock. Unit tests use isolated subreaper helpers; those tests do not establish arbitrary shared-namespace safety.
- Forced cleanup currently returns a non-clean native result and leaves the durable request unresolved. Kingdom needs a typed outcome that separates proven shutdown, forced termination, ordinary exit status, and missing proof.
- Authenticated evidence retrieval, exact runtime identity binding in Kingdom, workspace export after shutdown, provider credentials, and end-to-end workflow recovery are not implemented here.
- The boundary request ID differs from the public gateway request UUID in the tested transport. The next protocol change must carry that binding through to the retained evidence. Gateway duplicate rejection alone does not prove permanent journal deduplication.
- The released v0.1.5 gateway and CLI can return 124 while a command remains live. The custom gateway adds bounded termination for native API timeout and client cancellation. The CLI uses a separate SSH path and is not fixed by this gateway patch. A timeout or disconnected stream still requires confirmed whole-sandbox shutdown before replacement; it is not a command shutdown receipt.
- Do not accept the raw native exit status as Kingdom success or treat a missing journal as cleanup evidence.

## Verification Result

On 2026-10-21, the Linux ARM64 prototype passed 13 focused Rust tests and real OpenShell checks for ordinary completion, leaked-child removal, group escape denial, and journal access denial. A forced boundary crash and gateway restart retained the journal byte-for-byte and did not produce a terminal record for interrupted work. A native API probe confirmed that repeating a completed request UUID did not launch it again.

The initial timeout checks exposed the released gateway limitation above. Supported sandbox stop then left the workload container stopped with no running PID. Test containers, sandbox volumes, and gateway networks were removed.

The v2 gateway change first reproduced missing remote termination with a failing SSH test. After the fix, 12 SSH transport tests and 11 mutation-replay tests passed. Live native timeout and client cancellation then left no command or child process in independent trusted inspection. Their journal admissions remained unresolved, with no clean terminal records. The gateway binary and Z3 library hashes are recorded in `manifest.json`. These results qualify this experiment only; they do not establish a hosted Kingdom release or end-to-end provider execution.

The native probe can be repeated against an existing disposable sandbox with `/sandbox` writable:

```sh
go run ./experiments/openshell-managed-commands/nativeprobe \
  --endpoint 127.0.0.1:59543 --sandbox kd-managed-proof
```

It writes a unique marker, starts a command with a short execution timeout, and cancels another command after its readiness message. Inspect the trusted process state and journal afterwards, then stop and delete the disposable sandbox. The probe's exit status does not certify remote cleanup. Boundary evidence is under `kd-sandbox/central-openshell-20261021-supervisor/evidence/`; gateway cancellation evidence is under `kd-sandbox/central-openshell-20261021-cancellation/evidence/`.

## Source and License

This package contains a patch to [NVIDIA OpenShell v0.1.5](https://github.com/NVIDIA/OpenShell/tree/v0.1.5). Upstream notices are retained in the patched source. The patch and added Rust modules use Apache-2.0; the license is included in `LICENSE-APACHE-2.0`. This is an experimental Kingdom integration, not an NVIDIA-supported release.
