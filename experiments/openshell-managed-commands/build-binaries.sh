#!/bin/sh
# Run inside Dockerfile.builder with reviewed source mounted at /src.
set -eu
cd /src
export CARGO_TARGET_DIR=/target
export CARGO_BUILD_JOBS=1
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_DEV_DEBUG=0
linker=/usr/local/rustup/toolchains/1.95.0-aarch64-unknown-linux-gnu/lib/rustlib/aarch64-unknown-linux-gnu/bin/gcc-ld/ld.lld
cargo rustc --locked -p openshell-gateway --bin openshell-gateway --no-default-features --features compute-driver-docker,telemetry -- -C codegen-units=256 -C linker=clang -C link-arg=-fuse-ld="$linker"
cargo rustc --locked -p openshell-supervisor --bin openshell-supervisor -- -C codegen-units=256 -C linker=clang -C link-arg=-fuse-ld="$linker"
cargo build --locked -p openshell-sandbox --bin openshell-sandbox --features kingdom-managed-commands --target aarch64-unknown-linux-musl
mkdir -p /out
cp /target/debug/openshell-gateway /out/openshell-gateway
cp /target/debug/openshell-supervisor /out/openshell-supervisor
cp /target/aarch64-unknown-linux-musl/debug/openshell-sandbox /out/openshell-sandbox
cp /usr/lib/aarch64-linux-gnu/libz3.so.4 /out/libz3.so.4
cp /lib/aarch64-linux-gnu/libgcc_s.so.1 /out/libgcc_s.so.1
sha256sum /out/openshell-gateway /out/openshell-supervisor /out/openshell-sandbox /out/libz3.so.4 /out/libgcc_s.so.1
