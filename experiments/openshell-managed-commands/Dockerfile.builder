FROM rust@sha256:cf9dd0ec73e75f827fe59123fff9dc65af1a1c8363c3c31ee8d7f8ad0b6a5fb2
RUN apt-get update && apt-get install -y --no-install-recommends cmake clang libclang-dev pkg-config libssl-dev git && rm -rf /var/lib/apt/lists/*
WORKDIR /src
ENV CARGO_TARGET_DIR=/target CARGO_BUILD_JOBS=2 CARGO_PROFILE_DEV_DEBUG=0
RUN apt-get update && apt-get install -y --no-install-recommends musl-tools && rm -rf /var/lib/apt/lists/* && rustup target add aarch64-unknown-linux-musl
ENV CC_aarch64_unknown_linux_musl=musl-gcc CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=musl-gcc
RUN rustup toolchain install 1.95.0 --profile minimal --target aarch64-unknown-linux-musl
ENV RUSTUP_TOOLCHAIN=1.95.0
