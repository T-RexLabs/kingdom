FROM --platform=$BUILDPLATFORM node:26.2.0-bookworm-slim@sha256:e89172f5e6154ba212269866bf3fbadbca8eb7901e10c0eccf08f2147bfae505 AS frontend
WORKDIR /src/web
ADD --checksum=sha256:08a3d2d539b377a6b7ea2469b612672255ca71c30a62698530582cb9d35c268f https://registry.npmjs.org/pnpm/-/pnpm-12.3.4.tgz /tmp/pnpm.tgz
RUN mkdir /opt/pnpm && tar -xzf /tmp/pnpm.tgz -C /opt/pnpm --strip-components=1 && rm /tmp/pnpm.tgz
COPY web/package.json web/pnpm-lock.yaml web/pnpm-workspace.yaml ./
RUN node /opt/pnpm/bin/pnpm.mjs install --frozen-lockfile
COPY web/ ./
COPY contracts/ /src/contracts/
RUN node /opt/pnpm/bin/pnpm.mjs build

FROM --platform=$BUILDPLATFORM golang:1.27.0-bookworm@sha256:ded31c68586d2e49e760acc2e65a884b23d032e9bbbed0ae0c55abd3fcaf4452 AS backend
WORKDIR /src
ENV GOTOOLCHAIN=local
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY cmd/ cmd/
COPY internal/ internal/
COPY contracts/ contracts/
COPY web/embed.go web/
COPY --from=frontend /src/web/dist/ web/dist/
ARG TARGETOS
ARG TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH GOMAXPROCS=2 go build -p=2 -trimpath -o /out/kingdom-central ./cmd/kingdom-central
RUN mkdir -p /out/state && chmod 0700 /out/state

# Keep the account owner on the exact OpenCode release used by managed workers.
# Copy only the ELF and its musl dependencies; the final image has no shell.
FROM ghcr.io/anomalyco/opencode@sha256:08481eba3f9e6dbcc4b4bee9e399cd7abe384ebee12b849f0223ff41923344e0 AS account-runtime
ARG TARGETARCH
RUN test "$TARGETARCH" = arm64 && \
    echo "5eb186fb0a84a6dc4df26dbc470ba31f5dd560c7f00ce6e0c6b91df01eb9dea4  /usr/local/bin/opencode" | sha256sum -c - && \
    mkdir -p /account-runtime && \
    cp -L /lib/ld-musl-aarch64.so.1 /account-runtime/ld-musl-aarch64.so.1 && \
    cp -L /usr/lib/libstdc++.so.6 /account-runtime/libstdc++.so.6 && \
    cp -L /usr/lib/libgcc_s.so.1 /account-runtime/libgcc_s.so.1

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=backend /out/kingdom-central /usr/local/bin/kingdom-central
COPY --from=account-runtime --chmod=0555 /usr/local/bin/opencode /usr/local/bin/opencode
COPY --from=account-runtime --chmod=0555 /account-runtime/ld-musl-aarch64.so.1 /lib/ld-musl-aarch64.so.1
COPY --from=account-runtime --chmod=0555 /account-runtime/libstdc++.so.6 /usr/lib/libstdc++.so.6
COPY --from=account-runtime --chmod=0555 /account-runtime/libgcc_s.so.1 /usr/lib/libgcc_s.so.1
# Preserve the private directory mode when Docker initializes a state volume.
COPY --from=backend --chown=65532:65532 --chmod=0700 /out/state /var/lib/kingdom
USER 65532:65532
EXPOSE 9090
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/kingdom-central"]
CMD ["--config", "/etc/kingdom/service.json"]
