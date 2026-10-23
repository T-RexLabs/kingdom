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

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=backend /out/kingdom-central /usr/local/bin/kingdom-central
# Preserve the private directory mode when Docker initializes a state volume.
COPY --from=backend --chown=65532:65532 --chmod=0700 /out/state /var/lib/kingdom
USER 65532:65532
EXPOSE 9090
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/kingdom-central"]
CMD ["--config", "/etc/kingdom/service.json"]
