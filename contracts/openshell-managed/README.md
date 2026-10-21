# Managed OpenShell Protocol

This is a Kingdom extension to NVIDIA OpenShell v0.1.5. It is not an upstream
stable OpenShell API. Stock OpenShell does not advertise this capability.
Kingdom must reject an unsupported deployment before command admission.

`kingdom_managed.proto` defines the authenticated public API.
`kingdom_managed_internal.proto` defines the gateway-created supervisor relay
and trusted boundary messages. Internal authority comes from gateway access
checks and the authenticated sandbox session, never from a public command.
`options.proto` is copied unchanged from NVIDIA OpenShell v0.1.5, commit
`bdb956f2cd65`, and retains its upstream license notices. Go uses the pinned
upstream SDK's generated authorization option definitions.

Generate the public Go client with `sh contracts/openshell-managed/generate-go.sh`.
Required versions are protoc 36.2, protoc-gen-go v1.36.11, and
protoc-gen-go-grpc v1.6.2. Internal relay types are generated in the OpenShell
build only. The protocol and client do not enable a product execution backend.

## Evidence Rules

Persist the command identity and intended request before dispatch. A timeout,
connection loss, or missing response leaves dispatch uncertain. Never create a
replacement command or fall back to the host to resolve that uncertainty.

A trusted terminal receipt proves the recorded command group closed. It does
not settle a Kingdom workflow. Success also requires complete input, retained
output that matches its manifest, no forced cleanup, and the canonical provider
request, tool admission, workspace, and settlement checks.

Known fields use protobuf numeric field order and ordered map keys. Unknown
fields are removed recursively before digest calculation. Receipt hashes clear
only `receipt_sha256`. Output hashes cover each canonical `OutputChunk` preceded
by its four-byte big-endian encoded length, in sequence order. An empty output
hash is SHA-256 of the empty byte string.

The effective specification binds explicit arguments, working directory,
environment overrides, fixed input, and limits. It does not hash inherited
runtime credentials. The runtime owner must bind those separately to the
invocation's immutable credential revision.

## Runtime Archives

Finalization closes admission and archives every retained command before native
lifecycle operations may remove the runtime. An unknown command or unverified
receipt keeps deletion blocked. The manifest orders commands by their boundary
request ID. Its digest is SHA-256 over the UTF-8 domain
`kingdom.openshell.runtime-archive.v1`, one zero byte, and the canonical manifest.
The archive ID is the lowercase hexadecimal digest. `archive-vector.json`
checks the Rust and Go encoders against the same receipt and manifest.

Kingdom must compare the archive with its complete durable command registry and
retain the verified workspace export before requesting finalization. Archive
reads use the immutable archive ID and do not require a live sandbox. They still
require gateway authentication and current archive access. Archival preserves
failed and incomplete execution facts; it never turns them into success.
