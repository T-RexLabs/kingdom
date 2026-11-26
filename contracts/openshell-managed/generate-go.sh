#!/bin/sh
# Generate only the public client. Internal relay messages stay in OpenShell.
set -eu
cd "$(dirname "$0")/../.."
[ "$(protoc --version)" = 'libprotoc 36.2' ] || { echo 'protoc 36.2 required' >&2; exit 1; }
[ "$(protoc-gen-go --version)" = 'protoc-gen-go v1.36.11' ] || { echo 'protoc-gen-go v1.36.11 required' >&2; exit 1; }
[ "$(protoc-gen-go-grpc --version)" = 'protoc-gen-go-grpc 1.6.2' ] || { echo 'protoc-gen-go-grpc v1.6.2 required' >&2; exit 1; }
protoc -I contracts/openshell-managed \
  --go_out=. --go_opt=module=kingdom \
  --go_opt=Moptions.proto=github.com/NVIDIA/OpenShell/sdk/go/proto/optionsv1 \
  --go-grpc_out=. --go-grpc_opt=module=kingdom \
  --go-grpc_opt=Moptions.proto=github.com/NVIDIA/OpenShell/sdk/go/proto/optionsv1 \
  contracts/openshell-managed/kingdom_managed.proto
