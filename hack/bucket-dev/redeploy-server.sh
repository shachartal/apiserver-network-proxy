#!/usr/bin/env bash
# Copyright 2025 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# redeploy-server.sh — Rebuild and redeploy bucket-proxy-server after code changes.
#
# This script:
#   1. Rebuilds the Go binary for the current architecture
#   2. Rebuilds the Docker image
#   3. Imports it into k3d
#   4. Restarts the kube-apiserver pod to pick up the new image
#
# Usage: ./redeploy-server.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

CLUSTER_NAME="bucket-dev"
NAMESPACE="overlay-system"

# Detect target architecture.
HOST_ARCH="$(uname -m)"
case "$HOST_ARCH" in
    x86_64)  GOARCH="amd64" ;;
    aarch64|arm64) GOARCH="arm64" ;;
    *) echo "ERROR: unsupported architecture $HOST_ARCH"; exit 1 ;;
esac

echo "==> Building bucket-proxy-server for linux/${GOARCH}..."
cd "$REPO_ROOT"
GOOS=linux GOARCH=$GOARCH go build -o "bin/bucket-proxy-server-linux-${GOARCH}" ./cmd/bucket-server

echo "==> Building Docker image..."
TMPIMG=$(mktemp -d)
cp "$REPO_ROOT/bin/bucket-proxy-server-linux-${GOARCH}" "$TMPIMG/bucket-proxy-server"
cat > "$TMPIMG/Dockerfile" <<'DOCKERFILE'
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY bucket-proxy-server /bucket-proxy-server
ENTRYPOINT ["/bucket-proxy-server"]
DOCKERFILE

docker build -t bucket-proxy-server:dev "$TMPIMG"
rm -rf "$TMPIMG"

echo "==> Importing image into k3d cluster..."
k3d image import bucket-proxy-server:dev -c "$CLUSTER_NAME"

echo "==> Restarting kube-apiserver pod..."
# Delete the pod; it will be recreated by the kubelet.
# Since it's not a Deployment, we need to re-apply the manifest.
kubectl -n "$NAMESPACE" delete pod kube-apiserver --wait=false 2>/dev/null || true

# Re-apply the manifest to recreate the pod.
kubectl apply -f "$SCRIPT_DIR/manifests/apiserver.yaml"

echo "==> Waiting for kube-apiserver to be ready..."
kubectl -n "$NAMESPACE" wait --for=condition=Ready pod/kube-apiserver --timeout=60s

echo ""
echo "==> Server redeployed successfully!"
echo ""
echo "Check logs with:"
echo "  kubectl -n $NAMESPACE logs kube-apiserver -c bucket-proxy-server -f"
