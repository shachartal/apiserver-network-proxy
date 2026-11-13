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
#   1. Rebuilds the Go binary for the underlay's architecture
#   2. Rebuilds the container image
#   3. k3d: imports it into k3d; gke: pushes it to Artifact Registry
#   4. Rolls out the kube-apiserver Deployment with the new image
#
# Usage: ./redeploy-server.sh            (UNDERLAY=k3d|gke, see lib-underlay.sh)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Auto-source demo.env if present.
if [ -f "$SCRIPT_DIR/demo.env" ]; then
    set -a
    source "$SCRIPT_DIR/demo.env"
    set +a
fi

GCS_BUCKET="${GCS_BUCKET:-}"
GCS_PREFIX="${GCS_PREFIX:-bucket-dev/}"

source "$SCRIPT_DIR/lib-underlay.sh"

if [ -z "$GCS_BUCKET" ]; then
    echo "ERROR: GCS_BUCKET must be set (or use demo.env)"
    exit 1
fi
underlay_check_gke_vars

echo "==> Building bucket-proxy-server for linux/${SERVER_GOARCH}..."
cd "$REPO_ROOT"
GOOS=linux GOARCH=$SERVER_GOARCH go build -o "bin/bucket-proxy-server-linux-${SERVER_GOARCH}" ./cmd/bucket-server

if is_gke; then
    echo "==> Building and pushing image to Artifact Registry..."
    gke_ensure_context
    ar_login
    push_server_image_gke
else
    echo "==> Building Docker image..."
    build_server_image "$SERVER_GOARCH" "$SERVER_IMAGE"

    echo "==> Importing image into k3d cluster..."
    k3d image import "$SERVER_IMAGE" -c "$K3D_CLUSTER_NAME"
fi

echo "==> Rolling out kube-apiserver..."
apply_manifest apiserver
if ! is_gke; then
    # k3d reuses the tag bucket-proxy-server:dev, so the pod template is
    # unchanged and apply alone would not roll. (GKE deploys by digest, which
    # changes the template and triggers a rollout.)
    ukubectl -n "$NAMESPACE" rollout restart deployment/kube-apiserver
fi

echo "==> Waiting for kube-apiserver to be ready..."
ukubectl -n "$NAMESPACE" rollout status deployment/kube-apiserver --timeout="$APISERVER_WAIT_TIMEOUT"

# The port-forward was bound to the old pod; restart it.
start_overlay_port_forward

echo ""
echo "==> Server redeployed successfully!"
echo ""
echo "Check logs with:"
echo "  kubectl --context $UNDERLAY_CONTEXT -n $NAMESPACE logs deploy/kube-apiserver -c bucket-proxy-server -f"
