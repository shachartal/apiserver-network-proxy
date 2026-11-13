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

# teardown-control-plane.sh — Tear down the overlay control plane.
#
# k3d: deletes the k3d cluster and the local Docker image.
# gke: stops the port-forward and deletes the overlay-system namespace. The GKE
#      cluster and Artifact Registry images are left in place (Terraform owns
#      them; `terraform destroy` in hack/terraform/gke-demo removes them).
# Both: remove local staging/PKI directories.
#
# Worker VMs should be torn down first (via teardown-worker.sh or teardown.sh).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Auto-source demo.env if present.
if [ -f "$SCRIPT_DIR/demo.env" ]; then
    set -a
    source "$SCRIPT_DIR/demo.env"
    set +a
fi

source "$SCRIPT_DIR/lib-underlay.sh"

BUCKET_DIR="/tmp/bucket-dev"
PKI_DIR="/tmp/bucket-dev-pki"

log() { echo "==> $*"; }

if is_gke; then
    log "Stopping overlay apiserver port-forward"
    stop_overlay_port_forward

    log "Deleting namespace $NAMESPACE on $UNDERLAY_CONTEXT"
    ukubectl delete namespace "$NAMESPACE" --ignore-not-found --wait=true || true
else
    log "Deleting k3d cluster: $K3D_CLUSTER_NAME"
    k3d cluster delete "$K3D_CLUSTER_NAME" 2>/dev/null || true

    log "Removing bucket-proxy-server Docker image"
    docker rmi "$SERVER_IMAGE" 2>/dev/null || true
fi

log "Removing bucket directory: $BUCKET_DIR"
rm -rf "$BUCKET_DIR" 2>/dev/null || true

log "Removing PKI directory: $PKI_DIR"
rm -rf "$PKI_DIR"

log "Control plane teardown complete."
