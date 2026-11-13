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

# setup-control-plane.sh — Set up the overlay control plane (underlay + PKI + pods).
#
# This prepares the underlay cluster, generates control-plane PKI, uploads the
# CA cert to GCS, builds the bucket-proxy-server image, and deploys the overlay
# control plane pods (etcd, apiserver, controller-manager, scheduler).
#
# The underlay is selected by UNDERLAY (see lib-underlay.sh):
#   k3d (default) — creates a local k3d cluster
#   gke           — uses the existing GKE cluster from hack/terraform/gke-demo;
#                   mirrors registry.k8s.io images and pushes the server image
#                   to Artifact Registry; GCS via Workload Identity
#
# Run this once. Then use setup-worker.sh to add worker VMs.
#
# Usage:
#   ./setup-control-plane.sh                 full setup
#   ./setup-control-plane.sh --port-forward  only (re)start the local forward
#                                            to the overlay apiserver and check
#                                            it (UNDERLAY=gke; on k3d the port
#                                            is a host-mapped NodePort)
#
# Prerequisites:
#   - docker, kubectl, openssl, crane, gcloud; k3d for UNDERLAY=k3d
#   - Run 'make build-bucket-linux' first to build the linux server binary
#   - Run './upload-distributables.sh' once to upload kubelet, containerd, etc. to GCS
#
# Environment variables:
#   GCS_CREDENTIALS_FILE — path to GCS credentials JSON file (required; used
#                          locally by gcloud, and mounted into the pod on k3d)
#   GCS_BUCKET           — GCS bucket name (required)
#   GCS_PREFIX           — key prefix within the bucket (default: "bucket-dev/")
#   UNDERLAY             — "k3d" (default) or "gke"
#   GCP_PROJECT, GKE_AR_REPO, GKE_CLUSTER, GKE_REGION — UNDERLAY=gke, see lib-underlay.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Auto-source demo.env if present.
if [ -f "$SCRIPT_DIR/demo.env" ]; then
    set -a
    source "$SCRIPT_DIR/demo.env"
    set +a
fi

BUCKET_DIR="/tmp/bucket-dev"
PKI_DIR="/tmp/bucket-dev-pki"
GCS_CREDENTIALS_FILE="${GCS_CREDENTIALS_FILE:-}"
GCS_BUCKET="${GCS_BUCKET:-}"
GCS_PREFIX="${GCS_PREFIX:-bucket-dev/}"

source "$SCRIPT_DIR/lib-underlay.sh"

log() { echo ""; echo "===== $* ====="; echo ""; }

# Wait for the overlay apiserver to answer /healthz on the local port.
verify_overlay_apiserver() {
    echo "Testing overlay apiserver at https://127.0.0.1:${OVERLAY_LOCAL_PORT}..."
    for i in $(seq 1 10); do
        if kubectl --kubeconfig="$PKI_DIR/admin.kubeconfig" get --raw /healthz 2>/dev/null; then
            echo ""
            echo "Overlay apiserver is healthy!"
            return 0
        fi
        echo "  Waiting for apiserver to be reachable... ($i/10)"
        sleep 3
    done
    echo "WARNING: overlay apiserver not reachable at 127.0.0.1:${OVERLAY_LOCAL_PORT}"
    return 1
}

# --port-forward: re-establish local access to an existing overlay and exit.
if [ "${1:-}" = "--port-forward" ]; then
    if [ ! -f "$PKI_DIR/admin.kubeconfig" ]; then
        echo "ERROR: $PKI_DIR/admin.kubeconfig not found; run a full setup first."
        exit 1
    fi
    if is_gke; then
        log "Restarting overlay apiserver port-forward"
        gke_ensure_context
        start_overlay_port_forward
    else
        echo "UNDERLAY=k3d: port ${OVERLAY_LOCAL_PORT} is a k3d host mapping, no forward needed."
        echo "If unreachable, check the cluster is running: k3d cluster start ${K3D_CLUSTER_NAME}"
    fi
    verify_overlay_apiserver
    exit $?
elif [ $# -gt 0 ]; then
    echo "Usage: $0 [--port-forward]"
    exit 1
fi

# ============================================================
# 0. Preflight checks
# ============================================================
log "Preflight checks (underlay: $UNDERLAY)"

REQUIRED_CMDS="docker kubectl openssl crane gcloud"
is_gke || REQUIRED_CMDS="$REQUIRED_CMDS k3d"
for cmd in $REQUIRED_CMDS; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "ERROR: $cmd is required but not found in PATH"
        exit 1
    fi
done

underlay_check_gke_vars

if [ ! -f "$REPO_ROOT/bin/bucket-proxy-server-linux-${SERVER_GOARCH}" ]; then
    echo "ERROR: Server binary not found (bin/bucket-proxy-server-linux-${SERVER_GOARCH})."
    echo "Run 'make build-bucket-linux' first."
    exit 1
fi

if [ -z "$GCS_CREDENTIALS_FILE" ] || [ ! -f "$GCS_CREDENTIALS_FILE" ]; then
    echo "ERROR: GCS_CREDENTIALS_FILE must be set to a valid credentials file path"
    exit 1
fi

if [ -z "$GCS_BUCKET" ]; then
    echo "ERROR: GCS_BUCKET must be set to a GCS bucket name"
    exit 1
fi

# ============================================================
# 1. Create local staging directory
# ============================================================
log "Creating staging directory"

mkdir -p "$BUCKET_DIR"/{control-to-node,node-to-control}
mkdir -p "$PKI_DIR"

# ============================================================
# 2. Check that distributables exist in GCS
# ============================================================
log "Checking distributables in GCS"

# Use the GCS credentials file only for bucket operations. Not exported:
# on UNDERLAY=gke, gcloud must keep the user's own credentials for
# get-credentials, Artifact Registry and gke-gcloud-auth-plugin.
gcs() { CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE="$GCS_CREDENTIALS_FILE" gcloud storage "$@"; }
GCS_BASE="gs://${GCS_BUCKET}/${GCS_PREFIX}"

SENTINEL="${GCS_BASE}distributables/.uploaded"
if ! gcs ls "$SENTINEL" &>/dev/null; then
    echo "ERROR: Distributables not found in GCS."
    echo "Run ./upload-distributables.sh first to upload kubelet, containerd, etc."
    exit 1
fi
echo "Distributables found in GCS."

# ============================================================
# 3. Generate control-plane PKI
# ============================================================
log "Generating control-plane PKI"

"$SCRIPT_DIR/generate-pki.sh" "$PKI_DIR"

# ============================================================
# 4. Upload CA cert to GCS
# ============================================================
log "Uploading CA cert to GCS"

mkdir -p "$BUCKET_DIR/pki"
cp "$PKI_DIR/ca.crt" "$BUCKET_DIR/pki/ca.crt"
gcs cp "$BUCKET_DIR/pki/ca.crt" "${GCS_BASE}pki/ca.crt"

echo "CA cert upload complete."

# ============================================================
# 5. Prepare underlay cluster
# ============================================================
if is_gke; then
    log "Using GKE cluster: $GKE_CLUSTER ($GKE_REGION)"

    gke_ensure_context
    # Same as the k3d path: later scripts (show-costs.sh, benchmark-costs.sh)
    # use the current context to reach the overlay-system namespace.
    kubectl config use-context "$UNDERLAY_CONTEXT"

    # Start from a clean overlay (equivalent to recreating the k3d cluster).
    # This regenerates the PKI below, so old etcd data (in the PVC, deleted
    # with the namespace) would not match the new certs and service-account keys.
    ukubectl delete namespace "$NAMESPACE" --ignore-not-found --wait=true
else
    log "Creating k3d cluster: $K3D_CLUSTER_NAME"

    # Delete existing cluster if present.
    k3d cluster delete "$K3D_CLUSTER_NAME" 2>/dev/null || true

    k3d cluster create "$K3D_CLUSTER_NAME" \
        --k3s-arg "--disable=traefik@server:0" \
        --k3s-arg "--disable=servicelb@server:0" \
        --k3s-arg "--disable=metrics-server@server:0" \
        --port "${OVERLAY_LOCAL_PORT}:30443@server:0"

    # Wait for k3d to be ready.
    kubectl config use-context "$UNDERLAY_CONTEXT"
    ukubectl wait --for=condition=Ready node --all --timeout=60s
fi

# ============================================================
# 6. Build and publish container images
# ============================================================
if is_gke; then
    log "Publishing images to Artifact Registry: $GKE_AR_REPO"

    ar_login
    mirror_control_plane_images
    push_server_image_gke
else
    log "Building bucket-proxy-server container image"

    build_server_image "$SERVER_GOARCH" "$SERVER_IMAGE"
    k3d image import "$SERVER_IMAGE" -c "$K3D_CLUSTER_NAME"
fi

# ============================================================
# 7. Deploy overlay control plane
# ============================================================
log "Deploying overlay control plane in namespace $NAMESPACE"

ukubectl create namespace "$NAMESPACE" 2>/dev/null || true

# Create PKI secret.
ukubectl -n "$NAMESPACE" create secret generic overlay-pki \
    --from-file=ca.crt="$PKI_DIR/ca.crt" \
    --from-file=ca.key="$PKI_DIR/ca.key" \
    --from-file=apiserver.crt="$PKI_DIR/apiserver.crt" \
    --from-file=apiserver.key="$PKI_DIR/apiserver.key" \
    --from-file=apiserver-kubelet-client.crt="$PKI_DIR/apiserver-kubelet-client.crt" \
    --from-file=apiserver-kubelet-client.key="$PKI_DIR/apiserver-kubelet-client.key" \
    --from-file=sa.key="$PKI_DIR/sa.key" \
    --from-file=sa.pub="$PKI_DIR/sa.pub" \
    --dry-run=client -o yaml | ukubectl apply -f -

# Create kubeconfigs secret.
ukubectl -n "$NAMESPACE" create secret generic overlay-kubeconfigs \
    --from-file=controller-manager.kubeconfig="$PKI_DIR/controller-manager.kubeconfig" \
    --from-file=scheduler.kubeconfig="$PKI_DIR/scheduler.kubeconfig" \
    --dry-run=client -o yaml | ukubectl apply -f -

# GCS credentials secret for the bucket-proxy-server sidecar. Not used on GKE,
# where the sidecar authenticates via Workload Identity (the gke-demo Terraform
# binds the overlay-system namespace to roles/storage.admin).
if ! is_gke; then
    ukubectl -n "$NAMESPACE" create secret generic gcs-credentials \
        --from-file=application_default_credentials.json="$GCS_CREDENTIALS_FILE" \
        --dry-run=client -o yaml | ukubectl apply -f -
fi

# Apply manifests in order, waiting for dependencies.
apply_manifest egress-selector
apply_manifest etcd

# etcd is a StatefulSet with a PVC; first rollout includes volume provisioning.
echo "Waiting for etcd to be ready (timeout $ETCD_WAIT_TIMEOUT)..."
ukubectl -n "$NAMESPACE" rollout status statefulset/etcd --timeout="$ETCD_WAIT_TIMEOUT"

apply_manifest apiserver

echo "Waiting for kube-apiserver to be ready (timeout $APISERVER_WAIT_TIMEOUT)..."
ukubectl -n "$NAMESPACE" rollout status deployment/kube-apiserver --timeout="$APISERVER_WAIT_TIMEOUT"

apply_manifest controller-manager
apply_manifest scheduler

echo "Waiting for controller-manager and scheduler..."
ukubectl -n "$NAMESPACE" rollout status deployment/kube-controller-manager --timeout="$APISERVER_WAIT_TIMEOUT"
ukubectl -n "$NAMESPACE" rollout status deployment/kube-scheduler --timeout="$APISERVER_WAIT_TIMEOUT"
ukubectl -n "$NAMESPACE" get pods,pvc

# ============================================================
# 8. Verify overlay apiserver is reachable
# ============================================================
log "Verifying overlay apiserver"

start_overlay_port_forward
verify_overlay_apiserver || true

# ============================================================
# Summary
# ============================================================
log "Control plane setup complete!"

echo "Underlay cluster:  ${UNDERLAY_CONTEXT} (${UNDERLAY})"
echo "Overlay namespace: $NAMESPACE"
echo "GCS bucket:        gs://${GCS_BUCKET}/${GCS_PREFIX}"
echo "PKI directory:     $PKI_DIR"
echo ""
echo "Next step: run setup-worker.sh to add a worker VM."
