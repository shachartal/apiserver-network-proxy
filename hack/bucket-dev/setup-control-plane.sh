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

# setup-control-plane.sh — Set up the overlay control plane (k3d + PKI + pods).
#
# This creates the k3d cluster, generates control-plane PKI, uploads the CA
# cert to GCS, builds the bucket-proxy-server image, and deploys the overlay
# control plane pods (etcd, apiserver, controller-manager, scheduler).
#
# Run this once. Then use setup-worker.sh to add worker VMs.
#
# Prerequisites:
#   - docker, k3d, kubectl, openssl, crane, gcloud
#   - Run 'make build-bucket-linux' first to build the linux server binary
#   - Run './upload-distributables.sh' once to upload kubelet, containerd, etc. to GCS
#
# Environment variables:
#   GCS_CREDENTIALS_FILE — path to GCS credentials JSON file (required)
#   GCS_BUCKET           — GCS bucket name (required)
#   GCS_PREFIX           — key prefix within the bucket (default: "bucket-dev/")

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Auto-source demo.env if present.
if [ -f "$SCRIPT_DIR/demo.env" ]; then
    set -a
    source "$SCRIPT_DIR/demo.env"
    set +a
fi

CLUSTER_NAME="bucket-dev"
NAMESPACE="overlay-system"
BUCKET_DIR="/tmp/bucket-dev"
PKI_DIR="/tmp/bucket-dev-pki"
GCS_CREDENTIALS_FILE="${GCS_CREDENTIALS_FILE:-}"
GCS_BUCKET="${GCS_BUCKET:-}"
GCS_PREFIX="${GCS_PREFIX:-bucket-dev/}"

log() { echo ""; echo "===== $* ====="; echo ""; }

# Detect host architecture for the server image (k3d runs on the host).
HOST_ARCH="$(uname -m)"
case "$HOST_ARCH" in
    x86_64)  GOARCH="amd64" ;;
    aarch64|arm64) GOARCH="arm64" ;;
    *) echo "ERROR: unsupported architecture $HOST_ARCH"; exit 1 ;;
esac

# ============================================================
# 0. Preflight checks
# ============================================================
log "Preflight checks"

for cmd in docker k3d kubectl openssl crane gcloud; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "ERROR: $cmd is required but not found in PATH"
        exit 1
    fi
done

if [ ! -f "$REPO_ROOT/bin/bucket-proxy-server-linux-${GOARCH}" ]; then
    echo "ERROR: Server binary not found (bin/bucket-proxy-server-linux-${GOARCH})."
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

export CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE="$GCS_CREDENTIALS_FILE"
GCS_BASE="gs://${GCS_BUCKET}/${GCS_PREFIX}"

SENTINEL="${GCS_BASE}distributables/.uploaded"
if ! gcloud storage ls "$SENTINEL" &>/dev/null; then
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
gcloud storage cp "$BUCKET_DIR/pki/ca.crt" "${GCS_BASE}pki/ca.crt"

echo "CA cert upload complete."

# ============================================================
# 5. Create k3d cluster
# ============================================================
log "Creating k3d cluster: $CLUSTER_NAME"

# Delete existing cluster if present.
k3d cluster delete "$CLUSTER_NAME" 2>/dev/null || true

k3d cluster create "$CLUSTER_NAME" \
    --k3s-arg "--disable=traefik@server:0" \
    --k3s-arg "--disable=servicelb@server:0" \
    --k3s-arg "--disable=metrics-server@server:0" \
    --port "30443:30443@server:0"

# Wait for k3d to be ready.
kubectl config use-context "k3d-${CLUSTER_NAME}"
kubectl wait --for=condition=Ready node --all --timeout=60s

# ============================================================
# 6. Build and load bucket-proxy-server image into k3d
# ============================================================
log "Building bucket-proxy-server container image"

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

k3d image import bucket-proxy-server:dev -c "$CLUSTER_NAME"

# ============================================================
# 7. Deploy overlay control plane
# ============================================================
log "Deploying overlay control plane in namespace $NAMESPACE"

kubectl create namespace "$NAMESPACE" 2>/dev/null || true

# Create PKI secret.
kubectl -n "$NAMESPACE" create secret generic overlay-pki \
    --from-file=ca.crt="$PKI_DIR/ca.crt" \
    --from-file=ca.key="$PKI_DIR/ca.key" \
    --from-file=apiserver.crt="$PKI_DIR/apiserver.crt" \
    --from-file=apiserver.key="$PKI_DIR/apiserver.key" \
    --from-file=apiserver-kubelet-client.crt="$PKI_DIR/apiserver-kubelet-client.crt" \
    --from-file=apiserver-kubelet-client.key="$PKI_DIR/apiserver-kubelet-client.key" \
    --from-file=sa.key="$PKI_DIR/sa.key" \
    --from-file=sa.pub="$PKI_DIR/sa.pub" \
    --dry-run=client -o yaml | kubectl apply -f -

# Create kubeconfigs secret.
kubectl -n "$NAMESPACE" create secret generic overlay-kubeconfigs \
    --from-file=controller-manager.kubeconfig="$PKI_DIR/controller-manager.kubeconfig" \
    --from-file=scheduler.kubeconfig="$PKI_DIR/scheduler.kubeconfig" \
    --dry-run=client -o yaml | kubectl apply -f -

# Create GCS credentials secret for the bucket-proxy-server sidecar.
kubectl -n "$NAMESPACE" create secret generic gcs-credentials \
    --from-file=application_default_credentials.json="$GCS_CREDENTIALS_FILE" \
    --dry-run=client -o yaml | kubectl apply -f -

# Apply manifests in order, waiting for dependencies.
kubectl apply -f "$SCRIPT_DIR/manifests/egress-selector.yaml"
kubectl apply -f "$SCRIPT_DIR/manifests/etcd.yaml"

echo "Waiting for etcd to be ready..."
kubectl -n "$NAMESPACE" wait --for=condition=Ready pod/etcd --timeout=60s

sed -e "s|GCS_BUCKET_PLACEHOLDER|${GCS_BUCKET}|g" \
    -e "s|GCS_PREFIX_PLACEHOLDER|${GCS_PREFIX}|g" \
    "$SCRIPT_DIR/manifests/apiserver.yaml" | kubectl apply -f -

echo "Waiting for kube-apiserver to be ready..."
kubectl -n "$NAMESPACE" wait --for=condition=Ready pod/kube-apiserver --timeout=120s

kubectl apply -f "$SCRIPT_DIR/manifests/controller-manager.yaml"
kubectl apply -f "$SCRIPT_DIR/manifests/scheduler.yaml"

echo "Waiting for controller-manager and scheduler..."
sleep 5
kubectl -n "$NAMESPACE" get pods

# ============================================================
# 8. Verify overlay apiserver is reachable
# ============================================================
log "Verifying overlay apiserver"

echo "Testing overlay apiserver via NodePort..."
for i in $(seq 1 10); do
    if kubectl --kubeconfig="$PKI_DIR/admin.kubeconfig" get --raw /healthz 2>/dev/null; then
        echo ""
        echo "Overlay apiserver is healthy!"
        break
    fi
    echo "  Waiting for apiserver to be reachable... ($i/10)"
    sleep 3
done

# ============================================================
# Summary
# ============================================================
log "Control plane setup complete!"

echo "Underlay cluster:  k3d-${CLUSTER_NAME}"
echo "Overlay namespace: $NAMESPACE"
echo "GCS bucket:        gs://${GCS_BUCKET}/${GCS_PREFIX}"
echo "PKI directory:     $PKI_DIR"
echo ""
echo "Next step: run setup-worker.sh to add a worker VM."
