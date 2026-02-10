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

# setup.sh — Set up the local bucket-based Konnectivity dev environment.
#
# Architecture:
#   - k3d cluster (underlay) running overlay control plane pods
#   - Worker VM running kubelet + bucket-proxy-agent (multipass or GCP)
#   - GCS bucket for transport and distributable storage
#
# Prerequisites:
#   - docker, k3d, kubectl, openssl
#   - multipass (for VM_BACKEND=multipass) or gcloud (for VM_BACKEND=gcp)
#   - Run 'make build-bucket-linux' first to build the linux binaries
#   - Run './upload-distributables.sh' once to upload kubelet, containerd, etc. to GCS
#
# Environment variables:
#   VM_BACKEND           — "multipass" (default) or "gcp"
#   GCS_CREDENTIALS_FILE — path to GCS credentials JSON file (required)
#   GCS_BUCKET           — GCS bucket name (required)
#   GCS_PREFIX           — key prefix within the bucket (default: "bucket-dev/")
#
# GCP-specific environment variables (required when VM_BACKEND=gcp):
#   GCP_PROJECT          — GCP project ID
#   GCP_ZONE             — GCP zone (default: "us-east1-b")
#   GCP_NETWORK          — VPC name (from terraform output)
#   GCP_SUBNET           — Subnet self-link (from terraform output)
#   GCP_SERVICE_ACCOUNT  — Worker node service account email (from terraform output)

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
VM_NAME="bucket-agent-vm"
VM_BACKEND="${VM_BACKEND:-multipass}"
GCS_CREDENTIALS_FILE="${GCS_CREDENTIALS_FILE:-}"
GCS_BUCKET="${GCS_BUCKET:-}"
GCS_PREFIX="${GCS_PREFIX:-bucket-dev/}"
# Generate a unique node ID for each VM bootstrap to avoid stale bucket data
# from previous runs interfering with the new agent.
NODE_ID="bucket-agent-$(openssl rand -hex 4)"

# GCP-specific variables (only used when VM_BACKEND=gcp).
GCP_PROJECT="${GCP_PROJECT:-twistlock-dev-246815}"
GCP_ZONE="${GCP_ZONE:-us-east1-b}"
GCP_NETWORK="${GCP_NETWORK:-bucket-demo-vpc}"
GCP_SUBNET="${GCP_SUBNET:-https://www.googleapis.com/compute/v1/projects/twistlock-dev-246815/regions/us-east1/subnetworks/bucket-demo-subnet}"
GCP_SERVICE_ACCOUNT="${GCP_SERVICE_ACCOUNT:-bucket-demo-worker@twistlock-dev-246815.iam.gserviceaccount.com}"

log() { echo ""; echo "===== $* ====="; echo ""; }

# Detect target architecture.
# GCP VMs are always amd64; multipass VMs match the host.
if [ "$VM_BACKEND" = "gcp" ]; then
    GOARCH="amd64"
else
    HOST_ARCH="$(uname -m)"
    case "$HOST_ARCH" in
        x86_64)  GOARCH="amd64" ;;
        aarch64|arm64) GOARCH="arm64" ;;
        *) echo "ERROR: unsupported architecture $HOST_ARCH"; exit 1 ;;
    esac
fi

# ============================================================
# 0. Preflight checks
# ============================================================
log "Preflight checks"

echo "VM backend: $VM_BACKEND"

COMMON_CMDS="docker k3d kubectl openssl crane"
if [ "$VM_BACKEND" = "gcp" ]; then
    REQUIRED_CMDS="$COMMON_CMDS gcloud"
else
    REQUIRED_CMDS="$COMMON_CMDS multipass"
fi

for cmd in $REQUIRED_CMDS; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "ERROR: $cmd is required but not found in PATH"
        exit 1
    fi
done

if [ ! -f "$REPO_ROOT/bin/bucket-proxy-server-linux-${GOARCH}" ] || \
   [ ! -f "$REPO_ROOT/bin/bucket-proxy-agent-linux-${GOARCH}" ]; then
    echo "ERROR: Linux ${GOARCH} binaries not found. Run 'make build-bucket-linux' first."
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

if [ "$VM_BACKEND" = "gcp" ]; then
    for var in GCP_PROJECT GCP_NETWORK GCP_SUBNET GCP_SERVICE_ACCOUNT; do
        if [ -z "${!var}" ]; then
            echo "ERROR: $var must be set when VM_BACKEND=gcp"
            exit 1
        fi
    done
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
# 3. Generate PKI
# ============================================================
log "Generating PKI"

# Generate control-plane PKI (CA + apiserver, admin, controller-manager, scheduler certs).
# No extra SANs needed — the kubelet reaches the apiserver via the bucket
# transport reverse proxy at 127.0.0.1:6443, not directly.
"$SCRIPT_DIR/generate-pki.sh" "$PKI_DIR"

# Generate per-node PKI (kubelet cert + kubeconfig).
NODE_ID="$NODE_ID" \
KUBELET_APISERVER_URL="https://127.0.0.1:6443" \
    "$SCRIPT_DIR/generate-node-pki.sh" "$PKI_DIR"

# Stage public PKI files locally for upload to GCS.
# Private keys (kubelet kubeconfig) are injected via cloud-init, never uploaded.
mkdir -p "$BUCKET_DIR/pki"
cp "$PKI_DIR/ca.crt" "$BUCKET_DIR/pki/ca.crt"
echo "$NODE_ID" > "$BUCKET_DIR/pki/node-id"
# Keep kubeconfig locally for cloud-init injection.
KUBELET_KUBECONFIG="$PKI_DIR/kubelet-${NODE_ID}.kubeconfig"

# ============================================================
# 4. Upload PKI to GCS
# ============================================================
log "Uploading PKI to GCS"

# Upload only the public CA cert. Private keys and node-id are injected via cloud-init.
gcloud storage cp "$BUCKET_DIR/pki/ca.crt" "${GCS_BASE}pki/ca.crt"

echo "PKI upload complete."

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

# Create a minimal Dockerfile for the server.
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
# 9. Launch worker VM
# ============================================================
log "Launching worker VM ($VM_BACKEND): $VM_NAME"

echo "Node ID: $NODE_ID"

# Helper: run a command on the worker VM.
vm_exec() {
    if [ "$VM_BACKEND" = "gcp" ]; then
        gcloud compute ssh "$VM_NAME" \
            --project="$GCP_PROJECT" --zone="$GCP_ZONE" \
            --tunnel-through-iap --command="$*"
    else
        multipass exec "$VM_NAME" -- "$@"
    fi
}

if [ "$VM_BACKEND" = "gcp" ]; then
    # ---- GCP path ----

    # Delete existing VM if present.
    gcloud compute instances delete "$VM_NAME" \
        --project="$GCP_PROJECT" --zone="$GCP_ZONE" --quiet 2>/dev/null || true

    # Render cloud-init template (GCP variant: no credentials file).
    RENDERED_CLOUD_INIT=$(mktemp)
    awk -v kubeconfig_file="$KUBELET_KUBECONFIG" \
        -v gcs_bucket="$GCS_BUCKET" \
        -v gcs_prefix="$GCS_PREFIX" \
        -v node_id="$NODE_ID" '
    /^ *KUBELET_KUBECONFIG_PLACEHOLDER *$/ {
        while ((getline line < kubeconfig_file) > 0) {
            print "      " line
        }
        close(kubeconfig_file)
        next
    }
    {
        gsub(/GCS_BUCKET_PLACEHOLDER/, gcs_bucket)
        gsub(/GCS_PREFIX_PLACEHOLDER/, gcs_prefix)
        gsub(/NODE_ID_PLACEHOLDER/, node_id)
        print
    }
    ' "$SCRIPT_DIR/vm/cloud-init-gcp.yaml" > "$RENDERED_CLOUD_INIT"

    gcloud compute instances create "$VM_NAME" \
        --project="$GCP_PROJECT" \
        --zone="$GCP_ZONE" \
        --machine-type=n2-standard-2 \
        --network-interface="network=$GCP_NETWORK,subnet=$GCP_SUBNET,no-address" \
        --service-account="$GCP_SERVICE_ACCOUNT" \
        --scopes=cloud-platform \
        --image-family=ubuntu-2204-lts \
        --image-project=ubuntu-os-cloud \
        --boot-disk-size=10GB \
        --metadata-from-file=user-data="$RENDERED_CLOUD_INIT"
    rm -f "$RENDERED_CLOUD_INIT"

    echo "Waiting for cloud-init to complete (installs binaries and starts services)..."
    # GCE VMs take longer to become SSH-reachable via IAP.
    for i in $(seq 1 12); do
        if vm_exec "cloud-init status --wait" 2>/dev/null; then
            break
        fi
        echo "  Waiting for SSH via IAP... ($i/12)"
        sleep 10
    done

    echo "Verifying services..."
    vm_exec "sudo systemctl status bucket-proxy-agent --no-pager" || true
    vm_exec "sudo systemctl status kubelet --no-pager" || true

else
    # ---- Multipass path ----

    # Delete existing VM if present.
    multipass delete "$VM_NAME" --purge 2>/dev/null || true

    # Render cloud-init template.
    # Substitutes all placeholders: GCS credentials, kubelet kubeconfig, bucket name, prefix.
    RENDERED_CLOUD_INIT=$(mktemp)
    awk -v creds_file="$GCS_CREDENTIALS_FILE" \
        -v kubeconfig_file="$KUBELET_KUBECONFIG" \
        -v gcs_bucket="$GCS_BUCKET" \
        -v gcs_prefix="$GCS_PREFIX" \
        -v node_id="$NODE_ID" '
    /^ *GCS_CREDENTIALS_PLACEHOLDER *$/ {
        while ((getline line < creds_file) > 0) {
            print "      " line
        }
        close(creds_file)
        next
    }
    /^ *KUBELET_KUBECONFIG_PLACEHOLDER *$/ {
        while ((getline line < kubeconfig_file) > 0) {
            print "      " line
        }
        close(kubeconfig_file)
        next
    }
    {
        gsub(/GCS_BUCKET_PLACEHOLDER/, gcs_bucket)
        gsub(/GCS_PREFIX_PLACEHOLDER/, gcs_prefix)
        gsub(/NODE_ID_PLACEHOLDER/, node_id)
        print
    }
    ' "$SCRIPT_DIR/vm/cloud-init.yaml" > "$RENDERED_CLOUD_INIT"

    multipass launch 22.04 \
        --name "$VM_NAME" \
        --cpus 2 \
        --memory 2G \
        --disk 10G \
        --cloud-init "$RENDERED_CLOUD_INIT"
    rm -f "$RENDERED_CLOUD_INIT"

    echo "Waiting for cloud-init to complete (installs binaries and starts services)..."
    multipass exec "$VM_NAME" -- cloud-init status --wait || true

    echo "Verifying services..."
    multipass exec "$VM_NAME" -- sudo systemctl status bucket-proxy-agent --no-pager || true
    multipass exec "$VM_NAME" -- sudo systemctl status kubelet --no-pager || true
fi

# ============================================================
# 10. Wait for node registration
# ============================================================
log "Waiting for node to register with overlay apiserver"

for i in $(seq 1 30); do
    if kubectl --kubeconfig="$PKI_DIR/admin.kubeconfig" get node "$NODE_ID" &>/dev/null; then
        echo "Node $NODE_ID registered!"
        kubectl --kubeconfig="$PKI_DIR/admin.kubeconfig" get nodes
        break
    fi
    echo "  Waiting for node registration... ($i/30)"
    sleep 5
done

# ============================================================
# Summary
# ============================================================
log "Setup complete!"

echo "Underlay cluster:  k3d-${CLUSTER_NAME}"
echo "Overlay namespace: $NAMESPACE"
echo "GCS bucket:        gs://${GCS_BUCKET}/${GCS_PREFIX}"
echo "PKI directory:     $PKI_DIR"
echo "VM name:           $VM_NAME"
echo "VM backend:        $VM_BACKEND"
echo "Node ID:           $NODE_ID"
echo "Architecture:      $GOARCH"
echo ""
echo "--- Useful commands ---"
echo ""
echo "# Check overlay control plane pods:"
echo "  kubectl -n $NAMESPACE get pods"
echo ""
echo "# Access overlay apiserver:"
echo "  kubectl --kubeconfig=$PKI_DIR/admin.kubeconfig get nodes"
echo ""
echo "# Check bucket-proxy-server logs:"
echo "  kubectl -n $NAMESPACE logs kube-apiserver -c bucket-proxy-server -f"
echo ""
if [ "$VM_BACKEND" = "gcp" ]; then
    SSH_CMD="gcloud compute ssh $VM_NAME --project=$GCP_PROJECT --zone=$GCP_ZONE --tunnel-through-iap"
    echo "# Check bucket-proxy-agent logs in VM:"
    echo "  $SSH_CMD --command='sudo journalctl -u bucket-proxy-agent -f'"
    echo ""
    echo "# Check kubelet logs in VM:"
    echo "  $SSH_CMD --command='sudo journalctl -u kubelet -f'"
    echo ""
    echo "# SSH into VM:"
    echo "  $SSH_CMD"
else
    echo "# Check bucket-proxy-agent logs in VM:"
    echo "  multipass exec $VM_NAME -- sudo journalctl -u bucket-proxy-agent -f"
    echo ""
    echo "# Check kubelet logs in VM:"
    echo "  multipass exec $VM_NAME -- sudo journalctl -u kubelet -f"
    echo ""
    echo "# SSH into VM:"
    echo "  multipass shell $VM_NAME"
fi
echo ""
echo "# Check GCS bucket contents:"
echo "  gsutil ls gs://${GCS_BUCKET}/${GCS_PREFIX}"
echo ""
echo "# Redeploy bucket-proxy-server (after code changes):"
echo "  $SCRIPT_DIR/redeploy-server.sh"
echo ""
echo "# Tear down cluster and VM (preserves GCS distributables):"
echo "  VM_BACKEND=$VM_BACKEND $SCRIPT_DIR/teardown.sh"
echo ""
echo "# Upload distributables to GCS (run once or when deps change):"
echo "  $SCRIPT_DIR/upload-distributables.sh"
