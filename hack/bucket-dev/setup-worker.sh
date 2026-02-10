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

# setup-worker.sh — Launch a worker VM and register it with the overlay cluster.
#
# Can be called multiple times to add more workers. Each invocation generates
# a unique NODE_ID (or uses a caller-supplied one), creates per-node PKI,
# launches a VM with name = NODE_ID, and waits for the node to register.
#
# Prerequisites:
#   - Control plane must be running (run setup-control-plane.sh first)
#   - multipass (for VM_BACKEND=multipass) or gcloud (for VM_BACKEND=gcp)
#   - Agent binary uploaded to GCS (via upload-distributables.sh)
#
# Environment variables:
#   NODE_ID              — node identifier (default: auto-generated "bucket-agent-XXXXXXXX")
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

PKI_DIR="/tmp/bucket-dev-pki"
VM_BACKEND="${VM_BACKEND:-multipass}"
GCS_CREDENTIALS_FILE="${GCS_CREDENTIALS_FILE:-}"
GCS_BUCKET="${GCS_BUCKET:-}"
GCS_PREFIX="${GCS_PREFIX:-bucket-dev/}"
NODE_ID="${NODE_ID:-bucket-agent-$(openssl rand -hex 4)}"

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
echo "Node ID:    $NODE_ID"

# Check control-plane PKI exists.
if [ ! -f "$PKI_DIR/ca.crt" ] || [ ! -f "$PKI_DIR/ca.key" ]; then
    echo "ERROR: Control-plane PKI not found in $PKI_DIR."
    echo "Run setup-control-plane.sh first."
    exit 1
fi

COMMON_CMDS="openssl kubectl"
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
# 1. Generate per-node PKI
# ============================================================
log "Generating per-node PKI for $NODE_ID"

NODE_ID="$NODE_ID" \
KUBELET_APISERVER_URL="https://127.0.0.1:6443" \
    "$SCRIPT_DIR/generate-node-pki.sh" "$PKI_DIR"

KUBELET_KUBECONFIG="$PKI_DIR/kubelet-${NODE_ID}.kubeconfig"

# ============================================================
# 2. Launch worker VM
# ============================================================
log "Launching worker VM ($VM_BACKEND): $NODE_ID"

export CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE="$GCS_CREDENTIALS_FILE"

# Helper: run a command on the worker VM.
vm_exec() {
    if [ "$VM_BACKEND" = "gcp" ]; then
        gcloud compute ssh "$NODE_ID" \
            --project="$GCP_PROJECT" --zone="$GCP_ZONE" \
            --tunnel-through-iap --command="$*"
    else
        multipass exec "$NODE_ID" -- "$@"
    fi
}

if [ "$VM_BACKEND" = "gcp" ]; then
    # ---- GCP path ----

    # Delete existing VM with this name if present.
    gcloud compute instances delete "$NODE_ID" \
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

    gcloud compute instances create "$NODE_ID" \
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

    # Delete existing VM with this name if present.
    multipass delete "$NODE_ID" --purge 2>/dev/null || true

    # Render cloud-init template.
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
        --name "$NODE_ID" \
        --cpus 2 \
        --memory 2G \
        --disk 10G \
        --cloud-init "$RENDERED_CLOUD_INIT"
    rm -f "$RENDERED_CLOUD_INIT"

    echo "Waiting for cloud-init to complete (installs binaries and starts services)..."
    multipass exec "$NODE_ID" -- cloud-init status --wait || true

    echo "Verifying services..."
    multipass exec "$NODE_ID" -- sudo systemctl status bucket-proxy-agent --no-pager || true
    multipass exec "$NODE_ID" -- sudo systemctl status kubelet --no-pager || true
fi

# ============================================================
# 3. Wait for node registration
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
log "Worker setup complete!"

echo "Node ID:      $NODE_ID"
echo "VM backend:   $VM_BACKEND"
echo "Architecture: $GOARCH"
echo ""
echo "--- Useful commands ---"
echo ""
echo "# Check overlay nodes:"
echo "  kubectl --kubeconfig=$PKI_DIR/admin.kubeconfig get nodes"
echo ""
if [ "$VM_BACKEND" = "gcp" ]; then
    SSH_CMD="gcloud compute ssh $NODE_ID --project=$GCP_PROJECT --zone=$GCP_ZONE --tunnel-through-iap"
    echo "# Check bucket-proxy-agent logs:"
    echo "  $SSH_CMD --command='sudo journalctl -u bucket-proxy-agent -f'"
    echo ""
    echo "# Check kubelet logs:"
    echo "  $SSH_CMD --command='sudo journalctl -u kubelet -f'"
    echo ""
    echo "# SSH into VM:"
    echo "  $SSH_CMD"
else
    echo "# Check bucket-proxy-agent logs:"
    echo "  multipass exec $NODE_ID -- sudo journalctl -u bucket-proxy-agent -f"
    echo ""
    echo "# Check kubelet logs:"
    echo "  multipass exec $NODE_ID -- sudo journalctl -u kubelet -f"
    echo ""
    echo "# SSH into VM:"
    echo "  multipass shell $NODE_ID"
fi
echo ""
echo "# Tear down this worker:"
echo "  NODE_ID=$NODE_ID $SCRIPT_DIR/teardown-worker.sh"
echo ""
echo "# Redeploy agent binary to this worker:"
echo "  NODE_ID=$NODE_ID $SCRIPT_DIR/redeploy-agent.sh"
