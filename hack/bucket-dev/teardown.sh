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

# teardown.sh — Tear down the bucket-based Konnectivity dev environment.
#
# Environment variables:
#   VM_BACKEND           — "multipass" (default) or "gcp"
#   GCP_PROJECT          — GCP project ID (required when VM_BACKEND=gcp)
#   GCP_ZONE             — GCP zone (default: "us-east1-b")

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Auto-source demo.env if present.
if [ -f "$SCRIPT_DIR/demo.env" ]; then
    set -a
    source "$SCRIPT_DIR/demo.env"
    set +a
fi

CLUSTER_NAME="bucket-dev"
VM_NAME="bucket-agent-vm"
BUCKET_DIR="/tmp/bucket-dev"
PKI_DIR="/tmp/bucket-dev-pki"
VM_BACKEND="${VM_BACKEND:-multipass}"
GCP_PROJECT="${GCP_PROJECT:-}"
GCP_ZONE="${GCP_ZONE:-us-east1-b}"

log() { echo "==> $*"; }

OVERLAY_KUBECONFIG="${PKI_DIR}/admin.kubeconfig"

# Drain and delete the node from the overlay cluster before destroying the VM.
if [ -f "$OVERLAY_KUBECONFIG" ]; then
    NODES=$(kubectl --kubeconfig="$OVERLAY_KUBECONFIG" get nodes -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)
    if [ -n "$NODES" ]; then
        for node in $NODES; do
            log "Draining overlay node: $node"
            kubectl --kubeconfig="$OVERLAY_KUBECONFIG" drain "$node" \
                --ignore-daemonsets --delete-emptydir-data --force --timeout=30s 2>/dev/null || true
            log "Deleting overlay node: $node"
            kubectl --kubeconfig="$OVERLAY_KUBECONFIG" delete node "$node" --timeout=15s 2>/dev/null || true
        done
    fi
fi

if [ "$VM_BACKEND" = "gcp" ]; then
    log "Deleting GCP VM: $VM_NAME"
    gcloud compute instances delete "$VM_NAME" \
        --project="$GCP_PROJECT" --zone="$GCP_ZONE" --quiet 2>/dev/null || true
else
    # Stop services in VM first so they don't write more files as root.
    if multipass info "$VM_NAME" &>/dev/null; then
        log "Stopping services in VM"
        multipass exec "$VM_NAME" -- sudo systemctl stop bucket-proxy-agent 2>/dev/null || true
        multipass exec "$VM_NAME" -- sudo systemctl stop kubelet 2>/dev/null || true

        # Clean up root-owned files in bucket before unmounting.
        multipass exec "$VM_NAME" -- sudo rm -rf /mnt/bucket/node-to-control 2>/dev/null || true
        multipass exec "$VM_NAME" -- sudo rm -rf /mnt/bucket/control-to-node 2>/dev/null || true

        log "Unmounting and deleting Multipass VM: $VM_NAME"
        multipass umount "$VM_NAME" 2>/dev/null || true
        multipass stop "$VM_NAME" 2>/dev/null || true
    fi
    multipass delete "$VM_NAME" --purge 2>/dev/null || true
fi

log "Deleting k3d cluster: $CLUSTER_NAME"
k3d cluster delete "$CLUSTER_NAME" 2>/dev/null || true

log "Removing bucket directory: $BUCKET_DIR"
rm -rf "$BUCKET_DIR" 2>/dev/null || true

log "Removing PKI directory: $PKI_DIR"
rm -rf "$PKI_DIR"

log "Removing bucket-proxy-server Docker image"
docker rmi bucket-proxy-server:dev 2>/dev/null || true

log "Teardown complete."
