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

# teardown-worker.sh — Tear down a single worker VM.
#
# Drains and deletes the node from the overlay cluster, deletes the VM,
# and cleans up per-node PKI files.
#
# Environment variables:
#   NODE_ID              — node ID / VM name (required)
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

NODE_ID="${NODE_ID:?NODE_ID environment variable is required}"
PKI_DIR="/tmp/bucket-dev-pki"
VM_BACKEND="${VM_BACKEND:-multipass}"
GCP_PROJECT="${GCP_PROJECT:-}"
GCP_ZONE="${GCP_ZONE:-us-east1-b}"

log() { echo "==> $*"; }

OVERLAY_KUBECONFIG="${PKI_DIR}/admin.kubeconfig"

# ============================================================
# 1. Drain and delete the node from the overlay cluster
# ============================================================
if [ -f "$OVERLAY_KUBECONFIG" ]; then
    if kubectl --kubeconfig="$OVERLAY_KUBECONFIG" get node "$NODE_ID" &>/dev/null; then
        log "Draining overlay node: $NODE_ID"
        kubectl --kubeconfig="$OVERLAY_KUBECONFIG" drain "$NODE_ID" \
            --ignore-daemonsets --delete-emptydir-data --force --timeout=30s 2>/dev/null || true
        log "Deleting overlay node: $NODE_ID"
        kubectl --kubeconfig="$OVERLAY_KUBECONFIG" delete node "$NODE_ID" --timeout=15s 2>/dev/null || true
    fi
fi

# ============================================================
# 2. Delete the VM
# ============================================================
if [ "$VM_BACKEND" = "gcp" ]; then
    log "Deleting GCP VM: $NODE_ID"
    gcloud compute instances delete "$NODE_ID" \
        --project="$GCP_PROJECT" --zone="$GCP_ZONE" --quiet 2>/dev/null || true
else
    if multipass info "$NODE_ID" &>/dev/null; then
        log "Stopping services in VM: $NODE_ID"
        multipass exec "$NODE_ID" -- sudo systemctl stop bucket-proxy-agent 2>/dev/null || true
        multipass exec "$NODE_ID" -- sudo systemctl stop kubelet 2>/dev/null || true

        log "Deleting Multipass VM: $NODE_ID"
        multipass stop "$NODE_ID" 2>/dev/null || true
    fi
    multipass delete "$NODE_ID" --purge 2>/dev/null || true
fi

# ============================================================
# 3. Clean up per-node PKI files
# ============================================================
log "Cleaning up PKI files for $NODE_ID"
rm -f "$PKI_DIR/kubelet-${NODE_ID}".{crt,key,kubeconfig}

log "Worker $NODE_ID torn down."
