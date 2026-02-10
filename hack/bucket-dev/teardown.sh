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

# teardown.sh — Tear down the entire bucket-based Konnectivity dev environment.
#
# Discovers all worker VMs (from the overlay cluster and the VM backend),
# tears down each one, then tears down the control plane.
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

PKI_DIR="/tmp/bucket-dev-pki"
VM_BACKEND="${VM_BACKEND:-multipass}"
GCP_PROJECT="${GCP_PROJECT:-}"
GCP_ZONE="${GCP_ZONE:-us-east1-b}"

log() { echo "==> $*"; }

OVERLAY_KUBECONFIG="${PKI_DIR}/admin.kubeconfig"

# ============================================================
# 1. Discover all worker node IDs
# ============================================================
# Collect from both the overlay cluster and the VM backend to catch
# VMs that may not have registered (or whose registration was lost).
WORKER_IDS=""

# Source 1: overlay cluster nodes.
if [ -f "$OVERLAY_KUBECONFIG" ]; then
    NODES=$(kubectl --kubeconfig="$OVERLAY_KUBECONFIG" get nodes -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)
    for node in $NODES; do
        WORKER_IDS="$WORKER_IDS $node"
    done
fi

# Source 2: VM backend — look for VMs matching the bucket-agent-* naming pattern.
if [ "$VM_BACKEND" = "gcp" ]; then
    if [ -n "$GCP_PROJECT" ]; then
        GCP_VMS=$(gcloud compute instances list \
            --project="$GCP_PROJECT" \
            --zones="$GCP_ZONE" \
            --filter="name~'^bucket-agent-'" \
            --format="value(name)" 2>/dev/null || true)
        for vm in $GCP_VMS; do
            WORKER_IDS="$WORKER_IDS $vm"
        done
    fi
else
    # Multipass: list VMs whose name starts with bucket-agent-.
    MULTIPASS_VMS=$(multipass list --format csv 2>/dev/null | tail -n +2 | cut -d, -f1 | grep '^bucket-agent-' || true)
    for vm in $MULTIPASS_VMS; do
        WORKER_IDS="$WORKER_IDS $vm"
    done
fi

# Deduplicate (a node may appear in both the overlay cluster and the VM backend).
WORKER_IDS=$(echo "$WORKER_IDS" | tr ' ' '\n' | grep -v '^$' | sort -u | tr '\n' ' ')

# ============================================================
# 2. Tear down each worker
# ============================================================
if [ -n "$WORKER_IDS" ]; then
    log "Found worker(s):$WORKER_IDS"
    for node_id in $WORKER_IDS; do
        log "Tearing down worker: $node_id"
        NODE_ID="$node_id" "$SCRIPT_DIR/teardown-worker.sh"
    done
else
    log "No worker VMs found."
fi

# ============================================================
# 3. Clean up transport data in GCS bucket
# ============================================================
"$SCRIPT_DIR/cleanup-bucket.sh"

# ============================================================
# 4. Tear down the control plane
# ============================================================
"$SCRIPT_DIR/teardown-control-plane.sh"

log "Teardown complete."
