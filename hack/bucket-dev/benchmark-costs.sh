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

# benchmark-costs.sh — 1-hour cost benchmark for bucket transport.
#
# Deploys a workload pod, collects metrics every 60s for 1 hour,
# then produces a cost report via show-costs.sh.
#
# Usage:
#   ./benchmark-costs.sh
#
# Environment variables:
#   BENCHMARK_DURATION  — total benchmark duration in seconds (default: 600)
#   SAMPLE_INTERVAL     — seconds between metric snapshots (default: 60)
#   VM_NAME             — multipass VM name (default: bucket-agent-vm)
#   NAMESPACE           — kubernetes namespace (default: overlay-system)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

BENCHMARK_DURATION="${BENCHMARK_DURATION:-600}"
SAMPLE_INTERVAL="${SAMPLE_INTERVAL:-60}"
VM_NAME="${VM_NAME:-bucket-agent-vm}"
NAMESPACE="${NAMESPACE:-overlay-system}"
AGENT_ADMIN_PORT="${AGENT_ADMIN_PORT:-8094}"
SERVER_ADMIN_PORT="${SERVER_ADMIN_PORT:-8095}"
WORKLOAD_NAME="benchmark-log-generator"
PKI_DIR="${PKI_DIR:-/tmp/bucket-dev-pki}"
OVERLAY_KUBECONFIG="${OVERLAY_KUBECONFIG:-${PKI_DIR}/admin.kubeconfig}"

OUTPUT_DIR="/tmp/bucket-dev-benchmark/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUTPUT_DIR"

log() {
    echo "[$(date '+%H:%M:%S')] $*"
}

cleanup() {
    log "Cleaning up..."
    # Delete workload pod from overlay cluster if it exists.
    kubectl --kubeconfig="$OVERLAY_KUBECONFIG" delete pod "$WORKLOAD_NAME" --ignore-not-found=true 2>/dev/null || true
    # Kill background collector if running.
    if [ -n "${COLLECTOR_PID:-}" ]; then
        kill "$COLLECTOR_PID" 2>/dev/null || true
    fi
}

trap cleanup EXIT

# ---------------------------------------------------------------
# Step 1: Deploy workload pod
# ---------------------------------------------------------------
log "Deploying workload pod: $WORKLOAD_NAME (on overlay cluster)"

if [ ! -f "$OVERLAY_KUBECONFIG" ]; then
    log "WARNING: Overlay kubeconfig not found at $OVERLAY_KUBECONFIG"
    log "Set OVERLAY_KUBECONFIG to the overlay admin kubeconfig path."
    exit 1
fi

# Deploy on the overlay cluster so traffic flows through bucket transport.
kubectl --kubeconfig="$OVERLAY_KUBECONFIG" apply -f - <<'YAML'
apiVersion: v1
kind: Pod
metadata:
  name: benchmark-log-generator
  namespace: default
  labels:
    app: benchmark-log-generator
spec:
  tolerations:
  - operator: Exists
  containers:
  - name: logger
    image: busybox:1.36
    command:
    - /bin/sh
    - -c
    - |
      while true; do
        echo "$(date -Iseconds) $(head -c 20 /dev/urandom | base64)"
        sleep $((RANDOM % 5 + 1))
      done
  terminationGracePeriodSeconds: 1
YAML

log "Waiting for workload pod to be ready..."
kubectl --kubeconfig="$OVERLAY_KUBECONFIG" wait --for=condition=Ready "pod/$WORKLOAD_NAME" --timeout=120s
log "Workload pod is running."

# ---------------------------------------------------------------
# Step 2: Collect metrics every SAMPLE_INTERVAL seconds
# ---------------------------------------------------------------
log "Starting metrics collection (duration=${BENCHMARK_DURATION}s, interval=${SAMPLE_INTERVAL}s)"
log "Output directory: $OUTPUT_DIR"

collect_metrics() {
    local end_time=$(($(date +%s) + BENCHMARK_DURATION))
    local sample=0
    local local_port=18095

    # Start a long-lived port-forward for the server metrics.
    kubectl -n "$NAMESPACE" port-forward pod/kube-apiserver "${local_port}:${SERVER_ADMIN_PORT}" &>/dev/null &
    local pf_pid=$!
    sleep 2

    while [ "$(date +%s)" -lt "$end_time" ]; do
        local ts
        ts=$(date +%Y%m%d-%H%M%S)
        sample=$((sample + 1))

        # Collect agent metrics.
        local agent_file="$OUTPUT_DIR/agent-${ts}.prom"
        multipass exec "$VM_NAME" -- curl -s "http://127.0.0.1:${AGENT_ADMIN_PORT}/metrics" > "$agent_file" 2>/dev/null || true

        # Collect server metrics via port-forward.
        local server_file="$OUTPUT_DIR/server-${ts}.prom"
        curl -s "http://127.0.0.1:${local_port}/metrics" > "$server_file" 2>/dev/null || true

        log "  Sample #${sample} collected at ${ts}"

        # Copy as latest for show-costs.sh
        cp "$agent_file" "$OUTPUT_DIR/agent-latest.prom"
        cp "$server_file" "$OUTPUT_DIR/server-latest.prom"

        sleep "$SAMPLE_INTERVAL"
    done

    # Clean up port-forward.
    kill "$pf_pid" 2>/dev/null
    wait "$pf_pid" 2>/dev/null || true
}

collect_metrics &
COLLECTOR_PID=$!

log "Collector running in background (PID=$COLLECTOR_PID)"
log "Waiting for benchmark to complete (${BENCHMARK_DURATION}s)..."
log ""
log "You can monitor progress with:"
log "  ls -la $OUTPUT_DIR/"
log "  tail -1 $OUTPUT_DIR/agent-latest.prom"
log ""

# Wait for collector to finish.
wait "$COLLECTOR_PID" 2>/dev/null || true
COLLECTOR_PID=""

log "Metrics collection complete."
echo ""

# ---------------------------------------------------------------
# Step 3: Produce the final report
# ---------------------------------------------------------------
log "Generating cost report..."
echo ""

export VM_NAME NAMESPACE AGENT_ADMIN_PORT SERVER_ADMIN_PORT
bash "$SCRIPT_DIR/show-costs.sh" "$OUTPUT_DIR"

echo ""
log "Raw snapshots saved to: $OUTPUT_DIR"
log "Benchmark complete."
