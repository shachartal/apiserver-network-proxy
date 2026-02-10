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

# show-costs.sh — Analyze bucket store metrics and estimate GCS costs.
#
# Usage:
#   ./show-costs.sh [METRICS_DIR]
#
# If METRICS_DIR is provided, reads collected snapshots from that directory.
# Otherwise, fetches live metrics from the agent VM and server pod.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

NODE_ID="${NODE_ID:-}"
NAMESPACE="${NAMESPACE:-overlay-system}"
AGENT_ADMIN_PORT="${AGENT_ADMIN_PORT:-8094}"
SERVER_ADMIN_PORT="${SERVER_ADMIN_PORT:-8095}"

# GCS pricing per 10k operations (Standard storage)
PRICE_CLASS_A="0.05"   # $0.05 per 10k (objects.insert, objects.list)
PRICE_CLASS_B="0.004"  # $0.004 per 10k (objects.get)
PRICE_FREE="0.00"      # free (objects.delete)

METRICS_DIR="${1:-}"

fetch_agent_metrics() {
    if [ -n "$METRICS_DIR" ] && [ -f "$METRICS_DIR/agent-latest.prom" ]; then
        cat "$METRICS_DIR/agent-latest.prom"
    else
        multipass exec "$NODE_ID" -- curl -s "http://127.0.0.1:${AGENT_ADMIN_PORT}/metrics" 2>/dev/null || echo ""
    fi
}

fetch_server_metrics() {
    if [ -n "$METRICS_DIR" ] && [ -f "$METRICS_DIR/server-latest.prom" ]; then
        cat "$METRICS_DIR/server-latest.prom"
    else
        # Use port-forward since the server container may not have curl/wget.
        local local_port=18095
        kubectl -n "$NAMESPACE" port-forward pod/kube-apiserver "${local_port}:${SERVER_ADMIN_PORT}" &>/dev/null &
        local pf_pid=$!
        sleep 2
        curl -s "http://127.0.0.1:${local_port}/metrics" 2>/dev/null || echo ""
        kill "$pf_pid" 2>/dev/null
        wait "$pf_pid" 2>/dev/null || true
    fi
}

# Extract a counter value from prometheus text format.
# Usage: extract_counter <metrics_text> <operation> <status>
# Uses exact matching for operation names to avoid "list" matching "list_recursive".
extract_counter() {
    local metrics="$1" operation="$2" status="${3:-success}"
    local val
    val=$(echo "$metrics" | grep "^konnectivity_network_proxy_bucket_store_operations_total{" | \
        grep "operation=\"${operation}\"" | \
        grep -v "operation=\"${operation}_" | \
        grep "status=\"${status}\"" | \
        awk '{print $NF}' | head -1 || true)
    echo "${val:-0}"
}

# Multiply operations by cost rate. Usage: calc_cost <ops> <price_per_10k>
calc_cost() {
    local ops="${1:-0}" price="$2"
    echo "$ops $price" | awk '{printf "%.4f", ($1 / 10000) * $2}'
}

format_number() {
    printf "%'d" "${1:-0}" 2>/dev/null || echo "${1:-0}"
}

echo "============================================================"
echo "  GCS Bucket Store — Cost Estimation Report"
echo "============================================================"
echo ""

# --- Section A: Raw Metrics ---
echo "A) Raw Prometheus Metrics"
echo "------------------------------------------------------------"

agent_metrics=$(fetch_agent_metrics)
server_metrics=$(fetch_server_metrics)

if [ -z "$agent_metrics" ]; then
    echo "  [WARNING] Could not fetch agent metrics"
else
    echo ""
    echo "  AGENT (${NODE_ID}:${AGENT_ADMIN_PORT}):"
    echo "$agent_metrics" | grep "^konnectivity_network_proxy_bucket_" | sed 's/^/    /'
fi

if [ -z "$server_metrics" ]; then
    echo "  [WARNING] Could not fetch server metrics"
else
    echo ""
    echo "  SERVER (${NAMESPACE}/kube-apiserver:${SERVER_ADMIN_PORT}):"
    echo "$server_metrics" | grep "^konnectivity_network_proxy_bucket_" | sed 's/^/    /'
fi

echo ""

# --- Section B: Total API Calls ---
echo "B) Total API Calls per Component"
echo "------------------------------------------------------------"

agent_put=$(extract_counter "$agent_metrics" "put" "success")
agent_list=$(extract_counter "$agent_metrics" "list" "success")
agent_list_recursive=$(extract_counter "$agent_metrics" "list_recursive" "success")
agent_get=$(extract_counter "$agent_metrics" "get" "success")
agent_delete=$(extract_counter "$agent_metrics" "delete" "success")
agent_put=${agent_put:-0}; agent_list=${agent_list:-0}; agent_list_recursive=${agent_list_recursive:-0}; agent_get=${agent_get:-0}; agent_delete=${agent_delete:-0}
# Truncate to integer
agent_put=${agent_put%.*}; agent_list=${agent_list%.*}; agent_list_recursive=${agent_list_recursive%.*}; agent_get=${agent_get%.*}; agent_delete=${agent_delete%.*}
# Combine list and list_recursive (both Class A operations)
agent_list_all=$((agent_list + agent_list_recursive))
agent_total=$((agent_put + agent_list_all + agent_get + agent_delete))

server_put=$(extract_counter "$server_metrics" "put" "success")
server_list=$(extract_counter "$server_metrics" "list" "success")
server_list_recursive=$(extract_counter "$server_metrics" "list_recursive" "success")
server_get=$(extract_counter "$server_metrics" "get" "success")
server_delete=$(extract_counter "$server_metrics" "delete" "success")
server_put=${server_put:-0}; server_list=${server_list:-0}; server_list_recursive=${server_list_recursive:-0}; server_get=${server_get:-0}; server_delete=${server_delete:-0}
server_put=${server_put%.*}; server_list=${server_list%.*}; server_list_recursive=${server_list_recursive%.*}; server_get=${server_get%.*}; server_delete=${server_delete%.*}
# Combine list and list_recursive (both Class A operations)
server_list_all=$((server_list + server_list_recursive))
server_total=$((server_put + server_list_all + server_get + server_delete))

total_put=$((agent_put + server_put))
total_list_all=$((agent_list_all + server_list_all))
total_get=$((agent_get + server_get))
total_delete=$((agent_delete + server_delete))
grand_total=$((total_put + total_list_all + total_get + total_delete))

printf "  %-12s | %10s | %10s | %10s | %14s | %10s\n" "Component" "Put (A)" "List (A)" "Get (B)" "Delete (free)" "Total"
printf "  %-12s-|-%10s-|-%10s-|-%10s-|-%14s-|-%10s\n" "------------" "----------" "----------" "----------" "--------------" "----------"
printf "  %-12s | %10s | %10s | %10s | %14s | %10s\n" "agent" "$(format_number $agent_put)" "$(format_number $agent_list_all)" "$(format_number $agent_get)" "$(format_number $agent_delete)" "$(format_number $agent_total)"
printf "  %-12s | %10s | %10s | %10s | %14s | %10s\n" "server" "$(format_number $server_put)" "$(format_number $server_list_all)" "$(format_number $server_get)" "$(format_number $server_delete)" "$(format_number $server_total)"
printf "  %-12s-|-%10s-|-%10s-|-%10s-|-%14s-|-%10s\n" "------------" "----------" "----------" "----------" "--------------" "----------"
printf "  %-12s | %10s | %10s | %10s | %14s | %10s\n" "TOTAL" "$(format_number $total_put)" "$(format_number $total_list_all)" "$(format_number $total_get)" "$(format_number $total_delete)" "$(format_number $grand_total)"

echo ""

# --- Section C: Cost Estimation ---
echo "C) Cost Estimation with Per-Component Breakdown"
echo "------------------------------------------------------------"

agent_class_a_ops=$((agent_put + agent_list_all))
agent_class_a_cost=$(calc_cost $agent_class_a_ops "$PRICE_CLASS_A")
agent_class_b_cost=$(calc_cost $agent_get "$PRICE_CLASS_B")
agent_hourly=$(echo "$agent_class_a_cost $agent_class_b_cost" | awk '{printf "%.4f", $1 + $2}')

echo ""
echo "  AGENT (per-node cost — scales linearly with node count)"
printf "    Class A (put + list):  %s × \$%s/10k  = \$%s\n" "$(format_number $agent_class_a_ops)" "$PRICE_CLASS_A" "$agent_class_a_cost"
printf "      of which list:       %s\n" "$(format_number $agent_list)"
printf "      of which list_rec:   %s (consolidated poll)\n" "$(format_number $agent_list_recursive)"
printf "      of which put:        %s\n" "$(format_number $agent_put)"
printf "    Class B (get):         %s × \$%s/10k = \$%s\n" "$(format_number $agent_get)" "$PRICE_CLASS_B" "$agent_class_b_cost"
printf "    Free (delete):         %s × \$0.00      = \$0.00\n" "$(format_number $agent_delete)"
printf "    ── Agent hourly cost per node: \$%s\n" "$agent_hourly"

echo ""

server_class_a_ops=$((server_put + server_list_all))
server_class_a_cost=$(calc_cost $server_class_a_ops "$PRICE_CLASS_A")
server_class_b_cost=$(calc_cost $server_get "$PRICE_CLASS_B")
server_hourly=$(echo "$server_class_a_cost $server_class_b_cost" | awk '{printf "%.4f", $1 + $2}')

# Server per-node ops: put + get + delete scale per-node; list is shared
server_per_node_a_cost=$(calc_cost $server_put "$PRICE_CLASS_A")
server_per_node_b_cost=$(calc_cost $server_get "$PRICE_CLASS_B")
server_per_node_cost=$(echo "$server_per_node_a_cost $server_per_node_b_cost" | awk '{printf "%.4f", $1 + $2}')

server_shared_cost=$(calc_cost $server_list_all "$PRICE_CLASS_A")

echo "  SERVER (shared cost — List scales sub-linearly; Get/Put/Delete scale per-node)"
printf "    Class A (put + list):  %s × \$%s/10k  = \$%s\n" "$(format_number $server_class_a_ops)" "$PRICE_CLASS_A" "$server_class_a_cost"
printf "      of which list:       %s\n" "$(format_number $server_list)"
printf "      of which list_rec:   %s (per-node polling)\n" "$(format_number $server_list_recursive)"
printf "      of which put:        %s (per-node)\n" "$(format_number $server_put)"
printf "    Class B (get):         %s × \$%s/10k = \$%s\n" "$(format_number $server_get)" "$PRICE_CLASS_B" "$server_class_b_cost"
printf "    Free (delete):         %s × \$0.00      = \$0.00\n" "$(format_number $server_delete)"
printf "    ── Server hourly cost (1 node): \$%s\n" "$server_hourly"

echo ""

# Projection for N nodes
for nodes in 10 100 1000; do
    agent_total_cost=$(echo "$agent_hourly $nodes" | awk '{printf "%.2f", $1 * $2}')
    # Server: shared (list) + per-node * N
    server_total_cost=$(echo "$server_shared_cost $server_per_node_cost $nodes" | awk '{printf "%.2f", $1 + $2 * $3}')
    combined=$(echo "$agent_total_cost $server_total_cost" | awk '{printf "%.2f", $1 + $2}')
    monthly=$(echo "$combined" | awk '{printf "%.2f", $1 * 730}')
    echo "  PROJECTION: ${nodes} nodes"
    printf "    Agent: %s × \$%s  = \$%s/hr\n" "$nodes" "$agent_hourly" "$agent_total_cost"
    printf "    Server: \$%s base + %s × \$%s per-node = \$%s/hr\n" "$server_shared_cost" "$nodes" "$server_per_node_cost" "$server_total_cost"
    printf "    ── Estimated total: \$%s/hr (\$%s/month)\n" "$combined" "$monthly"
    echo ""
done

# --- Section D: Per-minute rate charts (if snapshots available) ---
if [ -n "$METRICS_DIR" ] && ls "$METRICS_DIR"/agent-*.prom >/dev/null 2>&1; then
    echo "D) Per-Minute Rate Charts"
    echo "------------------------------------------------------------"
    echo ""

    prev_ts=""
    prev_agent_total=""
    prev_server_total=""

    printf "  %-20s | %12s | %12s | %12s\n" "Timestamp" "Agent ops/m" "Server ops/m" "Total ops/m"
    printf "  %-20s-|-%12s-|-%12s-|-%12s\n" "--------------------" "------------" "------------" "------------"

    for snapshot in $(ls "$METRICS_DIR"/agent-*.prom 2>/dev/null | sort); do
        ts=$(basename "$snapshot" | sed 's/agent-//;s/\.prom//')
        # Skip the "latest" file
        [ "$ts" = "latest" ] && continue

        agent_snap=$(cat "$snapshot")
        server_snap_file="$METRICS_DIR/server-${ts}.prom"

        a_put=$(extract_counter "$agent_snap" "put" "success"); a_put=${a_put:-0}; a_put=${a_put%.*}
        a_list=$(extract_counter "$agent_snap" "list" "success"); a_list=${a_list:-0}; a_list=${a_list%.*}
        a_list_rec=$(extract_counter "$agent_snap" "list_recursive" "success"); a_list_rec=${a_list_rec:-0}; a_list_rec=${a_list_rec%.*}
        a_get=$(extract_counter "$agent_snap" "get" "success"); a_get=${a_get:-0}; a_get=${a_get%.*}
        a_del=$(extract_counter "$agent_snap" "delete" "success"); a_del=${a_del:-0}; a_del=${a_del%.*}
        a_total=$((a_put + a_list + a_list_rec + a_get + a_del))

        s_total=0
        if [ -f "$server_snap_file" ]; then
            server_snap=$(cat "$server_snap_file")
            s_put=$(extract_counter "$server_snap" "put" "success"); s_put=${s_put:-0}; s_put=${s_put%.*}
            s_list=$(extract_counter "$server_snap" "list" "success"); s_list=${s_list:-0}; s_list=${s_list%.*}
            s_list_rec=$(extract_counter "$server_snap" "list_recursive" "success"); s_list_rec=${s_list_rec:-0}; s_list_rec=${s_list_rec%.*}
            s_get=$(extract_counter "$server_snap" "get" "success"); s_get=${s_get:-0}; s_get=${s_get%.*}
            s_del=$(extract_counter "$server_snap" "delete" "success"); s_del=${s_del:-0}; s_del=${s_del%.*}
            s_total=$((s_put + s_list + s_list_rec + s_get + s_del))
        fi

        if [ -n "$prev_ts" ] && [ -n "$prev_agent_total" ]; then
            agent_rate=$((a_total - prev_agent_total))
            server_rate=$((s_total - prev_server_total))
            total_rate=$((agent_rate + server_rate))
            printf "  %-20s | %12s | %12s | %12s\n" "$ts" "$(format_number $agent_rate)" "$(format_number $server_rate)" "$(format_number $total_rate)"
        fi

        prev_ts="$ts"
        prev_agent_total="$a_total"
        prev_server_total="$s_total"
    done
    echo ""
else
    echo "D) Per-Minute Rate Charts"
    echo "------------------------------------------------------------"
    echo "  [SKIPPED] No snapshot directory provided or no snapshots found."
    echo "  Run benchmark-costs.sh to collect time-series data."
    echo ""
fi

echo "============================================================"
echo "  Report complete."
echo "============================================================"
