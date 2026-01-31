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
#   - Multipass VM running kubelet + bucket-proxy-agent
#   - Shared host directory as the simulated bucket
#
# Prerequisites:
#   - docker, k3d, multipass, kubectl, openssl
#   - Run 'make build-bucket-linux' first to build the linux binaries

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

CLUSTER_NAME="bucket-dev"
NAMESPACE="overlay-system"
BUCKET_DIR="/tmp/bucket-dev"
PKI_DIR="/tmp/bucket-dev-pki"
VM_NAME="bucket-agent-vm"
# Generate a unique node ID for each VM bootstrap to avoid stale bucket data
# from previous runs interfering with the new agent.
NODE_ID="bucket-agent-$(openssl rand -hex 4)"

log() { echo ""; echo "===== $* ====="; echo ""; }

# Detect target architecture.
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

for cmd in docker k3d multipass kubectl openssl; do
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

# ============================================================
# 1. Create bucket directory structure
# ============================================================
log "Creating bucket directory"

mkdir -p "$BUCKET_DIR"/{control-to-node,node-to-control,distributables}
mkdir -p "$PKI_DIR"

# ============================================================
# 2. Download distributables (runs on host, has internet)
# ============================================================
log "Preparing distributables for air-gapped VM"

"$SCRIPT_DIR/prepare-distributables.sh" "$BUCKET_DIR"

# ============================================================
# 3. Generate PKI
# ============================================================
log "Generating PKI"

# Detect the host IP that Multipass VMs can reach.
# On macOS with Multipass, the host is typically reachable at 192.168.64.1.
HOST_IP="${HOST_IP:-192.168.64.1}"

# Generate control-plane PKI (CA + apiserver, admin, controller-manager, scheduler certs).
APISERVER_EXTRA_SANS="IP:${HOST_IP}" \
    "$SCRIPT_DIR/generate-pki.sh" "$PKI_DIR"

# Generate per-node PKI (kubelet cert + kubeconfig).
NODE_ID="$NODE_ID" \
KUBELET_APISERVER_URL="https://${HOST_IP}:30443" \
    "$SCRIPT_DIR/generate-node-pki.sh" "$PKI_DIR"

# Copy PKI files and node config into the bucket so install-from-bucket.sh
# can find them without needing multipass transfer.
mkdir -p "$BUCKET_DIR/pki"
cp "$PKI_DIR/ca.crt" "$BUCKET_DIR/pki/ca.crt"
cp "$PKI_DIR/kubelet-${NODE_ID}.kubeconfig" "$BUCKET_DIR/pki/kubelet.kubeconfig"

# Write the node ID into the bucket so the VM can pick it up.
echo "$NODE_ID" > "$BUCKET_DIR/pki/node-id"

# ============================================================
# 4. Create k3d cluster
# ============================================================
log "Creating k3d cluster: $CLUSTER_NAME"

# Delete existing cluster if present.
k3d cluster delete "$CLUSTER_NAME" 2>/dev/null || true

k3d cluster create "$CLUSTER_NAME" \
    --k3s-arg "--disable=traefik@server:0" \
    --k3s-arg "--disable=servicelb@server:0" \
    --k3s-arg "--disable=metrics-server@server:0" \
    --volume "$BUCKET_DIR:/bucket@server:0" \
    --port "30443:30443@server:0"

# Wait for k3d to be ready.
kubectl config use-context "k3d-${CLUSTER_NAME}"
kubectl wait --for=condition=Ready node --all --timeout=60s

# ============================================================
# 5. Build and load bucket-proxy-server image into k3d
# ============================================================
log "Building bucket-proxy-server container image"

# Create a minimal Dockerfile for the server.
TMPIMG=$(mktemp -d)
cp "$REPO_ROOT/bin/bucket-proxy-server-linux-${GOARCH}" "$TMPIMG/bucket-proxy-server"
cat > "$TMPIMG/Dockerfile" <<'DOCKERFILE'
FROM debian:bookworm-slim
COPY bucket-proxy-server /bucket-proxy-server
ENTRYPOINT ["/bucket-proxy-server"]
DOCKERFILE

docker build -t bucket-proxy-server:dev "$TMPIMG"
rm -rf "$TMPIMG"

k3d image import bucket-proxy-server:dev -c "$CLUSTER_NAME"

# ============================================================
# 6. Deploy overlay control plane
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

# Apply manifests in order, waiting for dependencies.
kubectl apply -f "$SCRIPT_DIR/manifests/egress-selector.yaml"
kubectl apply -f "$SCRIPT_DIR/manifests/etcd.yaml"

echo "Waiting for etcd to be ready..."
kubectl -n "$NAMESPACE" wait --for=condition=Ready pod/etcd --timeout=60s

kubectl apply -f "$SCRIPT_DIR/manifests/apiserver.yaml"

echo "Waiting for kube-apiserver to be ready..."
kubectl -n "$NAMESPACE" wait --for=condition=Ready pod/kube-apiserver --timeout=120s

kubectl apply -f "$SCRIPT_DIR/manifests/controller-manager.yaml"
kubectl apply -f "$SCRIPT_DIR/manifests/scheduler.yaml"

echo "Waiting for controller-manager and scheduler..."
sleep 5
kubectl -n "$NAMESPACE" get pods

# ============================================================
# 7. Verify overlay apiserver is reachable
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
# 8. Launch Multipass VM
# ============================================================
log "Launching Multipass VM: $VM_NAME"

# Delete existing VM if present.
multipass delete "$VM_NAME" --purge 2>/dev/null || true

echo "Node ID: $NODE_ID"

# Launch WITHOUT --mount flag. We'll add a native mount after stopping the VM,
# because:
# - The default SSHFS mount has permission issues (root can't access files)
# - Native mounts require the VM to be stopped first
# - cloud-init runcmd runs before mounts are available anyway
#
# The cloud-init.yaml contains NODE_ID_PLACEHOLDER in systemd units.
# install-from-bucket.sh reads the node ID from the bucket and substitutes it
# at install time, so no templating is needed here.
multipass launch 22.04 \
    --name "$VM_NAME" \
    --cpus 2 \
    --memory 2G \
    --disk 10G \
    --cloud-init "$SCRIPT_DIR/vm/cloud-init.yaml"

echo "Waiting for cloud-init to complete..."
multipass exec "$VM_NAME" -- cloud-init status --wait || true

# ============================================================
# 9. Set up native mount (requires VM stop/start)
# ============================================================
log "Setting up native mount for bucket directory"

multipass stop "$VM_NAME"
multipass mount "$BUCKET_DIR" "${VM_NAME}:/mnt/bucket" --type=native
multipass start "$VM_NAME"

# multipass start blocks until the VM is responsive, so just verify it works.
echo "Verifying VM is responsive..."
multipass exec "$VM_NAME" -- hostname

# Verify mount is accessible.
echo "Checking mount..."
multipass exec "$VM_NAME" -- ls /mnt/bucket/distributables/
echo "Native mount verified."

# ============================================================
# 10. Install binaries, PKI, and start services from bucket
# ============================================================
log "Installing binaries and starting services in VM"

multipass exec "$VM_NAME" -- sudo /usr/local/bin/install-from-bucket.sh

echo "Waiting for services to start..."
sleep 5
multipass exec "$VM_NAME" -- sudo systemctl status bucket-proxy-agent --no-pager || true
multipass exec "$VM_NAME" -- sudo systemctl status kubelet --no-pager || true

# ============================================================
# 11. Wait for node registration
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
echo "Bucket directory:  $BUCKET_DIR"
echo "PKI directory:     $PKI_DIR"
echo "VM name:           $VM_NAME"
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
echo "# Check bucket-proxy-agent logs in VM:"
echo "  multipass exec $VM_NAME -- sudo journalctl -u bucket-proxy-agent -f"
echo ""
echo "# Check kubelet logs in VM:"
echo "  multipass exec $VM_NAME -- sudo journalctl -u kubelet -f"
echo ""
echo "# Check bucket directory contents:"
echo "  ls -la $BUCKET_DIR/node-to-control/"
echo ""
echo "# SSH into VM:"
echo "  multipass shell $VM_NAME"
echo ""
echo "# Tear down everything:"
echo "  $SCRIPT_DIR/teardown.sh"
