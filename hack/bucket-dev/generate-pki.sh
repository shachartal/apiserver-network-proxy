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

# generate-pki.sh — Generate all PKI material for the overlay control plane.
#
# Usage: ./generate-pki.sh <output-dir>
#
# Produces:
#   ca.crt, ca.key                         — Overlay CA
#   apiserver.crt, apiserver.key           — kube-apiserver serving cert
#   sa.key, sa.pub                         — ServiceAccount signing key pair
#   admin.crt, admin.key                   — Admin client cert (system:masters)
#   kubelet.crt, kubelet.key               — Kubelet client cert (system:node:${NODE_ID})
#   controller-manager.crt, controller-manager.key
#   scheduler.crt, scheduler.key
#   admin.kubeconfig                       — Admin kubeconfig
#   kubelet.kubeconfig                     — Kubelet kubeconfig
#   controller-manager.kubeconfig
#   scheduler.kubeconfig

set -euo pipefail

PKI_DIR="${1:?Usage: $0 <output-dir>}"
mkdir -p "$PKI_DIR"

# The apiserver will be reachable from the VM via NodePort on the host.
# We include common SANs; the caller can set APISERVER_EXTRA_SANS if needed.
APISERVER_EXTRA_SANS="${APISERVER_EXTRA_SANS:-}"

# The node ID used in the kubelet cert and kubeconfig.
# Callers can set NODE_ID to override the default.
NODE_ID="${NODE_ID:-bucket-agent-vm}"

DAYS=3650
RSA_BITS=2048

log() { echo "==> $*"; }

# --- CA ---
gen_ca() {
    log "Generating overlay CA"
    openssl genrsa -out "$PKI_DIR/ca.key" $RSA_BITS 2>/dev/null
    openssl req -x509 -new -nodes \
        -key "$PKI_DIR/ca.key" \
        -sha256 -days $DAYS \
        -subj "/CN=overlay-ca" \
        -out "$PKI_DIR/ca.crt"
}

# --- Helper: sign a cert ---
# sign_cert <name> <subject> <extra_openssl_args...>
sign_cert() {
    local name="$1" subj="$2"
    shift 2

    openssl genrsa -out "$PKI_DIR/${name}.key" $RSA_BITS 2>/dev/null
    openssl req -new -nodes \
        -key "$PKI_DIR/${name}.key" \
        -subj "$subj" \
        -out "$PKI_DIR/${name}.csr"
    openssl x509 -req \
        -in "$PKI_DIR/${name}.csr" \
        -CA "$PKI_DIR/ca.crt" \
        -CAkey "$PKI_DIR/ca.key" \
        -CAcreateserial \
        -sha256 -days $DAYS \
        "$@" \
        -out "$PKI_DIR/${name}.crt" 2>/dev/null
    rm -f "$PKI_DIR/${name}.csr"
}

# --- Helper: generate kubeconfig ---
gen_kubeconfig() {
    local name="$1" server="$2" user="$3"
    local cert="$PKI_DIR/${name}.crt"
    local key="$PKI_DIR/${name}.key"
    local ca="$PKI_DIR/ca.crt"
    local out="$PKI_DIR/${name}.kubeconfig"

    kubectl config set-cluster overlay \
        --certificate-authority="$ca" \
        --embed-certs=true \
        --server="$server" \
        --kubeconfig="$out"

    kubectl config set-credentials "$user" \
        --client-certificate="$cert" \
        --client-key="$key" \
        --embed-certs=true \
        --kubeconfig="$out"

    kubectl config set-context default \
        --cluster=overlay \
        --user="$user" \
        --kubeconfig="$out"

    kubectl config use-context default --kubeconfig="$out"
}

# ============================================================
# Generate all certs
# ============================================================

gen_ca

# --- apiserver serving cert ---
log "Generating apiserver serving cert"
SANS="DNS:localhost,DNS:kubernetes,DNS:kubernetes.default,DNS:kubernetes.default.svc,DNS:kube-apiserver,DNS:kube-apiserver.overlay-system.svc,IP:127.0.0.1,IP:10.96.0.1"
if [ -n "$APISERVER_EXTRA_SANS" ]; then
    SANS="${SANS},${APISERVER_EXTRA_SANS}"
fi

cat > "$PKI_DIR/apiserver-ext.cnf" <<EOF
[v3_req]
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = ${SANS}
EOF

sign_cert apiserver "/CN=kube-apiserver" -extfile "$PKI_DIR/apiserver-ext.cnf" -extensions v3_req
rm -f "$PKI_DIR/apiserver-ext.cnf"

# --- ServiceAccount signing keys ---
log "Generating ServiceAccount key pair"
openssl genrsa -out "$PKI_DIR/sa.key" $RSA_BITS 2>/dev/null
openssl rsa -in "$PKI_DIR/sa.key" -pubout -out "$PKI_DIR/sa.pub" 2>/dev/null

# --- Admin client cert (system:masters) ---
log "Generating admin client cert"
cat > "$PKI_DIR/admin-ext.cnf" <<EOF
[v3_req]
basicConstraints = CA:FALSE
keyUsage = digitalSignature
extendedKeyUsage = clientAuth
EOF
sign_cert admin "/O=system:masters/CN=admin" -extfile "$PKI_DIR/admin-ext.cnf" -extensions v3_req
rm -f "$PKI_DIR/admin-ext.cnf"

# --- Kubelet client cert ---
log "Generating kubelet client cert"
cat > "$PKI_DIR/kubelet-ext.cnf" <<EOF
[v3_req]
basicConstraints = CA:FALSE
keyUsage = digitalSignature
extendedKeyUsage = clientAuth
EOF
sign_cert kubelet "/O=system:nodes/CN=system:node:${NODE_ID}" -extfile "$PKI_DIR/kubelet-ext.cnf" -extensions v3_req
rm -f "$PKI_DIR/kubelet-ext.cnf"

# --- Controller-manager client cert ---
log "Generating controller-manager client cert"
cat > "$PKI_DIR/cm-ext.cnf" <<EOF
[v3_req]
basicConstraints = CA:FALSE
keyUsage = digitalSignature
extendedKeyUsage = clientAuth
EOF
sign_cert controller-manager "/CN=system:kube-controller-manager" -extfile "$PKI_DIR/cm-ext.cnf" -extensions v3_req
rm -f "$PKI_DIR/cm-ext.cnf"

# --- Scheduler client cert ---
log "Generating scheduler client cert"
cat > "$PKI_DIR/sched-ext.cnf" <<EOF
[v3_req]
basicConstraints = CA:FALSE
keyUsage = digitalSignature
extendedKeyUsage = clientAuth
EOF
sign_cert scheduler "/CN=system:kube-scheduler" -extfile "$PKI_DIR/sched-ext.cnf" -extensions v3_req
rm -f "$PKI_DIR/sched-ext.cnf"

# ============================================================
# Generate kubeconfigs
# ============================================================

# For in-cluster components, the apiserver is at https://kube-apiserver.overlay-system.svc:6443
IN_CLUSTER_SERVER="https://kube-apiserver.overlay-system.svc:6443"

# For admin access from the host, we'll use the NodePort
HOST_SERVER="${APISERVER_URL:-https://127.0.0.1:30443}"

# For kubelet in the VM, we'll use the host IP + NodePort
VM_SERVER="${KUBELET_APISERVER_URL:-https://192.168.64.1:30443}"

log "Generating kubeconfigs"
gen_kubeconfig admin "$HOST_SERVER" admin
gen_kubeconfig kubelet "$VM_SERVER" "system:node:${NODE_ID}"
gen_kubeconfig controller-manager "$IN_CLUSTER_SERVER" "system:kube-controller-manager"
gen_kubeconfig scheduler "$IN_CLUSTER_SERVER" "system:kube-scheduler"

# ============================================================
# Summary
# ============================================================
log "PKI generation complete. Files in $PKI_DIR:"
ls -la "$PKI_DIR"
