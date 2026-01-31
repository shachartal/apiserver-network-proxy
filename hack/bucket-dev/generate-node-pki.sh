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

# generate-node-pki.sh — Generate per-node PKI material (kubelet cert + kubeconfig).
#
# Uses the existing CA from the control-plane PKI directory.
# Can be called multiple times for different nodes without affecting the CA
# or any other control-plane certs.
#
# Usage: NODE_ID=<node-id> ./generate-node-pki.sh <pki-dir>
#
# Requires:
#   - <pki-dir>/ca.crt and <pki-dir>/ca.key to exist (from generate-pki.sh)
#   - NODE_ID environment variable
#
# Produces:
#   kubelet-<node-id>.crt, kubelet-<node-id>.key
#   kubelet-<node-id>.kubeconfig

set -euo pipefail

PKI_DIR="${1:?Usage: NODE_ID=<id> $0 <pki-dir>}"
NODE_ID="${NODE_ID:?NODE_ID environment variable is required}"

if [ ! -f "$PKI_DIR/ca.crt" ] || [ ! -f "$PKI_DIR/ca.key" ]; then
    echo "ERROR: CA files not found in $PKI_DIR. Run generate-pki.sh first."
    exit 1
fi

DAYS=3650
RSA_BITS=2048

log() { echo "==> $*"; }

# --- Helper: sign a cert ---
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

# Use a node-specific filename to allow multiple nodes' certs to coexist.
CERT_NAME="kubelet-${NODE_ID}"

log "Generating kubelet client cert for node ${NODE_ID}"
cat > "$PKI_DIR/${CERT_NAME}-ext.cnf" <<EOF
[v3_req]
basicConstraints = CA:FALSE
keyUsage = digitalSignature
extendedKeyUsage = clientAuth
EOF
sign_cert "$CERT_NAME" "/O=system:nodes/CN=system:node:${NODE_ID}" \
    -extfile "$PKI_DIR/${CERT_NAME}-ext.cnf" -extensions v3_req
rm -f "$PKI_DIR/${CERT_NAME}-ext.cnf"

# --- Generate kubeconfig ---
# For kubelet in the VM, use the host IP + NodePort.
VM_SERVER="${KUBELET_APISERVER_URL:-https://192.168.64.1:30443}"

log "Generating kubelet kubeconfig for node ${NODE_ID} (server: ${VM_SERVER})"
KUBECONFIG_FILE="$PKI_DIR/${CERT_NAME}.kubeconfig"

kubectl config set-cluster overlay \
    --certificate-authority="$PKI_DIR/ca.crt" \
    --embed-certs=true \
    --server="$VM_SERVER" \
    --kubeconfig="$KUBECONFIG_FILE"

kubectl config set-credentials "system:node:${NODE_ID}" \
    --client-certificate="$PKI_DIR/${CERT_NAME}.crt" \
    --client-key="$PKI_DIR/${CERT_NAME}.key" \
    --embed-certs=true \
    --kubeconfig="$KUBECONFIG_FILE"

kubectl config set-context default \
    --cluster=overlay \
    --user="system:node:${NODE_ID}" \
    --kubeconfig="$KUBECONFIG_FILE"

kubectl config use-context default --kubeconfig="$KUBECONFIG_FILE"

log "Node PKI complete for ${NODE_ID}:"
ls -la "$PKI_DIR/${CERT_NAME}".*
