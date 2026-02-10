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

# teardown-control-plane.sh — Tear down the overlay control plane.
#
# Deletes the k3d cluster, local staging/PKI directories, and the Docker image.
# Worker VMs should be torn down first (via teardown-worker.sh or teardown.sh).

set -euo pipefail

CLUSTER_NAME="bucket-dev"
BUCKET_DIR="/tmp/bucket-dev"
PKI_DIR="/tmp/bucket-dev-pki"

log() { echo "==> $*"; }

log "Deleting k3d cluster: $CLUSTER_NAME"
k3d cluster delete "$CLUSTER_NAME" 2>/dev/null || true

log "Removing bucket directory: $BUCKET_DIR"
rm -rf "$BUCKET_DIR" 2>/dev/null || true

log "Removing PKI directory: $PKI_DIR"
rm -rf "$PKI_DIR"

log "Removing bucket-proxy-server Docker image"
docker rmi bucket-proxy-server:dev 2>/dev/null || true

log "Control plane teardown complete."
