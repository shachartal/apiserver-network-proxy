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

# prepare-distributables.sh — Download all binaries and images needed by the
# air-gapped VM into the bucket under distributables/.
#
# Downloads BOTH amd64 and arm64 binaries. The install-from-bucket.sh script
# inside the VM detects its own architecture and picks the right files.
#
# This script runs on the HOST (with internet access). The VM reads from
# /mnt/bucket/distributables/ which maps to the same directory.
#
# Usage: ./prepare-distributables.sh <bucket-dir>

set -euo pipefail

BUCKET_DIR="${1:?Usage: $0 <bucket-dir>}"
DIST_DIR="${BUCKET_DIR}/distributables"
mkdir -p "$DIST_DIR"

K8S_VERSION="v1.30.0"
CONTAINERD_VERSION="1.7.20"
RUNC_VERSION="v1.1.13"
CNI_VERSION="v1.5.1"

log() { echo "==> $*"; }

download() {
    local url="$1" dest="$2"
    if [ -f "$dest" ]; then
        log "Already downloaded: $(basename "$dest")"
        return
    fi
    log "Downloading $(basename "$dest")..."
    curl -fsSL "$url" -o "$dest"
}

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"

# Download binaries for both architectures.
for ARCH in amd64 arm64; do
    ARCH_DIR="$DIST_DIR/$ARCH"
    mkdir -p "$ARCH_DIR/debs"

    log "--- Downloading $ARCH binaries ---"

    # Kubernetes binaries
    download "https://dl.k8s.io/release/${K8S_VERSION}/bin/linux/${ARCH}/kubelet" \
        "$ARCH_DIR/kubelet"
    download "https://dl.k8s.io/release/${K8S_VERSION}/bin/linux/${ARCH}/kubectl" \
        "$ARCH_DIR/kubectl"

    # Container runtime
    download "https://github.com/containerd/containerd/releases/download/v${CONTAINERD_VERSION}/containerd-${CONTAINERD_VERSION}-linux-${ARCH}.tar.gz" \
        "$ARCH_DIR/containerd.tar.gz"
    download "https://github.com/opencontainers/runc/releases/download/${RUNC_VERSION}/runc.${ARCH}" \
        "$ARCH_DIR/runc"
    download "https://github.com/containernetworking/plugins/releases/download/${CNI_VERSION}/cni-plugins-linux-${ARCH}-${CNI_VERSION}.tgz" \
        "$ARCH_DIR/cni-plugins.tgz"

    # Our bucket-proxy-agent binary
    AGENT_BIN="$REPO_ROOT/bin/bucket-proxy-agent-linux-${ARCH}"
    if [ -f "$AGENT_BIN" ]; then
        log "Copying bucket-proxy-agent (${ARCH})"
        cp "$AGENT_BIN" "$ARCH_DIR/bucket-proxy-agent"
    else
        log "WARNING: $AGENT_BIN not found — run 'make build-bucket-linux' first"
    fi

    # Debian packages (socat, conntrack and their deps)
    if [ "$ARCH" = "arm64" ]; then
        UBUNTU_MIRROR="http://ports.ubuntu.com/ubuntu-ports/pool"
    else
        UBUNTU_MIRROR="http://archive.ubuntu.com/ubuntu/pool"
    fi

    download "${UBUNTU_MIRROR}/main/s/socat/socat_1.7.4.1-3ubuntu4_${ARCH}.deb" \
        "$ARCH_DIR/debs/socat_${ARCH}.deb"
    download "${UBUNTU_MIRROR}/main/c/conntrack-tools/conntrack_1.4.6-2build2_${ARCH}.deb" \
        "$ARCH_DIR/debs/conntrack_${ARCH}.deb"
    download "${UBUNTU_MIRROR}/main/libn/libnetfilter-conntrack/libnetfilter-conntrack3_1.0.9-1_${ARCH}.deb" \
        "$ARCH_DIR/debs/libnetfilter-conntrack3_${ARCH}.deb"
    download "${UBUNTU_MIRROR}/main/libn/libnetfilter-cthelper/libnetfilter-cthelper0_1.0.0-1ubuntu1_${ARCH}.deb" \
        "$ARCH_DIR/debs/libnetfilter-cthelper0_${ARCH}.deb"
    download "${UBUNTU_MIRROR}/main/libn/libnetfilter-queue/libnetfilter-queue1_1.0.2-2_${ARCH}.deb" \
        "$ARCH_DIR/debs/libnetfilter-queue1_${ARCH}.deb"

    # Pause image — export per-arch.
    # docker save of cross-platform images can fail on Docker Desktop, so we
    # use a buildx-based approach: build a trivial FROM image for the target
    # platform and export it as an OCI tar.
    PAUSE_TAR="$ARCH_DIR/pause.tar"
    if [ -f "$PAUSE_TAR" ]; then
        log "Already exported: pause.tar ($ARCH)"
    elif command -v docker &>/dev/null; then
        PAUSE_IMAGE="registry.k8s.io/pause:3.9"
        log "Exporting pause image ($ARCH)..."
        TMPCTX=$(mktemp -d)
        echo "FROM --platform=linux/${ARCH} ${PAUSE_IMAGE}" > "$TMPCTX/Dockerfile"
        docker buildx build --platform "linux/${ARCH}" \
            --output "type=docker,dest=${PAUSE_TAR}" \
            --tag "${PAUSE_IMAGE}" \
            "$TMPCTX" 2>&1
        rm -rf "$TMPCTX"
    else
        log "WARNING: docker not available, skipping pause image"
    fi
done

# containerd systemd unit (arch-independent)
download "https://raw.githubusercontent.com/containerd/containerd/main/containerd.service" \
    "$DIST_DIR/containerd.service"

log ""
log "Distributables ready in $DIST_DIR:"
du -sh "$DIST_DIR"/amd64 "$DIST_DIR"/arm64 2>/dev/null || true
log "Total size: $(du -sh "$DIST_DIR" | cut -f1)"
