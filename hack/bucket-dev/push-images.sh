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

# push-images.sh — Push container images to a GCS bucket via a temporary
# CNCF Distribution registry running in Docker.
#
# The script starts a registry container configured with the GCS storage driver,
# then uses `crane copy` to push images through it. This ensures the bucket
# layout is always consistent with what the Distribution registry expects.
#
# Prerequisites:
#   - crane (https://github.com/google/go-containerregistry/tree/main/cmd/crane)
#   - docker
#
# Environment variables:
#   GCS_CREDENTIALS_FILE — path to GCS credentials JSON file (required)
#   GCS_BUCKET           — GCS bucket name (required)
#   GCS_PREFIX           — key prefix within the bucket (default: "bucket-dev/")

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

GCS_CREDENTIALS_FILE="${GCS_CREDENTIALS_FILE:?GCS_CREDENTIALS_FILE must be set}"
GCS_BUCKET="${GCS_BUCKET:?GCS_BUCKET must be set}"
GCS_PREFIX="${GCS_PREFIX:-bucket-dev/}"

REGISTRY_VERSION="3.0.0"

# Images to push into the bucket registry.
IMAGES=(
    "registry.k8s.io/pause:3.9"
)

log() { echo "==> $*"; }

if ! command -v crane &>/dev/null; then
    echo "ERROR: crane is required but not found in PATH"
    echo "Install it: go install github.com/google/go-containerregistry/cmd/crane@latest"
    exit 1
fi

if ! command -v docker &>/dev/null; then
    echo "ERROR: docker is required but not found in PATH"
    exit 1
fi

TEMP_PORT=5050
CONTAINER_NAME="push-images-registry-$$"
TEMP_CONFIG=$(mktemp)

cleanup() {
    log "Stopping temporary registry container..."
    docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
    rm -f "$TEMP_CONFIG"
}
trap cleanup EXIT

# Generate temporary registry config with GCS backend.
# We use GOOGLE_APPLICATION_CREDENTIALS env var instead of keyfile in config,
# because the keyfile option only supports service account JSON, while the env
# var supports all credential types including authorized_user ADC.
cat > "$TEMP_CONFIG" <<EOF
version: 0.1
storage:
  gcs:
    bucket: ${GCS_BUCKET}
    rootdirectory: ${GCS_PREFIX}images
  redirect:
    disable: true
http:
  addr: 0.0.0.0:5000
EOF

log "Starting temporary registry container on port ${TEMP_PORT}..."
docker run -d --rm \
    --name "$CONTAINER_NAME" \
    -p "127.0.0.1:${TEMP_PORT}:5000" \
    -e "GOOGLE_APPLICATION_CREDENTIALS=/etc/gcs/credentials.json" \
    -v "${GCS_CREDENTIALS_FILE}:/etc/gcs/credentials.json:ro" \
    -v "${TEMP_CONFIG}:/etc/distribution/config.yml:ro" \
    "distribution/distribution:${REGISTRY_VERSION}" \
    serve /etc/distribution/config.yml

# Wait for registry to become ready.
log "Waiting for registry readiness..."
for i in $(seq 1 30); do
    if curl -sf "http://127.0.0.1:${TEMP_PORT}/v2/" >/dev/null 2>&1; then
        log "Registry is ready."
        break
    fi
    if ! docker inspect "$CONTAINER_NAME" >/dev/null 2>&1; then
        echo "ERROR: Registry container exited unexpectedly."
        docker logs "$CONTAINER_NAME" 2>&1 || true
        exit 1
    fi
    if [ "$i" -eq 30 ]; then
        echo "ERROR: Registry did not become ready in time."
        docker logs "$CONTAINER_NAME" 2>&1 || true
        exit 1
    fi
    sleep 1
done

# Push each image through the local registry.
for image in "${IMAGES[@]}"; do
    log "Pushing $image -> 127.0.0.1:${TEMP_PORT}/${image}..."
    crane copy "$image" "127.0.0.1:${TEMP_PORT}/${image}" --insecure
done

log "All images pushed successfully."
