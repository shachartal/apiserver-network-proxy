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

# upload-distributables.sh — Download and upload static distributables to GCS.
#
# This script downloads kubelet, containerd, runc, etc. and uploads them to GCS.
# Run this ONCE when setting up a new bucket or when updating dependency versions.
# The setup.sh script assumes these are already present in GCS.
#
# Usage:
#   export GCS_BUCKET=my-bucket GCS_CREDENTIALS_FILE=/path/to/creds.json
#   ./upload-distributables.sh
#
# Environment variables:
#   GCS_CREDENTIALS_FILE — path to GCS credentials JSON file (required)
#   GCS_BUCKET           — GCS bucket name (required)
#   GCS_PREFIX           — key prefix within the bucket (default: "bucket-dev/")

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

BUCKET_DIR="/tmp/bucket-dev"
GCS_CREDENTIALS_FILE="${GCS_CREDENTIALS_FILE:-}"
GCS_BUCKET="${GCS_BUCKET:-}"
GCS_PREFIX="${GCS_PREFIX:-bucket-dev/}"

log() { echo ""; echo "===== $* ====="; echo ""; }

# ============================================================
# Preflight checks
# ============================================================
log "Preflight checks"

for cmd in gcloud curl crane; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "ERROR: $cmd is required but not found in PATH"
        exit 1
    fi
done

if [ -z "$GCS_CREDENTIALS_FILE" ] || [ ! -f "$GCS_CREDENTIALS_FILE" ]; then
    echo "ERROR: GCS_CREDENTIALS_FILE must be set to a valid credentials file path"
    exit 1
fi

if [ -z "$GCS_BUCKET" ]; then
    echo "ERROR: GCS_BUCKET must be set to a GCS bucket name"
    exit 1
fi

# ============================================================
# Check if distributables already exist in GCS
# ============================================================
log "Checking if distributables already exist in GCS"

export CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE="$GCS_CREDENTIALS_FILE"
GCS_BASE="gs://${GCS_BUCKET}/${GCS_PREFIX}"

# Check for a sentinel file to see if upload was already done.
SENTINEL="${GCS_BASE}distributables/.uploaded"
if gcloud storage ls "$SENTINEL" &>/dev/null; then
    echo "Distributables already uploaded to GCS."
    echo "To re-upload, delete the sentinel file:"
    echo "  gcloud storage rm $SENTINEL"
    echo ""
    echo "Or delete all distributables:"
    echo "  gcloud storage rm -r ${GCS_BASE}distributables/"
    exit 0
fi

# ============================================================
# Download distributables locally
# ============================================================
log "Downloading distributables"

mkdir -p "$BUCKET_DIR"
"$SCRIPT_DIR/prepare-distributables.sh" "$BUCKET_DIR"

# ============================================================
# Upload distributables to GCS
# ============================================================
log "Uploading distributables to GCS"

gcloud storage cp -r "$BUCKET_DIR/distributables" "${GCS_BASE}"

# Create sentinel file to indicate upload is complete.
echo "Uploaded at $(date -u +%Y-%m-%dT%H:%M:%SZ)" | gcloud storage cp - "$SENTINEL"

echo ""
echo "Upload complete: ${GCS_BASE}distributables/"
echo ""

# ============================================================
# Push container images to GCS
# ============================================================
log "Pushing container images to GCS"

GCS_CREDENTIALS_FILE="$GCS_CREDENTIALS_FILE" \
GCS_BUCKET="$GCS_BUCKET" \
GCS_PREFIX="$GCS_PREFIX" \
    "$SCRIPT_DIR/push-images.sh"

echo ""
log "Distributables and images uploaded successfully!"
echo ""
echo "You can now run setup.sh to deploy the environment."
