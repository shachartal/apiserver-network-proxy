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

# cleanup-bucket.sh — Delete all transport messages from the GCS bucket.
#
# Removes all objects under the three transport prefixes:
#   - control-to-node/    (server → agent messages)
#   - node-to-control/    (agent → server messages, heartbeats, registrations)
#   - node-to-control-reverse/  (reverse tunnel messages)
#
# Does NOT touch distributables/, pki/, or staging/ — only transport data.
#
# Environment variables:
#   GCS_CREDENTIALS_FILE — path to GCS credentials JSON file (required)
#   GCS_BUCKET           — GCS bucket name (required)
#   GCS_PREFIX           — key prefix within the bucket (default: "bucket-dev/")

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Auto-source demo.env if present.
if [ -f "$SCRIPT_DIR/demo.env" ]; then
    set -a
    source "$SCRIPT_DIR/demo.env"
    set +a
fi

GCS_CREDENTIALS_FILE="${GCS_CREDENTIALS_FILE:-}"
GCS_BUCKET="${GCS_BUCKET:-}"
GCS_PREFIX="${GCS_PREFIX:-bucket-dev/}"

log() { echo "==> $*"; }

if [ -z "$GCS_CREDENTIALS_FILE" ] || [ ! -f "$GCS_CREDENTIALS_FILE" ]; then
    echo "ERROR: GCS_CREDENTIALS_FILE must be set to a valid credentials file path"
    exit 1
fi

if [ -z "$GCS_BUCKET" ]; then
    echo "ERROR: GCS_BUCKET must be set to a GCS bucket name"
    exit 1
fi

export CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE="$GCS_CREDENTIALS_FILE"
GCS_BASE="gs://${GCS_BUCKET}/${GCS_PREFIX}"

TRANSPORT_PREFIXES=(
    "control-to-node/"
    "node-to-control/"
    "node-to-control-reverse/"
)

for prefix in "${TRANSPORT_PREFIXES[@]}"; do
    path="${GCS_BASE}${prefix}"
    if gcloud storage ls "$path" &>/dev/null; then
        log "Deleting $path**"
        gcloud storage rm -r "$path" 2>/dev/null || true
    else
        log "No objects at $path (skipping)"
    fi
done

log "Bucket transport data cleaned."
