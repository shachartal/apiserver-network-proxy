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

# redeploy-agent.sh — Rebuild and hot-deploy bucket-proxy-agent to the VM.
#
# This script:
#   1. Builds the bucket-proxy-agent binary for the target architecture
#   2. Uploads the binary to GCS
#   3. SSHs into the VM, downloads the new binary, and restarts the systemd service
#
# Usage: NODE_ID=bucket-agent-XXXX ./redeploy-agent.sh
#
# Environment variables:
#   NODE_ID              — node ID / VM name (required)
#   VM_BACKEND           — "multipass" (default) or "gcp"
#   GCS_CREDENTIALS_FILE — path to GCS credentials JSON file (required)
#   GCS_BUCKET           — GCS bucket name (required)
#   GCS_PREFIX           — key prefix within the bucket (default: "bucket-dev/")
#
# GCP-specific:
#   GCP_PROJECT          — GCP project ID
#   GCP_ZONE             — GCP zone (default: "us-east1-b")

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Auto-source demo.env if present.
if [ -f "$SCRIPT_DIR/demo.env" ]; then
    set -a
    source "$SCRIPT_DIR/demo.env"
    set +a
fi

VM_BACKEND="${VM_BACKEND:-multipass}"
NODE_ID="${NODE_ID:?NODE_ID environment variable is required}"
GCS_CREDENTIALS_FILE="${GCS_CREDENTIALS_FILE:-}"
GCS_BUCKET="${GCS_BUCKET:-}"
GCS_PREFIX="${GCS_PREFIX:-bucket-dev/}"

# GCP-specific variables.
GCP_PROJECT="${GCP_PROJECT:-}"
GCP_ZONE="${GCP_ZONE:-us-east1-b}"

# ============================================================
# Preflight checks
# ============================================================

if [ -z "$GCS_BUCKET" ]; then
    echo "ERROR: GCS_BUCKET must be set (or use demo.env)"
    exit 1
fi

if [ -z "$GCS_CREDENTIALS_FILE" ] || [ ! -f "$GCS_CREDENTIALS_FILE" ]; then
    echo "ERROR: GCS_CREDENTIALS_FILE must be set to a valid credentials file path"
    exit 1
fi

if [ "$VM_BACKEND" = "gcp" ] && [ -z "$GCP_PROJECT" ]; then
    echo "ERROR: GCP_PROJECT must be set when VM_BACKEND=gcp"
    exit 1
fi

# Detect target architecture.
if [ "$VM_BACKEND" = "gcp" ]; then
    GOARCH="amd64"
else
    HOST_ARCH="$(uname -m)"
    case "$HOST_ARCH" in
        x86_64)  GOARCH="amd64" ;;
        aarch64|arm64) GOARCH="arm64" ;;
        *) echo "ERROR: unsupported architecture $HOST_ARCH"; exit 1 ;;
    esac
fi

# ============================================================
# 1. Build the agent binary
# ============================================================

echo "==> Building bucket-proxy-agent for linux/${GOARCH}..."
cd "$REPO_ROOT"
GOOS=linux GOARCH=$GOARCH go build -o "bin/bucket-proxy-agent-linux-${GOARCH}" ./cmd/bucket-agent

# ============================================================
# 2. Upload the binary to GCS
# ============================================================

echo "==> Uploading binary to GCS..."
export CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE="$GCS_CREDENTIALS_FILE"
GCS_STAGING_PATH="gs://${GCS_BUCKET}/${GCS_PREFIX}staging/bucket-proxy-agent-linux-${GOARCH}"
gcloud storage cp "$REPO_ROOT/bin/bucket-proxy-agent-linux-${GOARCH}" "$GCS_STAGING_PATH"

# ============================================================
# 3. SSH into VM, download binary, restart service
# ============================================================

echo "==> Deploying to VM ($VM_BACKEND: $NODE_ID)..."

if [ "$VM_BACKEND" = "gcp" ]; then
    # GCP: the VM uses its service account to access GCS via the metadata server.
    gcloud compute ssh "$NODE_ID" \
        --project="$GCP_PROJECT" --zone="$GCP_ZONE" \
        --tunnel-through-iap \
        --command="$(cat <<REMOTE
set -euo pipefail
echo '==> Obtaining access token from metadata server...'
ACCESS_TOKEN=\$(curl -sf -H 'Metadata-Flavor: Google' \
  'http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token' \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['access_token'])")
ENCODED=\$(python3 -c "import urllib.parse; print(urllib.parse.quote('${GCS_PREFIX}staging/bucket-proxy-agent-linux-${GOARCH}', safe=''))")
echo '==> Downloading new binary from GCS...'
curl -sf -o /tmp/bucket-proxy-agent \
  -H "Authorization: Bearer \$ACCESS_TOKEN" \
  "https://storage.googleapis.com/storage/v1/b/${GCS_BUCKET}/o/\${ENCODED}?alt=media"
chmod +x /tmp/bucket-proxy-agent
echo '==> Stopping bucket-proxy-agent...'
sudo systemctl stop bucket-proxy-agent
sudo cp /tmp/bucket-proxy-agent /usr/local/bin/bucket-proxy-agent
echo '==> Starting bucket-proxy-agent...'
sudo systemctl start bucket-proxy-agent
rm -f /tmp/bucket-proxy-agent
echo '==> Done. Service status:'
sudo systemctl status bucket-proxy-agent --no-pager || true
REMOTE
)"
else
    # Multipass: the VM uses a credentials file for GCS access.
    multipass exec "$NODE_ID" -- bash -c "$(cat <<REMOTE
set -euo pipefail
CREDS_FILE='/etc/gcs/application_default_credentials.json'
json_get() { python3 -c "import json,sys; print(json.load(sys.stdin)['\$1'])"; }
echo '==> Obtaining access token...'
CLIENT_ID=\$(json_get client_id < \$CREDS_FILE)
CLIENT_SECRET=\$(json_get client_secret < \$CREDS_FILE)
REFRESH_TOKEN=\$(json_get refresh_token < \$CREDS_FILE)
ACCESS_TOKEN=\$(curl -sf -X POST 'https://oauth2.googleapis.com/token' \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d "client_id=\${CLIENT_ID}&client_secret=\${CLIENT_SECRET}&refresh_token=\${REFRESH_TOKEN}&grant_type=refresh_token" \
  | json_get access_token)
ENCODED=\$(python3 -c "import urllib.parse; print(urllib.parse.quote('${GCS_PREFIX}staging/bucket-proxy-agent-linux-${GOARCH}', safe=''))")
echo '==> Downloading new binary from GCS...'
curl -sf -o /tmp/bucket-proxy-agent \
  -H "Authorization: Bearer \$ACCESS_TOKEN" \
  "https://storage.googleapis.com/storage/v1/b/${GCS_BUCKET}/o/\${ENCODED}?alt=media"
chmod +x /tmp/bucket-proxy-agent
echo '==> Stopping bucket-proxy-agent...'
sudo systemctl stop bucket-proxy-agent
sudo cp /tmp/bucket-proxy-agent /usr/local/bin/bucket-proxy-agent
echo '==> Starting bucket-proxy-agent...'
sudo systemctl start bucket-proxy-agent
rm -f /tmp/bucket-proxy-agent
echo '==> Done. Service status:'
sudo systemctl status bucket-proxy-agent --no-pager || true
REMOTE
)"
fi

echo ""
echo "==> Agent redeployed successfully!"
echo ""
echo "Check logs with:"
if [ "$VM_BACKEND" = "gcp" ]; then
    echo "  gcloud compute ssh $NODE_ID --project=$GCP_PROJECT --zone=$GCP_ZONE --tunnel-through-iap --command='sudo journalctl -u bucket-proxy-agent -f'"
else
    echo "  multipass exec $NODE_ID -- sudo journalctl -u bucket-proxy-agent -f"
fi
