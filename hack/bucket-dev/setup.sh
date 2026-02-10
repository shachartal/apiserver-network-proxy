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
# Convenience wrapper that creates the control plane and one worker VM.
# For more control, run setup-control-plane.sh and setup-worker.sh separately.
#
# Architecture:
#   - k3d cluster (underlay) running overlay control plane pods
#   - Worker VM running kubelet + bucket-proxy-agent (multipass or GCP)
#   - GCS bucket for transport and distributable storage
#
# Prerequisites:
#   - docker, k3d, kubectl, openssl, crane
#   - multipass (for VM_BACKEND=multipass) or gcloud (for VM_BACKEND=gcp)
#   - Run 'make build-bucket-linux' first to build the linux binaries
#   - Run './upload-distributables.sh' once to upload kubelet, containerd, etc. to GCS
#
# Environment variables:
#   VM_BACKEND           — "multipass" (default) or "gcp"
#   GCS_CREDENTIALS_FILE — path to GCS credentials JSON file (required)
#   GCS_BUCKET           — GCS bucket name (required)
#   GCS_PREFIX           — key prefix within the bucket (default: "bucket-dev/")
#
# GCP-specific environment variables (required when VM_BACKEND=gcp):
#   GCP_PROJECT          — GCP project ID
#   GCP_ZONE             — GCP zone (default: "us-east1-b")
#   GCP_NETWORK          — VPC name (from terraform output)
#   GCP_SUBNET           — Subnet self-link (from terraform output)
#   GCP_SERVICE_ACCOUNT  — Worker node service account email (from terraform output)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Phase 1: Control plane (k3d, PKI, overlay pods).
"$SCRIPT_DIR/setup-control-plane.sh"

# Phase 2: Worker VM (per-node PKI, VM launch, node registration).
"$SCRIPT_DIR/setup-worker.sh"
