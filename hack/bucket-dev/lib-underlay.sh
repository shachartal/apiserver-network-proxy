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

# lib-underlay.sh — Shared helpers for the underlay cluster that hosts the
# overlay control plane. Sourced (not executed) by setup-control-plane.sh,
# redeploy-server.sh and teardown-control-plane.sh.
#
# The underlay is selected by UNDERLAY:
#   k3d (default) — local k3d cluster; server image imported with `k3d image
#                   import`; GCS credentials file mounted from a Secret.
#   gke           — existing GKE cluster (hack/terraform/gke-demo); images
#                   pushed to Artifact Registry (registry.k8s.io images are
#                   mirrored there, since private nodes cannot reach it); GCS
#                   access via Workload Identity; overlay apiserver reached
#                   with `kubectl port-forward`.
#
# Callers must set SCRIPT_DIR and REPO_ROOT and source demo.env first.
#
# Environment variables (UNDERLAY=gke):
#   GCP_PROJECT  — GCP project ID (required)
#   GKE_AR_REPO  — Artifact Registry path, e.g.
#                  us-east1-docker.pkg.dev/<project>/bucket-dev (required;
#                  `terraform output -raw artifact_registry_url` in gke-demo)
#   GKE_CLUSTER  — GKE cluster name (default: gke-demo)
#   GKE_REGION   — GKE cluster location (default: region of GCP_ZONE, else us-east1)
#   GKE_INSECURE_SKIP_TLS_VERIFY — "false" (default) or "true". When true, all
#                  kubectl calls against the GKE underlay pass
#                  --insecure-skip-tls-verify. Demo/non-prod only, for networks
#                  where a TLS-intercepting proxy re-signs the GKE control-plane
#                  cert with a CA kubectl does not trust. This disables server
#                  verification, so the bearer token kubectl sends is not
#                  protected against MITM. Prefer trusting the proxy's CA.
#                  The k3d path and the overlay admin.kubeconfig are unaffected.

UNDERLAY="${UNDERLAY:-k3d}"
NAMESPACE="overlay-system"
K3D_CLUSTER_NAME="bucket-dev"

# Local port the overlay admin kubeconfig points at (generate-pki.sh). On k3d
# it is a host-mapped NodePort; on GKE it is a kubectl port-forward.
OVERLAY_LOCAL_PORT=30443

# Extra flags for every kubectl call against the underlay (set per underlay).
UNDERLAY_KUBECTL_FLAGS=()

case "$UNDERLAY" in
    k3d)
        UNDERLAY_CONTEXT="k3d-${K3D_CLUSTER_NAME}"
        case "$(uname -m)" in
            x86_64)        SERVER_GOARCH="amd64" ;;
            aarch64|arm64) SERVER_GOARCH="arm64" ;;
            *) echo "ERROR: unsupported architecture $(uname -m)"; exit 1 ;;
        esac
        # Pods start immediately on the existing k3d node.
        ETCD_WAIT_TIMEOUT=60s
        APISERVER_WAIT_TIMEOUT=120s
        ;;
    gke)
        GCP_PROJECT="${GCP_PROJECT:-}"
        GKE_AR_REPO="${GKE_AR_REPO:-}"
        GKE_CLUSTER="${GKE_CLUSTER:-gke-demo}"
        if [ -n "${GCP_ZONE:-}" ]; then
            GKE_REGION="${GKE_REGION:-${GCP_ZONE%-*}}"
        else
            GKE_REGION="${GKE_REGION:-us-east1}"
        fi
        UNDERLAY_CONTEXT="gke_${GCP_PROJECT}_${GKE_REGION}_${GKE_CLUSTER}"
        # Autopilot nodes are amd64 by default.
        SERVER_GOARCH="amd64"
        # The first pod may wait for Autopilot to provision a node.
        ETCD_WAIT_TIMEOUT=600s
        APISERVER_WAIT_TIMEOUT=300s
        if [ "${GKE_INSECURE_SKIP_TLS_VERIFY:-false}" = "true" ]; then
            UNDERLAY_KUBECTL_FLAGS=(--insecure-skip-tls-verify)
        fi
        ;;
    *)
        echo "ERROR: UNDERLAY must be 'k3d' or 'gke' (got '$UNDERLAY')"
        exit 1
        ;;
esac

# Image reference used for the bucket-proxy-server sidecar. On k3d this is the
# locally imported tag; on GKE it is replaced by an Artifact Registry digest
# reference after push_server_image_gke.
SERVER_IMAGE="bucket-proxy-server:dev"

is_gke() { [ "$UNDERLAY" = "gke" ]; }

# kubectl against the underlay, independent of the current kubectl context.
# The ${arr[@]+...} form keeps an empty array safe under `set -u` on bash 3.2.
ukubectl() {
    kubectl ${UNDERLAY_KUBECTL_FLAGS[@]+"${UNDERLAY_KUBECTL_FLAGS[@]}"} \
        --context "$UNDERLAY_CONTEXT" "$@"
}

# Fail fast if required UNDERLAY=gke settings are missing.
underlay_check_gke_vars() {
    is_gke || return 0
    local var
    for var in GCP_PROJECT GKE_AR_REPO; do
        if [ -z "${!var}" ]; then
            echo "ERROR: $var must be set when UNDERLAY=gke"
            exit 1
        fi
    done
    # Catch copy-paste damage (e.g. zsh's trailing "%" after `terraform output
    # -raw`, quotes, whitespace) before it surfaces as an opaque crane error.
    if ! [[ "$GKE_AR_REPO" =~ ^[a-z0-9-]+-docker\.pkg\.dev/[a-z0-9-]+/[a-z0-9._-]+$ ]]; then
        echo "ERROR: GKE_AR_REPO is not a valid Artifact Registry path: '$GKE_AR_REPO'"
        echo "Expected: <region>-docker.pkg.dev/<project>/<repo>"
        echo "Get it with: terraform -chdir=hack/terraform/gke-demo output -raw artifact_registry_url; echo"
        exit 1
    fi
}

# Ensure a kubeconfig context exists for the GKE cluster. Fetches credentials
# only if the context is missing (does not change it otherwise).
gke_ensure_context() {
    if ! kubectl config get-contexts "$UNDERLAY_CONTEXT" >/dev/null 2>&1; then
        gcloud container clusters get-credentials "$GKE_CLUSTER" \
            --region "$GKE_REGION" --project "$GCP_PROJECT"
    fi
    local err
    if ! err=$(ukubectl get --raw /readyz 2>&1 >/dev/null); then
        echo "ERROR: Cannot reach GKE cluster $GKE_CLUSTER (context $UNDERLAY_CONTEXT)."
        echo "kubectl said:"
        echo "$err" | sed 's/^/    /'
        echo "Common causes: your egress IP is not in the cluster's master authorized"
        echo "networks (global_authorized_networks in gke-demo), gke-gcloud-auth-plugin"
        echo "missing, or expired gcloud login."
        echo "x509 errors: a TLS-intercepting proxy; see GKE_INSECURE_SKIP_TLS_VERIFY."
        exit 1
    fi
}

# Log docker (and therefore crane, which reads the docker config) into the
# Artifact Registry host with a short-lived gcloud access token.
ar_login() {
    local host="${GKE_AR_REPO%%/*}"
    gcloud auth print-access-token |
        docker login -u oauth2accesstoken --password-stdin "https://${host}" >/dev/null
}

# Container images referenced from registry.k8s.io by the overlay manifests.
control_plane_images() {
    grep -ho 'image: registry.k8s.io/[^ ]*' "$SCRIPT_DIR"/manifests/*.yaml |
        sed 's/^image: //' | sort -u
}

# Download each registry.k8s.io image and push it to Artifact Registry under
# <GKE_AR_REPO>/registry.k8s.io/... Skips images already present.
mirror_control_plane_images() {
    local image dest
    for image in $(control_plane_images); do
        dest="${GKE_AR_REPO}/${image}"
        if crane digest "$dest" >/dev/null 2>&1; then
            echo "Already mirrored: $dest"
            continue
        fi
        echo "Mirroring $image -> $dest"
        crane copy "$image" "$dest"
    done
}

# build_server_image <goarch> <image-ref>
# Builds the bucket-proxy-server image from the prebuilt linux binary.
build_server_image() {
    local goarch="$1" ref="$2" tmp
    local bin="$REPO_ROOT/bin/bucket-proxy-server-linux-${goarch}"
    if [ ! -f "$bin" ]; then
        echo "ERROR: Server binary not found ($bin)."
        echo "Run 'make build-bucket-linux' first."
        exit 1
    fi
    tmp=$(mktemp -d)
    cp "$bin" "$tmp/bucket-proxy-server"
    cat > "$tmp/Dockerfile" <<'DOCKERFILE'
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY bucket-proxy-server /bucket-proxy-server
ENTRYPOINT ["/bucket-proxy-server"]
DOCKERFILE
    docker build --platform "linux/${goarch}" -t "$ref" "$tmp"
    rm -rf "$tmp"
}

# Build the server image for amd64, push it to Artifact Registry with a unique
# tag, and set SERVER_IMAGE to the immutable digest reference so a redeploy
# always pulls the new image.
push_server_image_gke() {
    local ref="${GKE_AR_REPO}/bucket-proxy-server:dev-$(date +%Y%m%d-%H%M%S)"
    build_server_image "$SERVER_GOARCH" "$ref"
    docker push "$ref"
    SERVER_IMAGE="${GKE_AR_REPO}/bucket-proxy-server@$(crane digest "$ref")"
    echo "Server image: $SERVER_IMAGE"
}

# render_manifest <file> — print a manifest with placeholders filled in.
# On GKE, also rewrites image references to Artifact Registry and drops lines
# marked "# k3d-only" (the GCS credentials-file flag, mount and volume).
render_manifest() {
    local file="$1"
    local -a args=(
        -e "s|GCS_BUCKET_PLACEHOLDER|${GCS_BUCKET}|g"
        -e "s|GCS_PREFIX_PLACEHOLDER|${GCS_PREFIX}|g"
        -e "s|image: bucket-proxy-server:dev|image: ${SERVER_IMAGE}|"
    )
    if is_gke; then
        args+=(
            -e '/# k3d-only$/d'
            -e "s|image: registry.k8s.io/|image: ${GKE_AR_REPO}/registry.k8s.io/|"
        )
    fi
    sed "${args[@]}" "$file"
}

# apply_manifest <name> — render and apply manifests/<name>.yaml.
apply_manifest() {
    render_manifest "$SCRIPT_DIR/manifests/$1.yaml" | ukubectl apply -f -
}

# ------------------------------------------------------------
# Overlay apiserver access on GKE.
#
# The admin kubeconfig points at https://127.0.0.1:30443. On k3d that is a
# host-mapped NodePort. On GKE the nodes are private, so a background
# `kubectl port-forward` provides the same local endpoint. It runs detached so
# it outlives the setup script; its PID is kept next to the PKI.
# ------------------------------------------------------------

PORT_FORWARD_PID_FILE="/tmp/bucket-dev-pki/port-forward.pid"
PORT_FORWARD_LOG="/tmp/bucket-dev-pki/port-forward.log"

stop_overlay_port_forward() {
    is_gke || return 0
    if [ -f "$PORT_FORWARD_PID_FILE" ]; then
        kill "$(cat "$PORT_FORWARD_PID_FILE")" 2>/dev/null || true
        rm -f "$PORT_FORWARD_PID_FILE"
    fi
}

start_overlay_port_forward() {
    is_gke || return 0
    stop_overlay_port_forward
    mkdir -p "$(dirname "$PORT_FORWARD_PID_FILE")"
    nohup kubectl ${UNDERLAY_KUBECTL_FLAGS[@]+"${UNDERLAY_KUBECTL_FLAGS[@]}"} \
        --context "$UNDERLAY_CONTEXT" -n "$NAMESPACE" \
        port-forward svc/kube-apiserver "${OVERLAY_LOCAL_PORT}:6443" \
        >"$PORT_FORWARD_LOG" 2>&1 &
    echo $! > "$PORT_FORWARD_PID_FILE"
    echo "Started port-forward 127.0.0.1:${OVERLAY_LOCAL_PORT} -> svc/kube-apiserver (pid $!, log $PORT_FORWARD_LOG)"
    echo "If it drops (e.g. pod restart), rerun:"
    echo "  UNDERLAY=gke $SCRIPT_DIR/setup-control-plane.sh --port-forward"
}
