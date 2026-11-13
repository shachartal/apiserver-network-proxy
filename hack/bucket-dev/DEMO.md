# Bucket Transport Demo Guide

End-to-end demo of the Konnectivity bucket transport: a Kubernetes API server
communicating with kubelets via GCS instead of gRPC.

## What you're demonstrating

A Kubernetes control plane (kube-apiserver, etcd, controller-manager, scheduler)
runs in a k3d cluster on your laptop (or, with `UNDERLAY=gke`, in a GKE
Autopilot cluster; see [GKE underlay](#gke-underlay-optional)). One or more
worker VMs run a kubelet and
bucket-proxy-agent. The two sides communicate **exclusively through a GCS
bucket** -- no direct network path exists between them. Every `kubectl exec`,
pod creation, and log fetch flows as protobuf files through cloud storage.

```
┌─ Your laptop (k3d) ──────────────────────┐       ┌─ Worker VM ──────────────┐
│                                          │       │                          │
│  kube-apiserver ──UDS──▸ bucket-proxy-   │       │  bucket-proxy-  ──────▸  │
│                          server          │       │  agent           kubelet │
│                            │             │       │    │                     │
└────────────────────────────┼─────────────┘       └────┼─────────────────────┘
                             │                          │
                             ▼     GCS Bucket           ▼
                        ┌──────────────────────────────────────────────────┐
                        │  control-to-node/{id}/fwd/{s}-{seq}.pb           │
                        │  control-to-node/{id}/rev/{s}-{seq}.pb           │
                        │  node-to-control/{id}/{s}-{seq}.pb               │
                        │  node-to-control-reverse/{id}/{s}-{seq}.pb       │
                        │  node-to-control/{id}/heartbeat-*                │
                        │  node-to-control/{id}/register                   │
                        └──────────────────────────────────────────────────┘
```

## Prerequisites

Install these on your Mac:

| Tool | Install | Purpose |
|------|---------|---------|
| Docker Desktop | `brew install --cask docker` | Container runtime for k3d |
| k3d | `brew install k3d` | Local Kubernetes cluster |
| kubectl | `brew install kubectl` | Kubernetes CLI |
| gcloud | `brew install --cask google-cloud-sdk` | GCS access + GCP VM management |
| crane | `brew install crane` | Push container images to GCS registry |
| openssl | (preinstalled on macOS) | PKI generation |

You also need:

- A **GCS bucket** (e.g. `my-demo-bucket`) with appropriate permissions
- A **GCS credentials file** (service account key JSON) with `roles/storage.admin` on the bucket

For the GCP VM backend (recommended for the "zero network connectivity" demo),
you additionally need the Terraform-provisioned VPC from `hack/terraform/demo-env/`.

## Quick start (GCP VM, ~10 minutes)

### Step 0: One-time setup

Build the Linux binaries and upload distributables to GCS. You only need to do
this once (or when you change binary code).

```bash
# Build binaries
make build-bucket-linux

# Create demo.env from template (see Step 1)
cd hack/bucket-dev
cp demo.env.example demo.env
# Edit demo.env with your values

# Upload kubelet, containerd, CNI plugins, bucket-proxy-agent, and container
# images to GCS. Takes ~5 minutes on first run.
./upload-distributables.sh
```

### Step 1: Spin up everything

Create a `demo.env` from the template and fill in your values:

```bash
cd hack/bucket-dev
cp demo.env.example demo.env
# Edit demo.env with your GCS bucket, credentials, GCP project, etc.
```

Then run setup (all scripts auto-source `demo.env`):

```bash
# Option A: all-in-one (control plane + one worker)
./setup.sh

# Option B: step by step
./setup-control-plane.sh   # k3d cluster, PKI, overlay pods
./setup-worker.sh           # launches one worker VM (auto-generates NODE_ID)
```

This creates:
1. A k3d cluster with overlay control plane pods (etcd, apiserver, controller-manager, scheduler)
2. A GCP VM with zero internet access, running kubelet + bucket-proxy-agent
3. PKI in `/tmp/bucket-dev-pki/`

### Step 2: Verify the node registered

```bash
# Use the overlay cluster's admin kubeconfig
export KUBECONFIG=/tmp/bucket-dev-pki/admin.kubeconfig

kubectl get nodes
```

Expected output:
```
NAME                    STATUS   ROLES    AGE   VERSION
bucket-agent-a1b2c3d4   Ready    <none>   30s   v1.30.0
```

### Step 3: Add more workers (optional)

```bash
# Each invocation creates a new worker with a unique NODE_ID
./setup-worker.sh
./setup-worker.sh

# Or supply a specific NODE_ID
NODE_ID=bucket-agent-mynode ./setup-worker.sh

kubectl get nodes   # shows all registered workers
```

## Demo flows

All commands below assume `KUBECONFIG=/tmp/bucket-dev-pki/admin.kubeconfig`.

### Flow 1: Create a pod (proves full kubelet lifecycle through bucket)

```bash
kubectl run demo --image=busybox:1.36 --restart=Never -- sleep 3600
kubectl get pods -w
```

Wait for `Running`. This proves:
- apiserver DIAL_REQ traveled through GCS to the agent
- agent connected to kubelet, relayed CRI calls
- kubelet pulled the image from the GCS-backed OCI registry
- Pod status updates flowed back through GCS

### Flow 2: Interactive exec (proves bidirectional streaming)

```bash
kubectl exec -it demo -- sh
```

Inside the shell:
```bash
hostname
echo "hello from $(hostname)"
exit
```

Every keystroke flows as a protobuf DATA packet through GCS. This is the most
visceral demo -- you're typing through cloud storage.

### Flow 3: Pod logs (proves streaming reads)

```bash
# Start a pod that generates output
kubectl run logger --image=busybox:1.36 --restart=Never -- sh -c \
  'i=0; while true; do echo "line $i at $(date)"; i=$((i+1)); sleep 2; done'

# Stream logs (each line flows through GCS)
kubectl logs logger -f
```

Press Ctrl-C to stop following. You should see ~50ms latency between log lines.

### Flow 4: Check GCS bucket contents (proves it's really using storage)

```bash
# Show the live message traffic
gcloud storage ls "gs://${GCS_BUCKET}/bucket-dev/node-to-control/" --recursive

# You'll see heartbeat files and the registration marker:
# gs://my-bucket/bucket-dev/node-to-control/bucket-agent-xxxxx/heartbeat-00000000000000000042-1707400000000.hb
# gs://my-bucket/bucket-dev/node-to-control/bucket-agent-xxxxx/register
```

### Flow 5: Node liveness (proves heartbeat + registration)

Watch the server detect heartbeats:
```bash
kubectl -n overlay-system logs deploy/kube-apiserver -c bucket-proxy-server -f | grep -i heartbeat
```

You'll see:
```
"Heartbeat received" nodeID="bucket-agent-xxxxx"
```

### Flow 6: Reverse proxy (proves kubelet API access through bucket)

The reverse proxy lets the apiserver reach the kubelet's HTTPS API through the
bucket transport (for `kubectl logs`, `kubectl exec`, node status, etc.). This
is already working if flows 2 and 3 succeeded.

To see it explicitly in the server logs:
```bash
kubectl -n overlay-system logs deploy/kube-apiserver -c bucket-proxy-server -f | grep -i reverse
```

### Flow 7: Cost benchmark

Run a 10-minute benchmark that deploys a workload pod and measures GCS API calls:

```bash
cd hack/bucket-dev
NODE_ID=bucket-agent-XXXX BENCHMARK_DURATION=600 ./benchmark-costs.sh
```

This produces a cost projection for 10, 100, and 1000 nodes.

## Observing the system

### Server logs (bucket-proxy-server)

```bash
kubectl -n overlay-system logs deploy/kube-apiserver -c bucket-proxy-server -f
```

Key things to watch for:
- `Discovered new agent "..." via heartbeat — registering` -- node registration
- `Heartbeat received` -- periodic liveness
- `RegionalPoller: node registered` -- poller started dispatching messages

### Agent logs (bucket-proxy-agent on VM)

Replace `$NODE_ID` with the actual node ID (e.g. `bucket-agent-a1b2c3d4`).

For GCP:
```bash
gcloud compute ssh $NODE_ID \
  --project=$GCP_PROJECT --zone=$GCP_ZONE --tunnel-through-iap \
  --command='sudo journalctl -u bucket-proxy-agent -f'
```

For multipass:
```bash
multipass exec $NODE_ID -- sudo journalctl -u bucket-proxy-agent -f
```

### Kubelet logs

For GCP:
```bash
gcloud compute ssh $NODE_ID \
  --project=$GCP_PROJECT --zone=$GCP_ZONE --tunnel-through-iap \
  --command='sudo journalctl -u kubelet -f'
```

For multipass:
```bash
multipass exec $NODE_ID -- sudo journalctl -u kubelet -f
```

### Prometheus metrics

Server metrics (via port-forward):
```bash
kubectl -n overlay-system port-forward deploy/kube-apiserver 8095:8095 &
curl -s http://localhost:8095/metrics | grep konnectivity
```

Agent metrics:
```bash
# GCP
gcloud compute ssh $NODE_ID \
  --project=$GCP_PROJECT --zone=$GCP_ZONE --tunnel-through-iap \
  --command='curl -s http://127.0.0.1:8094/metrics' | grep konnectivity

# Multipass
multipass exec $NODE_ID -- curl -s http://127.0.0.1:8094/metrics | grep konnectivity
```

### Overlay control plane health

```bash
# All overlay pods should be Running
kubectl -n overlay-system get pods

# Overlay apiserver health
kubectl --kubeconfig=/tmp/bucket-dev-pki/admin.kubeconfig get --raw /healthz
```

## Iterating on code changes

After modifying Go code:

```bash
cd hack/bucket-dev

# Server changes only — rebuilds binary, Docker image, restarts pod (~30s)
./redeploy-server.sh

# Agent changes only — rebuilds binary, uploads to GCS, restarts service on VM (~30s)
NODE_ID=bucket-agent-XXXX ./redeploy-agent.sh
```

To re-upload distributables (only changed files are uploaded):
```bash
./upload-distributables.sh
```

The overlay control plane runs as controllers (etcd StatefulSet with a 1Gi
PVC; apiserver, controller-manager and scheduler Deployments), so pods that
are deleted or preempted are recreated automatically and etcd keeps its data.

**Note on agent startup timing**: The RegionalPoller implements a 5-minute grace
period for messages from unregistered nodes. This means agents can safely start
before the server discovers them via heartbeat — their early messages won't be
deleted, and will be processed once the server registers the node.

## Teardown

```bash
cd hack/bucket-dev

# Tear down a single worker (keeps control plane running)
NODE_ID=bucket-agent-XXXX ./teardown-worker.sh

# Tear down just the control plane (workers should be removed first)
./teardown-control-plane.sh

# Delete all transport messages from GCS (heartbeats, data, registrations)
./cleanup-bucket.sh

# Tear down everything (discovers all workers, cleans bucket, removes control plane)
./teardown.sh
```

`teardown.sh` discovers all worker VMs by querying both the overlay cluster and
the VM backend for names matching `bucket-agent-*`. It also runs
`cleanup-bucket.sh` to remove stale transport data from GCS.

It does **not** delete GCS distributables (so next setup is faster).

To also clean up GCS distributables:
```bash
gcloud storage rm -r "gs://${GCS_BUCKET}/bucket-dev/"
```

## Multipass variant (simpler, no GCP needed)

If you don't have a GCP project, you can use multipass for a local VM:

```bash
brew install multipass

cd hack/bucket-dev
cp demo.env.example demo.env
# Edit demo.env: set GCS_BUCKET, GCS_CREDENTIALS_FILE, VM_BACKEND=multipass
./setup.sh
```

Everything else works identically. The only difference: the VM has internet
access (it's local), so this variant doesn't demonstrate the "zero connectivity"
aspect as dramatically as the GCP variant with its deny-all-egress firewall.

## GKE underlay (optional)

By default the overlay control plane runs in a local k3d cluster. With
`UNDERLAY=gke` it runs in a GKE Autopilot cluster instead, which keeps it up
independently of your laptop. Worker VMs (`VM_BACKEND`) are unaffected.

What changes on GKE:

- The cluster comes from `hack/terraform/gke-demo/`: a private Autopilot
  cluster (no node external IPs, no Cloud NAT) whose control-plane endpoint
  accepts only `global_authorized_networks`, plus an Artifact Registry repo and
  a comms bucket.
- Images go to Artifact Registry. The registry.k8s.io control-plane images are
  mirrored there, since the private nodes cannot reach registry.k8s.io.
- The bucket-proxy-server sidecar reaches GCS via Workload Identity, so no
  credentials file is mounted.
- The overlay apiserver is reached with a background `kubectl port-forward` on
  `127.0.0.1:30443` (the same address as the k3d NodePort, so the admin
  kubeconfig is unchanged).

Additional prerequisites: `terraform` and `gke-gcloud-auth-plugin`
(`gcloud components install gke-gcloud-auth-plugin`).

```bash
# 1. Provision the cluster. global_authorized_networks is required: list the
#    egress IP(s) you'll run kubectl from.
cd hack/terraform/gke-demo
cat > terraform.tfvars <<'TFVARS'
project_id = "my-gcp-project"
global_authorized_networks = [
  { cidr_block = "203.0.113.7/32", display_name = "my-laptop" },
]
# labels = { team = "infra" }   # optional, e.g. if an org policy requires labels
TFVARS
terraform init && terraform apply

# 2. Point the scripts at it (in hack/bucket-dev/demo.env).
#    GCS_BUCKET  = terraform output -raw comms_bucket_name
#    GKE_AR_REPO = terraform output -raw artifact_registry_url
UNDERLAY=gke
GKE_AR_REPO=us-east1-docker.pkg.dev/my-gcp-project/bucket-dev

# 3. Same scripts as before.
cd ../../bucket-dev
./setup-control-plane.sh
./setup-worker.sh
```

If the port-forward drops (e.g. the apiserver pod restarted), restore it with
`./setup-control-plane.sh --port-forward`. `redeploy-server.sh` restarts it
automatically.

`teardown-control-plane.sh` deletes only the `overlay-system` namespace; the
cluster, registry and bucket stay until `terraform destroy` in
`hack/terraform/gke-demo/`.

If a TLS-intercepting proxy on your network re-signs the GKE endpoint's
certificate, kubectl fails with x509 errors. Trust the proxy's CA, or (demo
only) set `GKE_INSECURE_SKIP_TLS_VERIFY=true`.

## Architecture notes

**Per-stream message isolation**: Each Konnectivity connection (identified by the
DIAL request's `Random` field) gets its own independent sequence counter in the
bucket. Filenames use the format `{streamID}-{seq}.pb`. This means a failed GCS
write for one stream only blocks that stream — other streams on the same node
continue delivering messages without head-of-line blocking.

**Message retention for unregistered nodes**: The RegionalPoller preserves
messages from unregistered nodes for 5 minutes before deleting them. This grace
period allows agents to start before server discovery completes (via heartbeat),
preventing message loss during startup. Once a node registers, any buffered
messages are processed immediately in per-stream sequence order.
