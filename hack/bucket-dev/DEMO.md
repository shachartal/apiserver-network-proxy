# Bucket Transport Demo Guide

End-to-end demo of the Konnectivity bucket transport: a Kubernetes API server
communicating with kubelets via GCS instead of gRPC.

## What you're demonstrating

A Kubernetes control plane (kube-apiserver, etcd, controller-manager, scheduler)
runs in a k3d cluster on your laptop. A worker VM runs a kubelet and
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
                        ┌────────────────────────────────────┐
                        │  control-to-node/{id}/msg-*.pb     │
                        │  node-to-control/{id}/msg-*.pb     │
                        │  node-to-control/{id}/heartbeat-*  │
                        │  node-to-control/{id}/register     │
                        └────────────────────────────────────┘
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
./setup.sh
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
kubectl -n overlay-system logs kube-apiserver -c bucket-proxy-server -f | grep -i heartbeat
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
kubectl -n overlay-system logs kube-apiserver -c bucket-proxy-server -f | grep -i reverse
```

### Flow 7: Cost benchmark

Run a 10-minute benchmark that deploys a workload pod and measures GCS API calls:

```bash
cd hack/bucket-dev
BENCHMARK_DURATION=600 ./benchmark-costs.sh
```

This produces a cost projection for 10, 100, and 1000 nodes.

## Observing the system

### Server logs (bucket-proxy-server)

```bash
kubectl -n overlay-system logs kube-apiserver -c bucket-proxy-server -f
```

Key things to watch for:
- `Discovered new agent "..." via heartbeat — registering` -- node registration
- `Heartbeat received` -- periodic liveness
- `RegionalPoller: node registered` -- poller started dispatching messages

### Agent logs (bucket-proxy-agent on VM)

For GCP:
```bash
gcloud compute ssh bucket-agent-vm \
  --project=$GCP_PROJECT --zone=$GCP_ZONE --tunnel-through-iap \
  --command='sudo journalctl -u bucket-proxy-agent -f'
```

For multipass:
```bash
multipass exec bucket-agent-vm -- sudo journalctl -u bucket-proxy-agent -f
```

### Kubelet logs

For GCP:
```bash
gcloud compute ssh bucket-agent-vm \
  --project=$GCP_PROJECT --zone=$GCP_ZONE --tunnel-through-iap \
  --command='sudo journalctl -u kubelet -f'
```

For multipass:
```bash
multipass exec bucket-agent-vm -- sudo journalctl -u kubelet -f
```

### Prometheus metrics

Server metrics (via port-forward):
```bash
kubectl -n overlay-system port-forward pod/kube-apiserver 8095:8095 &
curl -s http://localhost:8095/metrics | grep konnectivity
```

Agent metrics:
```bash
# GCP
gcloud compute ssh bucket-agent-vm \
  --project=$GCP_PROJECT --zone=$GCP_ZONE --tunnel-through-iap \
  --command='curl -s http://127.0.0.1:8094/metrics' | grep konnectivity

# Multipass
multipass exec bucket-agent-vm -- curl -s http://127.0.0.1:8094/metrics | grep konnectivity
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
./redeploy-agent.sh
```

To force re-upload of distributables:
```bash
gcloud storage rm "gs://${GCS_BUCKET}/bucket-dev/distributables/.uploaded"
./upload-distributables.sh
```

**Known quirk**: After redeploying the server, the kube-apiserver pod sometimes
doesn't come back. Fix:
```bash
kubectl apply -f hack/bucket-dev/manifests/apiserver.yaml
```

After server restart, also restart the agent + kubelet on the VM to reset
sequence counters:
```bash
# GCP
gcloud compute ssh bucket-agent-vm \
  --project=$GCP_PROJECT --zone=$GCP_ZONE --tunnel-through-iap \
  --command='sudo systemctl restart bucket-proxy-agent && sudo systemctl restart kubelet'

# Multipass
multipass exec bucket-agent-vm -- sudo systemctl restart bucket-proxy-agent
multipass exec bucket-agent-vm -- sudo systemctl restart kubelet
```

## Teardown

```bash
cd hack/bucket-dev
./teardown.sh
```

This deletes: the VM, the k3d cluster, local PKI files, and the Docker image.
It does **not** delete GCS distributables (so next `setup.sh` is faster).

To also clean up GCS:
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

## Suggested simplifications

1. **Sequence counter reset on reconnect**: The need to manually restart the
   agent after a server redeploy (to reset sequence counters) is a friction
   point. The transport layer could negotiate starting sequence numbers on
   registration, or the server could accept messages starting from any sequence
   after a fresh registration.
