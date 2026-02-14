# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the Kubernetes **apiserver-network-proxy** (Konnectivity) — a tunneling proxy that enables the Kubernetes API server to reach cluster nodes through a reverse proxy. It has two communication modes:

1. **gRPC mode** (upstream, stable): Bidirectional gRPC streaming with mTLS between proxy-server and proxy-agent
2. **Bucket transport mode** (fork addition): Uses cloud object storage (GCS) instead of gRPC for server-agent communication

## Build & Test Commands

```bash
# Build all standard binaries (proxy-agent, proxy-server, test utilities)
make build

# Build bucket transport binaries only
make build-bucket

# Cross-compile bucket binaries for Linux
make build-bucket-linux

# Fast unit tests (excludes e2e)
make fast-test

# Full unit tests with coverage
make test

# Run a single test
go test -mod=vendor -race -run TestHeartbeatPublishAndMonitor ./pkg/bucket/...

# Run a single package's tests
go test -mod=vendor -race ./pkg/bucket/...

# Integration tests (requires built binaries)
make test-integration

# Lint
make lint

# Regenerate protobuf + mocks
make gen
```

All `go` commands require `-mod=vendor` — dependencies are vendored.

## Architecture

### Communication Flow

```
kube-apiserver → [gRPC UDS] → proxy-server → [gRPC mTLS OR bucket store] → proxy-agent → kubelet/pods
```

### Binaries

| Binary | Source | Purpose |
|---|---|---|
| `proxy-server` | `cmd/server/` | Accepts API server connections (port 8090), routes to agents (port 8091) via gRPC |
| `proxy-agent` | `cmd/agent/` | Runs on nodes, dials back to proxy-server, bridges to local endpoints |
| `bucket-proxy-server` | `cmd/bucket-server/` | Same role as proxy-server but uses bucket store for agent communication |
| `bucket-proxy-agent` | `cmd/bucket-agent/` | Same role as proxy-agent but uses bucket store |

### Key Packages

- **`pkg/server/`** — ProxyServer: accepts client connections, manages agent backends, routes packets by destination host. `proxystrategies/` handles routing policies.
- **`pkg/agent/`** — Agent client: dials proxy-server, manages endpoint connections to local services (kubelet, etc.).
- **`pkg/bucket/`** — Bucket transport layer (see below).

### Bucket Transport (`pkg/bucket/`)

Replaces gRPC between server and agent with object storage. Messages are protobuf-encoded `.pb` files stored at structured key paths using per-stream sequencing:

- `control-to-node/{nodeID}/fwd/{streamID}-{seq}.pb` — server → agent (forward proxy)
- `control-to-node/{nodeID}/rev/{streamID}-{seq}.pb` — server → agent (reverse proxy)
- `node-to-control/{nodeID}/{streamID}-{seq}.pb` — agent → server
- `node-to-control/{nodeID}/heartbeat-{seq}.hb` — agent liveness
- `node-to-control-reverse/{nodeID}/{streamID}-{seq}.pb` — reverse tunnel (kubelet→apiserver)

The `{streamID}` is the DIAL correlation value (`Random` field) that identifies a Konnectivity stream. Each stream has its own independent sequence counter, so a failed GCS Put only blocks the affected stream, not all streams on the node.

Key components:
- **`BucketTransport`** (`transport.go`) — `SendToStream(pkt, streamID)` semantics over a Store, with per-stream sequence counters and Nagle buffering for small packets
- **`Store` interface** (`store.go`) — Put/Get/List/ListRecursive/Delete. Implementations: `GCSStore` (production), `FSStore` (testing), `RetryStore` (wrapper), `MetricsStore` (Prometheus instrumentation)
- **`RegionalPoller`** (`regional_poller.go`) — Single `ListRecursive` call polls messages for ALL nodes, dispatches to per-node channels with per-stream contiguity enforcement. Adaptive interval (500ms–10s). Implements 5-minute grace period for unregistered nodes and stale stream eviction.
- **`HeartbeatMonitor`** (`heartbeat.go`) — Receives heartbeat updates from RegionalPoller via `UpdateHeartbeat` callback. Only scans for stale nodes in its own tick loop.
- **`BucketAgent`** (`agent.go`) — Bridges bucket packets to local TCP connections (mirrors `pkg/agent/` for gRPC mode). Maintains connID→streamID mapping.
- **`BucketAgentStream`** (`server_backend.go`) — Server-side adapter implementing the gRPC agent interface. Resolves streamID from packet type (DIAL_REQ/DATA/CLOSE) and intercepts DIAL_RSP to learn connID→streamID mappings.
- **`ReverseProxyHandler`** (`reverse_proxy_server.go`) — Server side of reverse tunnel: receives DIAL_REQ from agent, dials local target, relays data with per-stream sequencing.
- **`ReverseProxy`** (`reverse_proxy.go`) — Agent side of reverse tunnel: accepts TCP connections, tunnels through bucket using `rc.random` as streamID.
- **Sequence numbers**: Zero-padded to `seqWidth=20` digits in filenames for lexicographic ordering within each stream

### gRPC Protocol

Defined in `konnectivity-client/proto/client/client.proto`. Packet types: `DIAL_REQ`, `DIAL_RSP`, `DATA`, `CLOSE_REQ`, `CLOSE_RSP`, `DIAL_CLS`, `DRAIN`. Both gRPC and bucket transports use the same protobuf `Packet` type.

### Dev Environment (Bucket Transport)

Scripts in `hack/bucket-dev/` are split into control-plane and worker phases:

Setup:
- `setup.sh` — Convenience wrapper: runs `setup-control-plane.sh` then `setup-worker.sh`
- `setup-control-plane.sh` — Creates k3d cluster, PKI, overlay pods (run once)
- `setup-worker.sh` — Launches a worker VM with unique `NODE_ID` (can run multiple times)

Teardown:
- `teardown.sh` — Discovers all workers, tears them down, cleans GCS, removes control plane
- `teardown-worker.sh` — Tears down a single worker by `NODE_ID`
- `teardown-control-plane.sh` — Removes k3d cluster and local state
- `cleanup-bucket.sh` — Deletes all transport messages from GCS (not distributables)

Iteration:
- `redeploy-server.sh` — Rebuilds and redeploys bucket-proxy-server
- `redeploy-agent.sh` — Rebuilds and redeploys agent to a VM (requires `NODE_ID`)
- `benchmark-costs.sh` — 10-minute cost estimation run (requires `NODE_ID`)

The worker VM backend is controlled by `VM_BACKEND`:
- `multipass` (default) — Local VM, requires `multipass` CLI and a GCS credentials file
- `gcp` — GCE VM in a locked-down VPC, uses VM service account ADC for GCS (no credentials file on VM)

GCP-specific variables (set in `setup.sh` defaults or env):
- `GCP_PROJECT`, `GCP_ZONE`, `GCP_NETWORK`, `GCP_SUBNET`, `GCP_SERVICE_ACCOUNT`

GCP networking infrastructure (`hack/terraform/demo-env/`):
- Custom VPC with deny-all-egress firewall (zero internet access)
- Private Service Connect endpoint for GCS (`*.googleapis.com` → PSC IP via private DNS)
- IAP SSH for VM access without external IPs
- Worker service account with `roles/storage.admin`

Known quirks:
- After `redeploy-server.sh`, kube-apiserver pod may not restart. Fix: `kubectl apply -f hack/bucket-dev/manifests/apiserver.yaml`
- After server restart, also restart agent+kubelet on VM to reset sequence counters
- Containerd CRI sandbox image pulls bypass the `_default/hosts.toml` mirror; cloud-init pre-pulls the pause image directly from the local registry to work around this

## Code Conventions

- **Logging**: `klog/v2` structured logging. Use `klog.V(2).InfoS(...)` for operational, `V(4)` for debug, `V(5)` for trace. Errors: `klog.ErrorS(err, "msg", "key", val)`.
- **Vendoring**: All deps vendored. After modifying `go.mod`: `go mod vendor && go mod tidy`.
- **Linting**: golangci-lint v2 with gosec, govet, revive, unused. Formatter: gofmt.
- **Metrics**: Prometheus `client_golang`. Namespace `konnectivity_network_proxy`, subsystems `bucket`, `agent`, `server`.
- **Channel sends in transport code**: Use blocking sends with `defer recover()` to handle closed channels — never use non-blocking select/default that drops data.
- **Lock ordering**: When callbacks cross component boundaries (e.g., poller → heartbeat monitor → server), run `registerNode` in a goroutine to avoid lock ordering deadlocks.

## Default Ports

| Port | Purpose |
|---|---|
| 8090 | Frontend: API server → proxy-server (gRPC/mTLS) |
| 8091 | Agent: proxy-agent → proxy-server (gRPC/mTLS) |
| 8092 | Admin: pprof, `/metrics` |
| 8093 | Health: `/healthz`, `/readyz` |
