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

Replaces gRPC between server and agent with object storage. Messages are protobuf-encoded `.pb` files stored at structured key paths:

- `control-to-node/{nodeID}/msg-{seq}.pb` — server → agent
- `node-to-control/{nodeID}/msg-{seq}.pb` — agent → server
- `node-to-control/{nodeID}/heartbeat-{seq}.hb` — agent liveness
- `node-to-control-reverse/{nodeID}/` — reverse tunnel (kubelet→apiserver)

Key components:
- **`BucketTransport`** (`transport.go`) — Send/Recv semantics over a Store, with Nagle buffering for small packets
- **`Store` interface** (`store.go`) — Put/Get/List/ListRecursive/Delete. Implementations: `GCSStore` (production), `FSStore` (testing), `RetryStore` (wrapper), `MetricsStore` (Prometheus instrumentation)
- **`RegionalPoller`** (`regional_poller.go`) — Single `ListRecursive` call polls messages for ALL nodes, dispatches to per-node channels. Adaptive interval (500ms–10s)
- **`HeartbeatMonitor`** (`heartbeat.go`) — Receives heartbeat updates from RegionalPoller via `UpdateHeartbeat` callback. Only scans for stale nodes in its own tick loop.
- **`BucketAgent`** (`agent.go`) — Bridges bucket packets to local TCP connections (mirrors `pkg/agent/` for gRPC mode)
- **`ReverseProxyHandler`** (`reverse_proxy.go`) — Handles reverse tunnel: agent sends HTTP requests to server, server proxies to a target (e.g., kubelet API)
- **Sequence numbers**: Zero-padded to `seqWidth=20` digits in filenames for lexicographic ordering

### gRPC Protocol

Defined in `konnectivity-client/proto/client/client.proto`. Packet types: `DIAL_REQ`, `DIAL_RSP`, `DATA`, `CLOSE_REQ`, `CLOSE_RSP`, `DIAL_CLS`, `DRAIN`. Both gRPC and bucket transports use the same protobuf `Packet` type.

### Dev Environment (Bucket Transport)

Scripts in `hack/bucket-dev/`:
- `setup.sh` — Creates k3d cluster + multipass VM + GCS bucket
- `redeploy-server.sh` — Rebuilds and redeploys bucket-proxy-server
- `benchmark-costs.sh` — 10-minute cost estimation run

Known quirks:
- After `redeploy-server.sh`, kube-apiserver pod may not restart. Fix: `kubectl apply -f hack/bucket-dev/manifests/apiserver.yaml`
- After server restart, also restart agent+kubelet on VM to reset sequence counters

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
