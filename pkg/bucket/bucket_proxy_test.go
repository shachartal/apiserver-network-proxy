/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package bucket

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"k8s.io/apimachinery/pkg/util/wait"

	"sigs.k8s.io/apiserver-network-proxy/konnectivity-client/pkg/client"
	clientproto "sigs.k8s.io/apiserver-network-proxy/konnectivity-client/proto/client"
	"sigs.k8s.io/apiserver-network-proxy/pkg/server"
	"sigs.k8s.io/apiserver-network-proxy/pkg/server/proxystrategies"
)

func TestBucketProxy_EndToEnd(t *testing.T) {
	// 1. Set up filesystem-backed bucket store.
	// Use a manually managed temp dir because background goroutines may still
	// write files during teardown (e.g., CLOSE_RSP from remoteToProxy defers).
	storeDir, err := os.MkdirTemp("", "bucket-e2e-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(storeDir) })
	store := NewFSStore(storeDir)

	// 2. Create ProxyServer with default backend strategy.
	ps := server.NewProxyServer(
		"test-server",
		[]proxystrategies.ProxyStrategy{proxystrategies.ProxyStrategyDefault},
		1, // serverCount
		&server.AgentTokenAuthenticationOptions{}, // no agent auth
		10, // xfr channel size
	)

	// 3. Start a gRPC frontend server on a unix socket.
	sockPath := filepath.Join(t.TempDir(), "frontend.sock")
	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("Failed to listen on unix socket: %v", err)
	}
	grpcServer := grpc.NewServer()
	clientproto.RegisterProxyServiceServer(grpcServer, ps)
	go grpcServer.Serve(lis)
	t.Cleanup(grpcServer.Stop)

	// 4. Register a bucket-backed "agent" with the ProxyServer via RegionalPoller.
	ctx := context.Background()
	poller := NewRegionalPoller(ctx, store, "node-to-control/")
	poller.SetWorkerCount(2)
	go poller.Run()
	t.Cleanup(poller.Stop)
	transport := RegisterBucketAgent(ps, store, "node-1", poller, 0)
	t.Cleanup(transport.Close)

	// 5. Start the BucketAgent and AgentPoller for consolidated polling.
	agent := NewBucketAgent(context.Background(), store, "node-1", 0)
	agentPoller := NewAgentPoller(context.Background(), store, "node-1", agent.Transport(), nil, 50*time.Millisecond)
	go agentPoller.Run()
	t.Cleanup(agentPoller.Stop)
	go agent.Serve()
	t.Cleanup(agent.Stop)

	// 6. Wait for the backend to register.
	err = wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		numBackends := 0
		for _, bm := range ps.BackendManagers {
			numBackends += bm.NumBackends()
		}
		return numBackends > 0, nil
	})
	if err != nil {
		t.Fatalf("Backend never registered: %v", err)
	}

	// 7. Start a local echo HTTP server.
	echoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("hello-from-bucket"))
	}))
	t.Cleanup(echoServer.Close)

	// 8. Create a konnectivity tunnel through the gRPC frontend.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tunnel, err := client.CreateSingleUseGrpcTunnel(ctx, sockPath,
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", addr)
		}),
		grpc.WithBlock(),
		grpc.WithReturnConnectionError(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to create tunnel: %v", err)
	}

	// 9. Make an HTTP request through the tunnel.
	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext: tunnel.DialContext,
		},
	}

	resp, err := httpClient.Get(echoServer.URL)
	if err != nil {
		t.Fatalf("HTTP GET through bucket tunnel failed: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}

	// 10. Assert.
	if string(body) != "hello-from-bucket" {
		t.Errorf("Expected 'hello-from-bucket', got %q", string(body))
	}
	t.Logf("Success! Received: %s", string(body))
}
