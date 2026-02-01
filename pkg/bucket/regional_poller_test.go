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

func TestRegionalPoller_EndToEnd(t *testing.T) {
	store := NewFSStore(t.TempDir())

	// Create ProxyServer.
	ps := server.NewProxyServer(
		"test-server",
		[]proxystrategies.ProxyStrategy{proxystrategies.ProxyStrategyDefault},
		1,
		&server.AgentTokenAuthenticationOptions{},
		10,
	)

	// Start gRPC frontend. Use a short temp dir for the socket to avoid
	// exceeding the Unix socket path length limit (108 chars on most systems).
	sockDir, err := os.MkdirTemp("", "rp")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })
	sockPath := filepath.Join(sockDir, "fe.sock")
	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	clientproto.RegisterProxyServiceServer(grpcServer, ps)
	go grpcServer.Serve(lis)
	t.Cleanup(grpcServer.Stop)

	// Start a RegionalPoller for the server side.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	poller := NewRegionalPoller(ctx, store, "node-to-control/")
	poller.SetWorkerCount(2)
	go poller.Run()
	t.Cleanup(poller.Stop)

	// Register agent using the regional poller.
	transport := RegisterBucketAgentWithPoller(ps, store, "node-1", poller, 0)
	t.Cleanup(transport.Close)

	// Start the BucketAgent (node side still polls independently).
	agent := NewBucketAgent(context.Background(), store, "node-1", 50*time.Millisecond, 0)
	go agent.Serve()
	t.Cleanup(agent.Stop)

	// Wait for backend registration.
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

	// Start echo server.
	echoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("hello-from-regional-poller"))
	}))
	t.Cleanup(echoServer.Close)

	// Create tunnel.
	tunnelCtx, tunnelCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer tunnelCancel()

	tunnel, err := client.CreateSingleUseGrpcTunnel(tunnelCtx, sockPath,
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

	// Make HTTP request through the tunnel.
	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext: tunnel.DialContext,
		},
	}

	resp, err := httpClient.Get(echoServer.URL)
	if err != nil {
		t.Fatalf("HTTP GET through tunnel failed: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}

	if string(body) != "hello-from-regional-poller" {
		t.Errorf("Expected 'hello-from-regional-poller', got %q", string(body))
	}
	t.Logf("Success! Received: %s", string(body))
}
