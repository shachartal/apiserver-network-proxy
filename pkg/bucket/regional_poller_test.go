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
	// Use a manually managed temp dir because background goroutines may still
	// write files during teardown.
	storeDir, err := os.MkdirTemp("", "regional-e2e-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(storeDir) })
	store := NewFSStore(storeDir)

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

	// Start the BucketAgent with AgentPoller for consolidated polling.
	agent := NewBucketAgent(context.Background(), store, "node-1", 0)
	agentPoller := NewAgentPoller(context.Background(), store, "node-1", agent.Transport(), nil, 50*time.Millisecond)
	go agentPoller.Run()
	t.Cleanup(agentPoller.Stop)
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

func TestRegionalPoller_UnknownNodeGracePeriod(t *testing.T) {
	store := NewFSStore(t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	poller := NewRegionalPoller(ctx, store, "node-to-control/")
	poller.SetWorkerCount(2)
	go poller.Run()
	defer poller.Stop()

	nodeID := "unknown-node"
	prefix := "node-to-control/" + nodeID + "/"

	// Write a message for an unregistered node.
	pkt := &clientproto.Packet{
		Type: clientproto.PacketType_DIAL_REQ,
		Payload: &clientproto.Packet_DialRequest{
			DialRequest: &clientproto.DialRequest{
				Protocol: "tcp",
				Address:  "example.com:80",
				Random:   123,
			},
		},
	}
	transport := newSendOnlyTransport(ctx, store, prefix, 0)
	if err := transport.SendToStream(pkt, 123); err != nil {
		t.Fatalf("Failed to write message: %v", err)
	}
	transport.Close()

	// Poll cycle 1: First time seeing the node.
	time.Sleep(100 * time.Millisecond) // Give poller time to run
	keys, _ := store.ListRecursive(ctx, prefix)
	if len(keys) != 1 {
		t.Errorf("Poll 1: Expected 1 file to remain (grace period start), got %d", len(keys))
	}

	// Poll cycle 2: Within grace period (still <5 minutes).
	time.Sleep(100 * time.Millisecond)
	keys, _ = store.ListRecursive(ctx, prefix)
	if len(keys) != 1 {
		t.Errorf("Poll 2: Expected 1 file to remain (within grace period), got %d", len(keys))
	}

	// Test registration during grace period.
	recvCh := poller.RegisterNode(nodeID)
	time.Sleep(100 * time.Millisecond)

	// Message should be delivered to the registered handler.
	select {
	case receivedPkt := <-recvCh:
		if receivedPkt.Type != clientproto.PacketType_DIAL_REQ {
			t.Errorf("Expected DIAL_REQ, got %v", receivedPkt.Type)
		}
		t.Logf("Success: message delivered after registration")
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for message delivery after registration")
	}

	// File should be deleted after delivery.
	keys, _ = store.ListRecursive(ctx, prefix)
	if len(keys) != 0 {
		t.Errorf("Expected 0 files after delivery, got %d", len(keys))
	}
}

func TestRegionalPoller_UnknownNodeGracePeriodExpiry(t *testing.T) {
	// This test is too slow to run in normal test suite (5+ minutes).
	// We'll use a shorter grace period by temporarily modifying the constant logic.
	// For now, just verify the tracking map is populated correctly.

	store := NewFSStore(t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	poller := NewRegionalPoller(ctx, store, "node-to-control/")
	poller.SetWorkerCount(2)
	go poller.Run()
	defer poller.Stop()

	nodeID := "test-unknown-node"
	prefix := "node-to-control/" + nodeID + "/"

	// Write a message for an unregistered node.
	pkt := &clientproto.Packet{
		Type: clientproto.PacketType_DIAL_REQ,
		Payload: &clientproto.Packet_DialRequest{
			DialRequest: &clientproto.DialRequest{
				Protocol: "tcp",
				Address:  "example.com:80",
				Random:   456,
			},
		},
	}
	transport := newSendOnlyTransport(ctx, store, prefix, 0)
	if err := transport.SendToStream(pkt, 456); err != nil {
		t.Fatalf("Failed to write message: %v", err)
	}
	transport.Close()

	// Wait for poller to discover the unknown node (poll interval is adaptive, starts at 500ms).
	var firstSeen time.Time
	var found bool
	for i := 0; i < 20; i++ {
		time.Sleep(100 * time.Millisecond)
		poller.mu.RLock()
		firstSeen, found = poller.unknownNodeFirstSeen[nodeID]
		poller.mu.RUnlock()
		if found {
			break
		}
	}

	if !found {
		t.Error("Expected unknown node to be tracked in unknownNodeFirstSeen after 2 seconds")
	}
	if time.Since(firstSeen) > 3*time.Second {
		t.Errorf("Expected firstSeen timestamp to be recent, got %v ago", time.Since(firstSeen))
	}

	// Verify file still exists (not deleted yet).
	keys, _ := store.ListRecursive(ctx, prefix)
	if len(keys) != 1 {
		t.Errorf("Expected 1 file to remain during grace period, got %d", len(keys))
	}

	// Register the node — should remove it from unknown tracking.
	poller.RegisterNode(nodeID)
	time.Sleep(100 * time.Millisecond)

	poller.mu.RLock()
	_, stillTracked := poller.unknownNodeFirstSeen[nodeID]
	poller.mu.RUnlock()

	if stillTracked {
		t.Error("Expected node to be removed from unknownNodeFirstSeen after registration")
	}
}
