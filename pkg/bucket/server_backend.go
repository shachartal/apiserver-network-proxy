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
	"time"

	"google.golang.org/grpc/metadata"
	"k8s.io/klog/v2"

	client "sigs.k8s.io/apiserver-network-proxy/konnectivity-client/proto/client"
	"sigs.k8s.io/apiserver-network-proxy/pkg/server"
	"sigs.k8s.io/apiserver-network-proxy/proto/agent"
	"sigs.k8s.io/apiserver-network-proxy/proto/header"
)

// BucketAgentStream implements agent.AgentService_ConnectServer backed by a
// BucketTransport instead of a real gRPC stream. This allows ProxyServer.Connect()
// to work with bucket-based communication transparently.
type BucketAgentStream struct {
	transport *BucketTransport
	ctx       context.Context
}

var _ agent.AgentService_ConnectServer = (*BucketAgentStream)(nil)

func (s *BucketAgentStream) Send(pkt *client.Packet) error {
	return s.transport.Send(pkt)
}

func (s *BucketAgentStream) Recv() (*client.Packet, error) {
	return s.transport.Recv()
}

func (s *BucketAgentStream) Context() context.Context {
	return s.ctx
}

// SendHeader is called by ProxyServer.Connect() to send server metadata to the agent.
// In bucket mode, the agent doesn't receive this metadata, so this is a no-op.
func (s *BucketAgentStream) SendHeader(_ metadata.MD) error {
	return nil
}

func (s *BucketAgentStream) SetHeader(_ metadata.MD) error {
	return nil
}

func (s *BucketAgentStream) SetTrailer(_ metadata.MD) {}

func (s *BucketAgentStream) SendMsg(_ interface{}) error {
	return nil
}

func (s *BucketAgentStream) RecvMsg(_ interface{}) error {
	return nil
}

// RegisterBucketAgent creates a bucket-backed agent stream and registers it
// with the ProxyServer. The ProxyServer will treat this as a connected agent
// and route traffic to/from it via the bucket.
//
// This function starts a goroutine that runs ProxyServer.Connect() — it blocks
// until the transport is closed. Returns the transport so the caller can close it.
func RegisterBucketAgent(ps *server.ProxyServer, store Store, nodeID string, pollInterval, nagleDelay time.Duration) *BucketTransport {
	ctx := context.Background()

	// Server sends to control-to-node/{nodeID}/, receives from node-to-control/{nodeID}/
	transport := NewBucketTransport(ctx, store,
		"control-to-node/"+nodeID+"/",
		"node-to-control/"+nodeID+"/",
		pollInterval,
		nagleDelay,
	)

	connectTransport(ps, transport, nodeID)
	return transport
}

// RegisterBucketAgentWithPoller registers a bucket-backed agent that receives
// messages from a RegionalPoller instead of polling independently. The poller
// handles LIST operations centrally for all nodes in the region.
// Returns a send-only BucketTransport (polling disabled; recv comes from the poller).
func RegisterBucketAgentWithPoller(ps *server.ProxyServer, store Store, nodeID string, poller *RegionalPoller, nagleDelay time.Duration) *BucketTransport {
	ctx := context.Background()

	// Create a send-only transport (no polling — the poller handles recv).
	sendTransport := newSendOnlyTransport(ctx, store, "control-to-node/"+nodeID+"/", nagleDelay)

	// Register with the poller to receive packets.
	recvCh := poller.RegisterNode(nodeID)

	// Bridge the poller's recv channel into the transport's recv channel.
	go func() {
		for pkt := range recvCh {
			select {
			case sendTransport.recvCh <- pkt:
			case <-sendTransport.ctx.Done():
				return
			}
		}
		// Poller closed the channel (node unregistered or poller stopped).
		sendTransport.Close()
	}()

	connectTransport(ps, sendTransport, nodeID)
	return sendTransport
}

// connectTransport wires a BucketTransport to a ProxyServer as a connected agent.
func connectTransport(ps *server.ProxyServer, transport *BucketTransport, nodeID string) {
	// Build context with gRPC metadata that NewBackend() expects.
	md := metadata.New(map[string]string{
		header.AgentID: nodeID,
	})
	streamCtx := metadata.NewIncomingContext(context.Background(), md)

	stream := &BucketAgentStream{
		transport: transport,
		ctx:       streamCtx,
	}

	go func() {
		err := ps.Connect(stream)
		if err != nil {
			klog.V(2).InfoS("Bucket agent stream ended", "nodeID", nodeID, "err", err)
		}
	}()
}
