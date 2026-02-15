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
	"net"
	"time"

	"k8s.io/klog/v2"
)

// BucketAgent is the node-side component that bridges bucket-based communication
// with local endpoints (kubelet). It mirrors the packet handling logic of
// pkg/agent/Client.Serve() but uses a BucketTransport instead of gRPC.
type BucketAgent struct {
	relay  *connRelay
	nodeID string
	store  Store

	ctx    context.Context
	cancel context.CancelFunc
}

// NewBucketAgent creates a new agent that communicates via the given Store.
// The transport is send-only; receive is handled by an AgentPoller that pushes
// to the transport's recvCh via a consolidated ListRecursive.
func NewBucketAgent(ctx context.Context, store Store, nodeID string, nagleDelay time.Duration) *BucketAgent {
	ctx, cancel := context.WithCancel(ctx)
	// Agent sends to node-to-control/{nodeID}/.
	// Receive is handled externally by AgentPoller.
	transport := NewBucketTransport(ctx, store,
		"node-to-control/"+nodeID+"/",
		nagleDelay,
	)
	relay := newConnRelay(transport, "BucketAgent", func(protocol, address string) (net.Conn, error) {
		return net.DialTimeout(protocol, address, dialTimeout)
	})
	return &BucketAgent{
		relay:  relay,
		nodeID: nodeID,
		store:  store,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Transport returns the underlying BucketTransport, allowing an AgentPoller
// to push received packets into the transport's receive channel.
func (a *BucketAgent) Transport() *BucketTransport {
	return a.relay.transport
}

// Serve starts the main packet handling loop. Blocks until the context is
// cancelled or the transport returns EOF.
func (a *BucketAgent) Serve() {
	klog.V(2).InfoS("BucketAgent serving", "nodeID", a.nodeID)
	defer klog.V(2).InfoS("BucketAgent stopped", "nodeID", a.nodeID)

	// Start heartbeat publisher.
	hb := NewHeartbeatPublisher(a.ctx, a.store, a.nodeID, DefaultHeartbeatInterval)
	go hb.Run()

	a.relay.serve()
}

// Stop shuts down the agent.
func (a *BucketAgent) Stop() {
	a.cancel()
	a.relay.stop()
}
