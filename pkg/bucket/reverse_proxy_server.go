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

// ReverseProxyHandler is the server-side (control-plane) component of the
// reverse tunnel. It receives DIAL_REQ packets from the agent-side ReverseProxy
// over the bucket, dials a fixed local target (e.g., localhost:6443 for the
// apiserver), and relays data bidirectionally.
//
// This mirrors BucketAgent's logic, but in the opposite direction: the agent
// initiates connections (sends DIAL_REQ) and the server handles them.
type ReverseProxyHandler struct {
	relay  *connRelay
	nodeID string

	ctx    context.Context
	cancel context.CancelFunc
}

// NewReverseProxyHandler creates a server-side reverse proxy handler that
// receives packets from a RegionalPoller.
//   - Receives from: node-to-control-reverse/{nodeID}/ (via RegionalPoller)
//   - Sends to:      control-to-node/{nodeID}/rev/
func NewReverseProxyHandler(ctx context.Context, store Store, nodeID, targetAddr string, poller *RegionalPoller, nagleDelay time.Duration) *ReverseProxyHandler {
	ctx, cancel := context.WithCancel(ctx)
	sendTransport := NewBucketTransport(ctx, store, "control-to-node/"+nodeID+"/rev/", nagleDelay)

	recvCh := poller.RegisterNode(nodeID)
	go func() {
		for pkt := range recvCh {
			select {
			case sendTransport.recvCh <- pkt:
			case <-sendTransport.ctx.Done():
				return
			}
		}
		sendTransport.Close()
	}()

	relay := newConnRelay(sendTransport, "ReverseProxyHandler", func(_, _ string) (net.Conn, error) {
		return net.DialTimeout("tcp", targetAddr, dialTimeout)
	})
	return &ReverseProxyHandler{
		relay:  relay,
		nodeID: nodeID,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Serve starts the main packet handling loop. Blocks until the context is
// cancelled or the transport returns EOF.
func (h *ReverseProxyHandler) Serve() {
	klog.V(2).InfoS("ReverseProxyHandler serving", "nodeID", h.nodeID)
	defer klog.V(2).InfoS("ReverseProxyHandler stopped", "nodeID", h.nodeID)

	h.relay.serve()
}

// Stop shuts down the handler.
func (h *ReverseProxyHandler) Stop() {
	h.cancel()
	h.relay.stop()
}
