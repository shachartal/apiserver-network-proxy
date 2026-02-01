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
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/klog/v2"

	client "sigs.k8s.io/apiserver-network-proxy/konnectivity-client/proto/client"
)

// ReverseProxyHandler is the server-side (control-plane) component of the
// reverse tunnel. It receives DIAL_REQ packets from the agent-side ReverseProxy
// over the bucket, dials a fixed local target (e.g., localhost:6443 for the
// apiserver), and relays data bidirectionally.
//
// This mirrors BucketAgent's logic, but in the opposite direction: the agent
// initiates connections (sends DIAL_REQ) and the server handles them.
type ReverseProxyHandler struct {
	transport  *BucketTransport
	targetAddr string // always dial this address (ignores DIAL_REQ address for security)
	nodeID     string
	nextConnID atomic.Int64

	mu    sync.RWMutex
	conns map[int64]*endpointConn

	ctx    context.Context
	cancel context.CancelFunc
}

// NewReverseProxyHandler creates a server-side reverse proxy handler.
// It creates its own BucketTransport with reverse-direction prefixes:
//   - Receives from: node-to-control-reverse/{nodeID}/
//   - Sends to:      control-to-node-reverse/{nodeID}/
func NewReverseProxyHandler(ctx context.Context, store Store, nodeID, targetAddr string, pollInterval, nagleDelay time.Duration) *ReverseProxyHandler {
	ctx, cancel := context.WithCancel(ctx)
	transport := NewBucketTransport(ctx, store,
		"control-to-node-reverse/"+nodeID+"/",
		"node-to-control-reverse/"+nodeID+"/",
		pollInterval,
		nagleDelay,
	)
	return &ReverseProxyHandler{
		transport:  transport,
		targetAddr: targetAddr,
		nodeID:     nodeID,
		conns:      make(map[int64]*endpointConn),
		ctx:        ctx,
		cancel:     cancel,
	}
}

// NewReverseProxyHandlerWithPoller creates a server-side reverse proxy handler
// that receives packets from a RegionalPoller instead of polling independently.
func NewReverseProxyHandlerWithPoller(ctx context.Context, store Store, nodeID, targetAddr string, poller *RegionalPoller, nagleDelay time.Duration) *ReverseProxyHandler {
	ctx, cancel := context.WithCancel(ctx)
	sendTransport := newSendOnlyTransport(ctx, store, "control-to-node-reverse/"+nodeID+"/", nagleDelay)

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

	return &ReverseProxyHandler{
		transport:  sendTransport,
		targetAddr: targetAddr,
		nodeID:     nodeID,
		conns:      make(map[int64]*endpointConn),
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Serve starts the main packet handling loop. Blocks until the context is
// cancelled or the transport returns EOF.
func (h *ReverseProxyHandler) Serve() {
	klog.V(2).InfoS("ReverseProxyHandler serving", "nodeID", h.nodeID, "targetAddr", h.targetAddr)
	defer klog.V(2).InfoS("ReverseProxyHandler stopped", "nodeID", h.nodeID)
	defer h.cleanupAll()

	for {
		pkt, err := h.transport.Recv()
		if err != nil {
			if err == io.EOF || h.ctx.Err() != nil {
				return
			}
			klog.ErrorS(err, "ReverseProxyHandler recv error", "nodeID", h.nodeID)
			return
		}
		if pkt == nil {
			continue
		}

		switch pkt.Type {
		case client.PacketType_DIAL_REQ:
			h.handleDialReq(pkt)
		case client.PacketType_DATA:
			h.handleData(pkt)
		case client.PacketType_CLOSE_REQ:
			h.handleCloseReq(pkt)
		default:
			klog.V(4).InfoS("ReverseProxyHandler ignoring packet", "type", pkt.Type)
		}
	}
}

// Stop shuts down the handler.
func (h *ReverseProxyHandler) Stop() {
	h.cancel()
	h.transport.Close()
}

func (h *ReverseProxyHandler) handleDialReq(pkt *client.Packet) {
	dialReq := pkt.GetDialRequest()
	if dialReq == nil {
		klog.ErrorS(nil, "DIAL_REQ packet missing DialRequest")
		return
	}

	connID := h.nextConnID.Add(1)
	klog.V(3).InfoS("ReverseProxyHandler DIAL_REQ", "dialID", dialReq.Random, "requestedAddr", dialReq.Address, "targetAddr", h.targetAddr, "connID", connID)

	dialResp := &client.Packet{
		Type: client.PacketType_DIAL_RSP,
		Payload: &client.Packet_DialResponse{
			DialResponse: &client.DialResponse{
				Random: dialReq.Random,
			},
		},
	}

	// Always dial targetAddr, ignoring the address in the DIAL_REQ for security.
	conn, err := net.DialTimeout("tcp", h.targetAddr, dialTimeout)
	if err != nil {
		klog.V(1).InfoS("ReverseProxyHandler dial failed", "targetAddr", h.targetAddr, "err", err)
		dialResp.GetDialResponse().Error = err.Error()
		if sendErr := h.transport.Send(dialResp); sendErr != nil {
			klog.ErrorS(sendErr, "Failed to send DIAL_RSP error")
		}
		return
	}

	eConn := &endpointConn{
		conn:   conn,
		dataCh: make(chan []byte, dataChanSize),
	}

	h.mu.Lock()
	h.conns[connID] = eConn
	h.mu.Unlock()

	dialResp.GetDialResponse().ConnectID = connID
	if err := h.transport.Send(dialResp); err != nil {
		klog.ErrorS(err, "Failed to send DIAL_RSP", "connID", connID)
		eConn.close()
		h.mu.Lock()
		delete(h.conns, connID)
		h.mu.Unlock()
		return
	}

	go h.remoteToProxy(connID, eConn)
	go h.proxyToRemote(connID, eConn)
}

func (h *ReverseProxyHandler) handleData(pkt *client.Packet) {
	data := pkt.GetData()
	if data == nil || data.ConnectID == 0 {
		klog.ErrorS(nil, "DATA packet missing Data or ConnectID")
		return
	}

	h.mu.RLock()
	eConn, ok := h.conns[data.ConnectID]
	h.mu.RUnlock()

	if !ok {
		klog.V(2).InfoS("DATA for unknown connection", "connID", data.ConnectID)
		_ = h.transport.Send(&client.Packet{
			Type: client.PacketType_CLOSE_RSP,
			Payload: &client.Packet_CloseResponse{
				CloseResponse: &client.CloseResponse{
					ConnectID: data.ConnectID,
					Error:     "unrecognized connectID",
				},
			},
		})
		return
	}
	eConn.send(data.Data)
}

func (h *ReverseProxyHandler) handleCloseReq(pkt *client.Packet) {
	closeReq := pkt.GetCloseRequest()
	if closeReq == nil {
		return
	}
	connID := closeReq.ConnectID
	klog.V(4).InfoS("ReverseProxyHandler CLOSE_REQ", "connID", connID)

	h.mu.Lock()
	eConn, ok := h.conns[connID]
	if ok {
		delete(h.conns, connID)
	}
	h.mu.Unlock()

	if ok {
		eConn.close()
	}

	_ = h.transport.Send(&client.Packet{
		Type: client.PacketType_CLOSE_RSP,
		Payload: &client.Packet_CloseResponse{
			CloseResponse: &client.CloseResponse{
				ConnectID: connID,
			},
		},
	})
}

// remoteToProxy reads from the target endpoint and sends DATA packets back through the bucket.
func (h *ReverseProxyHandler) remoteToProxy(connID int64, eConn *endpointConn) {
	defer func() {
		klog.V(4).InfoS("ReverseProxyHandler remoteToProxy exiting", "connID", connID)
		h.mu.Lock()
		_, stillTracked := h.conns[connID]
		if stillTracked {
			delete(h.conns, connID)
		}
		h.mu.Unlock()

		eConn.close()

		if stillTracked {
			_ = h.transport.Send(&client.Packet{
				Type: client.PacketType_CLOSE_RSP,
				Payload: &client.Packet_CloseResponse{
					CloseResponse: &client.CloseResponse{
						ConnectID: connID,
					},
				},
			})
		}
	}()

	var buf [readBufferSize]byte
	for {
		n, err := eConn.conn.Read(buf[:])
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			if sendErr := h.transport.Send(&client.Packet{
				Type: client.PacketType_DATA,
				Payload: &client.Packet_Data{
					Data: &client.Data{
						Data:      data,
						ConnectID: connID,
					},
				},
			}); sendErr != nil {
				klog.ErrorS(sendErr, "Failed to send DATA", "connID", connID)
				return
			}
		}
		if err != nil {
			if err != io.EOF {
				klog.V(4).InfoS("Remote read error", "connID", connID, "err", err)
			}
			return
		}
	}
}

// proxyToRemote reads from the data channel and writes to the target endpoint.
func (h *ReverseProxyHandler) proxyToRemote(connID int64, eConn *endpointConn) {
	defer func() {
		klog.V(4).InfoS("ReverseProxyHandler proxyToRemote exiting", "connID", connID)
	}()

	for data := range eConn.dataCh {
		pos := 0
		for pos < len(data) {
			n, err := eConn.conn.Write(data[pos:])
			if err != nil {
				klog.V(4).InfoS("Write to endpoint failed", "connID", connID, "err", err)
				return
			}
			pos += n
		}
	}
}

func (h *ReverseProxyHandler) cleanupAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for id, eConn := range h.conns {
		eConn.close()
		delete(h.conns, id)
	}
}
