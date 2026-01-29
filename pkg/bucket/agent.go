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

const (
	dialTimeout    = 10 * time.Second
	dataChanSize   = 100
	readBufferSize = 1 << 12 // 4KB
)

// endpointConn tracks a single proxied connection to a local endpoint (e.g., kubelet).
type endpointConn struct {
	conn     net.Conn
	dataCh   chan []byte
	cleanupO sync.Once
}

func (e *endpointConn) send(data []byte) {
	select {
	case e.dataCh <- data:
	default:
		klog.V(2).InfoS("Data channel full, dropping data")
	}
}

func (e *endpointConn) close() {
	e.cleanupO.Do(func() {
		close(e.dataCh)
		if e.conn != nil {
			e.conn.Close()
		}
	})
}

// BucketAgent is the node-side component that bridges bucket-based communication
// with local endpoints (kubelet). It mirrors the packet handling logic of
// pkg/agent/Client.Serve() but uses a BucketTransport instead of gRPC.
type BucketAgent struct {
	transport  *BucketTransport
	nodeID     string
	store      Store
	nextConnID atomic.Int64

	mu    sync.RWMutex
	conns map[int64]*endpointConn

	ctx    context.Context
	cancel context.CancelFunc
}

// NewBucketAgent creates a new agent that communicates via the given Store.
func NewBucketAgent(ctx context.Context, store Store, nodeID string, pollInterval time.Duration) *BucketAgent {
	ctx, cancel := context.WithCancel(ctx)
	// Agent sends to node-to-control/{nodeID}/, receives from control-to-node/{nodeID}/
	transport := NewBucketTransport(ctx, store,
		"node-to-control/"+nodeID+"/",
		"control-to-node/"+nodeID+"/",
		pollInterval,
	)
	return &BucketAgent{
		transport: transport,
		nodeID:    nodeID,
		store:     store,
		conns:     make(map[int64]*endpointConn),
		ctx:       ctx,
		cancel:    cancel,
	}
}

// Serve starts the main packet handling loop. Blocks until the context is
// cancelled or the transport returns EOF.
func (a *BucketAgent) Serve() {
	klog.V(2).InfoS("BucketAgent serving", "nodeID", a.nodeID)
	defer klog.V(2).InfoS("BucketAgent stopped", "nodeID", a.nodeID)
	defer a.cleanupAll()

	// Start heartbeat publisher.
	hb := NewHeartbeatPublisher(a.ctx, a.store, a.nodeID, DefaultHeartbeatInterval)
	go hb.Run()

	for {
		pkt, err := a.transport.Recv()
		if err != nil {
			if err == io.EOF || a.ctx.Err() != nil {
				return
			}
			klog.ErrorS(err, "BucketAgent recv error", "nodeID", a.nodeID)
			return
		}
		if pkt == nil {
			continue
		}

		switch pkt.Type {
		case client.PacketType_DIAL_REQ:
			a.handleDialReq(pkt)
		case client.PacketType_DATA:
			a.handleData(pkt)
		case client.PacketType_CLOSE_REQ:
			a.handleCloseReq(pkt)
		default:
			klog.V(4).InfoS("BucketAgent ignoring packet", "type", pkt.Type)
		}
	}
}

// Stop shuts down the agent.
func (a *BucketAgent) Stop() {
	a.cancel()
	a.transport.Close()
}

func (a *BucketAgent) handleDialReq(pkt *client.Packet) {
	dialReq := pkt.GetDialRequest()
	if dialReq == nil {
		klog.ErrorS(nil, "DIAL_REQ packet missing DialRequest")
		return
	}

	connID := a.nextConnID.Add(1)
	klog.V(3).InfoS("BucketAgent DIAL_REQ", "dialID", dialReq.Random, "address", dialReq.Address, "connID", connID)

	dialResp := &client.Packet{
		Type: client.PacketType_DIAL_RSP,
		Payload: &client.Packet_DialResponse{
			DialResponse: &client.DialResponse{
				Random: dialReq.Random,
			},
		},
	}

	conn, err := net.DialTimeout(dialReq.Protocol, dialReq.Address, dialTimeout)
	if err != nil {
		klog.V(1).InfoS("BucketAgent dial failed", "address", dialReq.Address, "err", err)
		dialResp.GetDialResponse().Error = err.Error()
		if sendErr := a.transport.Send(dialResp); sendErr != nil {
			klog.ErrorS(sendErr, "Failed to send DIAL_RSP error")
		}
		return
	}

	eConn := &endpointConn{
		conn:   conn,
		dataCh: make(chan []byte, dataChanSize),
	}

	a.mu.Lock()
	a.conns[connID] = eConn
	a.mu.Unlock()

	dialResp.GetDialResponse().ConnectID = connID
	if err := a.transport.Send(dialResp); err != nil {
		klog.ErrorS(err, "Failed to send DIAL_RSP", "connID", connID)
		eConn.close()
		a.mu.Lock()
		delete(a.conns, connID)
		a.mu.Unlock()
		return
	}

	go a.remoteToProxy(connID, eConn)
	go a.proxyToRemote(connID, eConn)
}

func (a *BucketAgent) handleData(pkt *client.Packet) {
	data := pkt.GetData()
	if data == nil || data.ConnectID == 0 {
		klog.ErrorS(nil, "DATA packet missing Data or ConnectID")
		return
	}

	a.mu.RLock()
	eConn, ok := a.conns[data.ConnectID]
	a.mu.RUnlock()

	if !ok {
		klog.V(2).InfoS("DATA for unknown connection", "connID", data.ConnectID)
		_ = a.transport.Send(&client.Packet{
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

func (a *BucketAgent) handleCloseReq(pkt *client.Packet) {
	closeReq := pkt.GetCloseRequest()
	if closeReq == nil {
		return
	}
	connID := closeReq.ConnectID
	klog.V(4).InfoS("BucketAgent CLOSE_REQ", "connID", connID)

	a.mu.Lock()
	eConn, ok := a.conns[connID]
	if ok {
		delete(a.conns, connID)
	}
	a.mu.Unlock()

	if ok {
		eConn.close()
	}

	_ = a.transport.Send(&client.Packet{
		Type: client.PacketType_CLOSE_RSP,
		Payload: &client.Packet_CloseResponse{
			CloseResponse: &client.CloseResponse{
				ConnectID: connID,
			},
		},
	})
}

// remoteToProxy reads from the endpoint and sends DATA packets back through the bucket.
func (a *BucketAgent) remoteToProxy(connID int64, eConn *endpointConn) {
	defer func() {
		klog.V(4).InfoS("remoteToProxy exiting", "connID", connID)
		// Send CLOSE_RSP when remote closes
		a.mu.Lock()
		_, stillTracked := a.conns[connID]
		if stillTracked {
			delete(a.conns, connID)
		}
		a.mu.Unlock()

		eConn.close()

		if stillTracked {
			_ = a.transport.Send(&client.Packet{
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
			// Copy the data since buf will be reused.
			data := make([]byte, n)
			copy(data, buf[:n])
			if sendErr := a.transport.Send(&client.Packet{
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

// proxyToRemote reads from the data channel and writes to the endpoint.
func (a *BucketAgent) proxyToRemote(connID int64, eConn *endpointConn) {
	defer func() {
		klog.V(4).InfoS("proxyToRemote exiting", "connID", connID)
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

func (a *BucketAgent) cleanupAll() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for id, eConn := range a.conns {
		eConn.close()
		delete(a.conns, id)
	}
}
