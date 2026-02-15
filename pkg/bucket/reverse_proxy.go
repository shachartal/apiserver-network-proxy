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

// ReverseProxy is the agent-side (node) component of the reverse tunnel.
// It listens for TCP connections (e.g., from kubelet) and tunnels them
// through the bucket transport to the server-side ReverseProxyHandler.
//
// Each accepted TCP connection results in a DIAL_REQ sent to the server.
// The server responds with DIAL_RSP, after which data flows bidirectionally.
type ReverseProxy struct {
	listener  net.Listener
	transport *BucketTransport
	nodeID    string

	nextRandom atomic.Int64

	mu      sync.RWMutex
	pending map[int64]*reverseConn // random → conn (awaiting DIAL_RSP)
	active  map[int64]*reverseConn // connectID → conn (established)

	ctx    context.Context
	cancel context.CancelFunc
}

// reverseConn tracks a single TCP connection being tunneled through the bucket.
type reverseConn struct {
	tcpConn   net.Conn
	connectID int64
	random    int64
	dataCh    chan []byte
	connected chan struct{} // closed when DIAL_RSP is received
	dialErr   string
	cleanupO  sync.Once
}

func (rc *reverseConn) send(data []byte) {
	defer func() {
		if r := recover(); r != nil {
			klog.V(4).InfoS("send on closed reverse proxy data channel (connection already closed)")
		}
	}()
	if len(rc.dataCh) >= dataChanSize {
		klog.V(2).InfoS("Reverse proxy data channel near-full, backpressure active", "queued", len(rc.dataCh))
	}
	rc.dataCh <- data
}

func (rc *reverseConn) close() {
	rc.cleanupO.Do(func() {
		close(rc.dataCh)
		if rc.tcpConn != nil {
			rc.tcpConn.Close()
		}
	})
}

// NewReverseProxy creates a new agent-side reverse proxy.
// The transport is send-only; receive is handled by an AgentPoller that pushes
// to the transport's recvCh via a consolidated ListRecursive.
//   - Sends to:      node-to-control-reverse/{nodeID}/
//   - Receives from: control-to-node/{nodeID}/rev/ (via AgentPoller)
func NewReverseProxy(ctx context.Context, store Store, nodeID, listenAddr string, nagleDelay time.Duration) (*ReverseProxy, error) {
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	// Send-only transport; recv is fed by AgentPoller.
	transport := NewBucketTransport(ctx, store,
		"node-to-control-reverse/"+nodeID+"/",
		nagleDelay,
	)

	rp := &ReverseProxy{
		listener:  listener,
		transport: transport,
		nodeID:    nodeID,
		pending:   make(map[int64]*reverseConn),
		active:    make(map[int64]*reverseConn),
		ctx:       ctx,
		cancel:    cancel,
	}

	return rp, nil
}

// Transport returns the underlying BucketTransport, allowing an AgentPoller
// to push received packets into the transport's receive channel.
func (rp *ReverseProxy) Transport() *BucketTransport {
	return rp.transport
}

// Serve starts the accept loop and recv loop. Blocks until the context is cancelled.
func (rp *ReverseProxy) Serve() {
	klog.V(2).InfoS("ReverseProxy serving", "nodeID", rp.nodeID, "listenAddr", rp.listener.Addr().String())
	defer klog.V(2).InfoS("ReverseProxy stopped", "nodeID", rp.nodeID)

	go rp.recvLoop()
	rp.acceptLoop()
}

// Stop shuts down the reverse proxy.
func (rp *ReverseProxy) Stop() {
	rp.cancel()
	rp.listener.Close()
	rp.transport.Close()
	rp.cleanupAll()
}

func (rp *ReverseProxy) acceptLoop() {
	for {
		conn, err := rp.listener.Accept()
		if err != nil {
			if rp.ctx.Err() != nil {
				return
			}
			klog.V(2).InfoS("ReverseProxy accept error", "err", err)
			return
		}

		go rp.handleConn(conn)
	}
}

func (rp *ReverseProxy) handleConn(conn net.Conn) {
	random := rp.nextRandom.Add(1)

	rc := &reverseConn{
		tcpConn:   conn,
		random:    random,
		dataCh:    make(chan []byte, dataChanSize),
		connected: make(chan struct{}),
	}

	rp.mu.Lock()
	rp.pending[random] = rc
	rp.mu.Unlock()

	// Send DIAL_REQ to the server.
	dialReq := &client.Packet{
		Type: client.PacketType_DIAL_REQ,
		Payload: &client.Packet_DialRequest{
			DialRequest: &client.DialRequest{
				Protocol: "tcp",
				Address:  "reverse-tunnel", // server ignores this, dials targetAddr
				Random:   random,
			},
		},
	}
	if err := rp.transport.SendToStream(dialReq, random); err != nil {
		klog.ErrorS(err, "Failed to send DIAL_REQ", "random", random)
		rp.mu.Lock()
		delete(rp.pending, random)
		rp.mu.Unlock()
		rc.close()
		return
	}

	// Wait for DIAL_RSP.
	select {
	case <-rc.connected:
	case <-rp.ctx.Done():
		rp.mu.Lock()
		delete(rp.pending, random)
		rp.mu.Unlock()
		rc.close()
		return
	case <-time.After(dialTimeout):
		klog.V(1).InfoS("ReverseProxy DIAL_RSP timeout", "random", random)
		rp.mu.Lock()
		delete(rp.pending, random)
		rp.mu.Unlock()
		rc.close()
		return
	}

	if rc.dialErr != "" {
		klog.V(1).InfoS("ReverseProxy dial error from server", "random", random, "err", rc.dialErr)
		rc.close()
		return
	}

	klog.V(3).InfoS("ReverseProxy connection established", "random", random, "connID", rc.connectID)

	go rp.localToRemote(rc)
	go rp.remoteToLocal(rc)
}

func (rp *ReverseProxy) recvLoop() {
	for {
		pkt, err := rp.transport.Recv()
		if err != nil {
			if err == io.EOF || rp.ctx.Err() != nil {
				return
			}
			klog.ErrorS(err, "ReverseProxy recv error")
			return
		}
		if pkt == nil {
			continue
		}

		switch pkt.Type {
		case client.PacketType_DIAL_RSP:
			rp.handleDialRsp(pkt)
		case client.PacketType_DATA:
			rp.handleData(pkt)
		case client.PacketType_CLOSE_RSP:
			rp.handleCloseRsp(pkt)
		default:
			klog.V(4).InfoS("ReverseProxy ignoring packet", "type", pkt.Type)
		}
	}
}

func (rp *ReverseProxy) handleDialRsp(pkt *client.Packet) {
	dialRsp := pkt.GetDialResponse()
	if dialRsp == nil {
		return
	}

	rp.mu.Lock()
	rc, ok := rp.pending[dialRsp.Random]
	if ok {
		delete(rp.pending, dialRsp.Random)
		if dialRsp.Error != "" {
			rc.dialErr = dialRsp.Error
		} else {
			rc.connectID = dialRsp.ConnectID
			rp.active[dialRsp.ConnectID] = rc
		}
	}
	rp.mu.Unlock()

	if ok {
		close(rc.connected)
	}
}

func (rp *ReverseProxy) handleData(pkt *client.Packet) {
	data := pkt.GetData()
	if data == nil || data.ConnectID == 0 {
		return
	}

	rp.mu.RLock()
	rc, ok := rp.active[data.ConnectID]
	rp.mu.RUnlock()

	if !ok {
		klog.V(2).InfoS("ReverseProxy DATA for unknown connection", "connID", data.ConnectID)
		return
	}
	rc.send(data.Data)
}

func (rp *ReverseProxy) handleCloseRsp(pkt *client.Packet) {
	closeRsp := pkt.GetCloseResponse()
	if closeRsp == nil {
		return
	}
	connID := closeRsp.ConnectID

	rp.mu.Lock()
	rc, ok := rp.active[connID]
	if ok {
		delete(rp.active, connID)
	}
	rp.mu.Unlock()

	if ok {
		rc.close()
	}
}

// localToRemote reads from the TCP connection and sends DATA packets through the bucket.
func (rp *ReverseProxy) localToRemote(rc *reverseConn) {
	defer func() {
		klog.V(4).InfoS("ReverseProxy localToRemote exiting", "connID", rc.connectID)
		rp.mu.Lock()
		_, stillTracked := rp.active[rc.connectID]
		if stillTracked {
			delete(rp.active, rc.connectID)
		}
		rp.mu.Unlock()

		rc.close()

		if stillTracked {
			_ = rp.transport.SendToStream(&client.Packet{
				Type: client.PacketType_CLOSE_REQ,
				Payload: &client.Packet_CloseRequest{
					CloseRequest: &client.CloseRequest{
						ConnectID: rc.connectID,
					},
				},
			}, rc.random)
		}
	}()

	var buf [readBufferSize]byte
	for {
		n, err := rc.tcpConn.Read(buf[:])
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			if sendErr := rp.transport.SendToStream(&client.Packet{
				Type: client.PacketType_DATA,
				Payload: &client.Packet_Data{
					Data: &client.Data{
						Data:      data,
						ConnectID: rc.connectID,
					},
				},
			}, rc.random); sendErr != nil {
				klog.ErrorS(sendErr, "Failed to send DATA", "connID", rc.connectID)
				return
			}
		}
		if err != nil {
			if err != io.EOF {
				klog.V(4).InfoS("Local read error", "connID", rc.connectID, "err", err)
			}
			return
		}
	}
}

// remoteToLocal reads from the data channel and writes to the TCP connection.
func (rp *ReverseProxy) remoteToLocal(rc *reverseConn) {
	defer func() {
		klog.V(4).InfoS("ReverseProxy remoteToLocal exiting", "connID", rc.connectID)
	}()

	for data := range rc.dataCh {
		pos := 0
		for pos < len(data) {
			n, err := rc.tcpConn.Write(data[pos:])
			if err != nil {
				klog.V(4).InfoS("Write to local conn failed", "connID", rc.connectID, "err", err)
				return
			}
			pos += n
		}
	}
}

func (rp *ReverseProxy) cleanupAll() {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	for id, rc := range rp.pending {
		rc.close()
		delete(rp.pending, id)
	}
	for id, rc := range rp.active {
		rc.close()
		delete(rp.active, id)
	}
}
