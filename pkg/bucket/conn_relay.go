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

// endpointConn tracks a single proxied connection to a local endpoint.
type endpointConn struct {
	conn     net.Conn
	dataCh   chan []byte
	cleanupO sync.Once
}

func (e *endpointConn) send(data []byte) {
	defer func() {
		if r := recover(); r != nil {
			klog.V(4).InfoS("send on closed data channel (connection already closed)")
		}
	}()
	if len(e.dataCh) >= dataChanSize {
		klog.V(2).InfoS("Data channel near-full, backpressure active", "queued", len(e.dataCh))
	}
	e.dataCh <- data
}

func (e *endpointConn) close() {
	e.cleanupO.Do(func() {
		close(e.dataCh)
		if e.conn != nil {
			e.conn.Close()
		}
	})
}

// connRelay is the shared connection relay logic used by both BucketAgent
// and ReverseProxyHandler. It handles DIAL_REQ, DATA, and CLOSE_REQ packets,
// managing the mapping between connection IDs and endpoint connections.
type connRelay struct {
	transport  *BucketTransport
	logPrefix  string
	dialFunc   func(protocol, address string) (net.Conn, error)
	nextConnID atomic.Int64

	mu        sync.RWMutex
	conns     map[int64]*endpointConn
	streamMap map[int64]int64 // connID → random (streamID)
}

func newConnRelay(transport *BucketTransport, logPrefix string, dialFunc func(protocol, address string) (net.Conn, error)) *connRelay {
	return &connRelay{
		transport: transport,
		logPrefix: logPrefix,
		dialFunc:  dialFunc,
		conns:     make(map[int64]*endpointConn),
		streamMap: make(map[int64]int64),
	}
}

// serve runs the main packet handling loop. Blocks until the transport
// returns EOF or the context is cancelled.
func (r *connRelay) serve() {
	defer r.cleanupAll()

	for {
		pkt, err := r.transport.Recv()
		if err != nil {
			if err == io.EOF {
				return
			}
			klog.ErrorS(err, r.logPrefix+" recv error")
			return
		}
		if pkt == nil {
			continue
		}

		switch pkt.Type {
		case client.PacketType_DIAL_REQ:
			r.handleDialReq(pkt)
		case client.PacketType_DATA:
			r.handleData(pkt)
		case client.PacketType_CLOSE_REQ:
			r.handleCloseReq(pkt)
		default:
			klog.V(4).InfoS(r.logPrefix+" ignoring packet", "type", pkt.Type)
		}
	}
}

// stop shuts down the relay and its transport.
func (r *connRelay) stop() {
	r.transport.Close()
}

func (r *connRelay) handleDialReq(pkt *client.Packet) {
	dialReq := pkt.GetDialRequest()
	if dialReq == nil {
		klog.ErrorS(nil, "DIAL_REQ packet missing DialRequest")
		return
	}

	random := dialReq.Random
	connID := r.nextConnID.Add(1)
	klog.V(3).InfoS(r.logPrefix+" DIAL_REQ", "dialID", random, "address", dialReq.Address, "connID", connID)

	dialResp := &client.Packet{
		Type: client.PacketType_DIAL_RSP,
		Payload: &client.Packet_DialResponse{
			DialResponse: &client.DialResponse{
				Random: random,
			},
		},
	}

	conn, err := r.dialFunc(dialReq.Protocol, dialReq.Address)
	if err != nil {
		klog.V(1).InfoS(r.logPrefix+" dial failed", "address", dialReq.Address, "err", err)
		dialResp.GetDialResponse().Error = err.Error()
		if sendErr := r.transport.SendToStream(dialResp, random); sendErr != nil {
			klog.ErrorS(sendErr, "Failed to send DIAL_RSP error")
		}
		return
	}

	eConn := &endpointConn{
		conn:   conn,
		dataCh: make(chan []byte, dataChanSize),
	}

	r.mu.Lock()
	r.conns[connID] = eConn
	r.streamMap[connID] = random
	r.mu.Unlock()

	dialResp.GetDialResponse().ConnectID = connID
	if err := r.transport.SendToStream(dialResp, random); err != nil {
		klog.ErrorS(err, "Failed to send DIAL_RSP", "connID", connID)
		eConn.close()
		r.mu.Lock()
		delete(r.conns, connID)
		delete(r.streamMap, connID)
		r.mu.Unlock()
		return
	}

	go r.remoteToProxy(connID, random, eConn)
	go r.proxyToRemote(connID, eConn)
}

func (r *connRelay) handleData(pkt *client.Packet) {
	data := pkt.GetData()
	if data == nil || data.ConnectID == 0 {
		klog.ErrorS(nil, "DATA packet missing Data or ConnectID")
		return
	}

	r.mu.RLock()
	eConn, ok := r.conns[data.ConnectID]
	r.mu.RUnlock()

	if !ok {
		klog.V(2).InfoS("DATA for unknown connection (already closed)", "connID", data.ConnectID)
		return
	}
	eConn.send(data.Data)
}

func (r *connRelay) handleCloseReq(pkt *client.Packet) {
	closeReq := pkt.GetCloseRequest()
	if closeReq == nil {
		return
	}
	connID := closeReq.ConnectID
	klog.V(4).InfoS(r.logPrefix+" CLOSE_REQ", "connID", connID)

	r.mu.Lock()
	eConn, ok := r.conns[connID]
	random := r.streamMap[connID]
	if ok {
		delete(r.conns, connID)
		delete(r.streamMap, connID)
	}
	r.mu.Unlock()

	if !ok {
		klog.V(2).InfoS("CLOSE_REQ for unknown connection (already closed)", "connID", connID)
		return
	}

	eConn.close()

	_ = r.transport.SendToStream(&client.Packet{
		Type: client.PacketType_CLOSE_RSP,
		Payload: &client.Packet_CloseResponse{
			CloseResponse: &client.CloseResponse{
				ConnectID: connID,
			},
		},
	}, random)
}

// remoteToProxy reads from the endpoint and sends DATA packets back through the bucket.
func (r *connRelay) remoteToProxy(connID, random int64, eConn *endpointConn) {
	defer func() {
		klog.V(4).InfoS(r.logPrefix+" remoteToProxy exiting", "connID", connID)
		r.mu.Lock()
		_, stillTracked := r.conns[connID]
		if stillTracked {
			delete(r.conns, connID)
			delete(r.streamMap, connID)
		}
		r.mu.Unlock()

		eConn.close()

		if stillTracked {
			_ = r.transport.SendToStream(&client.Packet{
				Type: client.PacketType_CLOSE_RSP,
				Payload: &client.Packet_CloseResponse{
					CloseResponse: &client.CloseResponse{
						ConnectID: connID,
					},
				},
			}, random)
		}
	}()

	var buf [readBufferSize]byte
	for {
		n, err := eConn.conn.Read(buf[:])
		if n > 0 {
			data := make([]byte, n)
			copy(data, buf[:n])
			if sendErr := r.transport.SendToStream(&client.Packet{
				Type: client.PacketType_DATA,
				Payload: &client.Packet_Data{
					Data: &client.Data{
						Data:      data,
						ConnectID: connID,
					},
				},
			}, random); sendErr != nil {
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
func (r *connRelay) proxyToRemote(connID int64, eConn *endpointConn) {
	defer func() {
		klog.V(4).InfoS(r.logPrefix+" proxyToRemote exiting", "connID", connID)
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

func (r *connRelay) cleanupAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, eConn := range r.conns {
		eConn.close()
		delete(r.conns, id)
	}
}
