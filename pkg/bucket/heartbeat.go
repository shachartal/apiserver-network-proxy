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
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/klog/v2"
)

const (
	// heartbeatPrefix is the bucket directory for heartbeat files.
	heartbeatPrefix = "node-to-control/"
	// heartbeatSuffix distinguishes heartbeat files from data messages.
	heartbeatSuffix = ".hb"
	// DefaultHeartbeatInterval is how often agents publish heartbeats.
	DefaultHeartbeatInterval = 10 * time.Second
	// DefaultHeartbeatTimeout is how long before a node is considered dead.
	DefaultHeartbeatTimeout = 5 * time.Minute
)

// HeartbeatPublisher periodically writes a heartbeat file to the bucket
// so the server can detect that the agent is alive.
type HeartbeatPublisher struct {
	store    Store
	nodeID   string
	interval time.Duration
	seq      atomic.Uint64

	ctx    context.Context
	cancel context.CancelFunc
}

// NewHeartbeatPublisher creates a publisher that writes heartbeats for the given node.
func NewHeartbeatPublisher(ctx context.Context, store Store, nodeID string, interval time.Duration) *HeartbeatPublisher {
	ctx, cancel := context.WithCancel(ctx)
	return &HeartbeatPublisher{
		store:    store,
		nodeID:   nodeID,
		interval: interval,
		ctx:      ctx,
		cancel:   cancel,
	}
}

// Run starts publishing heartbeats. Blocks until the context is cancelled.
func (h *HeartbeatPublisher) Run() {
	klog.V(2).InfoS("HeartbeatPublisher started", "nodeID", h.nodeID, "interval", h.interval)
	defer klog.V(2).InfoS("HeartbeatPublisher stopped", "nodeID", h.nodeID)
	defer h.cleanup()

	// Publish immediately on start.
	h.publish()

	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			h.publish()
		}
	}
}

// cleanup deletes the current heartbeat file on shutdown so it doesn't
// remain in the bucket after the agent exits.
func (h *HeartbeatPublisher) cleanup() {
	seq := h.seq.Load()
	if seq == 0 {
		return
	}
	key := fmt.Sprintf("%s%s/heartbeat-%0*d%s", heartbeatPrefix, h.nodeID, seqWidth, seq, heartbeatSuffix)
	// Use a fresh context since h.ctx is already cancelled.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.store.Delete(ctx, key); err != nil {
		klog.V(2).InfoS("Failed to delete heartbeat on shutdown", "nodeID", h.nodeID, "key", key, "err", err)
	} else {
		klog.V(2).InfoS("Deleted heartbeat on shutdown", "nodeID", h.nodeID, "key", key)
	}
}

// Stop cancels the publisher.
func (h *HeartbeatPublisher) Stop() {
	h.cancel()
}

func (h *HeartbeatPublisher) publish() {
	seq := h.seq.Add(1)
	key := fmt.Sprintf("%s%s/heartbeat-%0*d%s", heartbeatPrefix, h.nodeID, seqWidth, seq, heartbeatSuffix)

	// Heartbeat payload is the current timestamp in milliseconds (little-endian).
	ts := time.Now().UnixMilli()
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, uint64(ts))

	if err := h.store.Put(h.ctx, key, data); err != nil {
		if h.ctx.Err() != nil {
			return
		}
		klog.V(2).InfoS("Failed to publish heartbeat", "nodeID", h.nodeID, "err", err)
		return
	}

	// Clean up the previous heartbeat file to avoid accumulation.
	if seq > 1 {
		prevKey := fmt.Sprintf("%s%s/heartbeat-%0*d%s", heartbeatPrefix, h.nodeID, seqWidth, seq-1, heartbeatSuffix)
		_ = h.store.Delete(h.ctx, prevKey)
	}

	klog.V(5).InfoS("Heartbeat published", "nodeID", h.nodeID, "seq", seq)
}

// HeartbeatMonitor watches for agent heartbeats on the server side.
// It tracks the last seen heartbeat time for each node and reports
// nodes that have gone stale.
type HeartbeatMonitor struct {
	store        Store
	pollInterval time.Duration
	timeout      time.Duration

	mu       sync.RWMutex
	lastSeen map[string]time.Time // nodeID → last heartbeat time

	ctx    context.Context
	cancel context.CancelFunc

	// OnNodeDiscovered is called when a new node's heartbeat is seen for the first time.
	OnNodeDiscovered func(nodeID string)

	// OnNodeStale is called when a node's heartbeat exceeds the timeout.
	OnNodeStale func(nodeID string)
}

// NewHeartbeatMonitor creates a monitor that watches the bucket for agent heartbeats.
func NewHeartbeatMonitor(ctx context.Context, store Store, pollInterval, timeout time.Duration) *HeartbeatMonitor {
	ctx, cancel := context.WithCancel(ctx)
	return &HeartbeatMonitor{
		store:        store,
		pollInterval: pollInterval,
		timeout:      timeout,
		lastSeen:     make(map[string]time.Time),
		ctx:          ctx,
		cancel:       cancel,
	}
}

// Run starts monitoring heartbeats. Blocks until the context is cancelled.
func (m *HeartbeatMonitor) Run() {
	klog.V(2).InfoS("HeartbeatMonitor started", "pollInterval", m.pollInterval, "timeout", m.timeout)
	defer klog.V(2).InfoS("HeartbeatMonitor stopped")

	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.check()
		}
	}
}

// Stop cancels the monitor.
func (m *HeartbeatMonitor) Stop() {
	m.cancel()
}

// IsAlive returns true if the given node has a recent heartbeat.
func (m *HeartbeatMonitor) IsAlive(nodeID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	last, ok := m.lastSeen[nodeID]
	if !ok {
		return false
	}
	return time.Since(last) < m.timeout
}

// LastSeen returns the last heartbeat time for a node, or zero if unknown.
func (m *HeartbeatMonitor) LastSeen(nodeID string) time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastSeen[nodeID]
}

// AliveNodes returns a list of node IDs with recent heartbeats.
func (m *HeartbeatMonitor) AliveNodes() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var nodes []string
	now := time.Now()
	for id, last := range m.lastSeen {
		if now.Sub(last) < m.timeout {
			nodes = append(nodes, id)
		}
	}
	return nodes
}

func (m *HeartbeatMonitor) check() {
	// List all node directories under node-to-control/.
	// Each node writes heartbeats to node-to-control/{nodeID}/heartbeat-*.hb
	// We discover nodes by listing the top-level prefix for subdirectories,
	// then check each node's heartbeat files.
	keys, err := m.store.List(m.ctx, heartbeatPrefix)
	if err != nil {
		if m.ctx.Err() != nil {
			return
		}
		klog.V(4).InfoS("HeartbeatMonitor list error", "err", err)
		return
	}

	// The List at the top-level returns node directories (for FSStore).
	// For each discovered node, list their heartbeat files.
	discoveredNodes := make(map[string]bool)
	for _, key := range keys {
		nodeID := extractNodeID(key)
		if nodeID == "" {
			continue
		}
		if discoveredNodes[nodeID] {
			continue
		}
		discoveredNodes[nodeID] = true
		m.checkNode(nodeID)
	}

	// Check for stale nodes.
	m.mu.RLock()
	now := time.Now()
	var staleNodes []string
	for id, last := range m.lastSeen {
		if now.Sub(last) >= m.timeout {
			staleNodes = append(staleNodes, id)
		}
	}
	m.mu.RUnlock()

	for _, id := range staleNodes {
		klog.V(2).InfoS("Node heartbeat timed out", "nodeID", id)
		m.mu.Lock()
		delete(m.lastSeen, id)
		m.mu.Unlock()
		if m.OnNodeStale != nil {
			m.OnNodeStale(id)
		}
	}
}

func (m *HeartbeatMonitor) checkNode(nodeID string) {
	prefix := fmt.Sprintf("%s%s/", heartbeatPrefix, nodeID)
	keys, err := m.store.List(m.ctx, prefix)
	if err != nil {
		if m.ctx.Err() != nil {
			return
		}
		klog.V(4).InfoS("HeartbeatMonitor node list error", "nodeID", nodeID, "err", err)
		return
	}

	// Find heartbeat files and read the latest one.
	for i := len(keys) - 1; i >= 0; i-- {
		key := keys[i]
		if !isHeartbeatKey(key) {
			continue
		}

		data, err := m.store.Get(m.ctx, key)
		if err != nil {
			continue
		}
		if len(data) < 8 {
			continue
		}

		tsMs := int64(binary.LittleEndian.Uint64(data))
		ts := time.UnixMilli(tsMs)

		m.mu.Lock()
		_, known := m.lastSeen[nodeID]
		m.lastSeen[nodeID] = ts
		m.mu.Unlock()

		if !known {
			klog.V(2).InfoS("New node discovered via heartbeat", "nodeID", nodeID)
			if m.OnNodeDiscovered != nil {
				m.OnNodeDiscovered(nodeID)
			}
		}

		klog.V(5).InfoS("Heartbeat received", "nodeID", nodeID, "timestamp", ts)
		return
	}
}

// extractNodeID gets the node ID from a key like "node-to-control/node-1/something".
func extractNodeID(key string) string {
	// Strip prefix.
	rest := key
	if len(rest) > len(heartbeatPrefix) {
		rest = rest[len(heartbeatPrefix):]
	}
	// The node ID is the first path component.
	for i, c := range rest {
		if c == '/' {
			return rest[:i]
		}
	}
	// If no slash, the whole thing might be a node directory name.
	return rest
}

// isHeartbeatKey checks if a key is a heartbeat file.
func isHeartbeatKey(key string) bool {
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == '/' {
			name := key[i+1:]
			return len(name) > len("heartbeat-") && name[:len("heartbeat-")] == "heartbeat-"
		}
	}
	return false
}
