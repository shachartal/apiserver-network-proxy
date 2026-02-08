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
	prevKey  string // last published heartbeat key, for deletion

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
	if h.prevKey == "" {
		return
	}
	// Use a fresh context since h.ctx is already cancelled.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.store.Delete(ctx, h.prevKey); err != nil {
		klog.V(2).InfoS("Failed to delete heartbeat on shutdown", "nodeID", h.nodeID, "key", h.prevKey, "err", err)
	} else {
		klog.V(2).InfoS("Deleted heartbeat on shutdown", "nodeID", h.nodeID, "key", h.prevKey)
	}
}

// Stop cancels the publisher.
func (h *HeartbeatPublisher) Stop() {
	h.cancel()
}

func (h *HeartbeatPublisher) publish() {
	seq := h.seq.Add(1)
	tsMs := time.Now().UnixMilli()
	key := fmt.Sprintf("%s%s/heartbeat-%0*d-%d%s", heartbeatPrefix, h.nodeID, seqWidth, seq, tsMs, heartbeatSuffix)

	// Timestamp is encoded in the key; payload is empty.
	if err := h.store.Put(h.ctx, key, nil); err != nil {
		if h.ctx.Err() != nil {
			return
		}
		klog.V(2).InfoS("Failed to publish heartbeat", "nodeID", h.nodeID, "err", err)
		return
	}

	// Clean up the previous heartbeat file to avoid accumulation.
	if h.prevKey != "" {
		_ = h.store.Delete(h.ctx, h.prevKey)
	}
	h.prevKey = key

	klog.V(5).InfoS("Heartbeat published", "nodeID", h.nodeID, "seq", seq)
}

// HeartbeatMonitor watches for agent heartbeats on the server side.
// It tracks the last seen heartbeat time for each node and reports
// nodes that have gone stale.
//
// Heartbeat updates are driven by RegionalPoller: when the poller sees a
// heartbeat key during its ListRecursive scan, it calls UpdateHeartbeat
// which parses the timestamp embedded in the key filename. This avoids
// any Get calls, saving one Class B GCS operation per node per poll cycle.
// The monitor's own tick loop only scans for stale nodes.
type HeartbeatMonitor struct {
	pollInterval time.Duration
	timeout      time.Duration

	mu        sync.RWMutex
	lastSeen  map[string]time.Time   // nodeID → last heartbeat time
	lastHBKey map[string]string      // nodeID → last heartbeat key seen (to avoid re-parses)

	ctx    context.Context
	cancel context.CancelFunc

	// OnNodeDiscovered is called when a new node's heartbeat is seen for the first time.
	OnNodeDiscovered func(nodeID string)

	// OnNodeStale is called when a node's heartbeat exceeds the timeout.
	OnNodeStale func(nodeID string)
}

// NewHeartbeatMonitor creates a monitor that watches the bucket for agent heartbeats.
func NewHeartbeatMonitor(ctx context.Context, pollInterval, timeout time.Duration) *HeartbeatMonitor {
	ctx, cancel := context.WithCancel(ctx)
	return &HeartbeatMonitor{
		pollInterval: pollInterval,
		timeout:      timeout,
		lastSeen:     make(map[string]time.Time),
		lastHBKey:    make(map[string]string),
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

// NotifyNodeSeen is called by RegionalPoller when it discovers a node via
// ListRecursive. This allows HeartbeatMonitor to track nodes passively
// without doing its own List calls for node discovery.
func (m *HeartbeatMonitor) NotifyNodeSeen(nodeID string) {
	m.mu.Lock()
	_, known := m.lastSeen[nodeID]
	if !known {
		// Initialize with current time; UpdateHeartbeat will update with actual heartbeat timestamp.
		m.lastSeen[nodeID] = time.Now()
	}
	m.mu.Unlock()

	if !known {
		klog.V(2).InfoS("New node discovered via RegionalPoller", "nodeID", nodeID)
		if m.OnNodeDiscovered != nil {
			m.OnNodeDiscovered(nodeID)
		}
	}
}

// UpdateHeartbeat is called by RegionalPoller when it sees a heartbeat key
// during its ListRecursive scan. It parses the timestamp embedded in the
// key filename (heartbeat-{seq}-{tsMs}.hb) and updates the node's last-seen
// time without any Get calls.
func (m *HeartbeatMonitor) UpdateHeartbeat(nodeID, key string) {
	// Skip if we already processed this exact heartbeat key.
	m.mu.RLock()
	if m.lastHBKey[nodeID] == key {
		m.mu.RUnlock()
		return
	}
	m.mu.RUnlock()

	ts, err := parseTimestampFromHBKey(key)
	if err != nil {
		klog.V(4).InfoS("HeartbeatMonitor failed to parse heartbeat key", "nodeID", nodeID, "key", key, "err", err)
		return
	}

	m.mu.Lock()
	_, known := m.lastSeen[nodeID]
	m.lastSeen[nodeID] = ts
	m.lastHBKey[nodeID] = key
	m.mu.Unlock()

	if !known {
		klog.V(2).InfoS("New node discovered via heartbeat", "nodeID", nodeID)
		if m.OnNodeDiscovered != nil {
			m.OnNodeDiscovered(nodeID)
		}
	}

	klog.V(5).InfoS("Heartbeat received", "nodeID", nodeID, "timestamp", ts)
}

// check scans for stale nodes in the in-memory map. Heartbeat updates
// are now driven by RegionalPoller calling UpdateHeartbeat, so this
// method no longer needs to do per-node List+Get calls.
func (m *HeartbeatMonitor) check() {
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
		delete(m.lastHBKey, id)
		m.mu.Unlock()
		if m.OnNodeStale != nil {
			m.OnNodeStale(id)
		}
	}
}

// parseTimestampFromHBKey extracts the millisecond timestamp from a heartbeat
// key of the form "…/heartbeat-{seq}-{tsMs}.hb".
func parseTimestampFromHBKey(key string) (time.Time, error) {
	// Find the filename after the last '/'.
	name := key
	if i := len(key) - 1; i >= 0 {
		for ; i >= 0; i-- {
			if key[i] == '/' {
				name = key[i+1:]
				break
			}
		}
	}
	// Strip the ".hb" suffix.
	if len(name) < len(heartbeatSuffix) || name[len(name)-len(heartbeatSuffix):] != heartbeatSuffix {
		return time.Time{}, fmt.Errorf("key %q missing %s suffix", key, heartbeatSuffix)
	}
	name = name[:len(name)-len(heartbeatSuffix)]
	// name is now "heartbeat-{seq}-{tsMs}"
	// Find the last '-' to extract the timestamp portion.
	lastDash := -1
	for i := len(name) - 1; i >= 0; i-- {
		if name[i] == '-' {
			lastDash = i
			break
		}
	}
	if lastDash < 0 {
		return time.Time{}, fmt.Errorf("key %q has no timestamp field", key)
	}
	tsStr := name[lastDash+1:]
	var tsMs int64
	for _, c := range tsStr {
		if c < '0' || c > '9' {
			return time.Time{}, fmt.Errorf("key %q has non-numeric timestamp %q", key, tsStr)
		}
		tsMs = tsMs*10 + int64(c-'0')
	}
	return time.UnixMilli(tsMs), nil
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
