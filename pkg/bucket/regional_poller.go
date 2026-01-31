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
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"k8s.io/klog/v2"

	client "sigs.k8s.io/apiserver-network-proxy/konnectivity-client/proto/client"
)

// RegionalPoller performs centralized polling for a region on the server side.
// Instead of each per-node BucketTransport polling independently, a single
// RegionalPoller does one LIST per poll interval on the response prefix
// (node-to-control/) and dispatches discovered messages to the appropriate
// node handler. This dramatically reduces bucket API calls at scale.
type RegionalPoller struct {
	store  Store
	prefix string // e.g. "node-to-control/"

	mu       sync.RWMutex
	handlers map[string]*nodeHandler // nodeID → handler

	// workerCount controls how many concurrent GET requests are made
	// after LIST discovery. Defaults to DefaultWorkerCount.
	workerCount int

	ctx    context.Context
	cancel context.CancelFunc
}

// nodeHandler tracks per-node state for dispatching messages.
type nodeHandler struct {
	recvSeq uint64
	recvCh  chan *client.Packet
}

const (
	// DefaultWorkerCount is the default number of concurrent download workers.
	DefaultWorkerCount = 10
)

// NewRegionalPoller creates a poller that watches the given prefix for messages
// from all nodes and dispatches them to registered handlers.
func NewRegionalPoller(ctx context.Context, store Store, prefix string) *RegionalPoller {
	ctx, cancel := context.WithCancel(ctx)
	return &RegionalPoller{
		store:       store,
		prefix:      ensureTrailingSlash(prefix),
		handlers:    make(map[string]*nodeHandler),
		workerCount: DefaultWorkerCount,
		ctx:         ctx,
		cancel:      cancel,
	}
}

// SetWorkerCount configures the number of concurrent download workers.
// Must be called before Run().
func (r *RegionalPoller) SetWorkerCount(n int) {
	if n < 1 {
		n = 1
	}
	r.workerCount = n
}

// RegisterNode adds a node to the poller. Returns a channel that will receive
// packets addressed to this node. The returned channel is closed when the node
// is unregistered or the poller is stopped.
func (r *RegionalPoller) RegisterNode(nodeID string) <-chan *client.Packet {
	r.mu.Lock()
	defer r.mu.Unlock()

	h := &nodeHandler{
		recvCh: make(chan *client.Packet, 100),
	}
	r.handlers[nodeID] = h
	klog.V(2).InfoS("RegionalPoller: node registered", "nodeID", nodeID)
	return h.recvCh
}

// UnregisterNode removes a node from the poller and closes its receive channel.
func (r *RegionalPoller) UnregisterNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if h, ok := r.handlers[nodeID]; ok {
		close(h.recvCh)
		delete(r.handlers, nodeID)
		klog.V(2).InfoS("RegionalPoller: node unregistered", "nodeID", nodeID)
	}
}

// Run starts the centralized polling loop with adaptive intervals.
// Blocks until the context is cancelled.
func (r *RegionalPoller) Run() {
	klog.V(2).InfoS("RegionalPoller started", "prefix", r.prefix, "workers", r.workerCount)
	defer klog.V(2).InfoS("RegionalPoller stopped")
	defer r.closeAll()

	currentInterval := minPollInterval
	timer := time.NewTimer(currentInterval)
	defer timer.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-timer.C:
			found := r.pollOnce()
			currentInterval = adaptiveInterval(currentInterval, found)
			timer.Reset(currentInterval)
		}
	}
}

// Stop cancels the poller.
func (r *RegionalPoller) Stop() {
	r.cancel()
}

func (r *RegionalPoller) closeAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, h := range r.handlers {
		close(h.recvCh)
		delete(r.handlers, id)
	}
}

// messageRef holds a discovered message's metadata for download.
type messageRef struct {
	nodeID string
	key    string
	seq    uint64
}

func (r *RegionalPoller) pollOnce() bool {
	// Discover all node directories under the prefix.
	topKeys, err := r.store.List(r.ctx, r.prefix)
	if err != nil {
		if r.ctx.Err() != nil {
			return false
		}
		klog.V(4).InfoS("RegionalPoller list error", "prefix", r.prefix, "err", err)
		return false
	}

	// Collect message refs from all nodes.
	var refs []messageRef
	r.mu.RLock()
	for _, key := range topKeys {
		nodeID := extractNodeIDFromPrefixedKey(r.prefix, key)
		if nodeID == "" {
			continue
		}
		h, ok := r.handlers[nodeID]
		if !ok {
			continue
		}

		nodePrefix := r.prefix + nodeID + "/"
		nodeKeys, err := r.store.List(r.ctx, nodePrefix)
		if err != nil {
			if r.ctx.Err() != nil {
				r.mu.RUnlock()
				return false
			}
			continue
		}

		for _, nk := range nodeKeys {
			seq, err := parseSeqFromKey(nk)
			if err != nil {
				continue
			}
			if seq <= h.recvSeq {
				// Already processed; queue for deletion.
				_ = r.store.Delete(r.ctx, nk)
				continue
			}
			refs = append(refs, messageRef{nodeID: nodeID, key: nk, seq: seq})
		}
	}
	r.mu.RUnlock()

	if len(refs) == 0 {
		return false
	}

	// Download and dispatch messages using a worker pool.
	r.downloadAndDispatch(refs)
	return true
}

func (r *RegionalPoller) downloadAndDispatch(refs []messageRef) {
	type result struct {
		ref messageRef
		pkt *client.Packet
	}

	results := make(chan result, len(refs))
	work := make(chan messageRef, len(refs))

	// Start workers.
	workerCount := r.workerCount
	if workerCount > len(refs) {
		workerCount = len(refs)
	}

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ref := range work {
				data, err := r.store.Get(r.ctx, ref.key)
				if err != nil {
					if r.ctx.Err() != nil {
						return
					}
					klog.V(4).InfoS("RegionalPoller get error", "key", ref.key, "err", err)
					continue
				}

				pkt := &client.Packet{}
				if err := proto.Unmarshal(data, pkt); err != nil {
					klog.ErrorS(err, "RegionalPoller unmarshal error", "key", ref.key)
					_ = r.store.Delete(r.ctx, ref.key)
					continue
				}

				results <- result{ref: ref, pkt: pkt}
			}
		}()
	}

	// Enqueue work.
	for _, ref := range refs {
		work <- ref
	}
	close(work)

	// Wait for all workers, then close results.
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect all results, then sort by sequence number to guarantee in-order
	// delivery. The worker pool downloads in parallel, so results arrive in
	// arbitrary order — but TLS and other stream protocols require strict ordering.
	var collected []result
	for res := range results {
		collected = append(collected, res)
	}
	sort.Slice(collected, func(i, j int) bool {
		return collected[i].ref.seq < collected[j].ref.seq
	})

	// Group by nodeID so we can check contiguity per-node.
	byNode := make(map[string][]result)
	for _, res := range collected {
		byNode[res.ref.nodeID] = append(byNode[res.ref.nodeID], res)
	}

	// Dispatch only the contiguous prefix for each node. If there is a gap
	// (e.g., seq 1,3 with 2 missing), deliver only up to the gap. Messages
	// beyond the gap are left in the bucket for the next poll cycle.
	// This prevents out-of-order delivery that would corrupt TLS streams.
	for nodeID, nodeResults := range byNode {
		r.mu.RLock()
		h, ok := r.handlers[nodeID]
		r.mu.RUnlock()

		if !ok {
			for _, res := range nodeResults {
				_ = r.store.Delete(r.ctx, res.ref.key)
			}
			continue
		}

		r.mu.RLock()
		expectedSeq := h.recvSeq + 1
		r.mu.RUnlock()

		for _, res := range nodeResults {
			if res.ref.seq != expectedSeq {
				// Gap detected — stop delivering for this node.
				// Remaining messages stay in the bucket for next poll.
				klog.V(4).InfoS("RegionalPoller gap detected, deferring remaining messages",
					"nodeID", nodeID, "expected", expectedSeq, "got", res.ref.seq)
				break
			}

			r.mu.Lock()
			h.recvSeq = res.ref.seq
			r.mu.Unlock()

			_ = r.store.Delete(r.ctx, res.ref.key)

			select {
			case h.recvCh <- res.pkt:
			case <-r.ctx.Done():
				return
			}
			expectedSeq++
		}
	}
}

// extractNodeIDFromPrefixedKey extracts the node ID from a key like "node-to-control/node-1/"
func extractNodeIDFromPrefixedKey(prefix, key string) string {
	rest := strings.TrimPrefix(key, prefix)
	rest = strings.TrimSuffix(rest, "/")
	// Should be just the node ID with no more slashes.
	if strings.Contains(rest, "/") {
		return ""
	}
	return rest
}

// adaptiveInterval computes the next poll interval based on activity.
func adaptiveInterval(current time.Duration, foundMessages bool) time.Duration {
	if foundMessages {
		return minPollInterval
	}
	next := time.Duration(float64(current) * backoffMultiplier)
	if next > maxPollInterval {
		return maxPollInterval
	}
	return next
}
