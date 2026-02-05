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
	"time"

	"google.golang.org/protobuf/proto"
	"k8s.io/klog/v2"

	client "sigs.k8s.io/apiserver-network-proxy/konnectivity-client/proto/client"
)

// AgentPoller performs consolidated polling for the agent side.
// Instead of each transport (forward + reverse) polling independently with
// separate List calls, a single AgentPoller does one ListRecursive per poll
// cycle on "control-to-node/{nodeID}/" and dispatches messages to the
// appropriate transport based on subdirectory (fwd/ or rev/).
//
// This halves the number of List API calls the agent makes.
type AgentPoller struct {
	store  Store
	prefix string // "control-to-node/{nodeID}/"

	fwdTransport *BucketTransport
	revTransport *BucketTransport
	fwdRecvSeq   uint64
	revRecvSeq   uint64

	adaptive     bool
	pollInterval time.Duration

	ctx    context.Context
	cancel context.CancelFunc
}

// NewAgentPoller creates a consolidated poller for the agent side.
// It polls "control-to-node/{nodeID}/" with ListRecursive and dispatches
// messages to the forward and reverse transports.
// If pollInterval is 0, adaptive polling is used (500ms–10s based on activity).
// revTransport may be nil if reverse proxy is not enabled.
func NewAgentPoller(ctx context.Context, store Store, nodeID string, fwdTransport, revTransport *BucketTransport, pollInterval time.Duration) *AgentPoller {
	adaptive := pollInterval == 0
	if adaptive {
		pollInterval = minPollInterval
	}
	ctx, cancel := context.WithCancel(ctx)
	return &AgentPoller{
		store:        store,
		prefix:       "control-to-node/" + nodeID + "/",
		fwdTransport: fwdTransport,
		revTransport: revTransport,
		adaptive:     adaptive,
		pollInterval: pollInterval,
		ctx:          ctx,
		cancel:       cancel,
	}
}

// Run starts the consolidated polling loop. Blocks until the context is cancelled.
func (p *AgentPoller) Run() {
	klog.V(2).InfoS("AgentPoller started", "prefix", p.prefix, "hasReverse", p.revTransport != nil)
	defer klog.V(2).InfoS("AgentPoller stopped")
	defer p.closeChannels()

	currentInterval := p.pollInterval
	timer := time.NewTimer(currentInterval)
	defer timer.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-timer.C:
			found := p.pollOnce()
			if p.adaptive {
				currentInterval = adaptiveInterval(currentInterval, found)
			}
			timer.Reset(currentInterval)
		}
	}
}

// Stop cancels the poller.
func (p *AgentPoller) Stop() {
	p.cancel()
}

func (p *AgentPoller) closeChannels() {
	// Close the receive channels so that transports' Recv() returns EOF.
	// Only close channels for non-nil transports.
	if p.fwdTransport != nil && p.fwdTransport.recvCh != nil {
		close(p.fwdTransport.recvCh)
	}
	if p.revTransport != nil && p.revTransport.recvCh != nil {
		close(p.revTransport.recvCh)
	}
}

// pollOnce does one ListRecursive and dispatches messages to transports.
// Returns true if any new messages were found and processed.
func (p *AgentPoller) pollOnce() bool {
	keys, err := p.store.ListRecursive(p.ctx, p.prefix)
	if err != nil {
		if p.ctx.Err() != nil {
			return false
		}
		klog.V(4).InfoS("AgentPoller list error", "prefix", p.prefix, "err", err)
		return false
	}

	// Sort to process each channel's messages in sequence order.
	sort.Strings(keys)

	found := false
	for _, key := range keys {
		// Parse the relative path after the prefix to determine channel.
		// e.g. "control-to-node/node-1/fwd/00001.pb" → relPath = "fwd/00001.pb"
		relPath := strings.TrimPrefix(key, p.prefix)

		var transport *BucketTransport
		var recvSeq *uint64
		switch {
		case strings.HasPrefix(relPath, "fwd/"):
			if p.fwdTransport == nil {
				continue
			}
			transport = p.fwdTransport
			recvSeq = &p.fwdRecvSeq
		case strings.HasPrefix(relPath, "rev/"):
			if p.revTransport == nil {
				continue
			}
			transport = p.revTransport
			recvSeq = &p.revRecvSeq
		default:
			// Unknown subdirectory, skip.
			continue
		}

		seq, err := parseSeqFromKey(key)
		if err != nil {
			continue
		}

		if seq <= *recvSeq {
			// Already processed; delete it.
			_ = p.store.Delete(p.ctx, key)
			continue
		}

		data, err := p.store.Get(p.ctx, key)
		if err != nil {
			if p.ctx.Err() != nil {
				return found
			}
			klog.V(4).InfoS("AgentPoller get error", "key", key, "err", err)
			continue
		}

		pkt := &client.Packet{}
		if err := proto.Unmarshal(data, pkt); err != nil {
			klog.ErrorS(err, "AgentPoller unmarshal error", "key", key)
			_ = p.store.Delete(p.ctx, key)
			continue
		}

		*recvSeq = seq
		found = true
		_ = p.store.Delete(p.ctx, key)

		select {
		case transport.recvCh <- pkt:
		case <-p.ctx.Done():
			return found
		}
	}
	return found
}
