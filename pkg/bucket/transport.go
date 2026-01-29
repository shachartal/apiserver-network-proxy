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
	"io"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"
	"k8s.io/klog/v2"

	client "sigs.k8s.io/apiserver-network-proxy/konnectivity-client/proto/client"
)

const (
	// seqWidth is the zero-pad width for sequence numbers in filenames.
	seqWidth = 20
	// fileSuffix is the extension for bucket message files.
	fileSuffix = ".pb"
	// minPollInterval is the fastest poll rate when messages are flowing.
	minPollInterval = 100 * time.Millisecond
	// maxPollInterval is the slowest poll rate when idle.
	maxPollInterval = 5 * time.Second
	// backoffMultiplier is the exponential backoff factor for idle polling.
	backoffMultiplier = 2.0
)

// BucketTransport provides Send/Recv semantics over a bucket Store.
// Each transport is bound to a specific send prefix and recv prefix,
// representing one direction pair (e.g., server→node and node→server).
type BucketTransport struct {
	store        Store
	sendPrefix   string // e.g. "control-to-node/node-1/"
	recvPrefix   string // e.g. "node-to-control/node-1/"
	sendSeq      atomic.Uint64
	recvSeq      uint64 // only accessed by polling goroutine
	pollInterval time.Duration
	adaptive     bool // when true, dynamically adjust poll interval

	ctx    context.Context
	cancel context.CancelFunc
	recvCh chan *client.Packet
}

// NewBucketTransport creates a transport that sends to sendPrefix and receives from recvPrefix.
// If pollInterval is 0, adaptive polling is enabled (100ms–5s based on activity).
func NewBucketTransport(ctx context.Context, store Store, sendPrefix, recvPrefix string, pollInterval time.Duration) *BucketTransport {
	adaptive := pollInterval == 0
	if adaptive {
		pollInterval = minPollInterval
	}
	ctx, cancel := context.WithCancel(ctx)
	t := &BucketTransport{
		store:        store,
		sendPrefix:   ensureTrailingSlash(sendPrefix),
		recvPrefix:   ensureTrailingSlash(recvPrefix),
		pollInterval: pollInterval,
		adaptive:     adaptive,
		ctx:          ctx,
		cancel:       cancel,
		recvCh:       make(chan *client.Packet, 100),
	}
	go t.pollLoop()
	return t
}

// newSendOnlyTransport creates a transport that can only send. Recv is fed
// externally (e.g., by a RegionalPoller pushing to recvCh).
func newSendOnlyTransport(ctx context.Context, store Store, sendPrefix string) *BucketTransport {
	ctx, cancel := context.WithCancel(ctx)
	return &BucketTransport{
		store:      store,
		sendPrefix: ensureTrailingSlash(sendPrefix),
		ctx:        ctx,
		cancel:     cancel,
		recvCh:     make(chan *client.Packet, 100),
	}
}

// Send marshals a Konnectivity Packet and writes it to the bucket.
func (t *BucketTransport) Send(pkt *client.Packet) error {
	data, err := proto.Marshal(pkt)
	if err != nil {
		return fmt.Errorf("marshal packet: %w", err)
	}

	seq := t.sendSeq.Add(1)
	key := fmt.Sprintf("%s%0*d%s", t.sendPrefix, seqWidth, seq, fileSuffix)

	if err := t.store.Put(t.ctx, key, data); err != nil {
		return fmt.Errorf("bucket put %s: %w", key, err)
	}
	return nil
}

// Recv blocks until a packet is available or the transport is closed.
// Returns io.EOF when the transport is closed.
func (t *BucketTransport) Recv() (*client.Packet, error) {
	select {
	case pkt, ok := <-t.recvCh:
		if !ok {
			return nil, io.EOF
		}
		return pkt, nil
	case <-t.ctx.Done():
		return nil, io.EOF
	}
}

// Close shuts down the transport and its polling goroutine.
func (t *BucketTransport) Close() {
	t.cancel()
}

// pollLoop continuously polls the bucket for new messages and pushes them to recvCh.
func (t *BucketTransport) pollLoop() {
	defer close(t.recvCh)

	currentInterval := t.pollInterval
	timer := time.NewTimer(currentInterval)
	defer timer.Stop()

	for {
		select {
		case <-t.ctx.Done():
			return
		case <-timer.C:
			found := t.pollOnce()
			if t.adaptive {
				currentInterval = t.nextInterval(currentInterval, found)
			}
			timer.Reset(currentInterval)
		}
	}
}

// nextInterval computes the next poll interval based on whether messages were found.
func (t *BucketTransport) nextInterval(current time.Duration, foundMessages bool) time.Duration {
	if foundMessages {
		return minPollInterval
	}
	next := time.Duration(float64(current) * backoffMultiplier)
	if next > maxPollInterval {
		return maxPollInterval
	}
	return next
}

// pollOnce polls for new messages. Returns true if any messages were found and processed.
func (t *BucketTransport) pollOnce() bool {
	keys, err := t.store.List(t.ctx, t.recvPrefix)
	if err != nil {
		if t.ctx.Err() != nil {
			return false
		}
		klog.V(4).InfoS("Bucket list error", "prefix", t.recvPrefix, "err", err)
		return false
	}

	found := false
	for _, key := range keys {
		seq, err := parseSeqFromKey(key)
		if err != nil {
			klog.V(4).InfoS("Skipping unparseable key", "key", key, "err", err)
			continue
		}
		if seq <= t.recvSeq {
			// Already processed; delete it.
			_ = t.store.Delete(t.ctx, key)
			continue
		}

		data, err := t.store.Get(t.ctx, key)
		if err != nil {
			if t.ctx.Err() != nil {
				return found
			}
			klog.V(4).InfoS("Bucket get error", "key", key, "err", err)
			continue
		}

		pkt := &client.Packet{}
		if err := proto.Unmarshal(data, pkt); err != nil {
			klog.ErrorS(err, "Failed to unmarshal packet", "key", key)
			_ = t.store.Delete(t.ctx, key)
			continue
		}

		t.recvSeq = seq
		found = true

		// Delete after successful read.
		_ = t.store.Delete(t.ctx, key)

		select {
		case t.recvCh <- pkt:
		case <-t.ctx.Done():
			return found
		}
	}
	return found
}

// parseSeqFromKey extracts the sequence number from a key like "prefix/00000000001.pb".
func parseSeqFromKey(key string) (uint64, error) {
	// Get the filename part after the last slash.
	idx := strings.LastIndex(key, "/")
	name := key
	if idx >= 0 {
		name = key[idx+1:]
	}
	// Strip suffix.
	name = strings.TrimSuffix(name, fileSuffix)
	return strconv.ParseUint(name, 10, 64)
}
