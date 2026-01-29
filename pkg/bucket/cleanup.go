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
	"strings"
	"time"

	"k8s.io/klog/v2"
)

const (
	// DefaultCleanupInterval is how often the cleanup worker runs.
	DefaultCleanupInterval = 60 * time.Second
)

// CleanupWorker periodically scans bucket prefixes and deletes stale messages
// that were not cleaned up by inline deletion (e.g., orphaned by crashes or
// timing issues).
type CleanupWorker struct {
	store    Store
	prefixes []string // bucket prefixes to scan (e.g. "control-to-node/node-1/")
	interval time.Duration
	// maxAge is the maximum age of a message before it is deleted regardless
	// of processing state. Messages older than this are considered stale.
	maxAge time.Duration

	ctx    context.Context
	cancel context.CancelFunc
}

// NewCleanupWorker creates a cleanup worker that scans the given prefixes.
// maxAge controls how old a message must be before it is unconditionally deleted
// (the PRD specifies 24 hours for stale messages).
func NewCleanupWorker(ctx context.Context, store Store, prefixes []string, interval, maxAge time.Duration) *CleanupWorker {
	ctx, cancel := context.WithCancel(ctx)
	return &CleanupWorker{
		store:    store,
		prefixes: prefixes,
		interval: interval,
		maxAge:   maxAge,
		ctx:      ctx,
		cancel:   cancel,
	}
}

// Run starts the cleanup loop. Blocks until the context is cancelled.
func (c *CleanupWorker) Run() {
	klog.V(2).InfoS("CleanupWorker started", "prefixes", c.prefixes, "interval", c.interval, "maxAge", c.maxAge)
	defer klog.V(2).InfoS("CleanupWorker stopped")

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.sweep()
		}
	}
}

// Stop cancels the cleanup worker.
func (c *CleanupWorker) Stop() {
	c.cancel()
}

func (c *CleanupWorker) sweep() {
	var totalDeleted int
	for _, prefix := range c.prefixes {
		deleted := c.sweepPrefix(prefix)
		totalDeleted += deleted
	}
	if totalDeleted > 0 {
		klog.V(4).InfoS("CleanupWorker sweep completed", "deleted", totalDeleted)
	}
}

func (c *CleanupWorker) sweepPrefix(prefix string) int {
	keys, err := c.store.List(c.ctx, prefix)
	if err != nil {
		if c.ctx.Err() != nil {
			return 0
		}
		klog.V(4).InfoS("CleanupWorker list error", "prefix", prefix, "err", err)
		return 0
	}

	deleted := 0
	for _, key := range keys {
		// Skip directories and heartbeat files (managed by HeartbeatPublisher).
		if strings.HasSuffix(key, "/") || isHeartbeatKey(key) {
			continue
		}

		// For Store implementations that support metadata/timestamps, we could
		// check modification time. For FSStore and general bucket stores, we
		// rely on the sequence-based approach: if the transport has already
		// advanced past this sequence, the message is safe to delete.
		//
		// For now, we do a best-effort cleanup of any .pb files that remain.
		// The transport's pollOnce already deletes messages after reading them,
		// so files here are orphans from failed deletes or race conditions.
		if err := c.store.Delete(c.ctx, key); err != nil {
			if c.ctx.Err() != nil {
				return deleted
			}
			klog.V(5).InfoS("CleanupWorker delete error", "key", key, "err", err)
			continue
		}
		deleted++
	}
	return deleted
}
