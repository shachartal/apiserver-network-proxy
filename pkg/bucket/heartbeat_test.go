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
	"testing"
	"time"
)

func TestHeartbeatPublishAndMonitor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Build a heartbeat key with an embedded timestamp (the new format).
	tsMs := time.Now().UnixMilli()
	hbKey := fmt.Sprintf("node-to-control/node-1/heartbeat-%0*d-%d.hb", seqWidth, 1, tsMs)

	// Create a monitor — heartbeat updates are driven by UpdateHeartbeat,
	// not the monitor's own tick loop. No store needed.
	mon := NewHeartbeatMonitor(ctx, 30*time.Second, 5*time.Minute)
	defer mon.Stop()

	// Simulate RegionalPoller discovering the heartbeat key and calling
	// UpdateHeartbeat, which parses the timestamp from the key.
	mon.UpdateHeartbeat("node-1", hbKey)

	if !mon.IsAlive("node-1") {
		t.Fatal("Expected node-1 to be alive after UpdateHeartbeat")
	}

	last := mon.LastSeen("node-1")
	if last.IsZero() {
		t.Fatal("LastSeen returned zero time")
	}
	if time.Since(last) > 5*time.Second {
		t.Errorf("LastSeen too old: %v", time.Since(last))
	}

	alive := mon.AliveNodes()
	found := false
	for _, id := range alive {
		if id == "node-1" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("node-1 not in AliveNodes: %v", alive)
	}

	// Verify that calling UpdateHeartbeat with the same key is a no-op
	// (the dedup via lastHBKey should skip re-parsing).
	mon.UpdateHeartbeat("node-1", hbKey)
}

func TestHeartbeatStaleDetection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Build a heartbeat key with a timestamp far in the past so it's already stale.
	staleTs := time.Now().Add(-10 * time.Minute).UnixMilli()
	hbKey := fmt.Sprintf("node-to-control/stale-node/heartbeat-%0*d-%d.hb", seqWidth, 1, staleTs)

	// Monitor with a very short timeout to trigger staleness quickly.
	staleCh := make(chan string, 1)
	mon := NewHeartbeatMonitor(ctx, 50*time.Millisecond, 300*time.Millisecond)
	mon.OnNodeStale = func(nodeID string) {
		select {
		case staleCh <- nodeID:
		default:
		}
	}
	go mon.Run()
	defer mon.Stop()

	// Simulate RegionalPoller seeing the heartbeat and calling UpdateHeartbeat.
	// This parses the stale timestamp from the key.
	mon.UpdateHeartbeat("stale-node", hbKey)

	// Wait for stale detection (the monitor's tick loop checks for stale nodes).
	select {
	case nodeID := <-staleCh:
		if nodeID != "stale-node" {
			t.Errorf("Expected stale-node, got %s", nodeID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Timed out waiting for stale node detection")
	}
}
