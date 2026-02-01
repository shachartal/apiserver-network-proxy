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
	"testing"
	"time"
)

func TestHeartbeatPublishAndMonitor(t *testing.T) {
	store := NewFSStore(t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start a publisher with a fast interval.
	pub := NewHeartbeatPublisher(ctx, store, "node-1", 100*time.Millisecond)
	go pub.Run()
	defer pub.Stop()

	// Start a monitor with a fast poll interval.
	mon := NewHeartbeatMonitor(ctx, store, 100*time.Millisecond, 5*time.Minute)
	go mon.Run()
	defer mon.Stop()

	// Wait for at least one heartbeat to be published and detected.
	deadline := time.After(5 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("Timed out waiting for heartbeat detection")
		default:
		}

		if mon.IsAlive("node-1") {
			break
		}
		time.Sleep(50 * time.Millisecond)
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
}

func TestHeartbeatStaleDetection(t *testing.T) {
	store := NewFSStore(t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Write a heartbeat file directly (simulating an agent that crashed
	// without graceful shutdown, so its heartbeat file remains).
	ts := time.Now().UnixMilli()
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, uint64(ts))
	if err := store.Put(ctx, "node-to-control/stale-node/heartbeat-0000000001.hb", data); err != nil {
		t.Fatalf("Failed to write heartbeat: %v", err)
	}

	// Monitor with a very short timeout to trigger staleness quickly.
	staleCh := make(chan string, 1)
	mon := NewHeartbeatMonitor(ctx, store, 50*time.Millisecond, 300*time.Millisecond)
	mon.OnNodeStale = func(nodeID string) {
		select {
		case staleCh <- nodeID:
		default:
		}
	}
	go mon.Run()
	defer mon.Stop()

	// Wait for stale detection.
	select {
	case nodeID := <-staleCh:
		if nodeID != "stale-node" {
			t.Errorf("Expected stale-node, got %s", nodeID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Timed out waiting for stale node detection")
	}
}
