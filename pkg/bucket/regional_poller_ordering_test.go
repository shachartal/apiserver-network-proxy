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

	"google.golang.org/protobuf/proto"

	clientproto "sigs.k8s.io/apiserver-network-proxy/konnectivity-client/proto/client"
)

// writePacket writes a protobuf packet to the store at the given sequence number
// under the node-to-control/{nodeID}/ prefix.
func writePacket(t *testing.T, store Store, nodeID string, seq uint64, payload string) {
	t.Helper()
	pkt := &clientproto.Packet{
		Type: clientproto.PacketType_DATA,
		Payload: &clientproto.Packet_Data{
			Data: &clientproto.Data{
				Data:      []byte(payload),
				ConnectID: 1,
			},
		},
	}
	data, err := proto.Marshal(pkt)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	key := fmt.Sprintf("node-to-control/%s/%020d.pb", nodeID, seq)
	if err := store.Put(context.Background(), key, data); err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
}

// drainChannel reads all available packets from ch without blocking.
func drainChannel(ch <-chan *clientproto.Packet) []*clientproto.Packet {
	var pkts []*clientproto.Packet
	for {
		select {
		case pkt, ok := <-ch:
			if !ok {
				return pkts
			}
			pkts = append(pkts, pkt)
		default:
			return pkts
		}
	}
}

func payloadString(pkt *clientproto.Packet) string {
	return string(pkt.GetData().GetData())
}

func TestRegionalPoller_InOrderDelivery(t *testing.T) {
	store := NewFSStore(t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	poller := NewRegionalPoller(ctx, store, "node-to-control/")
	poller.SetWorkerCount(4) // Multiple workers to exercise parallel download
	ch := poller.RegisterNode("node-1")

	// Write seq 1, 2, 3 in order.
	writePacket(t, store, "node-1", 1, "pkt-1")
	writePacket(t, store, "node-1", 2, "pkt-2")
	writePacket(t, store, "node-1", 3, "pkt-3")

	poller.pollOnce()
	pkts := drainChannel(ch)

	if len(pkts) != 3 {
		t.Fatalf("expected 3 packets, got %d", len(pkts))
	}
	for i, pkt := range pkts {
		expected := fmt.Sprintf("pkt-%d", i+1)
		if got := payloadString(pkt); got != expected {
			t.Errorf("packet %d: expected %q, got %q", i, expected, got)
		}
	}
}

func TestRegionalPoller_GapBlocksDelivery(t *testing.T) {
	store := NewFSStore(t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	poller := NewRegionalPoller(ctx, store, "node-to-control/")
	poller.SetWorkerCount(4)
	ch := poller.RegisterNode("node-1")

	// Write seq 1 and 3, skip seq 2 (simulating delayed write).
	writePacket(t, store, "node-1", 1, "pkt-1")
	writePacket(t, store, "node-1", 3, "pkt-3")

	poller.pollOnce()
	pkts := drainChannel(ch)

	// Only seq 1 should be delivered; seq 3 is held because seq 2 is missing.
	if len(pkts) != 1 {
		t.Fatalf("expected 1 packet (gap at seq 2 should block), got %d", len(pkts))
	}
	if got := payloadString(pkts[0]); got != "pkt-1" {
		t.Errorf("expected pkt-1, got %q", got)
	}

	// Seq 3 should still be in the store (not deleted).
	keys, _ := store.List(ctx, "node-to-control/node-1/")
	if len(keys) != 1 {
		t.Fatalf("expected 1 remaining key (seq 3), got %d: %v", len(keys), keys)
	}
}

func TestRegionalPoller_GapFillResumesDelivery(t *testing.T) {
	store := NewFSStore(t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	poller := NewRegionalPoller(ctx, store, "node-to-control/")
	poller.SetWorkerCount(4)
	ch := poller.RegisterNode("node-1")

	// Write seq 1, 3, 5 — gaps at 2 and 4.
	writePacket(t, store, "node-1", 1, "pkt-1")
	writePacket(t, store, "node-1", 3, "pkt-3")
	writePacket(t, store, "node-1", 5, "pkt-5")

	// Poll 1: should deliver only seq 1.
	poller.pollOnce()
	pkts := drainChannel(ch)
	if len(pkts) != 1 || payloadString(pkts[0]) != "pkt-1" {
		t.Fatalf("poll 1: expected [pkt-1], got %d packets", len(pkts))
	}

	// Now fill the gap at seq 2.
	writePacket(t, store, "node-1", 2, "pkt-2")

	// Poll 2: should deliver seq 2, 3 (contiguous run), then stop at gap before 5.
	poller.pollOnce()
	pkts = drainChannel(ch)
	if len(pkts) != 2 {
		t.Fatalf("poll 2: expected 2 packets [pkt-2, pkt-3], got %d", len(pkts))
	}
	if payloadString(pkts[0]) != "pkt-2" || payloadString(pkts[1]) != "pkt-3" {
		t.Errorf("poll 2: expected [pkt-2, pkt-3], got [%s, %s]",
			payloadString(pkts[0]), payloadString(pkts[1]))
	}

	// Fill remaining gap at seq 4.
	writePacket(t, store, "node-1", 4, "pkt-4")

	// Poll 3: should deliver seq 4, 5.
	poller.pollOnce()
	pkts = drainChannel(ch)
	if len(pkts) != 2 {
		t.Fatalf("poll 3: expected 2 packets [pkt-4, pkt-5], got %d", len(pkts))
	}
	if payloadString(pkts[0]) != "pkt-4" || payloadString(pkts[1]) != "pkt-5" {
		t.Errorf("poll 3: expected [pkt-4, pkt-5], got [%s, %s]",
			payloadString(pkts[0]), payloadString(pkts[1]))
	}

	// Store should be empty now.
	keys, _ := store.List(ctx, "node-to-control/node-1/")
	if len(keys) != 0 {
		t.Errorf("expected empty store, got %d keys: %v", len(keys), keys)
	}
}

func TestRegionalPoller_MultiNodeIndependentGaps(t *testing.T) {
	store := NewFSStore(t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	poller := NewRegionalPoller(ctx, store, "node-to-control/")
	poller.SetWorkerCount(4)
	ch1 := poller.RegisterNode("node-1")
	ch2 := poller.RegisterNode("node-2")

	// node-1: seq 1, 2 (complete)
	writePacket(t, store, "node-1", 1, "n1-pkt-1")
	writePacket(t, store, "node-1", 2, "n1-pkt-2")
	// node-2: seq 1, 3 (gap at 2)
	writePacket(t, store, "node-2", 1, "n2-pkt-1")
	writePacket(t, store, "node-2", 3, "n2-pkt-3")

	poller.pollOnce()

	// node-1 should get both packets.
	pkts1 := drainChannel(ch1)
	if len(pkts1) != 2 {
		t.Fatalf("node-1: expected 2 packets, got %d", len(pkts1))
	}

	// node-2 should get only seq 1 (gap at seq 2 blocks seq 3).
	pkts2 := drainChannel(ch2)
	if len(pkts2) != 1 {
		t.Fatalf("node-2: expected 1 packet, got %d", len(pkts2))
	}
	if payloadString(pkts2[0]) != "n2-pkt-1" {
		t.Errorf("node-2: expected n2-pkt-1, got %q", payloadString(pkts2[0]))
	}

	// node-2's gap should not affect node-1.
	// Verify seq 3 is still in store for node-2.
	keys, _ := store.List(ctx, "node-to-control/node-2/")
	if len(keys) != 1 {
		t.Errorf("node-2: expected 1 remaining key, got %d", len(keys))
	}
}

func TestRegionalPoller_LargeGapDoesNotSkip(t *testing.T) {
	store := NewFSStore(t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	poller := NewRegionalPoller(ctx, store, "node-to-control/")
	poller.SetWorkerCount(4)
	ch := poller.RegisterNode("node-1")

	// Write only seq 5 and 10 (large gaps).
	writePacket(t, store, "node-1", 5, "pkt-5")
	writePacket(t, store, "node-1", 10, "pkt-10")

	poller.pollOnce()
	pkts := drainChannel(ch)

	// Nothing should be delivered because seq 1 is missing.
	if len(pkts) != 0 {
		t.Fatalf("expected 0 packets (gap from seq 1), got %d", len(pkts))
	}

	// Both messages should still be in the store.
	keys, _ := store.List(ctx, "node-to-control/node-1/")
	if len(keys) != 2 {
		t.Errorf("expected 2 remaining keys, got %d", len(keys))
	}
}
