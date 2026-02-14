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

// writePacket writes a protobuf packet to the store at the given stream and
// sequence number under the node-to-control/{nodeID}/ prefix.
// Key format: {streamID}-{seqID}.pb
func writePacket(t *testing.T, store Store, nodeID string, streamID int64, seq uint64, payload string) {
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
	key := fmt.Sprintf("node-to-control/%s/%d-%020d.pb", nodeID, streamID, seq)
	if err := store.Put(context.Background(), key, data); err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
}

// writeClosePacket writes a CLOSE_RSP packet for stream state cleanup testing.
func writeClosePacket(t *testing.T, store Store, nodeID string, streamID int64, seq uint64, connID int64) {
	t.Helper()
	pkt := &clientproto.Packet{
		Type: clientproto.PacketType_CLOSE_RSP,
		Payload: &clientproto.Packet_CloseResponse{
			CloseResponse: &clientproto.CloseResponse{
				ConnectID: connID,
			},
		},
	}
	data, err := proto.Marshal(pkt)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	key := fmt.Sprintf("node-to-control/%s/%d-%020d.pb", nodeID, streamID, seq)
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

	// Write seq 1, 2, 3 on stream 1.
	writePacket(t, store, "node-1", 1, 1, "pkt-1")
	writePacket(t, store, "node-1", 1, 2, "pkt-2")
	writePacket(t, store, "node-1", 1, 3, "pkt-3")

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

	// Write seq 1 and 3 on stream 1, skip seq 2 (simulating delayed write).
	writePacket(t, store, "node-1", 1, 1, "pkt-1")
	writePacket(t, store, "node-1", 1, 3, "pkt-3")

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

	// Write seq 1, 3, 5 on stream 1 — gaps at 2 and 4.
	writePacket(t, store, "node-1", 1, 1, "pkt-1")
	writePacket(t, store, "node-1", 1, 3, "pkt-3")
	writePacket(t, store, "node-1", 1, 5, "pkt-5")

	// Poll 1: should deliver only seq 1.
	poller.pollOnce()
	pkts := drainChannel(ch)
	if len(pkts) != 1 || payloadString(pkts[0]) != "pkt-1" {
		t.Fatalf("poll 1: expected [pkt-1], got %d packets", len(pkts))
	}

	// Now fill the gap at seq 2.
	writePacket(t, store, "node-1", 1, 2, "pkt-2")

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
	writePacket(t, store, "node-1", 1, 4, "pkt-4")

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

	// node-1: stream 1 seq 1, 2 (complete)
	writePacket(t, store, "node-1", 1, 1, "n1-pkt-1")
	writePacket(t, store, "node-1", 1, 2, "n1-pkt-2")
	// node-2: stream 1 seq 1, 3 (gap at 2)
	writePacket(t, store, "node-2", 1, 1, "n2-pkt-1")
	writePacket(t, store, "node-2", 1, 3, "n2-pkt-3")

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

	// Write only seq 5 and 10 on stream 1 (large gaps).
	// Since expectedSeq=1 and the first file has seq=5, the poller treats
	// these as orphaned files (from a stream whose state was already cleaned
	// up) and deletes them.
	writePacket(t, store, "node-1", 1, 5, "pkt-5")
	writePacket(t, store, "node-1", 1, 10, "pkt-10")

	poller.pollOnce()
	pkts := drainChannel(ch)

	// Nothing should be delivered.
	if len(pkts) != 0 {
		t.Fatalf("expected 0 packets, got %d", len(pkts))
	}

	// Orphaned files should be deleted (not preserved forever).
	keys, _ := store.List(ctx, "node-to-control/node-1/")
	if len(keys) != 0 {
		t.Errorf("expected 0 remaining keys (orphans deleted), got %d: %v", len(keys), keys)
	}
}

func TestRegionalPoller_PerStreamGapIsolation(t *testing.T) {
	store := NewFSStore(t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	poller := NewRegionalPoller(ctx, store, "node-to-control/")
	poller.SetWorkerCount(4)
	ch := poller.RegisterNode("node-1")

	// Stream A (streamID=100): seq 1, 3 — gap at seq 2
	writePacket(t, store, "node-1", 100, 1, "A-pkt-1")
	writePacket(t, store, "node-1", 100, 3, "A-pkt-3")
	// Stream B (streamID=200): seq 1, 2 — complete
	writePacket(t, store, "node-1", 200, 1, "B-pkt-1")
	writePacket(t, store, "node-1", 200, 2, "B-pkt-2")

	poller.pollOnce()
	pkts := drainChannel(ch)

	// Stream A: only seq 1 delivered (gap at 2 blocks 3).
	// Stream B: both seq 1 and 2 delivered (no gap).
	// Total: 3 packets.
	if len(pkts) != 3 {
		t.Fatalf("expected 3 packets (1 from stream A, 2 from stream B), got %d", len(pkts))
	}

	// Verify payloads: order between streams is not guaranteed, but within a stream it is.
	payloads := make(map[string]bool)
	for _, pkt := range pkts {
		payloads[payloadString(pkt)] = true
	}
	for _, expected := range []string{"A-pkt-1", "B-pkt-1", "B-pkt-2"} {
		if !payloads[expected] {
			t.Errorf("expected payload %q not found in delivered packets", expected)
		}
	}
	if payloads["A-pkt-3"] {
		t.Error("A-pkt-3 should NOT have been delivered (gap at seq 2)")
	}

	// Stream A's seq 3 should still be in store.
	keys, _ := store.List(ctx, "node-to-control/node-1/")
	if len(keys) != 1 {
		t.Errorf("expected 1 remaining key (stream A seq 3), got %d: %v", len(keys), keys)
	}
}

func TestRegionalPoller_StreamCleanupAfterClose(t *testing.T) {
	store := NewFSStore(t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	poller := NewRegionalPoller(ctx, store, "node-to-control/")
	poller.SetWorkerCount(4)
	ch := poller.RegisterNode("node-1")

	// Stream 100: DATA seq 1, then CLOSE_RSP seq 2.
	writePacket(t, store, "node-1", 100, 1, "data")
	writeClosePacket(t, store, "node-1", 100, 2, 1)

	poller.pollOnce()
	pkts := drainChannel(ch)

	if len(pkts) != 2 {
		t.Fatalf("expected 2 packets, got %d", len(pkts))
	}
	if pkts[0].Type != clientproto.PacketType_DATA {
		t.Errorf("expected first packet DATA, got %v", pkts[0].Type)
	}
	if pkts[1].Type != clientproto.PacketType_CLOSE_RSP {
		t.Errorf("expected second packet CLOSE_RSP, got %v", pkts[1].Type)
	}

	// Verify stream state was cleaned up.
	poller.mu.RLock()
	h := poller.handlers["node-1"]
	_, hasRecvSeq := h.recvSeqs[100]
	_, hasActivity := h.lastActivity[100]
	poller.mu.RUnlock()

	if hasRecvSeq {
		t.Error("recvSeqs entry for stream 100 should be deleted after CLOSE_RSP")
	}
	if hasActivity {
		t.Error("lastActivity entry for stream 100 should be deleted after CLOSE_RSP")
	}

	// Now write new data on the SAME stream ID (simulating reuse).
	// Seq should start fresh at 1 again since state was cleaned up.
	writePacket(t, store, "node-1", 100, 1, "reused-stream")
	poller.pollOnce()
	pkts = drainChannel(ch)
	if len(pkts) != 1 {
		t.Fatalf("expected 1 packet after stream reuse, got %d", len(pkts))
	}
	if payloadString(pkts[0]) != "reused-stream" {
		t.Errorf("expected 'reused-stream', got %q", payloadString(pkts[0]))
	}
}

func TestRegionalPoller_OrphanedStreamCleanup(t *testing.T) {
	store := NewFSStore(t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	poller := NewRegionalPoller(ctx, store, "node-to-control/")
	poller.SetWorkerCount(4)
	_ = poller.RegisterNode("node-1")

	// Simulate orphaned files: streamID=0 at seq 3 and 5 (no seq 1 or 2).
	// This happens when streamID=0 error responses pile up after a stream
	// was already closed and its recvSeqs entry deleted.
	writePacket(t, store, "node-1", 0, 3, "orphan-3")
	writePacket(t, store, "node-1", 0, 5, "orphan-5")

	poller.pollOnce()

	// The poller should detect that expectedSeq=1 but got seq=3, and delete
	// the orphaned files instead of deferring them forever.
	keys, _ := store.List(ctx, "node-to-control/node-1/")
	if len(keys) != 0 {
		t.Errorf("expected orphaned files to be deleted, got %d remaining: %v", len(keys), keys)
	}
}

func TestRegionalPoller_OrphanedStreamDoesNotAffectActiveStreams(t *testing.T) {
	store := NewFSStore(t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	poller := NewRegionalPoller(ctx, store, "node-to-control/")
	poller.SetWorkerCount(4)
	ch := poller.RegisterNode("node-1")

	// Active stream 100: seq 1, 2 (complete)
	writePacket(t, store, "node-1", 100, 1, "active-1")
	writePacket(t, store, "node-1", 100, 2, "active-2")
	// Orphaned stream 0: seq 3 only (gap at 1,2 — permanently orphaned)
	writePacket(t, store, "node-1", 0, 3, "orphan-3")

	poller.pollOnce()

	// Active stream should be fully delivered.
	pkts := drainChannel(ch)
	if len(pkts) != 2 {
		t.Fatalf("expected 2 packets from active stream, got %d", len(pkts))
	}
	if payloadString(pkts[0]) != "active-1" || payloadString(pkts[1]) != "active-2" {
		t.Errorf("unexpected payloads: [%s, %s]", payloadString(pkts[0]), payloadString(pkts[1]))
	}

	// Orphaned files should be deleted.
	keys, _ := store.List(ctx, "node-to-control/node-1/")
	if len(keys) != 0 {
		t.Errorf("expected all files deleted (active delivered + orphans cleaned), got %d: %v", len(keys), keys)
	}
}
