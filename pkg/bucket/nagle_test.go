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
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	client "sigs.k8s.io/apiserver-network-proxy/konnectivity-client/proto/client"
)

// collectSentPackets reads all packets written to the store under sendPrefix.
func collectSentPackets(t *testing.T, store *FSStore, prefix string) []*client.Packet {
	t.Helper()
	keys, err := store.List(context.Background(), prefix)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	var pkts []*client.Packet
	for _, key := range keys {
		data, err := store.Get(context.Background(), key)
		if err != nil {
			t.Fatalf("Get %s: %v", key, err)
		}
		pkt := &client.Packet{}
		if err := proto.Unmarshal(data, pkt); err != nil {
			t.Fatalf("Unmarshal %s: %v", key, err)
		}
		pkts = append(pkts, pkt)
	}
	return pkts
}

func makeDataPacket(connectID int64, payload []byte) *client.Packet {
	return &client.Packet{
		Type: client.PacketType_DATA,
		Payload: &client.Packet_Data{
			Data: &client.Data{
				ConnectID: connectID,
				Data:      payload,
			},
		},
	}
}

func makeClosePacket(connectID int64) *client.Packet {
	return &client.Packet{
		Type: client.PacketType_CLOSE_RSP,
		Payload: &client.Packet_CloseResponse{
			CloseResponse: &client.CloseResponse{
				ConnectID: connectID,
			},
		},
	}
}

func TestNagle_CoalescesSmallDataPackets(t *testing.T) {
	dir := t.TempDir()
	store := NewFSStore(dir)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Use a long Nagle delay so packets stay buffered.
	transport := newSendOnlyTransport(ctx, store, "test/send/", 5*time.Second)
	defer transport.Close()

	// Send three small DATA packets for the same connectID.
	for i := 0; i < 3; i++ {
		if err := transport.Send(makeDataPacket(42, []byte("abc"))); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}

	// Nothing should be in the store yet (buffered by Nagle).
	pkts := collectSentPackets(t, store, "test/send/")
	if len(pkts) != 0 {
		t.Fatalf("Expected 0 packets in store (buffered), got %d", len(pkts))
	}

	// Flush by closing.
	transport.Close()

	// Now the coalesced packet should be in the store.
	pkts = collectSentPackets(t, store, "test/send/")
	if len(pkts) != 1 {
		t.Fatalf("Expected 1 coalesced packet, got %d", len(pkts))
	}
	data := pkts[0].GetData()
	if data == nil {
		t.Fatal("Expected DATA packet")
	}
	if data.ConnectID != 42 {
		t.Errorf("Expected connectID 42, got %d", data.ConnectID)
	}
	if string(data.Data) != "abcabcabc" {
		t.Errorf("Expected coalesced payload 'abcabcabc', got %q", string(data.Data))
	}
}

func TestNagle_NonDataFlushesBuffer(t *testing.T) {
	dir := t.TempDir()
	store := NewFSStore(dir)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	transport := newSendOnlyTransport(ctx, store, "test/send/", 5*time.Second)
	defer transport.Close()

	// Buffer some DATA.
	if err := transport.Send(makeDataPacket(10, []byte("hello"))); err != nil {
		t.Fatalf("Send DATA: %v", err)
	}

	// Sending a non-DATA packet should flush buffered DATA first, then send itself.
	if err := transport.Send(makeClosePacket(10)); err != nil {
		t.Fatalf("Send CLOSE: %v", err)
	}

	pkts := collectSentPackets(t, store, "test/send/")
	if len(pkts) != 2 {
		t.Fatalf("Expected 2 packets (DATA + CLOSE), got %d", len(pkts))
	}

	// First packet should be the flushed DATA.
	if pkts[0].Type != client.PacketType_DATA {
		t.Errorf("Expected first packet to be DATA, got %v", pkts[0].Type)
	}
	if string(pkts[0].GetData().Data) != "hello" {
		t.Errorf("Expected DATA payload 'hello', got %q", string(pkts[0].GetData().Data))
	}

	// Second packet should be CLOSE_RSP.
	if pkts[1].Type != client.PacketType_CLOSE_RSP {
		t.Errorf("Expected second packet to be CLOSE_RSP, got %v", pkts[1].Type)
	}
}

func TestNagle_TimerFlush(t *testing.T) {
	dir := t.TempDir()
	store := NewFSStore(dir)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Use a short Nagle delay.
	transport := newSendOnlyTransport(ctx, store, "test/send/", 50*time.Millisecond)
	defer transport.Close()

	if err := transport.Send(makeDataPacket(7, []byte("timer-test"))); err != nil {
		t.Fatalf("Send: %v", err)
	}

	// Immediately after send, nothing in store.
	pkts := collectSentPackets(t, store, "test/send/")
	if len(pkts) != 0 {
		t.Fatalf("Expected 0 packets immediately, got %d", len(pkts))
	}

	// Wait for the Nagle timer to fire.
	time.Sleep(150 * time.Millisecond)

	pkts = collectSentPackets(t, store, "test/send/")
	if len(pkts) != 1 {
		t.Fatalf("Expected 1 packet after timer, got %d", len(pkts))
	}
	if string(pkts[0].GetData().Data) != "timer-test" {
		t.Errorf("Expected 'timer-test', got %q", string(pkts[0].GetData().Data))
	}
}

func TestNagle_SizeThresholdFlush(t *testing.T) {
	dir := t.TempDir()
	store := NewFSStore(dir)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Use a long delay so only size triggers flush.
	transport := newSendOnlyTransport(ctx, store, "test/send/", 10*time.Second)
	defer transport.Close()

	// Send a payload larger than nagleMaxBytes (32KB).
	bigPayload := make([]byte, nagleMaxBytes+1)
	for i := range bigPayload {
		bigPayload[i] = 'X'
	}
	if err := transport.Send(makeDataPacket(99, bigPayload)); err != nil {
		t.Fatalf("Send: %v", err)
	}

	// Should be flushed immediately due to size.
	pkts := collectSentPackets(t, store, "test/send/")
	if len(pkts) != 1 {
		t.Fatalf("Expected 1 packet (size flush), got %d", len(pkts))
	}
	if int64(len(pkts[0].GetData().Data)) != int64(nagleMaxBytes+1) {
		t.Errorf("Expected payload size %d, got %d", nagleMaxBytes+1, len(pkts[0].GetData().Data))
	}
}

func TestNagle_PerConnectIDIsolation(t *testing.T) {
	dir := t.TempDir()
	store := NewFSStore(dir)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	transport := newSendOnlyTransport(ctx, store, "test/send/", 5*time.Second)
	defer transport.Close()

	// Buffer data for two different connectIDs.
	if err := transport.Send(makeDataPacket(1, []byte("conn1-a"))); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := transport.Send(makeDataPacket(2, []byte("conn2-a"))); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := transport.Send(makeDataPacket(1, []byte("conn1-b"))); err != nil {
		t.Fatalf("Send: %v", err)
	}

	// Nothing in store yet.
	pkts := collectSentPackets(t, store, "test/send/")
	if len(pkts) != 0 {
		t.Fatalf("Expected 0 packets, got %d", len(pkts))
	}

	// Flush via Close.
	transport.Close()

	pkts = collectSentPackets(t, store, "test/send/")
	if len(pkts) != 2 {
		t.Fatalf("Expected 2 packets (one per connectID), got %d", len(pkts))
	}

	// Verify payloads are coalesced per connectID.
	byConnID := make(map[int64]string)
	for _, pkt := range pkts {
		d := pkt.GetData()
		byConnID[d.ConnectID] = string(d.Data)
	}
	if byConnID[1] != "conn1-aconn1-b" {
		t.Errorf("ConnectID 1: expected 'conn1-aconn1-b', got %q", byConnID[1])
	}
	if byConnID[2] != "conn2-a" {
		t.Errorf("ConnectID 2: expected 'conn2-a', got %q", byConnID[2])
	}
}

func TestNagle_DisabledSendsImmediately(t *testing.T) {
	dir := t.TempDir()
	store := NewFSStore(dir)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// nagleDelay=0 means disabled.
	transport := newSendOnlyTransport(ctx, store, "test/send/", 0)
	defer transport.Close()

	if err := transport.Send(makeDataPacket(1, []byte("immediate"))); err != nil {
		t.Fatalf("Send: %v", err)
	}

	// Should be in store immediately.
	pkts := collectSentPackets(t, store, "test/send/")
	if len(pkts) != 1 {
		t.Fatalf("Expected 1 packet immediately (Nagle disabled), got %d", len(pkts))
	}
	if string(pkts[0].GetData().Data) != "immediate" {
		t.Errorf("Expected 'immediate', got %q", string(pkts[0].GetData().Data))
	}
}
