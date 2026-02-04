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

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetricsStorePut(t *testing.T) {
	dir := t.TempDir()
	fs := NewFSStore(dir)
	ms := NewMetricsStore(fs)

	before := testutil.ToFloat64(storeOperationsTotal.WithLabelValues("put", "A", "success"))
	beforeBytes := testutil.ToFloat64(storeBytesSentTotal)

	data := []byte("hello metrics")
	if err := ms.Put(context.Background(), "test/key1", data); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	after := testutil.ToFloat64(storeOperationsTotal.WithLabelValues("put", "A", "success"))
	afterBytes := testutil.ToFloat64(storeBytesSentTotal)

	if got := after - before; got != 1 {
		t.Errorf("expected put counter to increment by 1, got %v", got)
	}
	if got := afterBytes - beforeBytes; got != float64(len(data)) {
		t.Errorf("expected bytes_sent to increment by %d, got %v", len(data), got)
	}
}

func TestMetricsStoreGet(t *testing.T) {
	dir := t.TempDir()
	fs := NewFSStore(dir)
	ms := NewMetricsStore(fs)

	data := []byte("get test data")
	if err := ms.Put(context.Background(), "test/key2", data); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	before := testutil.ToFloat64(storeOperationsTotal.WithLabelValues("get", "B", "success"))
	beforeBytes := testutil.ToFloat64(storeBytesReceivedTotal)

	got, err := ms.Get(context.Background(), "test/key2")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	after := testutil.ToFloat64(storeOperationsTotal.WithLabelValues("get", "B", "success"))
	afterBytes := testutil.ToFloat64(storeBytesReceivedTotal)

	if string(got) != string(data) {
		t.Errorf("expected %q, got %q", data, got)
	}
	if diff := after - before; diff != 1 {
		t.Errorf("expected get counter to increment by 1, got %v", diff)
	}
	if diff := afterBytes - beforeBytes; diff != float64(len(data)) {
		t.Errorf("expected bytes_received to increment by %d, got %v", len(data), diff)
	}
}

func TestMetricsStoreList(t *testing.T) {
	dir := t.TempDir()
	fs := NewFSStore(dir)
	ms := NewMetricsStore(fs)

	for _, key := range []string{"prefix/a", "prefix/b", "prefix/c"} {
		if err := ms.Put(context.Background(), key, []byte("x")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	before := testutil.ToFloat64(storeOperationsTotal.WithLabelValues("list", "A", "success"))
	beforeKeys := testutil.ToFloat64(storeListKeysTotal)

	keys, err := ms.List(context.Background(), "prefix/")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	after := testutil.ToFloat64(storeOperationsTotal.WithLabelValues("list", "A", "success"))
	afterKeys := testutil.ToFloat64(storeListKeysTotal)

	if len(keys) != 3 {
		t.Errorf("expected 3 keys, got %d", len(keys))
	}
	if diff := after - before; diff != 1 {
		t.Errorf("expected list counter to increment by 1, got %v", diff)
	}
	if diff := afterKeys - beforeKeys; diff != 3 {
		t.Errorf("expected list_keys to increment by 3, got %v", diff)
	}
}

func TestMetricsStoreDelete(t *testing.T) {
	dir := t.TempDir()
	fs := NewFSStore(dir)
	ms := NewMetricsStore(fs)

	if err := ms.Put(context.Background(), "test/delme", []byte("bye")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	before := testutil.ToFloat64(storeOperationsTotal.WithLabelValues("delete", "free", "success"))

	if err := ms.Delete(context.Background(), "test/delme"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	after := testutil.ToFloat64(storeOperationsTotal.WithLabelValues("delete", "free", "success"))
	if diff := after - before; diff != 1 {
		t.Errorf("expected delete counter to increment by 1, got %v", diff)
	}
}

func TestMetricsStoreErrorStatus(t *testing.T) {
	dir := t.TempDir()
	fs := NewFSStore(dir)
	ms := NewMetricsStore(fs)

	before := testutil.ToFloat64(storeOperationsTotal.WithLabelValues("get", "B", "error"))

	_, err := ms.Get(context.Background(), "nonexistent/key")
	if err == nil {
		t.Fatal("expected error for nonexistent key")
	}

	after := testutil.ToFloat64(storeOperationsTotal.WithLabelValues("get", "B", "error"))
	if diff := after - before; diff != 1 {
		t.Errorf("expected error counter to increment by 1, got %v", diff)
	}
}

func TestMetricsStoreDurationRecorded(t *testing.T) {
	dir := t.TempDir()
	fs := NewFSStore(dir)
	ms := NewMetricsStore(fs)

	if err := ms.Put(context.Background(), "test/dur", []byte("data")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Verify the histogram has at least one observation by collecting metrics.
	count := testutil.CollectAndCount(storeOperationDurationSeconds)
	if count == 0 {
		t.Error("expected histogram to have observations, got 0 metrics")
	}
}

func TestMetricsStoreListRecursive(t *testing.T) {
	dir := t.TempDir()
	fs := NewFSStore(dir)
	ms := NewMetricsStore(fs)

	// Create a nested structure:
	// prefix/node1/001.pb
	// prefix/node1/002.pb
	// prefix/node2/001.pb
	testKeys := []string{
		"prefix/node1/001.pb",
		"prefix/node1/002.pb",
		"prefix/node2/001.pb",
	}
	for _, key := range testKeys {
		if err := ms.Put(context.Background(), key, []byte("x")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	before := testutil.ToFloat64(storeOperationsTotal.WithLabelValues("list_recursive", "A", "success"))
	beforeKeys := testutil.ToFloat64(storeListKeysTotal)

	keys, err := ms.ListRecursive(context.Background(), "prefix/")
	if err != nil {
		t.Fatalf("ListRecursive failed: %v", err)
	}

	after := testutil.ToFloat64(storeOperationsTotal.WithLabelValues("list_recursive", "A", "success"))
	afterKeys := testutil.ToFloat64(storeListKeysTotal)

	// Should return all 3 files, not just directories
	if len(keys) != 3 {
		t.Errorf("expected 3 keys, got %d: %v", len(keys), keys)
	}
	if diff := after - before; diff != 1 {
		t.Errorf("expected list_recursive counter to increment by 1, got %v", diff)
	}
	if diff := afterKeys - beforeKeys; diff != 3 {
		t.Errorf("expected list_keys to increment by 3, got %v", diff)
	}

	// Verify the keys are the full paths
	expected := map[string]bool{
		"prefix/node1/001.pb": true,
		"prefix/node1/002.pb": true,
		"prefix/node2/001.pb": true,
	}
	for _, k := range keys {
		if !expected[k] {
			t.Errorf("unexpected key %q", k)
		}
	}
}

func TestFSStoreListVsListRecursive(t *testing.T) {
	dir := t.TempDir()
	fs := NewFSStore(dir)
	ctx := context.Background()

	// Create nested structure
	testKeys := []string{
		"node-to-control/node1/001.pb",
		"node-to-control/node1/002.pb",
		"node-to-control/node2/001.pb",
	}
	for _, key := range testKeys {
		if err := fs.Put(ctx, key, []byte("x")); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// List (one level) should return directories
	list, err := fs.List(ctx, "node-to-control/")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(list) != 2 {
		t.Errorf("List: expected 2 items (node1/, node2/), got %d: %v", len(list), list)
	}
	// Check they end with /
	for _, k := range list {
		if k[len(k)-1] != '/' {
			t.Errorf("List item %q should end with /", k)
		}
	}

	// ListRecursive should return all files
	listRec, err := fs.ListRecursive(ctx, "node-to-control/")
	if err != nil {
		t.Fatalf("ListRecursive failed: %v", err)
	}
	if len(listRec) != 3 {
		t.Errorf("ListRecursive: expected 3 items, got %d: %v", len(listRec), listRec)
	}
	// Check they are full file paths (not ending with /)
	for _, k := range listRec {
		if k[len(k)-1] == '/' {
			t.Errorf("ListRecursive item %q should not end with /", k)
		}
	}
}
