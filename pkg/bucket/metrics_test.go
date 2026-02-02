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
