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
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	metricsNamespace = "konnectivity_network_proxy"
	metricsSubsystem = "bucket"
)

var (
	storeOperationsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Subsystem: metricsSubsystem,
			Name:      "store_operations_total",
			Help:      "Total number of bucket store operations, labeled by operation, GCS pricing class, and status.",
		},
		[]string{"operation", "gcs_class", "status"},
	)

	storeOperationDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: metricsNamespace,
			Subsystem: metricsSubsystem,
			Name:      "store_operation_duration_seconds",
			Help:      "Duration of bucket store operations in seconds.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"operation"},
	)

	storeBytesSentTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Subsystem: metricsSubsystem,
			Name:      "store_bytes_sent_total",
			Help:      "Total bytes written to the bucket store via Put operations.",
		},
	)

	storeBytesReceivedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Subsystem: metricsSubsystem,
			Name:      "store_bytes_received_total",
			Help:      "Total bytes read from the bucket store via Get operations.",
		},
	)

	storeListKeysTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: metricsNamespace,
			Subsystem: metricsSubsystem,
			Name:      "store_list_keys_total",
			Help:      "Total number of keys returned by List operations.",
		},
	)
)

func init() {
	prometheus.MustRegister(storeOperationsTotal)
	prometheus.MustRegister(storeOperationDurationSeconds)
	prometheus.MustRegister(storeBytesSentTotal)
	prometheus.MustRegister(storeBytesReceivedTotal)
	prometheus.MustRegister(storeListKeysTotal)
}

// MetricsStore wraps a Store with Prometheus metrics instrumentation.
// Each operation is counted, timed, and classified by GCS pricing tier.
type MetricsStore struct {
	inner Store
}

var _ Store = (*MetricsStore)(nil)

// NewMetricsStore wraps the given store with Prometheus metrics collection.
func NewMetricsStore(inner Store) *MetricsStore {
	return &MetricsStore{inner: inner}
}

func (s *MetricsStore) Put(ctx context.Context, key string, data []byte) error {
	start := time.Now()
	err := s.inner.Put(ctx, key, data)
	duration := time.Since(start)

	status := "success"
	if err != nil {
		status = "error"
	}

	storeOperationsTotal.WithLabelValues("put", "A", status).Inc()
	storeOperationDurationSeconds.WithLabelValues("put").Observe(duration.Seconds())
	if err == nil {
		storeBytesSentTotal.Add(float64(len(data)))
	}
	return err
}

func (s *MetricsStore) Get(ctx context.Context, key string) ([]byte, error) {
	start := time.Now()
	data, err := s.inner.Get(ctx, key)
	duration := time.Since(start)

	status := "success"
	if err != nil {
		status = "error"
	}

	storeOperationsTotal.WithLabelValues("get", "B", status).Inc()
	storeOperationDurationSeconds.WithLabelValues("get").Observe(duration.Seconds())
	if err == nil {
		storeBytesReceivedTotal.Add(float64(len(data)))
	}
	return data, err
}

func (s *MetricsStore) List(ctx context.Context, prefix string) ([]string, error) {
	start := time.Now()
	keys, err := s.inner.List(ctx, prefix)
	duration := time.Since(start)

	status := "success"
	if err != nil {
		status = "error"
	}

	storeOperationsTotal.WithLabelValues("list", "A", status).Inc()
	storeOperationDurationSeconds.WithLabelValues("list").Observe(duration.Seconds())
	if err == nil {
		storeListKeysTotal.Add(float64(len(keys)))
	}
	return keys, err
}

func (s *MetricsStore) Delete(ctx context.Context, key string) error {
	start := time.Now()
	err := s.inner.Delete(ctx, key)
	duration := time.Since(start)

	status := "success"
	if err != nil {
		status = "error"
	}

	storeOperationsTotal.WithLabelValues("delete", "free", status).Inc()
	storeOperationDurationSeconds.WithLabelValues("delete").Observe(duration.Seconds())
	return err
}
