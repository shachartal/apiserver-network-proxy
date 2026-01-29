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
	"time"

	"k8s.io/klog/v2"
)

// RetryPolicy configures retry behavior for store operations.
type RetryPolicy struct {
	// MaxRetries is the maximum number of retry attempts (0 means no retries).
	MaxRetries int
	// InitialBackoff is the delay before the first retry.
	InitialBackoff time.Duration
	// MaxBackoff caps the exponential backoff.
	MaxBackoff time.Duration
	// BackoffMultiplier is the factor by which the backoff grows each retry.
	BackoffMultiplier float64
}

// DefaultRetryPolicy returns a reasonable retry policy for cloud bucket operations.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxRetries:        3,
		InitialBackoff:    100 * time.Millisecond,
		MaxBackoff:        5 * time.Second,
		BackoffMultiplier: 2.0,
	}
}

// RetryStore wraps a Store with retry logic and exponential backoff.
type RetryStore struct {
	inner  Store
	policy RetryPolicy
}

var _ Store = (*RetryStore)(nil)

// NewRetryStore wraps the given store with retry logic.
func NewRetryStore(inner Store, policy RetryPolicy) *RetryStore {
	return &RetryStore{inner: inner, policy: policy}
}

func (s *RetryStore) Put(ctx context.Context, key string, data []byte) error {
	return s.retry(ctx, "Put", key, func() error {
		return s.inner.Put(ctx, key, data)
	})
}

func (s *RetryStore) Get(ctx context.Context, key string) ([]byte, error) {
	var result []byte
	err := s.retry(ctx, "Get", key, func() error {
		var err error
		result, err = s.inner.Get(ctx, key)
		return err
	})
	return result, err
}

func (s *RetryStore) List(ctx context.Context, prefix string) ([]string, error) {
	var result []string
	err := s.retry(ctx, "List", prefix, func() error {
		var err error
		result, err = s.inner.List(ctx, prefix)
		return err
	})
	return result, err
}

func (s *RetryStore) Delete(ctx context.Context, key string) error {
	return s.retry(ctx, "Delete", key, func() error {
		return s.inner.Delete(ctx, key)
	})
}

func (s *RetryStore) retry(ctx context.Context, op, key string, fn func() error) error {
	var lastErr error
	backoff := s.policy.InitialBackoff

	for attempt := 0; attempt <= s.policy.MaxRetries; attempt++ {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		if attempt < s.policy.MaxRetries {
			klog.V(4).InfoS("Store operation failed, retrying",
				"op", op, "key", key, "attempt", attempt+1,
				"maxRetries", s.policy.MaxRetries, "backoff", backoff, "err", lastErr)

			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}

			backoff = time.Duration(float64(backoff) * s.policy.BackoffMultiplier)
			if backoff > s.policy.MaxBackoff {
				backoff = s.policy.MaxBackoff
			}
		}
	}

	return fmt.Errorf("%s %q failed after %d attempts: %w", op, key, s.policy.MaxRetries+1, lastErr)
}
