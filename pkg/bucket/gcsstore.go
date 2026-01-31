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
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// GCSStore implements Store using Google Cloud Storage.
// Keys are mapped to GCS object names under an optional prefix within a bucket.
type GCSStore struct {
	client *storage.Client
	bucket string
	prefix string
}

var _ Store = (*GCSStore)(nil)

// NewGCSStore creates a GCS-backed Store.
// prefix is prepended to all keys (e.g. "dev/" makes key "foo" map to object "dev/foo").
func NewGCSStore(ctx context.Context, bucketName, prefix string, opts ...option.ClientOption) (*GCSStore, error) {
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating GCS client: %w", err)
	}

	// Validate bucket exists.
	if _, err := client.Bucket(bucketName).Attrs(ctx); err != nil {
		client.Close()
		return nil, fmt.Errorf("accessing GCS bucket %q: %w", bucketName, err)
	}

	return &GCSStore{
		client: client,
		bucket: bucketName,
		prefix: prefix,
	}, nil
}

func (s *GCSStore) objectName(key string) string {
	return s.prefix + key
}

func (s *GCSStore) Put(ctx context.Context, key string, data []byte) error {
	obj := s.client.Bucket(s.bucket).Object(s.objectName(key))
	w := obj.NewWriter(ctx)
	if _, err := w.Write(data); err != nil {
		w.Close()
		return fmt.Errorf("writing GCS object %q: %w", s.objectName(key), err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("closing GCS writer for %q: %w", s.objectName(key), err)
	}
	return nil
}

func (s *GCSStore) Get(ctx context.Context, key string) ([]byte, error) {
	obj := s.client.Bucket(s.bucket).Object(s.objectName(key))
	r, err := obj.NewReader(ctx)
	if err != nil {
		return nil, fmt.Errorf("reading GCS object %q: %w", s.objectName(key), err)
	}
	defer r.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("reading GCS object body %q: %w", s.objectName(key), err)
	}
	return data, nil
}

func (s *GCSStore) List(ctx context.Context, prefix string) ([]string, error) {
	gcsPrefix := s.objectName(prefix)
	query := &storage.Query{
		Prefix:    gcsPrefix,
		Delimiter: "/",
	}

	var keys []string
	it := s.client.Bucket(s.bucket).Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("listing GCS objects with prefix %q: %w", gcsPrefix, err)
		}

		// attrs.Prefix is set for "directory" entries (when Delimiter is used).
		// attrs.Name is set for actual objects.
		var gcsKey string
		if attrs.Prefix != "" {
			gcsKey = attrs.Prefix
		} else {
			gcsKey = attrs.Name
		}

		// Strip the store prefix to return keys relative to it.
		key := strings.TrimPrefix(gcsKey, s.prefix)
		keys = append(keys, key)
	}

	sort.Strings(keys)
	return keys, nil
}

func (s *GCSStore) Delete(ctx context.Context, key string) error {
	obj := s.client.Bucket(s.bucket).Object(s.objectName(key))
	err := obj.Delete(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("deleting GCS object %q: %w", s.objectName(key), err)
	}
	return nil
}

// Close closes the underlying GCS client.
func (s *GCSStore) Close() error {
	return s.client.Close()
}
