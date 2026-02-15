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

import "context"

// Store abstracts object storage operations (S3, GCS, Azure Blob, or filesystem).
// Keys are slash-separated paths (e.g. "control-to-node/node-1/0000000001.pb").
type Store interface {
	// Put writes data to the given key, creating parent directories as needed.
	Put(ctx context.Context, key string, data []byte) error

	// Get reads the data stored at the given key.
	Get(ctx context.Context, key string) ([]byte, error)

	// List returns immediate children under the given prefix (one level only).
	// For GCS, this uses Delimiter="/" to return only direct children.
	List(ctx context.Context, prefix string) ([]string, error)

	// ListRecursive returns all keys with the given prefix, recursively.
	// Unlike List, this returns the full path of every object, not just immediate children.
	ListRecursive(ctx context.Context, prefix string) ([]string, error)

	// Delete removes the object at the given key.
	Delete(ctx context.Context, key string) error

	// Close releases any resources held by the store (e.g., network connections).
	Close() error
}
