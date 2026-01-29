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
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// FSStore implements Store using the local filesystem.
// Keys are mapped to file paths under a root directory.
type FSStore struct {
	root string
}

// NewFSStore creates a filesystem-backed Store rooted at the given directory.
func NewFSStore(root string) *FSStore {
	return &FSStore{root: root}
}

func (s *FSStore) Put(_ context.Context, key string, data []byte) error {
	path := filepath.Join(s.root, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir for key %q: %w", key, err)
	}
	return os.WriteFile(path, data, 0o644)
}

func (s *FSStore) Get(_ context.Context, key string) ([]byte, error) {
	path := filepath.Join(s.root, filepath.FromSlash(key))
	return os.ReadFile(path)
}

func (s *FSStore) List(_ context.Context, prefix string) ([]string, error) {
	dir := filepath.Join(s.root, filepath.FromSlash(prefix))

	info, err := os.Stat(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("prefix %q is not a directory", prefix)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var keys []string
	for _, e := range entries {
		// Return keys with the prefix included, using forward slashes.
		key := prefix + e.Name()
		if e.IsDir() {
			key += "/"
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys, nil
}

func (s *FSStore) Delete(_ context.Context, key string) error {
	path := filepath.Join(s.root, filepath.FromSlash(key))
	err := os.Remove(path)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

var _ Store = (*FSStore)(nil)

// Ensure prefix ends with "/" for directory semantics.
func ensureTrailingSlash(s string) string {
	if !strings.HasSuffix(s, "/") {
		return s + "/"
	}
	return s
}
