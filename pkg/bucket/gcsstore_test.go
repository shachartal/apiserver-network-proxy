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
	"os"
	"testing"

	"google.golang.org/api/option"
)

// Compile-time check that GCSStore satisfies the Store interface.
var _ Store = (*GCSStore)(nil)

func TestGCSStore_ObjectName(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		key    string
		want   string
	}{
		{
			name:   "no prefix",
			prefix: "",
			key:    "control-to-node/n1/001.pb",
			want:   "control-to-node/n1/001.pb",
		},
		{
			name:   "with prefix",
			prefix: "dev/",
			key:    "control-to-node/n1/001.pb",
			want:   "dev/control-to-node/n1/001.pb",
		},
		{
			name:   "prefix without trailing slash",
			prefix: "staging",
			key:    "node-to-control/n2/002.pb",
			want:   "stagingnode-to-control/n2/002.pb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &GCSStore{prefix: tt.prefix}
			if got := s.objectName(tt.key); got != tt.want {
				t.Errorf("objectName(%q) = %q, want %q", tt.key, got, tt.want)
			}
		})
	}
}

// Integration tests that require a real GCS bucket.
// Set BUCKET_TEST_GCS_BUCKET to run these tests.
func skipUnlessGCSBucket(t *testing.T) string {
	t.Helper()
	bucket := os.Getenv("BUCKET_TEST_GCS_BUCKET")
	if bucket == "" {
		t.Skip("BUCKET_TEST_GCS_BUCKET not set, skipping GCS integration test")
	}
	return bucket
}

func TestGCSStore_Integration_PutGetDelete(t *testing.T) {
	bucketName := skipUnlessGCSBucket(t)
	ctx := context.Background()

	var opts []option.ClientOption
	if creds := os.Getenv("BUCKET_TEST_GCS_CREDENTIALS"); creds != "" {
		opts = append(opts, option.WithCredentialsFile(creds))
	}

	store, err := NewGCSStore(ctx, bucketName, "test-integration/", opts...)
	if err != nil {
		t.Fatalf("NewGCSStore: %v", err)
	}
	defer store.Close()

	key := "control-to-node/n1/001.pb"
	data := []byte("hello gcs")

	// Put
	if err := store.Put(ctx, key, data); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Get
	got, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("Get = %q, want %q", got, data)
	}

	// Delete
	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Delete again (idempotent)
	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("Delete (idempotent): %v", err)
	}

	// Get after delete should fail
	if _, err := store.Get(ctx, key); err == nil {
		t.Fatal("Get after Delete should return error")
	}
}

func TestGCSStore_Integration_List(t *testing.T) {
	bucketName := skipUnlessGCSBucket(t)
	ctx := context.Background()

	var opts []option.ClientOption
	if creds := os.Getenv("BUCKET_TEST_GCS_CREDENTIALS"); creds != "" {
		opts = append(opts, option.WithCredentialsFile(creds))
	}

	store, err := NewGCSStore(ctx, bucketName, "test-list/", opts...)
	if err != nil {
		t.Fatalf("NewGCSStore: %v", err)
	}
	defer store.Close()

	// Setup: write objects in two "directories"
	keys := []string{
		"node-to-control/n1/001.pb",
		"node-to-control/n1/002.pb",
		"node-to-control/n2/001.pb",
	}
	for _, k := range keys {
		if err := store.Put(ctx, k, []byte("data")); err != nil {
			t.Fatalf("Put(%q): %v", k, err)
		}
	}

	// Cleanup after test
	defer func() {
		for _, k := range keys {
			store.Delete(ctx, k)
		}
	}()

	// List top-level "node-to-control/" should return two directory entries.
	listed, err := store.List(ctx, "node-to-control/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	// Should see "node-to-control/n1/" and "node-to-control/n2/"
	if len(listed) != 2 {
		t.Fatalf("List returned %d items, want 2: %v", len(listed), listed)
	}
	if listed[0] != "node-to-control/n1/" {
		t.Errorf("listed[0] = %q, want %q", listed[0], "node-to-control/n1/")
	}
	if listed[1] != "node-to-control/n2/" {
		t.Errorf("listed[1] = %q, want %q", listed[1], "node-to-control/n2/")
	}

	// List "node-to-control/n1/" should return the two files.
	listed2, err := store.List(ctx, "node-to-control/n1/")
	if err != nil {
		t.Fatalf("List(n1): %v", err)
	}
	if len(listed2) != 2 {
		t.Fatalf("List(n1) returned %d items, want 2: %v", len(listed2), listed2)
	}
	if listed2[0] != "node-to-control/n1/001.pb" {
		t.Errorf("listed2[0] = %q, want %q", listed2[0], "node-to-control/n1/001.pb")
	}
	if listed2[1] != "node-to-control/n1/002.pb" {
		t.Errorf("listed2[1] = %q, want %q", listed2[1], "node-to-control/n1/002.pb")
	}
}
