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

package options

import (
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/pflag"
	"k8s.io/klog/v2"
)

type BucketProxyAgentOptions struct {
	// Bucket store configuration.
	StoreType          string // "fs" or "gcs"
	BucketDir          string // Path to the filesystem bucket directory (store-type=fs).
	GCSBucket          string // GCS bucket name (store-type=gcs).
	GCSPrefix          string // Optional prefix within GCS bucket.
	GCSCredentialsFile string // Optional path to service account key (default: ADC).

	// Node identity.
	NodeID string

	// Polling interval for bucket transport (0 = adaptive polling).
	PollInterval time.Duration

	// NagleDelay coalesces small DATA packets per connection before flushing.
	// 0 disables Nagle buffering (each packet is sent immediately).
	NagleDelay time.Duration

	// Health/admin ports.
	HealthPort        int
	HealthBindAddress string
	AdminPort         int
	AdminBindAddress  string
}

func NewBucketProxyAgentOptions() *BucketProxyAgentOptions {
	return &BucketProxyAgentOptions{
		StoreType:         "fs",
		BucketDir:         "",
		NodeID:            defaultNodeID(),
		PollInterval:      0, // adaptive polling by default
		HealthPort:        8093,
		HealthBindAddress: "",
		AdminPort:         8094,
		AdminBindAddress:  "127.0.0.1",
	}
}

func (o *BucketProxyAgentOptions) Flags() *pflag.FlagSet {
	flags := pflag.NewFlagSet("bucket-proxy-agent", pflag.ContinueOnError)
	flags.StringVar(&o.StoreType, "store-type", o.StoreType, "Backend store type: 'fs' or 'gcs'.")
	flags.StringVar(&o.BucketDir, "bucket-dir", o.BucketDir, "Path to the filesystem bucket directory for message storage (store-type=fs).")
	flags.StringVar(&o.GCSBucket, "gcs-bucket", o.GCSBucket, "GCS bucket name for message storage (store-type=gcs).")
	flags.StringVar(&o.GCSPrefix, "gcs-prefix", o.GCSPrefix, "Optional key prefix within the GCS bucket.")
	flags.StringVar(&o.GCSCredentialsFile, "gcs-credentials-file", o.GCSCredentialsFile, "Path to GCS service account key file. Empty uses application default credentials.")
	flags.StringVar(&o.NodeID, "node-id", o.NodeID, "Unique node ID for this agent. Can also be set via BUCKET_AGENT_NODE_ID env var.")
	flags.DurationVar(&o.PollInterval, "poll-interval", o.PollInterval, "Bucket polling interval. 0 enables adaptive polling (100ms-5s).")
	flags.DurationVar(&o.NagleDelay, "nagle-delay", o.NagleDelay, "Coalesce small DATA packets for this duration before flushing. 0 disables.")
	flags.IntVar(&o.HealthPort, "health-port", o.HealthPort, "Port for health check endpoint.")
	flags.StringVar(&o.HealthBindAddress, "health-bind-address", o.HealthBindAddress, "Bind address for health endpoint.")
	flags.IntVar(&o.AdminPort, "admin-port", o.AdminPort, "Port for admin endpoint.")
	flags.StringVar(&o.AdminBindAddress, "admin-bind-address", o.AdminBindAddress, "Bind address for admin endpoint.")
	return flags
}

func (o *BucketProxyAgentOptions) Print() {
	klog.V(1).Infof("StoreType set to %q.\n", o.StoreType)
	klog.V(1).Infof("BucketDir set to %q.\n", o.BucketDir)
	klog.V(1).Infof("GCSBucket set to %q.\n", o.GCSBucket)
	klog.V(1).Infof("GCSPrefix set to %q.\n", o.GCSPrefix)
	klog.V(1).Infof("NodeID set to %q.\n", o.NodeID)
	klog.V(1).Infof("PollInterval set to %v.\n", o.PollInterval)
	klog.V(1).Infof("NagleDelay set to %v.\n", o.NagleDelay)
	klog.V(1).Infof("HealthPort set to %d.\n", o.HealthPort)
	klog.V(1).Infof("HealthBindAddress set to %q.\n", o.HealthBindAddress)
	klog.V(1).Infof("AdminPort set to %d.\n", o.AdminPort)
	klog.V(1).Infof("AdminBindAddress set to %q.\n", o.AdminBindAddress)
}

func (o *BucketProxyAgentOptions) Validate() error {
	switch o.StoreType {
	case "fs":
		if o.BucketDir == "" {
			return fmt.Errorf("--bucket-dir is required when --store-type=fs")
		}
		if info, err := os.Stat(o.BucketDir); err != nil {
			return fmt.Errorf("--bucket-dir %q: %v", o.BucketDir, err)
		} else if !info.IsDir() {
			return fmt.Errorf("--bucket-dir %q is not a directory", o.BucketDir)
		}
	case "gcs":
		if o.GCSBucket == "" {
			return fmt.Errorf("--gcs-bucket is required when --store-type=gcs")
		}
	default:
		return fmt.Errorf("--store-type must be 'fs' or 'gcs', got %q", o.StoreType)
	}
	if o.NodeID == "" {
		return fmt.Errorf("--node-id is required")
	}
	return nil
}

func defaultNodeID() string {
	if id := os.Getenv("BUCKET_AGENT_NODE_ID"); id != "" {
		return id
	}
	return uuid.New().String()
}
