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

type BucketProxyServerOptions struct {
	// Bucket store configuration.
	StoreType          string // "fs" or "gcs"
	BucketDir          string // Path to the filesystem bucket directory (store-type=fs).
	GCSBucket          string // GCS bucket name (store-type=gcs).
	GCSPrefix          string // Optional prefix within GCS bucket.
	GCSCredentialsFile string // Optional path to service account key (default: ADC).

	// Frontend (kube-apiserver facing) configuration.
	// The frontend uses a UDS so kube-apiserver's EgressSelector can connect locally.
	UdsName       string
	DeleteUDSFile bool
	Mode          string // "grpc" or "http-connect"

	// Server identity.
	ServerID string

	// Health/admin ports.
	HealthPort        int
	HealthBindAddress string
	AdminPort         int
	AdminBindAddress  string

	// Polling configuration for the regional poller.
	WorkerCount int

	// Keepalive for frontend gRPC connections.
	FrontendKeepaliveTime time.Duration
}

func NewBucketProxyServerOptions() *BucketProxyServerOptions {
	return &BucketProxyServerOptions{
		StoreType:             "fs",
		BucketDir:             "",
		UdsName:               "",
		DeleteUDSFile:         true,
		Mode:                  "grpc",
		ServerID:              defaultServerID(),
		HealthPort:            8092,
		HealthBindAddress:     "",
		AdminPort:             8095,
		AdminBindAddress:      "127.0.0.1",
		WorkerCount:           10,
		FrontendKeepaliveTime: 1 * time.Hour,
	}
}

func (o *BucketProxyServerOptions) Flags() *pflag.FlagSet {
	flags := pflag.NewFlagSet("bucket-proxy-server", pflag.ContinueOnError)
	flags.StringVar(&o.StoreType, "store-type", o.StoreType, "Backend store type: 'fs' or 'gcs'.")
	flags.StringVar(&o.BucketDir, "bucket-dir", o.BucketDir, "Path to the filesystem bucket directory for message storage (store-type=fs).")
	flags.StringVar(&o.GCSBucket, "gcs-bucket", o.GCSBucket, "GCS bucket name for message storage (store-type=gcs).")
	flags.StringVar(&o.GCSPrefix, "gcs-prefix", o.GCSPrefix, "Optional key prefix within the GCS bucket.")
	flags.StringVar(&o.GCSCredentialsFile, "gcs-credentials-file", o.GCSCredentialsFile, "Path to GCS service account key file. Empty uses application default credentials.")
	flags.StringVar(&o.UdsName, "uds-name", o.UdsName, "Unix domain socket path for frontend (kube-apiserver) connections.")
	flags.BoolVar(&o.DeleteUDSFile, "delete-existing-uds-file", o.DeleteUDSFile, "If true and UDS file already exists, delete before listening.")
	flags.StringVar(&o.Mode, "mode", o.Mode, "Frontend mode: 'grpc' or 'http-connect'.")
	flags.StringVar(&o.ServerID, "server-id", o.ServerID, "Unique server ID. Can also be set via PROXY_SERVER_ID env var.")
	flags.IntVar(&o.HealthPort, "health-port", o.HealthPort, "Port for health check endpoint.")
	flags.StringVar(&o.HealthBindAddress, "health-bind-address", o.HealthBindAddress, "Bind address for health endpoint.")
	flags.IntVar(&o.AdminPort, "admin-port", o.AdminPort, "Port for admin/metrics endpoint.")
	flags.StringVar(&o.AdminBindAddress, "admin-bind-address", o.AdminBindAddress, "Bind address for admin endpoint.")
	flags.IntVar(&o.WorkerCount, "worker-count", o.WorkerCount, "Number of concurrent download workers per region for the regional poller.")
	flags.DurationVar(&o.FrontendKeepaliveTime, "frontend-keepalive-time", o.FrontendKeepaliveTime, "Keepalive time for frontend gRPC connections.")
	return flags
}

func (o *BucketProxyServerOptions) Print() {
	klog.V(1).Infof("StoreType set to %q.\n", o.StoreType)
	klog.V(1).Infof("BucketDir set to %q.\n", o.BucketDir)
	klog.V(1).Infof("GCSBucket set to %q.\n", o.GCSBucket)
	klog.V(1).Infof("GCSPrefix set to %q.\n", o.GCSPrefix)
	klog.V(1).Infof("UdsName set to %q.\n", o.UdsName)
	klog.V(1).Infof("DeleteUDSFile set to %v.\n", o.DeleteUDSFile)
	klog.V(1).Infof("Mode set to %q.\n", o.Mode)
	klog.V(1).Infof("ServerID set to %q.\n", o.ServerID)
	klog.V(1).Infof("HealthPort set to %d.\n", o.HealthPort)
	klog.V(1).Infof("HealthBindAddress set to %q.\n", o.HealthBindAddress)
	klog.V(1).Infof("AdminPort set to %d.\n", o.AdminPort)
	klog.V(1).Infof("AdminBindAddress set to %q.\n", o.AdminBindAddress)
	klog.V(1).Infof("WorkerCount set to %d.\n", o.WorkerCount)
	klog.V(1).Infof("FrontendKeepaliveTime set to %v.\n", o.FrontendKeepaliveTime)
}

func (o *BucketProxyServerOptions) Validate() error {
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
	if o.UdsName == "" {
		return fmt.Errorf("--uds-name is required (frontend UDS path)")
	}
	if o.Mode != "grpc" && o.Mode != "http-connect" {
		return fmt.Errorf("--mode must be 'grpc' or 'http-connect', got %q", o.Mode)
	}
	if o.WorkerCount < 1 {
		return fmt.Errorf("--worker-count must be >= 1, got %d", o.WorkerCount)
	}
	return nil
}

func defaultServerID() string {
	if id := os.Getenv("PROXY_SERVER_ID"); id != "" {
		return id
	}
	return uuid.New().String()
}
