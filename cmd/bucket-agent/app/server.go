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

package app

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/api/option"
	"k8s.io/klog/v2"

	"sigs.k8s.io/apiserver-network-proxy/cmd/bucket-agent/app/options"
	"sigs.k8s.io/apiserver-network-proxy/pkg/bucket"
)

const (
	ReadHeaderTimeout = 60 * time.Second
)

func NewBucketAgentCommand(a *BucketProxyAgent, o *options.BucketProxyAgentOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:  "bucket-proxy-agent",
		Long: "A bucket-based proxy agent that polls cloud storage for Konnectivity packets and forwards them to local endpoints (kubelet).",
		RunE: func(_ *cobra.Command, _ []string) error {
			stopCh := SetupSignalHandler()
			return a.Run(o, stopCh)
		},
	}
	return cmd
}

type BucketProxyAgent struct {
	agent        *bucket.BucketAgent
	healthServer *http.Server
	adminServer  *http.Server
}

func (a *BucketProxyAgent) Run(o *options.BucketProxyAgentOptions, stopCh <-chan struct{}) error {
	o.Print()
	if err := o.Validate(); err != nil {
		return fmt.Errorf("failed to validate options: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, err := createStore(ctx, o)
	if err != nil {
		return fmt.Errorf("failed to create store: %v", err)
	}

	// Create and start the bucket agent.
	a.agent = bucket.NewBucketAgent(ctx, store, o.NodeID, o.PollInterval, o.NagleDelay)

	// Start health and admin servers.
	if err := a.runHealthServer(o); err != nil {
		return fmt.Errorf("failed to start health server: %v", err)
	}
	if err := a.runAdminServer(o); err != nil {
		return fmt.Errorf("failed to start admin server: %v", err)
	}

	// Run the agent in a goroutine so we can listen for shutdown signals.
	agentDone := make(chan struct{})
	go func() {
		defer close(agentDone)
		klog.V(1).Infof("Bucket proxy agent serving as node %q", o.NodeID)
		a.agent.Serve()
	}()

	// Wait for shutdown signal or agent exit.
	select {
	case <-stopCh:
		klog.V(1).Infoln("Shutting down bucket proxy agent.")
		a.agent.Stop()
	case <-agentDone:
		klog.V(1).Infoln("Bucket agent exited.")
	}

	if a.healthServer != nil {
		a.healthServer.Close()
	}
	if a.adminServer != nil {
		a.adminServer.Close()
	}

	return nil
}

func (a *BucketProxyAgent) runHealthServer(o *options.BucketProxyAgentOptions) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "ok")
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		// Agent is ready if it's running.
		fmt.Fprint(w, "ok")
	})

	a.healthServer = &http.Server{
		Addr:              net.JoinHostPort(o.HealthBindAddress, strconv.Itoa(o.HealthPort)),
		Handler:           mux,
		MaxHeaderBytes:    1 << 20,
		ReadHeaderTimeout: ReadHeaderTimeout,
	}

	go func() {
		klog.V(1).Infof("Health server listening on %s:%d", o.HealthBindAddress, o.HealthPort)
		if err := a.healthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			klog.ErrorS(err, "health server error")
		}
	}()

	return nil
}

func (a *BucketProxyAgent) runAdminServer(o *options.BucketProxyAgentOptions) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "ok")
	})

	a.adminServer = &http.Server{
		Addr:              net.JoinHostPort(o.AdminBindAddress, strconv.Itoa(o.AdminPort)),
		Handler:           mux,
		MaxHeaderBytes:    1 << 20,
		ReadHeaderTimeout: ReadHeaderTimeout,
	}

	go func() {
		klog.V(1).Infof("Admin server listening on %s:%d", o.AdminBindAddress, o.AdminPort)
		if err := a.adminServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			klog.ErrorS(err, "admin server error")
		}
	}()

	return nil
}

func createStore(ctx context.Context, o *options.BucketProxyAgentOptions) (bucket.Store, error) {
	switch o.StoreType {
	case "fs":
		return bucket.NewFSStore(o.BucketDir), nil
	case "gcs":
		var opts []option.ClientOption
		if o.GCSCredentialsFile != "" {
			opts = append(opts, option.WithCredentialsFile(o.GCSCredentialsFile))
		}
		return bucket.NewGCSStore(ctx, o.GCSBucket, o.GCSPrefix, opts...)
	default:
		return nil, fmt.Errorf("unknown store type %q", o.StoreType)
	}
}

var shutdownSignals = []os.Signal{os.Interrupt, syscall.SIGTERM}

func SetupSignalHandler() <-chan struct{} {
	stop := make(chan struct{})
	c := make(chan os.Signal, 2)
	signal.Notify(c, shutdownSignals...)
	go func() {
		<-c
		close(stop)
		<-c
		os.Exit(1)
	}()
	return stop
}
