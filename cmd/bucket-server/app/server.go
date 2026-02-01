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
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"k8s.io/klog/v2"

	"google.golang.org/api/option"

	"sigs.k8s.io/apiserver-network-proxy/cmd/bucket-server/app/options"
	"sigs.k8s.io/apiserver-network-proxy/konnectivity-client/proto/client"
	"sigs.k8s.io/apiserver-network-proxy/pkg/bucket"
	"sigs.k8s.io/apiserver-network-proxy/pkg/server"
	"sigs.k8s.io/apiserver-network-proxy/pkg/server/proxystrategies"
)

const (
	ReadHeaderTimeout = 60 * time.Second
)

func NewBucketServerCommand(p *BucketProxyServer, o *options.BucketProxyServerOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:  "bucket-proxy-server",
		Long: "A bucket-based proxy server that accepts Konnectivity connections from kube-apiserver and communicates with agents via cloud storage buckets.",
		RunE: func(_ *cobra.Command, _ []string) error {
			stopCh := SetupSignalHandler()
			return p.Run(o, stopCh)
		},
	}
	return cmd
}

type BucketProxyServer struct {
	grpcServer   *grpc.Server
	healthServer *http.Server
	adminServer  *http.Server

	proxyServer   *server.ProxyServer
	poller        *bucket.RegionalPoller
	hbMonitor     *bucket.HeartbeatMonitor
	store         bucket.Store
	nagleDelay    time.Duration

	mu         sync.Mutex
	transports map[string]*bucket.BucketTransport // nodeID → transport
}

func (p *BucketProxyServer) Run(o *options.BucketProxyServerOptions, stopCh <-chan struct{}) error {
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
	p.store = store
	p.nagleDelay = o.NagleDelay
	p.transports = make(map[string]*bucket.BucketTransport)

	// Create ProxyServer.
	ps, err := proxystrategies.ParseProxyStrategies("default")
	if err != nil {
		return fmt.Errorf("failed to parse proxy strategies: %v", err)
	}
	p.proxyServer = server.NewProxyServer(o.ServerID, ps, 1, &server.AgentTokenAuthenticationOptions{}, 10)

	// Start frontend gRPC server on UDS.
	if err := p.runFrontendServer(ctx, o); err != nil {
		return fmt.Errorf("failed to start frontend server: %v", err)
	}

	// Start regional poller for centralized bucket polling.
	p.poller = bucket.NewRegionalPoller(ctx, p.store, "node-to-control/")
	p.poller.SetWorkerCount(o.WorkerCount)
	go p.poller.Run()

	// Start heartbeat monitor — agents are discovered dynamically.
	p.hbMonitor = bucket.NewHeartbeatMonitor(ctx, p.store, 10*time.Second, bucket.DefaultHeartbeatTimeout)
	p.hbMonitor.OnNodeDiscovered = func(nodeID string) {
		p.registerNode(nodeID)
	}
	p.hbMonitor.OnNodeStale = func(nodeID string) {
		p.unregisterNode(nodeID)
	}
	go p.hbMonitor.Run()

	// Start health and admin servers.
	if err := p.runHealthServer(o); err != nil {
		return fmt.Errorf("failed to start health server: %v", err)
	}
	if err := p.runAdminServer(o); err != nil {
		return fmt.Errorf("failed to start admin server: %v", err)
	}

	klog.V(1).Infoln("Bucket proxy server running. Waiting for agents to register via heartbeat...")

	<-stopCh
	klog.V(1).Infoln("Shutting down bucket proxy server.")

	// Cleanup transports and bucket files for all tracked nodes.
	p.mu.Lock()
	nodeIDs := make([]string, 0, len(p.transports))
	for nodeID, t := range p.transports {
		klog.V(2).Infof("Closing transport for node %q", nodeID)
		t.Close()
		p.poller.UnregisterNode(nodeID)
		nodeIDs = append(nodeIDs, nodeID)
	}
	p.mu.Unlock()

	for _, nodeID := range nodeIDs {
		p.cleanupNodeFiles(nodeID)
	}

	p.poller.Stop()
	p.hbMonitor.Stop()
	if p.grpcServer != nil {
		p.grpcServer.GracefulStop()
	}
	if p.healthServer != nil {
		p.healthServer.Close()
	}
	if p.adminServer != nil {
		p.adminServer.Close()
	}

	return nil
}

func (p *BucketProxyServer) registerNode(nodeID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, exists := p.transports[nodeID]; exists {
		return
	}

	klog.V(1).Infof("Discovered new agent %q via heartbeat — registering", nodeID)
	transport := bucket.RegisterBucketAgentWithPoller(p.proxyServer, p.store, nodeID, p.poller, p.nagleDelay)
	p.transports[nodeID] = transport
}

func (p *BucketProxyServer) unregisterNode(nodeID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	transport, exists := p.transports[nodeID]
	if !exists {
		return
	}

	klog.V(1).Infof("Agent %q heartbeat timed out — unregistering", nodeID)
	transport.Close()
	p.poller.UnregisterNode(nodeID)
	delete(p.transports, nodeID)

	// Clean up stale heartbeat and data files for this node in the background.
	go p.cleanupNodeFiles(nodeID)
}

// cleanupNodeFiles removes residual bucket objects for a stale node.
// This includes heartbeat files and any unconsumed data messages.
func (p *BucketProxyServer) cleanupNodeFiles(nodeID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	prefixes := []string{
		"node-to-control/" + nodeID + "/",
		"control-to-node/" + nodeID + "/",
	}
	for _, prefix := range prefixes {
		keys, err := p.store.List(ctx, prefix)
		if err != nil {
			klog.V(4).InfoS("Failed to list keys for stale node cleanup", "prefix", prefix, "err", err)
			continue
		}
		for _, key := range keys {
			if err := p.store.Delete(ctx, key); err != nil {
				klog.V(4).InfoS("Failed to delete stale key", "key", key, "err", err)
			}
		}
		if len(keys) > 0 {
			klog.V(2).InfoS("Cleaned up stale node files", "nodeID", nodeID, "prefix", prefix, "count", len(keys))
		}
	}
}

func (p *BucketProxyServer) runFrontendServer(ctx context.Context, o *options.BucketProxyServerOptions) error {
	if o.DeleteUDSFile {
		if err := os.Remove(o.UdsName); err != nil && !os.IsNotExist(err) {
			klog.ErrorS(err, "failed to delete existing UDS file", "file", o.UdsName)
		}
	}

	serverOptions := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: o.FrontendKeepaliveTime}),
	}
	p.grpcServer = grpc.NewServer(serverOptions...)
	client.RegisterProxyServiceServer(p.grpcServer, p.proxyServer)

	oldUmask := syscall.Umask(0007)
	defer syscall.Umask(oldUmask)
	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "unix", o.UdsName)
	if err != nil {
		return fmt.Errorf("failed to listen on UDS %q: %v", o.UdsName, err)
	}

	go func() {
		klog.V(1).Infof("Frontend gRPC server listening on %s", o.UdsName)
		if err := p.grpcServer.Serve(lis); err != nil {
			klog.ErrorS(err, "frontend gRPC server error")
		}
	}()

	return nil
}

func (p *BucketProxyServer) runHealthServer(o *options.BucketProxyServerOptions) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "ok")
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		ready, msg := p.proxyServer.Readiness.Ready()
		if ready {
			w.WriteHeader(200)
			fmt.Fprint(w, "ok")
			return
		}
		w.WriteHeader(500)
		fmt.Fprint(w, msg)
	})

	p.healthServer = &http.Server{
		Addr:              net.JoinHostPort(o.HealthBindAddress, strconv.Itoa(o.HealthPort)),
		Handler:           mux,
		MaxHeaderBytes:    1 << 20,
		ReadHeaderTimeout: ReadHeaderTimeout,
	}

	go func() {
		klog.V(1).Infof("Health server listening on %s:%d", o.HealthBindAddress, o.HealthPort)
		if err := p.healthServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			klog.ErrorS(err, "health server error")
		}
	}()

	return nil
}

func (p *BucketProxyServer) runAdminServer(o *options.BucketProxyServerOptions) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "ok")
	})

	p.adminServer = &http.Server{
		Addr:              net.JoinHostPort(o.AdminBindAddress, strconv.Itoa(o.AdminPort)),
		Handler:           mux,
		MaxHeaderBytes:    1 << 20,
		ReadHeaderTimeout: ReadHeaderTimeout,
	}

	go func() {
		klog.V(1).Infof("Admin server listening on %s:%d", o.AdminBindAddress, o.AdminPort)
		if err := p.adminServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			klog.ErrorS(err, "admin server error")
		}
	}()

	return nil
}

func createStore(ctx context.Context, o *options.BucketProxyServerOptions) (bucket.Store, error) {
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
