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
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestReverseProxy_EndToEnd(t *testing.T) {
	// 1. Start a local TCP server that simulates the apiserver.
	echoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("hello-from-reverse-tunnel"))
	}))
	t.Cleanup(echoServer.Close)

	// 2. Set up filesystem-backed bucket store.
	store := NewFSStore(t.TempDir())

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// 3. Create server-side ReverseProxyHandler pointing at the echo server.
	// The echo server address is the target the handler will always dial.
	handler := NewReverseProxyHandler(ctx, store, "node-1", echoServer.Listener.Addr().String(), 50*time.Millisecond, 0)
	go handler.Serve()
	t.Cleanup(handler.Stop)

	// 4. Create agent-side ReverseProxy listening on a random port.
	rp, err := NewReverseProxy(ctx, store, "node-1", "127.0.0.1:0", 0)
	if err != nil {
		t.Fatalf("Failed to create ReverseProxy: %v", err)
	}

	// Start AgentPoller for consolidated polling (forward transport is nil since
	// this test only exercises the reverse proxy path).
	rpPoller := NewAgentPoller(ctx, store, "node-1", nil, rp.Transport(), 50*time.Millisecond)
	go rpPoller.Run()
	t.Cleanup(rpPoller.Stop)

	go rp.Serve()
	t.Cleanup(rp.Stop)

	// Give transport polling a moment to start.
	time.Sleep(200 * time.Millisecond)

	// 5. Make an HTTP request through the reverse proxy.
	proxyAddr := rp.listener.Addr().String()
	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "tcp", proxyAddr)
			},
		},
		Timeout: 30 * time.Second,
	}

	resp, err := httpClient.Get("http://target-via-reverse-proxy/")
	if err != nil {
		t.Fatalf("HTTP GET through reverse proxy failed: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}

	if string(body) != "hello-from-reverse-tunnel" {
		t.Errorf("Expected 'hello-from-reverse-tunnel', got %q", string(body))
	}
	t.Logf("Success! Received: %s", string(body))
}

func TestReverseProxy_MultipleConnections(t *testing.T) {
	// TCP echo server: reads and echoes back data.
	echoListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}
	t.Cleanup(func() { echoListener.Close() })

	go func() {
		for {
			conn, err := echoListener.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				io.Copy(c, c)
			}(conn)
		}
	}()

	store := NewFSStore(t.TempDir())

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	handler := NewReverseProxyHandler(ctx, store, "node-2", echoListener.Addr().String(), 50*time.Millisecond, 0)
	go handler.Serve()
	t.Cleanup(handler.Stop)

	rp, err := NewReverseProxy(ctx, store, "node-2", "127.0.0.1:0", 0)
	if err != nil {
		t.Fatalf("Failed to create ReverseProxy: %v", err)
	}

	rpPoller := NewAgentPoller(ctx, store, "node-2", nil, rp.Transport(), 50*time.Millisecond)
	go rpPoller.Run()
	t.Cleanup(rpPoller.Stop)

	go rp.Serve()
	t.Cleanup(rp.Stop)

	time.Sleep(200 * time.Millisecond)

	proxyAddr := rp.listener.Addr().String()

	// Open multiple concurrent connections.
	const numConns = 5
	errCh := make(chan error, numConns)

	for i := 0; i < numConns; i++ {
		go func(idx int) {
			conn, err := net.DialTimeout("tcp", proxyAddr, 10*time.Second)
			if err != nil {
				errCh <- fmt.Errorf("conn %d: dial failed: %w", idx, err)
				return
			}
			defer conn.Close()

			msg := fmt.Sprintf("hello-%d", idx)
			_, err = conn.Write([]byte(msg))
			if err != nil {
				errCh <- fmt.Errorf("conn %d: write failed: %w", idx, err)
				return
			}

			buf := make([]byte, len(msg))
			conn.SetReadDeadline(time.Now().Add(10 * time.Second))
			_, err = io.ReadFull(conn, buf)
			if err != nil {
				errCh <- fmt.Errorf("conn %d: read failed: %w", idx, err)
				return
			}

			if string(buf) != msg {
				errCh <- fmt.Errorf("conn %d: expected %q, got %q", idx, msg, string(buf))
				return
			}

			errCh <- nil
		}(i)
	}

	for i := 0; i < numConns; i++ {
		if err := <-errCh; err != nil {
			t.Error(err)
		}
	}
}
