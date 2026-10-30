package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/boatkit-io/restream/pkg/relay/protocol"
	gws "github.com/gorilla/websocket"
)

func TestHandshakeDeadlineDefaultsPreserveCallerDialer(t *testing.T) {
	for _, configured := range []time.Duration{0, -time.Second, time.Minute} {
		dialer := &gws.Dialer{HandshakeTimeout: configured, EnableCompression: true}
		configuredStreamer := NewStreamer(nil, nil, nil, Config{Dialer: dialer})
		want := configured
		if want <= 0 {
			want = defaultHandshakeTimeout
		}
		if configuredStreamer.opts.Dialer.HandshakeTimeout != want || dialer.HandshakeTimeout != configured {
			t.Fatalf("timeout = %v, caller = %v; want %v, %v", configuredStreamer.opts.Dialer.HandshakeTimeout, dialer.HandshakeTimeout, want, configured)
		}
	}
}

func TestRunRecoversFromStalledHandshake(t *testing.T) {
	for _, stage := range []string{"upgrade", "connected"} {
		t.Run(stage, func(t *testing.T) {
			timeout := DefaultConfig().Dialer.HandshakeTimeout
			if timeout <= 0 || timeout > 30*time.Second {
				t.Fatalf("unbounded default handshake: %v", timeout)
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout+5*time.Second)
			defer cancel()
			var attempts atomic.Int32
			var failures atomic.Int32
			connected := make(chan struct{}, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				first := attempts.Add(1) == 1
				if first && stage == "upgrade" {
					<-r.Context().Done()
					return
				}
				conn, err := (&gws.Upgrader{}).Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer func() { _ = conn.Close() }()
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
				if !first {
					packet, err := protocol.EncodePacket(&protocol.ConnectedPacket{ProtocolVersion: protocol.CurrentVersion})
					if err != nil {
						t.Error(err)
						return
					}
					if err := conn.WriteMessage(gws.BinaryMessage, packet); err != nil {
						return
					}
				}
				// First connection deliberately withholds Connected. A disconnected peer
				// ends this read, proving the expired attempt was actually released.
				for {
					if _, _, err := conn.ReadMessage(); err != nil {
						return
					}
				}
			}))
			defer server.Close()
			streamer := NewStreamer(nil, nil, nil, Config{
				Endpoint: "ws" + strings.TrimPrefix(server.URL, "http"), ReconnectDelay: time.Millisecond,
				Callbacks: Callbacks{
					OnDialError: func(error) { failures.Add(1) },
					OnDisconnected: func(err error) {
						if err != nil {
							failures.Add(1)
						}
					},
					OnConnected: func(*protocol.ConnectedPacket) { connected <- struct{}{} },
				},
			})
			done := make(chan error, 1)
			go func() { done <- streamer.Run(ctx) }()
			defer func() { cancel(); streamer.Shutdown(); <-done }()
			select {
			case <-connected:
				if attempts.Load() != 2 || failures.Load() != 1 {
					t.Fatalf("attempts=%d failures=%d", attempts.Load(), failures.Load())
				}
			case <-ctx.Done():
				t.Fatal("stalled handshake prevented reconnect")
			}
		})
	}
}
