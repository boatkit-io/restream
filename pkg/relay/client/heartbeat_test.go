package client

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/boatkit-io/restream/pkg/relay/protocol"
	gws "github.com/gorilla/websocket"
)

func TestHeartbeatIsNegotiatedAndConnectionOwned(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "old relay", true: "capable relay"}[enabled], func(t *testing.T) {
			server, conn, cleanup := newTestWebsocketPair(t)
			defer cleanup()
			var sent atomic.Int32
			s := NewStreamer(nil, nil, nil, Config{HeartbeatInterval: 20 * time.Millisecond, Callbacks: Callbacks{OnBytesSent: func(n int) { sent.Add(int32(n)) }}})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stopped := make(chan error, 1)
			go func() { stopped <- s.handleConn(ctx, conn, Credentials{}) }()
			if _, _, err := server.ReadMessage(); err != nil {
				t.Fatal(err)
			} // hello, not admission
			time.Sleep(45 * time.Millisecond)
			if sent.Load() != 0 {
				t.Fatal("heartbeat before admission")
			}
			b, err := protocol.EncodePacket(&protocol.ConnectedPacket{ProtocolVersion: protocol.CurrentVersion, Capabilities: protocol.RelayCapabilities{Heartbeats: enabled}})
			if err != nil {
				t.Fatal(err)
			}
			if err := server.WriteMessage(gws.BinaryMessage, b); err != nil {
				t.Fatal(err)
			}
			if enabled {
				if err := server.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
					t.Fatal(err)
				}
				for range 3 {
					_, b, err := server.ReadMessage()
					if err != nil {
						t.Fatal(err)
					}
					if len(b) != 1 || b[0] != 18 {
						t.Fatalf("heartbeat bytes=%v", b)
					}
				}
			} else {
				time.Sleep(70 * time.Millisecond)
				if sent.Load() != 0 {
					t.Fatal("heartbeat without capability")
				}
			}
			cancel()
			select {
			case err := <-stopped:
				if err == nil {
					t.Fatal("expected relay read error after cancellation")
				}
			case <-time.After(time.Second):
				t.Fatal("reader did not stop")
			}
			// The writer may have finished a write before cancellation, but must settle.
			time.Sleep(30 * time.Millisecond)
			after := sent.Load()
			time.Sleep(60 * time.Millisecond)
			if sent.Load() != after {
				t.Fatal("heartbeat survives connection shutdown")
			}
		})
	}
}

func TestHeartbeatWriterPostponesForTrafficAndStops(t *testing.T) {
	server, conn, cleanup := newTestWebsocketPair(t)
	defer cleanup()
	s := NewStreamer(nil, nil, nil, Config{})
	queue := make(chan outboundPacket, 1)
	stop := make(chan struct{})
	const interval = 120 * time.Millisecond
	finished := s.handleSendQueue(conn, queue, stop, interval)
	read := func() []byte {
		t.Helper()
		if err := server.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		_, b, err := server.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	for range 4 {
		time.Sleep(65 * time.Millisecond)
		queue <- outboundPacket{bytes: []byte{128}}
		if b := read(); len(b) != 1 || b[0] != 128 {
			t.Fatalf("redundant heartbeat during traffic: %v", b)
		}
	}
	last := time.Now()
	if b := read(); len(b) != 1 || b[0] != 18 {
		t.Fatalf("idle heartbeat=%v", b)
	}
	if time.Since(last) < interval-25*time.Millisecond {
		t.Fatal("heartbeat timer did not reset after traffic")
	}
	close(stop)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("writer/timer did not stop")
	}
}

func TestFailedHeartbeatClosesOnlyItsConnection(t *testing.T) {
	server, conn, cleanup := newTestWebsocketPair(t)
	defer cleanup()
	replacementServer, replacement, cleanupReplacement := newTestWebsocketPair(t)
	defer cleanupReplacement()
	s := NewStreamer(nil, nil, nil, Config{HeartbeatInterval: time.Millisecond})
	s.startConn(replacement, protocol.RelayCapabilities{})
	defer s.closeCurrentConn()
	// Force an old generation's delayed writer to fail after replacement.
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	done := s.handleSendQueue(conn, make(chan outboundPacket), make(chan struct{}), time.Millisecond)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("failed heartbeat writer retained")
	}
	if !s.isConnected() {
		t.Fatal("old heartbeat failure closed replacement")
	}
	if err := s.enqueuePacket("replacement", []byte{128}); err != nil {
		t.Fatal(err)
	}
	if err := replacementServer.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := replacementServer.ReadMessage(); err != nil {
		t.Fatal(err)
	}
	_ = server
}

func TestStalledWriteRetiresHeartbeatWriter(t *testing.T) {
	server, conn, cleanup := newTestWebsocketPair(t)
	defer cleanup()
	if err := conn.UnderlyingConn().(*net.TCPConn).SetWriteBuffer(1024); err != nil {
		t.Fatal(err)
	}
	if err := server.UnderlyingConn().(*net.TCPConn).SetReadBuffer(1024); err != nil {
		t.Fatal(err)
	}
	s := NewStreamer(nil, nil, nil, Config{WriteTimeout: 25 * time.Millisecond})
	queue := make(chan outboundPacket, 1)
	stop := make(chan struct{})
	s.conn = conn
	s.sendQueue = queue
	s.sendDone = stop
	finished := s.handleSendQueue(conn, queue, stop, 10*time.Millisecond)
	// The peer never consumes this frame. Heartbeats must not queue indefinitely
	// behind a write or create a competing writer to escape backpressure.
	queue <- outboundPacket{bytes: make([]byte, 8*1024*1024)}
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("stalled writer/timer survived deadline")
	}
	if s.isConnected() {
		t.Fatal("stalled heartbeat connection remained active")
	}
}
