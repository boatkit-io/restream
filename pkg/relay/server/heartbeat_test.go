package server

import (
	"context"
	"testing"
	"time"

	"github.com/boatkit-io/restream/pkg/relay/protocol"
	"github.com/boatkit-io/restream/pkg/restream"
	gws "github.com/gorilla/websocket"
)

func TestServerHeartbeatRequiresAdmissionAndCapability(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "unnegotiated", true: "negotiated"}[enabled], func(t *testing.T) {
			server, conn, cleanup := newTestWebsocketPair(t)
			defer cleanup()
			events := make(chan string, 1)
			manager := NewDeviceManager(DeviceManagerConfig{Stores: func(string) ([]restream.Store, error) { return nil, nil }, EventHandler: func(_ *Device, _ *Connection, name string, _ []byte) error { events <- name; return nil }})
			relay := New(Config{DeviceManager: manager, Capabilities: protocol.RelayCapabilities{Heartbeats: enabled}, AuthenticateDevice: func(context.Context, *protocol.DeviceHello, *Connection) (restream.AccessLevel, error) {
				return restream.AccessLevelPublic, nil
			}})
			ended := make(chan error, 1)
			go func() { ended <- relay.AcceptConn(context.Background(), server) }()
			hello, err := protocol.EncodeDeviceHello(&protocol.DeviceHello{ProtocolVersion: protocol.CurrentVersion, DeviceID: "synthetic"})
			if err != nil {
				t.Fatal(err)
			}
			if err := conn.WriteMessage(gws.BinaryMessage, hello); err != nil {
				t.Fatal(err)
			}
			if _, _, err := conn.ReadMessage(); err != nil {
				t.Fatal(err)
			}
			if err := conn.WriteMessage(gws.BinaryMessage, []byte{18}); err != nil {
				t.Fatal(err)
			}
			if enabled {
				b, err := protocol.EncodePacket(&protocol.EventPacket{EventName: "after-heartbeat"})
				if err != nil {
					t.Fatal(err)
				}
				if err := conn.WriteMessage(gws.BinaryMessage, b); err != nil {
					t.Fatal(err)
				}
				select {
				case <-events:
				case <-time.After(time.Second):
					t.Fatal("heartbeat interrupted ordinary packet handling")
				}
				if err := conn.Close(); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case err := <-ended:
				if err == nil {
					t.Fatal("expected disconnect or rejection")
				}
			case <-time.After(time.Second):
				t.Fatal("heartbeat server did not finish")
			}
		})
	}
}
