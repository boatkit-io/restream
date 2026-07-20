package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/boatkit-io/restream/pkg/binarystreams"
	"github.com/boatkit-io/restream/pkg/relay/protocol"
	"github.com/boatkit-io/restream/pkg/restream"
	gws "github.com/gorilla/websocket"
)

func TestServerAcceptConnAuthenticatesAndDispatchesPackets(t *testing.T) {
	serverConn, clientConn, cleanup := newTestWebsocketPair(t)
	defer cleanup()

	tracker := &testDeviceTracker{}
	manager := NewDeviceManager(DeviceManagerConfig{
		Stores: func(deviceID string) ([]restream.Store, error) {
			if deviceID != "device-1" {
				t.Fatalf("store factory deviceID = %q, want device-1", deviceID)
			}
			return []restream.Store{
				restream.NewRelayStore[testState, *testState, *testPartial]("TestStore", &testState{}, restream.AccessLevelPublic),
			}, nil
		},
		OnDeviceConnected: func(_ *Device, conn *Connection) {
			tracker.connected = true
			tracker.accessLevel = conn.AccessLevel
		},
		OnDeviceDisconnected: func(*Device, *Connection) {
			tracker.disconnected = true
		},
		FullStateHandler: func(_ *Device, _ *Connection, storeName string, data []byte) error {
			tracker.fullStore = storeName
			tracker.fullData = data
			return nil
		},
		EventHandler: func(_ *Device, _ *Connection, eventName string, data []byte) error {
			tracker.eventName = eventName
			tracker.eventData = data
			return nil
		},
	})
	relayServer := New(Config{
		DeviceManager: manager,
		Capabilities: protocol.RelayCapabilities{
			OnDemandStoreStreaming: true,
		},
		Metadata: map[string]string{"relay": "test"},
		AuthenticateDevice: func(_ context.Context, hello *protocol.DeviceHello, conn *Connection) (restream.AccessLevel, error) {
			if hello.DeviceID != "device-1" {
				t.Fatalf("DeviceID = %q, want device-1", hello.DeviceID)
			}
			if hello.AuthType != "shared-key" {
				t.Fatalf("AuthType = %q, want shared-key", hello.AuthType)
			}
			if string(hello.AuthData) != "secret" {
				t.Fatalf("AuthData = %q, want secret", hello.AuthData)
			}
			if conn.DeviceID != "device-1" {
				t.Fatalf("connection DeviceID = %q, want device-1", conn.DeviceID)
			}
			return restream.AccessLevel(3), nil
		},
	})

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- relayServer.AcceptConn(context.Background(), serverConn)
	}()

	helloBytes, err := protocol.EncodeDeviceHello(&protocol.DeviceHello{
		ProtocolVersion: protocol.CurrentVersion,
		DeviceID:        "device-1",
		AuthType:        "shared-key",
		AuthData:        []byte("secret"),
	})
	if err != nil {
		t.Fatalf("EncodeDeviceHello failed: %v", err)
	}
	if err := clientConn.WriteMessage(gws.BinaryMessage, helloBytes); err != nil {
		t.Fatalf("Write hello failed: %v", err)
	}

	_, connectedBytes, err := clientConn.ReadMessage()
	if err != nil {
		t.Fatalf("Read connected failed: %v", err)
	}
	connectedPacketRaw, err := protocol.DecodePacket(connectedBytes)
	if err != nil {
		t.Fatalf("Decode connected failed: %v", err)
	}
	connectedPacket, ok := connectedPacketRaw.(*protocol.ConnectedPacket)
	if !ok {
		t.Fatalf("connected packet type = %T, want *ConnectedPacket", connectedPacketRaw)
	}
	if connectedPacket.Metadata["relay"] != "test" {
		t.Fatalf("connected metadata = %+v, want relay=test", connectedPacket.Metadata)
	}
	if !connectedPacket.Capabilities.OnDemandStoreStreaming {
		t.Fatal("connected packet did not advertise on-demand store streaming")
	}
	if !manager.HasDevice("device-1") {
		t.Fatal("device manager did not provision device-1")
	}

	fullStateBytes, err := protocol.EncodePacket(protocol.NewFullStatePacket("TestStore", []byte{1, 2, 3}))
	if err != nil {
		t.Fatalf("Encode full state failed: %v", err)
	}
	if err := clientConn.WriteMessage(gws.BinaryMessage, fullStateBytes); err != nil {
		t.Fatalf("Write full state failed: %v", err)
	}

	eventBytes, err := protocol.EncodePacket(&protocol.EventPacket{EventName: "TestEvent", Data: []byte{4, 5, 6}})
	if err != nil {
		t.Fatalf("Encode event failed: %v", err)
	}
	if err := clientConn.WriteMessage(gws.BinaryMessage, eventBytes); err != nil {
		t.Fatalf("Write event failed: %v", err)
	}

	clientConn.Close() //nolint:errcheck // Why: End server read loop.
	select {
	case err := <-serverDone:
		if err == nil {
			t.Fatal("AcceptConn returned nil, want read close error")
		}
	case <-time.After(time.Second):
		t.Fatal("AcceptConn did not return after client close")
	}

	if !tracker.connected {
		t.Fatal("device was not marked connected")
	}
	if tracker.accessLevel != restream.AccessLevel(3) {
		t.Fatalf("connection access level = %d, want 3", tracker.accessLevel)
	}
	if !tracker.disconnected {
		t.Fatal("device was not marked disconnected")
	}
	if tracker.fullStore != "TestStore" || string(tracker.fullData) != string([]byte{1, 2, 3}) {
		t.Fatalf("full state = %s/%v, want TestStore/[1 2 3]", tracker.fullStore, tracker.fullData)
	}
	if tracker.eventName != "TestEvent" || string(tracker.eventData) != string([]byte{4, 5, 6}) {
		t.Fatalf("event = %s/%v, want TestEvent/[4 5 6]", tracker.eventName, tracker.eventData)
	}
}

func TestConnectionSendRPCWritesCallPacket(t *testing.T) {
	serverConn, clientConn, cleanup := newTestWebsocketPair(t)
	defer cleanup()

	conn := NewConnection(serverConn)
	if err := conn.SendRPC(12, "Store.Method", restream.AccessLevel(3), []byte{7, 8, 9}); err != nil {
		t.Fatalf("SendRPC failed: %v", err)
	}

	messageType, message, err := clientConn.ReadMessage()
	if err != nil {
		t.Fatalf("Read RPC failed: %v", err)
	}
	if messageType != gws.BinaryMessage {
		t.Fatalf("message type = %d, want BinaryMessage", messageType)
	}
	packetRaw, err := protocol.DecodePacket(message)
	if err != nil {
		t.Fatalf("Decode RPC failed: %v", err)
	}
	packet, ok := packetRaw.(*protocol.RPCCallPacket)
	if !ok {
		t.Fatalf("RPC packet type = %T, want *RPCCallPacket", packetRaw)
	}
	if packet.RPCID != 12 || packet.MethodName != "Store.Method" || packet.AccessLevel != 3 {
		t.Fatalf("RPC packet = %+v, want id=12 method=Store.Method access=3", packet)
	}
	if string(packet.Request) != string([]byte{7, 8, 9}) {
		t.Fatalf("RPC request = %v, want [7 8 9]", packet.Request)
	}
}

func TestServerReadPacketsWrapsStorePacketError(t *testing.T) {
	serverConn, clientConn, cleanup := newTestWebsocketPair(t)
	defer cleanup()

	device := NewDevice("device-1", nil, DeviceManagerConfig{
		FullStateHandler: func(*Device, *Connection, string, []byte) error {
			return errors.New(`deserialize field "Enabled" (fieldID=4): unhandled deserialized bool val in DeserializeBool: 4`)
		},
	})
	relayServer := &Server{}
	errCh := make(chan error, 1)
	go func() {
		errCh <- relayServer.readPackets(NewConnection(serverConn), device)
	}()

	fullStateBytes, err := protocol.EncodePacket(protocol.NewFullStatePacket("Security", []byte{3, 1, 1, 4}))
	if err != nil {
		t.Fatalf("Encode full state failed: %v", err)
	}
	if err := clientConn.WriteMessage(gws.BinaryMessage, fullStateBytes); err != nil {
		t.Fatalf("Write full state failed: %v", err)
	}

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("readPackets error = nil, want packet context error")
		}
		for _, want := range []string{
			`handle relay full state packet for store "Security"`,
			"4 bytes",
			"fieldID=4",
			"DeserializeBool: 4",
		} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("readPackets error = %q, missing %q", err, want)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("readPackets did not return")
	}
}

func TestConnectionSendStoreSubscriptionWritesPacket(t *testing.T) {
	serverConn, clientConn, cleanup := newTestWebsocketPair(t)
	defer cleanup()

	conn := NewConnection(serverConn)
	if err := conn.SendStoreSubscription("TestStore", "values%&a", true); err != nil {
		t.Fatalf("SendStoreSubscription failed: %v", err)
	}

	messageType, message, err := clientConn.ReadMessage()
	if err != nil {
		t.Fatalf("Read store subscription failed: %v", err)
	}
	if messageType != gws.BinaryMessage {
		t.Fatalf("message type = %d, want BinaryMessage", messageType)
	}
	packetRaw, err := protocol.DecodePacket(message)
	if err != nil {
		t.Fatalf("Decode store subscription failed: %v", err)
	}
	packet, ok := packetRaw.(*protocol.StoreSubscriptionPacket)
	if !ok {
		t.Fatalf("store subscription packet type = %T, want *StoreSubscriptionPacket", packetRaw)
	}
	if packet.StoreName != "TestStore" || packet.Key != "values%&a" || packet.Action != protocol.StoreSubscribe {
		t.Fatalf("store subscription packet = %+v, want TestStore values%%&a subscribe", packet)
	}
}

func TestConnectionSendStoreStateWritesPackets(t *testing.T) {
	serverConn, clientConn, cleanup := newTestWebsocketPair(t)
	defer cleanup()

	conn := NewConnection(serverConn)
	if err := conn.SendFullState("TestStore", []byte{1, 2, 3}); err != nil {
		t.Fatalf("SendFullState failed: %v", err)
	}
	_, message, err := clientConn.ReadMessage()
	if err != nil {
		t.Fatalf("Read full state failed: %v", err)
	}
	packet, ok := mustDecodePacket(t, message).(*protocol.StoreStatePacket)
	if !ok {
		t.Fatalf("full state packet type = %T, want *StoreStatePacket", mustDecodePacket(t, message))
	}
	if packet.StoreName != "TestStore" || packet.Kind() != protocol.KindFullState || string(packet.Data) != string([]byte{1, 2, 3}) {
		t.Fatalf("full state packet = %+v, want TestStore full state", packet)
	}

	if err := conn.SendPartialState("TestStore", []byte{4, 5, 6}); err != nil {
		t.Fatalf("SendPartialState failed: %v", err)
	}
	_, message, err = clientConn.ReadMessage()
	if err != nil {
		t.Fatalf("Read partial state failed: %v", err)
	}
	packet, ok = mustDecodePacket(t, message).(*protocol.StoreStatePacket)
	if !ok {
		t.Fatalf("partial state packet type = %T, want *StoreStatePacket", mustDecodePacket(t, message))
	}
	if packet.StoreName != "TestStore" || packet.Kind() != protocol.KindPartialState || string(packet.Data) != string([]byte{4, 5, 6}) {
		t.Fatalf("partial state packet = %+v, want TestStore partial state", packet)
	}
}

func TestConnectionCloseWithReasonWritesCloseFrame(t *testing.T) {
	serverConn, clientConn, cleanup := newTestWebsocketPair(t)
	defer cleanup()

	conn := NewConnection(serverConn)
	if err := conn.CloseWithReason(gws.ClosePolicyViolation, duplicateRelayConnectionReason); err != nil {
		t.Fatalf("CloseWithReason failed: %v", err)
	}

	_, _, err := clientConn.ReadMessage()
	closeErr := assertCloseError(t, err)
	if closeErr.Code != gws.ClosePolicyViolation {
		t.Fatalf("close code = %d, want %d", closeErr.Code, gws.ClosePolicyViolation)
	}
	if closeErr.Text != duplicateRelayConnectionReason {
		t.Fatalf("close reason = %q, want %q", closeErr.Text, duplicateRelayConnectionReason)
	}
}

func TestDeviceConnectedClosesPreviousConnectionWithReason(t *testing.T) {
	serverConn, clientConn, cleanup := newTestWebsocketPair(t)
	defer cleanup()

	device := NewDevice("device-1", nil, DeviceManagerConfig{})
	device.DeviceConnected(NewConnection(serverConn))
	device.DeviceConnected(&Connection{})

	_, _, err := clientConn.ReadMessage()
	closeErr := assertCloseError(t, err)
	if closeErr.Code != gws.ClosePolicyViolation {
		t.Fatalf("close code = %d, want %d", closeErr.Code, gws.ClosePolicyViolation)
	}
	if closeErr.Text != duplicateRelayConnectionReason {
		t.Fatalf("close reason = %q, want %q", closeErr.Text, duplicateRelayConnectionReason)
	}
}

func TestServerAcceptConnSendsCloudSourceFullState(t *testing.T) {
	manager := NewDeviceManager(DeviceManagerConfig{
		Stores: func(string) ([]restream.Store, error) {
			return []restream.Store{
				restream.NewCloudSourceForDeviceStore[testState, *testState, *testPartial](
					"CloudSourceStore",
					&testState{Value: "cloud"},
					restream.AccessLevelPublic,
				),
			}, nil
		},
	})
	serverConn, clientConn, cleanup := newTestWebsocketPair(t)
	defer cleanup()

	relayServer := New(Config{
		DeviceManager: manager,
		AuthenticateDevice: func(context.Context, *protocol.DeviceHello, *Connection) (restream.AccessLevel, error) {
			return restream.AccessLevel(1), nil
		},
	})
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- relayServer.AcceptConn(context.Background(), serverConn)
	}()

	helloBytes, err := protocol.EncodeDeviceHello(&protocol.DeviceHello{
		ProtocolVersion: protocol.CurrentVersion,
		DeviceID:        "device-1",
	})
	if err != nil {
		t.Fatalf("EncodeDeviceHello failed: %v", err)
	}
	if err := clientConn.WriteMessage(gws.BinaryMessage, helloBytes); err != nil {
		t.Fatalf("Write hello failed: %v", err)
	}
	if _, _, err := clientConn.ReadMessage(); err != nil {
		t.Fatalf("Read connected failed: %v", err)
	}
	_, fullStateBytes, err := clientConn.ReadMessage()
	if err != nil {
		t.Fatalf("Read cloud-source full state failed: %v", err)
	}
	packet, ok := mustDecodePacket(t, fullStateBytes).(*protocol.StoreStatePacket)
	if !ok {
		t.Fatalf("cloud-source packet type = %T, want *StoreStatePacket", mustDecodePacket(t, fullStateBytes))
	}
	if packet.StoreName != "CloudSourceStore" || packet.Kind() != protocol.KindFullState {
		t.Fatalf("cloud-source packet = %+v, want full state for CloudSourceStore", packet)
	}
	var state testState
	if err := state.Deserialize(binarystreams.NewReaderFromBytes(packet.Data), nil); err != nil {
		t.Fatalf("Deserialize cloud-source full state failed: %v", err)
	}
	if state.Value != "cloud" {
		t.Fatalf("cloud-source full state value = %q, want cloud", state.Value)
	}

	clientConn.Close() //nolint:errcheck
	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("AcceptConn did not return after client close")
	}
}

func TestServerAcceptConnReplaysActiveStoreSubscriptions(t *testing.T) {
	manager := NewDeviceManager(DeviceManagerConfig{
		Stores: func(string) ([]restream.Store, error) {
			return []restream.Store{
				restream.NewRelayStore[testState, *testState, *testPartial]("TestStore", &testState{}, restream.AccessLevelPublic),
			}, nil
		},
	})
	device, err := manager.GetDevice("device-1")
	if err != nil {
		t.Fatalf("GetDevice failed: %v", err)
	}
	if err := device.StoreRegistry.ListeningToStoreKey("TestStore", "values%&a", restream.AccessLevelPublic); err != nil {
		t.Fatalf("ListeningToStoreKey failed: %v", err)
	}

	serverConn, clientConn, cleanup := newTestWebsocketPair(t)
	defer cleanup()

	relayServer := New(Config{
		DeviceManager: manager,
		AuthenticateDevice: func(context.Context, *protocol.DeviceHello, *Connection) (restream.AccessLevel, error) {
			return restream.AccessLevel(1), nil
		},
	})

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- relayServer.AcceptConn(context.Background(), serverConn)
	}()

	helloBytes, err := protocol.EncodeDeviceHello(&protocol.DeviceHello{
		ProtocolVersion: protocol.CurrentVersion,
		DeviceID:        "device-1",
	})
	if err != nil {
		t.Fatalf("EncodeDeviceHello failed: %v", err)
	}
	if err := clientConn.WriteMessage(gws.BinaryMessage, helloBytes); err != nil {
		t.Fatalf("Write hello failed: %v", err)
	}

	_, connectedBytes, err := clientConn.ReadMessage()
	if err != nil {
		t.Fatalf("Read connected failed: %v", err)
	}
	if _, ok := mustDecodePacket(t, connectedBytes).(*protocol.ConnectedPacket); !ok {
		t.Fatalf("first packet type = %T, want *ConnectedPacket", mustDecodePacket(t, connectedBytes))
	}

	_, subscriptionBytes, err := clientConn.ReadMessage()
	if err != nil {
		t.Fatalf("Read replayed subscription failed: %v", err)
	}
	subscriptionPacket, ok := mustDecodePacket(t, subscriptionBytes).(*protocol.StoreSubscriptionPacket)
	if !ok {
		t.Fatalf("replayed packet type = %T, want *StoreSubscriptionPacket", mustDecodePacket(t, subscriptionBytes))
	}
	if subscriptionPacket.StoreName != "TestStore" ||
		subscriptionPacket.Key != "values%&a" ||
		subscriptionPacket.Action != protocol.StoreSubscribe {
		t.Fatalf("replayed subscription = %+v, want TestStore values%%&a subscribe", subscriptionPacket)
	}

	clientConn.Close() //nolint:errcheck // Why: End server read loop.
	select {
	case err := <-serverDone:
		if err == nil {
			t.Fatal("AcceptConn returned nil, want read close error")
		}
	case <-time.After(time.Second):
		t.Fatal("AcceptConn did not return after client close")
	}
}

func TestServerAcceptConnSendsCloseReasonOnAuthenticationError(t *testing.T) {
	serverConn, clientConn, cleanup := newTestWebsocketPair(t)
	defer cleanup()

	authErr := errors.New("invalid relay credentials")
	relayServer := New(Config{
		DeviceManager: NewDeviceManager(DeviceManagerConfig{}),
		AuthenticateDevice: func(context.Context, *protocol.DeviceHello, *Connection) (restream.AccessLevel, error) {
			return restream.AccessLevel(0), authErr
		},
	})

	serverDone := make(chan error, 1)
	go func() {
		serverDone <- relayServer.AcceptConn(context.Background(), serverConn)
	}()

	helloBytes, err := protocol.EncodeDeviceHello(&protocol.DeviceHello{
		ProtocolVersion: protocol.CurrentVersion,
		DeviceID:        "device-1",
	})
	if err != nil {
		t.Fatalf("EncodeDeviceHello failed: %v", err)
	}
	if err := clientConn.WriteMessage(gws.BinaryMessage, helloBytes); err != nil {
		t.Fatalf("Write hello failed: %v", err)
	}

	_, _, err = clientConn.ReadMessage()
	closeErr := assertCloseError(t, err)
	if closeErr.Code != gws.CloseInternalServerErr {
		t.Fatalf("close code = %d, want %d", closeErr.Code, gws.CloseInternalServerErr)
	}
	if closeErr.Text != authErr.Error() {
		t.Fatalf("close reason = %q, want %q", closeErr.Text, authErr.Error())
	}

	select {
	case err := <-serverDone:
		if !errors.Is(err, authErr) {
			t.Fatalf("AcceptConn error = %v, want %v", err, authErr)
		}
	case <-time.After(time.Second):
		t.Fatal("AcceptConn did not return after authentication error")
	}
}

func TestCloudStoreSubscriptionForwardsToConnectedDevice(t *testing.T) {
	manager := NewDeviceManager(DeviceManagerConfig{
		Stores: func(string) ([]restream.Store, error) {
			return []restream.Store{
				restream.NewRelayStore[testState, *testState, *testPartial]("TestStore", &testState{}, restream.AccessLevelPublic),
			}, nil
		},
	})
	device, err := manager.GetDevice("device-1")
	if err != nil {
		t.Fatalf("GetDevice failed: %v", err)
	}

	serverConn, clientConn, cleanup := newTestWebsocketPair(t)
	defer cleanup()

	device.DeviceConnected(NewConnection(serverConn))
	if err := device.StoreRegistry.ListeningToStoreKey("TestStore", "values%&a", restream.AccessLevelPublic); err != nil {
		t.Fatalf("ListeningToStoreKey failed: %v", err)
	}

	_, subscriptionBytes, err := clientConn.ReadMessage()
	if err != nil {
		t.Fatalf("Read forwarded subscription failed: %v", err)
	}
	subscriptionPacket, ok := mustDecodePacket(t, subscriptionBytes).(*protocol.StoreSubscriptionPacket)
	if !ok {
		t.Fatalf("forwarded packet type = %T, want *StoreSubscriptionPacket", mustDecodePacket(t, subscriptionBytes))
	}
	if subscriptionPacket.StoreName != "TestStore" ||
		subscriptionPacket.Key != "values%&a" ||
		subscriptionPacket.Action != protocol.StoreSubscribe {
		t.Fatalf("forwarded subscription = %+v, want TestStore values%%&a subscribe", subscriptionPacket)
	}

	if err := device.StoreRegistry.StopListeningToStoreKey("TestStore", "values%&a"); err != nil {
		t.Fatalf("StopListeningToStoreKey failed: %v", err)
	}
	_, subscriptionBytes, err = clientConn.ReadMessage()
	if err != nil {
		t.Fatalf("Read forwarded unsubscribe failed: %v", err)
	}
	subscriptionPacket, ok = mustDecodePacket(t, subscriptionBytes).(*protocol.StoreSubscriptionPacket)
	if !ok {
		t.Fatalf("forwarded packet type = %T, want *StoreSubscriptionPacket", mustDecodePacket(t, subscriptionBytes))
	}
	if subscriptionPacket.StoreName != "TestStore" ||
		subscriptionPacket.Key != "values%&a" ||
		subscriptionPacket.Action != protocol.StoreUnsubscribe {
		t.Fatalf("forwarded subscription = %+v, want TestStore values%%&a unsubscribe", subscriptionPacket)
	}
}

func TestCustomCloudImplementationSubscriptionForwardsToConnectedDevice(t *testing.T) {
	manager := NewDeviceManager(DeviceManagerConfig{
		Stores: func(string) ([]restream.Store, error) {
			return []restream.Store{newCustomServerRelayStore("CustomStore")}, nil
		},
	})
	device, err := manager.GetDevice("device-1")
	if err != nil {
		t.Fatalf("GetDevice failed: %v", err)
	}

	serverConn, clientConn, cleanup := newTestWebsocketPair(t)
	defer cleanup()
	device.DeviceConnected(NewConnection(serverConn))

	if err := device.StoreRegistry.ListeningToStoreKey(
		"CustomStore",
		"pendingNotifications",
		restream.AccessLevelPublic,
	); err != nil {
		t.Fatalf("ListeningToStoreKey failed: %v", err)
	}
	_, subscriptionBytes, err := clientConn.ReadMessage()
	if err != nil {
		t.Fatalf("Read forwarded subscription failed: %v", err)
	}
	packet, ok := mustDecodePacket(t, subscriptionBytes).(*protocol.StoreSubscriptionPacket)
	if !ok {
		t.Fatalf("forwarded packet type = %T, want *StoreSubscriptionPacket", mustDecodePacket(t, subscriptionBytes))
	}
	if packet.StoreName != "CustomStore" || packet.Key != "pendingNotifications" ||
		packet.Action != protocol.StoreSubscribe {
		t.Fatalf("forwarded subscription = %+v", packet)
	}
}

func TestCloudWholeStoreSubscriptionForwardsToConnectedDevice(t *testing.T) {
	manager := NewDeviceManager(DeviceManagerConfig{
		Stores: func(string) ([]restream.Store, error) {
			return []restream.Store{
				restream.NewRelayStore[testState, *testState, *testPartial]("TestStore", &testState{}, restream.AccessLevelPublic),
			}, nil
		},
	})
	device, err := manager.GetDevice("device-1")
	if err != nil {
		t.Fatalf("GetDevice failed: %v", err)
	}

	serverConn, clientConn, cleanup := newTestWebsocketPair(t)
	defer cleanup()

	device.DeviceConnected(NewConnection(serverConn))
	if err := device.StoreRegistry.ListeningToStore("TestStore", restream.AccessLevelPublic); err != nil {
		t.Fatalf("ListeningToStore failed: %v", err)
	}

	_, subscriptionBytes, err := clientConn.ReadMessage()
	if err != nil {
		t.Fatalf("Read forwarded subscription failed: %v", err)
	}
	subscriptionPacket, ok := mustDecodePacket(t, subscriptionBytes).(*protocol.StoreSubscriptionPacket)
	if !ok {
		t.Fatalf("forwarded packet type = %T, want *StoreSubscriptionPacket", mustDecodePacket(t, subscriptionBytes))
	}
	if subscriptionPacket.StoreName != "TestStore" ||
		subscriptionPacket.Key != "" ||
		subscriptionPacket.Action != protocol.StoreSubscribe {
		t.Fatalf("forwarded subscription = %+v, want TestStore empty-key subscribe", subscriptionPacket)
	}
}

func TestCloudSourcePartialForwardsToConnectedDevice(t *testing.T) {
	manager := NewDeviceManager(DeviceManagerConfig{
		Stores: func(string) ([]restream.Store, error) {
			return []restream.Store{
				restream.NewCloudSourceForDeviceStore[testState, *testState, *testPartial](
					"CloudSourceStore",
					&testState{},
					restream.AccessLevelPublic,
				),
			}, nil
		},
	})
	device, err := manager.GetDevice("device-1")
	if err != nil {
		t.Fatalf("GetDevice failed: %v", err)
	}

	serverConn, clientConn, cleanup := newTestWebsocketPair(t)
	defer cleanup()

	device.DeviceConnected(NewConnection(serverConn))
	value := "friend"
	partialBytes, err := restream.SerializeToBytes(&testPartial{Value: &value}, nil)
	if err != nil {
		t.Fatalf("SerializeToBytes failed: %v", err)
	}
	if err := device.StoreRegistry.ApplyPartialToStore("CloudSourceStore", partialBytes); err != nil {
		t.Fatalf("ApplyPartialToStore failed: %v", err)
	}

	_, forwardedBytes, err := clientConn.ReadMessage()
	if err != nil {
		t.Fatalf("Read forwarded partial failed: %v", err)
	}
	packet, ok := mustDecodePacket(t, forwardedBytes).(*protocol.StoreStatePacket)
	if !ok {
		t.Fatalf("forwarded packet type = %T, want *StoreStatePacket", mustDecodePacket(t, forwardedBytes))
	}
	if packet.StoreName != "CloudSourceStore" || packet.Kind() != protocol.KindPartialState {
		t.Fatalf("forwarded packet = %+v, want partial state for CloudSourceStore", packet)
	}
	var partial testPartial
	if err := partial.Deserialize(binarystreams.NewReaderFromBytes(packet.Data), nil); err != nil {
		t.Fatalf("Deserialize forwarded partial failed: %v", err)
	}
	if partial.Value == nil || *partial.Value != "friend" {
		t.Fatalf("forwarded partial value = %v, want friend", partial.Value)
	}
}

type testDeviceTracker struct {
	connected    bool
	disconnected bool
	accessLevel  restream.AccessLevel
	fullStore    string
	fullData     []byte
	eventName    string
	eventData    []byte
}

func mustDecodePacket(t *testing.T, b []byte) protocol.Packet {
	t.Helper()

	packet, err := protocol.DecodePacket(b)
	if err != nil {
		t.Fatalf("DecodePacket failed: %v", err)
	}
	return packet
}

func assertCloseError(t *testing.T, err error) *gws.CloseError {
	t.Helper()

	if err == nil {
		t.Fatal("ReadMessage returned nil, want close error")
	}
	var closeErr *gws.CloseError
	if !errors.As(err, &closeErr) {
		t.Fatalf("ReadMessage error = %v, want websocket close error", err)
	}
	return closeErr
}

func newTestWebsocketPair(t *testing.T) (*gws.Conn, *gws.Conn, func()) {
	t.Helper()

	serverConnCh := make(chan *gws.Conn, 1)
	serverErrCh := make(chan error, 1)
	upgrader := gws.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			serverErrCh <- err
			return
		}
		serverConnCh <- conn
	}))

	clientURL := "ws" + strings.TrimPrefix(server.URL, "http")
	clientConn, resp, err := gws.DefaultDialer.Dial(clientURL, nil)
	if resp != nil && resp.Body != nil {
		resp.Body.Close() //nolint:errcheck // Why: Response body is only for handshake diagnostics.
	}
	if err != nil {
		server.Close()
		t.Fatalf("Dial test websocket failed: %v", err)
	}

	var serverConn *gws.Conn
	select {
	case serverConn = <-serverConnCh:
	case err := <-serverErrCh:
		clientConn.Close() //nolint:errcheck
		server.Close()
		t.Fatalf("Upgrade test websocket failed: %v", err)
	case <-time.After(time.Second):
		clientConn.Close() //nolint:errcheck
		server.Close()
		t.Fatal("timed out waiting for test websocket upgrade")
	}

	cleanup := func() {
		serverConn.Close() //nolint:errcheck
		clientConn.Close() //nolint:errcheck
		server.Close()
	}
	return serverConn, clientConn, cleanup
}
