package client

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

func mustBuildOutboundPacket(t *testing.T, packet outboundPacket) []byte {
	t.Helper()
	packetBytes, err := packet.buildBytes()
	if err != nil {
		t.Fatalf("build outbound packet failed: %v", err)
	}
	return packetBytes
}

func TestStorePolicyAllows(t *testing.T) {
	policy := StorePolicy{
		Include: map[string]struct{}{
			"IncludedStore": {},
		},
		Exclude: map[string]struct{}{
			"ExcludedStore": {},
		},
	}

	if !policy.Allows("IncludedStore") {
		t.Fatal("IncludedStore should be allowed")
	}
	if policy.Allows("OtherStore") {
		t.Fatal("OtherStore should not be allowed when Include is set")
	}
	if policy.Allows("ExcludedStore") {
		t.Fatal("ExcludedStore should not be allowed")
	}
}

func TestStorePolicyDebounceFor(t *testing.T) {
	policy := StorePolicy{
		DefaultDebounce: 500 * time.Millisecond,
		Debounce: map[string]time.Duration{
			"CustomStore":    time.Second,
			"ImmediateStore": 0,
		},
	}

	tests := []struct {
		name      string
		storeName string
		want      time.Duration
		wantOK    bool
	}{
		{name: "default", storeName: "DefaultStore", want: 500 * time.Millisecond, wantOK: true},
		{name: "store override", storeName: "CustomStore", want: time.Second, wantOK: true},
		{name: "explicitly disabled", storeName: "ImmediateStore", want: 0, wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := policy.DebounceFor(tt.storeName)
			if got != tt.want || ok != tt.wantOK {
				t.Fatalf("DebounceFor(%q) = (%s, %t), want (%s, %t)", tt.storeName, got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestStorePolicyDebounceForWithoutDefault(t *testing.T) {
	got, ok := (StorePolicy{}).DebounceFor("Store")
	if got != 0 || ok {
		t.Fatalf("DebounceFor() = (%s, %t), want (0s, false)", got, ok)
	}
}

func TestStaticConfigProvidesEndpointAndCredentials(t *testing.T) {
	s := NewStreamer(nil, nil, nil, Config{
		Endpoint: "wss://relay.example/device",
		Credentials: Credentials{
			DeviceID: "device-1",
			AuthType: "shared-key",
			AuthData: []byte("secret"),
		},
	})

	if s.opts.Endpoint != "wss://relay.example/device" {
		t.Fatalf("endpoint = %q, want wss://relay.example/device", s.opts.Endpoint)
	}

	credentials := s.opts.Credentials
	if credentials.DeviceID != "device-1" {
		t.Fatalf("DeviceID = %q, want device-1", credentials.DeviceID)
	}
	if credentials.AuthType != "shared-key" {
		t.Fatalf("AuthType = %q, want shared-key", credentials.AuthType)
	}
	if string(credentials.AuthData) != "secret" {
		t.Fatalf("AuthData = %q, want secret", credentials.AuthData)
	}
}

func TestRunRequiresEndpoint(t *testing.T) {
	s := NewStreamer(nil, nil, nil, Config{})

	err := s.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "endpoint") {
		t.Fatalf("Run error = %v, want endpoint error", err)
	}
}

func TestHandleConnWrapsRelayRPCPacketError(t *testing.T) {
	serverConn, clientConn, cleanup := newTestWebsocketPair(t)
	defer cleanup()

	s := NewStreamer(nil, func(string, restream.AccessLevel, []byte) ([]byte, bool, error) {
		return nil, true, errors.New(`deserialize field "Enabled" (fieldID=4): unhandled deserialized bool val in DeserializeBool: 4`)
	}, nil, Config{})
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.handleConn(context.Background(), clientConn, Credentials{
			DeviceID: "device-1",
			AuthType: "shared-key",
			AuthData: []byte("secret"),
		})
	}()

	if _, _, err := serverConn.ReadMessage(); err != nil {
		t.Fatalf("Read hello failed: %v", err)
	}
	connectedBytes, err := protocol.EncodePacket(&protocol.ConnectedPacket{ProtocolVersion: protocol.CurrentVersion})
	if err != nil {
		t.Fatalf("Encode connected failed: %v", err)
	}
	if err := serverConn.WriteMessage(gws.BinaryMessage, connectedBytes); err != nil {
		t.Fatalf("Write connected failed: %v", err)
	}
	rpcBytes, err := protocol.EncodePacket(&protocol.RPCCallPacket{
		RPCID:       12,
		MethodName:  "Security.SetEnabled",
		AccessLevel: byte(restream.AccessLevelPublic),
		Request:     []byte{4},
	})
	if err != nil {
		t.Fatalf("Encode RPC call failed: %v", err)
	}
	if err := serverConn.WriteMessage(gws.BinaryMessage, rpcBytes); err != nil {
		t.Fatalf("Write RPC call failed: %v", err)
	}

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("handleConn error = nil, want packet context error")
		}
		for _, want := range []string{
			`handle relay RPC call packet "Security.SetEnabled"`,
			"1 bytes",
			"fieldID=4",
			"DeserializeBool: 4",
		} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("handleConn error = %q, missing %q", err, want)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("handleConn did not return")
	}
}

func TestEnqueuePacketAfterDisconnectDoesNotPanic(t *testing.T) {
	s := &Streamer{}

	if err := s.enqueuePacket("test packet", []byte{1}); !errors.Is(err, ErrDisconnected) {
		t.Fatalf("enqueuePacket error = %v, want %v", err, ErrDisconnected)
	}
}

func TestEnqueuePacketFullQueueDoesNotBlock(t *testing.T) {
	done := make(chan struct{})
	sendQueue := make(chan outboundPacket, 1)
	sendQueue <- outboundPacket{bytes: []byte{1}}

	var queueFullInfo SendQueueFullInfo
	s := &Streamer{
		conn:      &gws.Conn{},
		sendQueue: sendQueue,
		sendDone:  done,
		opts: Config{
			Callbacks: Callbacks{
				OnSendQueueFull: func(info SendQueueFullInfo) {
					queueFullInfo = info
				},
			},
		},
	}

	if err := s.enqueuePacket("test packet", []byte{2}); !errors.Is(err, ErrSendQueueFull) {
		t.Fatalf("enqueuePacket error = %v, want %v", err, ErrSendQueueFull)
	}
	if queueFullInfo.PacketDescription != "test packet" {
		t.Fatalf("packet description = %q, want test packet", queueFullInfo.PacketDescription)
	}
	if queueFullInfo.QueueDepth != 1 || queueFullInfo.QueueCapacity != 1 {
		t.Fatalf("queue info = %+v, want depth/capacity 1/1", queueFullInfo)
	}
}

func TestSendEventWritesGenericEventPacket(t *testing.T) {
	done := make(chan struct{})
	sendQueue := make(chan outboundPacket, 1)

	s := &Streamer{
		conn:      &gws.Conn{},
		sendQueue: sendQueue,
		sendDone:  done,
	}

	if err := s.SendEvent("TrackedPointUpdate", []byte{1, 2, 3}); err != nil {
		t.Fatalf("SendEvent failed: %v", err)
	}

	packetBytes := mustBuildOutboundPacket(t, <-sendQueue)
	packet, err := protocol.DecodePacket(packetBytes)
	if err != nil {
		t.Fatalf("DecodePacket failed: %v", err)
	}
	eventPacket, ok := packet.(*protocol.EventPacket)
	if !ok {
		t.Fatalf("packet type = %T, want *protocol.EventPacket", packet)
	}
	if eventPacket.EventName != "TrackedPointUpdate" {
		t.Fatalf("event name = %q, want TrackedPointUpdate", eventPacket.EventName)
	}
	if string(eventPacket.Data) != string([]byte{1, 2, 3}) {
		t.Fatalf("event bytes = %v, want [1 2 3]", eventPacket.Data)
	}
}

func TestSendFullStateUsesStoreMinimumAccess(t *testing.T) {
	store := restream.NewRelayStore[streamerTestState, *streamerTestState, *streamerTestPartial](
		"TestStore",
		&streamerTestState{},
		restream.AccessLevel(2),
	)
	registry, err := restream.NewStoreRegistry([]restream.Store{store})
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}

	done := make(chan struct{})
	sendQueue := make(chan outboundPacket, 1)
	s := &Streamer{
		sr:        registry,
		conn:      &gws.Conn{},
		sendQueue: sendQueue,
		sendDone:  done,
	}

	if err := s.sendFullState("TestStore"); err != nil {
		t.Fatalf("sendFullState failed: %v", err)
	}

	outbound := <-sendQueue
	if outbound.storeName != "TestStore" || outbound.packetKind != protocol.KindFullState {
		t.Fatalf("outbound store metadata = %q/%d, want TestStore/%d",
			outbound.storeName, outbound.packetKind, protocol.KindFullState)
	}
	packetBytes := mustBuildOutboundPacket(t, outbound)
	packet, err := protocol.DecodePacket(packetBytes)
	if err != nil {
		t.Fatalf("DecodePacket failed: %v", err)
	}
	storePacket, ok := packet.(*protocol.StoreStatePacket)
	if !ok {
		t.Fatalf("packet type = %T, want *protocol.StoreStatePacket", packet)
	}
	if storePacket.StoreName != "TestStore" || storePacket.Kind() != protocol.KindFullState {
		t.Fatalf("store packet = %+v, want full state for TestStore", storePacket)
	}
}

func TestStorePacketSentCallbackRunsAfterSuccessfulWrite(t *testing.T) {
	serverConn, clientConn, cleanup := newTestWebsocketPair(t)
	defer cleanup()

	sent := make(chan StorePacketSentInfo, 1)
	s := &Streamer{opts: Config{
		WriteTimeout: time.Second,
		Callbacks: Callbacks{
			OnStorePacketSent: func(info StorePacketSentInfo) {
				sent <- info
			},
		},
	}}
	sendQueue := make(chan outboundPacket, 1)
	sendDone := make(chan struct{})
	s.handleSendQueue(clientConn, sendQueue, sendDone)

	packetBytes, err := protocol.EncodePacket(protocol.NewPartialStatePacket("TestStore", []byte{1, 2, 3}))
	if err != nil {
		t.Fatalf("EncodePacket failed: %v", err)
	}
	sendQueue <- outboundPacket{
		description: "partial state TestStore",
		storeName:   "TestStore",
		packetKind:  protocol.KindPartialState,
		bytes:       packetBytes,
	}

	_, receivedBytes, err := serverConn.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage failed: %v", err)
	}
	if string(receivedBytes) != string(packetBytes) {
		t.Fatalf("received bytes = %v, want %v", receivedBytes, packetBytes)
	}

	select {
	case info := <-sent:
		if info.StoreName != "TestStore" || info.PacketKind != protocol.KindPartialState || info.Bytes != len(packetBytes) {
			t.Fatalf("sent info = %+v, want TestStore partial state with %d bytes", info, len(packetBytes))
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for store packet sent callback")
	}
	close(sendDone)
}

func TestSendFullStateDefersSerializationUntilOutboundPacketBuild(t *testing.T) {
	serializeCount := 0
	store := restream.NewRelayStore[streamerTestState, *streamerTestState, *streamerTestPartial](
		"TestStore",
		&streamerTestState{
			onSerialize: func() {
				serializeCount++
			},
		},
		restream.AccessLevelPublic,
	)
	registry, err := restream.NewStoreRegistry([]restream.Store{store})
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}

	done := make(chan struct{})
	sendQueue := make(chan outboundPacket, 1)
	s := &Streamer{
		sr:        registry,
		conn:      &gws.Conn{},
		sendQueue: sendQueue,
		sendDone:  done,
	}

	if err := s.sendFullState("TestStore"); err != nil {
		t.Fatalf("sendFullState failed: %v", err)
	}
	if serializeCount != 0 {
		t.Fatalf("sendFullState serialized before enqueue: %d", serializeCount)
	}

	_ = mustBuildOutboundPacket(t, <-sendQueue)
	if serializeCount != 1 {
		t.Fatalf("outbound packet build serialized %d times, want 1", serializeCount)
	}
}

func TestSendPartialSnapshotsSerializedBytesBeforeEnqueue(t *testing.T) {
	serializeCount := 0
	value := "queued"
	done := make(chan struct{})
	sendQueue := make(chan outboundPacket, 1)
	s := &Streamer{
		conn:      &gws.Conn{},
		sendQueue: sendQueue,
		sendDone:  done,
	}

	if err := s.sendPartial("TestStore", &streamerTestPartial{
		Value: &value,
		onSerialize: func() {
			serializeCount++
		},
	}); err != nil {
		t.Fatalf("sendPartial failed: %v", err)
	}
	if serializeCount != 1 {
		t.Fatalf("sendPartial serialized %d times before enqueue, want 1", serializeCount)
	}
	value = "mutated after enqueue"

	packetBytes := mustBuildOutboundPacket(t, <-sendQueue)
	if serializeCount != 1 {
		t.Fatalf("outbound packet build reserialized partial; count = %d, want 1", serializeCount)
	}
	packet, err := protocol.DecodePacket(packetBytes)
	if err != nil {
		t.Fatalf("DecodePacket failed: %v", err)
	}
	storePacket, ok := packet.(*protocol.StoreStatePacket)
	if !ok {
		t.Fatalf("packet type = %T, want *protocol.StoreStatePacket", packet)
	}
	if storePacket.StoreName != "TestStore" || storePacket.Kind() != protocol.KindPartialState {
		t.Fatalf("store packet = %+v, want partial state for TestStore", storePacket)
	}
	var decoded streamerTestPartial
	if err := decoded.Deserialize(binarystreams.NewReaderFromBytes(storePacket.Data), nil); err != nil {
		t.Fatalf("partial deserialize failed: %v", err)
	}
	if decoded.Value == nil || *decoded.Value != "queued" {
		t.Fatalf("queued partial value = %v, want queued", decoded.Value)
	}
}

func TestGatherPartialDetachesRetainedValues(t *testing.T) {
	values := map[string]string{"status": "gathered"}
	s := NewStreamer(nil, nil, nil, Config{})
	if err := s.gatherPartial("TestStore", &streamerMapPartial{Values: values}, time.Hour, 1); err != nil {
		t.Fatalf("gatherPartial failed: %v", err)
	}

	values["status"] = "mutated after gather"
	gathered := s.gatheredPartials["TestStore"].(*streamerMapPartial)
	if gathered.Values["status"] != "gathered" {
		t.Fatalf("gathered partial value = %q, want gathered", gathered.Values["status"])
	}
	s.clearGatheredPartials()
}

func TestStreamerStoreTypesFilterRelayTraffic(t *testing.T) {
	relayStore := newStreamerTypedRelayStore("RelayStore", restream.StoreTypeDeviceWithRelay)
	noRelayStore := newStreamerTypedRelayStore("NoRelayStore", restream.StoreTypeDeviceWithNoRelay)
	cloudImplStore := newStreamerTypedRelayStore("CloudImplStore", restream.StoreTypeDeviceWithCloudImpl)
	deviceAndCloudStore := newStreamerTypedRelayStore("DeviceAndCloudStore", restream.StoreTypeDeviceAndCloud)
	cloudSourceStore := newStreamerTypedRelayStore("CloudSourceStore", restream.StoreTypeDeviceWithCloudSource)
	cloudImplOfDeviceStore := newStreamerTypedRelayStore("CloudImplOfDeviceStore", restream.StoreTypeCloudImplOfDevice)
	cloudOnlyStore := newStreamerTypedRelayStore("CloudOnlyStore", restream.StoreTypeCloudOnly)
	registry, err := restream.NewStoreRegistry([]restream.Store{
		relayStore,
		noRelayStore,
		cloudImplStore,
		deviceAndCloudStore,
		cloudSourceStore,
		cloudImplOfDeviceStore,
		cloudOnlyStore,
	})
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}

	done := make(chan struct{})
	sendQueue := make(chan outboundPacket, 4)
	s := &Streamer{
		sr:        registry,
		conn:      &gws.Conn{},
		sendQueue: sendQueue,
		sendDone:  done,
	}

	if err := s.sendFullStates(); err != nil {
		t.Fatalf("sendFullStates failed: %v", err)
	}
	fullStateStores := map[string]struct{}{}
	for len(sendQueue) > 0 {
		packetBytes := mustBuildOutboundPacket(t, <-sendQueue)
		packet, err := protocol.DecodePacket(packetBytes)
		if err != nil {
			t.Fatalf("DecodePacket failed: %v", err)
		}
		storePacket, ok := packet.(*protocol.StoreStatePacket)
		if !ok {
			t.Fatalf("packet type = %T, want *protocol.StoreStatePacket", packet)
		}
		if storePacket.Kind() != protocol.KindFullState {
			t.Fatalf("store packet kind = %v, want full state", storePacket.Kind())
		}
		fullStateStores[storePacket.StoreName] = struct{}{}
	}
	for _, unexpected := range []string{"NoRelayStore", "DeviceAndCloudStore", "CloudSourceStore", "CloudImplOfDeviceStore", "CloudOnlyStore"} {
		if _, ok := fullStateStores[unexpected]; ok {
			t.Fatalf("sendFullStates sent %s; want skipped", unexpected)
		}
	}
	for _, expected := range []string{"RelayStore", "CloudImplStore"} {
		if _, ok := fullStateStores[expected]; !ok {
			t.Fatalf("sendFullStates skipped %s; got %+v", expected, fullStateStores)
		}
	}

	s.partialCallback("NoRelayStore", nil, &streamerTestPartial{})
	s.partialCallback("DeviceAndCloudStore", nil, &streamerTestPartial{})
	s.partialCallback("CloudSourceStore", nil, &streamerTestPartial{})
	s.partialCallback("CloudImplOfDeviceStore", nil, &streamerTestPartial{})
	s.partialCallback("CloudOnlyStore", nil, &streamerTestPartial{})
	if len(sendQueue) != 0 {
		t.Fatalf("partialCallback queued %d packets for non-streaming stores", len(sendQueue))
	}

	s.partialCallback("CloudImplStore", nil, &streamerTestPartial{})
	if len(sendQueue) != 1 {
		t.Fatalf("partialCallback queued %d packets for streaming store, want 1", len(sendQueue))
	}
	packet, err := protocol.DecodePacket(mustBuildOutboundPacket(t, <-sendQueue))
	if err != nil {
		t.Fatalf("DecodePacket failed: %v", err)
	}
	storePacket, ok := packet.(*protocol.StoreStatePacket)
	if !ok {
		t.Fatalf("packet type = %T, want *protocol.StoreStatePacket", packet)
	}
	if storePacket.StoreName != "CloudImplStore" || storePacket.Kind() != protocol.KindPartialState {
		t.Fatalf("store packet = %+v, want partial state for CloudImplStore", storePacket)
	}
}

func TestStreamerAppliesInboundRelayStateOnlyForCloudSourceStores(t *testing.T) {
	stores := []restream.Store{
		newStreamerTypedRelayStore("RelayStore", restream.StoreTypeDeviceWithRelay),
		newStreamerTypedRelayStore("NoRelayStore", restream.StoreTypeDeviceWithNoRelay),
		newStreamerTypedRelayStore("CloudImplStore", restream.StoreTypeDeviceWithCloudImpl),
		newStreamerTypedRelayStore("DeviceAndCloudStore", restream.StoreTypeDeviceAndCloud),
		newStreamerTypedRelayStore("CloudSourceStore", restream.StoreTypeDeviceWithCloudSource),
		newStreamerTypedRelayStore("CloudImplOfDeviceStore", restream.StoreTypeCloudImplOfDevice),
		newStreamerTypedRelayStore("CloudSourceForDeviceStore", restream.StoreTypeCloudSourceForDevice),
		newStreamerTypedRelayStore("CloudOnlyStore", restream.StoreTypeCloudOnly),
	}
	registry, err := restream.NewStoreRegistry(stores)
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}
	s := NewStreamer(registry, nil, nil, Config{})
	defer s.Close() //nolint:errcheck

	applied := map[string]int{}
	registry.SubscribeToPartialApplies(func(storeName string, _ [][]any, _ restream.Partial) {
		applied[storeName]++
	})

	fullStateBytes, err := restream.SerializeToBytes(&streamerTestState{Value: "cloud full"}, nil)
	if err != nil {
		t.Fatalf("SerializeToBytes full state failed: %v", err)
	}
	partialValue := "cloud partial"
	partialBytes, err := restream.SerializeToBytes(&streamerTestPartial{Value: &partialValue}, nil)
	if err != nil {
		t.Fatalf("SerializeToBytes partial failed: %v", err)
	}

	for _, store := range stores {
		storeName := store.GetName()
		if err := s.handleStoreState(protocol.NewFullStatePacket(storeName, fullStateBytes)); err != nil {
			t.Fatalf("handleStoreState full for %s failed: %v", storeName, err)
		}
		if err := s.handleStoreState(protocol.NewPartialStatePacket(storeName, partialBytes)); err != nil {
			t.Fatalf("handleStoreState partial for %s failed: %v", storeName, err)
		}
	}

	for _, store := range stores {
		storeName := store.GetName()
		state := readStreamerTestStoreState(t, registry, storeName)
		if storeName == "CloudSourceStore" {
			if state.Value != "cloud partial" {
				t.Fatalf("%s value = %q, want cloud partial", storeName, state.Value)
			}
			if applied[storeName] != 1 {
				t.Fatalf("%s applied count = %d, want 1", storeName, applied[storeName])
			}
			continue
		}
		if state.Value != "" {
			t.Fatalf("%s value = %q, want unchanged", storeName, state.Value)
		}
		if applied[storeName] != 0 {
			t.Fatalf("%s applied count = %d, want 0", storeName, applied[storeName])
		}
	}
}

func TestStreamerStoreTypesFilterRelayedSubscriptions(t *testing.T) {
	noRelayStore := newStreamerTypedRelayStore("NoRelayStore", restream.StoreTypeDeviceWithNoRelay)
	registry, err := restream.NewStoreRegistry([]restream.Store{noRelayStore})
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}
	s := NewStreamer(registry, nil, nil, Config{})

	if err := s.handleStoreSubscription(&protocol.StoreSubscriptionPacket{
		StoreName: "NoRelayStore",
		Key:       "values%&a",
		Action:    protocol.StoreSubscribe,
	}); err != nil {
		t.Fatalf("handle subscribe failed: %v", err)
	}
	assertActiveRelayKeys(t, noRelayStore.RelayStore, nil)
}

func TestRelayedStoreSubscriptionsAreIdempotentAndCleanup(t *testing.T) {
	store := newStreamerTypedRelayStore("TestStore", restream.StoreTypeDeviceWithRelay)
	registry, err := restream.NewStoreRegistry([]restream.Store{store})
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}
	s := NewStreamer(registry, nil, nil, Config{})

	packet := &protocol.StoreSubscriptionPacket{
		StoreName: "TestStore",
		Key:       "values%&a",
		Action:    protocol.StoreSubscribe,
	}
	if err := s.handleStoreSubscription(packet); err != nil {
		t.Fatalf("handle subscribe failed: %v", err)
	}
	if err := s.handleStoreSubscription(packet); err != nil {
		t.Fatalf("handle duplicate subscribe failed: %v", err)
	}
	assertActiveRelayKeys(t, store.RelayStore, []string{"values%&a"})

	packet.Action = protocol.StoreUnsubscribe
	if err := s.handleStoreSubscription(packet); err != nil {
		t.Fatalf("handle final unsubscribe failed: %v", err)
	}
	assertActiveRelayKeys(t, store.RelayStore, nil)
	if err := s.handleStoreSubscription(packet); err != nil {
		t.Fatalf("handle duplicate unsubscribe failed: %v", err)
	}
	assertActiveRelayKeys(t, store.RelayStore, nil)

	packet.Action = protocol.StoreSubscribe
	if err := s.handleStoreSubscription(packet); err != nil {
		t.Fatalf("handle resubscribe failed: %v", err)
	}
	s.clearRelaySubscriptions()
	assertActiveRelayKeys(t, store.RelayStore, nil)
}

func TestRelayedWholeStoreSubscriptionUsesEmptyKey(t *testing.T) {
	store := newStreamerTypedRelayStore("TestStore", restream.StoreTypeDeviceWithRelay)
	registry, err := restream.NewStoreRegistry([]restream.Store{store})
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}
	s := NewStreamer(registry, nil, nil, Config{})

	if err := s.handleStoreSubscription(&protocol.StoreSubscriptionPacket{
		StoreName: "TestStore",
		Key:       "",
		Action:    protocol.StoreSubscribe,
	}); err != nil {
		t.Fatalf("handle whole-store subscribe failed: %v", err)
	}
	assertActiveRelayKeys(t, store.RelayStore, []string{""})

	if err := s.handleStoreSubscription(&protocol.StoreSubscriptionPacket{
		StoreName: "TestStore",
		Key:       "",
		Action:    protocol.StoreUnsubscribe,
	}); err != nil {
		t.Fatalf("handle whole-store unsubscribe failed: %v", err)
	}
	assertActiveRelayKeys(t, store.RelayStore, nil)
}

func TestOnDemandStoreStreamingRequiresPolicyAndRelayCapability(t *testing.T) {
	s := NewStreamer(nil, nil, nil, Config{StorePolicy: StorePolicy{OnDemand: true}})
	if s.configureOnDemandStoreStreaming(&protocol.ConnectedPacket{}) {
		t.Fatal("on-demand streaming enabled without relay capability")
	}
	if !s.configureOnDemandStoreStreaming(&protocol.ConnectedPacket{Capabilities: protocol.RelayCapabilities{
		OnDemandStoreStreaming: true,
	}}) {
		t.Fatal("on-demand streaming did not enable with policy and relay capability")
	}

	legacy := NewStreamer(nil, nil, nil, Config{})
	if legacy.configureOnDemandStoreStreaming(&protocol.ConnectedPacket{Capabilities: protocol.RelayCapabilities{
		OnDemandStoreStreaming: true,
	}}) {
		t.Fatal("on-demand streaming enabled without device policy opt-in")
	}
}

func TestOnDemandStoreStreamingSendsOnlyWhileStoreSubscribed(t *testing.T) {
	store := newStreamerTypedRelayStore("TestStore", restream.StoreTypeDeviceWithRelay)
	registry, err := restream.NewStoreRegistry([]restream.Store{store})
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}
	s, sendQueue := newConnectedOnDemandTestStreamer(registry, StorePolicy{})

	s.partialCallback("TestStore", nil, &streamerTestPartial{})
	if len(sendQueue) != 0 {
		t.Fatalf("unsubscribed partial queued %d packets", len(sendQueue))
	}

	if err := s.startRelayedStoreSubscription("TestStore", "values%&a"); err != nil {
		t.Fatalf("first subscription failed: %v", err)
	}
	assertQueuedStorePacketKind(t, sendQueue, protocol.KindFullState)

	s.partialCallback("TestStore", nil, &streamerTestPartial{})
	assertQueuedStorePacketKind(t, sendQueue, protocol.KindPartialState)

	if err := s.startRelayedStoreSubscription("TestStore", "values%&b"); err != nil {
		t.Fatalf("second subscription failed: %v", err)
	}
	if len(sendQueue) != 0 {
		t.Fatalf("second key subscription queued %d packets, want no additional full state", len(sendQueue))
	}

	if err := s.stopRelayedStoreSubscription("TestStore", "values%&a"); err != nil {
		t.Fatalf("first unsubscribe failed: %v", err)
	}
	s.partialCallback("TestStore", nil, &streamerTestPartial{})
	assertQueuedStorePacketKind(t, sendQueue, protocol.KindPartialState)

	if err := s.stopRelayedStoreSubscription("TestStore", "values%&b"); err != nil {
		t.Fatalf("last unsubscribe failed: %v", err)
	}
	s.partialCallback("TestStore", nil, &streamerTestPartial{})
	if len(sendQueue) != 0 {
		t.Fatalf("partial after last unsubscribe queued %d packets", len(sendQueue))
	}
}

func TestOnDemandStoreStreamingQueuesFullBeforeSubscriptionTriggeredPartial(t *testing.T) {
	store := &streamerSubscriptionUpdatingStore{
		streamerTypedRelayStore: newStreamerTypedRelayStore("TestStore", restream.StoreTypeDeviceWithRelay),
		value:                   "started",
	}
	registry, err := restream.NewStoreRegistry([]restream.Store{store})
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}
	store.registry = registry
	s, sendQueue := newConnectedOnDemandTestStreamer(registry, StorePolicy{})

	if err := s.startRelayedStoreSubscription("TestStore", ""); err != nil {
		t.Fatalf("subscription failed: %v", err)
	}
	if store.err != nil {
		t.Fatalf("subscription-triggered update failed: %v", store.err)
	}
	assertQueuedStorePacketKind(t, sendQueue, protocol.KindFullState)
	assertQueuedStorePacketKind(t, sendQueue, protocol.KindPartialState)
}

func TestOnDemandStoreStreamingDiscardsDebouncedAndStalePackets(t *testing.T) {
	store := newStreamerTypedRelayStore("TestStore", restream.StoreTypeDeviceWithRelay)
	registry, err := restream.NewStoreRegistry([]restream.Store{store})
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}
	s, sendQueue := newConnectedOnDemandTestStreamer(registry, StorePolicy{
		Debounce: map[string]time.Duration{"TestStore": time.Hour},
	})

	if err := s.startRelayedStoreSubscription("TestStore", ""); err != nil {
		t.Fatalf("subscription failed: %v", err)
	}
	staleFull := <-sendQueue
	s.partialCallback("TestStore", nil, &streamerTestPartial{})
	if len(s.gatheredPartials) != 1 {
		t.Fatalf("gathered partial count = %d, want 1", len(s.gatheredPartials))
	}
	if err := s.stopRelayedStoreSubscription("TestStore", ""); err != nil {
		t.Fatalf("unsubscribe failed: %v", err)
	}
	if len(s.gatheredPartials) != 0 {
		t.Fatalf("gathered partial survived unsubscribe: %#v", s.gatheredPartials)
	}
	if s.storePacketStillActive(staleFull) {
		t.Fatal("queued full state remained active after last unsubscribe")
	}

	if err := s.startRelayedStoreSubscription("TestStore", ""); err != nil {
		t.Fatalf("resubscription failed: %v", err)
	}
	freshFull := <-sendQueue
	if !s.storePacketStillActive(freshFull) {
		t.Fatal("new full state was not active after resubscribe")
	}
	if freshFull.generation == staleFull.generation {
		t.Fatalf("resubscription generation = %d, want different from stale generation", freshFull.generation)
	}
}

func newConnectedOnDemandTestStreamer(
	registry *restream.StoreRegistry,
	policy StorePolicy,
) (*Streamer, chan outboundPacket) {
	policy.OnDemand = true
	s := NewStreamer(registry, nil, nil, Config{StorePolicy: policy})
	s.conn = &gws.Conn{}
	s.sendDone = make(chan struct{})
	s.sendQueue = make(chan outboundPacket, 16)
	s.configureOnDemandStoreStreaming(&protocol.ConnectedPacket{Capabilities: protocol.RelayCapabilities{
		OnDemandStoreStreaming: true,
	}})
	return s, s.sendQueue
}

func assertQueuedStorePacketKind(t *testing.T, queue chan outboundPacket, want protocol.PacketKind) {
	t.Helper()
	packet := <-queue
	if packet.packetKind != want {
		t.Fatalf("queued packet kind = %d, want %d", packet.packetKind, want)
	}
	decoded, err := protocol.DecodePacket(mustBuildOutboundPacket(t, packet))
	if err != nil {
		t.Fatalf("DecodePacket failed: %v", err)
	}
	storePacket, ok := decoded.(*protocol.StoreStatePacket)
	if !ok || storePacket.Kind() != want {
		t.Fatalf("decoded packet = %#v, want store packet kind %d", decoded, want)
	}
}

func assertActiveRelayKeys(
	t *testing.T,
	store *restream.RelayStore[streamerTestState, *streamerTestState, *streamerTestPartial],
	expected []string,
) {
	t.Helper()

	actual := store.ActiveSubscriptionKeys()
	if len(actual) != len(expected) {
		t.Fatalf("active keys = %#v, want %#v", actual, expected)
	}
	for idx := range expected {
		if actual[idx] != expected[idx] {
			t.Fatalf("active keys = %#v, want %#v", actual, expected)
		}
	}
}

func readStreamerTestStoreState(t *testing.T, registry *restream.StoreRegistry, storeName string) *streamerTestState {
	t.Helper()
	snapshot, err := registry.GetFullStateSnapshot(storeName, restream.AccessLevelPublic)
	if err != nil {
		t.Fatalf("GetFullStateSnapshot %s failed: %v", storeName, err)
	}
	return snapshot.(*streamerTestState)
}

type streamerTypedRelayStore struct {
	*restream.RelayStore[streamerTestState, *streamerTestState, *streamerTestPartial]
	storeType restream.StoreType
}

type streamerSubscriptionUpdatingStore struct {
	*streamerTypedRelayStore
	registry *restream.StoreRegistry
	value    string
	err      error
}

func (s *streamerSubscriptionUpdatingStore) SubscriptionStarted() {
	partialBytes, err := restream.SerializeToBytes(&streamerTestPartial{Value: &s.value}, nil)
	if err == nil {
		err = s.registry.ApplyPartialToStore(s.GetName(), partialBytes)
	}
	s.err = err
}

func (*streamerSubscriptionUpdatingStore) SubscriptionEnded() {}

func newStreamerTypedRelayStore(name string, storeType restream.StoreType) *streamerTypedRelayStore {
	return &streamerTypedRelayStore{
		RelayStore: restream.NewRelayStore[streamerTestState, *streamerTestState, *streamerTestPartial](
			name,
			&streamerTestState{},
			restream.AccessLevelPublic,
		),
		storeType: storeType,
	}
}

func (s *streamerTypedRelayStore) GetStoreType() restream.StoreType {
	return s.storeType
}

type streamerTestState struct {
	Value       string
	onSerialize func()
}

func (s *streamerTestState) RestreamClone() *streamerTestState {
	if s == nil {
		return nil
	}
	clone := *s
	return &clone
}

func (s *streamerTestState) Serialize(w *binarystreams.Writer, _ *restream.VarInfoStruct) error {
	if s.onSerialize != nil {
		s.onSerialize()
	}
	return restream.SerializeValue(s.Value, w, &restream.VarInfoPrimitive{DataType: restream.SerializationTypeString})
}

func (s *streamerTestState) Deserialize(r *binarystreams.Reader, _ *restream.VarInfoStruct) error {
	return restream.DeserializeValue(&s.Value, r, &restream.VarInfoPrimitive{DataType: restream.SerializationTypeString})
}

type streamerTestPartial struct {
	Value       *string
	onSerialize func()
}

type streamerMapPartial struct {
	Values map[string]string
}

func (p *streamerMapPartial) Serialize(w *binarystreams.Writer, _ *restream.VarInfoStruct) error {
	return restream.SerializeValue(p.Values, w, &restream.VarInfoMap{
		KeyType:  &restream.VarInfoPrimitive{DataType: restream.SerializationTypeString},
		ElemType: &restream.VarInfoPrimitive{DataType: restream.SerializationTypeString},
	})
}

func (p *streamerMapPartial) Deserialize(r *binarystreams.Reader, _ *restream.VarInfoStruct) error {
	return restream.DeserializeValue(&p.Values, r, &restream.VarInfoMap{
		KeyType:  &restream.VarInfoPrimitive{DataType: restream.SerializationTypeString},
		ElemType: &restream.VarInfoPrimitive{DataType: restream.SerializationTypeString},
	})
}

func (p *streamerMapPartial) MergeOntoPartial(other any) {
	po := other.(*streamerMapPartial)
	if po.Values == nil {
		po.Values = map[string]string{}
	}
	for key, value := range p.Values {
		po.Values[key] = value
	}
}

func (p *streamerMapPartial) ApplyTo(state any) [][]any {
	po := state.(*map[string]string)
	if *po == nil {
		*po = map[string]string{}
	}
	for key, value := range p.Values {
		(*po)[key] = value
	}
	return [][]any{{"Values"}}
}

func (p *streamerTestPartial) Serialize(w *binarystreams.Writer, _ *restream.VarInfoStruct) error {
	if p.onSerialize != nil {
		p.onSerialize()
	}
	return restream.SerializeValue(p.Value, w, &restream.VarInfoPointer{
		NotNil:  false,
		SubType: &restream.VarInfoPrimitive{DataType: restream.SerializationTypeString},
	})
}

func (p *streamerTestPartial) Deserialize(r *binarystreams.Reader, _ *restream.VarInfoStruct) error {
	return restream.DeserializeValue(&p.Value, r, &restream.VarInfoPointer{
		NotNil:  false,
		SubType: &restream.VarInfoPrimitive{DataType: restream.SerializationTypeString},
	})
}

func (p *streamerTestPartial) MergeOntoPartial(other any) {
	po := other.(*streamerTestPartial)
	if p.Value != nil {
		po.Value = p.Value
	}
}

func (p *streamerTestPartial) ApplyTo(state any) [][]any {
	if p.Value == nil {
		return nil
	}
	st := state.(*streamerTestState)
	st.Value = *p.Value
	return [][]any{{"Value"}}
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
