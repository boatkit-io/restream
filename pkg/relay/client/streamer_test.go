package client

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/boatkit-io/restream/pkg/binarystreams"
	"github.com/boatkit-io/restream/pkg/relay/protocol"
	"github.com/boatkit-io/restream/pkg/restream"
	gws "github.com/gorilla/websocket"
)

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

func TestEnqueuePacketAfterDisconnectDoesNotPanic(t *testing.T) {
	s := &Streamer{}

	if err := s.enqueuePacket("test packet", []byte{1}); !errors.Is(err, ErrDisconnected) {
		t.Fatalf("enqueuePacket error = %v, want %v", err, ErrDisconnected)
	}
}

func TestEnqueuePacketFullQueueDoesNotBlock(t *testing.T) {
	done := make(chan struct{})
	sendQueue := make(chan []byte, 1)
	sendQueue <- []byte{1}

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
	sendQueue := make(chan []byte, 1)

	s := &Streamer{
		conn:      &gws.Conn{},
		sendQueue: sendQueue,
		sendDone:  done,
	}

	if err := s.SendEvent("TrackedPointUpdate", []byte{1, 2, 3}); err != nil {
		t.Fatalf("SendEvent failed: %v", err)
	}

	packetBytes := <-sendQueue
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
	sendQueue := make(chan []byte, 1)
	s := &Streamer{
		sr:        registry,
		conn:      &gws.Conn{},
		sendQueue: sendQueue,
		sendDone:  done,
	}

	if err := s.sendFullState("TestStore"); err != nil {
		t.Fatalf("sendFullState failed: %v", err)
	}

	packetBytes := <-sendQueue
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

func TestStreamerStoreTypesFilterRelayTraffic(t *testing.T) {
	relayStore := newStreamerTypedRelayStore("RelayStore", restream.StoreTypeDeviceWithRelay)
	noRelayStore := newStreamerTypedRelayStore("NoRelayStore", restream.StoreTypeDeviceWithNoRelay)
	cloudImplStore := newStreamerTypedRelayStore("CloudImplStore", restream.StoreTypeDeviceWithCloudImpl)
	deviceAndCloudStore := newStreamerTypedRelayStore("DeviceAndCloudStore", restream.StoreTypeDeviceAndCloud)
	cloudImplOfDeviceStore := newStreamerTypedRelayStore("CloudImplOfDeviceStore", restream.StoreTypeCloudImplOfDevice)
	cloudOnlyStore := newStreamerTypedRelayStore("CloudOnlyStore", restream.StoreTypeCloudOnly)
	registry, err := restream.NewStoreRegistry([]restream.Store{
		relayStore,
		noRelayStore,
		cloudImplStore,
		deviceAndCloudStore,
		cloudImplOfDeviceStore,
		cloudOnlyStore,
	})
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}

	done := make(chan struct{})
	sendQueue := make(chan []byte, 4)
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
		packetBytes := <-sendQueue
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
	for _, unexpected := range []string{"NoRelayStore", "DeviceAndCloudStore", "CloudImplOfDeviceStore", "CloudOnlyStore"} {
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
	s.partialCallback("CloudImplOfDeviceStore", nil, &streamerTestPartial{})
	s.partialCallback("CloudOnlyStore", nil, &streamerTestPartial{})
	if len(sendQueue) != 0 {
		t.Fatalf("partialCallback queued %d packets for non-streaming stores", len(sendQueue))
	}

	s.partialCallback("CloudImplStore", nil, &streamerTestPartial{})
	if len(sendQueue) != 1 {
		t.Fatalf("partialCallback queued %d packets for streaming store, want 1", len(sendQueue))
	}
	packet, err := protocol.DecodePacket(<-sendQueue)
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

type streamerTypedRelayStore struct {
	*restream.RelayStore[streamerTestState, *streamerTestState, *streamerTestPartial]
	storeType restream.StoreType
}

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
	Value string
}

func (*streamerTestState) Serialize(*binarystreams.Writer, *restream.VarInfoStruct) error {
	return nil
}

func (*streamerTestState) Deserialize(*binarystreams.Reader, *restream.VarInfoStruct) error {
	return nil
}

type streamerTestPartial struct {
	Value *string
}

func (*streamerTestPartial) Serialize(*binarystreams.Writer, *restream.VarInfoStruct) error {
	return nil
}

func (*streamerTestPartial) Deserialize(*binarystreams.Reader, *restream.VarInfoStruct) error {
	return nil
}

func (*streamerTestPartial) MergeOntoPartial(any) {
}

func (*streamerTestPartial) ApplyTo(any) [][]any {
	return nil
}
