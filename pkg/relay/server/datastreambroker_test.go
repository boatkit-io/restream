package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/boatkit-io/restream/pkg/relay/protocol"
	"github.com/boatkit-io/restream/pkg/restream"
)

type testDataStreamAllocator struct {
	mu                sync.Mutex
	allocated         []restream.DataStreamEndpoint
	allocatedSessions []string
	released          []restream.DataStreamEndpoint
	releaseCalls      int
	releaseFailures   int
	onAllocate        func()
}

func (a *testDataStreamAllocator) AllocateViewer(
	_ context.Context,
	sessionID string,
	_ string,
	_ restream.DataStreamSubscription,
) (restream.DataStreamEndpoint, error) {
	if a.onAllocate != nil {
		a.onAllocate()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	endpoint := restream.DataStreamEndpoint{
		LeaseID: fmt.Sprintf("lease-%d", len(a.allocated)+1),
		URL:     "wss://stream.example/viewer",
		Token:   "secret",
	}
	a.allocated = append(a.allocated, endpoint)
	a.allocatedSessions = append(a.allocatedSessions, sessionID)
	return endpoint, nil
}

func (a *testDataStreamAllocator) ReleaseSession(context.Context, string) error {
	return nil
}

func (a *testDataStreamAllocator) ReleaseViewer(
	_ context.Context,
	endpoint restream.DataStreamEndpoint,
) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.releaseCalls++
	if a.releaseFailures > 0 {
		a.releaseFailures--
		return errors.New("temporary release failure")
	}
	a.released = append(a.released, endpoint)
	return nil
}

func TestDeviceDataStreamBrokerAuthorizesAndRefCountsViewerLeases(t *testing.T) {
	store := restream.NewRelayStore[testState, *testState, *testPartial](
		"CameraMedia",
		&testState{},
		restream.AccessLevel(3),
	)
	registry, err := restream.NewStoreRegistry([]restream.Store{store})
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}
	device := NewDevice("device-a", registry, DeviceManagerConfig{})
	allocator := &testDataStreamAllocator{}
	broker := NewDeviceDataStreamBroker(device, allocator)
	allocatedBeforeSourceStart := false
	allocator.onAllocate = func() {
		allocatedBeforeSourceStart = len(device.ActiveDataStreamSubscriptions()) == 0
	}
	subscription := restream.DataStreamSubscription{
		StoreName:  "CameraMedia",
		StreamName: "CameraMedia.Video",
		Key:        "camera-a",
	}
	serverConn, clientConn, cleanup := newTestWebsocketPair(t)
	defer cleanup()
	relayConn := NewConnection(serverConn)
	relayConn.Capabilities.DataStreams = true
	device.DeviceConnected(relayConn)
	defer device.DeviceDisconnected(relayConn)
	transitions := make(chan *protocol.DataStreamSubscriptionRequestPacket, 4)
	go func() {
		for {
			_, message, readErr := clientConn.ReadMessage()
			if readErr != nil {
				return
			}
			packetRaw, decodeErr := protocol.DecodePacket(message)
			if decodeErr != nil {
				return
			}
			packet, ok := packetRaw.(*protocol.DataStreamSubscriptionRequestPacket)
			if !ok {
				continue
			}
			transitions <- packet
			device.HandleDataStreamSubscriptionResult(
				relayConn,
				&protocol.DataStreamSubscriptionResultPacket{OperationID: packet.OperationID},
			)
		}
	}()

	if _, err := broker.Open(context.Background(), subscription, restream.AccessLevel(2)); err == nil {
		t.Fatal("Open accepted viewer access below the store minimum")
	}
	if len(allocator.allocated) != 0 {
		t.Fatal("unauthorized Open reached the endpoint allocator")
	}

	first, err := broker.OpenForSession(
		context.Background(),
		"session-a",
		subscription,
		restream.AccessLevel(3),
	)
	if err != nil {
		t.Fatalf("first Open failed: %v", err)
	}
	if !allocatedBeforeSourceStart {
		t.Fatal("viewer endpoint was not allocated before source startup")
	}
	if len(allocator.allocatedSessions) != 1 || allocator.allocatedSessions[0] != "session-a" {
		t.Fatalf("allocator session IDs = %#v, want session-a", allocator.allocatedSessions)
	}
	second, err := broker.Open(context.Background(), subscription, restream.AccessLevel(3))
	if err != nil {
		t.Fatalf("second Open failed: %v", err)
	}
	if got := device.ActiveDataStreamSubscriptions(); len(got) != 1 || got[0] != subscription {
		t.Fatalf("active subscriptions = %#v, want one aggregate subscription", got)
	}
	packet := readDataStreamTransition(t, transitions)
	if packet.StoreName != subscription.StoreName ||
		packet.StreamName != subscription.StreamName ||
		packet.Key != subscription.Key ||
		packet.Action != protocol.StoreSubscribe {
		t.Fatalf("data stream start packet = %#v", packet)
	}

	if err := broker.Close(context.Background(), first); err != nil {
		t.Fatalf("first Close failed: %v", err)
	}
	if got := device.ActiveDataStreamSubscriptions(); len(got) != 1 {
		t.Fatalf("first Close removed aggregate subscription: %#v", got)
	}
	if err := broker.Close(context.Background(), second); err != nil {
		t.Fatalf("second Close failed: %v", err)
	}
	if got := device.ActiveDataStreamSubscriptions(); len(got) != 0 {
		t.Fatalf("last Close left aggregate subscription: %#v", got)
	}
	if len(allocator.released) != 2 {
		t.Fatalf("released endpoints = %d, want 2", len(allocator.released))
	}
	packet = readDataStreamTransition(t, transitions)
	if packet.Action != protocol.StoreUnsubscribe {
		t.Fatalf("data stream stop packet = %#v", packet)
	}
	if err := broker.Close(context.Background(), second); err != nil {
		t.Fatalf("idempotent duplicate Close failed: %v", err)
	}

	third, err := broker.Open(context.Background(), subscription, restream.AccessLevel(3))
	if err != nil {
		t.Fatalf("third Open failed: %v", err)
	}
	if packet = readDataStreamTransition(t, transitions); packet.Action != protocol.StoreSubscribe {
		t.Fatalf("third start packet = %#v", packet)
	}
	allocator.mu.Lock()
	allocator.releaseFailures = 1
	allocator.mu.Unlock()
	if err := broker.Close(context.Background(), third); err == nil {
		t.Fatal("Close with a temporary release failure returned nil")
	}
	if packet = readDataStreamTransition(t, transitions); packet.Action != protocol.StoreUnsubscribe {
		t.Fatalf("third stop packet = %#v", packet)
	}
	if err := broker.Close(context.Background(), third); err != nil {
		t.Fatalf("retrying partially completed Close failed: %v", err)
	}
	if err := broker.Close(context.Background(), third); err != nil {
		t.Fatalf("duplicate Close after retry failed: %v", err)
	}
	allocator.mu.Lock()
	releaseCalls := allocator.releaseCalls
	releasedCount := len(allocator.released)
	allocator.mu.Unlock()
	if releaseCalls != 4 || releasedCount != 3 {
		t.Fatalf(
			"release attempts/successes = %d/%d, want 4/3",
			releaseCalls,
			releasedCount,
		)
	}
	select {
	case unexpected := <-transitions:
		t.Fatalf("partial Close retry repeated a completed source stop: %#v", unexpected)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestDataStreamBrokerCloseSessionCompletesAfterDeviceDisconnect(t *testing.T) {
	store := restream.NewRelayStore[testState, *testState, *testPartial](
		"CameraMedia",
		&testState{},
		restream.AccessLevel(3),
	)
	registry, err := restream.NewStoreRegistry([]restream.Store{store})
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}
	device := NewDevice("device-a", registry, DeviceManagerConfig{})
	allocator := &testDataStreamAllocator{}
	broker := NewDeviceDataStreamBroker(device, allocator)
	subscription := restream.DataStreamSubscription{
		StoreName:  "CameraMedia",
		StreamName: "CameraMedia.Video",
		Key:        "camera-a",
	}
	serverConn, clientConn, cleanup := newTestWebsocketPair(t)
	defer cleanup()
	relayConn := NewConnection(serverConn)
	relayConn.Capabilities.DataStreams = true
	device.DeviceConnected(relayConn)
	transition := make(chan *protocol.DataStreamSubscriptionRequestPacket, 1)
	go func() {
		_, message, readErr := clientConn.ReadMessage()
		if readErr != nil {
			return
		}
		packetRaw, decodeErr := protocol.DecodePacket(message)
		if decodeErr != nil {
			return
		}
		packet, ok := packetRaw.(*protocol.DataStreamSubscriptionRequestPacket)
		if !ok {
			return
		}
		transition <- packet
		device.HandleDataStreamSubscriptionResult(
			relayConn,
			&protocol.DataStreamSubscriptionResultPacket{OperationID: packet.OperationID},
		)
	}()

	endpoint, err := broker.OpenForSession(
		context.Background(),
		"session-a",
		subscription,
		restream.AccessLevel(3),
	)
	if err != nil {
		t.Fatalf("OpenForSession failed: %v", err)
	}
	if packet := readDataStreamTransition(t, transition); packet.Action != protocol.StoreSubscribe {
		t.Fatalf("start packet = %#v, want subscribe", packet)
	}

	device.DeviceDisconnected(relayConn)
	if err := broker.CloseSession(context.Background(), "session-a"); err != nil {
		t.Fatalf("CloseSession after disconnect failed: %v", err)
	}
	if got := device.ActiveDataStreamSubscriptions(); len(got) != 0 {
		t.Fatalf("release after disconnect left active subscriptions: %#v", got)
	}
	allocator.mu.Lock()
	released := append([]restream.DataStreamEndpoint(nil), allocator.released...)
	allocator.mu.Unlock()
	if len(released) != 1 || released[0].LeaseID != endpoint.LeaseID {
		t.Fatalf("released endpoints = %#v, want lease %q", released, endpoint.LeaseID)
	}
}

func readDataStreamTransition(
	t *testing.T,
	transitions <-chan *protocol.DataStreamSubscriptionRequestPacket,
) *protocol.DataStreamSubscriptionRequestPacket {
	t.Helper()
	select {
	case packet := <-transitions:
		return packet
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for data stream transition")
		return nil
	}
}
