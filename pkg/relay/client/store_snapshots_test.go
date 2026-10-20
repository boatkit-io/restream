package client

import (
	"strings"
	"testing"
	"time"

	"github.com/boatkit-io/restream/pkg/relay/protocol"
	"github.com/boatkit-io/restream/pkg/restream"
)

func TestSnapshotResponseSupersedesQueuedFullAndDebouncedPartial(t *testing.T) {
	store := newStreamerTypedRelayStore("TestStore", restream.StoreTypeDeviceWithRelay)
	registry, err := restream.NewStoreRegistry([]restream.Store{store})
	if err != nil {
		t.Fatal(err)
	}
	s, queue := newConnectedOnDemandTestStreamer(registry, StorePolicy{Debounce: map[string]time.Duration{"TestStore": time.Hour}})
	if err := s.startRelayedStoreSubscription("TestStore", ""); err != nil {
		t.Fatal(err)
	}
	stale := <-queue
	s.partialCallback("TestStore", nil, &streamerTestPartial{})
	if err := s.handleStoreSnapshot(&protocol.StoreSnapshotRequestPacket{StoreName: "TestStore", RequestID: "wake:1"}); err != nil {
		t.Fatal(err)
	}
	if s.storePacketStillActive(stale) {
		t.Fatal("old queued full survived snapshot generation change")
	}
	if len(s.gatheredPartials) != 0 {
		t.Fatal("old debounced partial survived snapshot")
	}
	full := <-queue
	if full.packetKind != protocol.KindStoreSnapshotResponse || !s.storePacketStillActive(full) {
		t.Fatal("missing active full-state response")
	}
	decoded, err := protocol.DecodePacket(mustBuildOutboundPacket(t, full))
	if err != nil {
		t.Fatal(err)
	}
	response := decoded.(*protocol.StoreSnapshotResponsePacket)
	if response.StoreName != "TestStore" || response.RequestID != "wake:1" || len(response.Data) == 0 {
		t.Fatalf("response = %#v", response)
	}
	if s.relayStoreCatchingUp["TestStore"] {
		t.Fatal("snapshot left catch-up permanently enabled")
	}
	// Another snapshot invalidates the previous requested full state.
	err = s.handleStoreSnapshot(&protocol.StoreSnapshotRequestPacket{StoreName: "TestStore", RequestID: "wake:2"})
	if err != nil {
		t.Fatal(err)
	}
	if s.storePacketStillActive(full) {
		t.Fatal("superseded snapshot remains active")
	}
}

func TestSnapshotRequestBoundsAndInactiveSubscription(t *testing.T) {
	s := NewStreamer(nil, nil, nil, Config{})
	for _, request := range []*protocol.StoreSnapshotRequestPacket{
		{}, {StoreName: "S", RequestID: "i"},
		{StoreName: strings.Repeat("s", 257), RequestID: "i"},
		{StoreName: "S", RequestID: strings.Repeat("i", 129)},
	} {
		if err := s.handleStoreSnapshot(request); err == nil {
			t.Fatalf("accepted invalid/unsubscribed snapshot %#v", request)
		}
	}
}

func TestSnapshotBuffersPartialsBehindRequestedFullState(t *testing.T) {
	base := &streamerTestState{Value: "initial"}
	store := &streamerTypedRelayStore{RelayStore: restream.NewRelayStore[streamerTestState, *streamerTestState, *streamerTestPartial]("TestStore", base, restream.AccessLevelPublic), storeType: restream.StoreTypeDeviceWithRelay}
	registry, err := restream.NewStoreRegistry([]restream.Store{store})
	if err != nil {
		t.Fatal(err)
	}
	s, queue := newConnectedOnDemandTestStreamer(registry, StorePolicy{})
	if err := s.startRelayedStoreSubscription("TestStore", ""); err != nil {
		t.Fatal(err)
	}
	for len(queue) > 0 {
		<-queue
	}
	value := "concurrent-update"
	// Inject the update while GetFullStateSnapshot detaches the baseline,
	// before the full packet has been queued. The partial must be buffered.
	base.onClone = func() { s.partialCallback("TestStore", nil, &streamerTestPartial{Value: &value}) }
	if err := s.handleStoreSnapshot(&protocol.StoreSnapshotRequestPacket{StoreName: "TestStore", RequestID: "new-owner:1"}); err != nil {
		t.Fatal(err)
	}
	if len(queue) != 2 {
		t.Fatalf("queued packets = %d, want requested full/partial", len(queue))
	}
	if (<-queue).packetKind != protocol.KindStoreSnapshotResponse {
		t.Fatal("first packet is not requested full state")
	}
	if (<-queue).packetKind != protocol.KindPartialState {
		t.Fatal("partial did not follow full state")
	}
}
