package server

import (
	"reflect"
	"testing"

	"github.com/boatkit-io/restream/pkg/binarystreams"
	"github.com/boatkit-io/restream/pkg/relay/protocol"
	"github.com/boatkit-io/restream/pkg/restream"
	gws "github.com/gorilla/websocket"
)

func TestDeviceUnknownStorePolicy(t *testing.T) {
	device := NewDevice("device-1", mustStoreRegistry(t), DeviceManagerConfig{})
	if err := device.HandleFullState(nil, "MissingStore", nil); err == nil {
		t.Fatal("HandleFullState missing store error = nil, want error")
	}

	device = NewDevice("device-1", mustStoreRegistry(t), DeviceManagerConfig{UnknownStorePolicy: UnknownStoreIgnore})
	if err := device.HandleFullState(nil, "MissingStore", nil); err != nil {
		t.Fatalf("HandleFullState missing store with ignore policy failed: %v", err)
	}
}

func TestDeviceFFRPCHandlerForwardsWithoutPendingResponse(t *testing.T) {
	serverConn, clientConn, cleanup := newTestWebsocketPair(t)
	defer cleanup()

	device := NewDevice("device-1", mustStoreRegistry(t), DeviceManagerConfig{})
	device.DeviceConnected(NewConnection(serverConn))

	handled, err := device.FFRPCHandler(
		"Radio.TransmitAudio",
		restream.AccessLevel(3),
		[]byte{7, 8, 9},
	)
	if err != nil {
		t.Fatalf("FFRPCHandler failed: %v", err)
	}
	if !handled {
		t.Fatal("FFRPCHandler reported the relayed call as unhandled")
	}

	messageType, message, err := clientConn.ReadMessage()
	if err != nil {
		t.Fatalf("Read FFRPC packet failed: %v", err)
	}
	if messageType != gws.BinaryMessage {
		t.Fatalf("message type = %d, want BinaryMessage", messageType)
	}
	packetRaw, err := protocol.DecodePacket(message)
	if err != nil {
		t.Fatalf("Decode FFRPC packet failed: %v", err)
	}
	packet, ok := packetRaw.(*protocol.FFRPCCallPacket)
	if !ok {
		t.Fatalf("packet type = %T, want *FFRPCCallPacket", packetRaw)
	}
	if packet.MethodName != "Radio.TransmitAudio" || packet.AccessLevel != 3 {
		t.Fatalf("FFRPC packet = %+v", packet)
	}

	device.rpcMutex.Lock()
	pendingCount := len(device.rpcsPending)
	device.rpcMutex.Unlock()
	if pendingCount != 0 {
		t.Fatalf("pending RPC count = %d, want 0", pendingCount)
	}
}

func TestDeviceFFRPCHandlerForwardsAnnotations(t *testing.T) {
	serverConn, clientConn, cleanup := newTestWebsocketPair(t)
	defer cleanup()

	connection := NewConnection(serverConn)
	connection.Capabilities.RPCAnnotations = true
	device := NewDevice("device-1", mustStoreRegistry(t), DeviceManagerConfig{})
	device.DeviceConnected(connection)
	annotations := map[string]string{"principal_id": "cloud-user:42"}

	handled, err := device.FFRPCHandlerWithAnnotations(
		annotations,
		"Radio.TransmitAudio",
		restream.AccessLevel(3),
		[]byte{7, 8, 9},
	)
	if err != nil || !handled {
		t.Fatalf("FFRPCHandlerWithAnnotations = (%v, %v), want (true, nil)", handled, err)
	}

	_, message, err := clientConn.ReadMessage()
	if err != nil {
		t.Fatalf("Read FFRPC packet failed: %v", err)
	}
	packetRaw, err := protocol.DecodePacket(message)
	if err != nil {
		t.Fatalf("Decode FFRPC packet failed: %v", err)
	}
	packet := packetRaw.(*protocol.FFRPCCallPacket)
	if !reflect.DeepEqual(packet.Annotations, annotations) {
		t.Fatalf("annotations = %#v, want %#v", packet.Annotations, annotations)
	}
}

func TestDeviceAppliesDeviceStateOnlyToDeviceRelayUpdateStores(t *testing.T) {
	stores := []restream.Store{
		restream.NewRelayStore[testState, *testState, *testPartial](
			"CloudImplOfDeviceStore",
			&testState{Value: "initial"},
			restream.AccessLevelPublic,
		),
		restream.NewCloudSourceForDeviceStore[testState, *testState, *testPartial](
			"CloudSourceForDeviceStore",
			&testState{Value: "initial"},
			restream.AccessLevelPublic,
		),
		newServerTypedRelayStore("CloudOnlyStore", restream.StoreTypeCloudOnly),
		newServerTypedRelayStore("DeviceAndCloudStore", restream.StoreTypeDeviceAndCloud),
	}
	sr, err := restream.NewStoreRegistry(stores)
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}
	device := NewDevice("device-1", sr, DeviceManagerConfig{})

	fullBytes, err := restream.SerializeToBytes(&testState{Value: "device full"}, nil)
	if err != nil {
		t.Fatalf("SerializeToBytes full state failed: %v", err)
	}
	partialValue := "device partial"
	partialBytes, err := restream.SerializeToBytes(&testPartial{Value: &partialValue}, nil)
	if err != nil {
		t.Fatalf("SerializeToBytes partial failed: %v", err)
	}

	for _, store := range stores {
		storeName := store.GetName()
		if err := device.ApplyFullState(storeName, fullBytes); err != nil {
			t.Fatalf("ApplyFullState %s failed: %v", storeName, err)
		}
		if err := device.ApplyPartialState(storeName, partialBytes); err != nil {
			t.Fatalf("ApplyPartialState %s failed: %v", storeName, err)
		}
	}

	for _, store := range stores {
		storeName := store.GetName()
		state := readServerTestStoreState(t, sr, storeName)
		if storeName == "CloudImplOfDeviceStore" {
			if state.Value != "device partial" {
				t.Fatalf("%s value = %q, want device partial", storeName, state.Value)
			}
			continue
		}
		if state.Value != "initial" {
			t.Fatalf("%s value = %q, want initial", storeName, state.Value)
		}
	}
}

func TestCloudSourceForDeviceStoreDoesNotAcceptDeviceRelayUpdates(t *testing.T) {
	store := restream.NewCloudSourceForDeviceStore[testState, *testState, *testPartial](
		"CloudSourceStore",
		&testState{},
		restream.AccessLevelPublic,
	)

	registry, err := restream.NewStoreRegistry([]restream.Store{store})
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}
	accepts, err := registry.StoreAcceptsDeviceRelayUpdates(store.GetName())
	if err != nil {
		t.Fatalf("StoreAcceptsDeviceRelayUpdates failed: %v", err)
	}
	if accepts {
		t.Fatal("CloudSourceForDeviceStore accepts device relay updates")
	}
}

func TestDeviceDispatchesKeyedEventsToExactCloudSubscribers(t *testing.T) {
	device := NewDevice("device-1", mustStoreRegistry(t), DeviceManagerConfig{})
	var got []byte
	device.EventDispatcher.SubscribeToKeyedEvents(func(
		storeName string,
		eventName string,
		key string,
		eventBytes []byte,
	) {
		if storeName == "TestStore" && eventName == "audio" && key == "radio-a" {
			got = append([]byte(nil), eventBytes...)
		}
	})
	if err := device.EventDispatcher.ListeningToKeyedEvent("TestStore", "audio", "radio-a"); err != nil {
		t.Fatalf("ListeningToKeyedEvent failed: %v", err)
	}

	if err := device.HandleKeyedEvent(nil, "TestStore", "audio", "radio-b", []byte{9}); err != nil {
		t.Fatalf("HandleKeyedEvent for other key failed: %v", err)
	}
	if got != nil {
		t.Fatalf("other keyed event was dispatched: %v", got)
	}
	if err := device.HandleKeyedEvent(nil, "TestStore", "audio", "radio-a", []byte{1, 2, 3}); err != nil {
		t.Fatalf("HandleKeyedEvent failed: %v", err)
	}
	if string(got) != string([]byte{1, 2, 3}) {
		t.Fatalf("keyed event payload = %v", got)
	}
}

func mustStoreRegistry(t *testing.T) *restream.StoreRegistry {
	t.Helper()
	sr, err := restream.NewStoreRegistry([]restream.Store{
		restream.NewRelayStore[testState, *testState, *testPartial]("TestStore", &testState{}, restream.AccessLevelPublic),
	})
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}
	return sr
}

func readServerTestStoreState(t *testing.T, registry *restream.StoreRegistry, storeName string) *testState {
	t.Helper()
	snapshot, err := registry.GetFullStateSnapshot(storeName, restream.AccessLevelPublic)
	if err != nil {
		t.Fatalf("GetFullStateSnapshot %s failed: %v", storeName, err)
	}
	return snapshot.(*testState)
}

type serverTypedRelayStore struct {
	*restream.RelayStore[testState, *testState, *testPartial]
	storeType restream.StoreType
}

type customServerRelayStore struct {
	name string
	data *restream.StoreData[testState, *testState, *testPartial]
}

func newCustomServerRelayStore(name string) *customServerRelayStore {
	store := &customServerRelayStore{name: name}
	store.data = restream.NewStoreData[testState, *testState, *testPartial](store, &testState{})
	return store
}

func (s *customServerRelayStore) GetName() string {
	return s.name
}

func (s *customServerRelayStore) GetStoreData() restream.StoreDataBase {
	return s.data
}

func (s *customServerRelayStore) SubscribeToField(field []any, callback any) {
	s.data.SubscribeToField(field, callback)
}

func (*customServerRelayStore) GetStoreType() restream.StoreType {
	return restream.StoreTypeCloudImplOfDevice
}

func newServerTypedRelayStore(name string, storeType restream.StoreType) *serverTypedRelayStore {
	return &serverTypedRelayStore{
		RelayStore: restream.NewRelayStore[testState, *testState, *testPartial](
			name,
			&testState{Value: "initial"},
			restream.AccessLevelPublic,
		),
		storeType: storeType,
	}
}

func (s *serverTypedRelayStore) GetStoreType() restream.StoreType {
	return s.storeType
}

type testState struct {
	Value string
}

func (s *testState) RestreamClone() *testState {
	if s == nil {
		return nil
	}
	clone := *s
	return &clone
}

func (s *testState) Serialize(w *binarystreams.Writer, _ *restream.VarInfoStruct) error {
	return restream.SerializeValue(s.Value, w, &restream.VarInfoPrimitive{DataType: restream.SerializationTypeString})
}

func (s *testState) Deserialize(r *binarystreams.Reader, _ *restream.VarInfoStruct) error {
	return restream.DeserializeValue(&s.Value, r, &restream.VarInfoPrimitive{DataType: restream.SerializationTypeString})
}

type testPartial struct {
	Value *string
}

func (p *testPartial) Serialize(w *binarystreams.Writer, _ *restream.VarInfoStruct) error {
	return restream.SerializeValue(p.Value, w, &restream.VarInfoPointer{
		NotNil:  false,
		SubType: &restream.VarInfoPrimitive{DataType: restream.SerializationTypeString},
	})
}

func (p *testPartial) Deserialize(r *binarystreams.Reader, _ *restream.VarInfoStruct) error {
	return restream.DeserializeValue(&p.Value, r, &restream.VarInfoPointer{
		NotNil:  false,
		SubType: &restream.VarInfoPrimitive{DataType: restream.SerializationTypeString},
	})
}

func (p *testPartial) MergeOntoPartial(other any) {
	po := other.(*testPartial)
	if p.Value != nil {
		po.Value = p.Value
	}
}

func (p *testPartial) ApplyTo(state any) [][]any {
	st := state.(*testState)
	if p.Value == nil {
		return nil
	}
	st.Value = *p.Value
	return [][]any{{"Value"}}
}
