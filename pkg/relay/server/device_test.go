package server

import (
	"testing"

	"github.com/boatkit-io/restream/pkg/binarystreams"
	"github.com/boatkit-io/restream/pkg/restream"
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

func TestCloudSourceForDeviceStoreDoesNotForwardSubscriptions(t *testing.T) {
	store := restream.NewCloudSourceForDeviceStore[testState, *testState, *testPartial](
		"CloudSourceStore",
		&testState{},
		restream.AccessLevelPublic,
	)

	if _, ok := any(store).(relaySubscriptionStore); ok {
		t.Fatal("CloudSourceForDeviceStore implements relaySubscriptionStore, want cloud-owned store without subscription forwarding")
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
