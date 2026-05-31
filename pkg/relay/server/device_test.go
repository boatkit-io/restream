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

type testState struct {
	Value string
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
