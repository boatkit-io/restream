package restream

import (
	"testing"
	"time"

	"github.com/boatkit-io/restream/pkg/binarystreams"
)

type storeDataLockTestState struct {
	Value string
}

func (*storeDataLockTestState) Serialize(*binarystreams.Writer, *VarInfoStruct) error {
	return nil
}

func (*storeDataLockTestState) Deserialize(*binarystreams.Reader, *VarInfoStruct) error {
	return nil
}

type storeDataLockTestPartial struct {
	Value      string
	panicApply bool
}

func (*storeDataLockTestPartial) Serialize(*binarystreams.Writer, *VarInfoStruct) error {
	return nil
}

func (*storeDataLockTestPartial) Deserialize(*binarystreams.Reader, *VarInfoStruct) error {
	return nil
}

func (*storeDataLockTestPartial) MergeOntoPartial(any) {}

func (p *storeDataLockTestPartial) ApplyTo(target any) [][]any {
	if p.panicApply {
		panic("test partial apply panic")
	}

	state := target.(*storeDataLockTestState)
	state.Value = p.Value
	return [][]any{{"Value"}}
}

type storeDataLockTestStore struct {
	data *StoreData[storeDataLockTestState, *storeDataLockTestState, *storeDataLockTestPartial]
}

func (s *storeDataLockTestStore) GetName() string {
	return "store-data-lock-test"
}

func (s *storeDataLockTestStore) GetStoreData() StoreDataBase {
	return s.data
}

func (s *storeDataLockTestStore) GetStoreType() StoreType {
	return StoreTypeDeviceWithRelay
}

func (s *storeDataLockTestStore) SubscribeToField(field []any, callback any) {
	s.data.SubscribeToField(field, callback)
}

func newStoreDataLockTestStore() (*storeDataLockTestStore, *storeDataLockTestState) {
	state := &storeDataLockTestState{}
	store := &storeDataLockTestStore{}
	store.data = NewStoreData[storeDataLockTestState, *storeDataLockTestState, *storeDataLockTestPartial](store, state)
	return store, state
}

type storeDataSnapshotTestState struct {
	Value              string
	onPartialForFields func()
	onSerialize        func()
}

func (s *storeDataSnapshotTestState) RestreamClone() *storeDataSnapshotTestState {
	if s == nil {
		return nil
	}
	clone := *s
	return &clone
}

func (s *storeDataSnapshotTestState) Serialize(*binarystreams.Writer, *VarInfoStruct) error {
	if s.onSerialize != nil {
		s.onSerialize()
	}
	return nil
}

func (*storeDataSnapshotTestState) Deserialize(*binarystreams.Reader, *VarInfoStruct) error {
	return nil
}

func (s *storeDataSnapshotTestState) PartialForFields([][]any) (Partial, bool) {
	if s.onPartialForFields != nil {
		s.onPartialForFields()
	}
	return &storeDataSnapshotTestPartial{Value: s.Value, onSerialize: s.onSerialize}, true
}

type storeDataSnapshotTestPartial struct {
	Value       string
	onSerialize func()
}

func (p *storeDataSnapshotTestPartial) Serialize(*binarystreams.Writer, *VarInfoStruct) error {
	if p.onSerialize != nil {
		p.onSerialize()
	}
	return nil
}

func (*storeDataSnapshotTestPartial) Deserialize(*binarystreams.Reader, *VarInfoStruct) error {
	return nil
}

func (p *storeDataSnapshotTestPartial) MergeOntoPartial(any) {}

func (p *storeDataSnapshotTestPartial) ApplyTo(target any) [][]any {
	state := target.(*storeDataSnapshotTestState)
	state.Value = p.Value
	return [][]any{{"Value"}}
}

type storeDataSnapshotTestStore struct {
	data *StoreData[storeDataSnapshotTestState, *storeDataSnapshotTestState, *storeDataSnapshotTestPartial]
}

func (s *storeDataSnapshotTestStore) GetName() string {
	return "store-data-snapshot-test"
}

func (s *storeDataSnapshotTestStore) GetStoreData() StoreDataBase {
	return s.data
}

func (s *storeDataSnapshotTestStore) GetStoreType() StoreType {
	return StoreTypeDeviceWithRelay
}

func (s *storeDataSnapshotTestStore) SubscribeToField(field []any, callback any) {
	s.data.SubscribeToField(field, callback)
}

func newStoreDataSnapshotTestStore() (*storeDataSnapshotTestStore, *storeDataSnapshotTestState) {
	state := &storeDataSnapshotTestState{}
	store := &storeDataSnapshotTestStore{}
	store.data = NewStoreData[storeDataSnapshotTestState, *storeDataSnapshotTestState, *storeDataSnapshotTestPartial](store, state)
	return store, state
}

func TestReadStateRequiresCloneableState(t *testing.T) {
	store, _ := newStoreDataLockTestStore()

	assertPanics(t, func() {
		_ = store.data.ReadState()
	})
}

func TestReadStateReturnsClonedState(t *testing.T) {
	store, state := newStoreDataSnapshotTestStore()
	state.Value = "original"

	snapshot := store.data.ReadState()
	if snapshot == state {
		t.Fatal("ReadState returned the live state pointer")
	}
	if snapshot.Value != "original" {
		t.Fatalf("snapshot value = %q, want original", snapshot.Value)
	}

	snapshot.Value = "mutated snapshot"
	if state.Value != "original" {
		t.Fatalf("live state changed through snapshot: %q", state.Value)
	}

	applyStoreDataSnapshotTestPartial(t, store, &storeDataSnapshotTestPartial{Value: "updated after read"})
	if state.Value != "updated after read" {
		t.Fatalf("state value = %q, want update after read", state.Value)
	}
}

func TestApplyPartialUnlocksAfterPanic(t *testing.T) {
	store, state := newStoreDataLockTestStore()

	assertPanics(t, func() {
		store.data.ApplyPartial(&storeDataLockTestPartial{panicApply: true})
	})

	applyStoreDataLockTestPartial(t, store, &storeDataLockTestPartial{Value: "after apply panic"})
	if state.Value != "after apply panic" {
		t.Fatalf("state value = %q, want %q", state.Value, "after apply panic")
	}
}

func TestGetSerializedFullStateSerializesSnapshotOutsideReadLock(t *testing.T) {
	store, state := newStoreDataSnapshotTestStore()
	state.onSerialize = func() {
		store.data.ApplyPartial(&storeDataSnapshotTestPartial{Value: "updated during full serialize"})
	}

	assertCompletes(t, func() {
		if _, err := store.data.GetSerializedFullState(); err != nil {
			t.Fatalf("GetSerializedFullState failed: %v", err)
		}
	})
	if state.Value != "updated during full serialize" {
		t.Fatalf("state value = %q, want update from serialize callback", state.Value)
	}
}

func TestGetSerializedPartialForSubscriptionKeySerializesSnapshotOutsideReadLock(t *testing.T) {
	store, state := newStoreDataSnapshotTestStore()
	state.onSerialize = func() {
		store.data.ApplyPartial(&storeDataSnapshotTestPartial{Value: "updated during partial serialize"})
	}

	assertCompletes(t, func() {
		if _, exists, err := store.data.GetSerializedPartialForSubscriptionKey("value"); err != nil {
			t.Fatalf("GetSerializedPartialForSubscriptionKey failed: %v", err)
		} else if !exists {
			t.Fatal("expected subscription partial to exist")
		}
	})
	if state.Value != "updated during partial serialize" {
		t.Fatalf("state value = %q, want update from partial serialize callback", state.Value)
	}
}

func TestGetPartialSnapshotForSubscriptionKeyBuildsPartialOutsideReadLock(t *testing.T) {
	store, state := newStoreDataSnapshotTestStore()
	state.onPartialForFields = func() {
		store.data.ApplyPartial(&storeDataSnapshotTestPartial{Value: "updated during partial snapshot"})
	}

	assertCompletes(t, func() {
		if _, exists, err := store.data.GetPartialSnapshotForSubscriptionKey("value"); err != nil {
			t.Fatalf("GetPartialSnapshotForSubscriptionKey failed: %v", err)
		} else if !exists {
			t.Fatal("expected subscription partial to exist")
		}
	})
	if state.Value != "updated during partial snapshot" {
		t.Fatalf("state value = %q, want update from partial snapshot callback", state.Value)
	}
}

func assertPanics(t *testing.T, fn func()) {
	t.Helper()

	didPanic := false
	func() {
		defer func() {
			if recover() != nil {
				didPanic = true
			}
		}()
		fn()
	}()

	if !didPanic {
		t.Fatal("expected panic")
	}
}

func assertCompletes(t *testing.T, fn func()) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("operation did not complete")
	}
}

func applyStoreDataLockTestPartial(t *testing.T, store *storeDataLockTestStore, partial *storeDataLockTestPartial) {
	t.Helper()

	applyStoreDataPartial(t, func() {
		store.data.ApplyPartial(partial)
	})
}

func applyStoreDataSnapshotTestPartial(t *testing.T, store *storeDataSnapshotTestStore, partial *storeDataSnapshotTestPartial) {
	t.Helper()

	applyStoreDataPartial(t, func() {
		store.data.ApplyPartial(partial)
	})
}

func applyStoreDataPartial(t *testing.T, apply func()) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		apply()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for store mutation; store lock is likely still held")
	}
}
