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

func (s *storeDataLockTestStore) SubscribeToField(field []any, callback any) {
	s.data.SubscribeToField(field, callback)
}

func newStoreDataLockTestStore() (*storeDataLockTestStore, *storeDataLockTestState) {
	state := &storeDataLockTestState{}
	store := &storeDataLockTestStore{}
	store.data = NewStoreData[storeDataLockTestState, *storeDataLockTestState, *storeDataLockTestPartial](store, state)
	return store, state
}

func TestReadStateUnlocksAfterPanic(t *testing.T) {
	store, state := newStoreDataLockTestStore()

	assertPanics(t, func() {
		store.data.ReadState(func(*storeDataLockTestState) {
			panic("test read panic")
		})
	})

	applyStoreDataLockTestPartial(t, store, &storeDataLockTestPartial{Value: "after read panic"})
	if state.Value != "after read panic" {
		t.Fatalf("state value = %q, want %q", state.Value, "after read panic")
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

func applyStoreDataLockTestPartial(t *testing.T, store *storeDataLockTestStore, partial *storeDataLockTestPartial) {
	t.Helper()

	done := make(chan struct{})
	go func() {
		store.data.ApplyPartial(partial)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for store mutation; store lock is likely still held")
	}
}
