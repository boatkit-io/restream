package restream

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/boatkit-io/tugboat/pkg/subscribableevent"
)

type registryTestStore struct {
	data *registryTestStoreData

	storeStarted int
	storeEnded   int
	keyStarted   []string
	keyEnded     []string
}

func newRegistryTestStore() *registryTestStore {
	return &registryTestStore{
		data: &registryTestStoreData{},
	}
}

func (s *registryTestStore) GetName() string {
	return "registry-test"
}

func (s *registryTestStore) GetStoreData() StoreDataBase {
	return s.data
}

func (s *registryTestStore) SubscribeToField([]any, any) {
}

func (s *registryTestStore) SubscriptionStarted() {
	s.storeStarted++
}

func (s *registryTestStore) SubscriptionEnded() {
	s.storeEnded++
}

func (s *registryTestStore) SubscriptionStartedForKey(key string) {
	s.keyStarted = append(s.keyStarted, key)
}

func (s *registryTestStore) SubscriptionEndedForKey(key string) {
	s.keyEnded = append(s.keyEnded, key)
}

type registryTestStoreData struct {
	callbacks []PartialCallbackFunc
}

func (d *registryTestStoreData) AddCallback(callback PartialCallbackFunc) subscribableevent.SubscriptionId {
	d.callbacks = append(d.callbacks, callback)
	return subscribableevent.SubscriptionId(len(d.callbacks))
}

func (d *registryTestStoreData) RemoveCallback(subscribableevent.SubscriptionId) error {
	return nil
}

func (d *registryTestStoreData) DecodeAndSetFullState([]byte) error {
	return errors.New("not implemented")
}

func (d *registryTestStoreData) DecodeAndApplyPartial([]byte) error {
	return errors.New("not implemented")
}

func (d *registryTestStoreData) GetSerializedFullState() ([]byte, error) {
	return []byte{1, 2, 3}, nil
}

func TestStoreRegistryRefCountsDuplicateKeySubscriptions(t *testing.T) {
	store := newRegistryTestStore()
	registry, err := NewStoreRegistry([]Store{store})
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}

	mustNoError(t, registry.ListeningToStoreKey(store.GetName(), "values%&a"))
	mustNoError(t, registry.ListeningToStoreKey(store.GetName(), "values%&a"))
	mustNoError(t, registry.ListeningToStoreKey(store.GetName(), "values%&b"))

	info := registry.storeMap[store.GetName()]
	assertEqual(t, 3, info.ActiveSubCount)
	assertEqual(t, 2, info.ActiveKeySubCount["values%&a"])
	assertEqual(t, 1, info.ActiveKeySubCount["values%&b"])
	assertEqual(t, 1, store.storeStarted)
	assertEqual(t, 0, store.storeEnded)
	assertEqualSlices(t, []string{"values%&a", "values%&b"}, store.keyStarted)
	assertEqualSlices(t, nil, store.keyEnded)

	mustNoError(t, registry.StopListeningToStoreKey(store.GetName(), "values%&a"))
	assertEqual(t, 2, info.ActiveSubCount)
	assertEqual(t, 1, info.ActiveKeySubCount["values%&a"])
	assertEqualSlices(t, nil, store.keyEnded)

	mustNoError(t, registry.StopListeningToStoreKey(store.GetName(), "values%&b"))
	assertEqual(t, 1, info.ActiveSubCount)
	_, hasB := info.ActiveKeySubCount["values%&b"]
	assertEqual(t, false, hasB)
	assertEqualSlices(t, []string{"values%&b"}, store.keyEnded)
	assertEqual(t, 0, store.storeEnded)

	mustNoError(t, registry.StopListeningToStoreKey(store.GetName(), "values%&a"))
	assertEqual(t, 0, info.ActiveSubCount)
	_, hasA := info.ActiveKeySubCount["values%&a"]
	assertEqual(t, false, hasA)
	assertEqualSlices(t, []string{"values%&b", "values%&a"}, store.keyEnded)
	assertEqual(t, 1, store.storeEnded)

	if err := registry.StopListeningToStoreKey(store.GetName(), "values%&a"); err == nil {
		t.Fatal("expected double unsubscribe to fail")
	}
}

func TestStoreRegistryRefCountsWholeStoreSubscriptions(t *testing.T) {
	store := newRegistryTestStore()
	registry, err := NewStoreRegistry([]Store{store})
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}

	mustNoError(t, registry.ListeningToStore(store.GetName()))
	mustNoError(t, registry.ListeningToStore(store.GetName()))

	info := registry.storeMap[store.GetName()]
	assertEqual(t, 2, info.ActiveSubCount)
	assertEqual(t, 2, info.ActiveKeySubCount[""])
	assertEqual(t, 1, store.storeStarted)
	assertEqualSlices(t, []string{""}, store.keyStarted)

	mustNoError(t, registry.StopListeningToStore(store.GetName()))
	assertEqual(t, 1, info.ActiveSubCount)
	assertEqual(t, 1, info.ActiveKeySubCount[""])
	assertEqualSlices(t, nil, store.keyEnded)
	assertEqual(t, 0, store.storeEnded)

	mustNoError(t, registry.StopListeningToStore(store.GetName()))
	assertEqual(t, 0, info.ActiveSubCount)
	_, hasWhole := info.ActiveKeySubCount[""]
	assertEqual(t, false, hasWhole)
	assertEqualSlices(t, []string{""}, store.keyEnded)
	assertEqual(t, 1, store.storeEnded)
}

func TestStoreRegistryConcurrentKeySubscriptionUpdates(t *testing.T) {
	store := newRegistryTestStore()
	registry, err := NewStoreRegistry([]Store{store})
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}

	const workers = 64
	const iterations = 150

	start := make(chan struct{})
	errCh := make(chan error, workers*iterations*2)
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for idx := range iterations {
				key := fmt.Sprintf("values%%&%d", (worker+idx)%13)
				if err := registry.ListeningToStoreKey(store.GetName(), key); err != nil {
					errCh <- err
					return
				}
				if err := registry.StopListeningToStoreKey(store.GetName(), key); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}

	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}

	info := registry.storeMap[store.GetName()]
	assertEqual(t, 0, info.ActiveSubCount)
	assertEqual(t, 0, len(info.ActiveKeySubCount))
	assertEqual(t, store.storeStarted, store.storeEnded)
	assertEqual(t, len(store.keyStarted), len(store.keyEnded))
}

func mustNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func assertEqual[T comparable](t *testing.T, expected T, actual T) {
	t.Helper()
	if expected != actual {
		t.Fatalf("expected %#v, got %#v", expected, actual)
	}
}

func assertEqualSlices[T comparable](t *testing.T, expected []T, actual []T) {
	t.Helper()
	if len(expected) != len(actual) {
		t.Fatalf("expected %#v, got %#v", expected, actual)
	}
	for idx := range expected {
		if expected[idx] != actual[idx] {
			t.Fatalf("expected %#v, got %#v", expected, actual)
		}
	}
}
