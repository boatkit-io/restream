package restream

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/boatkit-io/tugboat/pkg/subscribableevent"
)

type registryTestStore struct {
	data               *registryTestStoreData
	storeType          StoreType
	minimumAccessLevel AccessLevel

	storeStarted int
	storeEnded   int
	keyStarted   []string
	keyEnded     []string

	onStoreStarted func()
	onStoreEnded   func()
	onKeyStarted   func(string)
	onKeyEnded     func(string)
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

func (s *registryTestStore) GetMinimumAccessLevel() AccessLevel {
	return s.minimumAccessLevel
}

func (s *registryTestStore) GetStoreType() StoreType {
	return s.storeType
}

func (s *registryTestStore) SubscribeToField([]any, any) {
}

func (s *registryTestStore) SubscriptionStarted() {
	s.storeStarted++
	if s.onStoreStarted != nil {
		s.onStoreStarted()
	}
}

func (s *registryTestStore) SubscriptionEnded() {
	s.storeEnded++
	if s.onStoreEnded != nil {
		s.onStoreEnded()
	}
}

func (s *registryTestStore) SubscriptionStartedForKey(key string) {
	s.keyStarted = append(s.keyStarted, key)
	if s.onKeyStarted != nil {
		s.onKeyStarted(key)
	}
}

func (s *registryTestStore) SubscriptionEndedForKey(key string) {
	s.keyEnded = append(s.keyEnded, key)
	if s.onKeyEnded != nil {
		s.onKeyEnded(key)
	}
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

func (d *registryTestStoreData) GetSerializedPartialForSubscriptionKey(string) ([]byte, bool, error) {
	return []byte{4, 5, 6}, true, nil
}

func TestStoreRegistryRefCountsDuplicateKeySubscriptions(t *testing.T) {
	store := newRegistryTestStore()
	registry, err := NewStoreRegistry([]Store{store})
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}

	mustNoError(t, registry.ListeningToStoreKey(store.GetName(), "values%&a", AccessLevelPublic))
	mustNoError(t, registry.ListeningToStoreKey(store.GetName(), "values%&a", AccessLevelPublic))
	mustNoError(t, registry.ListeningToStoreKey(store.GetName(), "values%&b", AccessLevelPublic))

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

func TestStoreRegistryTracksStoreType(t *testing.T) {
	store := newRegistryTestStore()
	store.storeType = StoreTypeDeviceWithNoRelay
	registry, err := NewStoreRegistry([]Store{store})
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}

	storeType, err := registry.GetStoreType(store.GetName())
	if err != nil {
		t.Fatalf("GetStoreType failed: %v", err)
	}
	assertEqual(t, StoreTypeDeviceWithNoRelay, storeType)

	streams, err := registry.StoreStreamsToRelay(store.GetName())
	if err != nil {
		t.Fatalf("StoreStreamsToRelay failed: %v", err)
	}
	assertEqual(t, false, streams)
}

func TestStoreRegistryRefCountsWholeStoreSubscriptions(t *testing.T) {
	store := newRegistryTestStore()
	registry, err := NewStoreRegistry([]Store{store})
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}

	mustNoError(t, registry.ListeningToStore(store.GetName(), AccessLevelPublic))
	mustNoError(t, registry.ListeningToStore(store.GetName(), AccessLevelPublic))

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

func TestStoreRegistrySubscriptionCallbacksRunOutsideSubscriptionMutex(t *testing.T) {
	store := newRegistryTestStore()
	registry, err := NewStoreRegistry([]Store{store})
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}

	assertSubscriptionMutexUnlocked := func(callbackName string) {
		if !registry.subscriptionMutex.TryLock() {
			t.Fatalf("%s ran while StoreRegistry subscription mutex was locked", callbackName)
		}
		registry.subscriptionMutex.Unlock()
	}
	store.onKeyStarted = func(string) {
		assertSubscriptionMutexUnlocked("SubscriptionStartedForKey")
	}
	store.onStoreStarted = func() {
		assertSubscriptionMutexUnlocked("SubscriptionStarted")
	}
	store.onKeyEnded = func(string) {
		assertSubscriptionMutexUnlocked("SubscriptionEndedForKey")
	}
	store.onStoreEnded = func() {
		assertSubscriptionMutexUnlocked("SubscriptionEnded")
	}

	mustNoError(t, registry.ListeningToStoreKey(store.GetName(), "values%&a", AccessLevelPublic))
	mustNoError(t, registry.StopListeningToStoreKey(store.GetName(), "values%&a"))
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
				if err := registry.ListeningToStoreKey(store.GetName(), key, AccessLevelPublic); err != nil {
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

func TestStoreRegistryRejectsInsufficientAccessForFullState(t *testing.T) {
	store := newRegistryTestStore()
	store.minimumAccessLevel = AccessLevel(2)
	registry, err := NewStoreRegistry([]Store{store})
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}

	_, err = registry.GetSerializedFullState(store.GetName(), AccessLevel(1))
	if !errors.Is(err, ErrInsufficientStoreAccess) {
		t.Fatalf("GetSerializedFullState error = %v, want ErrInsufficientStoreAccess", err)
	}

	stateBytes, err := registry.GetSerializedFullState(store.GetName(), AccessLevel(2))
	if err != nil {
		t.Fatalf("GetSerializedFullState with enough access failed: %v", err)
	}
	assertEqualSlices(t, []byte{1, 2, 3}, stateBytes)
}

func TestStoreRegistryRejectsInsufficientAccessForSubscription(t *testing.T) {
	store := newRegistryTestStore()
	store.minimumAccessLevel = AccessLevel(2)
	registry, err := NewStoreRegistry([]Store{store})
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}

	err = registry.ListeningToStoreKey(store.GetName(), "values%&a", AccessLevel(1))
	if !errors.Is(err, ErrInsufficientStoreAccess) {
		t.Fatalf("ListeningToStoreKey error = %v, want ErrInsufficientStoreAccess", err)
	}

	info := registry.storeMap[store.GetName()]
	assertEqual(t, 0, info.ActiveSubCount)
	assertEqual(t, 0, len(info.ActiveKeySubCount))
	assertEqual(t, 0, store.storeStarted)
	assertEqualSlices(t, nil, store.keyStarted)

	mustNoError(t, registry.ListeningToStoreKey(store.GetName(), "values%&a", AccessLevel(2)))
	assertEqual(t, 1, info.ActiveSubCount)
	assertEqual(t, 1, info.ActiveKeySubCount["values%&a"])
	assertEqual(t, 1, store.storeStarted)
	assertEqualSlices(t, []string{"values%&a"}, store.keyStarted)
}

func TestStoreRegistryRejectsInsufficientAccessForKeyedPartialFetch(t *testing.T) {
	store := newRegistryTestStore()
	store.minimumAccessLevel = AccessLevel(2)
	registry, err := NewStoreRegistry([]Store{store})
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}

	_, _, err = registry.GetSerializedPartialForSubscriptionKey(store.GetName(), "values%&a", AccessLevel(1))
	if !errors.Is(err, ErrInsufficientStoreAccess) {
		t.Fatalf("GetSerializedPartialForSubscriptionKey error = %v, want ErrInsufficientStoreAccess", err)
	}

	partialBytes, exists, err := registry.GetSerializedPartialForSubscriptionKey(store.GetName(), "values%&a", AccessLevel(2))
	if err != nil {
		t.Fatalf("GetSerializedPartialForSubscriptionKey with enough access failed: %v", err)
	}
	if !exists {
		t.Fatal("expected keyed partial to exist")
	}
	assertEqualSlices(t, []byte{4, 5, 6}, partialBytes)
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
