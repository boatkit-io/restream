package restream

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

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
	callbacks        []PartialCallbackFunc
	decodedFullState []byte
	decodeFullErr    error
}

func (d *registryTestStoreData) AddCallback(callback PartialCallbackFunc) subscribableevent.SubscriptionId {
	d.callbacks = append(d.callbacks, callback)
	return subscribableevent.SubscriptionId(len(d.callbacks))
}

func (d *registryTestStoreData) RemoveCallback(subscribableevent.SubscriptionId) error {
	return nil
}

func (d *registryTestStoreData) DecodeAndSetFullState(state []byte) error {
	d.decodedFullState = append([]byte(nil), state...)
	return d.decodeFullErr
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

	mustNoError(t, registry.ListeningToStoreKey(store.GetName(), "~1%&1%&a", AccessLevelPublic))
	mustNoError(t, registry.ListeningToStoreKey(store.GetName(), "~1%&1%&a", AccessLevelPublic))
	mustNoError(t, registry.ListeningToStoreKey(store.GetName(), "~1%&1%&b", AccessLevelPublic))

	info := registry.storeMap[store.GetName()]
	assertEqual(t, 3, info.ActiveSubCount)
	assertEqual(t, 2, info.ActiveKeySubCount["~1%&1%&a"])
	assertEqual(t, 1, info.ActiveKeySubCount["~1%&1%&b"])
	assertEqual(t, 1, store.storeStarted)
	assertEqual(t, 0, store.storeEnded)
	assertEqualSlices(t, []string{"~1%&1%&a", "~1%&1%&b"}, store.keyStarted)
	assertEqualSlices(t, nil, store.keyEnded)

	mustNoError(t, registry.StopListeningToStoreKey(store.GetName(), "~1%&1%&a"))
	assertEqual(t, 2, info.ActiveSubCount)
	assertEqual(t, 1, info.ActiveKeySubCount["~1%&1%&a"])
	assertEqualSlices(t, nil, store.keyEnded)

	mustNoError(t, registry.StopListeningToStoreKey(store.GetName(), "~1%&1%&b"))
	assertEqual(t, 1, info.ActiveSubCount)
	_, hasB := info.ActiveKeySubCount["~1%&1%&b"]
	assertEqual(t, false, hasB)
	assertEqualSlices(t, []string{"~1%&1%&b"}, store.keyEnded)
	assertEqual(t, 0, store.storeEnded)

	mustNoError(t, registry.StopListeningToStoreKey(store.GetName(), "~1%&1%&a"))
	assertEqual(t, 0, info.ActiveSubCount)
	_, hasA := info.ActiveKeySubCount["~1%&1%&a"]
	assertEqual(t, false, hasA)
	assertEqualSlices(t, []string{"~1%&1%&b", "~1%&1%&a"}, store.keyEnded)
	assertEqual(t, 1, store.storeEnded)

	if err := registry.StopListeningToStoreKey(store.GetName(), "~1%&1%&a"); err == nil {
		t.Fatal("expected double unsubscribe to fail")
	}
}

func TestStoreRegistryPublishesAggregateSubscriptionTransitions(t *testing.T) {
	store := newRegistryTestStore()
	registry, err := NewStoreRegistry([]Store{store})
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}

	type transition struct {
		key        string
		subscribed bool
	}
	var transitions []transition
	registry.SubscribeToStoreSubscriptions(func(storeName string, key string, subscribed bool) {
		if storeName != store.GetName() {
			t.Fatalf("transition store = %q, want %q", storeName, store.GetName())
		}
		transitions = append(transitions, transition{key: key, subscribed: subscribed})
	})

	mustNoError(t, registry.ListeningToStoreKey(store.GetName(), "~1%&1%&a", AccessLevelPublic))
	mustNoError(t, registry.ListeningToStoreKey(store.GetName(), "~1%&1%&a", AccessLevelPublic))
	mustNoError(t, registry.ListeningToStoreKey(store.GetName(), "~1%&1%&b", AccessLevelPublic))
	mustNoError(t, registry.StopListeningToStoreKey(store.GetName(), "~1%&1%&a"))
	mustNoError(t, registry.StopListeningToStoreKey(store.GetName(), "~1%&1%&a"))
	mustNoError(t, registry.StopListeningToStoreKey(store.GetName(), "~1%&1%&b"))

	want := []transition{
		{key: "~1%&1%&a", subscribed: true},
		{key: "~1%&1%&b", subscribed: true},
		{key: "~1%&1%&a", subscribed: false},
		{key: "~1%&1%&b", subscribed: false},
	}
	if len(transitions) != len(want) {
		t.Fatalf("transitions = %#v, want %#v", transitions, want)
	}
	for idx := range want {
		if transitions[idx] != want[idx] {
			t.Fatalf("transitions = %#v, want %#v", transitions, want)
		}
	}

	keys, err := registry.GetActiveStoreSubscriptionKeys(store.GetName())
	if err != nil {
		t.Fatalf("GetActiveStoreSubscriptionKeys failed: %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("active keys = %#v, want none", keys)
	}
}

func TestStoreRegistryTracksActiveKeySubscriptionActivity(t *testing.T) {
	store := newRegistryTestStore()
	registry, err := NewStoreRegistry([]Store{store})
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}

	now := time.Date(2026, time.August, 9, 12, 0, 0, 0, time.UTC)
	registry.now = func() time.Time { return now }
	keyA := "~1%&1%&a"
	keyB := "~1%&1%&b"

	if _, active, err := registry.GetStoreSubscriptionActivity(store.GetName(), keyA); err != nil || active {
		t.Fatalf("initial activity = active %t, error %v; want inactive", active, err)
	}
	mustNoError(t, registry.ListeningToStoreKey(store.GetName(), keyA, AccessLevelPublic))
	mustNoError(t, registry.ListeningToStoreKey(store.GetName(), keyA, AccessLevelPublic))
	mustNoError(t, registry.ListeningToStoreKey(store.GetName(), keyB, AccessLevelPublic))

	activityA, active, err := registry.GetStoreSubscriptionActivity(store.GetName(), keyA)
	if err != nil || !active {
		t.Fatalf("key A activity = active %t, error %v; want active", active, err)
	}
	assertEqual(t, 2, activityA.ReferenceCount)
	assertEqual(t, now, activityA.StartedAt)
	if !activityA.LastUpdateAt.IsZero() {
		t.Fatalf("key A last update = %v, want zero before a matching state", activityA.LastUpdateAt)
	}

	partialAt := now.Add(time.Second)
	now = partialAt
	registry.PartialCallback(store.GetName(), [][]any{{byte(1), "a"}}, nil)
	activityA, _, err = registry.GetStoreSubscriptionActivity(store.GetName(), keyA)
	mustNoError(t, err)
	assertEqual(t, partialAt, activityA.LastUpdateAt)
	assertEqual(t, partialAt, activityA.LastPartialAt)
	activityB, _, err := registry.GetStoreSubscriptionActivity(store.GetName(), keyB)
	mustNoError(t, err)
	if !activityB.LastUpdateAt.IsZero() {
		t.Fatalf("non-matching key B last update = %v, want zero", activityB.LastUpdateAt)
	}

	fullAt := now.Add(time.Second)
	now = fullAt
	mustNoError(t, registry.SetFullStateToStore(store.GetName(), []byte{7, 8, 9}))
	activityA, _, err = registry.GetStoreSubscriptionActivity(store.GetName(), keyA)
	mustNoError(t, err)
	activityB, _, err = registry.GetStoreSubscriptionActivity(store.GetName(), keyB)
	mustNoError(t, err)
	assertEqual(t, fullAt, activityA.LastFullStateAt)
	assertEqual(t, fullAt, activityB.LastFullStateAt)
	assertEqual(t, fullAt, activityB.LastUpdateAt)

	resetAt := now.Add(time.Second)
	now = resetAt
	mustNoError(t, registry.ResetStoreSubscriptionActivity(store.GetName()))
	activityA, _, err = registry.GetStoreSubscriptionActivity(store.GetName(), keyA)
	mustNoError(t, err)
	activityB, _, err = registry.GetStoreSubscriptionActivity(store.GetName(), keyB)
	mustNoError(t, err)
	assertEqual(t, resetAt, activityA.StartedAt)
	assertEqual(t, resetAt, activityB.StartedAt)
	if !activityA.LastUpdateAt.IsZero() || !activityA.LastFullStateAt.IsZero() || !activityA.LastPartialAt.IsZero() {
		t.Fatalf("key A activity after reset = %#v, want no current-generation state", activityA)
	}
	assertEqual(t, 2, activityA.ReferenceCount)

	mustNoError(t, registry.StopListeningToStoreKey(store.GetName(), keyA))
	activityA, active, err = registry.GetStoreSubscriptionActivity(store.GetName(), keyA)
	if err != nil || !active {
		t.Fatalf("key A after one stop = active %t, error %v; want active", active, err)
	}
	assertEqual(t, 1, activityA.ReferenceCount)
	mustNoError(t, registry.StopListeningToStoreKey(store.GetName(), keyA))
	if _, active, err = registry.GetStoreSubscriptionActivity(store.GetName(), keyA); err != nil || active {
		t.Fatalf("key A final activity = active %t, error %v; want inactive", active, err)
	}
}

func TestStoreRegistryPublishesSuccessfulFullStateApplies(t *testing.T) {
	store := newRegistryTestStore()
	registry, err := NewStoreRegistry([]Store{store})
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}

	var callbacks [][]byte
	subID := registry.SubscribeToFullStateApplies(func(storeName string, stateBytes []byte) {
		if storeName != store.GetName() {
			t.Fatalf("full-state store = %q, want %q", storeName, store.GetName())
		}
		callbacks = append(callbacks, append([]byte(nil), stateBytes...))
	})

	stateBytes := []byte{7, 8, 9}
	if err := registry.SetFullStateToStore(store.GetName(), stateBytes); err != nil {
		t.Fatalf("SetFullStateToStore failed: %v", err)
	}
	stateBytes[0] = 0
	if len(callbacks) != 1 || string(callbacks[0]) != string([]byte{7, 8, 9}) {
		t.Fatalf("full-state callbacks = %#v, want [7 8 9]", callbacks)
	}

	if err := registry.UnsubscribeFromFullStateApplies(subID); err != nil {
		t.Fatalf("UnsubscribeFromFullStateApplies failed: %v", err)
	}
	if err := registry.SetFullStateToStore(store.GetName(), []byte{10}); err != nil {
		t.Fatalf("second SetFullStateToStore failed: %v", err)
	}
	if len(callbacks) != 1 {
		t.Fatalf("full-state callback count = %d, want 1 after unsubscribe", len(callbacks))
	}

	store.data.decodeFullErr = errors.New("decode failed")
	if err := registry.SetFullStateToStore(store.GetName(), []byte{11}); err == nil {
		t.Fatal("SetFullStateToStore error = nil, want decode error")
	}
	if len(callbacks) != 1 {
		t.Fatalf("failed decode published a callback: %#v", callbacks)
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

	mustNoError(t, registry.ListeningToStoreKey(store.GetName(), "~1%&1%&a", AccessLevelPublic))
	mustNoError(t, registry.StopListeningToStoreKey(store.GetName(), "~1%&1%&a"))
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

	err = registry.ListeningToStoreKey(store.GetName(), "~1%&1%&a", AccessLevel(1))
	if !errors.Is(err, ErrInsufficientStoreAccess) {
		t.Fatalf("ListeningToStoreKey error = %v, want ErrInsufficientStoreAccess", err)
	}

	info := registry.storeMap[store.GetName()]
	assertEqual(t, 0, info.ActiveSubCount)
	assertEqual(t, 0, len(info.ActiveKeySubCount))
	assertEqual(t, 0, store.storeStarted)
	assertEqualSlices(t, nil, store.keyStarted)

	mustNoError(t, registry.ListeningToStoreKey(store.GetName(), "~1%&1%&a", AccessLevel(2)))
	assertEqual(t, 1, info.ActiveSubCount)
	assertEqual(t, 1, info.ActiveKeySubCount["~1%&1%&a"])
	assertEqual(t, 1, store.storeStarted)
	assertEqualSlices(t, []string{"~1%&1%&a"}, store.keyStarted)
}

func TestStoreRegistryRejectsInsufficientAccessForKeyedPartialFetch(t *testing.T) {
	store := newRegistryTestStore()
	store.minimumAccessLevel = AccessLevel(2)
	registry, err := NewStoreRegistry([]Store{store})
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}

	_, _, err = registry.GetSerializedPartialForSubscriptionKey(store.GetName(), "~1%&1%&a", AccessLevel(1))
	if !errors.Is(err, ErrInsufficientStoreAccess) {
		t.Fatalf("GetSerializedPartialForSubscriptionKey error = %v, want ErrInsufficientStoreAccess", err)
	}

	partialBytes, exists, err := registry.GetSerializedPartialForSubscriptionKey(store.GetName(), "~1%&1%&a", AccessLevel(2))
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
