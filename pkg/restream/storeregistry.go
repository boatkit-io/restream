package restream

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/boatkit-io/tugboat/pkg/subscribableevent"
	"github.com/samber/lo"
)

// StoreRegistry is a centralized registry of all Stores, used for coordination with subscriptions from
// connected web clients, since it all happens by strings over the wire -- so it needs to be able to look up a
// Store/StoreData by name.
type StoreRegistry struct {
	storeMap                  map[string]*StoreInfo
	subscriptionMutex         sync.Mutex
	subscriptionCallbackMutex sync.Mutex

	partialApplyCallbacks subscribableevent.Event[PartialCallbackFunc]
	fullStateCallbacks    subscribableevent.Event[FullStateCallbackFunc]
	subscriptionCallbacks subscribableevent.Event[StoreSubscriptionCallbackFunc]
}

// StoreInfo holds info about a store for the StoreRegistry
type StoreInfo struct {
	Name              string
	StoreData         StoreDataBase
	Store             Store
	StoreType         StoreType
	MinAccessLevel    AccessLevel
	SubAwareCallbacks SubscriptionAwareStore
	KeySubCallbacks   KeySubscriptionAwareStore
	ActiveSubCount    int
	ActiveKeySubCount map[string]int
}

// ErrInsufficientStoreAccess is returned when a caller's access level is below a store's minimum.
var ErrInsufficientStoreAccess = errors.New("insufficient store access")

// InsufficientStoreAccessError describes a rejected store read or subscription.
type InsufficientStoreAccessError struct {
	StoreName          string
	AccessLevel        AccessLevel
	MinimumAccessLevel AccessLevel
}

func (e *InsufficientStoreAccessError) Error() string {
	return fmt.Sprintf(
		"store %s requires access level %d, caller has %d",
		e.StoreName,
		e.MinimumAccessLevel,
		e.AccessLevel,
	)
}

func (e *InsufficientStoreAccessError) Unwrap() error {
	return ErrInsufficientStoreAccess
}

type subscriptionKeyStoreData interface {
	GetSerializedPartialForSubscriptionKey(key string) ([]byte, bool, error)
}

type fullStateSnapshotStoreData interface {
	GetFullStateSnapshot() (Serializable, error)
}

type subscriptionKeySnapshotStoreData interface {
	GetPartialSnapshotForSubscriptionKey(key string) (Serializable, bool, error)
}

// NewStoreRegistry brings up a StoreRegistry, holding an explicit list of Stores/StoreDatas (this list may not grow
// over time -- has to be initted up front).
func NewStoreRegistry(storeList []Store) (*StoreRegistry, error) {
	sdr := &StoreRegistry{
		storeMap: map[string]*StoreInfo{},

		partialApplyCallbacks: subscribableevent.NewEvent[PartialCallbackFunc](),
		fullStateCallbacks:    subscribableevent.NewEvent[FullStateCallbackFunc](),
		subscriptionCallbacks: subscribableevent.NewEvent[StoreSubscriptionCallbackFunc](),
	}

	for _, s := range storeList {
		si := StoreInfo{
			Name:           s.GetName(),
			Store:          s,
			StoreData:      s.GetStoreData(),
			StoreType:      StoreTypeForStore(s),
			MinAccessLevel: StoreMinimumAccessLevel(s),
		}

		if sasV, implements := s.(SubscriptionAwareStore); implements {
			si.SubAwareCallbacks = sasV
		}
		if kasV, implements := s.(KeySubscriptionAwareStore); implements {
			si.KeySubCallbacks = kasV
		}

		si.StoreData.AddCallback(sdr.PartialCallback)

		sdr.storeMap[si.Name] = &si
	}

	return sdr, nil
}

// StoreMinimumAccessLevel returns the minimum access level for a store, defaulting to public access.
func StoreMinimumAccessLevel(store Store) AccessLevel {
	if store == nil {
		return AccessLevelPublic
	}
	if accessStore, ok := store.(MinimumAccessLevelStore); ok {
		return accessStore.GetMinimumAccessLevel()
	}
	return AccessLevelPublic
}

// SubscribeToPartialApplies adds a subscription to any ApplyPartial calls, which SDR will then distribute callbacks to
// when applicable ApplyPartial calls have been made.  Used by websockets currently.
func (s *StoreRegistry) SubscribeToPartialApplies(cb PartialCallbackFunc) subscribableevent.SubscriptionId {
	return s.partialApplyCallbacks.Subscribe(cb)
}

// UnsubscribeFromPartialApplies unsubscribes from the above SubscribeToPartialApplies call.
func (s *StoreRegistry) UnsubscribeFromPartialApplies(sid subscribableevent.SubscriptionId) error {
	return s.partialApplyCallbacks.Unsubscribe(sid)
}

// SubscribeToFullStateApplies adds a subscription to successful full-state replacements.
func (s *StoreRegistry) SubscribeToFullStateApplies(cb FullStateCallbackFunc) subscribableevent.SubscriptionId {
	return s.fullStateCallbacks.Subscribe(cb)
}

// UnsubscribeFromFullStateApplies removes a full-state replacement subscription.
func (s *StoreRegistry) UnsubscribeFromFullStateApplies(sid subscribableevent.SubscriptionId) error {
	return s.fullStateCallbacks.Unsubscribe(sid)
}

// SubscribeToStoreSubscriptions adds a subscription to aggregate store/key 0-to-1 and 1-to-0 transitions.
func (s *StoreRegistry) SubscribeToStoreSubscriptions(
	cb StoreSubscriptionCallbackFunc,
) subscribableevent.SubscriptionId {
	return s.subscriptionCallbacks.Subscribe(cb)
}

// UnsubscribeFromStoreSubscriptions removes a store subscription transition callback.
func (s *StoreRegistry) UnsubscribeFromStoreSubscriptions(sid subscribableevent.SubscriptionId) error {
	return s.subscriptionCallbacks.Unsubscribe(sid)
}

// PartialCallback is a callback for any Partial application for all storedatas in the SDR
func (s *StoreRegistry) PartialCallback(storeName string, fields [][]any, partial Partial) {
	s.partialApplyCallbacks.Fire(storeName, fields, partial)
}

// IsStoreValid checks to see if a store is valid
func (s *StoreRegistry) IsStoreValid(storeName string) bool {
	_, has := s.storeMap[storeName]
	return has
}

// GetFullStateSnapshot returns a serializable full-state snapshot for a store.
func (s *StoreRegistry) GetFullStateSnapshot(storeName string, accessLevel AccessLevel) (Serializable, error) {
	si, has := s.storeMap[storeName]
	if !has {
		return nil, fmt.Errorf("no store found (%s) in GetFullStateSnapshot", storeName)
	}
	if err := requireStoreAccess(si, accessLevel); err != nil {
		return nil, err
	}

	if provider, ok := si.StoreData.(fullStateSnapshotStoreData); ok {
		return provider.GetFullStateSnapshot()
	}
	stateBytes, err := si.StoreData.GetSerializedFullState()
	if err != nil {
		return nil, err
	}
	return RawSerializable(stateBytes), nil
}

// GetSerializedFullState returns a pre-serialized full state object for a store.
func (s *StoreRegistry) GetSerializedFullState(storeName string, accessLevel AccessLevel) ([]byte, error) {
	snapshot, err := s.GetFullStateSnapshot(storeName, accessLevel)
	if err != nil {
		return nil, err
	}
	return SerializeToBytes(snapshot, nil)
}

// GetPartialSnapshotForSubscriptionKey returns an initial keyed partial snapshot for a store, when the store supports it.
func (s *StoreRegistry) GetPartialSnapshotForSubscriptionKey(storeName string, key string, accessLevel AccessLevel) (Serializable, bool, error) {
	si, has := s.storeMap[storeName]
	if !has {
		return nil, false, fmt.Errorf("no store found (%s) in GetPartialSnapshotForSubscriptionKey", storeName)
	}
	if err := requireStoreAccess(si, accessLevel); err != nil {
		return nil, false, err
	}
	if provider, ok := si.StoreData.(subscriptionKeySnapshotStoreData); ok {
		return provider.GetPartialSnapshotForSubscriptionKey(key)
	}
	if provider, ok := si.StoreData.(subscriptionKeyStoreData); ok {
		partialBytes, exists, err := provider.GetSerializedPartialForSubscriptionKey(key)
		if err != nil || !exists {
			return nil, exists, err
		}
		return RawSerializable(partialBytes), true, nil
	}
	return nil, false, nil
}

// GetSerializedPartialForSubscriptionKey returns an initial keyed partial for a store, when the store supports it.
func (s *StoreRegistry) GetSerializedPartialForSubscriptionKey(storeName string, key string, accessLevel AccessLevel) ([]byte, bool, error) {
	snapshot, exists, err := s.GetPartialSnapshotForSubscriptionKey(storeName, key, accessLevel)
	if err != nil || !exists {
		return nil, exists, err
	}
	ret, err := SerializeToBytes(snapshot, nil)
	return ret, true, err
}

// ListeningToStore is a callback to indicate that someone has subscribed to the store
func (s *StoreRegistry) ListeningToStore(storeName string, accessLevel AccessLevel) error {
	return s.ListeningToStoreKey(storeName, "", accessLevel)
}

// ListeningToStoreKey is a callback to indicate that someone has subscribed to a store key.
func (s *StoreRegistry) ListeningToStoreKey(storeName string, key string, accessLevel AccessLevel) error {
	s.subscriptionMutex.Lock()

	si, has := s.storeMap[storeName]
	if !has {
		s.subscriptionMutex.Unlock()
		return fmt.Errorf("no store found (%s) in ListeningToStoreKey", storeName)
	}
	if err := requireStoreAccess(si, accessLevel); err != nil {
		s.subscriptionMutex.Unlock()
		return err
	}

	var keySubCallbacks KeySubscriptionAwareStore
	var subAwareCallbacks SubscriptionAwareStore
	keyStarted := false
	si.ActiveSubCount++
	if si.ActiveKeySubCount == nil {
		si.ActiveKeySubCount = map[string]int{}
	}
	si.ActiveKeySubCount[key]++
	if si.ActiveKeySubCount[key] == 1 {
		keyStarted = true
		if si.KeySubCallbacks != nil {
			keySubCallbacks = si.KeySubCallbacks
		}
	}
	if si.ActiveSubCount == 1 && si.SubAwareCallbacks != nil {
		subAwareCallbacks = si.SubAwareCallbacks
	}
	s.subscriptionMutex.Unlock()

	if keySubCallbacks != nil || subAwareCallbacks != nil || keyStarted {
		s.subscriptionCallbackMutex.Lock()
		defer s.subscriptionCallbackMutex.Unlock()
		if keySubCallbacks != nil {
			keySubCallbacks.SubscriptionStartedForKey(key)
		}
		if subAwareCallbacks != nil {
			subAwareCallbacks.SubscriptionStarted()
		}
		if keyStarted {
			s.subscriptionCallbacks.Fire(storeName, key, true)
		}
	}
	return nil
}

// StopListeningToStore is a callback to indicate that someone has unsubscribed from the store
func (s *StoreRegistry) StopListeningToStore(storeName string) error {
	return s.StopListeningToStoreKey(storeName, "")
}

// StopListeningToStoreKey is a callback to indicate that someone has unsubscribed from a store key.
func (s *StoreRegistry) StopListeningToStoreKey(storeName string, key string) error {
	s.subscriptionMutex.Lock()

	si, has := s.storeMap[storeName]
	if !has {
		s.subscriptionMutex.Unlock()
		return fmt.Errorf("no store found (%s) in StopListeningToStoreKey", storeName)
	}

	if si.ActiveSubCount == 0 {
		s.subscriptionMutex.Unlock()
		return fmt.Errorf("active sub count 0 in StopListeningToStoreKey for %s", storeName)
	}
	if si.ActiveKeySubCount[key] == 0 {
		s.subscriptionMutex.Unlock()
		return fmt.Errorf("active key sub count 0 in StopListeningToStoreKey for %s/%s", storeName, key)
	}

	var keySubCallbacks KeySubscriptionAwareStore
	var subAwareCallbacks SubscriptionAwareStore
	keyEnded := false
	si.ActiveSubCount--
	si.ActiveKeySubCount[key]--
	if si.ActiveKeySubCount[key] == 0 {
		delete(si.ActiveKeySubCount, key)
		keyEnded = true
		if si.KeySubCallbacks != nil {
			keySubCallbacks = si.KeySubCallbacks
		}
	}
	if si.ActiveSubCount == 0 && si.SubAwareCallbacks != nil {
		subAwareCallbacks = si.SubAwareCallbacks
	}
	s.subscriptionMutex.Unlock()

	if keySubCallbacks != nil || subAwareCallbacks != nil || keyEnded {
		s.subscriptionCallbackMutex.Lock()
		defer s.subscriptionCallbackMutex.Unlock()
		if keySubCallbacks != nil {
			keySubCallbacks.SubscriptionEndedForKey(key)
		}
		if subAwareCallbacks != nil {
			subAwareCallbacks.SubscriptionEnded()
		}
		if keyEnded {
			s.subscriptionCallbacks.Fire(storeName, key, false)
		}
	}
	return nil
}

// GetActiveStoreSubscriptionKeys returns the aggregate active subscription keys for a store.
func (s *StoreRegistry) GetActiveStoreSubscriptionKeys(storeName string) ([]string, error) {
	s.subscriptionMutex.Lock()
	defer s.subscriptionMutex.Unlock()

	si, has := s.storeMap[storeName]
	if !has {
		return nil, fmt.Errorf("no store found (%s) in GetActiveStoreSubscriptionKeys", storeName)
	}
	keys := make([]string, 0, len(si.ActiveKeySubCount))
	for key, count := range si.ActiveKeySubCount {
		if count > 0 {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

// CheckStoreAccess verifies that accessLevel is enough to read or subscribe to storeName.
func (s *StoreRegistry) CheckStoreAccess(storeName string, accessLevel AccessLevel) error {
	si, has := s.storeMap[storeName]
	if !has {
		return fmt.Errorf("no store found (%s) in CheckStoreAccess", storeName)
	}
	return requireStoreAccess(si, accessLevel)
}

// GetStoreMinimumAccessLevel returns the access level required to read or subscribe to storeName.
func (s *StoreRegistry) GetStoreMinimumAccessLevel(storeName string) (AccessLevel, error) {
	si, has := s.storeMap[storeName]
	if !has {
		return AccessLevelPublic, fmt.Errorf("no store found (%s) in GetStoreMinimumAccessLevel", storeName)
	}
	return si.MinAccessLevel, nil
}

// GetStoreType returns the configured store type for storeName.
func (s *StoreRegistry) GetStoreType(storeName string) (StoreType, error) {
	si, has := s.storeMap[storeName]
	if !has {
		return StoreTypeDeviceWithRelay, fmt.Errorf("no store found (%s) in GetStoreType", storeName)
	}
	return si.StoreType, nil
}

// StoreStreamsToRelay reports whether storeName should send full states, partials, and subscription lifecycles to a relay.
func (s *StoreRegistry) StoreStreamsToRelay(storeName string) (bool, error) {
	storeType, err := s.GetStoreType(storeName)
	if err != nil {
		return false, err
	}
	return StoreTypeStreamsToRelay(storeType), nil
}

// StoreReceivesFromRelay reports whether storeName should accept full states and partials from a relay.
func (s *StoreRegistry) StoreReceivesFromRelay(storeName string) (bool, error) {
	storeType, err := s.GetStoreType(storeName)
	if err != nil {
		return false, err
	}
	return StoreTypeReceivesFromRelay(storeType), nil
}

// StoreAcceptsDeviceRelayUpdates reports whether storeName should accept full states and partials from a device relay.
func (s *StoreRegistry) StoreAcceptsDeviceRelayUpdates(storeName string) (bool, error) {
	storeType, err := s.GetStoreType(storeName)
	if err != nil {
		return false, err
	}
	return StoreTypeAcceptsDeviceRelayUpdates(storeType), nil
}

// StoreStreamsFromRelay reports whether storeName should send cloud-originated full states and partials to a device.
func (s *StoreRegistry) StoreStreamsFromRelay(storeName string) (bool, error) {
	storeType, err := s.GetStoreType(storeName)
	if err != nil {
		return false, err
	}
	return StoreTypeStreamsFromRelay(storeType), nil
}

func requireStoreAccess(si *StoreInfo, accessLevel AccessLevel) error {
	if accessLevel >= si.MinAccessLevel {
		return nil
	}
	return &InsufficientStoreAccessError{
		StoreName:          si.Name,
		AccessLevel:        accessLevel,
		MinimumAccessLevel: si.MinAccessLevel,
	}
}

// SetFullStateToStore finds a store for a storename and sets its full state to the new bytes
func (s *StoreRegistry) SetFullStateToStore(storeName string, stateBytes []byte) error {
	si, has := s.storeMap[storeName]
	if !has {
		return fmt.Errorf("no store found (%s) in SetFullStateToStore", storeName)
	}
	if err := si.StoreData.DecodeAndSetFullState(stateBytes); err != nil {
		return err
	}
	s.fullStateCallbacks.Fire(storeName, append([]byte(nil), stateBytes...))
	return nil
}

// ApplyPartialToStore finds a store for a storename and applies a partial's raw bytes to it
func (s *StoreRegistry) ApplyPartialToStore(storeName string, partialBytes []byte) error {
	si, has := s.storeMap[storeName]
	if !has {
		return fmt.Errorf("no store found (%s) in ApplyPartialToStore", storeName)
	}
	return si.StoreData.DecodeAndApplyPartial(partialBytes)
}

// GetAllStoreNames returns all store names tracked
func (s *StoreRegistry) GetAllStoreNames() []string {
	return lo.Keys(s.storeMap)
}
