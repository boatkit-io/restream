package restream

import (
	"errors"
	"fmt"
	"sync"

	"github.com/boatkit-io/tugboat/pkg/subscribableevent"
	"github.com/samber/lo"
)

// StoreRegistry is a centralized registry of all Stores, used for coordination with subscriptions from
// connected web clients, since it all happens by strings over the wire -- so it needs to be able to look up a
// Store/StoreData by name.
type StoreRegistry struct {
	storeMap          map[string]*StoreInfo
	subscriptionMutex sync.Mutex

	partialApplyCallbacks subscribableevent.Event[PartialCallbackFunc]
}

// StoreInfo holds info about a store for the StoreRegistry
type StoreInfo struct {
	Name              string
	StoreData         StoreDataBase
	Store             Store
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

// NewStoreRegistry brings up a StoreRegistry, holding an explicit list of Stores/StoreDatas (this list may not grow
// over time -- has to be initted up front).
func NewStoreRegistry(storeList []Store) (*StoreRegistry, error) {
	sdr := &StoreRegistry{
		storeMap: map[string]*StoreInfo{},

		partialApplyCallbacks: subscribableevent.NewEvent[PartialCallbackFunc](),
	}

	for _, s := range storeList {
		si := StoreInfo{
			Name:           s.GetName(),
			Store:          s,
			StoreData:      s.GetStoreData(),
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

// PartialCallback is a callback for any Partial application for all storedatas in the SDR
func (s *StoreRegistry) PartialCallback(storeName string, fields [][]any, partial Partial) {
	s.partialApplyCallbacks.Fire(storeName, fields, partial)
}

// IsStoreValid checks to see if a store is valid
func (s *StoreRegistry) IsStoreValid(storeName string) bool {
	_, has := s.storeMap[storeName]
	return has
}

// GetSerializedFullState returns a pre-serialized full state object for a store.
func (s *StoreRegistry) GetSerializedFullState(storeName string, accessLevel AccessLevel) ([]byte, error) {
	si, has := s.storeMap[storeName]
	if !has {
		return nil, fmt.Errorf("no store found (%s) in GetSerializedFullState", storeName)
	}
	if err := requireStoreAccess(si, accessLevel); err != nil {
		return nil, err
	}

	return si.StoreData.GetSerializedFullState()
}

// GetSerializedPartialForSubscriptionKey returns an initial keyed partial for a store, when the store supports it.
func (s *StoreRegistry) GetSerializedPartialForSubscriptionKey(storeName string, key string, accessLevel AccessLevel) ([]byte, bool, error) {
	si, has := s.storeMap[storeName]
	if !has {
		return nil, false, fmt.Errorf("no store found (%s) in GetSerializedPartialForSubscriptionKey", storeName)
	}
	if err := requireStoreAccess(si, accessLevel); err != nil {
		return nil, false, err
	}
	if provider, ok := si.StoreData.(subscriptionKeyStoreData); ok {
		return provider.GetSerializedPartialForSubscriptionKey(key)
	}
	return nil, false, nil
}

// ListeningToStore is a callback to indicate that someone has subscribed to the store
func (s *StoreRegistry) ListeningToStore(storeName string, accessLevel AccessLevel) error {
	return s.ListeningToStoreKey(storeName, "", accessLevel)
}

// ListeningToStoreKey is a callback to indicate that someone has subscribed to a store key.
func (s *StoreRegistry) ListeningToStoreKey(storeName string, key string, accessLevel AccessLevel) error {
	s.subscriptionMutex.Lock()
	defer s.subscriptionMutex.Unlock()

	si, has := s.storeMap[storeName]
	if !has {
		return fmt.Errorf("no store found (%s) in ListeningToStoreKey", storeName)
	}
	if err := requireStoreAccess(si, accessLevel); err != nil {
		return err
	}

	si.ActiveSubCount++
	if si.ActiveKeySubCount == nil {
		si.ActiveKeySubCount = map[string]int{}
	}
	si.ActiveKeySubCount[key]++
	if si.ActiveKeySubCount[key] == 1 && si.KeySubCallbacks != nil {
		si.KeySubCallbacks.SubscriptionStartedForKey(key)
	}
	if si.ActiveSubCount == 1 && si.SubAwareCallbacks != nil {
		si.SubAwareCallbacks.SubscriptionStarted()
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
	defer s.subscriptionMutex.Unlock()

	si, has := s.storeMap[storeName]
	if !has {
		return fmt.Errorf("no store found (%s) in StopListeningToStoreKey", storeName)
	}

	if si.ActiveSubCount == 0 {
		return fmt.Errorf("active sub count 0 in StopListeningToStoreKey for %s", storeName)
	}
	if si.ActiveKeySubCount[key] == 0 {
		return fmt.Errorf("active key sub count 0 in StopListeningToStoreKey for %s/%s", storeName, key)
	}
	si.ActiveSubCount--
	si.ActiveKeySubCount[key]--
	if si.ActiveKeySubCount[key] == 0 {
		delete(si.ActiveKeySubCount, key)
		if si.KeySubCallbacks != nil {
			si.KeySubCallbacks.SubscriptionEndedForKey(key)
		}
	}
	if si.ActiveSubCount == 0 && si.SubAwareCallbacks != nil {
		si.SubAwareCallbacks.SubscriptionEnded()
	}
	return nil
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
	return si.StoreData.DecodeAndSetFullState(stateBytes)
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
