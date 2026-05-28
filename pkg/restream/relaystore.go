package restream

import (
	"sort"
	"sync"
)

// RelayStoreKeySubscriptionForwarder forwards a relay-store keyed subscription lifecycle change to its source device.
type RelayStoreKeySubscriptionForwarder func(storeName string, key string, subscribe bool)

// RelayStore is a simplified store that's just a relay storedata holder from a device's store
type RelayStore[S any, SP StoreDataPtrType[S], P Partial] struct {
	name string

	storeData *StoreData[S, SP, P]

	subscriptionMutex sync.Mutex
	activeKeySubCount map[string]int
	keySubForwarder   RelayStoreKeySubscriptionForwarder
}

// NewRelayStore returns a new RelayStore
func NewRelayStore[S any, SP StoreDataPtrType[S], P Partial](name string, baseState *S) *RelayStore[S, SP, P] {
	s := &RelayStore[S, SP, P]{
		name: name,
	}

	s.storeData = NewStoreData[S, SP, P](s, baseState)

	return s
}

// GetName implements Store.
func (s *RelayStore[S, SP, P]) GetName() string {
	return s.name
}

// GetStoreData implements Store.
func (s *RelayStore[S, SP, P]) GetStoreData() StoreDataBase {
	return s.storeData
}

// SubscribeToField implements the restream.Store interface
func (s *RelayStore[S, SP, P]) SubscribeToField(field []any, callback any) {
	s.storeData.SubscribeToField(field, callback)
}

// SetKeySubscriptionForwarder configures a callback for cloud-side keyed subscription lifecycle changes.
func (s *RelayStore[S, SP, P]) SetKeySubscriptionForwarder(forwarder RelayStoreKeySubscriptionForwarder) {
	s.subscriptionMutex.Lock()
	s.keySubForwarder = forwarder
	s.subscriptionMutex.Unlock()
}

// ActiveSubscriptionKeys returns the currently active keyed subscriptions for this relay store.
func (s *RelayStore[S, SP, P]) ActiveSubscriptionKeys() []string {
	s.subscriptionMutex.Lock()
	defer s.subscriptionMutex.Unlock()

	keys := make([]string, 0, len(s.activeKeySubCount))
	for key, count := range s.activeKeySubCount {
		if count > 0 {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

// SubscriptionStartedForKey implements KeySubscriptionAwareStore.
func (s *RelayStore[S, SP, P]) SubscriptionStartedForKey(key string) {
	var forwarder RelayStoreKeySubscriptionForwarder

	s.subscriptionMutex.Lock()
	if s.activeKeySubCount == nil {
		s.activeKeySubCount = map[string]int{}
	}
	s.activeKeySubCount[key]++
	if s.activeKeySubCount[key] == 1 {
		forwarder = s.keySubForwarder
	}
	s.subscriptionMutex.Unlock()

	if forwarder != nil {
		forwarder(s.name, key, true)
	}
}

// SubscriptionEndedForKey implements KeySubscriptionAwareStore.
func (s *RelayStore[S, SP, P]) SubscriptionEndedForKey(key string) {
	var forwarder RelayStoreKeySubscriptionForwarder

	s.subscriptionMutex.Lock()
	count := s.activeKeySubCount[key]
	switch {
	case count <= 0:
	case count == 1:
		delete(s.activeKeySubCount, key)
		forwarder = s.keySubForwarder
	default:
		s.activeKeySubCount[key]--
	}
	s.subscriptionMutex.Unlock()

	if forwarder != nil {
		forwarder(s.name, key, false)
	}
}

// GetPartialForSubscriptionKey implements SubscriptionKeyStateProvider.
func (s *RelayStore[S, SP, P]) GetPartialForSubscriptionKey(key string) (Partial, bool, error) {
	return partialForSubscriptionKeyFromState(s.storeData, key)
}
