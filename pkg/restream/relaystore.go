package restream

// RelayStore is a simplified store that's just a relay storedata holder from a device's store
type RelayStore[S any, SP StoreDataPtrType[S], P Partial] struct {
	name string

	storeData *StoreData[S, SP, P]
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

// GetPartialForSubscriptionKey implements SubscriptionKeyStateProvider.
func (s *RelayStore[S, SP, P]) GetPartialForSubscriptionKey(key string) (Partial, bool, error) {
	return partialForSubscriptionKeyFromState(s.storeData, key)
}
