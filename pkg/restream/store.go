package restream

// Store is a basic interface for a store, to be tracked by the StoreRegistry
type Store interface {
	GetName() string
	GetStoreData() StoreDataBase
	SubscribeToField(field []any, callback any)
}

// SubscriptionAwareStore is a deeper interface for a store that cares about being subscription-aware
type SubscriptionAwareStore interface {
	SubscriptionStarted()
	SubscriptionEnded()
}

// KeySubscriptionAwareStore is implemented by stores that want per-subscription-key lifecycle callbacks.
type KeySubscriptionAwareStore interface {
	SubscriptionStartedForKey(key string)
	SubscriptionEndedForKey(key string)
}

// StateFieldPartialProvider is implemented by generated state structs that can create a partial snapshot for field paths.
type StateFieldPartialProvider interface {
	PartialForFields(fields [][]any) (Partial, bool)
}
