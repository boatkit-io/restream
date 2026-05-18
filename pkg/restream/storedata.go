package restream

import (
	"fmt"
	"reflect"

	"github.com/boatkit-io/restream/pkg/binarystreams"
	"github.com/boatkit-io/restream/pkg/smartmutex"
	"github.com/boatkit-io/tugboat/pkg/subscribableevent"
)

// typeAnyArrayOfAny is a cached reflection type for []any
var typeAnyArrayOfAny = reflect.TypeFor[[]any]()

// PartialCallbackFunc is a reusable type for the callbacks for partial applications
type PartialCallbackFunc = func(storeName string, fields [][]any, partial Partial)

// StoreDataBase is a basic interface for all typed stores, since golang generics are so gimpy
type StoreDataBase interface {
	AddCallback(PartialCallbackFunc) subscribableevent.SubscriptionId
	RemoveCallback(subscribableevent.SubscriptionId) error
	DecodeAndSetFullState([]byte) error
	DecodeAndApplyPartial([]byte) error
	GetSerializedFullState() ([]byte, error)
}

var _ StoreDataBase = (*StoreData[fakeStruct, *fakeStruct, *fakePartial])(nil)

// StoreDataPtrType is a type constraint for asserting that it is both Serializable and a pointer to a store's state structure
type StoreDataPtrType[S any] interface {
	Serializable
	*S
}

// StoreData is a manager for a store's data.  The state structure itself is stored in the store, but the StoreData is created
// with a reference to the state.  StoreData then handles mutations to the store state (you're not allowed to directly touch
// store state, you have to use SetField), and allows subscriptions to changes via SubscribeToField.
type StoreData[S any, SP StoreDataPtrType[S], P Partial] struct {
	// Name is needed for callbacks
	name string

	// Mutex protecting access to the state struct
	stateMutex smartmutex.SmartMutex

	// Full (serializable) state structure (pointer)
	state SP

	// Perf optimization to hold the reflect.value for the base state
	stateReflect reflect.Value

	partialCallbacks subscribableevent.Event[PartialCallbackFunc]

	subscriptions *fieldSubTier
}

// AddCallback implements StoreDataBase.
func (d *StoreData[S, SP, PS]) AddCallback(cf func(storeName string, fields [][]any,
	partial Partial)) subscribableevent.SubscriptionId {
	return d.partialCallbacks.Subscribe(cf)
}

// RemoveCallback implements StoreDataBase.
func (d *StoreData[S, SP, PS]) RemoveCallback(sid subscribableevent.SubscriptionId) error {
	return d.partialCallbacks.Unsubscribe(sid)
}

// NewStoreData builds a new StoreData for a given named store, taking a reference to the store's state structure.
func NewStoreData[S any, SP StoreDataPtrType[S], P Partial](store Store, state SP) *StoreData[S, SP, P] {
	name := store.GetName()

	// Pre-calc the reflected state since we use it repeatedly
	ds := reflect.ValueOf(state)
	if ds.Kind() != reflect.Pointer {
		panic(fmt.Sprintf("Store %s passed non-pointer state structure", name))
	}
	ds = ds.Elem()
	if ds.Kind() != reflect.Struct {
		panic(fmt.Sprintf("Store %s passed non-pointer-to-struct state structure", name))
	}

	return &StoreData[S, SP, P]{
		name:         name,
		state:        state,
		stateReflect: ds,

		partialCallbacks: subscribableevent.NewEvent[PartialCallbackFunc](),
		subscriptions:    newFieldSubTier(nil),
	}
}

// GetSerializedFullState returns the full state from a storedata after serializing it while under a read mutex before returning
func (d *StoreData[S, SP, PS]) GetSerializedFullState() ([]byte, error) {
	var ret []byte
	var retError error
	d.ReadState(func(state SP) {
		w, b := binarystreams.NewMemoryWriter()
		if err := state.Serialize(w, nil); err != nil {
			retError = err
			return
		}
		if err := w.Flush(); err != nil {
			retError = err
			return
		}
		ret = b.Bytes()
	})
	return ret, retError
}

// ReadState is a helper to read the current state under a lock, calling the callback with the state
func (d *StoreData[S, SP, PS]) ReadState(cb func(SP)) {
	d.stateMutex.RLock()
	defer d.stateMutex.RUnlock()
	cb(d.state)
}

// getFieldValue is a helper to get a reflection value for a given field array.
func (d *StoreData[S, SP, PS]) getFieldValue(field []any) reflect.Value {
	ds := d.stateReflect
	for _, f := range field {
		fv := reflect.ValueOf(f)
		kind := fv.Kind()
		switch kind {
		case reflect.String:
			ds = ds.FieldByName(fv.String())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
			switch ds.Kind() {
			case reflect.Slice:
				var idx int
				switch kind { //nolint: exhaustive // Why: Other types not supported
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
					idx = int(fv.Uint())
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
					idx = int(fv.Int())
				}
				ds = ds.Index(idx)
			case reflect.Map:
				ds = ds.MapIndex(fv)
			default:
				panic(fmt.Sprintf("Invalid int type %+v for field %+v of set %+v", ds.Kind(), f, field))
			}
		default:
			panic(fmt.Sprintf("Invalid field type %+v for field %+v of set %+v", kind, f, field))
		}

		// Walk into the pointer to keep going
		if ds.Kind() == reflect.Pointer {
			ds = ds.Elem()
		}
	}

	return ds
}

// ApplyPartial is the only allowed way to mutate store state -- build a partial with whatever needs changing, and then
// apply it.  It will end up applied to store state and send to all subscribers.
func (d *StoreData[S, SP, PS]) ApplyPartial(partial PS) {
	var fields [][]any
	func() {
		d.stateMutex.Lock()
		defer d.stateMutex.Unlock()
		fields = partial.ApplyTo(d.state)
	}()

	d.partialCallbacks.Fire(d.name, fields, partial)

	for _, f := range fields {
		d.triggerSubs(f)
	}
}

// DecodeAndSetFullState will use reflection to decode the right state struct for the store and then set it
func (d *StoreData[S, SP, PS]) DecodeAndSetFullState(b []byte) error {
	t := reflect.TypeFor[S]()
	nrv := reflect.New(t)
	iv := nrv.Interface()
	if err := iv.(Serializable).Deserialize(binarystreams.NewReaderFromBytes(b), nil); err != nil {
		return err
	}
	d.stateMutex.Lock()
	defer d.stateMutex.Unlock()
	*d.state = *iv.(SP)
	return nil
}

// DecodeAndApplyPartial will use reflection to decode the right partial for the store and then apply it
func (d *StoreData[S, SP, PS]) DecodeAndApplyPartial(b []byte) error {
	t := reflect.TypeFor[PS]().Elem()
	nrv := reflect.New(t)
	iv := nrv.Interface()
	if err := iv.(Serializable).Deserialize(binarystreams.NewReaderFromBytes(b), nil); err != nil {
		return err
	}
	d.ApplyPartial(iv.(PS))
	return nil
}

// triggerSubs is an internal helper to break up triggering subscriptions from the field changes themselves
func (d *StoreData[S, SP, PS]) triggerSubs(field []any) {
	// Get the set of possible subscriptions to fire
	possibleSubs := make(map[*subInfo]bool)

	t := d.subscriptions
	for _, f := range field {
		for _, s := range t.subs {
			possibleSubs[s] = true
		}

		c, exists := t.children[f]
		if !exists {
			break
		}
		t = c
	}

	// Now filter each one to make sure it actually needs firing
	for s := range possibleSubs {
		// Only need to walk up the smaller of the two sets of fields and ensure equality up to that far -- any mismatches
		// above that level will always still be fine, in either direction.
		mismatch := false
		maxField := len(s.field)
		if len(field) < maxField {
			maxField = len(field)
		}
		for i := 0; i < maxField; i++ {
			if s.field[i] != field[i] {
				mismatch = true
				break
			}
		}
		if mismatch {
			continue
		}

		switch {
		case s.takesType && s.takesField:
			fv := d.getFieldValue(s.field)
			s.callback.Call([]reflect.Value{reflect.ValueOf(field), fv})
		case s.takesType:
			fv := d.getFieldValue(s.field)
			s.callback.Call([]reflect.Value{fv})
		case s.takesField:
			s.callback.Call([]reflect.Value{reflect.ValueOf(field)})
		default:
			s.callback.Call([]reflect.Value{})
		}
	}
}

// SubscribeToField subscribes to updates of the passed field and any entries under it.  The callback will be called with a
// copy of the entire data structure at the subscribed level, even if a subfield was updated, with the list of fields from the update.
func (d *StoreData[S, SP, PS]) SubscribeToField(field []any, callback any) {
	ct := reflect.TypeOf(callback)
	if ct.Kind() != reflect.Func {
		panic(fmt.Sprintf("Non-func (%T) passed to SubscribeToField for field %+v", callback, field))
	}

	ft := d.getFieldValue(field).Type()
	takesField := false
	takesType := false
	switch ct.NumIn() {
	case 2:
		takesType = true
		takesField = true
		if ct.In(1) != ft {
			panic(fmt.Sprintf("SubscribeToField callback for field %+v has wrong arg 1 type (got %s, expected %s)",
				field, ct.In(1).String(), ft.String()))
		}
	case 1:
		switch ct.In(0) {
		case typeAnyArrayOfAny:
			takesField = true
		case ft:
			takesType = true
		default:
			panic(fmt.Sprintf("SubscribeToField callback for field %+v has wrong arg 0 type (got %s, expected %s or %s)",
				field, ct.In(0).String(), typeAnyArrayOfAny.String(), ft.String()))
		}
	case 0:
		takesType = false
		takesField = false
	default:
		panic(fmt.Sprintf("SubscribeToField callback for field %+v has wrong number of args (got %d, expected 1 or 2)", field, ct.NumIn()))
	}

	si := &subInfo{
		field:      field,
		callback:   reflect.ValueOf(callback),
		takesType:  takesType,
		takesField: takesField,
	}

	// Find/build the sub tier for the requested field at full depth
	t := d.subscriptions
	for _, f := range field {
		c, exists := t.children[f]
		if !exists {
			c = newFieldSubTier(t)
			t.children[f] = c
		}
		t = c
	}

	// If you subscribe at a deep level (A,B,C), you need updates for A and A,B, so we want to walk all the way up the chain
	// and store subscriptions at A; A,B; and A,B,C.
	for {
		t.subs = append(t.subs, si)
		t = t.parent
		if t == nil {
			break
		}
	}
}

// subInfo is a helper structure for storing information about a subscription
type subInfo struct {
	field []any

	callback   reflect.Value
	takesField bool
	takesType  bool
}

// fieldSubTier holds a set of subscriptions for a "tier", which is what I'm thinking of as one branch of the field structure.
type fieldSubTier struct {
	parent   *fieldSubTier
	children map[any]*fieldSubTier

	subs []*subInfo
}

// newFieldSubTier returns a new subTier under a given parent.
func newFieldSubTier(parent *fieldSubTier) *fieldSubTier {
	return &fieldSubTier{
		parent:   parent,
		children: make(map[any]*fieldSubTier),
		subs:     []*subInfo{},
	}
}
