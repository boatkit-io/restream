package restream

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/boatkit-io/restream/pkg/binarystreams"
	"github.com/boatkit-io/restream/pkg/smartmutex"
	"github.com/boatkit-io/tugboat/pkg/subscribableevent"
	"github.com/sirupsen/logrus"
)

// @restream.Ignore
const subscribableEventPkgPath = "github.com/boatkit-io/tugboat/pkg/subscribableevent"

// EventCallbackFunc is called when any event registered on an EventDispatcher fires.
type EventCallbackFunc = func(eventName string, eventBytes []byte)

// KeyedEventCallbackFunc is called when a keyed event with active subscribers fires.
type KeyedEventCallbackFunc = func(storeName string, eventName string, key string, eventBytes []byte)

// KeyedEventSubscriptionCallbackFunc is called for aggregate keyed-event subscription transitions.
type KeyedEventSubscriptionCallbackFunc = func(storeName string, eventName string, key string, subscribed bool)

// KeyedEventSubscription identifies one store-owned keyed event subscription.
type KeyedEventSubscription struct {
	StoreName string
	EventName string
	Key       string
}

type keyedEventSubscriptionKey struct {
	storeName string
	eventName string
	key       string
}

type eventInfo struct {
	EventName       string
	EventValue      reflect.Value
	CallbackType    reflect.Type
	EventPacketType reflect.Type
	SubscriptionID  subscribableevent.SubscriptionId
	StoreName       string
	Keyed           bool
}

// EventDispatcher is a centralized registration point for server-originated events.
type EventDispatcher struct {
	log *logrus.Logger

	mutex       smartmutex.SmartMutex
	eventLookup map[string]eventInfo

	eventCallbacks                  subscribableevent.Event[EventCallbackFunc]
	keyedEventCallbacks             subscribableevent.Event[KeyedEventCallbackFunc]
	keyedEventSubscriptionCallbacks subscribableevent.Event[KeyedEventSubscriptionCallbackFunc]

	keyedSubscriptionMutex         sync.Mutex
	keyedSubscriptionCallbackMutex sync.Mutex
	activeKeyedSubscriptions       map[keyedEventSubscriptionKey]int
}

// NewEventDispatcher builds a new EventDispatcher.
func NewEventDispatcher(log *logrus.Logger) *EventDispatcher {
	return &EventDispatcher{
		log: log,

		mutex:                           smartmutex.SmartMutex{Name: "restream.EventDispatcher.mutex"},
		eventLookup:                     map[string]eventInfo{},
		eventCallbacks:                  subscribableevent.NewEvent[EventCallbackFunc](),
		keyedEventCallbacks:             subscribableevent.NewEvent[KeyedEventCallbackFunc](),
		keyedEventSubscriptionCallbacks: subscribableevent.NewEvent[KeyedEventSubscriptionCallbackFunc](),
		activeKeyedSubscriptions:        map[keyedEventSubscriptionKey]int{},
	}
}

// RegisterEvent subscribes this dispatcher to a subscribableevent.Event. The first generatedTypes entry must be the
// generated event packet struct type. A second callback type may be provided for registration-time signature validation.
func (d *EventDispatcher) RegisterEvent(name string, event any, generatedTypes ...reflect.Type) {
	d.registerEvent(name, event, "", false, generatedTypes...)
}

// RegisterKeyedEvent registers a store-owned event whose first callback argument is its string subscription key.
// The key is routing metadata and is not included in the serialized event packet. Keyed events are serialized and
// dispatched only while at least one listener is subscribed to that exact store/event/key tuple.
func (d *EventDispatcher) RegisterKeyedEvent(
	name string,
	event any,
	storeName string,
	generatedTypes ...reflect.Type,
) {
	if strings.TrimSpace(storeName) == "" {
		panic("RegisterKeyedEvent requires a store name for " + name)
	}
	d.registerEvent(name, event, storeName, true, generatedTypes...)
}

func (d *EventDispatcher) registerEvent(
	name string,
	event any,
	storeName string,
	keyed bool,
	generatedTypes ...reflect.Type,
) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	if _, exists := d.eventLookup[name]; exists {
		panic("Double-registration of event: " + name)
	}
	if len(generatedTypes) == 0 || generatedTypes[0] == nil {
		panic("RegisterEvent requires a generated event packet type for " + name)
	}

	eventPacketType := generatedTypes[0]
	if eventPacketType.Kind() != reflect.Struct {
		panic(fmt.Sprintf("Event packet type for %s must be a struct, got %+v", name, eventPacketType))
	}
	if _, ok := reflect.New(eventPacketType).Interface().(Serializable); !ok {
		panic(fmt.Sprintf("Event packet type for %s does not implement Serializable: %+v", name, eventPacketType))
	}

	eventValue, subscribeMethod, callbackType := eventSubscribeMethod(event)
	if len(generatedTypes) > 1 && generatedTypes[1] != nil && !sameFuncSignature(generatedTypes[1], callbackType) {
		panic(fmt.Sprintf("Event callback type for %s is %+v, generated type was %+v", name, callbackType, generatedTypes[1]))
	}
	if callbackType.NumOut() != 0 {
		panic(fmt.Sprintf("Event callback type for %s must not return values: %+v", name, callbackType))
	}
	packetArgumentOffset := 0
	if keyed {
		packetArgumentOffset = 1
	}
	if callbackType.NumIn()-packetArgumentOffset != eventPacketType.NumField() {
		panic(fmt.Sprintf(
			"Event %s has %d serialized params but packet type %+v has %d fields",
			name, callbackType.NumIn()-packetArgumentOffset, eventPacketType, eventPacketType.NumField(),
		))
	}
	if keyed && (callbackType.NumIn() == 0 || callbackType.In(0).Kind() != reflect.String) {
		panic(fmt.Sprintf("Keyed event %s must have a string key as its first callback argument", name))
	}

	callback := reflect.MakeFunc(callbackType, func(args []reflect.Value) []reflect.Value {
		d.fireEvent(name, storeName, keyed, eventPacketType, args)
		return nil
	})
	subscriptionIDRaw := subscribeMethod.Call([]reflect.Value{callback})[0]
	subscriptionID := subscriptionIDRaw.Interface().(subscribableevent.SubscriptionId)

	d.eventLookup[name] = eventInfo{
		EventName:       name,
		EventValue:      eventValue,
		CallbackType:    callbackType,
		EventPacketType: eventPacketType,
		SubscriptionID:  subscriptionID,
		StoreName:       storeName,
		Keyed:           keyed,
	}
}

// SubscribeToEvents adds a subscription to all events fired through the dispatcher.
func (d *EventDispatcher) SubscribeToEvents(cb EventCallbackFunc) subscribableevent.SubscriptionId {
	return d.eventCallbacks.Subscribe(cb)
}

// UnsubscribeFromEvents removes a subscription created with SubscribeToEvents.
func (d *EventDispatcher) UnsubscribeFromEvents(sid subscribableevent.SubscriptionId) error {
	return d.eventCallbacks.Unsubscribe(sid)
}

// SubscribeToKeyedEvents adds a subscription to serialized keyed events that have active listeners.
func (d *EventDispatcher) SubscribeToKeyedEvents(cb KeyedEventCallbackFunc) subscribableevent.SubscriptionId {
	return d.keyedEventCallbacks.Subscribe(cb)
}

// UnsubscribeFromKeyedEvents removes a subscription created with SubscribeToKeyedEvents.
func (d *EventDispatcher) UnsubscribeFromKeyedEvents(sid subscribableevent.SubscriptionId) error {
	return d.keyedEventCallbacks.Unsubscribe(sid)
}

// SubscribeToKeyedEventSubscriptions observes aggregate exact-key 0-to-1 and 1-to-0 subscription transitions.
func (d *EventDispatcher) SubscribeToKeyedEventSubscriptions(
	cb KeyedEventSubscriptionCallbackFunc,
) subscribableevent.SubscriptionId {
	return d.keyedEventSubscriptionCallbacks.Subscribe(cb)
}

// UnsubscribeFromKeyedEventSubscriptions removes a keyed-event subscription transition callback.
func (d *EventDispatcher) UnsubscribeFromKeyedEventSubscriptions(
	sid subscribableevent.SubscriptionId,
) error {
	return d.keyedEventSubscriptionCallbacks.Unsubscribe(sid)
}

// ListeningToKeyedEvent adds one listener to a store-owned keyed event.
func (d *EventDispatcher) ListeningToKeyedEvent(storeName string, eventName string, key string) error {
	subKey, err := validatedKeyedEventSubscriptionKey(storeName, eventName, key)
	if err != nil {
		return err
	}

	d.keyedSubscriptionMutex.Lock()
	d.activeKeyedSubscriptions[subKey]++
	first := d.activeKeyedSubscriptions[subKey] == 1
	d.keyedSubscriptionMutex.Unlock()

	if first {
		d.keyedSubscriptionCallbackMutex.Lock()
		d.keyedEventSubscriptionCallbacks.Fire(storeName, eventName, key, true)
		d.keyedSubscriptionCallbackMutex.Unlock()
	}
	return nil
}

// StopListeningToKeyedEvent removes one listener from a store-owned keyed event.
func (d *EventDispatcher) StopListeningToKeyedEvent(storeName string, eventName string, key string) error {
	subKey, err := validatedKeyedEventSubscriptionKey(storeName, eventName, key)
	if err != nil {
		return err
	}

	d.keyedSubscriptionMutex.Lock()
	count := d.activeKeyedSubscriptions[subKey]
	if count == 0 {
		d.keyedSubscriptionMutex.Unlock()
		return fmt.Errorf("no active keyed event subscription for %s/%s/%s", storeName, eventName, key)
	}
	count--
	last := count == 0
	if last {
		delete(d.activeKeyedSubscriptions, subKey)
	} else {
		d.activeKeyedSubscriptions[subKey] = count
	}
	d.keyedSubscriptionMutex.Unlock()

	if last {
		d.keyedSubscriptionCallbackMutex.Lock()
		d.keyedEventSubscriptionCallbacks.Fire(storeName, eventName, key, false)
		d.keyedSubscriptionCallbackMutex.Unlock()
	}
	return nil
}

// HasKeyedEventSubscribers reports whether an exact store/event/key tuple has at least one active listener.
func (d *EventDispatcher) HasKeyedEventSubscribers(storeName string, eventName string, key string) bool {
	subKey := keyedEventSubscriptionKey{storeName: storeName, eventName: eventName, key: key}
	d.keyedSubscriptionMutex.Lock()
	has := d.activeKeyedSubscriptions[subKey] > 0
	d.keyedSubscriptionMutex.Unlock()
	return has
}

// GetActiveKeyedEventSubscriptions returns all exact keyed-event subscriptions with active listeners.
func (d *EventDispatcher) GetActiveKeyedEventSubscriptions() []KeyedEventSubscription {
	d.keyedSubscriptionMutex.Lock()
	ret := make([]KeyedEventSubscription, 0, len(d.activeKeyedSubscriptions))
	for subKey, count := range d.activeKeyedSubscriptions {
		if count == 0 {
			continue
		}
		ret = append(ret, KeyedEventSubscription{
			StoreName: subKey.storeName,
			EventName: subKey.eventName,
			Key:       subKey.key,
		})
	}
	d.keyedSubscriptionMutex.Unlock()

	sort.Slice(ret, func(i int, j int) bool {
		if ret[i].StoreName != ret[j].StoreName {
			return ret[i].StoreName < ret[j].StoreName
		}
		if ret[i].EventName != ret[j].EventName {
			return ret[i].EventName < ret[j].EventName
		}
		return ret[i].Key < ret[j].Key
	})
	return ret
}

// FireSerializedEvent deserializes a generated event packet and fires the registered typed event.
func (d *EventDispatcher) FireSerializedEvent(name string, eventBytes []byte) error {
	d.mutex.RLock()
	info, exists := d.eventLookup[name]
	d.mutex.RUnlock()
	if !exists {
		return fmt.Errorf("unknown event %s", name)
	}
	if info.Keyed {
		return fmt.Errorf("keyed event %s requires FireSerializedKeyedEvent", name)
	}

	eventPacketValue := reflect.New(info.EventPacketType)
	eventPacket := eventPacketValue.Interface().(Serializable)
	if err := eventPacket.Deserialize(binarystreams.NewReaderFromBytes(eventBytes), nil); err != nil {
		return err
	}

	eventPacketElem := eventPacketValue.Elem()
	args := make([]reflect.Value, eventPacketElem.NumField())
	for idx := 0; idx < eventPacketElem.NumField(); idx++ {
		arg := eventPacketElem.Field(idx)
		callbackArgType := info.CallbackType.In(idx)
		if !arg.Type().AssignableTo(callbackArgType) {
			if !arg.Type().ConvertibleTo(callbackArgType) {
				return fmt.Errorf("event %s field %d has type %+v, cannot assign to callback arg %+v", name, idx, arg.Type(), callbackArgType)
			}
			arg = arg.Convert(callbackArgType)
		}
		args[idx] = arg
	}

	info.EventValue.MethodByName("Fire").Call(args)
	return nil
}

// FireSerializedKeyedEvent dispatches an already serialized store-owned keyed event without requiring a typed
// registration on this dispatcher. This is used by relay servers, which route application event payloads opaquely.
func (d *EventDispatcher) FireSerializedKeyedEvent(
	storeName string,
	eventName string,
	key string,
	eventBytes []byte,
) error {
	if _, err := validatedKeyedEventSubscriptionKey(storeName, eventName, key); err != nil {
		return err
	}
	if !d.HasKeyedEventSubscribers(storeName, eventName, key) {
		return nil
	}
	d.keyedEventCallbacks.Fire(storeName, eventName, key, eventBytes)
	return nil
}

func validatedKeyedEventSubscriptionKey(
	storeName string,
	eventName string,
	key string,
) (keyedEventSubscriptionKey, error) {
	if strings.TrimSpace(storeName) == "" {
		return keyedEventSubscriptionKey{}, fmt.Errorf("keyed event store name is empty")
	}
	if strings.TrimSpace(eventName) == "" {
		return keyedEventSubscriptionKey{}, fmt.Errorf("keyed event name is empty")
	}
	if key == "" {
		return keyedEventSubscriptionKey{}, fmt.Errorf("keyed event key is empty")
	}
	return keyedEventSubscriptionKey{storeName: storeName, eventName: eventName, key: key}, nil
}

func eventSubscribeMethod(event any) (reflect.Value, reflect.Value, reflect.Type) {
	if event == nil {
		panic("nil passed to RegisterEvent")
	}

	eventValue := reflect.ValueOf(event)
	eventType := eventValue.Type()
	if eventType.Kind() != reflect.Pointer {
		panic(fmt.Sprintf("Non-pointer subscribableevent.Event passed to RegisterEvent: %+v", eventValue.Type()))
	}
	eventType = eventType.Elem()
	if eventType.PkgPath() != subscribableEventPkgPath || !strings.HasPrefix(eventType.Name(), "Event[") {
		panic(fmt.Sprintf("Non-subscribableevent.Event passed to RegisterEvent: %+v", eventValue.Type()))
	}

	subscribeMethod := eventValue.MethodByName("Subscribe")
	if !subscribeMethod.IsValid() {
		panic(fmt.Sprintf("subscribableevent.Event passed to RegisterEvent has no Subscribe method: %+v", eventValue.Type()))
	}

	subscribeType := subscribeMethod.Type()
	if subscribeType.NumIn() != 1 || subscribeType.NumOut() != 1 {
		panic(fmt.Sprintf("Invalid Subscribe method on event passed to RegisterEvent: %+v", subscribeType))
	}

	callbackType := subscribeType.In(0)
	if callbackType.Kind() != reflect.Func {
		panic(fmt.Sprintf("Subscribe callback must be a function for RegisterEvent: %+v", callbackType))
	}
	if subscribeType.Out(0) != reflect.TypeFor[subscribableevent.SubscriptionId]() {
		panic(fmt.Sprintf("Subscribe must return subscribableevent.SubscriptionId for RegisterEvent: %+v", subscribeType))
	}

	return eventValue, subscribeMethod, callbackType
}

func sameFuncSignature(a, b reflect.Type) bool {
	if a.Kind() != reflect.Func || b.Kind() != reflect.Func {
		return false
	}
	if a.NumIn() != b.NumIn() || a.NumOut() != b.NumOut() {
		return false
	}
	for idx := 0; idx < a.NumIn(); idx++ {
		if a.In(idx) != b.In(idx) {
			return false
		}
	}
	for idx := 0; idx < a.NumOut(); idx++ {
		if a.Out(idx) != b.Out(idx) {
			return false
		}
	}
	return true
}

func (d *EventDispatcher) fireEvent(
	name string,
	storeName string,
	keyed bool,
	eventPacketType reflect.Type,
	args []reflect.Value,
) {
	key := ""
	if keyed {
		key = args[0].String()
		if !d.HasKeyedEventSubscribers(storeName, name, key) {
			return
		}
	}

	eventPacketValue := reflect.New(eventPacketType)
	eventPacketElem := eventPacketValue.Elem()

	packetArgs := args
	if keyed {
		packetArgs = args[1:]
	}
	for idx, arg := range packetArgs {
		field := eventPacketElem.Field(idx)
		if !arg.Type().AssignableTo(field.Type()) {
			if !arg.Type().ConvertibleTo(field.Type()) {
				panic(fmt.Sprintf("Event %s arg %d has type %+v, cannot assign to packet field %+v", name, idx, arg.Type(), field.Type()))
			}
			arg = arg.Convert(field.Type())
		}
		field.Set(arg)
	}

	eventPacket := eventPacketValue.Interface().(Serializable)
	eventBytes, err := SerializeToBytes(eventPacket, nil)
	if err != nil {
		if d.log != nil {
			d.log.Errorf("Error serializing event %s: %+v", name, err)
		}
		return
	}

	if keyed {
		d.keyedEventCallbacks.Fire(storeName, name, key, eventBytes)
		return
	}
	d.eventCallbacks.Fire(name, eventBytes)
}
