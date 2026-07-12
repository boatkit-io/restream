package restream

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/boatkit-io/restream/pkg/binarystreams"
	"github.com/boatkit-io/restream/pkg/smartmutex"
	"github.com/boatkit-io/tugboat/pkg/subscribableevent"
	"github.com/sirupsen/logrus"
)

// @restream.Ignore
const subscribableEventPkgPath = "github.com/boatkit-io/tugboat/pkg/subscribableevent"

// EventCallbackFunc is called when any event registered on an EventDispatcher fires.
type EventCallbackFunc = func(eventName string, eventBytes []byte)

type eventInfo struct {
	EventName       string
	EventValue      reflect.Value
	CallbackType    reflect.Type
	EventPacketType reflect.Type
	SubscriptionID  subscribableevent.SubscriptionId
}

// EventDispatcher is a centralized registration point for server-originated events.
type EventDispatcher struct {
	log *logrus.Logger

	mutex       smartmutex.SmartMutex
	eventLookup map[string]eventInfo

	eventCallbacks subscribableevent.Event[EventCallbackFunc]
}

// NewEventDispatcher builds a new EventDispatcher.
func NewEventDispatcher(log *logrus.Logger) *EventDispatcher {
	return &EventDispatcher{
		log: log,

		mutex:          smartmutex.SmartMutex{Name: "restream.EventDispatcher.mutex"},
		eventLookup:    map[string]eventInfo{},
		eventCallbacks: subscribableevent.NewEvent[EventCallbackFunc](),
	}
}

// RegisterEvent subscribes this dispatcher to a subscribableevent.Event. The first generatedTypes entry must be the
// generated event packet struct type. A second callback type may be provided for registration-time signature validation.
func (d *EventDispatcher) RegisterEvent(name string, event any, generatedTypes ...reflect.Type) {
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
	if callbackType.NumIn() != eventPacketType.NumField() {
		panic(fmt.Sprintf(
			"Event %s has %d params but packet type %+v has %d fields",
			name, callbackType.NumIn(), eventPacketType, eventPacketType.NumField(),
		))
	}

	callback := reflect.MakeFunc(callbackType, func(args []reflect.Value) []reflect.Value {
		d.fireEvent(name, eventPacketType, args)
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

// FireSerializedEvent deserializes a generated event packet and fires the registered typed event.
func (d *EventDispatcher) FireSerializedEvent(name string, eventBytes []byte) error {
	d.mutex.RLock()
	info, exists := d.eventLookup[name]
	d.mutex.RUnlock()
	if !exists {
		return fmt.Errorf("unknown event %s", name)
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

func (d *EventDispatcher) fireEvent(name string, eventPacketType reflect.Type, args []reflect.Value) {
	eventPacketValue := reflect.New(eventPacketType)
	eventPacketElem := eventPacketValue.Elem()

	for idx, arg := range args {
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

	d.eventCallbacks.Fire(name, eventBytes)
}
