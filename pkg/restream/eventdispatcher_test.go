package restream

import (
	"reflect"
	"testing"

	"github.com/boatkit-io/restream/pkg/binarystreams"
	"github.com/boatkit-io/tugboat/pkg/subscribableevent"
	"github.com/sirupsen/logrus"
)

type eventDispatcherTestCallback func(test int)
type keyedEventDispatcherTestCallback func(key string, test int)

func TestEventDispatcherSubscribesAndSerializesEvents(t *testing.T) {
	eventd := NewEventDispatcher(logrus.StandardLogger())
	event := subscribableevent.NewEvent[eventDispatcherTestCallback]()

	var gotName string
	var gotBytes []byte
	eventd.SubscribeToEvents(func(eventName string, eventBytes []byte) {
		gotName = eventName
		gotBytes = eventBytes
	})

	eventd.RegisterEvent("call", &event, reflect.TypeFor[callEvent](), reflect.TypeFor[func(int)]())
	event.Fire(4)

	if gotName != "call" {
		t.Fatalf("expected event name call, got %q", gotName)
	}

	var packet callEvent
	if err := packet.Deserialize(binarystreams.NewReaderFromBytes(gotBytes), nil); err != nil {
		t.Fatalf("event packet deserialize failed: %v", err)
	}
	if packet.Test != 4 {
		t.Fatalf("expected event packet test=4, got %d", packet.Test)
	}
}

func TestEventDispatcherFiresSerializedEvent(t *testing.T) {
	eventd := NewEventDispatcher(logrus.StandardLogger())
	event := subscribableevent.NewEvent[eventDispatcherTestCallback]()

	var gotTyped int
	event.Subscribe(func(test int) {
		gotTyped = test
	})

	var gotName string
	var gotBytes []byte
	eventd.SubscribeToEvents(func(eventName string, eventBytes []byte) {
		gotName = eventName
		gotBytes = eventBytes
	})

	eventd.RegisterEvent("call2", &event, reflect.TypeFor[call2Event](), reflect.TypeFor[func(int)]())

	eventBytes, err := SerializeToBytes(&call2Event{Test: 7}, nil)
	if err != nil {
		t.Fatalf("event packet serialize failed: %v", err)
	}
	if err := eventd.FireSerializedEvent("call2", eventBytes); err != nil {
		t.Fatalf("FireSerializedEvent failed: %v", err)
	}

	if gotTyped != 7 {
		t.Fatalf("expected typed event test=7, got %d", gotTyped)
	}
	if gotName != "call2" {
		t.Fatalf("expected event name call2, got %q", gotName)
	}

	var packet call2Event
	if err := packet.Deserialize(binarystreams.NewReaderFromBytes(gotBytes), nil); err != nil {
		t.Fatalf("event packet deserialize failed: %v", err)
	}
	if packet.Test != 7 {
		t.Fatalf("expected event packet test=7, got %d", packet.Test)
	}
}

func TestEventDispatcherFiresSerializedEventErrorsForUnknownEvent(t *testing.T) {
	eventd := NewEventDispatcher(logrus.StandardLogger())

	eventBytes, err := SerializeToBytes(&callEvent{Test: 7}, nil)
	if err != nil {
		t.Fatalf("event packet serialize failed: %v", err)
	}
	if err := eventd.FireSerializedEvent("missing", eventBytes); err == nil {
		t.Fatal("expected FireSerializedEvent to fail for unknown event")
	}
}

func TestEventDispatcherRefCountsAndRoutesKeyedEvents(t *testing.T) {
	eventd := NewEventDispatcher(logrus.StandardLogger())
	event := subscribableevent.NewEvent[keyedEventDispatcherTestCallback]()

	var transitions []string
	eventd.SubscribeToKeyedEventSubscriptions(func(storeName string, eventName string, key string, subscribed bool) {
		transitions = append(transitions, storeName+"/"+eventName+"/"+key+"/"+map[bool]string{true: "on", false: "off"}[subscribed])
	})

	type receivedEvent struct {
		storeName string
		eventName string
		key       string
		test      int
	}
	var received []receivedEvent
	eventd.SubscribeToKeyedEvents(func(storeName string, eventName string, key string, eventBytes []byte) {
		var packet keyedcallEvent
		if err := packet.Deserialize(binarystreams.NewReaderFromBytes(eventBytes), nil); err != nil {
			t.Fatalf("event packet deserialize failed: %v", err)
		}
		received = append(received, receivedEvent{
			storeName: storeName,
			eventName: eventName,
			key:       key,
			test:      packet.Test,
		})
	})

	eventd.RegisterKeyedEvent(
		"keyed.call",
		&event,
		"TestStore",
		reflect.TypeFor[keyedcallEvent](),
		reflect.TypeFor[func(string, int)](),
	)

	event.Fire("radio-a", 1)
	if len(received) != 0 {
		t.Fatalf("keyed event without listeners was dispatched: %+v", received)
	}

	if err := eventd.ListeningToKeyedEvent("TestStore", "keyed.call", "radio-a"); err != nil {
		t.Fatalf("ListeningToKeyedEvent failed: %v", err)
	}
	if err := eventd.ListeningToKeyedEvent("TestStore", "keyed.call", "radio-a"); err != nil {
		t.Fatalf("duplicate ListeningToKeyedEvent failed: %v", err)
	}
	if err := eventd.ListeningToKeyedEvent("TestStore", "keyed.call", "radio-b"); err != nil {
		t.Fatalf("second key ListeningToKeyedEvent failed: %v", err)
	}

	if got := eventd.GetActiveKeyedEventSubscriptions(); !reflect.DeepEqual(got, []KeyedEventSubscription{
		{StoreName: "TestStore", EventName: "keyed.call", Key: "radio-a"},
		{StoreName: "TestStore", EventName: "keyed.call", Key: "radio-b"},
	}) {
		t.Fatalf("active keyed event subscriptions = %+v", got)
	}

	event.Fire("radio-c", 2)
	event.Fire("radio-a", 3)
	if !reflect.DeepEqual(received, []receivedEvent{{
		storeName: "TestStore",
		eventName: "keyed.call",
		key:       "radio-a",
		test:      3,
	}}) {
		t.Fatalf("received keyed events = %+v", received)
	}

	if err := eventd.StopListeningToKeyedEvent("TestStore", "keyed.call", "radio-a"); err != nil {
		t.Fatalf("first StopListeningToKeyedEvent failed: %v", err)
	}
	event.Fire("radio-a", 4)
	if len(received) != 2 || received[1].test != 4 {
		t.Fatalf("refcounted keyed event was not dispatched: %+v", received)
	}
	if err := eventd.StopListeningToKeyedEvent("TestStore", "keyed.call", "radio-a"); err != nil {
		t.Fatalf("last StopListeningToKeyedEvent failed: %v", err)
	}
	event.Fire("radio-a", 5)
	if len(received) != 2 {
		t.Fatalf("keyed event was dispatched after last unsubscribe: %+v", received)
	}

	if !reflect.DeepEqual(transitions, []string{
		"TestStore/keyed.call/radio-a/on",
		"TestStore/keyed.call/radio-b/on",
		"TestStore/keyed.call/radio-a/off",
	}) {
		t.Fatalf("keyed event transitions = %+v", transitions)
	}
}

func TestEventDispatcherRoutesSerializedKeyedEventsOpaquely(t *testing.T) {
	eventd := NewEventDispatcher(nil)
	var got []byte
	eventd.SubscribeToKeyedEvents(func(storeName string, eventName string, key string, eventBytes []byte) {
		if storeName == "TestStore" && eventName == "audio" && key == "radio-a" {
			got = append([]byte(nil), eventBytes...)
		}
	})

	if err := eventd.FireSerializedKeyedEvent("TestStore", "audio", "radio-a", []byte{1, 2}); err != nil {
		t.Fatalf("FireSerializedKeyedEvent without subscribers failed: %v", err)
	}
	if got != nil {
		t.Fatalf("serialized keyed event without listeners was dispatched: %v", got)
	}

	if err := eventd.ListeningToKeyedEvent("TestStore", "audio", "radio-a"); err != nil {
		t.Fatalf("ListeningToKeyedEvent failed: %v", err)
	}
	if err := eventd.FireSerializedKeyedEvent("TestStore", "audio", "radio-a", []byte{1, 2}); err != nil {
		t.Fatalf("FireSerializedKeyedEvent failed: %v", err)
	}
	if !reflect.DeepEqual(got, []byte{1, 2}) {
		t.Fatalf("serialized keyed event payload = %v", got)
	}
}
