package restream

import (
	"reflect"
	"testing"

	"github.com/boatkit-io/restream/pkg/binarystreams"
	"github.com/boatkit-io/tugboat/pkg/subscribableevent"
	"github.com/sirupsen/logrus"
)

type eventDispatcherTestCallback func(test int)

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
