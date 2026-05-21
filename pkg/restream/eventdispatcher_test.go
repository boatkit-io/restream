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
