package keyedevents

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/boatkit-io/restream/pkg/binarystreams"
	"github.com/boatkit-io/restream/pkg/restream"
)

func ExampleRegisterAudioEvent() {
	eventd := restream.NewEventDispatcher(nil)
	audioEvent := RegisterAudioEvent(eventd)

	eventd.SubscribeToKeyedEvents(func(_ string, _ string, radioID string, eventBytes []byte) {
		var event RadioAudioEvent
		if err := event.Deserialize(binarystreams.NewReaderFromBytes(eventBytes), nil); err != nil {
			panic(err)
		}
		fmt.Printf("%s: %v\n", radioID, event.Audio)
	})

	if err := eventd.ListeningToKeyedEvent(RadioStoreName, AudioEventName, "radio-a"); err != nil {
		panic(err)
	}
	audioEvent.Fire("radio-b", []byte{9, 9, 9})
	audioEvent.Fire("radio-a", []byte{1, 2, 3})

	// Output:
	// radio-a: [1 2 3]
}

func TestAudioEventUsesExactKeysAndRefcounts(t *testing.T) {
	eventd := restream.NewEventDispatcher(nil)
	audioEvent := RegisterAudioEvent(eventd)

	var transitions []string
	eventd.SubscribeToKeyedEventSubscriptions(func(_ string, _ string, key string, subscribed bool) {
		transitions = append(transitions, fmt.Sprintf("%s:%t", key, subscribed))
	})

	type receivedAudio struct {
		key   string
		audio []byte
	}
	var received []receivedAudio
	eventd.SubscribeToKeyedEvents(func(_ string, _ string, key string, eventBytes []byte) {
		var event RadioAudioEvent
		if err := event.Deserialize(binarystreams.NewReaderFromBytes(eventBytes), nil); err != nil {
			t.Fatalf("deserialize audio event: %v", err)
		}
		received = append(received, receivedAudio{key: key, audio: event.Audio})
	})

	audioEvent.Fire("radio-a", []byte{0})
	mustNoError(t, eventd.ListeningToKeyedEvent(RadioStoreName, AudioEventName, "radio-a"))
	mustNoError(t, eventd.ListeningToKeyedEvent(RadioStoreName, AudioEventName, "radio-a"))
	mustNoError(t, eventd.ListeningToKeyedEvent(RadioStoreName, AudioEventName, "radio-b"))

	audioEvent.Fire("radio-c", []byte{1})
	audioEvent.Fire("radio-a", []byte{2, 3})
	if !reflect.DeepEqual(received, []receivedAudio{{key: "radio-a", audio: []byte{2, 3}}}) {
		t.Fatalf("received audio = %#v", received)
	}

	mustNoError(t, eventd.StopListeningToKeyedEvent(RadioStoreName, AudioEventName, "radio-a"))
	audioEvent.Fire("radio-a", []byte{4})
	if !reflect.DeepEqual(received, []receivedAudio{
		{key: "radio-a", audio: []byte{2, 3}},
		{key: "radio-a", audio: []byte{4}},
	}) {
		t.Fatalf("audio stopped before final unsubscribe: %#v", received)
	}

	mustNoError(t, eventd.StopListeningToKeyedEvent(RadioStoreName, AudioEventName, "radio-a"))
	audioEvent.Fire("radio-a", []byte{5})
	if len(received) != 2 {
		t.Fatalf("audio continued after final unsubscribe: %#v", received)
	}

	if !reflect.DeepEqual(transitions, []string{"radio-a:true", "radio-b:true", "radio-a:false"}) {
		t.Fatalf("subscription transitions = %#v", transitions)
	}

	eventBytes, err := restream.SerializeToBytes(&RadioAudioEvent{Audio: []byte{6, 7}}, nil)
	mustNoError(t, err)
	if len(eventBytes) != 4 {
		t.Fatalf("serialized payload length = %d, want 4 bytes (array framing + audio only)", len(eventBytes))
	}
}

func mustNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
