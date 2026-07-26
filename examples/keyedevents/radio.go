// Package keyedevents demonstrates an on-demand byte stream routed by an exact device key.
package keyedevents

import (
	"reflect"

	"github.com/boatkit-io/restream/pkg/restream"
	"github.com/boatkit-io/tugboat/pkg/subscribableevent"
)

const (
	RadioStoreName = "RadioStore"
	AudioEventName = "Radio.Audio"
)

// AudioCallback receives the source radio ID as routing metadata and one encoded audio chunk as its payload.
type AudioCallback func(radioID string, audio []byte)

// RegisterAudioEvent registers the typed keyed event and returns the source event that a radio implementation fires.
func RegisterAudioEvent(eventd *restream.EventDispatcher) *subscribableevent.Event[AudioCallback] {
	event := subscribableevent.NewEvent[AudioCallback]()
	eventd.RegisterKeyedEvent("Radio.Audio", &event, RadioStoreName, reflect.TypeFor[RadioAudioEvent](), reflect.TypeFor[func(string, []uint8)]())
	return &event
}
