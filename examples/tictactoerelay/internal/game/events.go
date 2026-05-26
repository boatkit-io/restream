package game

import (
	"reflect"
	"time"

	"github.com/boatkit-io/restream/pkg/restream"
	"github.com/boatkit-io/tugboat/pkg/subscribableevent"
)

const ServerTimeEventName = "ServerTime"

type ServerTimeCallback func(currentTime time.Time)

func RegisterServerTimeEvent(eventd *restream.EventDispatcher) *subscribableevent.Event[ServerTimeCallback] {
	event := subscribableevent.NewEvent[ServerTimeCallback]()
	eventd.RegisterEvent("ServerTime", &event, reflect.TypeFor[ServerTimeEvent](), reflect.TypeFor[func(time.Time)]())
	return &event
}
