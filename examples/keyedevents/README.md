# Keyed Events

This focused example models an on-demand radio audio stream. The radio ID is an exact subscription key; only the audio byte slice is serialized in the generated event packet, and the browser receives that Go `[]byte` as a `Uint8Array`.

Run the sample and its assertions:

```bash
go test ./...
```

`ExampleRegisterAudioEvent` prints one subscribed radio's packet and is checked as a Go executable example. `TestAudioEventUsesExactKeysAndRefcounts` additionally verifies:

- events are suppressed before the first subscription;
- another radio's audio does not fan out;
- duplicate subscriptions produce one start transition;
- delivery continues until the final unsubscribe;
- the routing key is not duplicated in the generated payload.

The complete registration is in `radio.go`:

```go
type AudioCallback func(radioID string, audio []byte)

func RegisterAudioEvent(eventd *restream.EventDispatcher) *subscribableevent.Event[AudioCallback] {
	event := subscribableevent.NewEvent[AudioCallback]()
	eventd.RegisterKeyedEvent(
		"Radio.Audio",
		&event,
		RadioStoreName,
		reflect.TypeFor[RadioAudioEvent](),
		reflect.TypeFor[func(string, []byte)](),
	)
	return &event
}
```

A server passes that dispatcher to `restream.AddSocketHandlers`. A generated browser client then selects the same exact radio:

```typescript
const unsubscribe = rss.subscribeToKeyedEvent(
  RadioStoreName,
  RadioAudioEvent,
  "00:11:22:33:44:55",
  event => audioPlayer.write(event.audio ?? new Uint8Array()),
);
```

In an actual store, use `SubscribeToKeyedEventSubscriptions` to turn the radio's network audio subscription on at the aggregate start transition and off at the aggregate stop transition.

Regenerate the packet after changing `AudioCallback`:

```bash
go tool github.com/boatkit-io/restream/cmd/codegen -project .
```
