# Fire-and-Forget RPCs

This focused example models microphone audio frames sent from a browser to a radio. An FFRPC uses Restream's generated request serialization and access checks but carries no call ID and produces no response.

Run the executable example and its assertions:

```bash
go test ./...
```

The Go receiver registers a request-only handler:

```go
rpcd.RegisterFFRPCHandler(
    "Radio.TransmitAudio",
    AccessLevelAdmin,
    func(radioID string, sequence uint32, audio []byte) error {
        return radio.OnAudio(radioID, sequence, audio)
    },
    reflect.TypeFor[RadioTransmitAudioRequest](),
)
```

After code generation, the browser sends the generated request without creating a promise:

```typescript
socket.sendFFRPC(
  RadioTransmitAudioRequest.fromValues("radio-a", sequence, encodedAudio),
);
```

Pass `rpcd.FireFFRPC` as the optional final argument to `restream.AddSocketHandlers`. For a device relay streamer, pass it as the optional final argument to `relayclient.NewStreamer`. Cloud viewers use `device.FFRPCHandler`, which forwards the request without allocating relay response state.

FFRPC execution is detached on the receiver. Callers receive no application error or completion signal, so use a normal RPC for operations that must be acknowledged.
