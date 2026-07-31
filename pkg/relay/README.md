# ReStream Relay

The relay packages are split into three layers:

- `protocol`: versioned device-to-relay packets.
- `client`: device-side streamer from a local `restream.StoreRegistry` to a relay websocket.
- `server`: cloud-side websocket acceptor and optional per-device relay data manager.

High-bandwidth data uses the same authenticated Restream subscription lifecycle
without entering the state/event packet queues:

- A viewer sends a `datastreamsub` request for a store-owned stream and exact
  key. `AddSocketHandlersWithOptions` checks the owning store's access level,
  asks a `DataStreamBroker` for a viewer-specific endpoint lease, and releases
  it on unsubscribe or socket disconnect.
- `server.DeviceDataStreamBroker` refcounts those viewer leases and forwards
  only aggregate asynchronous transitions to the device. Each transition has
  an operation ID and completion result. Exact-stream changes are serialized,
  and active sources are restored with acknowledged retries after reconnect.
- The device registers each stream and its independent minimum access level
  with `restream.DataStreamDispatcher`. Registration access is checked using
  the authenticated viewer access forwarded by the cloud, so a public metadata
  store can still expose an admin-only media stream.
- Device handlers run on per-stream workers, not the relay packet reader.
  Different streams may start concurrently, while start/stop changes for the
  same stream complete in order. Handlers must honor `context.Context` and make
  both operations idempotent.
- Producers send `pkg/datastream.Envelope` records over a separate bounded
  data-plane connection. Payload bytes never enter the normal relay `Streamer`
  queue.

Device registration looks like:

```go
streams := restream.NewDataStreamDispatcher()
streams.RegisterDataStream(
    "CameraStore",
    "Camera.Video",
    restream.AccessLevel(3), // application admin level
    func(ctx context.Context, cameraID string, subscribe bool) error {
        return cameraMedia.SetRelayStreaming(ctx, cameraID, subscribe)
    },
)

streamer := client.NewStreamer(storeRegistry, rpcHandler, eventDispatcher, client.Config{
    Endpoint:    "wss://relay.example/device",
    DataStreams: streams,
    Credentials: credentials,
})
```

Endpoint credentials are scoped to the authenticated parent subscription.
`ExpiresAtUnixMilli == 0` is the normal non-expiring form: Restream performs no
credential-renewal subprotocol, and unsubscribe/disconnect explicitly revokes
the lease. Allocators and the data-plane service must treat `ReleaseViewer` as
idempotent and bind token validity to the live lease. A forever-valid stateless
bearer token is not safe; revocation state must survive as long as the token
can otherwise be accepted. Restream retains partial close progress and retries
failed release/source-stop phases for the lifetime of the relay process.

When viewer sessions are enabled, the relay broker implements
`SessionDataStreamBroker`. It passes the public Restream session UUID to the
endpoint allocator so a future GoatStream service can correlate all viewer
resources with that session. GoatStream still authenticates its viewer
connection independently; knowing a session UUID grants no access.

Allocator implementations receive the session UUID in `AllocateViewer` and
must implement idempotent `ReleaseSession`. Restream calls `ReleaseSession`
when the owning viewer session explicitly closes or reaches its detached
timeout, while normal per-stream unsubscribe continues to call
`ReleaseViewer`.

The base relay `CurrentVersion` changes only for an incompatible change to
framing or fundamental packet processing. Optional features use capabilities
in hello metadata. New packet kinds decode as `RawPacket`; a sender must not use
an optional packet until its peer advertises the corresponding capability.
Receivers may ignore unknown optional packets or reject unsupported operations
without taking the base relay offline.

Restream bounds state retained per viewer connection (subscriptions, catch-up
buffers, data leases, and in-flight RPC/FFRPC work) and bounds each device relay
websocket message. The application that creates the Socket.IO server must also
configure its transport-level maximum message size, because
`AddSocketHandlersWithOptions` receives an already-created socket and cannot
change the Engine.IO/Socket.IO frame limit after connection setup.

The cloud-side composition is intentionally endpoint-implementation agnostic:

```go
broker := server.NewDeviceDataStreamBroker(device, goatStreamAllocator)
err := restream.AddSocketHandlersWithOptions(
    viewerSocket,
    log,
    device.StoreRegistry,
    device.RPCHandler,
    device.EventDispatcher,
    accessLookup,
    restream.SocketHandlerOptions{
        FFRPCHandler:     device.FFRPCHandler,
        DataStreamBroker: broker,
    },
)
```

`goatStreamAllocator` can initially live in the GoatCloud process. Moving it to
a separately scaled service later does not change the Restream viewer or device
subscription contracts.

Device clients use static config:

```go
streamer := client.NewStreamer(storeRegistry, rpcHandler, eventDispatcher, client.Config{
    Endpoint: "wss://relay.example/device",
    Credentials: client.Credentials{
        DeviceID: "device-1",
        AuthType: "shared-key",
        AuthData: []byte(secret),
    },
})
```

Cloud relays provide authentication and the per-device session:

```go
relay := server.New(server.Config{
    DeviceManager: deviceManager,
    AuthenticateDevice: func(ctx context.Context, hello *protocol.DeviceHello, conn *server.Connection) (restream.AccessLevel, error) {
        if err := validateDevice(hello.DeviceID, hello.AuthType, hello.AuthData); err != nil {
            return restream.AccessLevel(0), err
        }
        return restream.AccessLevel(1), nil
    },
})
```

The access level returned by `AuthenticateDevice` is the device connection's level. Cloud viewer access is still supplied separately by the `restream.AddSocketHandlers` access lookup used when serving websocket clients from the relay.

For simple relays, `server.DeviceManager` creates a `server.Device` per device from a configured store factory after `AuthenticateDevice` approves the device hello. Applications that need custom event fanout, custom packet handling, or device connection stores should use the callbacks on `server.DeviceManagerConfig`.

Relay stores must carry the same minimum store access level as the device-side store so the cloud registry can enforce the same read and subscription rules for viewers. For generated stores, use the package-level `NewRelayStores` helper; codegen evaluates each store's optional `GetMinimumAccessLevel` method and hardcodes that minimum into the generated relay store factory:

```go
manager := server.NewDeviceManager(server.DeviceManagerConfig{
    Stores: func(deviceID string) ([]restream.Store, error) {
        return game.NewRelayStores(), nil
    },
})
```

For relay codegen, `GetMinimumAccessLevel` must have the exact signature `GetMinimumAccessLevel() restream.AccessLevel`, and its body must be a single `return` of a compile-time integer constant or a conversion of one, for example `return restream.AccessLevel(auth.AccessLevelAdmin)`. Stores without the optional method use `restream.AccessLevelPublic`.

`NewRelayStores` includes only stores annotated as `@restream.store(Name)` or `@restream.store(Name, DeviceWithRelay)`. `DeviceWithCloudImpl` stores still stream full states and partials from the device but expect a custom cloud store implementation annotated as `CloudImplOfDevice`. `DeviceWithNoRelay`, `DeviceAndCloud`, `CloudImplOfDevice`, and `CloudOnly` stores are skipped by the device relay streamer.
