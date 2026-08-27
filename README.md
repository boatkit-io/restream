# ReStream

ReStream is a data streaming framework based on [ReSub](https://github.com/boatkit-io/resub).  The intent is for golang serverside applications to be able to stream data to other golang services and web apps in real time, with fully-codegenned data stores and models based on the host golang side models.  There are also provisions for RPCs to use strongly-typed request/response models codegenned from golang-side functions to automatically be typesafe from the client side.  It uses similar patterns as protobuf for field serialization/deserialization, but is more compact and bespoke for golang/typescript, supporting a tight integration with native types.

# Examples

- [Tic Tac Toe](examples/tictactoe) contains the full getting-started tutorial and completed direct client-server example.
- [Tic Tac Toe Relay](examples/tictactoerelay) builds on the base example with a device server, cloud relay, and web client switcher.
- [Keyed Events](examples/keyedevents) is a focused, tested radio-audio example showing exact-key subscriptions in Go and TypeScript.
- [Fire-and-Forget RPCs](examples/ffrpc) demonstrates request-only browser-to-device audio frames with no response tracking.

# Details

## Stores

The data model for resub is designed around Stores that hold all state and emit events when changes are made.  See [the ReSub complete example](https://github.com/boatkit-io/resub) to get a basic idea of how to think about stores.

In ReStream, we use the same store model as in ReSub, but the stores are created in golang and streamed over to codegenned TypeScript versions of the stores.

### Field-Keyed TypeScript Subscriptions

Generated ReStream store states support field-keyed ReSub subscriptions on the TypeScript side. Use `@autoSubscribeWithKey` with generated stable field-ID constants when a getter should only re-run for partial updates that touch that part of the store.

```typescript
import { AutoSubscribeStore, autoSubscribeWithKey, formCompoundKey } from '@boatkit-io/resub';
import { TriggerStore } from '@boatkit-io/restream';

import {
    DeviceStoreName,
    DevicePGNInfoFieldIDRxCount,
    DeviceStoreState,
    DeviceStoreStateFieldIDDevicePGNs,
    DeviceStoreStatePartial,
} from './restream/PackageDevice';

@AutoSubscribeStore
class DeviceStore extends TriggerStore<DeviceStoreState> {
    constructor() {
        super(DeviceStoreName, DeviceStoreState, DeviceStoreStatePartial);
    }

    @autoSubscribeWithKey(DeviceStoreStateFieldIDDevicePGNs)
    getAllDevicePGNs() {
        return this._state.devicePGNs;
    }
}
```

For generated ReStream stores, subscription keys are treated as store-state field paths rather than arbitrary opaque tokens. Build nested key paths with ReSub's compound-key helper and the generated constants for every struct field. Map keys and array indexes remain literal path segments:

```typescript
@autoSubscribeWithKey(formCompoundKey(
    DeviceStoreStateFieldIDDevicePGNs,
    "CAN0",
    DevicePGNInfoFieldIDRxCount,
))
getCAN0RxCount() {
    return this._state.devicePGNs?.get("CAN0")?.rxCount;
}
```

The TypeScript runtime serializes generated paths with a version marker, for example `~1%&3%&CAN0%&7`. Go retains and forwards that same field-ID path through the stable `restream:",fID=N"` tags. Readable Go- or TypeScript-style field names are not accepted: use the generated constants so refactors remain typechecked and production bundles do not retain a parallel name table. Map keys are exact: in the example above, `CAN0` will not match `can0`. Full-store subscriptions still update for any store change, while field-keyed subscriptions update only when the generated partial reports that exact field path or one of its parent/child paths.

Field-ID subscriptions are a breaking wire-protocol requirement. Every client, server, and relay in a deployment must be upgraded together.

Viewer sockets use keyed delivery by default. For a direct local viewer where
bandwidth is inexpensive and the client should retain complete store state,
select full-store delivery. Subscription lifecycle callbacks still receive the
exact field keys, but the first active key receives a full baseline and all
store partials flow until the last key stops:

```go
restream.AddSocketHandlersWithOptions(
    conn, log, stores, rpcHandler, events, accessLookup,
    restream.SocketHandlerOptions{
        StoreDeliveryMode: restream.StoreDeliveryModeFullStore,
    },
)
```

Cloud viewer sockets should retain the default
`StoreDeliveryModeKeyed`, allowing relay-side subscription refcounting and
on-demand catch-up to limit device uplink traffic.

## Resumable Viewer Sessions

Viewer sessions retain Restream state across transient Socket.IO disconnects.
The session owns store, keyed-event, and data-stream subscriptions while a
socket is merely attached to it. A random UUIDv4 session ID is kept only in the
live TypeScript `ReStreamSocket`; it is a correlation identifier, never an
authentication credential. Every attachment authenticates normally and the
server verifies that the resulting owner and scope match the session.

Create one manager for the lifetime of the store/device scope:

```go
sessions := restream.NewViewerSessionManager(restream.ViewerSessionManagerOptions{
    DetachedTimeout: 60 * time.Second,
})
defer sessions.Close()

err := restream.AddSocketHandlersWithOptions(
    conn,
    log,
    stores,
    rpcHandler,
    events,
    accessLookup,
    restream.SocketHandlerOptions{
        SessionManager: sessions,
        SessionIdentityLookup: func() (restream.ViewerSessionIdentity, error) {
            return restream.ViewerSessionIdentity{
                OwnerID:     authenticatedSubject,
                ScopeID:     deviceID,
                AccessLevel: currentAccessLevel,
            }, nil
        },
    },
)
```

Negotiate the optional capability in the application's normal login response,
then enable the corresponding client handshake and wait for it after
authentication succeeds. Keeping the constructor in legacy mode until the
server replies allows new clients to roll out before new servers without a base
protocol-version bump:

```typescript
const restreamSocket = new ReStreamSocket(socket);
restreamSocket.setViewerSessionsEnabled(loginResponse.viewerSessions === true);
await restreamSocket.markAuthenticated();
```

Socket reconnections reuse the in-memory session ID. Refreshing or replacing
the page creates a new JavaScript client with no session ID, so the server
creates a fresh session and sends normal subscription baselines. Applications
may call `closeSession()` during page teardown as a best-effort optimization;
the configured detached timeout is authoritative.

While detached, each subscribed store retains at most one outbound update:

- partial followed by partial merges through `Partial.MergeOntoPartial`;
- a full state discards the retained partial;
- partials following a full state apply directly to the detached full-state
  snapshot;
- another full state replaces the earlier full state.

Reattachment emits that single accumulated update before switching back to
live delivery. Keyed store filtering happens before accumulation. Ordered
events and pending responses use a separately bounded queue, while
high-bandwidth media is never buffered by Restream. Subscription manifests on
attachment are authoritative, allowing subscriptions changed while offline to
be reconciled without duplicate refcounts.

## Keyed Events

Keyed events are transient, typed messages delivered only while a client is subscribed to one exact `{store, event, key}` tuple. They are useful for relatively expensive or high-volume sources such as a radio audio stream, camera frames, debug traces, or device logs. Unlike a field-keyed store subscription, a keyed event does not fetch state or match parent and child field paths: its key is an opaque exact-match identifier.

Define the key as the first string argument of the event callback, followed by the values that should be serialized:

```go
const (
	RadioStoreName = "RadioStore"
	AudioEventName = "Radio.Audio"
)

type AudioCallback func(radioID string, audio []byte)

func RegisterAudioEvent(eventd *restream.EventDispatcher) *subscribableevent.Event[AudioCallback] {
	event := subscribableevent.NewEvent[AudioCallback]()
	eventd.RegisterKeyedEvent("Radio.Audio", &event, RadioStoreName, nil, nil)
	return &event
}
```

Run codegen after adding the registration. It generates the typed `RadioAudioEvent`, fills in the two reflection arguments, and omits `radioID` from the serialized packet because the key already travels in the keyed-event envelope. Go `[]byte` payloads are exposed to TypeScript as `Uint8Array`:

```bash
go tool github.com/boatkit-io/restream/cmd/codegen -project .
```

Fire the source event normally. Serialization and delivery are skipped unless that exact radio key currently has at least one listener:

```go
audioEvent.Fire(normalizedRadioID, audioBytes)
```

The browser subscribes through `ReStreamSocket`. Multiple local callbacks for the same tuple produce one network subscription; the final local unsubscribe produces the network unsubscribe. Active subscriptions are replayed after reconnect:

```typescript
const unsubscribe = rss.subscribeToKeyedEvent(
  RadioStoreName,
  RadioAudioEvent,
  normalizedRadioID,
  event => audioPlayer.write(event.audio ?? new Uint8Array()),
);

// Later:
unsubscribe();
```

The store must be present in the server's `StoreRegistry`. `AddSocketHandlers` applies that store's normal minimum-access check before accepting a keyed-event subscription. Applications that need to start and stop the underlying source can observe aggregate 0-to-1 and 1-to-0 transitions:

```go
eventd.SubscribeToKeyedEventSubscriptions(func(storeName, eventName, key string, subscribed bool) {
	if storeName == RadioStoreName && eventName == AudioEventName {
		setRadioAudioStreaming(key, subscribed)
	}
})
```

Device/cloud relays forward these transitions to the device and carry matching events back to cloud viewers. The relay protocol filters by the exact tuple on both sides and replays active subscriptions when the device reconnects. Cloud code should pass the relay device's `EventDispatcher` to `AddSocketHandlers`; if `ConfigureDevice` replaces that dispatcher, assign it there before returning so Restream attaches subscription forwarding to the replacement.

See the runnable [Keyed Events example](examples/keyedevents), including its exact-routing and refcount tests.

## RPC Context and Transport Annotations

RPC callbacks normally declare only their serialized request parameters. A callback that needs caller metadata may opt in by declaring `context.Context` as its first parameter:

```go
rpcd.RegisterRPCHandler(
	"Document.Update",
	AccessLevelAdmin,
	func(ctx context.Context, documentID string, contents []byte) error {
		call, ok := restream.RPCCallInfoFromContext(ctx)
		if ok {
			log.WithFields(logrus.Fields{
				"callerAccessLevel": call.AccessLevel,
				"requestID":         call.Annotations["request_id"],
			}).Info("Document updated")
		}
		return documents.Update(documentID, contents)
	},
	reflect.TypeFor[DocumentUpdateRequest](),
	reflect.TypeFor[DocumentUpdateResponse](),
)
```

The context parameter is dispatcher-only. Codegen omits it from the generated Go request structure and TypeScript RPC, so adding it does not change the serialized request or require callers to supply another argument. The dispatcher constructs and passes a context only when the registered callback has the exact leading `context.Context` parameter; callbacks without it retain the existing call path. The same optional leading parameter is supported for FFRPC callbacks.

`RPCCallInfo` contains the authenticated caller's `AccessLevel` and an optional `Annotations` map. Annotation keys and values are opaque to ReStream: applications define their meaning and should populate trusted identity or connection metadata only after authenticating the caller. An annotation is transport metadata, not part of the typed RPC request.

Call `FireRPCWithAnnotations` when a transport has metadata to attach. Existing transports can continue to use `FireRPC`:

```go
response, handled, err := rpcd.FireRPCWithAnnotations(
	map[string]string{"request_id": requestID},
	methodName,
	callerAccessLevel,
	requestBytes,
)
```

The device relay advertises RPC-annotation support when `relayclient.Config.RPCHandlerWithAnnotations` or `FFRPCHandlerWithAnnotations` is configured. A cloud relay may then use `server.Connection.SendRPCWithAnnotations` or `SendFFRPCWithAnnotations`; it must use the corresponding original method for a device whose `Capabilities.RPCAnnotations` is false. Annotations are an optional trailing field on capable relay RPC and FFRPC packets, while unannotated packets retain their legacy byte representation.

## Fire-and-Forget RPCs

FFRPCs are typed client-to-server calls for data whose caller does not need an acknowledgement. They use the same generated request encoding and access checks as normal RPCs, but their websocket and relay envelopes have no call ID. The TypeScript client creates no promise or pending-call entry, and the receiving Go transport dispatches the handler in a goroutine without emitting a response.

Register an FFRPC with a callback that returns nothing or an `error`. Returned errors are available only for receiver-side logging:

```go
rpcd.RegisterFFRPCHandler(
	"Radio.TransmitAudio",
	AccessLevelAdmin,
	func(radioID string, sequence uint32, audio []byte) error {
		return radio.AcceptAudio(radioID, sequence, audio)
	},
	nil,
)
```

Run codegen to create the request-only `RadioTransmitAudioRequest` and fill in its reflection argument. The browser sends it with `sendFFRPC`, which returns `void`:

```typescript
rss.sendFFRPC(
  RadioTransmitAudioRequest.fromValues(radioID, sequence, encodedAudio),
);
```

Pass the dispatcher to a direct websocket server as the optional final argument:

```go
restream.AddSocketHandlers(
	conn,
	log,
	sdr,
	rpcd.FireRPC,
	eventd,
	accessLookup,
	rpcd.FireFFRPC,
)
```

When caller annotations matter, use `AddSocketHandlersWithOptions` with `RPCAnnotations` and `FFRPCHandlerWithAnnotations: rpcd.FireFFRPCWithAnnotations`. Configure a device relay's `FFRPCHandlerWithAnnotations` the same way, and have a cloud viewer use `device.FFRPCHandlerWithAnnotations`. The relay forwards the request and its trusted transport metadata without allocating an RPC ID or pending response. The legacy unannotated handlers remain available for callers that do not need metadata. Legacy and annotated variants are mutually exclusive on every configuration surface; providing both is rejected instead of choosing one silently.

FFRPC delivery still uses the underlying reliable websocket, but execution completion and application errors are deliberately invisible to the sender. Use a normal RPC when the caller must know whether an operation succeeded. For high-rate payloads, include a sequence number so the receiver can identify stale or reordered work.

See the runnable [Fire-and-Forget RPC example](examples/ffrpc).

## Access Levels

ReStream access checks use `restream.AccessLevel`, which is an integer level assigned by the application. `restream.AccessLevelPublic` is `0` and is the default, lowest access level. Higher numbers represent more access. ReStream does not define roles; applications map their own roles, sessions, device credentials, or users to numeric levels.

Websocket servers provide the connected client's current level through `AddSocketHandlers`:

```go
restream.AddSocketHandlers(conn, log, sdr, rpcd.FireRPC, eventd, func() (restream.AccessLevel, error) {
    return currentUserAccessLevel, nil
})
```

RPC handlers already use this level. The second argument to `RegisterRPCHandler` is the minimum access level required to call that RPC:

```go
rpcd.RegisterRPCHandler("AdminStore.DeleteItem", AccessLevelAdmin, func(id string) error {
    // ...
}, reflect.TypeFor[DeleteItemRequest](), reflect.TypeFor[DeleteItemResponse]())
```

### Store Minimum Access

Stores may also require a minimum access level for any client-visible store data. Implement `GetMinimumAccessLevel` on the store to opt in:

```go
func (s *AdminStore) GetMinimumAccessLevel() restream.AccessLevel {
    return restream.AccessLevel(AccessLevelAdmin)
}
```

Stores that do not implement this optional method are treated as `restream.AccessLevelPublic`. The `StoreRegistry` enforces the minimum level when a client fetches full store state, fetches keyed subscription catchup state, or starts a whole-store or keyed subscription. Websocket partial updates are only sent to subscribed clients and re-check the store minimum before emitting.

If you use `StoreRegistry` directly, pass the connected caller's level to access-sensitive methods:

```go
stateBytes, err := sdr.GetSerializedFullState(storeName, userAccessLevel)
err = sdr.ListeningToStoreKey(storeName, key, userAccessLevel)
```

Denied reads or subscriptions return an error that matches `restream.ErrInsufficientStoreAccess`.

For generated stores, put `GetMinimumAccessLevel` in the handwritten store file next to `New<StoreName>`. The `@restream.store` annotation still generates the standard `Store` boilerplate separately.

Cloud relay stores need the same access level because they do not have the original device-side store instance. For generated stores, use the package-level `NewRelayStores` helper. Codegen evaluates each store's optional `GetMinimumAccessLevel` method and hardcodes that minimum into the generated relay store factory:

```go
stores := game.NewRelayStores()
```

The method must have the exact signature `GetMinimumAccessLevel() restream.AccessLevel`. For relay codegen, its body must be a single `return` of a compile-time integer constant, or a conversion of one, such as `return AccessLevelAdmin` when the constant is untyped or already a `restream.AccessLevel`, or `return restream.AccessLevel(auth.AccessLevelAdmin)` when the application constant uses a different named integer type. Codegen resolves the constant value through Go type information and emits `restream.AccessLevel(<value>)`, so generated relay code does not import the package that defined the constant. If there is no method, codegen uses `restream.AccessLevelPublic`. Custom relay stores can still call `restream.NewRelayStore` directly.

Generated stores can also declare their relay topology with an optional second `@restream.store` argument:
* `DeviceWithRelay` (default) generates a relay store and streams full states, partials, and relayed subscription lifecycle messages from the device.
* `DeviceWithCloudImpl` marks the device-side half of a custom cloud implementation: it streams device state but does not generate a relay store.
* `DeviceAndCloud` marks a single implementation that can run independently on both device and cloud, so it is skipped by relay store generation and device relay streaming.
* `CloudImplOfDevice` marks the cloud-side half of a custom implementation and is skipped by both relay store generation and device relay streaming.
* `DeviceWithCloudSource` marks a device-side store whose source of truth is the cloud; codegen creates a `CloudSourceForDevice` store that streams cloud state down to the device.

Note: `DeviceWithNoRelay` and `CloudOnly` also do not generate relay stores and are skipped by the device relay streamer.

## Annotations

For structs that should generate client-side types, serializers/deserializers, or store boilerplate, place one of these annotations in a comment immediately preceding the `struct` definition on the golang side:
* `@restream.store(StoreName[, StoreType])` generates the common Go store boilerplate for the annotated store struct into the adjacent `_rs.go` file: the `<StructName>Name` constant, `GetName`, `GetStoreData`, `SubscribeToField`, and `GetStoreType`. Codegen also generates a package-level `NewRelayStores()` helper containing only `DeviceWithRelay` stores. It ensures `<StructName>State` exists with `@restream.partials`, and ensures the store struct has a `storeData` field with the conventional `*restream.StoreData[<StructName>State, *<StructName>State, *<StructName>StatePartial]` type. If `storeData` references a state type in another parsed package, such as `storestates.BoardStoreState`, codegen preserves that package qualifier and annotates the state struct in that package; include both packages in `inputDirs`.
* `@restream.serializers` only generates serialization/deserialization functions for the full structure and is not extensible -- no field ID numbers are generated, so structures can not evolve and must exactly match on the client and serverside for serialization to work
* `@restream.fields` is for structures that may evolve over time, and generates stable IDs for every field that is used in serialization/deserialization so that structures are forwards-and-backwards compatible across disparate wire versions of your application
* `@restream.partials` is for structures that will want to send compact partial deltas across a wire protocol.  These partials will support changes to individual fields, and have optimizations for maps and arrays to allow for specific operations like setting individual elements as an optimized operation.

Structs with generics are also automatically supported -- the types used used by the generics are serialized in front of the structure's contents, allowing the deserializer to know what types to pull off the wire.

### Buffering Store Callbacks

Stores that update faster than their consumers need can opt into callback buffering when they construct `StoreData`:

```go
s.storeData = restream.NewStoreDataWithOptions[MyState, *MyState, *MyStatePartial](
    s,
    initialState,
    restream.StoreDataOptions{OutputBufferDuration: 250 * time.Millisecond},
)
```

`ApplyPartial` still updates the live state synchronously. ReStream gathers the changed partials for the configured fixed window, then fires `AddCallback` and `SubscribeToField` callbacks once with the merged latest values. The existing `NewStoreData` constructor keeps immediate callback delivery.

## Cloud Relay Server

ReStream is designed to work well for both directly-hosted web applications/API servers as well as remotely-hosted servers (i.e. on an IOT device) that relay data up to a cloud-based server.  Helpers are in the restream packages for both the device-side relaying and creating the relay server itself.  Both sides have very simple out of the box configs to get you started and extension points to add in complexity as your project advances.  See the `tictactoerelay` example for how to set up and use the relay server.

### Debouncing Updates

There's helpers inside the streaming client to aggregate updates over a time period per store before updating the relay server, to accommodate limited upstream bandwidth from an IOT device with fast updates that it can't (or just shouldn't bother to) stream at full fidelity.  See `pkg/relay/client/StorePolicy` for details on configuring the Debounce per storename.

## `restream.yaml` Options

`restream.yaml` is loaded from the project root when running codegen with `-project`. These are the keys currently read by the generator:

| Option | Type | Description |
| --- | --- | --- |
| `inputDirs` | `[]string` | Go source directories to parse for `@restream.*` annotations. Relative paths are resolved from the project root. |
| `tsDir` | `string` | Optional output directory for generated TypeScript package files. When set, TypeScript generation runs, files are written into this directory, and `pnpm exec eslint --fix .` is run from the parent directory of `tsDir`. |
| `tsImports` | `[]object` | Optional custom TypeScript imports added to every generated TypeScript package file. See `tsImports` fields below. |
| `goImports` | `[]string` | Additional Go import paths to include in generated Go files. Use this when generated code needs project or standard-library packages that are not part of the default generated imports. |
| `goRelayStoresDir` | `string` | Optional output directory for the generated `NewRelayStores()` helper. When set, all generated relay store factories are written to this package instead of the source store packages, and store-name constants are generated there for relay/cloud code to import without depending on store implementations. |
| `goRelayStoresPackage` | `string` | Optional Go package name to use for `goRelayStoresDir`. Defaults to the base name of `goRelayStoresDir`. |
| `additionalEnums` | `[]string` | Extra Go enum or primitive alias types to emit into the generated TypeScript, even when they are not discovered through parsed struct fields. Values use `<go/package/import/path>.<TypeName>`, for example `github.com/acme/app/pkg/model.Status`. Only used when `tsDir` is set. |
| `buildSerializers` | `[]string` | Extra Go types to generate serializer/deserializer code for even when they are not annotated in an `inputDirs` source file. Values use `<go/package/import/path>/<TypeName>`, for example `github.com/acme/app/pkg/model/User`. This also creates a `ReStreamExtraSerializers` lookup. |
| `goExtraFile` | `string` | Output file for project-level generated Go code, such as code produced by `buildSerializers`. Required when `buildSerializers` is set. Relative paths are resolved from the project root, and the generated package name is inferred from the file's parent directory. |

### `tsImports`

Each `tsImports` entry supports these fields:

| Field | Type | Description |
| --- | --- | --- |
| `path` | `string` | TypeScript module specifier used in the generated `from '<path>'` import. |
| `imports` | `[]string` | Named imports rendered as `import { A, B } from '<path>';`. Ignored when `importRoot` is set. |
| `importRoot` | `string` | Default or namespace import expression rendered as `import <importRoot> from '<path>';`, such as `BinaryReader` or `* as ReStreamDecoders`. |

### Example with all options

```yaml
inputDirs:
  - pkg/model
  - pkg/services
tsDir: web/src/restream
tsImports:
  - path: "@/shared/DateHelpers"
    imports:
      - DateString
  - path: "@/utils/BinaryReader"
    importRoot: BinaryReader
goImports:
  - github.com/acme/app/pkg/model
additionalEnums:
  - github.com/acme/app/pkg/model.Status
buildSerializers:
  - github.com/acme/app/pkg/model/User
goExtraFile: pkg/model/restream_extra_rs.go
```
