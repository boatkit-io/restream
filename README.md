# ReStream

ReStream is a data streaming framework based on [ReSub](https://github.com/boatkit-io/resub).  The intent is for golang serverside applications to be able to stream data to other golang services and web apps in real time, with fully-codegenned data stores and models based on the host golang side models.  There are also provisions for RPCs to use strongly-typed request/response models codegenned from golang-side functions to automatically be typesafe from the client side.  It uses similar patterns as protobuf for field serialization/deserialization, but is more compact and bespoke for golang/typescript, supporting a tight integration with native types.

# Examples

- [Tic Tac Toe](examples/tictactoe) contains the full getting-started tutorial and completed direct client-server example.
- [Tic Tac Toe Relay](examples/tictactoerelay) builds on the base example with a device server, cloud relay, and web client switcher.

# Details

## Stores

The data model for resub is designed around Stores that hold all state and emit events when changes are made.  See [the ReSub complete example](https://github.com/boatkit-io/resub) to get a basic idea of how to think about stores.

In ReStream, we use the same store model as in ReSub, but the stores are created in golang and streamed over to codegenned TypeScript versions of the stores.

### Field-Keyed TypeScript Subscriptions

Generated ReStream store states support field-keyed ReSub subscriptions on the TypeScript side. Use `@autoSubscribeWithKey` with a generated field name or nested key path when a getter should only re-run for partial updates that touch that part of the store.

```typescript
import { AutoSubscribeStore, autoSubscribeWithKey, formCompoundKey } from '@boatkit-io/resub';
import { TriggerStore } from '@boatkit-io/restream';

import { DeviceStoreName, DeviceStoreState, DeviceStoreStatePartial } from './restream/PackageDevice';

@AutoSubscribeStore
class DeviceStore extends TriggerStore<DeviceStoreState> {
    constructor() {
        super(DeviceStoreName, DeviceStoreState, DeviceStoreStatePartial);
    }

    @autoSubscribeWithKey("DevicePGNs")
    getAllDevicePGNs() {
        return this._state.devicePGNs;
    }
}
```

For generated ReStream stores, subscription keys are treated as store-state field paths rather than arbitrary opaque tokens. Field names are normalized between Go-style names and generated TypeScript names, so `DevicePGNs` and `devicePGNs` refer to the same field. Build nested key paths with ReSub's compound-key helper:

```typescript
@autoSubscribeWithKey(formCompoundKey("DevicePGNs", "CAN0", "RxCount"))
getCAN0RxCount() {
    return this._state.devicePGNs?.get("CAN0")?.rxCount;
}
```

Struct field names in nested paths are normalized the same way, but map keys are exact. In the example above, `CAN0` is a map key and will not match `can0`. Full-store subscriptions still update for any store change, while field-keyed subscriptions update only when the generated partial reports that exact field path or one of its parent/child paths.

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

Generated stores can also declare their relay topology with an optional second `@restream.store` argument: `DeviceWithRelay` (default), `DeviceWithNoRelay`, `DeviceWithCloudImpl`, `DeviceAndCloud`, `CloudImplOfDevice`, or `CloudOnly`. `DeviceWithRelay` generates a relay store and streams full states, partials, and relayed subscription lifecycle messages from the device. `DeviceWithCloudImpl` marks the device-side half of a custom cloud implementation: it streams device state but does not generate a relay store. `DeviceAndCloud` marks a single implementation that can run independently on both device and cloud, so it is skipped by relay store generation and device relay streaming. `CloudImplOfDevice` marks the cloud-side half of a custom implementation and is skipped by both relay store generation and device relay streaming. `DeviceWithNoRelay` and `CloudOnly` also do not generate relay stores and are skipped by the device relay streamer.

## Annotations

For structs that should generate client-side types, serializers/deserializers, or store boilerplate, place one of these annotations in a comment immediately preceding the `struct` definition on the golang side:
* `@restream.store(StoreName[, StoreType])` generates the common Go store boilerplate for the annotated store struct into the adjacent `_rs.go` file: the `<StructName>Name` constant, `GetName`, `GetStoreData`, `SubscribeToField`, and `GetStoreType`. Codegen also generates a package-level `NewRelayStores()` helper containing only `DeviceWithRelay` stores. It ensures `<StructName>State` exists with `@restream.partials`, and ensures the store struct has a `storeData` field with the conventional `*restream.StoreData[<StructName>State, *<StructName>State, *<StructName>StatePartial]` type. If `storeData` references a state type in another parsed package, such as `storestates.BoardStoreState`, codegen preserves that package qualifier and annotates the state struct in that package; include both packages in `inputDirs`.
* `@restream.serializers` only generates serialization/deserialization functions for the full structure and is not extensible -- no field ID numbers are generated, so structures can not evolve and must exactly match on the client and serverside for serialization to work
* `@restream.fields` is for structures that may evolve over time, and generates stable IDs for every field that is used in serialization/deserialization so that structures are forwards-and-backwards compatible across disparate wire versions of your application
* `@restream.partials` is for structures that will want to send compact partial deltas across a wire protocol.  These partials will support changes to individual fields, and have optimizations for maps and arrays to allow for specific operations like setting individual elements as an optimized operation.

Structs with generics are also automatically supported -- the types used used by the generics are serialized in front of the structure's contents, allowing the deserializer to know what types to pull off the wire.

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
