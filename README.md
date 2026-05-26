# ReStream

ReStream is a data streaming framework based on [ReSub](https://github.com/boatkit-io/resub).  The intent is for golang serverside applications to be able to stream data to other golang services and web apps in real time, with fully-codegenned data stores and models based on the host golang side models.  There are also provisions for RPCs to use strongly-typed request/response models codegenned from golang-side functions to automatically be typesafe from the client side.  It uses similar patterns as protobuf for field serialization/deserialization, but is more compact and bespoke for golang/typescript, supporting a tight integration with native types.

# Examples

- [Tic Tac Toe](examples/tictactoe) contains the full getting-started tutorial and completed direct client-server example.
- [Tic Tac Toe Relay](examples/tictactoerelay) builds on the base example with a device server, cloud relay, and web client switcher.

# Details

## Stores

The data model for resub is designed around Stores that hold all state and emit events when changes are made.  See [the ReSub complete example](https://github.com/boatkit-io/resub) to get a basic idea of how to think about stores.

In ReStream, we use the same store model as in ReSub, but the stores are created in golang and streamed over to codegenned TypeScript versions of the stores.  The pattern for ReStream stores is to first create a 

## Annotations

For any structs that should generate client-side types and serializers/deserializers from, there are 3 levels of annotations available to place in a comment immediately preceding the `struct` definition on the golang side:
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
