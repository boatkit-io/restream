# ReStream

ReStream is a data streaming framework based on [ReSub](https://github.com/microsoft/resub).  The intent is for golang serverside applications to be able to stream data to other golang services and web apps in real time, with fully-codegenned data stores and models based on the host golang side models.  There are also provisions for RPCs to use strongly-typed request/response models codegenned from golang-side functions to automatically be typesafe from the client side.  It uses similar patterns as protobuf for field serialization/deserialization, but is more compact and bespoke for golang/typescript, supporting a tight integration with native types.

# Getting Started/Example

Let's build a persistent tic-tac-toe game together using ReStream.  The final output of this example will be reflected in [examples/tictactoe](examples/tictactoe).

## Golang Scaffolding

Let's stand up a basic golang project.  This tutorial will assume that you have [mise](https://mise.jdx.dev/installing-mise.html) installed:

```bash
mise use go@latest
go mod init github.com/boatkit-io/restream/examples/tictactoe
```

To adopt restream, first pull the package into your golang project as a dependency and a tool:

```bash
go get github.com/boatkit-io/restream
go get -tool github.com/boatkit-io/restream/cmd/codegen
```

## Store State

Let's define our store which will hold the tic tac toe state.  Make a new `boardstorestate.go` file in `cmd/server`:

```go
package main

// @restream.partials
type BoardStoreState struct {
	Board [][]string
	XTurn bool
}

const BoardStoreName = "BoardStore"
```

Next, create a new `restream.yaml` file at the root of your project that lists this directory to crawl for things to codegen:

```yaml
inputDirs:
  - cmd/server
```

Now let's generate the restream structs.  Run this from the root of your project:

```bash
go tool github.com/boatkit-io/restream/cmd/codegen -project .
```

This should update your BoardStoreState struct with field numbers and a new MAXFIELD comment (this stores the largest previously used field ID in case you remove a field in the future, ensuring compatibility):

```go
type BoardStoreState struct {
	// MAXFIELD(2)
	Board [][]string `restream:",fID=1"`
	XTurn bool       `restream:",fID=2"`
}
```

It will also have created a new `boardstorestate_rs.go` file with a bunch of codegenned structs/serializers/deserializers.

## First Store

Let's wire this up to a store now.  Create a new `boardstore.go` file alongside the store state:

```go
package main

import (
	"github.com/boatkit-io/restream/pkg/restream"
)

type BoardStore struct {
	storeData *restream.StoreData[BoardStoreState, *BoardStoreState, *BoardStoreStatePartial]
}

func NewBoardStore() (*BoardStore, error) {
	s := &BoardStore{}

	initialState := &BoardStoreState{
		Board: [][]string{},
		XTurn: true,
	}

	s.storeData = restream.NewStoreData[BoardStoreState, *BoardStoreState, *BoardStoreStatePartial](s, initialState)

	return s, nil
}

func (s *BoardStore) GetName() string {
	return BoardStoreName
}

func (s *BoardStore) GetStoreData() restream.StoreDataBase {
	return s.storeData
}

func (s *BoardStore) SubscribeToField(field []any, callback any) {
	s.storeData.SubscribeToField(field, callback)
}
```

Now we have a basic store and state structure.  Let's get a basic service going.  Create a new `main.go` file in `cmd/server` with the following contents to do basic store setup and bolt it to a websocket listener via the standard library HTTP server:

```go
package main

import (
	"net/http"

	"github.com/boatkit-io/restream/pkg/restream"
	"github.com/boatkit-io/restream/pkg/websocketencoder"
	"github.com/sirupsen/logrus"
	"github.com/zishang520/socket.io/servers/socket/v3"
	"github.com/zishang520/socket.io/v3/pkg/types"
)

func main() {
	log := logrus.New()

	boardStore, err := NewBoardStore()
	if err != nil {
		panic(err)
	}

	sdr, err := restream.NewStoreRegistry([]restream.Store{
		boardStore,
	})
	if err != nil {
		panic(err)
	}

	router := http.NewServeMux()

	so := socket.ServerOptions{}
	so.SetParser(websocketencoder.NewParser())
	so.SetCors(&types.Cors{
		Origin: "*",
	})
	io := socket.NewServer(nil, &so)

	if err := io.On("connection", func(clients ...any) {
		conn := clients[0].(*socket.Socket)
		restream.AddSocketHandlers(conn, log, sdr, nil, func() (restream.AccessLevel, error) {
			return restream.AccessLevel(1), nil
		})
	}); err != nil {
		panic(err)
	}

	socketHandler := io.ServeHandler(&so)
	router.Handle("/socket", socketHandler)

	http.ListenAndServe(":8080", router)
}
```

Now let's run it:

```bash
go run ./cmd/server
```

You'll see no output, but it shouldn't crash.  If you go to `http://localhost:8080` you should get a 404, and that means your server is running!

## Web App Scaffolding

Now we need to whip up a web app to test this all out.  Add `node = "lts"` and `pnpm = "latest"` to your `.mise.toml` file `[tools]` section and run `mise install` to make sure they're ready.  Now let's bootstrap a super basic react/typescript/socket.io web app:

```bash
pnpm create vite web --template react-ts --no-interactive
cd web
pnpm install
pnpm add socket.io-client
pnpm add @boatkit-io/restream
pnpm add resub
```

For now, because `ReSub` still uses legacy decorators, you have to go into your `tsconfig.app.json` file and add `"experimentalDecorators": true,` as a top level field.

Let's make a real quick UI.  Open the `App.tsx` file that the vite template auto-created and replace it with:

```typescript
import { ReStreamSocket } from '@boatkit-io/restream';
import './App.css'
import BoardStore from './stores/BoardStore'

import SocketIoClient from 'socket.io-client';
import { withResubAutoSubscriptions } from 'resub';

const socket = SocketIoClient('http://localhost:8080', {
  path: '/socket',
  reconnection: true,
});

const rss = new ReStreamSocket(socket);

socket.on('connect', () => {
  // no auth, just send it
  rss.markAuthenticated();
});

socket.open();

// eslint-disable-next-line react-refresh/only-export-components
function App() {
  const board = BoardStore.getBoard();
  const xTurn = BoardStore.getXTurn();
  const nextToken = xTurn ? 'X' : 'O';
  
  return (
    <>
      <h1>Tic Tac Toe</h1>
      <h2>Current Player: {nextToken}</h2>
      <div className="board">
        <table align="center">
          <tbody>
            {board.map((row, rowIndex) => (
              <tr key={rowIndex}>
                {row.map((cell, cellIndex) => (
                  <td key={cellIndex}>{cell || ' '}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  )
}

export default withResubAutoSubscriptions(App)
```

Note the `withResubAutoSubscriptions` at the bottom -- you have to wrap component rendering in that to get auto-subscriptions to work.
Let's start the web dev hot reload cycle:

```bash
pnpm dev
```

Now open the URL it gives you (should be [http://localhost:5173/]) in a browser, and you should see a fixed tic-tac-toe board!  This means it's connected to your web server and gotten the state out of it.

## RPCs

Let's add the ability to place a token on the game board.  In `boardstore.go`'s `NewBoardStore` function, start passing in a new `restream.RPCDispatcher`:

```go
func NewBoardStore(rpcd *restream.RPCDispatcher) (*BoardStore, error) {
```

Now add the following RPC registration to the bottom of the `NewBoardStore` function:

```go
rpcd.RegisterRPCHandler("PlaceToken", 1, func(x, y int) error {
	partial := &BoardStoreStatePartial{
		Board: restream.NewPartialArray[[]string](),
	}
	var newRow []string
	var xTurn bool
	s.storeData.ReadState(func(state *BoardStoreState) {
		newRow = append([]string{}, state.Board[y]...)
		xTurn = state.XTurn
	})
	if newRow[x] != "" {
		return errors.New("cell already occupied")
	}
	if xTurn {
		newRow[x] = "X"
	} else {
		newRow[x] = "O"
	}
	partial.Board.Set(y, newRow)
	partial.XTurn = restream.Ptr(!xTurn)
	s.storeData.ApplyPartial(partial)
	return nil
}, nil, nil)
```

Note the `, nil, nil` for the RPC types -- codegen will fill that in in a moment.  Next, we'll handle RPC dispatching.  In `main.go`, start setting up an RPCDispatcher at the top of your `main` function and pass it into your `NewBoardStore` function:

```go
	rpcd := restream.NewRPCDispatcher(log)
	boardStore, err := NewBoardStore(rpcd)
```

Near the bottom of your `main` function, pass in the RPC handler function to the socket handler:

```go
		restream.AddSocketHandlers(conn, log, sdr, rpcd.FireRPC, func() (restream.AccessLevel, error) {
```

Now re-run codegen to generate the RPC types/signatures:

```bash
go tool github.com/boatkit-io/restream/cmd/codegen -project .
```

You should now have `PlaceTokenRequest` and `PlaceTokenResponse` types added to a new `boardstore_rs.go` file alongside `boardstore.go`, as well as that codegen should have filled in `reflect.TypeFor[PlaceTokenRequest](), reflect.TypeFor[PlaceTokenResponse]()` for the `nil, nil` type parameters in your RPC registration.  Let's add a click handler for it on the website:

```typescript
<td key={cellIndex} onClick={async () => { try { await rss.sendRPC(PlaceTokenRequest.fromValues(cellIndex, rowIndex)); } catch (error) { alert(error); } }}>{cell || ' '}</td>
```

It'll even auto pass the responses through, so RPCs can be bidirectional (i.e. they can get return values, even if they're errors)!  Now re-run the server and the website dev build should have picked up the change already.  You should be able to play a very simple game of multiplayer tic tac toe!  Open the site in multiple browsers to see the automatic streaming of all state.

# Details

## Stores

The data model for resub is designed around Stores that hold all state and emit events when changes are made.  See [the ReSub complete example](https://github.com/microsoft/resub) to get a basic idea of how to think about stores.

In ReStream, we use the same store model as in ReSub, but the stores are created in golang and streamed over to codegenned TypeScript versions of the stores.  The pattern for ReStream stores is to first create a 

## Annotations

For any structs that should generate client-side types and serializers/deserializers from, there are 3 levels of annotations available to place in a comment immediately preceding the `struct` definition on the golang side:
* `@restream.serializers` only generates serialization/deserialization functions for the full structure and is not extensible -- no field ID numbers are generated, so structures can not evolve and must exactly match on the client and serverside for serialization to work
* `@restream.fields` is for structures that may evolve over time, and generates stable IDs for every field that is used in serialization/deserialization so that structures are forwards-and-backwards compatible across disparate wire versions of your application
* `@restream.partials` is for structures that will want to send compact partial deltas across a wire protocol.  These partials will support changes to individual fields, and have optimizations for maps and arrays to allow for specific operations like setting individual elements as an optimized operation.

Structs with generics are also automatically supported -- the types used used by the generics are serialized in front of the structure's contents, allowing the deserializer to know what types to pull off the wire.

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
