# Tic Tac Toe Tutorial

Let's build a multiplayer server-side-tracked tic-tac-toe game with ReStream. The completed output of this example lives in this directory.

This example keeps the game model in `internal/game` and the runnable websocket server in `cmd/server`. That split is a little more structure than the smallest possible example, but it makes the same game package reusable by the relay example.

## Golang Scaffolding

This tutorial assumes that you have [mise](https://mise.jdx.dev/installing-mise.html) installed:

```bash
mise use go@latest
go mod init github.com/boatkit-io/restream/examples/tictactoe
```

Add ReStream as both a dependency and a codegen tool:

```bash
go get github.com/boatkit-io/restream
go get -tool github.com/boatkit-io/restream/cmd/codegen
```

Create the game package and server command directories:

```bash
mkdir -p internal/game cmd/server
```

## Store State

Define the store state in `internal/game/boardstorestate.go`:

```go
package game

// @restream.partials
type BoardStoreState struct {
	Board [][]string
	XTurn bool
}

const BoardStoreName = "BoardStore"
```

Create `restream.yaml` at the project root. For the first codegen pass, keep this Go-only because the web app does not exist yet:

```yaml
inputDirs:
  - internal/game
```

Run codegen:

```bash
go tool github.com/boatkit-io/restream/cmd/codegen -project .
```

Codegen updates the state with stable field IDs and writes generated Go outputs:

```go
type BoardStoreState struct {
	// MAXFIELD(2)
	Board [][]string `restream:",fID=1"`
	XTurn bool       `restream:",fID=2"`
}
```

## First Store

Create `internal/game/boardstore.go`:

```go
package game

import (
	"github.com/boatkit-io/restream/pkg/restream"
)

type BoardStore struct {
	storeData *restream.StoreData[BoardStoreState, *BoardStoreState, *BoardStoreStatePartial]
}

func NewBoardStore(rpcd *restream.RPCDispatcher) (*BoardStore, error) {
	s := &BoardStore{}

	initialState := &BoardStoreState{
		Board: [][]string{
			{"", "", "O"},
			{"", "X", ""},
			{"O", "", "X"},
		},
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

Now create `cmd/server/main.go`:

```go
package main

import (
	"net/http"

	"github.com/boatkit-io/restream/examples/tictactoe/internal/game"
	"github.com/boatkit-io/restream/pkg/restream"
	"github.com/boatkit-io/restream/pkg/websocketencoder"
	"github.com/sirupsen/logrus"
	"github.com/zishang520/socket.io/servers/socket/v3"
	"github.com/zishang520/socket.io/v3/pkg/types"
)

func main() {
	log := logrus.New()

	rpcd := restream.NewRPCDispatcher(log)
	boardStore, err := game.NewBoardStore(rpcd)
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
	so.SetCors(&types.Cors{Origin: "*"})
	io := socket.NewServer(nil, &so)

	if err := io.On("connection", func(clients ...any) {
		conn := clients[0].(*socket.Socket)
		restream.AddSocketHandlers(conn, log, sdr, rpcd.FireRPC, nil, func() (restream.AccessLevel, error) {
			return restream.AccessLevel(1), nil
		})
	}); err != nil {
		panic(err)
	}

	socketHandler := io.ServeHandler(&so)
	router.Handle("/socket", socketHandler)
	router.Handle("/socket/", socketHandler)

	http.ListenAndServe(":8080", router)
}
```

Run it:

```bash
go run ./cmd/server
```

The server has no route at `/`, so `http://localhost:8080` returning 404 means it is running.

## Web App Scaffolding

Add `node = "lts"` and `pnpm = "latest"` to your `.mise.toml` `[tools]` section and run `mise install`.

Create the web app:

```bash
pnpm create vite web --template react-ts --no-interactive
cd web
pnpm install
pnpm add socket.io-client
pnpm add @boatkit-io/restream
pnpm add @boatkit-io/resub
```

Now that `web/package.json` exists, enable TypeScript output in `restream.yaml`:

```yaml
inputDirs:
  - internal/game
tsDir: web/src/restream
```

Run codegen again:

```bash
go tool github.com/boatkit-io/restream/cmd/codegen -project .
```

Configure `vite.config.ts` to support decorators:

```typescript
import { defineConfig } from 'vite'
import { standardDecorators } from '@boatkit-io/resub/vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [standardDecorators(), react()],
})
```

Create `web/src/stores/BoardStore.ts`:

```typescript
import { TriggerStore } from '@boatkit-io/restream';
import { AutoSubscribeStore, autoSubscribe } from '@boatkit-io/resub';

import { BoardStoreName, BoardStoreState, BoardStoreStatePartial } from '../restream/PackageGame';

@AutoSubscribeStore
class BoardStore extends TriggerStore<BoardStoreState> {
    constructor() {
        super(BoardStoreName, BoardStoreState, BoardStoreStatePartial);
    }

    @autoSubscribe
    getBoard(): string[][] {
        return (this._state.board ?? []).map((row) => row ?? []);
    }

    @autoSubscribe
    getXTurn(): boolean {
        return this._state.xTurn;
    }
}

export default new BoardStore();
```

Replace `web/src/App.tsx` with a basic board view:

```typescript
import { ReStreamSocket } from '@boatkit-io/restream';
import './App.css'
import BoardStore from './stores/BoardStore'

import SocketIoClient from 'socket.io-client';
import { withResubAutoSubscriptions } from '@boatkit-io/resub';

const socket = SocketIoClient('http://localhost:8080', {
  path: '/socket',
  reconnection: true,
});

const rss = new ReStreamSocket(socket);

socket.on('connect', () => {
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

Start the web dev server:

```bash
pnpm dev
```

Open `http://localhost:5173/`. You should see the server-owned board state.

## RPCs

Add a `PlaceToken` RPC to the bottom of `NewBoardStore` in `internal/game/boardstore.go`:

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

Add the `errors` and `reflect` imports. The trailing `nil, nil` values are placeholders that codegen replaces with the generated request/response types, using `reflect.TypeFor`.

Run codegen again:

```bash
go tool github.com/boatkit-io/restream/cmd/codegen -project .
```

Codegen creates `PlaceTokenRequest` and `PlaceTokenResponse` in Go and TypeScript, and fills in:

```go
reflect.TypeFor[PlaceTokenRequest](), reflect.TypeFor[PlaceTokenResponse]()
```

On the client, import the generated RPC request:

```typescript
import { PlaceTokenRequest } from './restream/PackageGame';
```

Then update the cell renderer:

```tsx
<td key={cellIndex} onClick={async () => { try { await rss.sendRPC(PlaceTokenRequest.fromValues(cellIndex, rowIndex)); } catch (error) { alert(error); } }}>{cell || ' '}</td>
```

Restart the Go server. You should be able to play a simple multiplayer tic-tac-toe game across multiple browser tabs.

## Events

Stores are durable state. Events are useful for typed server-originated messages that do not need to become store state. Add a clock event that fires every 500ms.

Create `internal/game/events.go`:

```go
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
	eventd.RegisterEvent("ServerTime", &event, nil, nil)
	return &event
}
```

Keeping this registration in `internal/game` lets other server commands, including the relay example, expose the same typed events without duplicating event definitions.

Wire it into `cmd/server/main.go`:

```go
eventd := restream.NewEventDispatcher(log)
serverTimeEvent := game.RegisterServerTimeEvent(eventd)
```

Pass `eventd` to `AddSocketHandlers`:

```go
restream.AddSocketHandlers(conn, log, sdr, rpcd.FireRPC, eventd, func() (restream.AccessLevel, error) {
```

Add a ticker:

```go
go func() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for t := range ticker.C {
		serverTimeEvent.Fire(t)
	}
}()
```

Run codegen again. It creates `ServerTimeEvent` and replaces the event placeholders with generated type info:

```bash
go tool github.com/boatkit-io/restream/cmd/codegen -project .
```

On the client, import React state helpers and the event:

```typescript
import { useEffect, useState } from 'react';
import { PlaceTokenRequest, ServerTimeEvent } from './restream/PackageGame';
```

Subscribe inside `App`:

```typescript
const [lastServerTime, setLastServerTime] = useState<Date>();

useEffect(() => rss.subscribeToEvent(ServerTimeEvent, (event) => {
  setLastServerTime(event.currentTime);
}), []);
```

Render it:

```tsx
<h2>Last Server Time: {lastServerTime ? lastServerTime.toLocaleString() : 'waiting...'}</h2>
```

Restart the server. The board state still streams through the store, and the clock updates through typed events.
