# Tic Tac Toe Relay

This example builds on the base [tictactoe](../tictactoe) tutorial. The base example already keeps its game state, store, RPC, and events in `internal/game`, so this relay example only adds:

- a device-side relay streamer in `cmd/server`
- a cloud relay process in `cmd/relay`
- a web URL parameter that switches between direct and relay connections

The relay auth in this example is intentionally simple and hardcoded: device ID `tictactoe-local`, auth type `shared-key`, and credential `tictactoe-secret`.

## Starting Point

Start from the completed base `examples/tictactoe` project:

```bash
cd examples
cp -R tictactoe tictactoerelay
cd tictactoerelay
rm -rf web/node_modules web/dist
```

Update `go.mod` to use the new module path:

```go
module github.com/boatkit-io/restream/examples/tictactoerelay
```

Update copied Go imports to use the new module path:

```bash
perl -pi -e 's#github.com/boatkit-io/restream/examples/tictactoe#github.com/boatkit-io/restream/examples/tictactoerelay#g' cmd/server/main.go internal/game/*.go
```

Optionally update `web/package.json` so the package name is distinct:

```json
{
  "name": "tictactoerelay-web"
}
```

Configure relay store generation to write the cloud relay factory into a package that does not expose or import the device-side store implementation:

```yaml
inputDirs:
  - internal/game
tsDir: web/src/restream
goRelayStoresDir: internal/relaystores
```

## Device Server

The device server still owns the real game state and still serves direct web clients on `:8080`. Add a relay streamer so it also connects to the cloud relay and streams the same stores, events, and RPC surface.

In `cmd/server/main.go`, add the relay client import:

```go
import (
	"context"

	relayclient "github.com/boatkit-io/restream/pkg/relay/client"
)
```

Add local relay constants near the top of the file:

```go
const (
	deviceID        = "tictactoe-local"
	relayEndpoint   = "ws://localhost:8090/device"
	relayAuthType   = "shared-key"
	relayCredential = "tictactoe-secret"
)
```

After creating the store registry, start the relay streamer:

```go
streamer := relayclient.NewStreamer(sdr, rpcd.FireRPC, eventd, relayclient.Config{
	Endpoint: relayEndpoint,
	Credentials: relayclient.Credentials{
		DeviceID: deviceID,
		AuthType: relayAuthType,
		AuthData: []byte(relayCredential),
		Metadata: map[string]string{
			"example": "tictactoerelay",
		},
	},
})
defer streamer.Close() //nolint:errcheck
go func() {
	if err := streamer.Run(context.Background()); err != nil {
		log.WithError(err).Error("relay streamer stopped")
	}
}()
```

The streamer sends full store states, store partials, and server-originated events to the relay. It also receives relay-forwarded RPC calls from cloud viewers and dispatches them through `rpcd.FireRPC`.

## Cloud Relay Server

Add a second Go command at `cmd/relay/main.go`. It has two endpoints:

- `/device` is a plain websocket endpoint for the device streamer.
- `/socket` is a socket.io endpoint for cloud web viewers.

```go
package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/boatkit-io/restream/examples/tictactoerelay/internal/game"
	"github.com/boatkit-io/restream/examples/tictactoerelay/internal/relaystores"
	"github.com/boatkit-io/restream/pkg/relay/protocol"
	relayserver "github.com/boatkit-io/restream/pkg/relay/server"
	"github.com/boatkit-io/restream/pkg/restream"
	"github.com/boatkit-io/restream/pkg/websocketencoder"
	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"
	"github.com/zishang520/socket.io/servers/socket/v3"
	"github.com/zishang520/socket.io/v3/pkg/types"
)

const (
	deviceID        = "tictactoe-local"
	relayAuthType   = "shared-key"
	relayCredential = "tictactoe-secret"
)

func main() {
	log := logrus.New()

	manager := relayserver.NewDeviceManager(relayserver.DeviceManagerConfig{
		Stores: func(_ string) ([]restream.Store, error) {
			return relaystores.NewRelayStores(), nil
		},
		ConfigureDevice: func(device *relayserver.Device) error {
			game.RegisterServerTimeEvent(device.EventDispatcher)
			return nil
		},
		EventHandler: func(device *relayserver.Device, _ *relayserver.Connection, eventName string, eventBytes []byte) error {
			return device.EventDispatcher.FireSerializedEvent(eventName, eventBytes)
		},
	})

	deviceRelay := relayserver.New(relayserver.Config{
		DeviceManager: manager,
		Metadata: map[string]string{
			"example": "tictactoerelay",
		},
		AuthenticateDevice: func(_ context.Context, hello *protocol.DeviceHello, _ *relayserver.Connection) (restream.AccessLevel, error) {
			if hello.DeviceID != deviceID || hello.AuthType != relayAuthType || string(hello.AuthData) != relayCredential {
				return restream.AccessLevel(0), fmt.Errorf("invalid relay credentials")
			}
			return restream.AccessLevel(1), nil
		},
	})

	router := http.NewServeMux()
	deviceUpgrader := websocket.Upgrader{
		CheckOrigin: func(_ *http.Request) bool {
			return true
		},
	}
	router.HandleFunc("/device", func(w http.ResponseWriter, r *http.Request) {
		conn, err := deviceUpgrader.Upgrade(w, r, nil)
		if err != nil {
			log.WithError(err).Warn("failed to upgrade device websocket")
			return
		}
		if err := deviceRelay.AcceptConn(r.Context(), conn); err != nil {
			log.WithError(err).Warn("device relay connection closed")
		}
	})

	so := socket.ServerOptions{}
	so.SetParser(websocketencoder.NewParser())
	so.SetCors(&types.Cors{Origin: "*"})
	io := socket.NewServer(nil, &so)

	if err := io.On("connection", func(clients ...any) {
		conn := clients[0].(*socket.Socket)
		device, err := manager.GetDevice(deviceID)
		if err != nil {
			log.WithError(err).Warn("failed to load relay device")
			conn.Disconnect(true)
			return
		}
		if err := restream.AddSocketHandlers(conn, log, device.StoreRegistry, device.RPCHandler, device.EventDispatcher, func() (restream.AccessLevel, error) {
			return restream.AccessLevel(1), nil
		}); err != nil {
			log.WithError(err).Warn("failed to add relay socket handlers")
			conn.Disconnect(true)
		}
	}); err != nil {
		panic(err)
	}

	socketHandler := io.ServeHandler(&so)
	router.Handle("/socket", socketHandler)
	router.Handle("/socket/", socketHandler)

	http.ListenAndServe(":8090", router) //nolint:errcheck
}
```

The `DeviceManager` is the reusable relay piece:

- `Stores` builds relay stores for each device. `relaystores.NewRelayStores()` is generated into `internal/relaystores` by `goRelayStoresDir`; it includes `DeviceWithRelay` stores and hardcodes each source store's optional `GetMinimumAccessLevel` constant into the relay store factory.
- `AuthenticateDevice` checks the device hello and returns the device connection access level.
- `ConfigureDevice` registers the same typed events that the device server exposes.
- `EventHandler` passes serialized device-originated events through the relay device's `EventDispatcher`.
- `device.RPCHandler` forwards cloud viewer RPCs back to the connected device server. Cloud viewer access still comes from the relay's `AddSocketHandlers` access lookup.

## Web Switcher

The base web app already imports generated types from `PackageGame`. Change the socket URL creation in `web/src/App.tsx` to use a `mode` URL parameter:

```typescript
const params = new URLSearchParams(window.location.search);
const connectionMode = params.get('mode') === 'relay' ? 'relay' : 'direct';
const serverURL = connectionMode === 'relay' ? 'http://localhost:8090' : 'http://localhost:8080';

const socket = SocketIoClient(serverURL, {
  path: '/socket',
  reconnection: true,
});
```

Add links if you want to switch in-browser:

```tsx
<p>
  Connected to {connectionMode === 'relay' ? 'cloud relay on :8090' : 'direct server on :8080'}.
  {' '}
  <a href="?mode=direct">Direct</a>
  {' | '}
  <a href="?mode=relay">Relay</a>
</p>
```

## Run It

Start the game server:

```bash
go run ./cmd/server
```

Start the relay server in another terminal:

```bash
go run ./cmd/relay
```

The server process keeps trying to connect to the relay, so the start order between `cmd/server` and `cmd/relay` does not matter.

Start the website:

```bash
cd web
pnpm install
pnpm dev
```

Open the direct connection:

```text
http://localhost:5173/?mode=direct
```

Open the relay connection:

```text
http://localhost:5173/?mode=relay
```

Both pages should show the same board. Clicks through either page call the same `PlaceToken` RPC on the game server. The direct page sends RPCs straight to `cmd/server`; the relay page sends RPCs to `cmd/relay`, which forwards them to the connected game server process over the relay protocol.

## Codegen

Because this example still uses the same `internal/game` package as the base example, codegen uses the same command. The copied `BoardStore` uses `@restream.store(BoardStore)`, so this also regenerates `BoardStoreName`, the Store interface methods including `GetStoreType`, and the TypeScript store-name constant from the annotation:

```bash
go tool github.com/boatkit-io/restream/cmd/codegen -project .
```
