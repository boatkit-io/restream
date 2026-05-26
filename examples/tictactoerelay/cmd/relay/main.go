package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/boatkit-io/restream/examples/tictactoerelay/internal/game"
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
			return []restream.Store{
				restream.NewRelayStore[game.BoardStoreState, *game.BoardStoreState, *game.BoardStoreStatePartial](
					game.BoardStoreName,
					&game.BoardStoreState{},
				),
			}, nil
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
	so.SetCors(&types.Cors{
		Origin: "*",
	})
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

	http.ListenAndServe(":8090", router) //nolint:errcheck // Why: Example server exits on listener failure.
}
