package main

import (
	"context"
	"net/http"
	"time"

	"github.com/boatkit-io/restream/examples/tictactoerelay/internal/game"
	relayclient "github.com/boatkit-io/restream/pkg/relay/client"
	"github.com/boatkit-io/restream/pkg/restream"
	"github.com/boatkit-io/restream/pkg/websocketencoder"
	"github.com/sirupsen/logrus"
	"github.com/zishang520/socket.io/servers/socket/v3"
	"github.com/zishang520/socket.io/v3/pkg/types"
)

const (
	deviceID        = "tictactoe-local"
	relayEndpoint   = "ws://localhost:8090/device"
	relayAuthType   = "shared-key"
	relayCredential = "tictactoe-secret"
)

func main() {
	log := logrus.New()

	rpcd := restream.NewRPCDispatcher(log)
	eventd := restream.NewEventDispatcher(log)
	serverTimeEvent := game.RegisterServerTimeEvent(eventd)

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
	defer streamer.Close() //nolint:errcheck // Why: Process shutdown best-effort cleanup.
	go func() {
		if err := streamer.Run(context.Background()); err != nil {
			log.WithError(err).Error("relay streamer stopped")
		}
	}()

	router := http.NewServeMux()

	so := socket.ServerOptions{}
	so.SetParser(websocketencoder.NewParser())
	so.SetCors(&types.Cors{
		Origin: "*",
	})
	io := socket.NewServer(nil, &so)

	if err := io.On("connection", func(clients ...any) {
		conn := clients[0].(*socket.Socket)
		if err := restream.AddSocketHandlers(conn, log, sdr, rpcd.FireRPC, eventd, func() (restream.AccessLevel, error) {
			return restream.AccessLevel(1), nil
		}); err != nil {
			log.WithError(err).Warn("failed to add direct socket handlers")
			conn.Disconnect(true)
		}
	}); err != nil {
		panic(err)
	}

	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for t := range ticker.C {
			serverTimeEvent.Fire(t)
		}
	}()

	socketHandler := io.ServeHandler(&so)
	router.Handle("/socket", socketHandler)
	router.Handle("/socket/", socketHandler)

	http.ListenAndServe(":8080", router) //nolint:errcheck // Why: Example server exits on listener failure.
}
