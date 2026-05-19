package main

import (
	"net/http"

	"github.com/boatkit-io/restream/pkg/restream"
	"github.com/boatkit-io/restream/pkg/websocketencoder"
	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
	"github.com/zishang520/socket.io/servers/socket/v3"
	"github.com/zishang520/socket.io/v3/pkg/types"
)

func main() {
	log := logrus.New()

	rpcd := restream.NewRPCDispatcher(log)
	boardStore, err := NewBoardStore(rpcd)
	if err != nil {
		panic(err)
	}

	sdr, err := restream.NewStoreRegistry([]restream.Store{
		boardStore,
	})
	if err != nil {
		panic(err)
	}

	router := mux.NewRouter()

	so := socket.ServerOptions{}
	so.SetParser(websocketencoder.NewParser())
	so.SetCors(&types.Cors{
		Origin: "*",
	})
	io := socket.NewServer(nil, &so)

	if err := io.On("connection", func(clients ...any) {
		conn := clients[0].(*socket.Socket)
		restream.AddSocketHandlers(conn, log, sdr, rpcd.FireRPC, func() (restream.AccessLevel, error) {
			return restream.AccessLevel(1), nil
		})
	}); err != nil {
		panic(err)
	}

	router.PathPrefix("/socket").Handler(io.ServeHandler(&so))

	http.ListenAndServe(":8080", router)
}
