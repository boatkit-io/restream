package main

import (
	"net/http"
	"reflect"
	"time"

	"github.com/boatkit-io/restream/pkg/restream"
	"github.com/boatkit-io/restream/pkg/websocketencoder"
	"github.com/boatkit-io/tugboat/pkg/subscribableevent"
	"github.com/sirupsen/logrus"
	"github.com/zishang520/socket.io/servers/socket/v3"
	"github.com/zishang520/socket.io/v3/pkg/types"
)

type serverTimeCallback func(currentTime time.Time)

func main() {
	log := logrus.New()

	rpcd := restream.NewRPCDispatcher(log)
	eventd := restream.NewEventDispatcher(log)
	serverTimeEvent := subscribableevent.NewEvent[serverTimeCallback]()
	eventd.RegisterEvent("ServerTime", &serverTimeEvent, reflect.TypeFor[ServerTimeEvent](), reflect.TypeFor[func(time.Time)]())

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

	router := http.NewServeMux()

	so := socket.ServerOptions{}
	so.SetParser(websocketencoder.NewParser())
	so.SetCors(&types.Cors{
		Origin: "*",
	})
	io := socket.NewServer(nil, &so)

	if err := io.On("connection", func(clients ...any) {
		conn := clients[0].(*socket.Socket)
		restream.AddSocketHandlers(conn, log, sdr, rpcd.FireRPC, eventd, func() (restream.AccessLevel, error) {
			return restream.AccessLevel(1), nil
		})
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

	http.ListenAndServe(":8080", router)
}
