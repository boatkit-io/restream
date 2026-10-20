package client

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestHandshakeHeadersAreObtainedBeforeEveryDial(t *testing.T) {
	var calls atomic.Int32
	received := make(chan string, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		received <- request.Header.Get("X-Synthetic-Admission")
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	streamer := NewStreamer(nil, nil, nil, Config{
		Endpoint: "ws" + strings.TrimPrefix(server.URL, "http"), ReconnectDelay: time.Millisecond,
		HandshakeHeaders: func(context.Context) (http.Header, error) {
			n := calls.Add(1)
			if n == 1 {
				return nil, errors.New("bounded admission unavailable")
			}
			return http.Header{"X-Synthetic-Admission": {strconv.Itoa(int(n))}}, nil
		},
	})
	defer streamer.Close() //nolint:errcheck
	done := make(chan error, 1)
	go func() { done <- streamer.Run(ctx) }()
	for _, want := range []string{"2", "3"} {
		select {
		case got := <-received:
			if got != want {
				t.Fatalf("dial header %s, want %s", got, want)
			}
		case <-time.After(time.Second):
			t.Fatal("admission did not retry")
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled admission loop did not stop")
	}
}
