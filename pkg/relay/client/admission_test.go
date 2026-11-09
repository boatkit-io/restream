package client

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gws "github.com/gorilla/websocket"
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

func TestStoppedConnectionAttemptDoesNotReportDialFailure(t *testing.T) {
	for _, stage := range []string{"headers", "dial"} {
		for _, stop := range []string{"cancel", "shutdown"} {
			t.Run(stage+"/"+stop, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				started, release := make(chan struct{}), make(chan struct{})
				defer close(release)
				var failures atomic.Int32
				attempt := func() error {
					close(started)
					<-release
					// An adapter may sanitize the transport error, so stopping
					// must use the owner's lifetime instead of the error text.
					return errors.New("connection preparation unavailable")
				}
				opts := Config{Endpoint: "ws://synthetic.invalid", ReconnectDelay: time.Hour,
					Callbacks: Callbacks{OnDialError: func(error) { failures.Add(1) }}}
				if stage == "headers" {
					opts.HandshakeHeaders = func(context.Context) (http.Header, error) { return nil, attempt() }
				} else {
					opts.Dialer = &gws.Dialer{NetDialContext: func(context.Context, string, string) (net.Conn, error) {
						return nil, attempt()
					}}
				}
				streamer := NewStreamer(nil, nil, nil, opts)
				defer streamer.Close() //nolint:errcheck
				done := make(chan error, 1)
				go func() { done <- streamer.Run(ctx) }()
				select {
				case <-started:
				case <-time.After(time.Second):
					t.Fatal("connection attempt did not start")
				}
				if stop == "shutdown" {
					streamer.Shutdown()
				} else {
					cancel()
				}
				release <- struct{}{}
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(time.Second):
					t.Fatal("stopped connection attempt did not return")
				}
				if failures.Load() != 0 {
					t.Fatal("stopped connection attempt reported a dial failure")
				}
			})
		}
	}
}

func TestStoppedHeaderPreparationDoesNotDialAfterLateSuccess(t *testing.T) {
	for _, stop := range []string{"cancel", "shutdown"} {
		t.Run(stop, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var streamer *Streamer
			var dials int
			streamer = NewStreamer(nil, nil, nil, Config{
				Endpoint: "ws://synthetic.invalid",
				HandshakeHeaders: func(context.Context) (http.Header, error) {
					if stop == "shutdown" {
						streamer.Shutdown()
					} else {
						cancel()
					}
					return http.Header{"Authorization": {"Bearer synthetic-ticket"}}, nil
				},
				Dialer: &gws.Dialer{NetDialContext: func(context.Context, string, string) (net.Conn, error) {
					dials++
					return nil, errors.New("retired connection attempt must not dial")
				}},
			})
			defer streamer.Close() //nolint:errcheck
			if err := streamer.Run(ctx); err != nil {
				t.Fatal(err)
			}
			if dials != 0 {
				t.Fatal("retired connection attempt opened a transport")
			}
		})
	}
}

func TestActiveHeaderPreparationFailureIsReportedAndRetried(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	failure := errors.New("connection preparation unavailable")
	var attempts, failures int
	streamer := NewStreamer(nil, nil, nil, Config{
		Endpoint: "ws://synthetic.invalid", ReconnectDelay: time.Millisecond,
		HandshakeHeaders: func(context.Context) (http.Header, error) { attempts++; return nil, failure },
		Callbacks: Callbacks{OnDialError: func(err error) {
			if !errors.Is(err, failure) {
				t.Errorf("unexpected dial failure: %v", err)
			}
			failures++
			if failures == 2 {
				cancel()
			}
		}},
	})
	defer streamer.Close() //nolint:errcheck
	if err := streamer.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 || failures != 2 {
		t.Fatalf("attempts=%d failures=%d, want two reported failures", attempts, failures)
	}
}
