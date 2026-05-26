package client

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/boatkit-io/restream/pkg/relay/protocol"
	gws "github.com/gorilla/websocket"
)

func TestStorePolicyAllows(t *testing.T) {
	policy := StorePolicy{
		Include: map[string]struct{}{
			"IncludedStore": {},
		},
		Exclude: map[string]struct{}{
			"ExcludedStore": {},
		},
	}

	if !policy.Allows("IncludedStore") {
		t.Fatal("IncludedStore should be allowed")
	}
	if policy.Allows("OtherStore") {
		t.Fatal("OtherStore should not be allowed when Include is set")
	}
	if policy.Allows("ExcludedStore") {
		t.Fatal("ExcludedStore should not be allowed")
	}
}

func TestStaticConfigProvidesEndpointAndCredentials(t *testing.T) {
	s := NewStreamer(nil, nil, nil, Config{
		Endpoint: "wss://relay.example/device",
		Credentials: Credentials{
			DeviceID: "device-1",
			AuthType: "shared-key",
			AuthData: []byte("secret"),
		},
	})

	if s.opts.Endpoint != "wss://relay.example/device" {
		t.Fatalf("endpoint = %q, want wss://relay.example/device", s.opts.Endpoint)
	}

	credentials := s.opts.Credentials
	if credentials.DeviceID != "device-1" {
		t.Fatalf("DeviceID = %q, want device-1", credentials.DeviceID)
	}
	if credentials.AuthType != "shared-key" {
		t.Fatalf("AuthType = %q, want shared-key", credentials.AuthType)
	}
	if string(credentials.AuthData) != "secret" {
		t.Fatalf("AuthData = %q, want secret", credentials.AuthData)
	}
}

func TestRunRequiresEndpoint(t *testing.T) {
	s := NewStreamer(nil, nil, nil, Config{})

	err := s.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "endpoint") {
		t.Fatalf("Run error = %v, want endpoint error", err)
	}
}

func TestEnqueuePacketAfterDisconnectDoesNotPanic(t *testing.T) {
	s := &Streamer{}

	if err := s.enqueuePacket("test packet", []byte{1}); !errors.Is(err, ErrDisconnected) {
		t.Fatalf("enqueuePacket error = %v, want %v", err, ErrDisconnected)
	}
}

func TestEnqueuePacketFullQueueDoesNotBlock(t *testing.T) {
	done := make(chan struct{})
	sendQueue := make(chan []byte, 1)
	sendQueue <- []byte{1}

	var queueFullInfo SendQueueFullInfo
	s := &Streamer{
		conn:      &gws.Conn{},
		sendQueue: sendQueue,
		sendDone:  done,
		opts: Config{
			Callbacks: Callbacks{
				OnSendQueueFull: func(info SendQueueFullInfo) {
					queueFullInfo = info
				},
			},
		},
	}

	if err := s.enqueuePacket("test packet", []byte{2}); !errors.Is(err, ErrSendQueueFull) {
		t.Fatalf("enqueuePacket error = %v, want %v", err, ErrSendQueueFull)
	}
	if queueFullInfo.PacketDescription != "test packet" {
		t.Fatalf("packet description = %q, want test packet", queueFullInfo.PacketDescription)
	}
	if queueFullInfo.QueueDepth != 1 || queueFullInfo.QueueCapacity != 1 {
		t.Fatalf("queue info = %+v, want depth/capacity 1/1", queueFullInfo)
	}
}

func TestSendEventWritesGenericEventPacket(t *testing.T) {
	done := make(chan struct{})
	sendQueue := make(chan []byte, 1)

	s := &Streamer{
		conn:      &gws.Conn{},
		sendQueue: sendQueue,
		sendDone:  done,
	}

	if err := s.SendEvent("TrackedPointUpdate", []byte{1, 2, 3}); err != nil {
		t.Fatalf("SendEvent failed: %v", err)
	}

	packetBytes := <-sendQueue
	packet, err := protocol.DecodePacket(packetBytes)
	if err != nil {
		t.Fatalf("DecodePacket failed: %v", err)
	}
	eventPacket, ok := packet.(*protocol.EventPacket)
	if !ok {
		t.Fatalf("packet type = %T, want *protocol.EventPacket", packet)
	}
	if eventPacket.EventName != "TrackedPointUpdate" {
		t.Fatalf("event name = %q, want TrackedPointUpdate", eventPacket.EventName)
	}
	if string(eventPacket.Data) != string([]byte{1, 2, 3}) {
		t.Fatalf("event bytes = %v, want [1 2 3]", eventPacket.Data)
	}
}
