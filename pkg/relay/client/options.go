// Package client streams a local Restream store registry to a relay server.
package client

import (
	"time"

	"github.com/boatkit-io/restream/pkg/relay/protocol"
	gws "github.com/gorilla/websocket"
)

const (
	defaultReconnectDelay = time.Second
	defaultWriteTimeout   = 5 * time.Second
	defaultSendQueueSize  = 1000
)

// Credentials are sent in the device hello packet when connecting to a relay server.
type Credentials struct {
	DeviceID string
	AuthType string
	AuthData []byte
	Metadata map[string]string
}

// StorePolicy controls which stores are sent to the relay and how partials are debounced.
type StorePolicy struct {
	Include  map[string]struct{}
	Exclude  map[string]struct{}
	Debounce map[string]time.Duration
}

// Allows reports whether storeName should be streamed.
func (p StorePolicy) Allows(storeName string) bool {
	if len(p.Include) > 0 {
		if _, ok := p.Include[storeName]; !ok {
			return false
		}
	}
	if _, ok := p.Exclude[storeName]; ok {
		return false
	}
	return true
}

// DebounceFor returns the configured debounce duration for storeName.
func (p StorePolicy) DebounceFor(storeName string) (time.Duration, bool) {
	d, ok := p.Debounce[storeName]
	return d, ok && d > 0
}

// SendQueueFullInfo describes a dropped packet caused by relay send queue backpressure.
type SendQueueFullInfo struct {
	PacketDescription string
	Bytes             int
	QueueDepth        int
	QueueCapacity     int
}

// Callbacks observe streamer lifecycle and extension packets.
type Callbacks struct {
	OnConnected     func(*protocol.ConnectedPacket)
	OnDisconnected  func(error)
	OnDialError     func(error)
	OnBytesSent     func(int)
	OnSendQueueFull func(SendQueueFullInfo)
	OnCustomPacket  func(*protocol.CustomPacket) error
	OnRawPacket     func(*protocol.RawPacket) error
}

// Config configures a Streamer.
type Config struct {
	Endpoint    string
	Credentials Credentials

	StorePolicy StorePolicy

	ReconnectDelay time.Duration
	WriteTimeout   time.Duration
	SendQueueSize  int
	Dialer         *gws.Dialer

	Callbacks Callbacks
}

// DefaultConfig returns the relay client defaults.
func DefaultConfig() Config {
	dialer := gws.Dialer{
		EnableCompression: true,
	}
	return Config{
		ReconnectDelay: defaultReconnectDelay,
		WriteTimeout:   defaultWriteTimeout,
		SendQueueSize:  defaultSendQueueSize,
		Dialer:         &dialer,
	}
}

func applyDefaults(config Config) Config {
	defaults := DefaultConfig()
	if config.ReconnectDelay <= 0 {
		config.ReconnectDelay = defaults.ReconnectDelay
	}
	if config.WriteTimeout <= 0 {
		config.WriteTimeout = defaults.WriteTimeout
	}
	if config.SendQueueSize <= 0 {
		config.SendQueueSize = defaults.SendQueueSize
	}
	if config.Dialer == nil {
		config.Dialer = defaults.Dialer
	}
	return config
}
