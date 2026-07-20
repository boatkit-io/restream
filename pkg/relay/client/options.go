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

// StorePolicy controls additional store filtering and partial debounce behavior for stores that stream to a relay.
type StorePolicy struct {
	Include         map[string]struct{}
	Exclude         map[string]struct{}
	Debounce        map[string]time.Duration
	DefaultDebounce time.Duration
	// OnDemand sends device store state only while the relay reports at least one active subscription.
	// The policy falls back to legacy always-on streaming unless the relay advertises support.
	OnDemand bool
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

// DebounceFor returns the configured debounce duration for storeName. An entry
// in Debounce takes precedence over DefaultDebounce, including a non-positive
// entry that disables debounce for that store.
func (p StorePolicy) DebounceFor(storeName string) (time.Duration, bool) {
	if d, ok := p.Debounce[storeName]; ok {
		return d, d > 0
	}
	return p.DefaultDebounce, p.DefaultDebounce > 0
}

// SendQueueFullInfo describes a dropped packet caused by relay send queue backpressure.
type SendQueueFullInfo struct {
	PacketDescription string
	Bytes             int
	QueueDepth        int
	QueueCapacity     int
}

// StorePacketSentInfo describes a store packet successfully written to the relay connection.
type StorePacketSentInfo struct {
	StoreName  string
	PacketKind protocol.PacketKind
	Bytes      int
}

// Callbacks observe streamer lifecycle and extension packets.
type Callbacks struct {
	OnConnected       func(*protocol.ConnectedPacket)
	OnDisconnected    func(error)
	OnDialError       func(error)
	OnBytesSent       func(int)
	OnStorePacketSent func(StorePacketSentInfo)
	OnSendQueueFull   func(SendQueueFullInfo)
	OnCustomPacket    func(*protocol.CustomPacket) error
	OnRawPacket       func(*protocol.RawPacket) error
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
