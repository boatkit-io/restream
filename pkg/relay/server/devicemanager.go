package server

import (
	"context"
	"fmt"
	"sync"

	"github.com/boatkit-io/restream/pkg/relay/protocol"
	"github.com/boatkit-io/restream/pkg/restream"
)

// StoreFactory builds the relay stores used to aggregate one device's streamed state.
type StoreFactory func(deviceID string) ([]restream.Store, error)

// StoreStateHandler handles a serialized store state packet from a device.
type StoreStateHandler func(device *Device, conn *Connection, storeName string, data []byte) error

// EventHandler handles serialized device-originated events.
type EventHandler func(device *Device, conn *Connection, eventName string, eventBytes []byte) error

// KeyedEventHandler handles serialized store-owned keyed events from a device.
type KeyedEventHandler func(
	device *Device,
	conn *Connection,
	storeName string,
	eventName string,
	key string,
	eventBytes []byte,
) error

// RPCResponseHandler handles a serialized device RPC response.
type RPCResponseHandler func(device *Device, conn *Connection, rpcID uint32, rpcData []byte) error

// RelayRPCHandler handles an RPC initiated by an authenticated device. Device identity
// comes from the authenticated relay connection rather than the request payload.
type RelayRPCHandler func(
	ctx context.Context,
	device *Device,
	conn *Connection,
	methodName string,
	request []byte,
) (response []byte, handled bool, err error)

// CustomPacketHandler handles relay custom packets from a device.
type CustomPacketHandler func(device *Device, conn *Connection, packet *protocol.CustomPacket) error

// RawPacketHandler handles unknown relay protocol packets from a device.
type RawPacketHandler func(device *Device, conn *Connection, packet *protocol.RawPacket) error

// UnknownStorePolicy controls how Device handles streamed store packets not present in its registry.
type UnknownStorePolicy int

const (
	// UnknownStoreError returns an error when a device streams a store that is not registered.
	UnknownStoreError UnknownStorePolicy = iota
	// UnknownStoreIgnore ignores packets for unknown stores.
	UnknownStoreIgnore
)

// DeviceManagerConfig configures a DeviceManager.
type DeviceManagerConfig struct {
	Stores    StoreFactory
	GlobalRPC restream.RPCHandlerFunc
	// GlobalFFRPC and GlobalFFRPCWithAnnotations are mutually exclusive.
	GlobalFFRPC                restream.FFRPCHandlerFunc
	GlobalFFRPCWithAnnotations restream.FFRPCHandlerWithAnnotationsFunc
	FullStateHandler           StoreStateHandler
	PartialStateHandler        StoreStateHandler
	EventHandler               EventHandler
	KeyedEventHandler          KeyedEventHandler
	RPCResponseHandler         RPCResponseHandler
	RelayRPCHandler            RelayRPCHandler
	CustomPacketHandler        CustomPacketHandler
	RawPacketHandler           RawPacketHandler
	UnknownStorePolicy         UnknownStorePolicy
	ConfigureDevice            func(*Device) error

	OnDeviceConnected    func(*Device, *Connection)
	OnDeviceDisconnected func(*Device, *Connection)
}

// DeviceManager owns the relay data for all known devices.
type DeviceManager struct {
	config DeviceManagerConfig

	mutex   sync.Mutex
	devices map[string]*Device
}

// NewDeviceManager creates a DeviceManager.
func NewDeviceManager(config DeviceManagerConfig) *DeviceManager {
	validateDeviceManagerFFRPCHandlers(config)
	return &DeviceManager{
		config:  config,
		devices: map[string]*Device{},
	}
}

func validateDeviceManagerFFRPCHandlers(config DeviceManagerConfig) {
	if config.GlobalFFRPC != nil && config.GlobalFFRPCWithAnnotations != nil {
		panic("GlobalFFRPC and GlobalFFRPCWithAnnotations are mutually exclusive")
	}
}

// HasDevice reports whether data has been created for deviceID.
func (m *DeviceManager) HasDevice(deviceID string) bool {
	m.mutex.Lock()
	_, has := m.devices[deviceID]
	m.mutex.Unlock()
	return has
}

// GetDevice gets or creates relay data for deviceID.
func (m *DeviceManager) GetDevice(deviceID string) (*Device, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	device, has := m.devices[deviceID]
	if has {
		return device, nil
	}

	if m.config.Stores == nil {
		return nil, fmt.Errorf("relay device store factory is not configured")
	}
	stores, err := m.config.Stores(deviceID)
	if err != nil {
		return nil, err
	}
	sr, err := restream.NewStoreRegistry(stores)
	if err != nil {
		return nil, err
	}

	device = NewDevice(deviceID, sr, m.config)
	if m.config.ConfigureDevice != nil {
		if err := m.config.ConfigureDevice(device); err != nil {
			return nil, err
		}
	}
	device.configureRelaySubscriptionForwarding()
	m.devices[deviceID] = device
	return device, nil
}
