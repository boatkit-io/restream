package server

import (
	"fmt"
	"sync"

	"github.com/boatkit-io/restream/pkg/relay/protocol"
	"github.com/boatkit-io/restream/pkg/restream"
	gws "github.com/gorilla/websocket"
)

const (
	duplicateRelayConnectionReason          = "replaced by a newer relay connection for this device"
	storeSubscriptionForwardingFailedReason = "store subscription forwarding failed; reconnect required"
)

// Device stores aggregated relay data for one device.
type Device struct {
	DeviceID        string
	StoreRegistry   *restream.StoreRegistry
	EventDispatcher *restream.EventDispatcher

	config DeviceManagerConfig

	relaySubscriptionStores []relaySubscriptionStore

	connMutex sync.RWMutex
	conn      *Connection

	rpcMutex    sync.Mutex
	rpcNextID   uint32
	rpcsPending map[uint32]pendingRPC
}

type pendingRPC struct {
	conn   *Connection
	respCh chan []byte
}

type relaySubscriptionStore interface {
	restream.Store
	SetKeySubscriptionForwarder(restream.RelayStoreKeySubscriptionForwarder)
	ActiveSubscriptionKeys() []string
}

// NewDevice creates a Device around an existing store registry.
func NewDevice(deviceID string, sr *restream.StoreRegistry, config DeviceManagerConfig) *Device {
	return &Device{
		DeviceID:        deviceID,
		StoreRegistry:   sr,
		EventDispatcher: restream.NewEventDispatcher(nil),

		config: config,

		rpcNextID:   1,
		rpcsPending: map[uint32]pendingRPC{},
	}
}

// DeviceConnected records an active relay connection for this device.
func (d *Device) DeviceConnected(conn *Connection) {
	var previous *Connection
	d.connMutex.Lock()
	if d.conn != nil && d.conn != conn {
		previous = d.conn
	}
	d.conn = conn
	d.connMutex.Unlock()

	if previous != nil {
		previous.CloseWithReason(gws.ClosePolicyViolation, duplicateRelayConnectionReason) //nolint:errcheck // Why: Closing stale connection best-effort.
		d.closePendingRPCsForConn(previous)
	}

	if d.config.OnDeviceConnected != nil {
		d.config.OnDeviceConnected(d, conn)
	}
}

// DeviceDisconnected clears an active relay connection for this device.
func (d *Device) DeviceDisconnected(conn *Connection) {
	d.connMutex.Lock()
	wasCurrent := d.conn == conn
	if wasCurrent {
		d.conn = nil
	}
	d.connMutex.Unlock()

	if wasCurrent && d.config.OnDeviceDisconnected != nil {
		d.config.OnDeviceDisconnected(d, conn)
	}

	d.closePendingRPCsForConn(conn)
}

func (d *Device) configureRelaySubscriptionForwarding(stores []restream.Store) {
	for _, store := range stores {
		relayStore, ok := store.(relaySubscriptionStore)
		if !ok {
			continue
		}
		relayStore.SetKeySubscriptionForwarder(d.forwardStoreSubscription)
		d.relaySubscriptionStores = append(d.relaySubscriptionStores, relayStore)
	}
}

func (d *Device) forwardStoreSubscription(storeName string, key string, subscribe bool) {
	d.connMutex.RLock()
	conn := d.conn
	d.connMutex.RUnlock()
	if conn == nil {
		return
	}

	if err := conn.SendStoreSubscription(storeName, key, subscribe); err != nil {
		conn.CloseWithReason(gws.CloseGoingAway, storeSubscriptionForwardingFailedReason) //nolint:errcheck // Why: Closing forces reconnect and subscription replay.
	}
}

func (d *Device) sendActiveStoreSubscriptions(conn *Connection) error {
	for _, store := range d.relaySubscriptionStores {
		for _, key := range store.ActiveSubscriptionKeys() {
			if err := conn.SendStoreSubscription(store.GetName(), key, true); err != nil {
				return err
			}
		}
	}
	return nil
}

// HandleFullState handles a full store state packet from the connected device.
func (d *Device) HandleFullState(conn *Connection, storeName string, data []byte) error {
	if d.config.FullStateHandler != nil {
		return d.config.FullStateHandler(d, conn, storeName, data)
	}
	return d.ApplyFullState(storeName, data)
}

// ApplyFullState applies a full store state packet to this device's store registry.
func (d *Device) ApplyFullState(storeName string, data []byte) error {
	if !d.StoreRegistry.IsStoreValid(storeName) {
		return d.handleUnknownStore(storeName)
	}
	return d.StoreRegistry.SetFullStateToStore(storeName, data)
}

// HandlePartialState handles a partial store state packet from the connected device.
func (d *Device) HandlePartialState(conn *Connection, storeName string, data []byte) error {
	if d.config.PartialStateHandler != nil {
		return d.config.PartialStateHandler(d, conn, storeName, data)
	}
	return d.ApplyPartialState(storeName, data)
}

// ApplyPartialState applies a partial store state packet to this device's store registry.
func (d *Device) ApplyPartialState(storeName string, data []byte) error {
	if !d.StoreRegistry.IsStoreValid(storeName) {
		return d.handleUnknownStore(storeName)
	}
	return d.StoreRegistry.ApplyPartialToStore(storeName, data)
}

// HandleEvent handles a serialized event packet from the connected device.
func (d *Device) HandleEvent(conn *Connection, eventName string, eventBytes []byte) error {
	if d.config.EventHandler == nil {
		return nil
	}
	return d.config.EventHandler(d, conn, eventName, eventBytes)
}

// HandleRPCResponse handles a serialized RPC response packet from the connected device.
func (d *Device) HandleRPCResponse(conn *Connection, rpcID uint32, rpcData []byte) error {
	if d.config.RPCResponseHandler != nil {
		return d.config.RPCResponseHandler(d, conn, rpcID, rpcData)
	}
	return d.CompleteRPCResponse(rpcID, rpcData)
}

// CompleteRPCResponse completes a pending RPC forwarded by RPCHandler.
func (d *Device) CompleteRPCResponse(rpcID uint32, rpcData []byte) error {
	d.rpcMutex.Lock()
	pending, ok := d.rpcsPending[rpcID]
	if !ok {
		d.rpcMutex.Unlock()
		return fmt.Errorf("no pending RPC found for ID %d", rpcID)
	}
	pending.respCh <- rpcData
	close(pending.respCh)
	delete(d.rpcsPending, rpcID)
	d.rpcMutex.Unlock()
	return nil
}

// HandleCustomPacket handles a relay custom packet from the connected device.
func (d *Device) HandleCustomPacket(conn *Connection, packet *protocol.CustomPacket) error {
	if d.config.CustomPacketHandler != nil {
		return d.config.CustomPacketHandler(d, conn, packet)
	}
	return nil
}

// HandleRawPacket handles an unknown relay protocol packet from the connected device.
func (d *Device) HandleRawPacket(conn *Connection, packet *protocol.RawPacket) error {
	if d.config.RawPacketHandler != nil {
		return d.config.RawPacketHandler(d, conn, packet)
	}
	return nil
}

// RPCHandler handles cloud viewer RPCs by trying GlobalRPC first, then forwarding unhandled RPCs to the connected device.
func (d *Device) RPCHandler(name string, accessLevel restream.AccessLevel, binaryData []byte) ([]byte, bool, error) {
	if d.config.GlobalRPC != nil {
		resp, handled, err := d.config.GlobalRPC(name, accessLevel, binaryData)
		if handled {
			return resp, true, err
		}
	}

	d.connMutex.RLock()
	conn := d.conn
	d.connMutex.RUnlock()
	if conn == nil {
		return nil, false, fmt.Errorf("no connected device available to handle request")
	}

	d.rpcMutex.Lock()
	respCh := make(chan []byte, 1)
	rpcID := d.rpcNextID
	d.rpcNextID++
	d.rpcsPending[rpcID] = pendingRPC{conn: conn, respCh: respCh}
	d.rpcMutex.Unlock()

	if err := conn.SendRPC(rpcID, name, accessLevel, binaryData); err != nil {
		d.rpcMutex.Lock()
		delete(d.rpcsPending, rpcID)
		d.rpcMutex.Unlock()
		return nil, false, fmt.Errorf("error sending RPC: %w", err)
	}

	resp, open := <-respCh
	if !open {
		return nil, false, fmt.Errorf("device disconnected while waiting for response")
	}

	d.rpcMutex.Lock()
	delete(d.rpcsPending, rpcID)
	d.rpcMutex.Unlock()
	return resp, true, nil
}

func (d *Device) closePendingRPCsForConn(conn *Connection) {
	d.rpcMutex.Lock()
	for rpcID, pending := range d.rpcsPending {
		if pending.conn != conn {
			continue
		}
		close(pending.respCh)
		delete(d.rpcsPending, rpcID)
	}
	d.rpcMutex.Unlock()
}

func (d *Device) handleUnknownStore(storeName string) error {
	if d.config.UnknownStorePolicy == UnknownStoreIgnore {
		return nil
	}
	return fmt.Errorf("unknown relay store %s", storeName)
}
