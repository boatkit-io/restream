package server

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/boatkit-io/restream/pkg/relay/protocol"
	"github.com/boatkit-io/restream/pkg/restream"
	"github.com/boatkit-io/tugboat/pkg/subscribableevent"
	gws "github.com/gorilla/websocket"
)

const (
	duplicateRelayConnectionReason               = "replaced by a newer relay connection for this device"
	storeSubscriptionForwardingFailedReason      = "store subscription forwarding failed; reconnect required"
	keyedEventSubscriptionForwardingFailedReason = "keyed event subscription forwarding failed; reconnect required"
	dataStreamSubscriptionForwardingFailedReason = "data stream subscription forwarding failed; reconnect required"
	relayStateForwardingFailedReason             = "relay state forwarding failed; reconnect required"
	defaultDataStreamTransitionTimeout           = 35 * time.Second
	maxActiveDataStreamIdentities                = 1024
	maxPendingDataStreamOperations               = 2048
	maxPendingDeviceRPCs                         = 4096
	defaultDeviceRPCTimeout                      = 30 * time.Second
)

// Device stores aggregated relay data for one device.
type Device struct {
	DeviceID        string
	StoreRegistry   *restream.StoreRegistry
	EventDispatcher *restream.EventDispatcher

	config DeviceManagerConfig

	relaySubscriptionSubID      subscribableevent.SubscriptionId
	keyedEventSubscriptionSubID subscribableevent.SubscriptionId
	relayForwardMutex           sync.Mutex
	relayForwardSubID           subscribableevent.SubscriptionId
	relayForwardConn            *Connection

	connMutex sync.RWMutex
	conn      *Connection

	dataStreamSubscriptionMutex sync.Mutex
	dataStreamSubscriptions     map[restream.DataStreamSubscription]*dataStreamSerial
	dataStreamOperationMutex    sync.Mutex
	dataStreamOperationNext     uint32
	dataStreamOperations        map[uint32]pendingDataStreamOperation

	rpcMutex    sync.Mutex
	rpcNextID   uint32
	rpcsPending map[uint32]pendingRPC
}

type pendingRPC struct {
	conn   *Connection
	respCh chan []byte
}

type dataStreamSerial struct {
	mutex       sync.Mutex
	count       int
	refs        int
	accessLevel restream.AccessLevel
}

type pendingDataStreamOperation struct {
	conn   *Connection
	result chan error
}

// NewDevice creates a Device around an existing store registry.
func NewDevice(deviceID string, sr *restream.StoreRegistry, config DeviceManagerConfig) *Device {
	validateDeviceManagerFFRPCHandlers(config)
	return &Device{
		DeviceID:        deviceID,
		StoreRegistry:   sr,
		EventDispatcher: restream.NewEventDispatcher(nil),

		config: config,

		dataStreamSubscriptions: map[restream.DataStreamSubscription]*dataStreamSerial{},
		dataStreamOperationNext: 1,
		dataStreamOperations:    map[uint32]pendingDataStreamOperation{},
		rpcNextID:               1,
		rpcsPending:             map[uint32]pendingRPC{},
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

	d.startRelayStateForwarding(conn)

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

	if wasCurrent {
		d.stopRelayStateForwarding(conn)
	}

	if wasCurrent && d.config.OnDeviceDisconnected != nil {
		d.config.OnDeviceDisconnected(d, conn)
	}

	d.closePendingRPCsForConn(conn)
	d.closePendingDataStreamOperationsForConn(conn)
}

func (d *Device) configureRelaySubscriptionForwarding() {
	if d.StoreRegistry != nil {
		d.relaySubscriptionSubID = d.StoreRegistry.SubscribeToStoreSubscriptions(d.forwardStoreSubscription)
	}
	if d.EventDispatcher != nil {
		d.keyedEventSubscriptionSubID = d.EventDispatcher.SubscribeToKeyedEventSubscriptions(
			d.forwardKeyedEventSubscription,
		)
	}
}

func (d *Device) forwardStoreSubscription(storeName string, key string, subscribe bool) {
	allowed, err := d.StoreRegistry.StoreAcceptsDeviceRelayUpdates(storeName)
	if err != nil || !allowed {
		return
	}

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

func (d *Device) forwardKeyedEventSubscription(
	storeName string,
	eventName string,
	key string,
	subscribe bool,
) {
	if d.StoreRegistry == nil {
		return
	}
	allowed, err := d.StoreRegistry.StoreAcceptsDeviceRelayUpdates(storeName)
	if err != nil || !allowed {
		return
	}

	d.connMutex.RLock()
	conn := d.conn
	d.connMutex.RUnlock()
	if conn == nil {
		return
	}

	if err := conn.SendKeyedEventSubscription(storeName, eventName, key, subscribe); err != nil {
		_ = conn.CloseWithReason(
			gws.CloseGoingAway,
			keyedEventSubscriptionForwardingFailedReason,
		)
	}
}

func (d *Device) sendActiveStoreSubscriptions(conn *Connection) error {
	storeNames := d.StoreRegistry.GetAllStoreNames()
	sort.Strings(storeNames)
	for _, storeName := range storeNames {
		allowed, err := d.StoreRegistry.StoreAcceptsDeviceRelayUpdates(storeName)
		if err != nil {
			return err
		}
		if !allowed {
			continue
		}
		keys, err := d.StoreRegistry.GetActiveStoreSubscriptionKeys(storeName)
		if err != nil {
			return err
		}
		for _, key := range keys {
			if err := conn.SendStoreSubscription(storeName, key, true); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *Device) sendActiveKeyedEventSubscriptions(conn *Connection) error {
	if d.EventDispatcher == nil || d.StoreRegistry == nil {
		return nil
	}
	for _, subscription := range d.EventDispatcher.GetActiveKeyedEventSubscriptions() {
		allowed, err := d.StoreRegistry.StoreAcceptsDeviceRelayUpdates(subscription.StoreName)
		if err != nil {
			return err
		}
		if !allowed {
			continue
		}
		if err := conn.SendKeyedEventSubscription(
			subscription.StoreName,
			subscription.EventName,
			subscription.Key,
			true,
		); err != nil {
			return err
		}
	}
	return nil
}

// ListeningToDataStream records one cloud consumer of a high-bandwidth stream.
// The device receives only the aggregate 0-to-1 transition.
func (d *Device) ListeningToDataStream(
	ctx context.Context,
	subscription restream.DataStreamSubscription,
	accessLevel restream.AccessLevel,
) error {
	if err := subscription.Validate(); err != nil {
		return err
	}
	if d.StoreRegistry == nil {
		return fmt.Errorf("data stream subscription received without a store registry")
	}
	if err := d.StoreRegistry.CheckStoreAccess(subscription.StoreName, accessLevel); err != nil {
		return err
	}
	allowed, err := d.StoreRegistry.StoreAcceptsDeviceRelayUpdates(subscription.StoreName)
	if err != nil {
		return err
	}
	if !allowed {
		return fmt.Errorf("store %q does not accept device relay data streams", subscription.StoreName)
	}

	serial, err := d.acquireDataStreamSerial(subscription, true)
	if err != nil {
		return err
	}
	defer d.releaseDataStreamSerial(subscription, serial)

	serial.mutex.Lock()
	defer serial.mutex.Unlock()
	if serial.count > 0 {
		serial.count++
		if accessLevel > serial.accessLevel {
			serial.accessLevel = accessLevel
		}
		return nil
	}

	if err := d.performDataStreamTransition(ctx, subscription, accessLevel, true); err != nil {
		return err
	}
	serial.count = 1
	serial.accessLevel = accessLevel
	return nil
}

// StopListeningToDataStream removes one cloud consumer. The device receives
// only the aggregate 1-to-0 transition.
func (d *Device) StopListeningToDataStream(
	ctx context.Context,
	subscription restream.DataStreamSubscription,
) error {
	if err := subscription.Validate(); err != nil {
		return err
	}

	serial, err := d.acquireDataStreamSerial(subscription, false)
	if err != nil {
		return err
	}
	if serial == nil {
		return nil
	}
	defer d.releaseDataStreamSerial(subscription, serial)

	serial.mutex.Lock()
	defer serial.mutex.Unlock()
	if serial.count == 0 {
		return nil
	}
	if serial.count > 1 {
		serial.count--
		return nil
	}
	if err := d.performDataStreamTransition(ctx, subscription, serial.accessLevel, false); err != nil {
		return err
	}
	serial.count = 0
	return nil
}

// ActiveDataStreamSubscriptions returns the aggregate active stream identities.
func (d *Device) ActiveDataStreamSubscriptions() []restream.DataStreamSubscription {
	d.dataStreamSubscriptionMutex.Lock()
	subscriptions := make([]restream.DataStreamSubscription, 0, len(d.dataStreamSubscriptions))
	for subscription, serial := range d.dataStreamSubscriptions {
		serial.mutex.Lock()
		if serial.count > 0 {
			subscriptions = append(subscriptions, subscription)
		}
		serial.mutex.Unlock()
	}
	d.dataStreamSubscriptionMutex.Unlock()
	sort.Slice(subscriptions, func(i int, j int) bool {
		left := subscriptions[i]
		right := subscriptions[j]
		if left.StoreName != right.StoreName {
			return left.StoreName < right.StoreName
		}
		if left.StreamName != right.StreamName {
			return left.StreamName < right.StreamName
		}
		return left.Key < right.Key
	})
	return subscriptions
}

func (d *Device) restoreActiveDataStreamSubscriptions(conn *Connection) {
	if !conn.Capabilities.DataStreams {
		return
	}
	for _, subscription := range d.ActiveDataStreamSubscriptions() {
		go d.restoreDataStreamSubscription(conn, subscription)
	}
}

func (d *Device) restoreDataStreamSubscription(
	conn *Connection,
	subscription restream.DataStreamSubscription,
) {
	delay := time.Second
	for {
		d.connMutex.RLock()
		currentConn := d.conn
		d.connMutex.RUnlock()
		if currentConn != conn {
			return
		}

		serial, err := d.acquireDataStreamSerial(subscription, false)
		if err != nil || serial == nil {
			return
		}
		serial.mutex.Lock()
		if serial.count == 0 {
			serial.mutex.Unlock()
			d.releaseDataStreamSerial(subscription, serial)
			return
		}
		accessLevel := serial.accessLevel
		err = d.performDataStreamTransition(
			context.Background(),
			subscription,
			accessLevel,
			true,
		)
		serial.mutex.Unlock()
		d.releaseDataStreamSerial(subscription, serial)
		if err == nil {
			return
		}

		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-conn.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		}
		if delay < 30*time.Second {
			delay *= 2
			if delay > 30*time.Second {
				delay = 30 * time.Second
			}
		}
	}
}

func (d *Device) acquireDataStreamSerial(
	subscription restream.DataStreamSubscription,
	create bool,
) (*dataStreamSerial, error) {
	d.dataStreamSubscriptionMutex.Lock()
	defer d.dataStreamSubscriptionMutex.Unlock()
	serial := d.dataStreamSubscriptions[subscription]
	if serial == nil && create {
		if len(d.dataStreamSubscriptions) >= maxActiveDataStreamIdentities {
			return nil, fmt.Errorf("too many active data stream identities")
		}
		serial = &dataStreamSerial{}
		d.dataStreamSubscriptions[subscription] = serial
	}
	if serial != nil {
		serial.refs++
	}
	return serial, nil
}

func (d *Device) releaseDataStreamSerial(
	subscription restream.DataStreamSubscription,
	serial *dataStreamSerial,
) {
	d.dataStreamSubscriptionMutex.Lock()
	serial.refs--
	if serial.refs == 0 {
		serial.mutex.Lock()
		idle := serial.count == 0
		serial.mutex.Unlock()
		if idle && d.dataStreamSubscriptions[subscription] == serial {
			delete(d.dataStreamSubscriptions, subscription)
		}
	}
	d.dataStreamSubscriptionMutex.Unlock()
}

func (d *Device) performDataStreamTransition(
	ctx context.Context,
	subscription restream.DataStreamSubscription,
	accessLevel restream.AccessLevel,
	subscribe bool,
) error {
	d.connMutex.RLock()
	conn := d.conn
	d.connMutex.RUnlock()
	if conn == nil {
		return fmt.Errorf("device is disconnected")
	}
	if !conn.Capabilities.DataStreams {
		return fmt.Errorf("connected device does not advertise data-stream support")
	}

	operationID, result, err := d.registerDataStreamOperation(conn)
	if err != nil {
		return err
	}
	if err := conn.SendDataStreamSubscription(
		operationID,
		subscription,
		accessLevel,
		subscribe,
	); err != nil {
		d.cancelDataStreamOperation(operationID, conn)
		_ = conn.CloseWithReason(gws.CloseGoingAway, dataStreamSubscriptionForwardingFailedReason)
		return err
	}

	waitCtx := ctx
	if waitCtx == nil {
		waitCtx = context.Background()
	}
	if _, hasDeadline := waitCtx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(waitCtx, defaultDataStreamTransitionTimeout)
		defer cancel()
	}
	select {
	case err, open := <-result:
		if !open {
			return fmt.Errorf("device disconnected while changing data stream subscription")
		}
		return err
	case <-waitCtx.Done():
		d.cancelDataStreamOperation(operationID, conn)
		return fmt.Errorf("data stream subscription transition: %w", waitCtx.Err())
	}
}

func (d *Device) registerDataStreamOperation(
	conn *Connection,
) (uint32, <-chan error, error) {
	d.dataStreamOperationMutex.Lock()
	if len(d.dataStreamOperations) >= maxPendingDataStreamOperations {
		d.dataStreamOperationMutex.Unlock()
		return 0, nil, fmt.Errorf("too many pending device data stream operations")
	}
	operationID := d.nextDataStreamOperationIDLocked()
	result := make(chan error, 1)
	d.dataStreamOperations[operationID] = pendingDataStreamOperation{
		conn:   conn,
		result: result,
	}
	d.dataStreamOperationMutex.Unlock()
	return operationID, result, nil
}

func (d *Device) nextDataStreamOperationIDLocked() uint32 {
	for {
		operationID := d.dataStreamOperationNext
		d.dataStreamOperationNext++
		if d.dataStreamOperationNext == 0 {
			d.dataStreamOperationNext = 1
		}
		if operationID != 0 {
			if _, exists := d.dataStreamOperations[operationID]; !exists {
				return operationID
			}
		}
	}
}

func (d *Device) cancelDataStreamOperation(operationID uint32, conn *Connection) {
	d.dataStreamOperationMutex.Lock()
	pending, exists := d.dataStreamOperations[operationID]
	if exists && pending.conn == conn {
		delete(d.dataStreamOperations, operationID)
	}
	d.dataStreamOperationMutex.Unlock()
}

// HandleDataStreamSubscriptionResult completes one asynchronous source
// transition without coupling failures to the main relay read loop.
func (d *Device) HandleDataStreamSubscriptionResult(
	conn *Connection,
	packet *protocol.DataStreamSubscriptionResultPacket,
) {
	d.dataStreamOperationMutex.Lock()
	pending, exists := d.dataStreamOperations[packet.OperationID]
	if exists && pending.conn == conn {
		delete(d.dataStreamOperations, packet.OperationID)
	}
	d.dataStreamOperationMutex.Unlock()
	if !exists || pending.conn != conn {
		return
	}
	if packet.Error != "" {
		pending.result <- fmt.Errorf("%s", packet.Error)
	} else {
		pending.result <- nil
	}
	close(pending.result)
}

func (d *Device) sendRelayFullStates(conn *Connection) error {
	if d.StoreRegistry == nil {
		return nil
	}
	for _, storeName := range d.StoreRegistry.GetAllStoreNames() {
		allowed, err := d.StoreRegistry.StoreStreamsFromRelay(storeName)
		if err != nil {
			return err
		}
		if !allowed {
			continue
		}
		accessLevel, err := d.StoreRegistry.GetStoreMinimumAccessLevel(storeName)
		if err != nil {
			return err
		}
		stateSnapshot, err := d.StoreRegistry.GetFullStateSnapshot(storeName, accessLevel)
		if err != nil {
			return err
		}
		stateBytes, err := restream.SerializeToBytes(stateSnapshot, nil)
		if err != nil {
			return err
		}
		if err := conn.SendFullState(storeName, stateBytes); err != nil {
			return err
		}
	}
	return nil
}

func (d *Device) startRelayStateForwarding(conn *Connection) {
	if d.StoreRegistry == nil {
		return
	}

	d.stopRelayStateForwarding(nil)

	subID := d.StoreRegistry.SubscribeToPartialApplies(func(storeName string, _ [][]any, partial restream.Partial) {
		d.forwardRelayPartial(conn, storeName, partial)
	})

	d.relayForwardMutex.Lock()
	d.relayForwardSubID = subID
	d.relayForwardConn = conn
	d.relayForwardMutex.Unlock()
}

func (d *Device) stopRelayStateForwarding(conn *Connection) {
	d.relayForwardMutex.Lock()
	if conn != nil && d.relayForwardConn != conn {
		d.relayForwardMutex.Unlock()
		return
	}
	subID := d.relayForwardSubID
	shouldUnsubscribe := d.relayForwardConn != nil
	d.relayForwardSubID = 0
	d.relayForwardConn = nil
	d.relayForwardMutex.Unlock()

	if shouldUnsubscribe && d.StoreRegistry != nil {
		d.StoreRegistry.UnsubscribeFromPartialApplies(subID) //nolint:errcheck // Why: Cleanup is best effort on connection churn.
	}
}

func (d *Device) forwardRelayPartial(conn *Connection, storeName string, partial restream.Partial) {
	allowed, err := d.StoreRegistry.StoreStreamsFromRelay(storeName)
	if err != nil || !allowed {
		return
	}

	d.connMutex.RLock()
	currentConn := d.conn
	d.connMutex.RUnlock()
	if currentConn != conn {
		return
	}

	partialBytes, err := restream.SerializeToBytes(partial, nil)
	if err != nil {
		conn.CloseWithReason(gws.CloseGoingAway, relayStateForwardingFailedReason) //nolint:errcheck // Why: Closing forces reconnect and state replay.
		return
	}
	if err := conn.SendPartialState(storeName, partialBytes); err != nil {
		conn.CloseWithReason(gws.CloseGoingAway, relayStateForwardingFailedReason) //nolint:errcheck // Why: Closing forces reconnect and state replay.
	}
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
	allowed, err := d.StoreRegistry.StoreAcceptsDeviceRelayUpdates(storeName)
	if err != nil {
		return err
	}
	if !allowed {
		return nil
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
	allowed, err := d.StoreRegistry.StoreAcceptsDeviceRelayUpdates(storeName)
	if err != nil {
		return err
	}
	if !allowed {
		return nil
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

// HandleKeyedEvent handles a serialized store-owned keyed event packet from the connected device.
func (d *Device) HandleKeyedEvent(
	conn *Connection,
	storeName string,
	eventName string,
	key string,
	eventBytes []byte,
) error {
	if !d.StoreRegistry.IsStoreValid(storeName) {
		return d.handleUnknownStore(storeName)
	}
	allowed, err := d.StoreRegistry.StoreAcceptsDeviceRelayUpdates(storeName)
	if err != nil {
		return err
	}
	if !allowed {
		return nil
	}
	if d.config.KeyedEventHandler != nil {
		return d.config.KeyedEventHandler(d, conn, storeName, eventName, key, eventBytes)
	}
	return d.EventDispatcher.FireSerializedKeyedEvent(storeName, eventName, key, eventBytes)
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
		// Timed-out and disconnected RPC responses are harmlessly stale.
		return nil
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
	if len(d.rpcsPending) >= maxPendingDeviceRPCs {
		d.rpcMutex.Unlock()
		return nil, false, fmt.Errorf("too many pending device RPCs")
	}
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

	timer := time.NewTimer(defaultDeviceRPCTimeout)
	defer timer.Stop()
	var resp []byte
	select {
	case varResp, open := <-respCh:
		if !open {
			return nil, false, fmt.Errorf("device disconnected while waiting for response")
		}
		resp = varResp
	case <-timer.C:
		d.rpcMutex.Lock()
		delete(d.rpcsPending, rpcID)
		d.rpcMutex.Unlock()
		return nil, true, fmt.Errorf("timed out waiting for device RPC response")
	}

	d.rpcMutex.Lock()
	delete(d.rpcsPending, rpcID)
	d.rpcMutex.Unlock()
	return resp, true, nil
}

// FFRPCHandler handles cloud viewer FFRPCs by trying GlobalFFRPC first, then
// forwarding unhandled calls to the connected device without retaining any
// response state.
func (d *Device) FFRPCHandler(name string, accessLevel restream.AccessLevel, binaryData []byte) (bool, error) {
	return d.FFRPCHandlerWithAnnotations(nil, name, accessLevel, binaryData)
}

// FFRPCHandlerWithAnnotations handles cloud viewer FFRPCs without allowing
// trusted transport identity to be lost on either the global or device path.
func (d *Device) FFRPCHandlerWithAnnotations(
	annotations map[string]string,
	name string,
	accessLevel restream.AccessLevel,
	binaryData []byte,
) (bool, error) {
	if d.config.GlobalFFRPCWithAnnotations != nil {
		handled, err := d.config.GlobalFFRPCWithAnnotations(annotations, name, accessLevel, binaryData)
		if handled {
			return true, err
		}
	} else if d.config.GlobalFFRPC != nil {
		handled, err := d.config.GlobalFFRPC(name, accessLevel, binaryData)
		if handled {
			return true, err
		}
	}

	d.connMutex.RLock()
	conn := d.conn
	d.connMutex.RUnlock()
	if conn == nil {
		return false, fmt.Errorf("no connected device available to handle request")
	}
	var err error
	if conn.Capabilities.RPCAnnotations {
		err = conn.SendFFRPCWithAnnotations(name, accessLevel, binaryData, annotations)
	} else {
		err = conn.SendFFRPC(name, accessLevel, binaryData)
	}
	if err != nil {
		return false, fmt.Errorf("error sending FFRPC: %w", err)
	}
	return true, nil
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

func (d *Device) closePendingDataStreamOperationsForConn(conn *Connection) {
	d.dataStreamOperationMutex.Lock()
	for operationID, pending := range d.dataStreamOperations {
		if pending.conn != conn {
			continue
		}
		close(pending.result)
		delete(d.dataStreamOperations, operationID)
	}
	d.dataStreamOperationMutex.Unlock()
}

func (d *Device) handleUnknownStore(storeName string) error {
	if d.config.UnknownStorePolicy == UnknownStoreIgnore {
		return nil
	}
	return fmt.Errorf("unknown relay store %s", storeName)
}
