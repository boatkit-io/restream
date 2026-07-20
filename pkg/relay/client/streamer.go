package client

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/boatkit-io/restream/pkg/binarystreams"
	"github.com/boatkit-io/restream/pkg/relay/protocol"
	"github.com/boatkit-io/restream/pkg/restream"
	"github.com/boatkit-io/tugboat/pkg/subscribableevent"
	gws "github.com/gorilla/websocket"
)

var (
	// ErrDisconnected is returned when a packet cannot be queued because the streamer is disconnected.
	ErrDisconnected = errors.New("relay streamer disconnected")
	// ErrSendQueueFull is returned when the bounded outbound send queue is full.
	ErrSendQueueFull = errors.New("relay streamer send queue full")
)

// Streamer streams local Restream state to a relay server.
type Streamer struct {
	sr  *restream.StoreRegistry
	rpc restream.RPCHandlerFunc
	ed  *restream.EventDispatcher

	opts Config

	connMutex sync.RWMutex
	sendQueue chan outboundPacket
	sendDone  chan struct{}
	conn      *gws.Conn
	shutdown  atomic.Bool

	gatherMutex      sync.Mutex
	gatherTimeout    *time.Time
	gatherCancel     context.CancelFunc
	gatheredPartials map[string]restream.Partial
	gatherStart      map[string]time.Time
	gatherGeneration map[string]uint64

	partialSubID subscribableevent.SubscriptionId
	eventSubID   subscribableevent.SubscriptionId

	relaySubscriptionMutex sync.Mutex
	relaySubscriptions     map[relaySubscriptionKey]struct{}
	relayStoreSubCount     map[string]int
	relayStoreGeneration   map[string]uint64
	relayStoreCatchingUp   map[string]bool
	relayCatchupPartials   map[string]restream.Partial
	onDemandStoreStreaming bool
}

type relaySubscriptionKey struct {
	storeName string
	key       string
}

type outboundPacket struct {
	description string
	storeName   string
	packetKind  protocol.PacketKind
	generation  uint64
	bytes       []byte
	build       func() ([]byte, error)
}

func (p outboundPacket) buildBytes() ([]byte, error) {
	if p.build != nil {
		return p.build()
	}
	return p.bytes, nil
}

func (p outboundPacket) byteCount() int {
	return len(p.bytes)
}

// NewStreamer creates a device-side relay streamer.
func NewStreamer(
	sr *restream.StoreRegistry,
	rpc restream.RPCHandlerFunc,
	ed *restream.EventDispatcher,
	config Config,
) *Streamer {
	opts := applyDefaults(config)

	s := &Streamer{
		sr:  sr,
		rpc: rpc,
		ed:  ed,

		opts: opts,

		gatheredPartials:     map[string]restream.Partial{},
		gatherStart:          map[string]time.Time{},
		gatherGeneration:     map[string]uint64{},
		relaySubscriptions:   map[relaySubscriptionKey]struct{}{},
		relayStoreSubCount:   map[string]int{},
		relayStoreGeneration: map[string]uint64{},
		relayStoreCatchingUp: map[string]bool{},
		relayCatchupPartials: map[string]restream.Partial{},
	}

	if sr != nil {
		s.partialSubID = sr.SubscribeToPartialApplies(s.partialCallback)
	}
	if ed != nil {
		s.eventSubID = ed.SubscribeToEvents(func(eventName string, eventBytes []byte) {
			if err := s.SendEvent(eventName, eventBytes); err != nil {
				s.closeCurrentConnOnSendError(err)
			}
		})
	}

	return s
}

// Run connects to the relay server and reconnects until ctx is cancelled or Shutdown is called.
func (s *Streamer) Run(ctx context.Context) error {
	if s.opts.Endpoint == "" {
		return fmt.Errorf("relay streamer endpoint is not configured")
	}

	for !s.shutdown.Load() {
		conn, resp, err := s.opts.Dialer.Dial(s.opts.Endpoint, nil)
		closeResponseBody(resp)
		if err != nil {
			s.onDialError(err)
			if !sleepOrDone(ctx, s.opts.ReconnectDelay) {
				return nil
			}
			continue
		}
		conn.EnableWriteCompression(true)

		err = s.handleConn(ctx, conn, s.opts.Credentials)
		s.closeConn(conn)
		s.clearGatheredPartials()
		if s.shutdown.Load() || ctx.Err() != nil {
			s.onDisconnected(nil)
			return nil
		}
		s.onDisconnected(err)

		if !sleepOrDone(ctx, s.opts.ReconnectDelay) {
			return nil
		}
	}

	return nil
}

// Shutdown stops the streamer and closes the current relay connection.
func (s *Streamer) Shutdown() {
	s.shutdown.Store(true)
	s.closeCurrentConn()
}

// Close unsubscribes the streamer from registry/event callbacks and shuts it down.
func (s *Streamer) Close() error {
	s.Shutdown()

	var retErr error
	if s.sr != nil {
		if err := s.sr.UnsubscribeFromPartialApplies(s.partialSubID); err != nil {
			retErr = err
		}
	}
	if s.ed != nil {
		if err := s.ed.UnsubscribeFromEvents(s.eventSubID); err != nil && retErr == nil {
			retErr = err
		}
	}
	return retErr
}

// IsShutdown reports whether Shutdown has been called.
func (s *Streamer) IsShutdown() bool {
	return s.shutdown.Load()
}

// SendEvent sends a serialized Restream event to the relay server.
func (s *Streamer) SendEvent(eventName string, eventBytes []byte) error {
	packetBytes, err := protocol.EncodePacket(&protocol.EventPacket{
		EventName: eventName,
		Data:      eventBytes,
	})
	if err != nil {
		return err
	}

	return s.enqueuePacket("event "+eventName, packetBytes)
}

func (s *Streamer) handleConn(ctx context.Context, conn *gws.Conn, credentials Credentials) error {
	defer func() {
		// Make local subscription callbacks observe a disconnected streamer while relay keys are unwound.
		s.closeConn(conn)
		s.clearRelaySubscriptions()
	}()

	closeOnContextDone := make(chan struct{})
	defer close(closeOnContextDone)
	go func() {
		select {
		case <-ctx.Done():
			conn.Close() //nolint:errcheck // Why: Closing best effort to unblock ReadMessage.
		case <-closeOnContextDone:
		}
	}()

	helloBytes, err := protocol.EncodeDeviceHello(&protocol.DeviceHello{
		ProtocolVersion: protocol.CurrentVersion,
		DeviceID:        credentials.DeviceID,
		AuthType:        credentials.AuthType,
		AuthData:        credentials.AuthData,
		Metadata:        credentials.Metadata,
	})
	if err != nil {
		return err
	}
	if err := conn.WriteMessage(gws.BinaryMessage, helloBytes); err != nil {
		return err
	}

	for {
		mt, message, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		if mt != gws.BinaryMessage {
			return fmt.Errorf("expected binary relay packet, got message type %d", mt)
		}

		packet, err := protocol.DecodePacket(message)
		if err != nil {
			return fmt.Errorf("decode relay packet from server (%d bytes): %w", len(message), err)
		}

		switch packet := packet.(type) {
		case *protocol.ConnectedPacket:
			if packet.ProtocolVersion != protocol.CurrentVersion {
				return fmt.Errorf("unsupported relay protocol version: %d", packet.ProtocolVersion)
			}
			onDemand := s.configureOnDemandStoreStreaming(packet)
			s.startConn(conn)
			s.onConnected(packet)
			if !onDemand {
				if err := s.sendFullStates(); err != nil {
					return fmt.Errorf("handle relay connected packet: send full states: %w", err)
				}
			}
		case *protocol.RPCCallPacket:
			if err := s.handleRPCCall(packet); err != nil {
				return fmt.Errorf("handle relay RPC call packet %q (%d bytes): %w",
					packet.MethodName, len(packet.Request), err)
			}
		case *protocol.StoreSubscriptionPacket:
			if err := s.handleStoreSubscription(packet); err != nil {
				return fmt.Errorf("handle relay store subscription packet action %d for store %q key %q: %w",
					packet.Action, packet.StoreName, packet.Key, err)
			}
		case *protocol.StoreStatePacket:
			if err := s.handleStoreState(packet); err != nil {
				return fmt.Errorf("handle relay store state packet for store %q (%d bytes): %w",
					packet.StoreName, len(packet.Data), err)
			}
		case *protocol.CustomPacket:
			if s.opts.Callbacks.OnCustomPacket != nil {
				if err := s.opts.Callbacks.OnCustomPacket(packet); err != nil {
					return fmt.Errorf("handle relay custom packet %q (%d bytes): %w",
						packet.Name, len(packet.Payload), err)
				}
			}
		case *protocol.RawPacket:
			if s.opts.Callbacks.OnRawPacket != nil {
				if err := s.opts.Callbacks.OnRawPacket(packet); err != nil {
					return fmt.Errorf("handle relay raw packet kind %d (%d bytes): %w",
						packet.PacketKind, len(packet.Payload), err)
				}
			}
		default:
			return fmt.Errorf("unhandled relay packet type %T", packet)
		}
	}
}

func (s *Streamer) sendFullStates() error {
	if s.sr == nil {
		return nil
	}
	for _, storeName := range s.sr.GetAllStoreNames() {
		allowed, err := s.allowsRelayStoreTraffic(storeName)
		if err != nil {
			return err
		}
		if !allowed {
			continue
		}
		if err := s.sendFullState(storeName); err != nil {
			return err
		}
	}
	return nil
}

func (s *Streamer) partialCallback(storeName string, _ [][]any, partial restream.Partial) {
	if !s.isConnected() {
		return
	}
	allowed, err := s.allowsRelayStoreTraffic(storeName)
	if err != nil || !allowed {
		return
	}

	generation := uint64(0)
	s.relaySubscriptionMutex.Lock()
	if s.onDemandStoreStreaming {
		if s.relayStoreSubCount[storeName] == 0 {
			s.relaySubscriptionMutex.Unlock()
			return
		}
		generation = s.relayStoreGeneration[storeName]
		if s.relayStoreCatchingUp[storeName] {
			partialSnapshot, snapshotErr := snapshotPartial(partial)
			if snapshotErr != nil {
				s.relaySubscriptionMutex.Unlock()
				s.closeCurrentConnOnSendError(snapshotErr)
				return
			}
			if gathered, exists := s.relayCatchupPartials[storeName]; exists {
				partialSnapshot.MergeOntoPartial(gathered)
			} else {
				s.relayCatchupPartials[storeName] = partialSnapshot
			}
			s.relaySubscriptionMutex.Unlock()
			return
		}
	}

	if debounce, ok := s.opts.StorePolicy.DebounceFor(storeName); ok {
		if err := s.gatherPartial(storeName, partial, debounce, generation); err != nil {
			s.closeCurrentConnOnSendError(err)
		}
		s.relaySubscriptionMutex.Unlock()
		return
	}

	if err := s.sendPartialForGeneration(storeName, partial, generation); err != nil {
		s.closeCurrentConnOnSendError(err)
	}
	s.relaySubscriptionMutex.Unlock()
}

func (s *Streamer) allowsRelayStoreTraffic(storeName string) (bool, error) {
	if !s.opts.StorePolicy.Allows(storeName) {
		return false, nil
	}
	if s.sr == nil {
		return false, nil
	}
	return s.sr.StoreStreamsToRelay(storeName)
}

func (s *Streamer) allowsRelayStoreInbound(storeName string) (bool, error) {
	if !s.opts.StorePolicy.Allows(storeName) {
		return false, nil
	}
	if s.sr == nil {
		return false, nil
	}
	return s.sr.StoreReceivesFromRelay(storeName)
}

func (s *Streamer) handleStoreState(packet *protocol.StoreStatePacket) error {
	allowed, err := s.allowsRelayStoreInbound(packet.StoreName)
	if err != nil || !allowed {
		return err
	}

	switch packet.Kind() {
	case protocol.KindFullState:
		return s.sr.SetFullStateToStore(packet.StoreName, packet.Data)
	case protocol.KindPartialState:
		return s.sr.ApplyPartialToStore(packet.StoreName, packet.Data)
	default:
		return fmt.Errorf("unhandled store packet type %d", packet.Kind())
	}
}

func (s *Streamer) gatherPartial(
	storeName string,
	partial restream.Partial,
	debounce time.Duration,
	generation uint64,
) error {
	partialSnapshot, err := snapshotPartial(partial)
	if err != nil {
		return err
	}

	s.gatherMutex.Lock()
	if gathered, exists := s.gatheredPartials[storeName]; exists && s.gatherGeneration[storeName] == generation {
		partialSnapshot.MergeOntoPartial(gathered)
	} else {
		s.gatheredPartials[storeName] = partialSnapshot
		s.gatherStart[storeName] = time.Now()
		s.gatherGeneration[storeName] = generation
	}
	s.gatherMutex.Unlock()

	s.recalcGatherTimeout(debounce)
	return nil
}

func (s *Streamer) recalcGatherTimeout(_ time.Duration) {
	if !s.isConnected() {
		return
	}

	s.gatherMutex.Lock()
	if len(s.gatheredPartials) == 0 {
		s.gatherMutex.Unlock()
		return
	}

	var nextExp *time.Time
	toSend := map[string]restream.Partial{}
	toSendGeneration := map[string]uint64{}
	for storeName, gatherStart := range s.gatherStart {
		debounce, ok := s.opts.StorePolicy.DebounceFor(storeName)
		if !ok {
			toSend[storeName] = s.gatheredPartials[storeName]
			toSendGeneration[storeName] = s.gatherGeneration[storeName]
			continue
		}
		if time.Since(gatherStart) >= debounce {
			toSend[storeName] = s.gatheredPartials[storeName]
			toSendGeneration[storeName] = s.gatherGeneration[storeName]
		} else {
			exp := gatherStart.Add(debounce)
			if nextExp == nil || exp.Before(*nextExp) {
				nextExp = &exp
			}
		}
	}

	for storeName := range toSend {
		delete(s.gatheredPartials, storeName)
		delete(s.gatherStart, storeName)
		delete(s.gatherGeneration, storeName)
	}

	if nextExp != nil {
		if s.gatherTimeout == nil || nextExp.Before(*s.gatherTimeout) {
			if s.gatherCancel != nil {
				s.gatherCancel()
			}
			ctx, cancel := context.WithCancel(context.Background())
			s.gatherTimeout = nextExp
			s.gatherCancel = cancel
			go func(exp time.Time) {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Until(exp)):
					s.gatherMutex.Lock()
					s.gatherTimeout = nil
					s.gatherCancel = nil
					s.gatherMutex.Unlock()

					s.recalcGatherTimeout(0)
				}
			}(*nextExp)
		}
	} else if s.gatherCancel != nil {
		s.gatherCancel()
		s.gatherTimeout = nil
		s.gatherCancel = nil
	}
	s.gatherMutex.Unlock()

	for storeName, partial := range toSend {
		if err := s.sendPartialForGeneration(storeName, partial, toSendGeneration[storeName]); err != nil {
			s.closeCurrentConnOnSendError(err)
		}
	}
}

func (s *Streamer) sendFullState(storeName string) error {
	return s.sendFullStateForGeneration(storeName, 0)
}

func (s *Streamer) sendFullStateForGeneration(storeName string, generation uint64) error {
	accessLevel, err := s.sr.GetStoreMinimumAccessLevel(storeName)
	if err != nil {
		return err
	}
	stateSnapshot, err := s.sr.GetFullStateSnapshot(storeName, accessLevel)
	if err != nil {
		return err
	}
	return s.enqueueStorePacketBuilder(
		"full state "+storeName,
		storeName,
		protocol.KindFullState,
		generation,
		func() ([]byte, error) {
			stateBytes, err := restream.SerializeToBytes(stateSnapshot, nil)
			if err != nil {
				return nil, err
			}
			return protocol.EncodePacket(protocol.NewFullStatePacket(storeName, stateBytes))
		},
	)
}

func (s *Streamer) sendPartial(storeName string, partial restream.Partial) error {
	return s.sendPartialForGeneration(storeName, partial, 0)
}

func (s *Streamer) sendPartialForGeneration(
	storeName string,
	partial restream.Partial,
	generation uint64,
) error {
	partialBytes, err := restream.SerializeToBytes(partial, nil)
	if err != nil {
		return err
	}
	packetBytes, err := protocol.EncodePacket(protocol.NewPartialStatePacket(storeName, partialBytes))
	if err != nil {
		return err
	}
	return s.enqueueOutboundPacket(outboundPacket{
		description: "partial state " + storeName,
		storeName:   storeName,
		packetKind:  protocol.KindPartialState,
		generation:  generation,
		bytes:       packetBytes,
	})
}

// snapshotPartial detaches a retained partial from values that ApplyTo may have installed directly into live store state.
func snapshotPartial(partial restream.Partial) (restream.Partial, error) {
	partialType := reflect.TypeOf(partial)
	if partialType == nil || partialType.Kind() != reflect.Pointer {
		return nil, fmt.Errorf("relay partial type %T is not a pointer", partial)
	}
	partialValue := reflect.ValueOf(partial)
	if partialValue.IsNil() {
		return nil, fmt.Errorf("relay partial type %T is nil", partial)
	}

	partialBytes, err := restream.SerializeToBytes(partial, nil)
	if err != nil {
		return nil, err
	}
	snapshot, ok := reflect.New(partialType.Elem()).Interface().(restream.Partial)
	if !ok {
		return nil, fmt.Errorf("relay partial snapshot type %s does not implement restream.Partial", partialType)
	}
	if err := snapshot.Deserialize(binarystreams.NewReaderFromBytes(partialBytes), nil); err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (s *Streamer) sendRPCResponse(rpcID uint32, resp []byte) error {
	packetBytes, err := protocol.EncodePacket(&protocol.RPCResponsePacket{
		RPCID:    rpcID,
		Response: resp,
	})
	if err != nil {
		return err
	}
	return s.enqueuePacket("rpc response", packetBytes)
}

func (s *Streamer) handleRPCCall(packet *protocol.RPCCallPacket) error {
	if s.rpc == nil {
		return fmt.Errorf("relay RPC received but no RPC handler is configured")
	}
	resp, handled, err := s.rpc(packet.MethodName, restream.AccessLevel(packet.AccessLevel), packet.Request)
	if err != nil {
		return err
	}
	if !handled {
		return fmt.Errorf("unhandled RPC %s", packet.MethodName)
	}
	return s.sendRPCResponse(packet.RPCID, resp)
}

func (s *Streamer) handleStoreSubscription(packet *protocol.StoreSubscriptionPacket) error {
	allowed, err := s.allowsRelayStoreTraffic(packet.StoreName)
	if err != nil {
		return err
	}
	if !allowed {
		return nil
	}

	switch packet.Action {
	case protocol.StoreSubscribe:
		return s.startRelayedStoreSubscription(packet.StoreName, packet.Key)
	case protocol.StoreUnsubscribe:
		return s.stopRelayedStoreSubscription(packet.StoreName, packet.Key)
	default:
		return fmt.Errorf("invalid store subscription action %d", packet.Action)
	}
}

func (s *Streamer) configureOnDemandStoreStreaming(packet *protocol.ConnectedPacket) bool {
	enabled := s.opts.StorePolicy.OnDemand &&
		packet.Capabilities.OnDemandStoreStreaming
	s.relaySubscriptionMutex.Lock()
	s.onDemandStoreStreaming = enabled
	s.relaySubscriptionMutex.Unlock()
	return enabled
}

func (s *Streamer) startRelayedStoreSubscription(storeName string, key string) error {
	subKey := relaySubscriptionKey{storeName: storeName, key: key}
	accessLevel, err := s.sr.GetStoreMinimumAccessLevel(storeName)
	if err != nil {
		return err
	}

	s.relaySubscriptionMutex.Lock()
	if s.relaySubscriptions == nil {
		s.relaySubscriptions = map[relaySubscriptionKey]struct{}{}
	}
	if _, exists := s.relaySubscriptions[subKey]; exists {
		s.relaySubscriptionMutex.Unlock()
		return nil
	}
	s.relaySubscriptions[subKey] = struct{}{}
	s.relayStoreSubCount[storeName]++
	firstStoreSubscription := s.relayStoreSubCount[storeName] == 1
	onDemand := s.onDemandStoreStreaming
	generation := s.relayStoreGeneration[storeName]
	if onDemand && firstStoreSubscription {
		generation++
		s.relayStoreGeneration[storeName] = generation
		s.relayStoreCatchingUp[storeName] = true
		delete(s.relayCatchupPartials, storeName)
	}
	s.relaySubscriptionMutex.Unlock()

	if err := s.sr.ListeningToStoreKey(storeName, key, accessLevel); err != nil {
		s.rollbackRelayedStoreSubscription(subKey)
		return err
	}
	if !onDemand || !firstStoreSubscription {
		return nil
	}

	if err := s.sendFullStateForGeneration(storeName, generation); err != nil {
		s.rollbackRelayedStoreSubscription(subKey)
		s.sr.StopListeningToStoreKey(storeName, key) //nolint:errcheck // Why: Preserve the original send error.
		return err
	}

	s.relaySubscriptionMutex.Lock()
	if s.relayStoreSubCount[storeName] > 0 &&
		s.relayStoreGeneration[storeName] == generation &&
		s.relayStoreCatchingUp[storeName] {
		if partial := s.relayCatchupPartials[storeName]; partial != nil {
			if err := s.sendPartialForGeneration(storeName, partial, generation); err != nil {
				s.relaySubscriptionMutex.Unlock()
				return err
			}
		}
		delete(s.relayCatchupPartials, storeName)
		delete(s.relayStoreCatchingUp, storeName)
	}
	s.relaySubscriptionMutex.Unlock()
	return nil
}

func (s *Streamer) rollbackRelayedStoreSubscription(subKey relaySubscriptionKey) {
	s.relaySubscriptionMutex.Lock()
	if _, exists := s.relaySubscriptions[subKey]; exists {
		delete(s.relaySubscriptions, subKey)
		s.relayStoreSubCount[subKey.storeName]--
		if s.relayStoreSubCount[subKey.storeName] <= 0 {
			delete(s.relayStoreSubCount, subKey.storeName)
			delete(s.relayStoreCatchingUp, subKey.storeName)
			delete(s.relayCatchupPartials, subKey.storeName)
		}
	}
	s.relaySubscriptionMutex.Unlock()
}

func (s *Streamer) stopRelayedStoreSubscription(storeName string, key string) error {
	subKey := relaySubscriptionKey{storeName: storeName, key: key}

	s.relaySubscriptionMutex.Lock()
	if _, exists := s.relaySubscriptions[subKey]; !exists {
		s.relaySubscriptionMutex.Unlock()
		return nil
	}
	delete(s.relaySubscriptions, subKey)
	s.relayStoreSubCount[storeName]--
	lastStoreSubscription := s.relayStoreSubCount[storeName] <= 0
	if lastStoreSubscription {
		delete(s.relayStoreSubCount, storeName)
		delete(s.relayStoreCatchingUp, storeName)
		delete(s.relayCatchupPartials, storeName)
	}
	s.relaySubscriptionMutex.Unlock()
	if lastStoreSubscription {
		s.discardGatheredPartial(storeName)
	}

	return s.sr.StopListeningToStoreKey(storeName, key)
}

func (s *Streamer) isConnected() bool {
	s.connMutex.RLock()
	defer s.connMutex.RUnlock()

	if s.conn == nil || s.sendQueue == nil || s.sendDone == nil {
		return false
	}

	select {
	case <-s.sendDone:
		return false
	default:
		return true
	}
}

func (s *Streamer) startConn(conn *gws.Conn) {
	sendQueue := make(chan outboundPacket, s.opts.SendQueueSize)
	sendDone := make(chan struct{})

	s.closeCurrentConn()

	s.connMutex.Lock()
	s.conn = conn
	s.sendQueue = sendQueue
	s.sendDone = sendDone
	s.connMutex.Unlock()

	s.handleSendQueue(conn, sendQueue, sendDone)
}

func (s *Streamer) closeCurrentConn() {
	s.connMutex.RLock()
	conn := s.conn
	s.connMutex.RUnlock()

	if conn != nil {
		s.closeConn(conn)
	}
}

func (s *Streamer) closeConn(conn *gws.Conn) {
	if conn == nil {
		return
	}

	s.connMutex.Lock()
	if s.conn == conn {
		if s.sendDone != nil {
			close(s.sendDone)
		}
		s.conn = nil
		s.sendQueue = nil
		s.sendDone = nil
	}
	s.connMutex.Unlock()

	conn.Close() //nolint:errcheck // Why: Closing best effort.
}

func (s *Streamer) clearGatheredPartials() {
	s.gatherMutex.Lock()
	if s.gatherCancel != nil {
		s.gatherCancel()
	}
	s.gatherTimeout = nil
	s.gatherCancel = nil
	s.gatherStart = map[string]time.Time{}
	s.gatheredPartials = map[string]restream.Partial{}
	s.gatherGeneration = map[string]uint64{}
	s.gatherMutex.Unlock()
}

func (s *Streamer) discardGatheredPartial(storeName string) {
	s.gatherMutex.Lock()
	delete(s.gatherStart, storeName)
	delete(s.gatheredPartials, storeName)
	delete(s.gatherGeneration, storeName)
	if len(s.gatheredPartials) == 0 && s.gatherCancel != nil {
		s.gatherCancel()
		s.gatherTimeout = nil
		s.gatherCancel = nil
	}
	s.gatherMutex.Unlock()
}

func (s *Streamer) clearRelaySubscriptions() {
	s.relaySubscriptionMutex.Lock()
	keys := make([]relaySubscriptionKey, 0, len(s.relaySubscriptions))
	for key := range s.relaySubscriptions {
		keys = append(keys, key)
	}
	s.relaySubscriptions = map[relaySubscriptionKey]struct{}{}
	s.relayStoreSubCount = map[string]int{}
	s.relayStoreCatchingUp = map[string]bool{}
	s.relayCatchupPartials = map[string]restream.Partial{}
	s.onDemandStoreStreaming = false
	s.relaySubscriptionMutex.Unlock()

	if s.sr == nil {
		return
	}
	for _, key := range keys {
		s.sr.StopListeningToStoreKey(key.storeName, key.key) //nolint:errcheck // Why: Cleanup is best effort on relay disconnect.
	}
}

func (s *Streamer) closeCurrentConnOnSendError(err error) {
	if errors.Is(err, ErrDisconnected) {
		return
	}
	s.closeCurrentConn()
}

func (s *Streamer) enqueuePacket(packetDescription string, b []byte) error {
	return s.enqueueOutboundPacket(outboundPacket{
		description: packetDescription,
		bytes:       b,
	})
}

func (s *Streamer) enqueueStorePacketBuilder(
	packetDescription string,
	storeName string,
	packetKind protocol.PacketKind,
	generation uint64,
	build func() ([]byte, error),
) error {
	return s.enqueueOutboundPacket(outboundPacket{
		description: packetDescription,
		storeName:   storeName,
		packetKind:  packetKind,
		generation:  generation,
		build:       build,
	})
}

func (s *Streamer) enqueueOutboundPacket(packet outboundPacket) error {
	s.connMutex.RLock()
	sendQueue := s.sendQueue
	sendDone := s.sendDone
	s.connMutex.RUnlock()

	if sendQueue == nil || sendDone == nil {
		return ErrDisconnected
	}

	select {
	case <-sendDone:
		return ErrDisconnected
	case sendQueue <- packet:
		return nil
	default:
		if s.opts.Callbacks.OnSendQueueFull != nil {
			s.opts.Callbacks.OnSendQueueFull(SendQueueFullInfo{
				PacketDescription: packet.description,
				Bytes:             packet.byteCount(),
				QueueDepth:        len(sendQueue),
				QueueCapacity:     cap(sendQueue),
			})
		}
		return ErrSendQueueFull
	}
}

func (s *Streamer) handleSendQueue(conn *gws.Conn, sendQueue <-chan outboundPacket, sendDone <-chan struct{}) {
	go func() {
		for {
			select {
			case <-sendDone:
				return
			case packet, ok := <-sendQueue:
				if !ok {
					return
				}
				select {
				case <-sendDone:
					return
				default:
				}
				if !s.storePacketStillActive(packet) {
					continue
				}

				b, err := packet.buildBytes()
				if err != nil {
					s.closeConn(conn)
					return
				}

				deadline := time.Now().Add(s.opts.WriteTimeout)
				if err := conn.SetWriteDeadline(deadline); err != nil {
					s.closeConn(conn)
					return
				}
				if err := conn.WriteMessage(gws.BinaryMessage, b); err != nil {
					s.closeConn(conn)
					return
				}
				if s.opts.Callbacks.OnBytesSent != nil {
					s.opts.Callbacks.OnBytesSent(len(b))
				}
				if packet.storeName != "" && s.opts.Callbacks.OnStorePacketSent != nil {
					s.opts.Callbacks.OnStorePacketSent(StorePacketSentInfo{
						StoreName:  packet.storeName,
						PacketKind: packet.packetKind,
						Bytes:      len(b),
					})
				}
			}
		}
	}()
}

func (s *Streamer) storePacketStillActive(packet outboundPacket) bool {
	if packet.storeName == "" || packet.generation == 0 {
		return true
	}
	s.relaySubscriptionMutex.Lock()
	defer s.relaySubscriptionMutex.Unlock()
	return s.onDemandStoreStreaming &&
		s.relayStoreSubCount[packet.storeName] > 0 &&
		s.relayStoreGeneration[packet.storeName] == packet.generation
}

func (s *Streamer) onConnected(packet *protocol.ConnectedPacket) {
	if s.opts.Callbacks.OnConnected != nil {
		s.opts.Callbacks.OnConnected(packet)
	}
}

func (s *Streamer) onDisconnected(err error) {
	if s.opts.Callbacks.OnDisconnected != nil {
		s.opts.Callbacks.OnDisconnected(err)
	}
}

func (s *Streamer) onDialError(err error) {
	if s.opts.Callbacks.OnDialError != nil {
		s.opts.Callbacks.OnDialError(err)
	}
}

func closeResponseBody(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		resp.Body.Close() //nolint:errcheck // Why: Response body is only for handshake diagnostics.
	}
}

func sleepOrDone(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
