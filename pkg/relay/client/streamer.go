package client

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

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

	partialSubID subscribableevent.SubscriptionId
	eventSubID   subscribableevent.SubscriptionId

	relaySubscriptionMutex sync.Mutex
	relaySubscriptions     map[relaySubscriptionKey]struct{}
}

type relaySubscriptionKey struct {
	storeName string
	key       string
}

type outboundPacket struct {
	description string
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

		gatheredPartials:   map[string]restream.Partial{},
		gatherStart:        map[string]time.Time{},
		relaySubscriptions: map[relaySubscriptionKey]struct{}{},
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
	defer s.clearRelaySubscriptions()

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
			return err
		}

		switch packet := packet.(type) {
		case *protocol.ConnectedPacket:
			if packet.ProtocolVersion != protocol.CurrentVersion {
				return fmt.Errorf("unsupported relay protocol version: %d", packet.ProtocolVersion)
			}
			s.startConn(conn)
			s.onConnected(packet)
			if err := s.sendFullStates(); err != nil {
				return err
			}
		case *protocol.RPCCallPacket:
			if err := s.handleRPCCall(packet); err != nil {
				return err
			}
		case *protocol.StoreSubscriptionPacket:
			if err := s.handleStoreSubscription(packet); err != nil {
				return err
			}
		case *protocol.CustomPacket:
			if s.opts.Callbacks.OnCustomPacket != nil {
				if err := s.opts.Callbacks.OnCustomPacket(packet); err != nil {
					return err
				}
			}
		case *protocol.RawPacket:
			if s.opts.Callbacks.OnRawPacket != nil {
				if err := s.opts.Callbacks.OnRawPacket(packet); err != nil {
					return err
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

	if debounce, ok := s.opts.StorePolicy.DebounceFor(storeName); ok {
		s.gatherPartial(storeName, partial, debounce)
		return
	}

	if err := s.sendPartial(storeName, partial); err != nil {
		s.closeCurrentConnOnSendError(err)
	}
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

func (s *Streamer) gatherPartial(storeName string, partial restream.Partial, debounce time.Duration) {
	s.gatherMutex.Lock()
	if gathered, exists := s.gatheredPartials[storeName]; exists {
		partial.MergeOntoPartial(gathered)
	} else {
		s.gatheredPartials[storeName] = partial
		s.gatherStart[storeName] = time.Now()
	}
	s.gatherMutex.Unlock()

	s.recalcGatherTimeout(debounce)
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
	for storeName, gatherStart := range s.gatherStart {
		debounce, ok := s.opts.StorePolicy.DebounceFor(storeName)
		if !ok {
			toSend[storeName] = s.gatheredPartials[storeName]
			continue
		}
		if time.Since(gatherStart) >= debounce {
			toSend[storeName] = s.gatheredPartials[storeName]
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
		if err := s.sendPartial(storeName, partial); err != nil {
			s.closeCurrentConnOnSendError(err)
		}
	}
}

func (s *Streamer) sendFullState(storeName string) error {
	accessLevel, err := s.sr.GetStoreMinimumAccessLevel(storeName)
	if err != nil {
		return err
	}
	stateSnapshot, err := s.sr.GetFullStateSnapshot(storeName, accessLevel)
	if err != nil {
		return err
	}
	return s.enqueuePacketBuilder("full state "+storeName, func() ([]byte, error) {
		stateBytes, err := restream.SerializeToBytes(stateSnapshot, nil)
		if err != nil {
			return nil, err
		}
		return protocol.EncodePacket(protocol.NewFullStatePacket(storeName, stateBytes))
	})
}

func (s *Streamer) sendPartial(storeName string, partial restream.Partial) error {
	return s.enqueuePacketBuilder("partial state "+storeName, func() ([]byte, error) {
		partialBytes, err := restream.SerializeToBytes(partial, nil)
		if err != nil {
			return nil, err
		}
		return protocol.EncodePacket(protocol.NewPartialStatePacket(storeName, partialBytes))
	})
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

func (s *Streamer) startRelayedStoreSubscription(storeName string, key string) error {
	subKey := relaySubscriptionKey{storeName: storeName, key: key}

	s.relaySubscriptionMutex.Lock()
	if s.relaySubscriptions == nil {
		s.relaySubscriptions = map[relaySubscriptionKey]struct{}{}
	}
	if _, exists := s.relaySubscriptions[subKey]; exists {
		s.relaySubscriptionMutex.Unlock()
		return nil
	}
	s.relaySubscriptions[subKey] = struct{}{}
	s.relaySubscriptionMutex.Unlock()

	accessLevel, err := s.sr.GetStoreMinimumAccessLevel(storeName)
	if err != nil {
		s.relaySubscriptionMutex.Lock()
		delete(s.relaySubscriptions, subKey)
		s.relaySubscriptionMutex.Unlock()
		return err
	}

	if err := s.sr.ListeningToStoreKey(storeName, key, accessLevel); err != nil {
		s.relaySubscriptionMutex.Lock()
		delete(s.relaySubscriptions, subKey)
		s.relaySubscriptionMutex.Unlock()
		return err
	}
	return nil
}

func (s *Streamer) stopRelayedStoreSubscription(storeName string, key string) error {
	subKey := relaySubscriptionKey{storeName: storeName, key: key}

	s.relaySubscriptionMutex.Lock()
	if _, exists := s.relaySubscriptions[subKey]; !exists {
		s.relaySubscriptionMutex.Unlock()
		return nil
	}
	delete(s.relaySubscriptions, subKey)
	s.relaySubscriptionMutex.Unlock()

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
	s.gatherMutex.Unlock()
}

func (s *Streamer) clearRelaySubscriptions() {
	if s.sr == nil {
		return
	}

	s.relaySubscriptionMutex.Lock()
	keys := make([]relaySubscriptionKey, 0, len(s.relaySubscriptions))
	for key := range s.relaySubscriptions {
		keys = append(keys, key)
	}
	s.relaySubscriptions = map[relaySubscriptionKey]struct{}{}
	s.relaySubscriptionMutex.Unlock()

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

func (s *Streamer) enqueuePacketBuilder(packetDescription string, build func() ([]byte, error)) error {
	return s.enqueueOutboundPacket(outboundPacket{
		description: packetDescription,
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
			}
		}
	}()
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
