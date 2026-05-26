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
	sendQueue chan []byte
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

		gatheredPartials: map[string]restream.Partial{},
		gatherStart:      map[string]time.Time{},
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
		if !s.opts.StorePolicy.Allows(storeName) {
			continue
		}
		if err := s.sendFullState(storeName); err != nil {
			return err
		}
	}
	return nil
}

func (s *Streamer) partialCallback(storeName string, _ [][]any, partial restream.Partial) {
	if !s.isConnected() || !s.opts.StorePolicy.Allows(storeName) {
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
	sBytes, err := s.sr.GetSerializedFullState(storeName)
	if err != nil {
		return err
	}
	packetBytes, err := protocol.EncodePacket(protocol.NewFullStatePacket(storeName, sBytes))
	if err != nil {
		return err
	}
	return s.enqueuePacket("full state "+storeName, packetBytes)
}

func (s *Streamer) sendPartial(storeName string, partial restream.Partial) error {
	partialBytes, err := restream.SerializeToBytes(partial, nil)
	if err != nil {
		return err
	}
	packetBytes, err := protocol.EncodePacket(protocol.NewPartialStatePacket(storeName, partialBytes))
	if err != nil {
		return err
	}
	return s.enqueuePacket("partial state "+storeName, packetBytes)
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
	sendQueue := make(chan []byte, s.opts.SendQueueSize)
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

func (s *Streamer) closeCurrentConnOnSendError(err error) {
	if errors.Is(err, ErrDisconnected) {
		return
	}
	s.closeCurrentConn()
}

func (s *Streamer) enqueuePacket(packetDescription string, b []byte) error {
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
	case sendQueue <- b:
		return nil
	default:
		if s.opts.Callbacks.OnSendQueueFull != nil {
			s.opts.Callbacks.OnSendQueueFull(SendQueueFullInfo{
				PacketDescription: packetDescription,
				Bytes:             len(b),
				QueueDepth:        len(sendQueue),
				QueueCapacity:     cap(sendQueue),
			})
		}
		return ErrSendQueueFull
	}
}

func (s *Streamer) handleSendQueue(conn *gws.Conn, sendQueue <-chan []byte, sendDone <-chan struct{}) {
	go func() {
		for {
			select {
			case <-sendDone:
				return
			case b := <-sendQueue:
				select {
				case <-sendDone:
					return
				default:
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
