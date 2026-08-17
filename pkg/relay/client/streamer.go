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
	// ErrRelayRPCUnsupported is returned when the connected relay does not advertise device-originated RPCs.
	ErrRelayRPCUnsupported = errors.New("connected relay does not support relay RPCs")
)

const (
	maxDataStreamWorkers           = 1024
	maxPendingDataStreamOperations = 4096
	maxRelayedStoreSubscriptions   = 4096
	maxRelayedKeyedSubscriptions   = 4096
	maxInFlightRelayFFRPCs         = 256
	maxPendingRelayRPCs            = 4096
	dataStreamOperationQueueSize   = 64
	maxDataStreamResultErrorBytes  = 4096
	dataStreamTransitionTimeout    = 30 * time.Second
)

// Streamer streams local Restream state to a relay server.
type Streamer struct {
	sr                   *restream.StoreRegistry
	rpc                  restream.RPCHandlerFunc
	rpcWithAnnotations   restream.RPCHandlerWithAnnotationsFunc
	ffrpc                restream.FFRPCHandlerFunc
	ffrpcWithAnnotations restream.FFRPCHandlerWithAnnotationsFunc
	ed                   *restream.EventDispatcher

	opts Config

	connMutex       sync.RWMutex
	sendQueue       chan outboundPacket
	sendDone        chan struct{}
	conn            *gws.Conn
	shutdown        atomic.Bool
	relayRPCs       bool
	relayRPCNextID  uint32
	relayRPCPending map[uint32]pendingRelayRPC

	gatherMutex      sync.Mutex
	gatherTimeout    *time.Time
	gatherCancel     context.CancelFunc
	gatheredPartials map[string]restream.Partial
	gatherStart      map[string]time.Time
	gatherGeneration map[string]uint64

	partialSubID    subscribableevent.SubscriptionId
	eventSubID      subscribableevent.SubscriptionId
	keyedEventSubID subscribableevent.SubscriptionId

	relaySubscriptionMutex       sync.Mutex
	relaySubscriptions           map[relaySubscriptionKey]struct{}
	relayKeyedEventSubscriptions map[restream.KeyedEventSubscription]struct{}
	relayDataStreamSubscriptions map[restream.DataStreamSubscription]restream.AccessLevel
	relayStoreSubCount           map[string]int
	relayStoreGeneration         map[string]uint64
	relayStoreCatchingUp         map[string]bool
	relayCatchupPartials         map[string]restream.Partial
	onDemandStoreStreaming       bool

	dataStreamWorkerMutex sync.Mutex
	dataStreamWorkers     map[restream.DataStreamSubscription]*dataStreamWorker
	dataStreamPending     int
	ffrpcSlots            chan struct{}
}

type relaySubscriptionKey struct {
	storeName string
	key       string
}

type pendingRelayRPC struct {
	conn   *gws.Conn
	result chan relayRPCResult
}

type relayRPCResult struct {
	response []byte
	err      error
}

type outboundPacket struct {
	description string
	storeName   string
	packetKind  protocol.PacketKind
	generation  uint64
	bytes       []byte
	build       func() ([]byte, error)
}

type dataStreamOperation struct {
	ctx          context.Context
	cancel       context.CancelFunc
	conn         *gws.Conn
	operationID  uint32
	subscription restream.DataStreamSubscription
	accessLevel  restream.AccessLevel
	subscribe    bool
}

type dataStreamWorker struct {
	operations chan dataStreamOperation
}

type activeRelayDataStream struct {
	subscription restream.DataStreamSubscription
	accessLevel  restream.AccessLevel
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
	ffrpcHandlers ...restream.FFRPCHandlerFunc,
) *Streamer {
	if len(ffrpcHandlers) > 1 {
		panic("NewStreamer accepts at most one FFRPC handler")
	}
	if len(ffrpcHandlers) == 1 && ffrpcHandlers[0] != nil && config.FFRPCHandlerWithAnnotations != nil {
		panic("NewStreamer legacy FFRPC handler and Config.FFRPCHandlerWithAnnotations are mutually exclusive")
	}
	if rpc != nil && config.RPCHandlerWithAnnotations != nil {
		panic("NewStreamer RPC handler and Config.RPCHandlerWithAnnotations are mutually exclusive")
	}
	var ffrpc restream.FFRPCHandlerFunc
	if len(ffrpcHandlers) == 1 {
		ffrpc = ffrpcHandlers[0]
	}
	opts := applyDefaults(config)

	s := &Streamer{
		sr:                   sr,
		rpc:                  rpc,
		rpcWithAnnotations:   opts.RPCHandlerWithAnnotations,
		ffrpc:                ffrpc,
		ffrpcWithAnnotations: opts.FFRPCHandlerWithAnnotations,
		ed:                   ed,

		opts: opts,

		gatheredPartials:             map[string]restream.Partial{},
		gatherStart:                  map[string]time.Time{},
		gatherGeneration:             map[string]uint64{},
		relaySubscriptions:           map[relaySubscriptionKey]struct{}{},
		relayKeyedEventSubscriptions: map[restream.KeyedEventSubscription]struct{}{},
		relayDataStreamSubscriptions: map[restream.DataStreamSubscription]restream.AccessLevel{},
		relayStoreSubCount:           map[string]int{},
		relayStoreGeneration:         map[string]uint64{},
		relayStoreCatchingUp:         map[string]bool{},
		relayCatchupPartials:         map[string]restream.Partial{},
		relayRPCPending:              map[uint32]pendingRelayRPC{},
		dataStreamWorkers:            map[restream.DataStreamSubscription]*dataStreamWorker{},
		ffrpcSlots:                   make(chan struct{}, maxInFlightRelayFFRPCs),
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
		s.keyedEventSubID = ed.SubscribeToKeyedEvents(func(
			storeName string,
			eventName string,
			key string,
			eventBytes []byte,
		) {
			if !s.isRelayedKeyedEventSubscribed(storeName, eventName, key) {
				return
			}
			if err := s.SendKeyedEvent(storeName, eventName, key, eventBytes); err != nil {
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
		if err := s.ed.UnsubscribeFromKeyedEvents(s.keyedEventSubID); err != nil && retErr == nil {
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

// SendKeyedEvent sends a serialized store-owned keyed event to the relay server.
func (s *Streamer) SendKeyedEvent(
	storeName string,
	eventName string,
	key string,
	eventBytes []byte,
) error {
	packetBytes, err := protocol.EncodePacket(&protocol.KeyedEventPacket{
		StoreName: storeName,
		EventName: eventName,
		Key:       key,
		Data:      eventBytes,
	})
	if err != nil {
		return err
	}

	return s.enqueuePacket("keyed event "+storeName+"/"+eventName+"/"+key, packetBytes)
}

// CallRelayRPC invokes an application RPC on the authenticated relay server.
func (s *Streamer) CallRelayRPC(ctx context.Context, methodName string, request []byte) ([]byte, error) {
	if ctx == nil {
		return nil, fmt.Errorf("relay RPC context is nil")
	}
	if methodName == "" {
		return nil, fmt.Errorf("relay RPC method name is empty")
	}

	s.connMutex.Lock()
	if s.conn == nil || s.sendQueue == nil || s.sendDone == nil {
		s.connMutex.Unlock()
		return nil, ErrDisconnected
	}
	if !s.relayRPCs {
		s.connMutex.Unlock()
		return nil, ErrRelayRPCUnsupported
	}
	if len(s.relayRPCPending) >= maxPendingRelayRPCs {
		s.connMutex.Unlock()
		return nil, fmt.Errorf("too many pending relay RPCs")
	}
	conn := s.conn
	rpcID, err := s.nextRelayRPCIDLocked()
	if err != nil {
		s.connMutex.Unlock()
		return nil, err
	}
	resultCh := make(chan relayRPCResult, 1)
	s.relayRPCPending[rpcID] = pendingRelayRPC{conn: conn, result: resultCh}
	s.connMutex.Unlock()

	packetBytes, err := protocol.EncodePacket(&protocol.RelayRPCCallPacket{
		RPCID:      rpcID,
		MethodName: methodName,
		Request:    request,
	})
	if err != nil {
		s.removePendingRelayRPC(conn, rpcID)
		return nil, err
	}
	if err := s.enqueuePacketForConn(conn, "relay RPC "+methodName, packetBytes); err != nil {
		s.removePendingRelayRPC(conn, rpcID)
		return nil, err
	}

	select {
	case result := <-resultCh:
		return result.response, result.err
	case <-ctx.Done():
		s.removePendingRelayRPC(conn, rpcID)
		return nil, ctx.Err()
	}
}

func (s *Streamer) nextRelayRPCIDLocked() (uint32, error) {
	for attempts := 0; attempts <= len(s.relayRPCPending); attempts++ {
		s.relayRPCNextID++
		if s.relayRPCNextID == 0 {
			s.relayRPCNextID++
		}
		if _, exists := s.relayRPCPending[s.relayRPCNextID]; !exists {
			return s.relayRPCNextID, nil
		}
	}
	return 0, fmt.Errorf("no relay RPC IDs available")
}

func (s *Streamer) removePendingRelayRPC(conn *gws.Conn, rpcID uint32) {
	s.connMutex.Lock()
	if pending, exists := s.relayRPCPending[rpcID]; exists && pending.conn == conn {
		delete(s.relayRPCPending, rpcID)
	}
	s.connMutex.Unlock()
}

func (s *Streamer) handleRelayRPCResponse(conn *gws.Conn, packet *protocol.RelayRPCResponsePacket) {
	s.connMutex.Lock()
	pending, exists := s.relayRPCPending[packet.RPCID]
	if exists && pending.conn == conn {
		delete(s.relayRPCPending, packet.RPCID)
	} else {
		exists = false
	}
	s.connMutex.Unlock()
	if !exists {
		return
	}
	var err error
	if packet.Error != "" {
		err = errors.New(packet.Error)
	}
	pending.result <- relayRPCResult{response: packet.Response, err: err}
}

func (s *Streamer) handleConn(ctx context.Context, conn *gws.Conn, credentials Credentials) error {
	connCtx, cancelConn := context.WithCancel(ctx)
	conn.SetReadLimit(s.opts.MaxReadMessageBytes)
	defer func() {
		// Cancel transition handlers before snapshotting active streams. A
		// successful start and disconnect cleanup are committed under the same
		// subscription mutex, so cleanup cannot miss a just-completed start.
		cancelConn()
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
		Metadata: protocol.DeviceMetadataWithCapabilities(
			credentials.Metadata,
			protocol.DeviceCapabilities{
				DataStreams:    s.opts.DataStreams != nil && s.opts.DataStreams.HasRegistrations(),
				RPCAnnotations: s.rpcWithAnnotations != nil || s.ffrpcWithAnnotations != nil,
			},
		),
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
			s.startConn(conn, packet.Capabilities.RelayRPCs)
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
		case *protocol.RelayRPCResponsePacket:
			s.handleRelayRPCResponse(conn, packet)
		case *protocol.FFRPCCallPacket:
			select {
			case s.ffrpcSlots <- struct{}{}:
			default:
				return fmt.Errorf("too many in-flight relay FFRPCs")
			}
			go func(ffrpcPacket *protocol.FFRPCCallPacket) {
				defer func() { <-s.ffrpcSlots }()
				_ = s.handleFFRPCCall(ffrpcPacket)
			}(packet)
		case *protocol.StoreSubscriptionPacket:
			if err := s.handleStoreSubscription(packet); err != nil {
				return fmt.Errorf("handle relay store subscription packet action %d for store %q key %q: %w",
					packet.Action, packet.StoreName, packet.Key, err)
			}
		case *protocol.KeyedEventSubscriptionPacket:
			if err := s.handleKeyedEventSubscription(packet); err != nil {
				return fmt.Errorf(
					"handle relay keyed event subscription packet action %d for store %q event %q key %q: %w",
					packet.Action,
					packet.StoreName,
					packet.EventName,
					packet.Key,
					err,
				)
			}
		case *protocol.DataStreamSubscriptionPacket:
			// This legacy optional form has no capability or acknowledgement.
			// Ignore it without disturbing the base relay connection.
		case *protocol.DataStreamSubscriptionRequestPacket:
			if err := s.handleDataStreamSubscription(connCtx, conn, packet); err != nil {
				return fmt.Errorf(
					"handle relay data stream subscription packet action %d for store %q stream %q key %q: %w",
					packet.Action,
					packet.StoreName,
					packet.StreamName,
					packet.Key,
					err,
				)
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
			partialSnapshot, snapshotErr := restream.ClonePartial(partial)
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
	partialSnapshot, err := restream.ClonePartial(partial)
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
	if s.rpc == nil && s.rpcWithAnnotations == nil {
		return fmt.Errorf("relay RPC received but no RPC handler is configured")
	}
	var resp []byte
	var handled bool
	var err error
	if s.rpcWithAnnotations != nil {
		resp, handled, err = s.rpcWithAnnotations(
			packet.Annotations,
			packet.MethodName,
			restream.AccessLevel(packet.AccessLevel),
			packet.Request,
		)
	} else {
		resp, handled, err = s.rpc(packet.MethodName, restream.AccessLevel(packet.AccessLevel), packet.Request)
	}
	if err != nil {
		return err
	}
	if !handled {
		return fmt.Errorf("unhandled RPC %s", packet.MethodName)
	}
	return s.sendRPCResponse(packet.RPCID, resp)
}

func (s *Streamer) handleFFRPCCall(packet *protocol.FFRPCCallPacket) error {
	if s.ffrpcWithAnnotations == nil && s.ffrpc == nil {
		return fmt.Errorf("relay FFRPC received but no FFRPC handler is configured")
	}
	var handled bool
	var err error
	if s.ffrpcWithAnnotations != nil {
		handled, err = s.ffrpcWithAnnotations(
			packet.Annotations, packet.MethodName, restream.AccessLevel(packet.AccessLevel), packet.Request,
		)
	} else {
		handled, err = s.ffrpc(packet.MethodName, restream.AccessLevel(packet.AccessLevel), packet.Request)
	}
	if err != nil {
		return err
	}
	if !handled {
		return fmt.Errorf("unhandled FFRPC %s", packet.MethodName)
	}
	return nil
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

func (s *Streamer) handleKeyedEventSubscription(packet *protocol.KeyedEventSubscriptionPacket) error {
	allowed, err := s.allowsRelayStoreTraffic(packet.StoreName)
	if err != nil || !allowed {
		return err
	}
	if s.ed == nil {
		return fmt.Errorf("relay keyed event subscription received without an event dispatcher")
	}

	subscription := restream.KeyedEventSubscription{
		StoreName: packet.StoreName,
		EventName: packet.EventName,
		Key:       packet.Key,
	}
	switch packet.Action {
	case protocol.StoreSubscribe:
		return s.startRelayedKeyedEventSubscription(subscription)
	case protocol.StoreUnsubscribe:
		return s.stopRelayedKeyedEventSubscription(subscription)
	default:
		return fmt.Errorf("invalid keyed event subscription action %d", packet.Action)
	}
}

func (s *Streamer) startRelayedKeyedEventSubscription(subscription restream.KeyedEventSubscription) error {
	s.relaySubscriptionMutex.Lock()
	if s.relayKeyedEventSubscriptions == nil {
		s.relayKeyedEventSubscriptions = map[restream.KeyedEventSubscription]struct{}{}
	}
	if _, exists := s.relayKeyedEventSubscriptions[subscription]; exists {
		s.relaySubscriptionMutex.Unlock()
		return nil
	}
	if len(s.relayKeyedEventSubscriptions) >= maxRelayedKeyedSubscriptions {
		s.relaySubscriptionMutex.Unlock()
		return fmt.Errorf("too many relayed keyed event subscriptions")
	}
	s.relayKeyedEventSubscriptions[subscription] = struct{}{}
	s.relaySubscriptionMutex.Unlock()

	if err := s.ed.ListeningToKeyedEvent(subscription.StoreName, subscription.EventName, subscription.Key); err != nil {
		s.relaySubscriptionMutex.Lock()
		delete(s.relayKeyedEventSubscriptions, subscription)
		s.relaySubscriptionMutex.Unlock()
		return err
	}
	return nil
}

func (s *Streamer) stopRelayedKeyedEventSubscription(subscription restream.KeyedEventSubscription) error {
	s.relaySubscriptionMutex.Lock()
	if _, exists := s.relayKeyedEventSubscriptions[subscription]; !exists {
		s.relaySubscriptionMutex.Unlock()
		return nil
	}
	delete(s.relayKeyedEventSubscriptions, subscription)
	s.relaySubscriptionMutex.Unlock()
	return s.ed.StopListeningToKeyedEvent(subscription.StoreName, subscription.EventName, subscription.Key)
}

func (s *Streamer) isRelayedKeyedEventSubscribed(storeName string, eventName string, key string) bool {
	subscription := restream.KeyedEventSubscription{
		StoreName: storeName,
		EventName: eventName,
		Key:       key,
	}
	s.relaySubscriptionMutex.Lock()
	_, subscribed := s.relayKeyedEventSubscriptions[subscription]
	s.relaySubscriptionMutex.Unlock()
	return subscribed
}

func (s *Streamer) handleDataStreamSubscription(
	ctx context.Context,
	conn *gws.Conn,
	packet *protocol.DataStreamSubscriptionRequestPacket,
) error {
	allowed, err := s.allowsRelayStoreTraffic(packet.StoreName)
	if err != nil {
		return s.sendDataStreamSubscriptionResult(conn, packet.OperationID, err)
	}
	if !allowed {
		return s.sendDataStreamSubscriptionResult(
			conn,
			packet.OperationID,
			fmt.Errorf("store %q does not allow relay stream traffic", packet.StoreName),
		)
	}
	if s.opts.DataStreams == nil {
		return s.sendDataStreamSubscriptionResult(
			conn,
			packet.OperationID,
			fmt.Errorf("relay data stream subscription received without a dispatcher"),
		)
	}

	subscription := restream.DataStreamSubscription{
		StoreName:  packet.StoreName,
		StreamName: packet.StreamName,
		Key:        packet.Key,
	}
	if err := subscription.Validate(); err != nil {
		return s.sendDataStreamSubscriptionResult(conn, packet.OperationID, err)
	}
	accessLevel := restream.AccessLevel(packet.AccessLevel)
	if err := s.opts.DataStreams.CheckAccess(subscription, accessLevel); err != nil {
		return s.sendDataStreamSubscriptionResult(conn, packet.OperationID, err)
	}

	operation := dataStreamOperation{
		ctx:          ctx,
		conn:         conn,
		operationID:  packet.OperationID,
		subscription: subscription,
		accessLevel:  accessLevel,
	}
	switch packet.Action {
	case protocol.StoreSubscribe:
		operation.subscribe = true
	case protocol.StoreUnsubscribe:
	default:
		return s.sendDataStreamSubscriptionResult(
			conn,
			packet.OperationID,
			fmt.Errorf("invalid data stream subscription action %d", packet.Action),
		)
	}
	if err := s.enqueueDataStreamOperation(operation); err != nil {
		return err
	}
	return nil
}

func (s *Streamer) relayedDataStreamState(
	subscription restream.DataStreamSubscription,
) (restream.AccessLevel, bool) {
	s.relaySubscriptionMutex.Lock()
	defer s.relaySubscriptionMutex.Unlock()
	activeAccessLevel, exists := s.relayDataStreamSubscriptions[subscription]
	return activeAccessLevel, exists
}

func (s *Streamer) commitRelayedDataStreamState(
	ctx context.Context,
	subscription restream.DataStreamSubscription,
	accessLevel restream.AccessLevel,
	active bool,
) bool {
	s.relaySubscriptionMutex.Lock()
	if active && ctx.Err() != nil {
		s.relaySubscriptionMutex.Unlock()
		return false
	}
	if active {
		s.relayDataStreamSubscriptions[subscription] = accessLevel
	} else {
		delete(s.relayDataStreamSubscriptions, subscription)
	}
	s.relaySubscriptionMutex.Unlock()
	return true
}

func (s *Streamer) enqueueDataStreamOperation(operation dataStreamOperation) error {
	s.dataStreamWorkerMutex.Lock()
	if s.dataStreamPending >= maxPendingDataStreamOperations {
		s.dataStreamWorkerMutex.Unlock()
		return fmt.Errorf("too many pending data stream operations")
	}
	worker := s.dataStreamWorkers[operation.subscription]
	if worker == nil {
		s.relaySubscriptionMutex.Lock()
		_, alreadyActive := s.relayDataStreamSubscriptions[operation.subscription]
		identityCount := len(s.relayDataStreamSubscriptions)
		for workerSubscription := range s.dataStreamWorkers {
			if _, active := s.relayDataStreamSubscriptions[workerSubscription]; !active {
				identityCount++
			}
		}
		s.relaySubscriptionMutex.Unlock()
		if !alreadyActive && identityCount >= maxDataStreamWorkers {
			s.dataStreamWorkerMutex.Unlock()
			return fmt.Errorf("too many relayed data stream identities")
		}
		if len(s.dataStreamWorkers) >= maxDataStreamWorkers {
			s.dataStreamWorkerMutex.Unlock()
			return fmt.Errorf("too many active data stream operation workers")
		}
		worker = &dataStreamWorker{
			operations: make(chan dataStreamOperation, dataStreamOperationQueueSize),
		}
		s.dataStreamWorkers[operation.subscription] = worker
		go s.runDataStreamWorker(operation.subscription, worker)
	}
	select {
	case worker.operations <- operation:
		s.dataStreamPending++
		s.dataStreamWorkerMutex.Unlock()
		return nil
	default:
		s.dataStreamWorkerMutex.Unlock()
		return fmt.Errorf("data stream operation queue is full")
	}
}

func (s *Streamer) runDataStreamWorker(
	subscription restream.DataStreamSubscription,
	worker *dataStreamWorker,
) {
	for {
		operation := <-worker.operations
		activeAccessLevel, active := s.relayedDataStreamState(operation.subscription)
		var err error
		switch {
		case operation.subscribe && active:
			// Starts are idempotent. Preserve the access level that authorized
			// the actual active source transition.
		case !operation.subscribe && !active:
			// Stops are idempotent, including disconnect cleanup racing a
			// viewer unsubscribe.
		default:
			if !operation.subscribe {
				operation.accessLevel = activeAccessLevel
			}
			operationCtx, cancelOperation := context.WithTimeout(
				operation.ctx,
				dataStreamTransitionTimeout,
			)
			err = s.opts.DataStreams.Dispatch(
				operationCtx,
				operation.subscription,
				operation.accessLevel,
				operation.subscribe,
			)
			cancelOperation()
			if err == nil {
				committed := s.commitRelayedDataStreamState(
					operation.ctx,
					operation.subscription,
					operation.accessLevel,
					operation.subscribe,
				)
				if operation.subscribe && !committed {
					cleanupCtx, cancelCleanup := context.WithTimeout(
						context.Background(),
						dataStreamTransitionTimeout,
					)
					err = s.opts.DataStreams.Dispatch(
						cleanupCtx,
						operation.subscription,
						operation.accessLevel,
						false,
					)
					cancelCleanup()
					if err == nil {
						err = operation.ctx.Err()
					} else {
						// The source did start and its compensating stop failed.
						// Keep that fact so a reconnect or later cleanup can
						// issue another idempotent stop.
						s.commitRelayedDataStreamState(
							context.Background(),
							operation.subscription,
							operation.accessLevel,
							true,
						)
					}
				}
			}
		}
		if operation.cancel != nil {
			operation.cancel()
		}
		if operation.conn != nil && operation.operationID != 0 {
			if sendErr := s.sendDataStreamSubscriptionResult(
				operation.conn,
				operation.operationID,
				err,
			); sendErr != nil {
				s.closeCurrentConnOnSendError(sendErr)
			}
		}

		s.dataStreamWorkerMutex.Lock()
		s.dataStreamPending--
		if len(worker.operations) == 0 && s.dataStreamWorkers[subscription] == worker {
			delete(s.dataStreamWorkers, subscription)
			s.dataStreamWorkerMutex.Unlock()
			return
		}
		s.dataStreamWorkerMutex.Unlock()
	}
}

func (s *Streamer) sendDataStreamSubscriptionResult(
	conn *gws.Conn,
	operationID uint32,
	operationErr error,
) error {
	errorMessage := ""
	if operationErr != nil {
		errorMessage = operationErr.Error()
		if len(errorMessage) > maxDataStreamResultErrorBytes {
			errorMessage = errorMessage[:maxDataStreamResultErrorBytes]
		}
	}
	packetBytes, err := protocol.EncodePacket(&protocol.DataStreamSubscriptionResultPacket{
		OperationID: operationID,
		Error:       errorMessage,
	})
	if err != nil {
		return err
	}
	return s.enqueuePacketForConn(conn, "data stream subscription result", packetBytes)
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
	if len(s.relaySubscriptions) >= maxRelayedStoreSubscriptions {
		s.relaySubscriptionMutex.Unlock()
		return fmt.Errorf("too many relayed store subscriptions")
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

func (s *Streamer) startConn(conn *gws.Conn, relayRPCs bool) {
	sendQueue := make(chan outboundPacket, s.opts.SendQueueSize)
	sendDone := make(chan struct{})

	s.closeCurrentConn()

	s.connMutex.Lock()
	s.conn = conn
	s.sendQueue = sendQueue
	s.sendDone = sendDone
	s.relayRPCs = relayRPCs
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
		s.relayRPCs = false
		for rpcID, pending := range s.relayRPCPending {
			if pending.conn == conn {
				delete(s.relayRPCPending, rpcID)
				pending.result <- relayRPCResult{err: ErrDisconnected}
			}
		}
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
	keyedEventSubscriptions := make(
		[]restream.KeyedEventSubscription,
		0,
		len(s.relayKeyedEventSubscriptions),
	)
	for subscription := range s.relayKeyedEventSubscriptions {
		keyedEventSubscriptions = append(keyedEventSubscriptions, subscription)
	}
	dataStreamSubscriptions := make(
		[]activeRelayDataStream,
		0,
		len(s.relayDataStreamSubscriptions),
	)
	for subscription, accessLevel := range s.relayDataStreamSubscriptions {
		dataStreamSubscriptions = append(dataStreamSubscriptions, activeRelayDataStream{
			subscription: subscription,
			accessLevel:  accessLevel,
		})
	}
	s.relaySubscriptions = map[relaySubscriptionKey]struct{}{}
	s.relayKeyedEventSubscriptions = map[restream.KeyedEventSubscription]struct{}{}
	s.relayStoreSubCount = map[string]int{}
	s.relayStoreCatchingUp = map[string]bool{}
	s.relayCatchupPartials = map[string]restream.Partial{}
	s.onDemandStoreStreaming = false
	s.relaySubscriptionMutex.Unlock()

	if s.sr != nil {
		for _, key := range keys {
			s.sr.StopListeningToStoreKey(key.storeName, key.key) //nolint:errcheck // Why: Cleanup is best effort on relay disconnect.
		}
	}
	if s.ed != nil {
		for _, subscription := range keyedEventSubscriptions {
			s.ed.StopListeningToKeyedEvent( //nolint:errcheck // Why: Cleanup is best effort on relay disconnect.
				subscription.StoreName,
				subscription.EventName,
				subscription.Key,
			)
		}
	}
	if s.opts.DataStreams != nil {
		for _, active := range dataStreamSubscriptions {
			cleanupCtx, cancel := context.WithTimeout(
				context.Background(),
				defaultWriteTimeout*6,
			)
			if err := s.enqueueDataStreamOperation(dataStreamOperation{
				ctx:          cleanupCtx,
				cancel:       cancel,
				subscription: active.subscription,
				accessLevel:  active.accessLevel,
				subscribe:    false,
			}); err != nil {
				cancel()
			}
		}
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

func (s *Streamer) enqueuePacketForConn(
	conn *gws.Conn,
	packetDescription string,
	b []byte,
) error {
	packet := outboundPacket{description: packetDescription, bytes: b}
	s.connMutex.RLock()
	if s.conn != conn {
		s.connMutex.RUnlock()
		return ErrDisconnected
	}
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
		return ErrSendQueueFull
	}
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
