package restream

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/boatkit-io/restream/pkg/smartmutex"
	"github.com/boatkit-io/tugboat/pkg/subscribableevent"
	"github.com/mitchellh/mapstructure"
	"github.com/samber/lo"
	"github.com/sirupsen/logrus"
	"github.com/zishang520/socket.io/servers/socket/v3"
	socketTypes "github.com/zishang520/socket.io/v3/pkg/types"
)

// StoreSubscriptionAction is an enum for the type of store subscription action
type StoreSubscriptionAction uint

const maxSocketMethodNameBytes = 512

// StoreSubscriptionActions
const (
	// Subscribe
	Subscribe StoreSubscriptionAction = 0
	// Unsubscribe
	Unsubscribe StoreSubscriptionAction = 1
)

// StoreSubscriptionMessage is a message for subscribing/unsubscribing from stores
type StoreSubscriptionMessage struct {
	StoreName string                  `json:"storeName"`
	Action    StoreSubscriptionAction `json:"action"`
	Key       string                  `json:"key"`
}

// KeyedEventSubscriptionMessage is a message for subscribing/unsubscribing from a store-owned keyed event.
type KeyedEventSubscriptionMessage struct {
	StoreName string                  `json:"storeName"`
	EventName string                  `json:"eventName"`
	Action    StoreSubscriptionAction `json:"action"`
	Key       string                  `json:"key"`
}

// DataStreamSubscriptionMessage starts or stops one viewer data-plane lease.
// SubscriptionID is client-generated and scoped to this viewer socket.
type DataStreamSubscriptionMessage struct {
	SubscriptionID string                  `json:"subscriptionID"`
	StoreName      string                  `json:"storeName"`
	StreamName     string                  `json:"streamName"`
	Action         StoreSubscriptionAction `json:"action"`
	Key            string                  `json:"key"`
}

// DataStreamEndpointMessage resolves a data stream subscription request.
type DataStreamEndpointMessage struct {
	SubscriptionID string              `json:"subscriptionID"`
	Endpoint       *DataStreamEndpoint `json:"endpoint,omitempty"`
	Error          string              `json:"error,omitempty"`
}

// ViewerSessionStoreSubscription is one desired store state subscription in a
// session attachment manifest.
type ViewerSessionStoreSubscription struct {
	StoreName string `json:"storeName"`
	Key       string `json:"key"`
}

// ViewerSessionDataStreamSubscription is one desired high-bandwidth stream in
// a session attachment manifest.
type ViewerSessionDataStreamSubscription struct {
	SubscriptionID string `json:"subscriptionID"`
	StoreName      string `json:"storeName"`
	StreamName     string `json:"streamName"`
	Key            string `json:"key"`
}

// ViewerSessionAttachRequest creates a fresh session when SessionID is empty or
// resumes an existing in-memory session otherwise. The subscription manifests
// are authoritative for the newly attached client.
type ViewerSessionAttachRequest struct {
	SessionID               string                                `json:"sessionID"`
	StoreSubscriptions      []ViewerSessionStoreSubscription      `json:"storeSubscriptions"`
	KeyedEventSubscriptions []KeyedEventSubscriptionMessage       `json:"keyedEventSubscriptions"`
	DataStreamSubscriptions []ViewerSessionDataStreamSubscription `json:"dataStreamSubscriptions"`
}

// ViewerSessionCapabilities advertises optional viewer transport features.
type ViewerSessionCapabilities struct {
	DataStreams bool `json:"dataStreams"`
}

// ViewerSessionAttachResponse resolves one attachment request.
type ViewerSessionAttachResponse struct {
	SessionID    string                    `json:"sessionID"`
	Resumed      bool                      `json:"resumed"`
	Capabilities ViewerSessionCapabilities `json:"capabilities"`
	Error        string                    `json:"error,omitempty"`
}

// ViewerSessionCloseMessage explicitly destroys an authenticated viewer
// session. Closure is idempotent.
type ViewerSessionCloseMessage struct {
	SessionID string `json:"sessionID"`
}

// StoreUpdateMessageKind is an enum for what type of store update message it is
type StoreUpdateMessageKind uint

// StoreUpdateMessageKinds
const (
	// Full: Full update
	StoreUpdateFull StoreUpdateMessageKind = 0
	// Partial: A partial struct
	StoreUpdatePartial StoreUpdateMessageKind = 2
)

// StoreUpdateMessage is a message containing data from a store update
type StoreUpdateMessage struct {
	Time      int64                  `json:"time"`
	Kind      StoreUpdateMessageKind `json:"kind"`
	StoreName string                 `json:"storeName"`
}

// StoreUpdateFullMessage is a StoreUpdateMessage for a full copy of an entire store's data (first sent after subscription to set
// a baseline)
type StoreUpdateFullMessage struct {
	StoreUpdateMessage `msgpack:",noinline"`

	State socketTypes.BufferInterface `json:"state"`
}

// StoreUpdatePartialMessage is a StoreUpdateMessage for a partial update to a store's data
type StoreUpdatePartialMessage struct {
	StoreUpdateMessage `msgpack:",noinline"`

	Partial socketTypes.BufferInterface `json:"partial"`
}

// EventMessage is emitted when a server-side EventDispatcher registered event fires.
type EventMessage struct {
	Time      int64                       `json:"time"`
	EventName string                      `json:"eventName"`
	Event     socketTypes.BufferInterface `json:"event"`
}

// KeyedEventMessage is emitted when a subscribed store-owned keyed event fires.
type KeyedEventMessage struct {
	Time      int64                       `json:"time"`
	StoreName string                      `json:"storeName"`
	EventName string                      `json:"eventName"`
	Key       string                      `json:"key"`
	Event     socketTypes.BufferInterface `json:"event"`
}

// RPCCallMessage is a message sent by the client with an RPC call (i.e. a `BlahStore.SetXYZ` message/call)
type RPCCallMessage struct {
	CallID     int                         `json:"callID"`
	MethodName string                      `json:"methodName"`
	Request    socketTypes.BufferInterface `json:"request"`
}

// FFRPCCallMessage is a fire-and-forget RPC sent by the client. It deliberately
// has no call ID because the server never sends a response.
type FFRPCCallMessage struct {
	MethodName string                      `json:"methodName"`
	Request    socketTypes.BufferInterface `json:"request"`
}

// RPCCallResponseMessage is a message sent by the server in response to an RPC call
type RPCCallResponseMessage struct {
	CallID   int                         `json:"callID"`
	Response socketTypes.BufferInterface `json:"response"`
	Error    *RPCCallError               `json:"error"`
}

// RPCCallError is the model for an error that supports a message and optional associated data mappings
type RPCCallError struct {
	Message string         `json:"message"`
	Data    map[string]any `json:"data"`
}

const (
	// SocketEventNameStoreUpdate - Store Update
	SocketEventNameStoreUpdate = "storeupdate"
	// SocketEventNameStoreSubscription - Store Subscription
	SocketEventNameStoreSubscription = "storesub"

	// SocketEventNameEvent - Server-originated EventDispatcher event
	SocketEventNameEvent = "event"
	// SocketEventNameKeyedEvent - Server-originated store-owned keyed event
	SocketEventNameKeyedEvent = "keyedevent"
	// SocketEventNameKeyedEventSubscription - Store-owned keyed event subscription
	SocketEventNameKeyedEventSubscription = "keyedeventsub"
	// SocketEventNameDataStreamSubscription - High-bandwidth stream subscription lifecycle
	SocketEventNameDataStreamSubscription = "datastreamsub"
	// SocketEventNameDataStreamEndpoint - Allocated high-bandwidth viewer endpoint
	SocketEventNameDataStreamEndpoint = "datastreamendpoint"
	// SocketEventNameViewerSessionAttach creates or resumes a viewer session.
	SocketEventNameViewerSessionAttach = "viewersessionattach"
	// SocketEventNameViewerSessionAttached resolves a viewer session attachment.
	SocketEventNameViewerSessionAttached = "viewersessionattached"
	// SocketEventNameViewerSessionClose explicitly destroys a viewer session.
	SocketEventNameViewerSessionClose = "viewersessionclose"

	// SocketEventNameRPCCall - RPC Call
	SocketEventNameRPCCall = "rpccall"
	// SocketEventNameRPCCallResponse - RPC Call Response
	SocketEventNameRPCCallResponse = "rpccallresp"
	// SocketEventNameFFRPCCall - Fire-and-forget RPC Call
	SocketEventNameFFRPCCall = "ffrpc"
)

// emitMessage is a struct for storing queued message to be emitted through the websocket
type emitMessage struct {
	Name         string
	Message      any
	Build        func() (emitMessage, error)
	StoreName    string
	StoreFull    Serializable
	StorePartial Partial
	Bytes        int
}

func (m emitMessage) resolve() (emitMessage, error) {
	if m.Build == nil {
		return m, nil
	}
	return m.Build()
}

type AccessLookupFunc func() (AccessLevel, error)

// StoreDeliveryMode controls whether a viewer receives only its subscribed
// field paths or complete store state while any field in that store is in use.
type StoreDeliveryMode uint8

const (
	// StoreDeliveryModeKeyed filters catch-ups and live updates to the
	// viewer's exact field subscriptions. This is appropriate for cloud relays.
	StoreDeliveryModeKeyed StoreDeliveryMode = iota
	// StoreDeliveryModeFullStore sends one full baseline when a store first
	// becomes active and all store updates until its final subscription stops.
	StoreDeliveryModeFullStore
)

// SocketHandlerOptions configures optional Restream websocket surfaces.
type SocketHandlerOptions struct {
	FFRPCHandler          FFRPCHandlerFunc
	DataStreamBroker      DataStreamBroker
	SessionManager        *ViewerSessionManager
	SessionIdentityLookup ViewerSessionIdentityLookupFunc
	StoreDeliveryMode     StoreDeliveryMode
	Limits                SocketHandlerLimits
}

// SocketHandlerLimits bounds state and work retained by one viewer connection.
// Reaching a limit disconnects the client and runs normal subscription cleanup.
type SocketHandlerLimits struct {
	MaxStoreSubscriptions        int
	MaxKeyedEventSubscriptions   int
	MaxDataStreamSubscriptions   int
	MaxBufferedStoreUpdates      int
	MaxBufferedSessionEvents     int
	MaxBufferedSessionBytes      int
	MaxBufferedSessionStores     int
	MaxBufferedSessionStoreBytes int
	MaxSessionManifestBytes      int
	MaxInFlightRPCs              int
	MaxInFlightFFRPCs            int
	MaxRPCRequestBytes           int
	MaxFFRPCRequestBytes         int
}

func (l SocketHandlerLimits) withDefaults() SocketHandlerLimits {
	if l.MaxStoreSubscriptions <= 0 {
		l.MaxStoreSubscriptions = 4096
	}
	if l.MaxKeyedEventSubscriptions <= 0 {
		l.MaxKeyedEventSubscriptions = 4096
	}
	if l.MaxDataStreamSubscriptions <= 0 {
		l.MaxDataStreamSubscriptions = 64
	}
	if l.MaxBufferedStoreUpdates <= 0 {
		l.MaxBufferedStoreUpdates = 1024
	}
	if l.MaxBufferedSessionEvents <= 0 {
		l.MaxBufferedSessionEvents = 1024
	}
	if l.MaxBufferedSessionBytes <= 0 {
		l.MaxBufferedSessionBytes = 8 * 1024 * 1024
	}
	if l.MaxBufferedSessionStores <= 0 {
		l.MaxBufferedSessionStores = 1024
	}
	if l.MaxBufferedSessionStoreBytes <= 0 {
		l.MaxBufferedSessionStoreBytes = 32 * 1024 * 1024
	}
	if l.MaxSessionManifestBytes <= 0 {
		l.MaxSessionManifestBytes = 4 * 1024 * 1024
	}
	if l.MaxInFlightRPCs <= 0 {
		l.MaxInFlightRPCs = 128
	}
	if l.MaxInFlightFFRPCs <= 0 {
		l.MaxInFlightFFRPCs = 128
	}
	if l.MaxRPCRequestBytes <= 0 {
		l.MaxRPCRequestBytes = 16 * 1024 * 1024
	}
	if l.MaxFFRPCRequestBytes <= 0 {
		l.MaxFFRPCRequestBytes = 4 * 1024 * 1024
	}
	return l
}

type trackedDataStreamSubscription struct {
	subscription DataStreamSubscription
	endpoint     *DataStreamEndpoint
	cancel       context.CancelFunc
}

type bufferedViewerStoreUpdate struct {
	fullState Serializable
	partial   Partial
	bytes     int
}

type socketTrackerConfig struct {
	log          *logrus.Logger
	sr           *StoreRegistry
	rpch         RPCHandlerFunc
	ffrpch       FFRPCHandlerFunc
	ed           *EventDispatcher
	dataStreams  DataStreamBroker
	accessLookup AccessLookupFunc
	storeMode    StoreDeliveryMode
	limits       SocketHandlerLimits
}

// socketTracker is a handler struct holding the information for a single websocket connection
type socketTracker struct {
	log         *logrus.Logger
	sr          *StoreRegistry
	rpch        RPCHandlerFunc
	ffrpch      FFRPCHandlerFunc
	ed          *EventDispatcher
	dataStreams DataStreamBroker
	storeMode   StoreDeliveryMode
	limits      SocketHandlerLimits
	lifetimeCtx context.Context
	cancel      context.CancelFunc
	rpcSlots    chan struct{}
	ffrpcSlots  chan struct{}

	accessMutex  sync.RWMutex
	accessLookup AccessLookupFunc

	sessionID       string
	sessionManager  *ViewerSessionManager
	sessionIdentity ViewerSessionIdentity

	attachmentMutex      sync.RWMutex
	conn                 *socket.Socket
	attachmentGeneration uint64
	attachmentReady      bool
	attachLifecycleMutex sync.Mutex
	deliveryMutex        sync.Mutex

	emitQueueMutex sync.RWMutex
	emitQueue      chan emitMessage

	partialApplySubID   subscribableevent.SubscriptionId
	fullStateApplySubID subscribableevent.SubscriptionId
	eventSubID          subscribableevent.SubscriptionId
	keyedEventSubID     subscribableevent.SubscriptionId

	storeSubscriptions          map[string]map[string]int
	keyedEventSubscriptions     map[KeyedEventSubscription]int
	dataStreamSubscriptions     map[string]trackedDataStreamSubscription
	storeSubscriptionCount      int
	keyedEventSubscriptionCount int
	subscriptionMutex           smartmutex.SmartMutex

	storeUpdateQueueMutex    sync.Mutex
	pendingStoreCatchups     map[string]int
	bufferedStoreUpdates     map[string][]emitMessage
	bufferedStoreUpdateCount int
	sessionBufferMutex       sync.Mutex
	sessionStoreUpdates      map[string]*bufferedViewerStoreUpdate
	sessionStoreUpdateBytes  int
	sessionEvents            []emitMessage
	sessionEventBytes        int
	destroyOnce              sync.Once
}

func newSocketTracker(config socketTrackerConfig) *socketTracker {
	limits := config.limits.withDefaults()
	lifetimeCtx, cancel := context.WithCancel(context.Background())
	tracker := &socketTracker{
		log:          config.log,
		sr:           config.sr,
		rpch:         config.rpch,
		ffrpch:       config.ffrpch,
		ed:           config.ed,
		dataStreams:  config.dataStreams,
		accessLookup: config.accessLookup,
		storeMode:    config.storeMode,
		limits:       limits,
		lifetimeCtx:  lifetimeCtx,
		cancel:       cancel,
		rpcSlots:     make(chan struct{}, limits.MaxInFlightRPCs),
		ffrpcSlots:   make(chan struct{}, limits.MaxInFlightFFRPCs),

		emitQueue:               make(chan emitMessage, max(100, limits.MaxBufferedSessionStores)),
		subscriptionMutex:       smartmutex.SmartMutex{Name: "restream.socketTracker.subscriptionMutex"},
		storeSubscriptions:      map[string]map[string]int{},
		keyedEventSubscriptions: map[KeyedEventSubscription]int{},
		dataStreamSubscriptions: map[string]trackedDataStreamSubscription{},
		sessionStoreUpdates:     map[string]*bufferedViewerStoreUpdate{},
	}
	tracker.subscribeToSources()
	tracker.handleEmitQueue()
	return tracker
}

func (st *socketTracker) subscribeToSources() {
	if st.sr != nil {
		st.partialApplySubID = st.sr.SubscribeToPartialApplies(st.PartialCallback)
		st.fullStateApplySubID = st.sr.SubscribeToFullStateApplies(st.FullStateCallback)
	}
	if st.ed != nil {
		st.eventSubID = st.ed.SubscribeToEvents(st.EventCallback)
		st.keyedEventSubID = st.ed.SubscribeToKeyedEvents(st.KeyedEventCallback)
	}
}

func AddSocketHandlers(
	conn *socket.Socket,
	log *logrus.Logger,
	sr *StoreRegistry,
	rpch RPCHandlerFunc,
	ed *EventDispatcher,
	accessLookup AccessLookupFunc,
	ffrpcHandlers ...FFRPCHandlerFunc,
) error {
	if len(ffrpcHandlers) > 1 {
		return fmt.Errorf("AddSocketHandlers accepts at most one FFRPC handler")
	}
	var ffrpch FFRPCHandlerFunc
	if len(ffrpcHandlers) == 1 {
		ffrpch = ffrpcHandlers[0]
	}
	return AddSocketHandlersWithOptions(conn, log, sr, rpch, ed, accessLookup, SocketHandlerOptions{
		FFRPCHandler: ffrpch,
	})
}

// AddSocketHandlersWithOptions adds Restream handlers including optional
// high-bandwidth data stream endpoint allocation.
func AddSocketHandlersWithOptions(
	conn *socket.Socket,
	log *logrus.Logger,
	sr *StoreRegistry,
	rpch RPCHandlerFunc,
	ed *EventDispatcher,
	accessLookup AccessLookupFunc,
	options SocketHandlerOptions,
) error {
	if options.StoreDeliveryMode != StoreDeliveryModeKeyed &&
		options.StoreDeliveryMode != StoreDeliveryModeFullStore {
		return fmt.Errorf("invalid store delivery mode %d", options.StoreDeliveryMode)
	}
	config := socketTrackerConfig{
		log:          log,
		sr:           sr,
		rpch:         rpch,
		ffrpch:       options.FFRPCHandler,
		ed:           ed,
		dataStreams:  options.DataStreamBroker,
		accessLookup: accessLookup,
		storeMode:    options.StoreDeliveryMode,
		limits:       options.Limits.withDefaults(),
	}
	if options.SessionManager != nil {
		if options.SessionIdentityLookup == nil {
			return errors.New("viewer sessions require a session identity lookup")
		}
		binding := &viewerSessionSocket{
			conn:           conn,
			manager:        options.SessionManager,
			identityLookup: options.SessionIdentityLookup,
			config:         config,
		}
		return binding.register()
	}

	tracker := newSocketTracker(config)
	generation, previous := tracker.attachSocket(conn)
	if previous != nil {
		previous.Disconnect(true)
	}
	if err := tracker.registerOperationalHandlers(conn, generation); err != nil {
		tracker.destroySession()
		conn.Disconnect(true)
		return err
	}
	tracker.finishAttach()
	return nil
}

type viewerSessionSocket struct {
	conn           *socket.Socket
	manager        *ViewerSessionManager
	identityLookup ViewerSessionIdentityLookupFunc
	config         socketTrackerConfig

	mu         sync.Mutex
	tracker    *socketTracker
	generation uint64
	sessionID  string
	attaching  bool
}

func (s *viewerSessionSocket) register() error {
	if err := s.conn.On(SocketEventNameViewerSessionAttach, s.onAttach); err != nil {
		s.conn.Disconnect(true)
		return err
	}
	if err := s.conn.On(SocketEventNameViewerSessionClose, s.onClose); err != nil {
		s.conn.Disconnect(true)
		return err
	}
	if err := s.conn.On("disconnect", s.onDisconnect); err != nil {
		s.conn.Disconnect(true)
		return err
	}
	return nil
}

func (s *viewerSessionSocket) onAttach(params ...any) {
	s.mu.Lock()
	alreadyAttached := s.tracker != nil || s.attaching
	if !alreadyAttached {
		s.attaching = true
	}
	s.mu.Unlock()
	if alreadyAttached {
		s.emitAttachError("viewer socket already has a session")
		return
	}
	defer func() {
		s.mu.Lock()
		s.attaching = false
		s.mu.Unlock()
	}()
	var request ViewerSessionAttachRequest
	if len(params) == 0 {
		s.emitAttachError("missing viewer session attachment")
		return
	}
	if err := mapstructure.Decode(params[0], &request); err != nil {
		s.emitAttachError("invalid viewer session attachment")
		return
	}
	identity, err := s.identityLookup()
	if err != nil {
		s.emitAttachError("could not verify viewer session identity")
		return
	}
	tracker, resumed, err := s.manager.attach(request, identity, s.config)
	if err != nil {
		s.emitAttachError(err.Error())
		return
	}
	tracker.attachLifecycleMutex.Lock()
	defer tracker.attachLifecycleMutex.Unlock()

	generation, previous := tracker.attachSocket(s.conn)
	if err := tracker.registerOperationalHandlers(s.conn, generation); err != nil {
		tracker.detachSocket(generation)
		s.manager.abort(tracker.sessionID, tracker)
		if previous != nil && previous != s.conn {
			previous.Disconnect(true)
		}
		s.emitAttachError("could not attach viewer session handlers")
		return
	}
	if err := tracker.reconcileSessionManifest(request, identity.AccessLevel); err != nil {
		tracker.detachSocket(generation)
		s.manager.abort(tracker.sessionID, tracker)
		if previous != nil && previous != s.conn {
			previous.Disconnect(true)
		}
		s.emitAttachError(err.Error())
		return
	}

	s.mu.Lock()
	if s.tracker != nil {
		s.mu.Unlock()
		s.manager.detach(tracker.sessionID, tracker, generation)
		s.emitAttachError("viewer socket already has a session")
		return
	}
	s.tracker = tracker
	s.generation = generation
	s.sessionID = tracker.sessionID
	s.mu.Unlock()

	if err := s.conn.Emit(SocketEventNameViewerSessionAttached, ViewerSessionAttachResponse{
		SessionID: tracker.sessionID,
		Resumed:   resumed,
		Capabilities: ViewerSessionCapabilities{
			DataStreams: tracker.dataStreams != nil,
		},
	}); err != nil {
		if resumed {
			s.manager.detach(tracker.sessionID, tracker, generation)
		} else {
			tracker.detachSocket(generation)
			s.manager.abort(tracker.sessionID, tracker)
		}
		return
	}
	if tracker.finishAttach() {
		s.manager.markAttached(tracker.sessionID, tracker)
	}
	if previous != nil && previous != s.conn {
		previous.Disconnect(true)
	}
}

func (s *viewerSessionSocket) emitAttachError(message string) {
	_ = s.conn.Emit(SocketEventNameViewerSessionAttached, ViewerSessionAttachResponse{
		Error: message,
	})
}

func (s *viewerSessionSocket) onClose(params ...any) {
	var message ViewerSessionCloseMessage
	if len(params) == 0 || mapstructure.Decode(params[0], &message) != nil {
		return
	}
	if message.SessionID == "" || len(message.SessionID) > 64 {
		return
	}
	identity, err := s.identityLookup()
	if err != nil {
		return
	}
	if err := s.manager.closeSession(message.SessionID, identity); err != nil {
		return
	}
	s.conn.Disconnect(true)
}

func (s *viewerSessionSocket) onDisconnect(...any) {
	s.mu.Lock()
	tracker := s.tracker
	generation := s.generation
	sessionID := s.sessionID
	s.mu.Unlock()
	if tracker != nil {
		s.manager.detach(sessionID, tracker, generation)
	}
}

func (st *socketTracker) registerOperationalHandlers(
	conn *socket.Socket,
	generation uint64,
) error {
	register := func(name string, handler func(...any)) error {
		return conn.On(name, func(params ...any) {
			if !st.isCurrentAttachment(conn, generation) {
				return
			}
			handler(params...)
		})
	}
	if st.sessionManager == nil {
		if err := conn.On("disconnect", func(...any) {
			if st.detachSocket(generation) {
				st.destroySession()
			}
		}); err != nil {
			return err
		}
	}
	if err := register(SocketEventNameStoreSubscription, st.onStoreSubscription); err != nil {
		return err
	}
	if err := register(SocketEventNameKeyedEventSubscription, st.onKeyedEventSubscription); err != nil {
		return err
	}
	if err := register(SocketEventNameDataStreamSubscription, st.onDataStreamSubscription); err != nil {
		return err
	}
	if err := register(SocketEventNameRPCCall, st.onRPCCall); err != nil {
		return err
	}
	return register(SocketEventNameFFRPCCall, st.onFFRPCCall)
}

func (st *socketTracker) attachSocket(conn *socket.Socket) (uint64, *socket.Socket) {
	st.attachmentMutex.Lock()
	previous := st.conn
	st.attachmentGeneration++
	generation := st.attachmentGeneration
	st.conn = conn
	st.attachmentReady = false
	st.attachmentMutex.Unlock()
	return generation, previous
}

func (st *socketTracker) detachSocket(generation uint64) bool {
	st.attachmentMutex.Lock()
	defer st.attachmentMutex.Unlock()
	if generation != st.attachmentGeneration || st.conn == nil {
		return false
	}
	st.conn = nil
	st.attachmentReady = false
	return true
}

func (st *socketTracker) isCurrentAttachment(conn *socket.Socket, generation uint64) bool {
	st.attachmentMutex.RLock()
	defer st.attachmentMutex.RUnlock()
	return st.conn == conn && st.attachmentGeneration == generation
}

func (st *socketTracker) isCompatibleSessionConfig(config socketTrackerConfig) bool {
	return st.sr == config.sr &&
		st.ed == config.ed &&
		st.dataStreams == config.dataStreams &&
		st.storeMode == config.storeMode
}

func (st *socketTracker) updateSessionConfig(
	config socketTrackerConfig,
	identity ViewerSessionIdentity,
) {
	st.accessMutex.Lock()
	st.accessLookup = config.accessLookup
	st.rpch = config.rpch
	st.sessionIdentity = identity
	st.accessMutex.Unlock()
}

func (st *socketTracker) finishAttach() bool {
	st.deliveryMutex.Lock()
	defer st.deliveryMutex.Unlock()

	st.attachmentMutex.Lock()
	if st.conn == nil {
		st.attachmentMutex.Unlock()
		return false
	}
	st.sessionBufferMutex.Lock()
	storeUpdates := st.sessionStoreUpdates
	sessionEvents := st.sessionEvents
	st.sessionStoreUpdates = map[string]*bufferedViewerStoreUpdate{}
	st.sessionStoreUpdateBytes = 0
	st.sessionEvents = nil
	st.sessionEventBytes = 0
	st.attachmentReady = true
	st.sessionBufferMutex.Unlock()
	st.attachmentMutex.Unlock()

	storeNames := make([]string, 0, len(storeUpdates))
	for storeName := range storeUpdates {
		storeNames = append(storeNames, storeName)
	}
	sort.Strings(storeNames)
	for _, storeName := range storeNames {
		update := storeUpdates[storeName]
		message, err := buildBufferedStoreUpdateMessage(storeName, update)
		if err != nil {
			st.abortSessionForError("build resumed store update", err)
			return false
		}
		if message.Name != "" && !st.enqueueAttachedMessage(message) {
			return false
		}
	}
	for _, message := range sessionEvents {
		if !st.enqueueAttachedMessage(message) {
			return false
		}
	}

	st.subscriptionMutex.RLock()
	endpoints := make([]DataStreamEndpointMessage, 0, len(st.dataStreamSubscriptions))
	for subscriptionID, tracked := range st.dataStreamSubscriptions {
		if tracked.endpoint == nil {
			continue
		}
		endpoint := *tracked.endpoint
		endpoints = append(endpoints, DataStreamEndpointMessage{
			SubscriptionID: subscriptionID,
			Endpoint:       &endpoint,
		})
	}
	st.subscriptionMutex.RUnlock()
	sort.Slice(endpoints, func(i, j int) bool {
		return endpoints[i].SubscriptionID < endpoints[j].SubscriptionID
	})
	for _, endpoint := range endpoints {
		if !st.enqueueAttachedMessage(emitMessage{
			Name:    SocketEventNameDataStreamEndpoint,
			Message: endpoint,
		}) {
			return false
		}
	}
	return true
}

func buildBufferedStoreUpdateMessage(
	storeName string,
	update *bufferedViewerStoreUpdate,
) (emitMessage, error) {
	var (
		message emitMessage
		err     error
	)
	if update.fullState != nil {
		message, err = buildFullStoreUpdateMessage(storeName, update.fullState)
	} else if update.partial != nil {
		message, err = buildPartialStoreUpdateMessage(storeName, update.partial)
	}
	return message, err
}

func (st *socketTracker) abortSessionForError(operation string, err error) {
	if st.log != nil {
		st.log.WithError(err).Warn(operation)
	}
	if st.sessionManager != nil {
		go st.sessionManager.abort(st.sessionID, st)
	} else {
		go st.destroySession()
	}
	st.disconnect()
}

func (s *socketTracker) destroySession() {
	s.destroyOnce.Do(func() {
		s.attachLifecycleMutex.Lock()
		defer s.attachLifecycleMutex.Unlock()
		s.cleanupDisconnect()
	})
}

func (s *socketTracker) onDisconnect(...any) {
	s.destroySession()
}

func (s *socketTracker) cleanupDisconnect() {
	s.attachmentMutex.Lock()
	conn := s.conn
	s.conn = nil
	s.attachmentReady = false
	s.attachmentMutex.Unlock()
	if conn != nil {
		conn.Disconnect(true)
	}
	if s.cancel != nil {
		s.cancel()
	}
	s.emitQueueMutex.Lock()
	if s.emitQueue != nil {
		close(s.emitQueue)
		s.emitQueue = nil
	}
	s.emitQueueMutex.Unlock()

	if s.sr != nil {
		s.sr.UnsubscribeFromPartialApplies(s.partialApplySubID)     //nolint:errcheck // Why: Best effort
		s.sr.UnsubscribeFromFullStateApplies(s.fullStateApplySubID) //nolint:errcheck // Why: Best effort
	}
	if s.ed != nil {
		s.ed.UnsubscribeFromEvents(s.eventSubID)           //nolint:errcheck // Why: Best effort
		s.ed.UnsubscribeFromKeyedEvents(s.keyedEventSubID) //nolint:errcheck // Why: Best effort
	}

	s.subscriptionMutex.Lock()
	storeSubs := lo.MapValues(s.storeSubscriptions, func(subs map[string]int, _ string) map[string]int {
		return lo.Assign(map[string]int{}, subs)
	})
	keyedEventSubs := lo.Assign(map[KeyedEventSubscription]int{}, s.keyedEventSubscriptions)
	dataStreamSubs := lo.Assign(
		map[string]trackedDataStreamSubscription{},
		s.dataStreamSubscriptions,
	)
	s.storeSubscriptions = map[string]map[string]int{}
	s.keyedEventSubscriptions = map[KeyedEventSubscription]int{}
	s.dataStreamSubscriptions = map[string]trackedDataStreamSubscription{}
	s.storeSubscriptionCount = 0
	s.keyedEventSubscriptionCount = 0
	s.subscriptionMutex.Unlock()
	s.storeUpdateQueueMutex.Lock()
	s.pendingStoreCatchups = nil
	s.bufferedStoreUpdates = nil
	s.bufferedStoreUpdateCount = 0
	s.storeUpdateQueueMutex.Unlock()
	for storeName, keySubs := range storeSubs {
		for key := range keySubs {
			if s.sr != nil {
				if err := s.sr.StopListeningToStoreKey(storeName, key); err != nil {
					s.log.Errorf("Error StopListeningToStoreKey to %s/%s -- possible double unsubscribe? Reason: %+v", storeName, key, err)
				}
			}
		}
	}
	for subscription, count := range keyedEventSubs {
		if count == 0 || s.ed == nil {
			continue
		}
		if err := s.ed.StopListeningToKeyedEvent(
			subscription.StoreName,
			subscription.EventName,
			subscription.Key,
		); err != nil {
			s.log.Errorf(
				"Error StopListeningToKeyedEvent to %s/%s/%s during disconnect: %+v",
				subscription.StoreName,
				subscription.EventName,
				subscription.Key,
				err,
			)
		}
	}
	sessionBroker, hasSessionBroker := s.dataStreams.(SessionDataStreamBroker)
	for _, tracked := range dataStreamSubs {
		if tracked.cancel != nil {
			tracked.cancel()
		}
		if hasSessionBroker || s.dataStreams == nil || tracked.endpoint == nil {
			continue
		}
		broker := s.dataStreams
		endpoint := *tracked.endpoint
		log := s.log
		go func() {
			closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			err := broker.Close(closeCtx, endpoint)
			cancel()
			if err != nil && log != nil {
				log.Errorf(
					"Error closing data stream lease %s during disconnect: %+v",
					endpoint.LeaseID,
					err,
				)
			}
		}()
	}
	if hasSessionBroker && s.sessionID != "" {
		sessionID := s.sessionID
		log := s.log
		go func() {
			closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			err := sessionBroker.CloseSession(closeCtx, sessionID)
			cancel()
			if err != nil && log != nil {
				log.Errorf("Error closing data stream session %s: %+v", sessionID, err)
			}
		}()
	}
}

// handleEmitQueue is a helper to fork a goroutine to handle emitting messages through the websocket
func (st *socketTracker) handleEmitQueue() {
	go func() {
		for {
			st.emitQueueMutex.RLock()
			ch := st.emitQueue
			st.emitQueueMutex.RUnlock()
			if ch == nil {
				return
			}
			msg, ok := <-ch
			if !ok {
				// Channel closed
				return
			}
			msg, err := msg.resolve()
			if err != nil {
				st.abortSessionForError("build viewer emit message", err)
				continue
			}
			st.attachmentMutex.RLock()
			conn := st.conn
			ready := st.attachmentReady
			generation := st.attachmentGeneration
			st.attachmentMutex.RUnlock()
			if conn == nil || !ready {
				if err := st.bufferDetachedMessage(msg); err != nil {
					st.abortSessionForError("buffer detached viewer message", err)
				}
				continue
			}
			err = conn.Emit(msg.Name, msg.Message)
			if err != nil {
				if st.log != nil {
					st.log.Warnf("Error emitting message: %+v", err)
				}
				if bufferErr := st.bufferDetachedMessage(msg); bufferErr != nil {
					st.abortSessionForError("buffer failed viewer emit", bufferErr)
					continue
				}
				if st.sessionManager != nil {
					st.sessionManager.detach(st.sessionID, st, generation)
				} else {
					st.destroySession()
				}
				conn.Disconnect(true)
			}
		}
	}()
}

// emitMessage adds a single message to emit to the emit queue
func (st *socketTracker) emitMessage(name string, arg any) {
	st.queueEmitMessage(emitMessage{Name: name, Message: arg})
}

func (st *socketTracker) queueEmitMessage(msg emitMessage) {
	st.deliveryMutex.Lock()
	defer st.deliveryMutex.Unlock()

	st.attachmentMutex.RLock()
	ready := st.conn != nil && st.attachmentReady
	st.attachmentMutex.RUnlock()
	if !ready && st.sessionManager != nil {
		resolved, err := msg.resolve()
		if err == nil {
			err = st.bufferDetachedMessage(resolved)
		}
		if err != nil {
			st.abortSessionForError("buffer detached viewer message", err)
		}
		return
	}
	st.enqueueAttachedMessage(msg)
}

func (st *socketTracker) enqueueAttachedMessage(msg emitMessage) bool {
	st.emitQueueMutex.Lock()
	if st.emitQueue == nil {
		st.emitQueueMutex.Unlock()
		return false
	}
	select {
	case st.emitQueue <- msg:
		st.emitQueueMutex.Unlock()
		return true
	default:
		queueCapacity := cap(st.emitQueue)
		// Overflow always disconnects this client, so drain the doomed queue for
		// diagnostics and close it before allowing another producer to enqueue.
		queuedCount, queuedSummary := drainEmitQueueSummary(st.emitQueue)
		close(st.emitQueue)
		st.emitQueue = nil
		st.emitQueueMutex.Unlock()
		if st.log != nil {
			st.log.Warnf(
				"Disconnecting websocket client with full emit queue while sending %s; queued messages (%d/%d): %s",
				describeEmitMessage(msg),
				queuedCount,
				queueCapacity,
				queuedSummary,
			)
		}
		// A queue overflow can be detected while the caller holds another
		// socketTracker mutex (for example while ordering a catch-up snapshot).
		// Disconnect asynchronously so cleanup cannot re-enter that mutex.
		if st.sessionManager != nil {
			go st.sessionManager.abort(st.sessionID, st)
		} else {
			go st.destroySession()
		}
		go st.disconnect()
		return false
	}
}

func (st *socketTracker) bufferDetachedMessage(msg emitMessage) error {
	st.sessionBufferMutex.Lock()
	defer st.sessionBufferMutex.Unlock()

	if msg.StoreName != "" && (msg.StoreFull != nil || msg.StorePartial != nil) {
		return st.bufferDetachedStoreUpdateLocked(msg)
	}
	if endpoint, ok := msg.Message.(DataStreamEndpointMessage); ok && endpoint.Endpoint != nil {
		// Successful endpoints are retained in dataStreamSubscriptions and sent
		// once from finishAttach. Only transient endpoint errors need ordered
		// buffering here.
		return nil
	}
	if len(st.sessionEvents) >= st.limits.MaxBufferedSessionEvents {
		return fmt.Errorf(
			"detached viewer event limit exceeded (%d)",
			st.limits.MaxBufferedSessionEvents,
		)
	}
	messageBytes := estimateEmitMessageBytes(msg)
	if messageBytes > st.limits.MaxBufferedSessionBytes-st.sessionEventBytes {
		return fmt.Errorf(
			"detached viewer event byte limit exceeded (%d)",
			st.limits.MaxBufferedSessionBytes,
		)
	}
	st.sessionEvents = append(st.sessionEvents, msg)
	st.sessionEventBytes += messageBytes
	return nil
}

func (st *socketTracker) bufferDetachedStoreUpdateLocked(msg emitMessage) error {
	update := st.sessionStoreUpdates[msg.StoreName]
	if update == nil {
		if len(st.sessionStoreUpdates) >= st.limits.MaxBufferedSessionStores {
			return fmt.Errorf(
				"detached viewer store limit exceeded (%d)",
				st.limits.MaxBufferedSessionStores,
			)
		}
		update = &bufferedViewerStoreUpdate{}
		st.sessionStoreUpdates[msg.StoreName] = update
	}
	st.sessionStoreUpdateBytes -= update.bytes

	storePartial := msg.StorePartial
	if storePartial != nil {
		snapshot, err := ClonePartial(storePartial)
		if err != nil {
			return err
		}
		storePartial = snapshot
	}

	switch {
	case msg.StoreFull != nil:
		update.fullState = msg.StoreFull
		update.partial = nil
	case storePartial != nil && update.fullState != nil:
		storePartial.ApplyTo(update.fullState)
	case storePartial != nil && update.partial != nil:
		storePartial.MergeOntoPartial(update.partial)
	case storePartial != nil:
		update.partial = storePartial
	}

	var retained Serializable
	if update.fullState != nil {
		retained = update.fullState
	} else {
		retained = update.partial
	}
	retainedBytes, err := SerializeToBytes(retained, nil)
	if err != nil {
		return err
	}
	update.bytes = len(retainedBytes)
	if update.bytes > st.limits.MaxBufferedSessionStoreBytes-st.sessionStoreUpdateBytes {
		return fmt.Errorf(
			"detached viewer store byte limit exceeded (%d)",
			st.limits.MaxBufferedSessionStoreBytes,
		)
	}
	st.sessionStoreUpdateBytes += update.bytes
	return nil
}

func estimateEmitMessageBytes(msg emitMessage) int {
	if msg.Bytes > 0 {
		return msg.Bytes
	}
	switch message := msg.Message.(type) {
	case EventMessage:
		return len(message.EventName) + message.Event.Len()
	case KeyedEventMessage:
		return len(message.StoreName) + len(message.EventName) + len(message.Key) + message.Event.Len()
	case RPCCallResponseMessage:
		size := 16
		if message.Response != nil {
			size += message.Response.Len()
		}
		if message.Error != nil {
			size += len(message.Error.Message)
		}
		return size
	case DataStreamEndpointMessage:
		size := len(message.SubscriptionID) + len(message.Error)
		if message.Endpoint != nil {
			size += len(message.Endpoint.LeaseID) + len(message.Endpoint.URL) + len(message.Endpoint.Token)
			for key, value := range message.Endpoint.Metadata {
				size += len(key) + len(value)
			}
		}
		return size
	default:
		return 256
	}
}

func drainEmitQueueSummary(queue chan emitMessage) (int, string) {
	counts := map[string]int{}
	total := 0
	for {
		select {
		case msg := <-queue:
			counts[describeEmitMessage(msg)]++
			total++
		default:
			keys := make([]string, 0, len(counts))
			for key := range counts {
				keys = append(keys, key)
			}
			sort.Strings(keys)

			parts := make([]string, 0, len(keys))
			for _, key := range keys {
				parts = append(parts, fmt.Sprintf("%s: %d", key, counts[key]))
			}
			if len(parts) == 0 {
				return total, "none"
			}
			return total, strings.Join(parts, ", ")
		}
	}
}

func describeEmitMessage(msg emitMessage) string {
	switch message := msg.Message.(type) {
	case StoreUpdateFullMessage:
		return fmt.Sprintf("%s/full store=%q", msg.Name, message.StoreName)
	case *StoreUpdateFullMessage:
		return fmt.Sprintf("%s/full store=%q", msg.Name, message.StoreName)
	case StoreUpdatePartialMessage:
		return fmt.Sprintf("%s/partial store=%q", msg.Name, message.StoreName)
	case *StoreUpdatePartialMessage:
		return fmt.Sprintf("%s/partial store=%q", msg.Name, message.StoreName)
	case KeyedEventMessage:
		return fmt.Sprintf(
			"%s store=%q event=%q key=%q",
			msg.Name,
			message.StoreName,
			message.EventName,
			message.Key,
		)
	case *KeyedEventMessage:
		return fmt.Sprintf(
			"%s store=%q event=%q key=%q",
			msg.Name,
			message.StoreName,
			message.EventName,
			message.Key,
		)
	default:
		return msg.Name
	}
}

func (st *socketTracker) disconnect() {
	st.attachmentMutex.RLock()
	conn := st.conn
	st.attachmentMutex.RUnlock()
	if conn != nil {
		conn.Disconnect(true)
	}
}

func (st *socketTracker) disconnectForLimit(limitName string, limit int) {
	if st.log != nil {
		st.log.Warnf(
			"Disconnecting websocket client after exceeding %s limit (%d)",
			limitName,
			limit,
		)
	}
	if st.sessionManager != nil {
		go st.sessionManager.abort(st.sessionID, st)
	}
	st.disconnect()
}

func (st *socketTracker) lookupAccessLevel() (AccessLevel, error) {
	st.accessMutex.RLock()
	lookup := st.accessLookup
	st.accessMutex.RUnlock()
	if lookup == nil {
		return AccessLevelPublic, nil
	}
	return lookup()
}

func (st *socketTracker) lookupRPCHandler() RPCHandlerFunc {
	st.accessMutex.RLock()
	handler := st.rpch
	st.accessMutex.RUnlock()
	return handler
}

func (st *socketTracker) removeTrackedStoreSubscription(storeName string, key string) {
	st.storeUpdateQueueMutex.Lock()
	defer st.storeUpdateQueueMutex.Unlock()
	st.subscriptionMutex.Lock()
	defer st.subscriptionMutex.Unlock()

	keySubs, exists := st.storeSubscriptions[storeName]
	if !exists || keySubs[key] == 0 {
		return
	}
	keySubs[key]--
	if st.storeSubscriptionCount > 0 {
		st.storeSubscriptionCount--
	}
	if keySubs[key] > 0 {
		return
	}
	delete(keySubs, key)
	if len(keySubs) == 0 {
		delete(st.storeSubscriptions, storeName)
	}
}

func (st *socketTracker) removeTrackedKeyedEventSubscription(subscription KeyedEventSubscription) {
	st.subscriptionMutex.Lock()
	defer st.subscriptionMutex.Unlock()

	if st.keyedEventSubscriptions[subscription] == 0 {
		return
	}
	st.keyedEventSubscriptions[subscription]--
	if st.keyedEventSubscriptionCount > 0 {
		st.keyedEventSubscriptionCount--
	}
	if st.keyedEventSubscriptions[subscription] == 0 {
		delete(st.keyedEventSubscriptions, subscription)
	}
}

// onStoreSubscription is a helper that is called when a store subscription message is received
func (st *socketTracker) onStoreSubscription(params ...any) {
	if len(params) == 0 {
		st.log.Error("Missing store subscription message")
		st.disconnect()
		return
	}
	var subMsg StoreSubscriptionMessage
	if err := mapstructure.Decode(params[0], &subMsg); err != nil {
		st.log.Errorf("Error parsing store subscription message: %+v", err)
		st.disconnect()
		return
	}

	if !st.sr.IsStoreValid(subMsg.StoreName) {
		st.log.Errorf("Client referenced a subscription to an invalid store %s", subMsg.StoreName)
		st.disconnect()
		return
	}
	if len(subMsg.Key) > 4096 {
		st.log.Errorf("Client referenced an oversized store subscription key")
		st.disconnect()
		return
	}

	switch subMsg.Action {
	case Subscribe:
		userAccessLevel, err := st.lookupAccessLevel()
		if err != nil {
			st.log.Errorf("Error looking up user access level: %+v", err)
			st.disconnect()
			return
		}
		if err := st.subscribeStoreKey(subMsg.StoreName, subMsg.Key, userAccessLevel); err != nil {
			st.log.Errorf("Store subscription failed for %s/%s: %+v", subMsg.StoreName, subMsg.Key, err)
			st.disconnect()
			return
		}
	case Unsubscribe:
		if err := st.unsubscribeStoreKey(subMsg.StoreName, subMsg.Key); err != nil {
			st.log.Errorf("Store unsubscription failed for %s/%s: %+v", subMsg.StoreName, subMsg.Key, err)
		}
	default:
		st.log.Errorf("Invalid store subscription action %d", subMsg.Action)
		st.disconnect()
	}
}

func (st *socketTracker) subscribeStoreKey(
	storeName string,
	key string,
	accessLevel AccessLevel,
) error {
	return st.subscribeStoreKeyWithCatchup(storeName, key, accessLevel, true)
}

func (st *socketTracker) subscribeStoreKeyWithCatchup(
	storeName string,
	key string,
	accessLevel AccessLevel,
	catchup bool,
) error {
	if err := st.sr.CheckStoreAccess(storeName, accessLevel); err != nil {
		return err
	}

	st.storeUpdateQueueMutex.Lock()
	st.subscriptionMutex.Lock()
	if st.storeSubscriptionCount >= st.limits.MaxStoreSubscriptions {
		st.subscriptionMutex.Unlock()
		st.storeUpdateQueueMutex.Unlock()
		return fmt.Errorf("store subscription limit exceeded (%d)", st.limits.MaxStoreSubscriptions)
	}
	keySubs := st.storeSubscriptions[storeName]
	firstStore := len(keySubs) == 0
	if keySubs == nil {
		keySubs = map[string]int{}
		st.storeSubscriptions[storeName] = keySubs
	}
	keySubs[key]++
	st.storeSubscriptionCount++
	firstKey := keySubs[key] == 1
	catchupNeeded := firstKey && catchup &&
		(st.storeMode == StoreDeliveryModeKeyed || firstStore)
	if catchupNeeded {
		if st.pendingStoreCatchups == nil {
			st.pendingStoreCatchups = map[string]int{}
		}
		st.pendingStoreCatchups[storeName]++
	}
	st.subscriptionMutex.Unlock()
	st.storeUpdateQueueMutex.Unlock()
	if !firstKey {
		return nil
	}

	if err := st.sr.ListeningToStoreKey(storeName, key, accessLevel); err != nil {
		if catchupNeeded {
			st.cancelSubscriptionCatchup(storeName)
		}
		st.removeTrackedStoreSubscription(storeName, key)
		return err
	}
	if catchupNeeded {
		if err := st.emitSubscriptionCatchup(storeName, key, accessLevel); err != nil {
			st.cancelSubscriptionCatchup(storeName)
			st.removeTrackedStoreSubscription(storeName, key)
			return errors.Join(err, st.sr.StopListeningToStoreKey(storeName, key))
		}
	}
	return nil
}

func (st *socketTracker) unsubscribeStoreKey(storeName string, key string) error {
	st.storeUpdateQueueMutex.Lock()
	st.subscriptionMutex.Lock()
	keySubs, exists := st.storeSubscriptions[storeName]
	if !exists || keySubs[key] == 0 {
		st.subscriptionMutex.Unlock()
		st.storeUpdateQueueMutex.Unlock()
		return nil
	}
	keySubs[key]--
	if st.storeSubscriptionCount > 0 {
		st.storeSubscriptionCount--
	}
	last := keySubs[key] == 0
	if last {
		delete(keySubs, key)
		if len(keySubs) == 0 {
			delete(st.storeSubscriptions, storeName)
		}
	}
	st.subscriptionMutex.Unlock()
	st.storeUpdateQueueMutex.Unlock()
	if !last {
		return nil
	}
	return st.sr.StopListeningToStoreKey(storeName, key)
}

// onKeyedEventSubscription handles an exact store/event/key subscription lifecycle message.
func (st *socketTracker) onKeyedEventSubscription(params ...any) {
	var subMsg KeyedEventSubscriptionMessage
	if len(params) == 0 {
		st.log.Error("Missing keyed event subscription message")
		st.disconnect()
		return
	}
	if err := mapstructure.Decode(params[0], &subMsg); err != nil {
		st.log.Errorf("Error parsing keyed event subscription message: %+v", err)
		st.disconnect()
		return
	}
	if st.ed == nil || st.sr == nil {
		st.log.Error("Keyed event subscription received without an event dispatcher or store registry")
		st.disconnect()
		return
	}
	if !st.sr.IsStoreValid(subMsg.StoreName) {
		st.log.Errorf("Client referenced a keyed event subscription for invalid store %s", subMsg.StoreName)
		st.disconnect()
		return
	}
	if subMsg.EventName == "" || subMsg.Key == "" ||
		len(subMsg.EventName) > 256 || len(subMsg.Key) > 4096 {
		st.log.Errorf(
			"Client referenced an invalid keyed event subscription for %s/%s/%s",
			subMsg.StoreName,
			subMsg.EventName,
			subMsg.Key,
		)
		st.disconnect()
		return
	}

	subscription := KeyedEventSubscription{
		StoreName: subMsg.StoreName,
		EventName: subMsg.EventName,
		Key:       subMsg.Key,
	}

	switch subMsg.Action {
	case Subscribe:
		userAccessLevel, err := st.lookupAccessLevel()
		if err != nil {
			st.log.Errorf("Error looking up user access level: %+v", err)
			st.disconnect()
			return
		}
		if err := st.subscribeKeyedEvent(subscription, userAccessLevel); err != nil {
			st.log.Errorf("Keyed event subscription failed for %s/%s/%s: %+v",
				subMsg.StoreName, subMsg.EventName, subMsg.Key, err)
			st.disconnect()
		}
	case Unsubscribe:
		if err := st.unsubscribeKeyedEvent(subscription); err != nil {
			st.log.Errorf("Keyed event unsubscription failed for %s/%s/%s: %+v",
				subMsg.StoreName, subMsg.EventName, subMsg.Key, err)
		}
	default:
		st.log.Errorf("Invalid keyed event subscription action %d", subMsg.Action)
		st.disconnect()
	}
}

func (st *socketTracker) subscribeKeyedEvent(
	subscription KeyedEventSubscription,
	accessLevel AccessLevel,
) error {
	if err := st.sr.CheckStoreAccess(subscription.StoreName, accessLevel); err != nil {
		return err
	}
	st.subscriptionMutex.Lock()
	if st.keyedEventSubscriptionCount >= st.limits.MaxKeyedEventSubscriptions {
		st.subscriptionMutex.Unlock()
		return fmt.Errorf(
			"keyed event subscription limit exceeded (%d)",
			st.limits.MaxKeyedEventSubscriptions,
		)
	}
	st.keyedEventSubscriptions[subscription]++
	st.keyedEventSubscriptionCount++
	first := st.keyedEventSubscriptions[subscription] == 1
	st.subscriptionMutex.Unlock()
	if !first {
		return nil
	}
	if err := st.ed.ListeningToKeyedEvent(
		subscription.StoreName,
		subscription.EventName,
		subscription.Key,
	); err != nil {
		st.removeTrackedKeyedEventSubscription(subscription)
		return err
	}
	return nil
}

func (st *socketTracker) unsubscribeKeyedEvent(subscription KeyedEventSubscription) error {
	st.subscriptionMutex.Lock()
	count := st.keyedEventSubscriptions[subscription]
	if count == 0 {
		st.subscriptionMutex.Unlock()
		return nil
	}
	count--
	if st.keyedEventSubscriptionCount > 0 {
		st.keyedEventSubscriptionCount--
	}
	last := count == 0
	if last {
		delete(st.keyedEventSubscriptions, subscription)
	} else {
		st.keyedEventSubscriptions[subscription] = count
	}
	st.subscriptionMutex.Unlock()
	if !last {
		return nil
	}
	return st.ed.StopListeningToKeyedEvent(
		subscription.StoreName,
		subscription.EventName,
		subscription.Key,
	)
}

type viewerSessionStoreSubscriptionKey struct {
	storeName string
	key       string
}

func (st *socketTracker) reconcileSessionManifest(
	request ViewerSessionAttachRequest,
	accessLevel AccessLevel,
) error {
	manifestBytes := len(request.SessionID)
	for _, subscription := range request.StoreSubscriptions {
		manifestBytes += len(subscription.StoreName) + len(subscription.Key)
	}
	for _, subscription := range request.KeyedEventSubscriptions {
		manifestBytes += len(subscription.StoreName) +
			len(subscription.EventName) +
			len(subscription.Key)
	}
	for _, subscription := range request.DataStreamSubscriptions {
		manifestBytes += len(subscription.SubscriptionID) +
			len(subscription.StoreName) +
			len(subscription.StreamName) +
			len(subscription.Key)
	}
	if manifestBytes > st.limits.MaxSessionManifestBytes {
		return fmt.Errorf(
			"viewer session manifest byte limit exceeded (%d)",
			st.limits.MaxSessionManifestBytes,
		)
	}
	if len(request.StoreSubscriptions) > st.limits.MaxStoreSubscriptions {
		return fmt.Errorf("store subscription limit exceeded (%d)", st.limits.MaxStoreSubscriptions)
	}
	if len(request.KeyedEventSubscriptions) > st.limits.MaxKeyedEventSubscriptions {
		return fmt.Errorf(
			"keyed event subscription limit exceeded (%d)",
			st.limits.MaxKeyedEventSubscriptions,
		)
	}
	if len(request.DataStreamSubscriptions) > st.limits.MaxDataStreamSubscriptions {
		return fmt.Errorf(
			"data stream subscription limit exceeded (%d)",
			st.limits.MaxDataStreamSubscriptions,
		)
	}

	desiredStores := map[viewerSessionStoreSubscriptionKey]struct{}{}
	for _, subscription := range request.StoreSubscriptions {
		if len(subscription.StoreName) > maxSocketMethodNameBytes ||
			st.sr == nil || !st.sr.IsStoreValid(subscription.StoreName) {
			return fmt.Errorf("invalid store subscription %q", subscription.StoreName)
		}
		if len(subscription.Key) > 4096 {
			return errors.New("oversized store subscription key")
		}
		if err := st.sr.CheckStoreAccess(subscription.StoreName, accessLevel); err != nil {
			return err
		}
		desiredStores[viewerSessionStoreSubscriptionKey{
			storeName: subscription.StoreName,
			key:       subscription.Key,
		}] = struct{}{}
	}

	desiredKeyedEvents := map[KeyedEventSubscription]struct{}{}
	for _, subscription := range request.KeyedEventSubscriptions {
		keyed := KeyedEventSubscription{
			StoreName: subscription.StoreName,
			EventName: subscription.EventName,
			Key:       subscription.Key,
		}
		if st.ed == nil || st.sr == nil || !st.sr.IsStoreValid(keyed.StoreName) ||
			keyed.EventName == "" || keyed.Key == "" ||
			len(keyed.EventName) > 256 || len(keyed.Key) > 4096 {
			return fmt.Errorf(
				"invalid keyed event subscription %q/%q/%q",
				keyed.StoreName,
				keyed.EventName,
				keyed.Key,
			)
		}
		if err := st.sr.CheckStoreAccess(keyed.StoreName, accessLevel); err != nil {
			return err
		}
		desiredKeyedEvents[keyed] = struct{}{}
	}

	desiredDataStreams := map[string]DataStreamSubscription{}
	for _, subscription := range request.DataStreamSubscriptions {
		if st.dataStreams == nil || st.sr == nil {
			return errors.New("data streaming is not available")
		}
		if subscription.SubscriptionID == "" || len(subscription.SubscriptionID) > 256 {
			return errors.New("invalid data stream subscription ID")
		}
		stream := DataStreamSubscription{
			StoreName:  subscription.StoreName,
			StreamName: subscription.StreamName,
			Key:        subscription.Key,
		}
		if err := stream.Validate(); err != nil {
			return err
		}
		if err := st.sr.CheckStoreAccess(stream.StoreName, accessLevel); err != nil {
			return err
		}
		if existing, duplicate := desiredDataStreams[subscription.SubscriptionID]; duplicate &&
			existing != stream {
			return fmt.Errorf(
				"data stream subscription ID %q is duplicated",
				subscription.SubscriptionID,
			)
		}
		desiredDataStreams[subscription.SubscriptionID] = stream
	}

	st.subscriptionMutex.RLock()
	currentStores := map[viewerSessionStoreSubscriptionKey]struct{}{}
	for storeName, keys := range st.storeSubscriptions {
		for key, count := range keys {
			if count > 0 {
				currentStores[viewerSessionStoreSubscriptionKey{
					storeName: storeName,
					key:       key,
				}] = struct{}{}
			}
		}
	}
	currentKeyedEvents := map[KeyedEventSubscription]struct{}{}
	for subscription, count := range st.keyedEventSubscriptions {
		if count > 0 {
			currentKeyedEvents[subscription] = struct{}{}
		}
	}
	currentDataStreams := map[string]DataStreamSubscription{}
	for subscriptionID, tracked := range st.dataStreamSubscriptions {
		currentDataStreams[subscriptionID] = tracked.subscription
	}
	st.subscriptionMutex.RUnlock()

	changedStores := map[string]struct{}{}
	for subscription := range currentStores {
		if _, retained := desiredStores[subscription]; retained {
			continue
		}
		changedStores[subscription.storeName] = struct{}{}
	}
	for subscription := range desiredStores {
		if _, retained := currentStores[subscription]; retained {
			continue
		}
		changedStores[subscription.storeName] = struct{}{}
	}
	if len(changedStores) > 0 {
		st.sessionBufferMutex.Lock()
		for storeName := range changedStores {
			if update := st.sessionStoreUpdates[storeName]; update != nil {
				st.sessionStoreUpdateBytes -= update.bytes
				delete(st.sessionStoreUpdates, storeName)
			}
		}
		st.sessionBufferMutex.Unlock()
	}
	for subscription := range currentStores {
		if _, retained := desiredStores[subscription]; retained {
			continue
		}
		if err := st.unsubscribeStoreKey(subscription.storeName, subscription.key); err != nil {
			return err
		}
	}
	for subscription := range desiredStores {
		if _, retained := currentStores[subscription]; retained {
			continue
		}
		if err := st.subscribeStoreKeyWithCatchup(
			subscription.storeName,
			subscription.key,
			accessLevel,
			false,
		); err != nil {
			return err
		}
	}
	for storeName := range changedStores {
		keys := make([]string, 0)
		for subscription := range desiredStores {
			if subscription.storeName == storeName {
				keys = append(keys, subscription.key)
			}
		}
		sort.Strings(keys)
		if err := st.queueSessionManifestBaseline(storeName, keys, accessLevel); err != nil {
			return err
		}
	}

	for subscription := range currentKeyedEvents {
		if _, retained := desiredKeyedEvents[subscription]; retained {
			continue
		}
		if err := st.unsubscribeKeyedEvent(subscription); err != nil {
			return err
		}
	}
	for subscription := range desiredKeyedEvents {
		if _, retained := currentKeyedEvents[subscription]; retained {
			continue
		}
		if err := st.subscribeKeyedEvent(subscription, accessLevel); err != nil {
			return err
		}
	}
	st.dropUnsubscribedBufferedKeyedEvents(desiredKeyedEvents)

	for subscriptionID, subscription := range currentDataStreams {
		desired, retained := desiredDataStreams[subscriptionID]
		if retained && desired == subscription {
			continue
		}
		st.closeDataStream(subscriptionID)
	}
	for subscriptionID, subscription := range desiredDataStreams {
		current, retained := currentDataStreams[subscriptionID]
		if retained && current == subscription {
			continue
		}
		st.openDataStream(subscriptionID, subscription)
	}
	return nil
}

func (st *socketTracker) dropUnsubscribedBufferedKeyedEvents(
	desired map[KeyedEventSubscription]struct{},
) {
	st.sessionBufferMutex.Lock()
	defer st.sessionBufferMutex.Unlock()
	retained := st.sessionEvents[:0]
	retainedBytes := 0
	for _, message := range st.sessionEvents {
		keyedMessage, ok := message.Message.(KeyedEventMessage)
		if ok {
			subscription := KeyedEventSubscription{
				StoreName: keyedMessage.StoreName,
				EventName: keyedMessage.EventName,
				Key:       keyedMessage.Key,
			}
			if _, subscribed := desired[subscription]; !subscribed {
				continue
			}
		}
		retained = append(retained, message)
		retainedBytes += estimateEmitMessageBytes(message)
	}
	st.sessionEvents = retained
	st.sessionEventBytes = retainedBytes
}

func (st *socketTracker) onDataStreamSubscription(params ...any) {
	var subMsg DataStreamSubscriptionMessage
	if len(params) == 0 {
		st.emitDataStreamError("", "missing data stream subscription message")
		return
	}
	if err := mapstructure.Decode(params[0], &subMsg); err != nil {
		st.emitDataStreamError(subMsg.SubscriptionID, "invalid data stream subscription message")
		return
	}
	if st.dataStreams == nil || st.sr == nil {
		st.emitDataStreamError(subMsg.SubscriptionID, "data streaming is not available")
		return
	}
	if strings.TrimSpace(subMsg.SubscriptionID) == "" || len(subMsg.SubscriptionID) > 256 {
		st.emitDataStreamError(subMsg.SubscriptionID, "invalid data stream subscription ID")
		return
	}

	subscription := DataStreamSubscription{
		StoreName:  subMsg.StoreName,
		StreamName: subMsg.StreamName,
		Key:        subMsg.Key,
	}
	if err := subscription.Validate(); err != nil {
		st.emitDataStreamError(subMsg.SubscriptionID, err.Error())
		return
	}

	switch subMsg.Action {
	case Subscribe:
		st.openDataStream(subMsg.SubscriptionID, subscription)
	case Unsubscribe:
		st.closeDataStream(subMsg.SubscriptionID)
	default:
		st.emitDataStreamError(subMsg.SubscriptionID, "invalid data stream subscription action")
	}
}

func (st *socketTracker) openDataStream(
	subscriptionID string,
	subscription DataStreamSubscription,
) {
	userAccessLevel, err := st.lookupAccessLevel()
	if err != nil {
		st.emitDataStreamError(subscriptionID, "could not verify stream access")
		return
	}
	if err := st.sr.CheckStoreAccess(subscription.StoreName, userAccessLevel); err != nil {
		st.emitDataStreamError(subscriptionID, "data stream access denied")
		return
	}

	st.subscriptionMutex.Lock()
	if _, exists := st.dataStreamSubscriptions[subscriptionID]; exists {
		st.subscriptionMutex.Unlock()
		st.emitDataStreamError(subscriptionID, "data stream subscription ID is already active")
		return
	}
	if len(st.dataStreamSubscriptions) >= st.limits.MaxDataStreamSubscriptions {
		st.subscriptionMutex.Unlock()
		st.disconnectForLimit(
			"data stream subscriptions",
			st.limits.MaxDataStreamSubscriptions,
		)
		return
	}
	openCtx, cancel := context.WithCancel(st.lifetimeCtx)
	st.dataStreamSubscriptions[subscriptionID] = trackedDataStreamSubscription{
		subscription: subscription,
		cancel:       cancel,
	}
	st.subscriptionMutex.Unlock()

	go func() {
		defer cancel()
		var endpoint DataStreamEndpoint
		var err error
		if sessionBroker, ok := st.dataStreams.(SessionDataStreamBroker); ok && st.sessionID != "" {
			endpoint, err = sessionBroker.OpenForSession(
				openCtx,
				st.sessionID,
				subscription,
				userAccessLevel,
			)
		} else {
			endpoint, err = st.dataStreams.Open(openCtx, subscription, userAccessLevel)
		}
		if err != nil {
			st.subscriptionMutex.Lock()
			tracked, active := st.dataStreamSubscriptions[subscriptionID]
			accepted := active && tracked.subscription == subscription
			if accepted {
				delete(st.dataStreamSubscriptions, subscriptionID)
			}
			st.subscriptionMutex.Unlock()
			if accepted && !errors.Is(err, context.Canceled) {
				st.emitDataStreamError(subscriptionID, err.Error())
			}
			return
		}

		st.subscriptionMutex.Lock()
		tracked, active := st.dataStreamSubscriptions[subscriptionID]
		accepted := active && tracked.subscription == subscription
		if accepted {
			tracked.endpoint = &endpoint
			st.dataStreamSubscriptions[subscriptionID] = tracked
		}
		st.subscriptionMutex.Unlock()
		if !accepted {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
			_ = st.dataStreams.Close(closeCtx, endpoint)
			closeCancel()
			return
		}
		st.emitMessage(SocketEventNameDataStreamEndpoint, DataStreamEndpointMessage{
			SubscriptionID: subscriptionID,
			Endpoint:       &endpoint,
		})
	}()
}

func (st *socketTracker) closeDataStream(subscriptionID string) {
	st.subscriptionMutex.Lock()
	tracked, active := st.dataStreamSubscriptions[subscriptionID]
	if active {
		delete(st.dataStreamSubscriptions, subscriptionID)
	}
	st.subscriptionMutex.Unlock()
	if active && tracked.cancel != nil {
		tracked.cancel()
	}
	if !active || tracked.endpoint == nil {
		return
	}
	go func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := st.dataStreams.Close(closeCtx, *tracked.endpoint)
		cancel()
		if err != nil && st.log != nil {
			st.log.Errorf("Error closing data stream lease %s: %+v", tracked.endpoint.LeaseID, err)
		}
	}()
}

func (st *socketTracker) emitDataStreamError(subscriptionID string, message string) {
	st.emitMessage(SocketEventNameDataStreamEndpoint, DataStreamEndpointMessage{
		SubscriptionID: subscriptionID,
		Error:          message,
	})
}

func (st *socketTracker) emitSubscriptionCatchup(storeName string, key string, accessLevel AccessLevel) error {
	message, err := st.buildSubscriptionCatchupMessage(storeName, key, accessLevel)
	if err != nil {
		return err
	}
	st.completeSubscriptionCatchup(storeName, message)
	return nil
}

func (st *socketTracker) queueSessionManifestBaseline(
	storeName string,
	keys []string,
	accessLevel AccessLevel,
) error {
	if st.storeMode == StoreDeliveryModeFullStore {
		message, err := st.buildSubscriptionCatchupMessage(storeName, "", accessLevel)
		if err != nil {
			return err
		}
		st.queueLiveStoreUpdate(storeName, message)
		return nil
	}
	for _, key := range keys {
		message, err := st.buildSubscriptionCatchupMessage(storeName, key, accessLevel)
		if err != nil {
			return err
		}
		st.queueLiveStoreUpdate(storeName, message)
		if message.StoreFull != nil {
			return nil
		}
	}
	return nil
}

func (st *socketTracker) buildSubscriptionCatchupMessage(
	storeName string,
	key string,
	accessLevel AccessLevel,
) (emitMessage, error) {
	if st.storeMode == StoreDeliveryModeKeyed && key != "" {
		partialSnapshot, exists, err := st.sr.GetPartialSnapshotForSubscriptionKey(storeName, key, accessLevel)
		if err != nil {
			return emitMessage{}, err
		}
		if exists {
			message, err := buildPartialStoreUpdateMessage(storeName, partialSnapshot)
			if err != nil {
				return emitMessage{}, err
			}
			return message, nil
		}
	}

	stateSnapshot, err := st.sr.GetFullStateSnapshot(storeName, accessLevel)
	if err != nil {
		return emitMessage{}, err
	}
	if st.sessionManager != nil {
		stateBytes, err := SerializeToBytes(stateSnapshot, nil)
		if err != nil {
			return emitMessage{}, err
		}
		stateSnapshot, err = st.sr.DecodeFullStateSnapshot(storeName, stateBytes, accessLevel)
		if err != nil {
			return emitMessage{}, err
		}
	}

	message, err := buildFullStoreUpdateMessage(storeName, stateSnapshot)
	if err != nil {
		return emitMessage{}, err
	}
	return message, nil
}

func buildFullStoreUpdateMessage(storeName string, state Serializable) (emitMessage, error) {
	update := StoreUpdateMessage{
		Time:      time.Now().UnixMilli(),
		Kind:      StoreUpdateFull,
		StoreName: storeName,
	}
	stateBytes, err := SerializeToBytes(state, nil)
	if err != nil {
		return emitMessage{}, err
	}
	return emitMessage{
		Name:      SocketEventNameStoreUpdate,
		StoreName: storeName,
		StoreFull: state,
		Bytes:     len(stateBytes),
		Message: StoreUpdateFullMessage{
			StoreUpdateMessage: update,
			State:              socketTypes.NewBytesBuffer(stateBytes),
		},
	}, nil
}

func buildPartialStoreUpdateMessage(storeName string, partial Serializable) (emitMessage, error) {
	update := StoreUpdateMessage{
		Time:      time.Now().UnixMilli(),
		Kind:      StoreUpdatePartial,
		StoreName: storeName,
	}
	partialBytes, err := SerializeToBytes(partial, nil)
	if err != nil {
		return emitMessage{}, err
	}
	message := emitMessage{
		Name:      SocketEventNameStoreUpdate,
		StoreName: storeName,
		Bytes:     len(partialBytes),
		Message: StoreUpdatePartialMessage{
			StoreUpdateMessage: update,
			Partial:            socketTypes.NewBytesBuffer(partialBytes),
		},
	}
	if typed, ok := partial.(Partial); ok {
		message.StorePartial = typed
	}
	return message, nil
}

func (st *socketTracker) emitPartialStoreUpdateSnapshot(storeName string, partial Serializable) {
	message, err := buildPartialStoreUpdateMessage(storeName, partial)
	if err != nil {
		if st.log != nil {
			st.log.Warnf("Error serializing partial store update: %+v", err)
		}
		st.disconnect()
		return
	}
	st.queueLiveStoreUpdate(storeName, message)
}

func (st *socketTracker) queueLiveStoreUpdate(storeName string, message emitMessage) {
	st.storeUpdateQueueMutex.Lock()
	if st.pendingStoreCatchups[storeName] > 0 {
		if st.bufferedStoreUpdateCount >= st.limits.MaxBufferedStoreUpdates {
			st.storeUpdateQueueMutex.Unlock()
			st.disconnectForLimit(
				"buffered store updates",
				st.limits.MaxBufferedStoreUpdates,
			)
			return
		}
		if st.bufferedStoreUpdates == nil {
			st.bufferedStoreUpdates = map[string][]emitMessage{}
		}
		st.bufferedStoreUpdates[storeName] = append(st.bufferedStoreUpdates[storeName], message)
		st.bufferedStoreUpdateCount++
		st.storeUpdateQueueMutex.Unlock()
		return
	}
	st.storeUpdateQueueMutex.Unlock()
	st.queueEmitMessage(message)
}

func (st *socketTracker) completeSubscriptionCatchup(storeName string, message emitMessage) {
	st.storeUpdateQueueMutex.Lock()
	defer st.storeUpdateQueueMutex.Unlock()

	st.queueEmitMessage(message)
	remaining := st.pendingStoreCatchups[storeName] - 1
	if remaining > 0 {
		st.pendingStoreCatchups[storeName] = remaining
		return
	}

	delete(st.pendingStoreCatchups, storeName)
	bufferedUpdates := st.bufferedStoreUpdates[storeName]
	st.bufferedStoreUpdateCount -= len(bufferedUpdates)
	if st.bufferedStoreUpdateCount < 0 {
		st.bufferedStoreUpdateCount = 0
	}
	for _, buffered := range bufferedUpdates {
		st.queueEmitMessage(buffered)
	}
	delete(st.bufferedStoreUpdates, storeName)
}

func (st *socketTracker) cancelSubscriptionCatchup(storeName string) {
	st.storeUpdateQueueMutex.Lock()
	defer st.storeUpdateQueueMutex.Unlock()

	remaining := st.pendingStoreCatchups[storeName] - 1
	if remaining > 0 {
		st.pendingStoreCatchups[storeName] = remaining
		return
	}
	delete(st.pendingStoreCatchups, storeName)
	st.bufferedStoreUpdateCount -= len(st.bufferedStoreUpdates[storeName])
	if st.bufferedStoreUpdateCount < 0 {
		st.bufferedStoreUpdateCount = 0
	}
	delete(st.bufferedStoreUpdates, storeName)
}

// EventCallback is registered with the EventDispatcher to relay server-side events to the websocket client.
func (st *socketTracker) EventCallback(eventName string, eventBytes []byte) {
	m := EventMessage{
		Time:      time.Now().UnixMilli(),
		EventName: eventName,
		Event:     socketTypes.NewBytesBuffer(eventBytes),
	}

	st.emitMessage(SocketEventNameEvent, m)
}

// KeyedEventCallback relays a keyed event only when this websocket owns the exact subscription.
func (st *socketTracker) KeyedEventCallback(
	storeName string,
	eventName string,
	key string,
	eventBytes []byte,
) {
	subscription := KeyedEventSubscription{StoreName: storeName, EventName: eventName, Key: key}
	st.subscriptionMutex.RLock()
	subscribed := st.keyedEventSubscriptions[subscription] > 0
	st.subscriptionMutex.RUnlock()
	if !subscribed {
		return
	}

	st.emitMessage(SocketEventNameKeyedEvent, KeyedEventMessage{
		Time:      time.Now().UnixMilli(),
		StoreName: storeName,
		EventName: eventName,
		Key:       key,
		Event:     socketTypes.NewBytesBuffer(eventBytes),
	})
}

// onRPCCall is a helper that is called when an RPC call message is received
func (st *socketTracker) onRPCCall(params ...any) {
	rpch := st.lookupRPCHandler()
	if rpch == nil {
		st.log.Errorf("RPCCall received but no RPCHandlerFunc was provided")
		st.disconnect()
		return
	}

	if len(params) == 0 {
		st.log.Error("Missing rpccall message")
		st.disconnect()
		return
	}
	var rpcMsg RPCCallMessage
	if err := mapstructure.Decode(params[0], &rpcMsg); err != nil {
		st.log.Errorf("Error parsing rpccall message: %+v", err)
		st.disconnect()
		return
	}
	if strings.TrimSpace(rpcMsg.MethodName) == "" ||
		len(rpcMsg.MethodName) > maxSocketMethodNameBytes ||
		rpcMsg.Request == nil {
		st.log.Error("Invalid rpccall message")
		st.disconnect()
		return
	}
	requestBytes := rpcMsg.Request.Bytes()
	if len(requestBytes) > st.limits.MaxRPCRequestBytes {
		st.disconnectForLimit("RPC request bytes", st.limits.MaxRPCRequestBytes)
		return
	}
	select {
	case st.rpcSlots <- struct{}{}:
	default:
		st.disconnectForLimit("in-flight RPCs", st.limits.MaxInFlightRPCs)
		return
	}

	// Spawn to a goroutine since it might take a while to get a response and we don't want to block the main thread
	go func() {
		defer func() { <-st.rpcSlots }()
		userAccessLevel, err := st.lookupAccessLevel()
		if err != nil {
			st.log.Errorf("Error looking up user access level: %+v", err)
			st.disconnect()
			return
		}

		respBytes, handled, err := rpch(rpcMsg.MethodName, userAccessLevel, requestBytes)
		var errObj *RPCCallError
		if err != nil {
			st.log.WithField("rpcName", rpcMsg.MethodName).Errorf("Error handling RPC call: %+v", err)

			errObj = &RPCCallError{
				Message: err.Error(),
				Data:    map[string]any{},
			}
		} else if !handled {
			st.log.Errorf("Unhandled RPC call: %s", rpcMsg.MethodName)
			st.disconnect()
			return
		}

		resp := RPCCallResponseMessage{
			CallID:   rpcMsg.CallID,
			Response: socketTypes.NewBytesBuffer(respBytes),
			Error:    errObj,
		}

		st.emitMessage(SocketEventNameRPCCallResponse, resp)
	}()
}

// onFFRPCCall dispatches an FFRPC asynchronously without allocating a call ID,
// retaining response state, or emitting anything back to the caller.
func (st *socketTracker) onFFRPCCall(params ...any) {
	if st.ffrpch == nil {
		st.log.Errorf("FFRPC received but no FFRPCHandlerFunc was provided")
		st.disconnect()
		return
	}

	if len(params) == 0 {
		st.log.Error("Missing ffrpc message")
		st.disconnect()
		return
	}
	var rpcMsg FFRPCCallMessage
	if err := mapstructure.Decode(params[0], &rpcMsg); err != nil {
		st.log.Errorf("Error parsing ffrpc message: %+v", err)
		st.disconnect()
		return
	}
	if strings.TrimSpace(rpcMsg.MethodName) == "" ||
		len(rpcMsg.MethodName) > maxSocketMethodNameBytes ||
		rpcMsg.Request == nil {
		st.log.Error("Invalid ffrpc message")
		st.disconnect()
		return
	}
	requestBytes := rpcMsg.Request.Bytes()
	if len(requestBytes) > st.limits.MaxFFRPCRequestBytes {
		st.disconnectForLimit("FFRPC request bytes", st.limits.MaxFFRPCRequestBytes)
		return
	}
	select {
	case st.ffrpcSlots <- struct{}{}:
	default:
		st.disconnectForLimit("in-flight FFRPCs", st.limits.MaxInFlightFFRPCs)
		return
	}

	go func() {
		defer func() { <-st.ffrpcSlots }()
		userAccessLevel, err := st.lookupAccessLevel()
		if err != nil {
			st.log.Errorf("Error looking up user access level: %+v", err)
			st.disconnect()
			return
		}

		handled, err := st.ffrpch(rpcMsg.MethodName, userAccessLevel, requestBytes)
		if err != nil {
			st.log.WithField("ffrpcName", rpcMsg.MethodName).Errorf("Error handling FFRPC call: %+v", err)
		} else if !handled {
			st.log.Errorf("Unhandled FFRPC call: %s", rpcMsg.MethodName)
			st.disconnect()
		}
	}()
}

// PartialCallback is a callback registered with subscribed stores, which is called back in the event of any SetField
// calls on those stores, so we can relay the field update to the connected websocket client
func (st *socketTracker) PartialCallback(storeName string, fields [][]any, partial Partial) {
	filteredPartial, ok := st.partialForSubscriptions(storeName, fields, partial)
	if !ok {
		return
	}

	if st.sr != nil {
		userAccessLevel, err := st.lookupAccessLevel()
		if err != nil {
			st.log.Errorf("Error looking up user access level: %+v", err)
			st.disconnect()
			return
		}
		if err := st.sr.CheckStoreAccess(storeName, userAccessLevel); err != nil {
			st.log.Errorf("Store partial update denied for %s: %+v", storeName, err)
			st.disconnect()
			return
		}
	}

	st.emitPartialStoreUpdateSnapshot(storeName, filteredPartial)
}

// FullStateCallback relays a full-state replacement to viewers currently subscribed to that store.
func (st *socketTracker) FullStateCallback(storeName string, stateBytes []byte) {
	st.subscriptionMutex.RLock()
	keySubs, subscribed := st.storeSubscriptions[storeName]
	keys := make([]string, 0, len(keySubs))
	for key, count := range keySubs {
		if count > 0 {
			keys = append(keys, key)
		}
	}
	st.subscriptionMutex.RUnlock()
	if !subscribed || len(keys) == 0 {
		return
	}
	sort.Strings(keys)

	userAccessLevel := AccessLevelPublic
	if st.sr != nil {
		var err error
		userAccessLevel, err = st.lookupAccessLevel()
		if err != nil {
			st.log.Errorf("Error looking up user access level: %+v", err)
			st.disconnect()
			return
		}
		if err := st.sr.CheckStoreAccess(storeName, userAccessLevel); err != nil {
			st.log.Errorf("Store full update denied for %s: %+v", storeName, err)
			st.disconnect()
			return
		}
	}

	// A whole-store subscription needs the replacement verbatim. Keyed subscriptions instead get fresh keyed
	// snapshots rebuilt from the newly applied state so a device reconnect does not bypass field filtering.
	if st.storeMode == StoreDeliveryModeKeyed && st.sr != nil && keys[0] != "" {
		keyedMessages := make([]emitMessage, 0, len(keys))
		for _, key := range keys {
			partial, exists, err := st.sr.GetPartialSnapshotForSubscriptionKey(storeName, key, userAccessLevel)
			if err != nil {
				if st.log != nil {
					st.log.Warnf("Error rebuilding keyed store update after full state for %s/%s: %+v", storeName, key, err)
				}
				st.disconnect()
				return
			}
			if !exists {
				keyedMessages = nil
				break
			}
			message, err := buildPartialStoreUpdateMessage(storeName, partial)
			if err != nil {
				if st.log != nil {
					st.log.Warnf("Error serializing keyed store update after full state: %+v", err)
				}
				st.disconnect()
				return
			}
			keyedMessages = append(keyedMessages, message)
		}
		if keyedMessages != nil {
			for _, message := range keyedMessages {
				st.queueLiveStoreUpdate(storeName, message)
			}
			return
		}
	}

	var stateSnapshot Serializable = RawSerializable(stateBytes)
	if st.sessionManager != nil {
		decodedSnapshot, err := st.sr.DecodeFullStateSnapshot(storeName, stateBytes, userAccessLevel)
		if err != nil {
			if st.log != nil {
				st.log.Warnf("Error decoding detached full store update: %+v", err)
			}
			st.disconnect()
			return
		}
		stateSnapshot = decodedSnapshot
	}
	message, err := buildFullStoreUpdateMessage(storeName, stateSnapshot)
	if err != nil {
		if st.log != nil {
			st.log.Warnf("Error serializing full store update: %+v", err)
		}
		st.disconnect()
		return
	}
	st.queueLiveStoreUpdate(storeName, message)
}

func (st *socketTracker) partialForSubscriptions(storeName string, fields [][]any, partial Partial) (Partial, bool) {
	st.subscriptionMutex.RLock()
	keySubs, exists := st.storeSubscriptions[storeName]
	hasWholeSub := exists && keySubs[""] > 0
	subscriptionKeys := []string{}
	if st.storeMode == StoreDeliveryModeKeyed && exists && !hasWholeSub {
		for key, subCount := range keySubs {
			if key == "" || subCount == 0 {
				continue
			}
			subscriptionKeys = append(subscriptionKeys, key)
		}
	}
	st.subscriptionMutex.RUnlock()

	if !exists {
		// no sub, no care
		return nil, false
	}
	if st.storeMode == StoreDeliveryModeKeyed && !hasWholeSub {
		matchingFields := [][]any{}
		for _, key := range subscriptionKeys {
			subscribedField := FieldPathFromSubscriptionKey(key)
			for _, field := range fields {
				if FieldPathAffectsSubscription(field, subscribedField) {
					matchingFields = append(matchingFields, subscribedField)
					break
				}
			}
		}
		if len(matchingFields) == 0 {
			return nil, false
		}
		filteredPartial, ok := FilterPartialToFields(partial, matchingFields)
		if !ok {
			return nil, false
		}
		partial = filteredPartial
	}

	return partial, true
}
