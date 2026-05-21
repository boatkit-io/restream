package restream

import (
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

// RPCCallMessage is a message sent by the client with an RPC call (i.e. a `BlahStore.SetXYZ` message/call)
type RPCCallMessage struct {
	CallID     int                         `json:"callID"`
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

	// SocketEventNameRPCCall - RPC Call
	SocketEventNameRPCCall = "rpccall"
	// SocketEventNameRPCCallResponse - RPC Call Response
	SocketEventNameRPCCallResponse = "rpccallresp"
)

// emitMessage is a struct for storing queued message to be emitted through the websocket
type emitMessage struct {
	Name    string
	Message any
}

type AccessLookupFunc func() (AccessLevel, error)

// socketTracker is a handler struct holding the information for a single websocket connection
type socketTracker struct {
	log          *logrus.Logger
	sr           *StoreRegistry
	rpch         RPCHandlerFunc
	ed           *EventDispatcher
	accessLookup AccessLookupFunc

	emitQueueMutex sync.RWMutex
	emitQueue      chan emitMessage

	conn *socket.Socket

	partialApplySubID subscribableevent.SubscriptionId
	eventSubID        subscribableevent.SubscriptionId

	storeSubscriptions map[string]map[string]int
	subscriptionMutex  smartmutex.SmartMutex
	disconnectOnce     sync.Once
}

func AddSocketHandlers(
	conn *socket.Socket,
	log *logrus.Logger,
	sr *StoreRegistry,
	rpch RPCHandlerFunc,
	ed *EventDispatcher,
	accessLookup AccessLookupFunc,
) error {
	st := &socketTracker{
		conn:         conn,
		log:          log,
		sr:           sr,
		rpch:         rpch,
		ed:           ed,
		accessLookup: accessLookup,

		emitQueue:          make(chan emitMessage, 100),
		storeSubscriptions: map[string]map[string]int{},
	}

	if err := conn.On("disconnect", st.onDisconnect); err != nil {
		conn.Disconnect(true)
		return err
	}

	if err := conn.On(SocketEventNameStoreSubscription, st.onStoreSubscription); err != nil {
		conn.Disconnect(true)
		return err
	}

	if err := conn.On(SocketEventNameRPCCall, st.onRPCCall); err != nil {
		conn.Disconnect(true)
		return err
	}

	st.partialApplySubID = st.sr.SubscribeToPartialApplies(st.PartialCallback)
	if st.ed != nil {
		st.eventSubID = st.ed.SubscribeToEvents(st.EventCallback)
	}

	st.handleEmitQueue()

	return nil
}

// onDisconnect is a helper called when the websocket client disconnects, to clean everything up
func (st *socketTracker) onDisconnect(...any) {
	st.disconnectOnce.Do(st.cleanupDisconnect)
}

func (s *socketTracker) cleanupDisconnect() {
	s.emitQueueMutex.Lock()
	if s.emitQueue != nil {
		close(s.emitQueue)
		s.emitQueue = nil
	}
	s.emitQueueMutex.Unlock()

	if s.sr != nil {
		s.sr.UnsubscribeFromPartialApplies(s.partialApplySubID) //nolint:errcheck // Why: Best effort
	}
	if s.ed != nil {
		s.ed.UnsubscribeFromEvents(s.eventSubID) //nolint:errcheck // Why: Best effort
	}

	s.subscriptionMutex.RLock()
	storeSubs := lo.MapValues(s.storeSubscriptions, func(subs map[string]int, _ string) map[string]int {
		return lo.Assign(map[string]int{}, subs)
	})
	s.subscriptionMutex.RUnlock()
	for storeName, keySubs := range storeSubs {
		for key := range keySubs {
			if s.sr != nil {
				if err := s.sr.StopListeningToStoreKey(storeName, key); err != nil {
					s.log.Errorf("Error StopListeningToStoreKey to %s/%s -- possible double unsubscribe? Reason: %+v", storeName, key, err)
				}
			}
		}
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
			if st.conn == nil {
				return
			}
			err := st.conn.Emit(msg.Name, msg.Message)
			if err != nil {
				st.log.Warnf("Error emitting message: %+v", err)
				st.conn.Disconnect(true)
				return
			}
		}
	}()
}

// emitMessage adds a single message to emit to the emit queue
func (st *socketTracker) emitMessage(name string, arg any) {
	st.emitQueueMutex.RLock()
	if st.emitQueue == nil {
		st.emitQueueMutex.RUnlock()
		return
	}
	st.emitQueue <- emitMessage{Name: name, Message: arg}
	st.emitQueueMutex.RUnlock()
}

// onStoreSubscription is a helper that is called when a store subscription message is received
func (st *socketTracker) onStoreSubscription(params ...any) {
	var subMsg StoreSubscriptionMessage
	if err := mapstructure.Decode(params[0], &subMsg); err != nil {
		st.log.Errorf("Error parsing store subscription message: %+v", err)
		st.conn.Disconnect(true)
		return
	}

	if !st.sr.IsStoreValid(subMsg.StoreName) {
		st.log.Errorf("Client referenced a subscription to an invalid store %s", subMsg.StoreName)
		st.conn.Disconnect(true)
		return
	}

	switch subMsg.Action {
	case Subscribe:
		key := subMsg.Key
		st.subscriptionMutex.Lock()
		keySubs := st.storeSubscriptions[subMsg.StoreName]
		if keySubs == nil {
			keySubs = map[string]int{}
			st.storeSubscriptions[subMsg.StoreName] = keySubs
		}
		keySubs[key]++
		firstKey := keySubs[key] == 1
		st.subscriptionMutex.Unlock()

		if !firstKey {
			return
		}

		if err := st.emitSubscriptionCatchup(subMsg.StoreName, key); err != nil {
			st.log.Errorf("Error sending subscription catchup for %s/%s: %+v", subMsg.StoreName, key, err)
			st.conn.Disconnect(true)
			return
		}

		if err := st.sr.ListeningToStoreKey(subMsg.StoreName, key); err != nil {
			st.log.Errorf("Error ListeningToStoreKey to %s/%s from packet -- possible double subscribe? Reason: %+v", subMsg.StoreName, key, err)
		}
	case Unsubscribe:
		key := subMsg.Key
		st.subscriptionMutex.Lock()
		last := false
		keySubs, exists := st.storeSubscriptions[subMsg.StoreName]
		if !exists || keySubs[key] == 0 {
			st.subscriptionMutex.Unlock()
			st.log.Errorf("Unsubscription for %s/%s with no prior subscriptions", subMsg.StoreName, key)
			return
		}

		keySubs[key]--
		last = keySubs[key] == 0

		st.subscriptionMutex.Unlock()

		if !last {
			return
		}

		if err := st.sr.StopListeningToStoreKey(subMsg.StoreName, key); err != nil {
			st.log.Errorf(
				"Error StopListeningToStoreKey to %s/%s from packet -- possible double unsubscribe? Reason: %+v",
				subMsg.StoreName, key, err,
			)
		}

		st.subscriptionMutex.Lock()
		delete(keySubs, key)
		if len(keySubs) == 0 {
			delete(st.storeSubscriptions, subMsg.StoreName)
		}
		st.subscriptionMutex.Unlock()
	}
}

func (st *socketTracker) emitSubscriptionCatchup(storeName string, key string) error {
	if key != "" {
		pBytes, exists, err := st.sr.GetSerializedPartialForSubscriptionKey(storeName, key)
		if err != nil {
			return err
		}
		if exists {
			st.emitPartialStoreUpdate(storeName, pBytes)
			return nil
		}
	}

	sBytes, err := st.sr.GetSerializedFullState(storeName)
	if err != nil {
		return err
	}

	m := StoreUpdateFullMessage{
		StoreUpdateMessage: StoreUpdateMessage{
			Time:      time.Now().UnixMilli(),
			Kind:      StoreUpdateFull,
			StoreName: storeName,
		},
		State: socketTypes.NewBytesBuffer(sBytes),
	}
	st.emitMessage(SocketEventNameStoreUpdate, m)
	return nil
}

func (st *socketTracker) emitPartialStoreUpdate(storeName string, partialBytes []byte) {
	m := StoreUpdatePartialMessage{
		StoreUpdateMessage: StoreUpdateMessage{
			Time:      time.Now().UnixMilli(),
			Kind:      StoreUpdatePartial,
			StoreName: storeName,
		},
		Partial: socketTypes.NewBytesBuffer(partialBytes),
	}

	st.emitMessage(SocketEventNameStoreUpdate, m)
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

// onRPCCall is a helper that is called when an RPC call message is received
func (st *socketTracker) onRPCCall(params ...any) {
	if st.rpch == nil {
		st.log.Errorf("RPCCall received but no RPCHandlerFunc was provided")
		st.conn.Disconnect(true)
		return
	}

	var rpcMsg RPCCallMessage
	if err := mapstructure.Decode(params[0], &rpcMsg); err != nil {
		st.log.Errorf("Error parsing rpccall message: %+v", err)
		st.conn.Disconnect(true)
		return
	}

	// Spawn to a goroutine since it might take a while to get a response and we don't want to block the main thread
	go func() {
		userAccessLevel, err := st.accessLookup()
		if err != nil {
			st.log.Errorf("Error looking up user access level: %+v", err)
			st.conn.Disconnect(true)
			return
		}

		respBytes, handled, err := st.rpch(rpcMsg.MethodName, userAccessLevel, rpcMsg.Request.Bytes())
		var errObj *RPCCallError
		if err != nil {
			st.log.Errorf("Error handling RPC call: %+v", err)

			errObj = &RPCCallError{
				Message: err.Error(),
				Data:    map[string]any{},
			}
		} else if !handled {
			st.log.Errorf("Unhandled RPC call: %s", rpcMsg.MethodName)
			st.conn.Disconnect(true)
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

// PartialCallback is a callback registered with subscribed stores, which is called back in the event of any SetField
// calls on those stores, so we can relay the field update to the connected websocket client
func (st *socketTracker) PartialCallback(storeName string, fields [][]any, partial Partial) {
	filteredPartial, ok := st.partialForSubscriptions(storeName, fields, partial)
	if !ok {
		return
	}

	b, err := SerializeToBytes(filteredPartial, nil)
	if err != nil {
		st.log.Warnf("Error serializing store state: %+v", err)
		st.conn.Disconnect(true)
		return
	}

	st.emitPartialStoreUpdate(storeName, b)
}

func (st *socketTracker) partialForSubscriptions(storeName string, fields [][]any, partial Partial) (Partial, bool) {
	st.subscriptionMutex.RLock()
	keySubs, exists := st.storeSubscriptions[storeName]
	hasWholeSub := exists && keySubs[""] > 0
	matchingFields := [][]any{}
	if exists && !hasWholeSub {
		for key, subCount := range keySubs {
			if key == "" || subCount == 0 {
				continue
			}
			subscribedField := FieldPathFromSubscriptionKey(key)
			for _, field := range fields {
				if FieldPathAffectsSubscription(field, subscribedField) {
					matchingFields = append(matchingFields, subscribedField)
					break
				}
			}
		}
	}
	st.subscriptionMutex.RUnlock()

	if !exists {
		// no sub, no care
		return nil, false
	}
	if !hasWholeSub {
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
