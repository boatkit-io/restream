package restream

import (
	"context"
	"fmt"
	"reflect"

	"github.com/boatkit-io/restream/pkg/binarystreams"
	"github.com/boatkit-io/restream/pkg/smartmutex"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type AccessLevel int

const (
	// AccessLevelPublic is the default minimum access level for stores and RPCs.
	AccessLevelPublic AccessLevel = 0
)

// rpcInfo is a dump of information about a single RPC that was registered by a store
type rpcInfo struct {
	MinAccessLevel            AccessLevel
	Callback                  any
	CallbackValue             reflect.Value
	ArgKinds                  []reflect.Kind
	ArgTypes                  []reflect.Type
	RequestType, ResponseType reflect.Type
	HasContext                bool
}

// RPCHandlerFunc is a helper type for a function that handles an RPC call
type RPCHandlerFunc func(name string, minAccessLevel AccessLevel, binaryData []byte) ([]byte, bool, error)

// RPCHandlerWithAnnotationsFunc handles an RPC together with transport-supplied
// key/value annotations that remain separate from its serialized request.
type RPCHandlerWithAnnotationsFunc func(
	annotations map[string]string,
	name string,
	minAccessLevel AccessLevel,
	binaryData []byte,
) ([]byte, bool, error)

// RPCCallInfo contains trusted request metadata made available to RPC
// callbacks that declare context.Context as their first parameter.
type RPCCallInfo struct {
	AccessLevel AccessLevel
	Annotations map[string]string
}

type rpcCallInfoContextKey struct{}

// RPCCallInfoFromContext returns request metadata attached by the dispatcher.
func RPCCallInfoFromContext(ctx context.Context) (RPCCallInfo, bool) {
	if ctx == nil {
		return RPCCallInfo{}, false
	}
	info, ok := ctx.Value(rpcCallInfoContextKey{}).(RPCCallInfo)
	return info, ok
}

// FFRPCHandlerFunc handles a fire-and-forget RPC call. The bool reports whether
// a handler was registered; errors are only available to the receiving process
// because FFRPCs never send a response to the caller.
type FFRPCHandlerFunc func(name string, minAccessLevel AccessLevel, binaryData []byte) (bool, error)

type ffrpcInfo struct {
	MinAccessLevel AccessLevel
	CallbackValue  reflect.Value
	ArgKinds       []reflect.Kind
	RequestType    reflect.Type
	ReturnsError   bool
	HasContext     bool
}

// Dispatcher is a service/struct that handles being a centralized registration point for RPCs, since the RPCs need to fan out
// to multiple stores.  So the Dispatcher centrally registers RPCs, blind to who is handling them, and when a client calls an
// RPC, the dispatcher looks up the target and dispatches the call to them.
type RPCDispatcher struct {
	log *logrus.Logger

	mutex       smartmutex.SmartMutex
	rpcLookup   map[string]rpcInfo
	ffrpcLookup map[string]ffrpcInfo
}

// NewRPCDispatcher builds a new Dispatcher
func NewRPCDispatcher(log *logrus.Logger) *RPCDispatcher {
	return &RPCDispatcher{
		log: log,

		mutex:       smartmutex.SmartMutex{Name: "restream.RPCDispatcher.mutex"},
		rpcLookup:   make(map[string]rpcInfo),
		ffrpcLookup: make(map[string]ffrpcInfo),
	}
}

// RegisterRPCHandler is called by a store to register an RPC handler back to the store by name (the name must be identicaly to
// what is used on the client side, which is usually [StoreName].[MethodName])
func (d *RPCDispatcher) RegisterRPCHandler(name string, accessLevel AccessLevel, callback any, requestType, responseType reflect.Type) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	if _, exists := d.rpcLookup[name]; exists {
		panic("Double-registration of RPC: " + name)
	}
	if _, exists := d.ffrpcLookup[name]; exists {
		panic("Double-registration of RPC: " + name)
	}

	tt := reflect.TypeOf(callback)
	if tt.Kind() != reflect.Func {
		panic(fmt.Sprintf("Non-function passed to RegisterRPCHandler: %+v", tt))
	}
	if tt.NumOut() == 0 || tt.NumOut() > 2 {
		panic(fmt.Sprintf("Function returning %d vars passed to RegisterRPCHandler: %+v", tt.NumOut(), tt))
	}

	errIdx := 0
	if tt.NumOut() == 2 {
		errIdx = 1
	}
	if !tt.Out(errIdx).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		panic(fmt.Sprintf("Function not returning an error as the last var passed to RegisterRPCHandler: %+v", tt))
	}

	argOffset := rpcContextArgumentOffset(tt)
	pc := tt.NumIn() - argOffset
	kinds := make([]reflect.Kind, pc)
	types := make([]reflect.Type, pc)
	for i := 0; i < pc; i++ {
		kinds[i] = tt.In(i + argOffset).Kind()
		types[i] = tt.In(i + argOffset)
	}

	info := rpcInfo{
		MinAccessLevel: accessLevel,
		Callback:       callback,
		CallbackValue:  reflect.ValueOf(callback),
		ArgKinds:       kinds,
		ArgTypes:       types,
		RequestType:    requestType,
		ResponseType:   responseType,
		HasContext:     argOffset == 1,
	}

	d.rpcLookup[name] = info
}

// RegisterFFRPCHandler registers a fire-and-forget RPC handler. FFRPC callbacks
// may return nothing or one error; an error is logged locally but is never sent
// back to the caller.
func (d *RPCDispatcher) RegisterFFRPCHandler(
	name string,
	accessLevel AccessLevel,
	callback any,
	requestType reflect.Type,
) {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	if _, exists := d.rpcLookup[name]; exists {
		panic("Double-registration of FFRPC: " + name)
	}
	if _, exists := d.ffrpcLookup[name]; exists {
		panic("Double-registration of FFRPC: " + name)
	}

	tt := reflect.TypeOf(callback)
	if tt.Kind() != reflect.Func {
		panic(fmt.Sprintf("Non-function passed to RegisterFFRPCHandler: %+v", tt))
	}
	if tt.NumOut() > 1 {
		panic(fmt.Sprintf("Function returning %d vars passed to RegisterFFRPCHandler: %+v", tt.NumOut(), tt))
	}
	returnsError := tt.NumOut() == 1
	if returnsError && !tt.Out(0).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		panic(fmt.Sprintf("Function not returning an error passed to RegisterFFRPCHandler: %+v", tt))
	}

	argOffset := rpcContextArgumentOffset(tt)
	argKinds := make([]reflect.Kind, tt.NumIn()-argOffset)
	for index := range argKinds {
		argKinds[index] = tt.In(index + argOffset).Kind()
	}
	d.ffrpcLookup[name] = ffrpcInfo{
		MinAccessLevel: accessLevel,
		CallbackValue:  reflect.ValueOf(callback),
		ArgKinds:       argKinds,
		RequestType:    requestType,
		ReturnsError:   returnsError,
		HasContext:     argOffset == 1,
	}
}

// FireRPC is called by a client to fire an RPC to a store
func (d *RPCDispatcher) FireRPC(name string, accessLevel AccessLevel, binaryData []byte) ([]byte, bool, error) {
	return d.FireRPCWithAnnotations(nil, name, accessLevel, binaryData)
}

// FireRPCWithAnnotations dispatches an RPC with transport-supplied annotations.
func (d *RPCDispatcher) FireRPCWithAnnotations(
	annotations map[string]string,
	name string,
	accessLevel AccessLevel,
	binaryData []byte,
) ([]byte, bool, error) {
	d.mutex.Lock()
	rpc, exists := d.rpcLookup[name]
	d.mutex.Unlock()
	if !exists {
		return nil, false, nil
	}

	if accessLevel < rpc.MinAccessLevel {
		err := fmt.Errorf("RPC (%s) called with insufficient access (%+v < %+v)", name, accessLevel, rpc.MinAccessLevel)
		d.log.Errorf("%+v", err.Error())
		return nil, true, err
	}

	rv := reflect.New(rpc.RequestType)
	req := rv.Interface().(Serializable)
	if err := req.Deserialize(binarystreams.NewReaderFromBytes(binaryData), nil); err != nil {
		return nil, true, err
	}

	numArgs := len(rpc.ArgKinds)
	rve := rv.Elem()
	numFields := rve.NumField()
	if numArgs != numFields {
		err := fmt.Errorf("RPC (%s) called with %v params when it should have been %v", name, numFields, numArgs)
		d.log.Errorf("%+v", err.Error())
		return nil, true, err
	}

	argOffset := 0
	if rpc.HasContext {
		argOffset = 1
	}
	argVs := make([]reflect.Value, numArgs+argOffset)
	if rpc.HasContext {
		argVs[0] = reflect.ValueOf(newRPCCallContext(accessLevel, annotations))
	}
	for i := range numArgs {
		argVs[i+argOffset] = rve.Field(i)
	}

	// RPC function returns already checked in RegisterRPCHandler, so we can trust them
	respRaw := rpc.CallbackValue.Call(argVs)
	rsv := reflect.New(rpc.ResponseType)
	resp := rsv.Interface().(Serializable)

	rsve := rsv.Elem()
	errIdx := 1
	if len(respRaw) == 1 {
		errIdx = 0
	} else {
		rsve.FieldByName("Result").Set(respRaw[0])
	}

	var errRet error
	if !respRaw[errIdx].IsNil() {
		errRet = respRaw[errIdx].Interface().(error)
		errorStr := errRet.Error()
		rsve.FieldByName("Error").Set(reflect.ValueOf(&errorStr))
		d.log.Errorf("Error response to RPC %s: %s", name, errRet)
	}

	var respBytes []byte
	if resp != nil {
		var err error
		respBytes, err = SerializeToBytes(resp, nil)
		if err != nil {
			err := errors.Wrap(err, "Error serializing RPC response")
			d.log.Errorf("%+v", err.Error())
			return nil, true, err
		}
	}

	return respBytes, true, nil
}

// FireFFRPC dispatches a fire-and-forget RPC. It reports local dispatch errors
// to the transport for logging, but no response is serialized for the caller.
func (d *RPCDispatcher) FireFFRPC(name string, accessLevel AccessLevel, binaryData []byte) (bool, error) {
	d.mutex.Lock()
	ffrpc, exists := d.ffrpcLookup[name]
	d.mutex.Unlock()
	if !exists {
		return false, nil
	}

	if accessLevel < ffrpc.MinAccessLevel {
		err := fmt.Errorf(
			"FFRPC (%s) called with insufficient access (%+v < %+v)",
			name,
			accessLevel,
			ffrpc.MinAccessLevel,
		)
		if d.log != nil {
			d.log.Error(err)
		}
		return true, err
	}

	rv := reflect.New(ffrpc.RequestType)
	req := rv.Interface().(Serializable)
	if err := req.Deserialize(binarystreams.NewReaderFromBytes(binaryData), nil); err != nil {
		return true, err
	}

	rve := rv.Elem()
	if len(ffrpc.ArgKinds) != rve.NumField() {
		err := fmt.Errorf(
			"FFRPC (%s) called with %v params when it should have been %v",
			name,
			rve.NumField(),
			len(ffrpc.ArgKinds),
		)
		if d.log != nil {
			d.log.Error(err)
		}
		return true, err
	}

	argOffset := 0
	if ffrpc.HasContext {
		argOffset = 1
	}
	argValues := make([]reflect.Value, len(ffrpc.ArgKinds)+argOffset)
	if ffrpc.HasContext {
		argValues[0] = reflect.ValueOf(newRPCCallContext(accessLevel, nil))
	}
	for index := range ffrpc.ArgKinds {
		argValues[index+argOffset] = rve.Field(index)
	}
	results := ffrpc.CallbackValue.Call(argValues)
	if ffrpc.ReturnsError && !results[0].IsNil() {
		err := results[0].Interface().(error)
		if d.log != nil {
			d.log.WithError(err).Errorf("Error handling FFRPC %s", name)
		}
		return true, err
	}
	return true, nil
}

func rpcContextArgumentOffset(callbackType reflect.Type) int {
	if callbackType.NumIn() > 0 && callbackType.In(0) == reflect.TypeFor[context.Context]() {
		return 1
	}
	return 0
}

func newRPCCallContext(accessLevel AccessLevel, annotations map[string]string) context.Context {
	return context.WithValue(context.Background(), rpcCallInfoContextKey{}, RPCCallInfo{
		AccessLevel: accessLevel,
		Annotations: annotations,
	})
}
