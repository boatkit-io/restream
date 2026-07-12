package restream

import (
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
}

// RPCHandlerFunc is a helper type for a function that handles an RPC call
type RPCHandlerFunc func(name string, minAccessLevel AccessLevel, binaryData []byte) ([]byte, bool, error)

// Dispatcher is a service/struct that handles being a centralized registration point for RPCs, since the RPCs need to fan out
// to multiple stores.  So the Dispatcher centrally registers RPCs, blind to who is handling them, and when a client calls an
// RPC, the dispatcher looks up the target and dispatches the call to them.
type RPCDispatcher struct {
	log *logrus.Logger

	mutex     smartmutex.SmartMutex
	rpcLookup map[string]rpcInfo
}

// NewRPCDispatcher builds a new Dispatcher
func NewRPCDispatcher(log *logrus.Logger) *RPCDispatcher {
	return &RPCDispatcher{
		log: log,

		mutex:     smartmutex.SmartMutex{Name: "restream.RPCDispatcher.mutex"},
		rpcLookup: make(map[string]rpcInfo),
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

	pc := tt.NumIn()
	kinds := make([]reflect.Kind, pc)
	types := make([]reflect.Type, pc)
	for i := 0; i < pc; i++ {
		kinds[i] = tt.In(i).Kind()
		types[i] = tt.In(i)
	}

	info := rpcInfo{
		MinAccessLevel: accessLevel,
		Callback:       callback,
		CallbackValue:  reflect.ValueOf(callback),
		ArgKinds:       kinds,
		ArgTypes:       types,
		RequestType:    requestType,
		ResponseType:   responseType,
	}

	d.rpcLookup[name] = info
}

// FireRPC is called by a client to fire an RPC to a store
func (d *RPCDispatcher) FireRPC(name string, accessLevel AccessLevel, binaryData []byte) ([]byte, bool, error) {
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

	argVs := make([]reflect.Value, numArgs)
	for i := range argVs {
		argVs[i] = rve.Field(i)
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
