package restream

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/boatkit-io/restream/pkg/binarystreams"
	"github.com/boatkit-io/restream/pkg/smartmutex"
	"github.com/boatkit-io/tugboat/pkg/subscribableevent"
)

// typeAnyArrayOfAny is a cached reflection type for []any
var typeAnyArrayOfAny = reflect.TypeFor[[]any]()

const (
	storeDataLockWarnAfter              = 100 * time.Millisecond
	minimumAdaptiveOutputBufferDuration = 5 * time.Millisecond
	adaptiveOutputPressureAge           = 250 * time.Millisecond
	adaptiveOutputRecoveryInterval      = 5 * time.Second
	outputPressureLogInterval           = 30 * time.Second
)

// ErrCloudSourceStoreMutation is raised when local code attempts to mutate a device store
// whose source of truth is the cloud relay.
var ErrCloudSourceStoreMutation = errors.New("cloud-source stores can only be updated from the relay")

// PartialCallbackFunc is a reusable type for the callbacks for partial applications
type PartialCallbackFunc = func(storeName string, fields [][]any, partial Partial)

// FullStateCallbackFunc is called after a serialized full state has replaced a store's state.
type FullStateCallbackFunc = func(storeName string, stateBytes []byte)

// StoreSubscriptionCallbackFunc is called for aggregate store/key subscription transitions.
type StoreSubscriptionCallbackFunc = func(storeName string, key string, subscribed bool)

// StoreDataBase is a basic interface for all typed stores, since golang generics are so gimpy
type StoreDataBase interface {
	AddCallback(PartialCallbackFunc) subscribableevent.SubscriptionId
	RemoveCallback(subscribableevent.SubscriptionId) error
	DecodeAndSetFullState([]byte) error
	DecodeAndApplyPartial([]byte) error
	GetSerializedFullState() ([]byte, error)
}

var _ StoreDataBase = (*StoreData[fakeStruct, *fakeStruct, *fakePartial])(nil)

// StoreDataPtrType is a type constraint for asserting that it is both Serializable and a pointer to a store's state structure
type StoreDataPtrType[S any] interface {
	Serializable
	*S
}

// StoreData is a manager for a store's data.  The state structure itself is stored in the store, but the StoreData is created
// with a reference to the state.  StoreData then handles mutations to the store state (you're not allowed to directly touch
// store state, you have to use SetField), and allows subscriptions to changes via SubscribeToField.
type StoreData[S any, SP StoreDataPtrType[S], P Partial] struct {
	// Name is needed for callbacks
	name string

	// Mutex protecting access to the state struct
	stateMutex smartmutex.SmartMutex

	// Full (serializable) state structure (pointer)
	state SP

	// Perf optimization to hold the reflect.value for the base state
	stateReflect reflect.Value

	partialCallbacks subscribableevent.Event[PartialCallbackFunc]

	subscriptions      *fieldSubTier
	subscriptionsMutex sync.RWMutex

	localApplyErr error

	applyOrderMutex sync.Mutex

	outputBufferDuration          time.Duration
	outputQueueMutex              sync.Mutex
	outputQueueCond               *sync.Cond
	outputQueue                   []storeDataOutput[S]
	outputNextSequence            uint64
	outputSubscriptionGeneration  uint64
	outputFiringSequence          atomic.Uint64
	outputState                   SP
	outputEffectiveBufferDuration time.Duration
	outputMaxBufferDuration       time.Duration
	outputInFlightEnqueuedAt      time.Time
	outputInFlightGatherDuration  time.Duration
	outputMaxQueueDepth           int
	outputBackoffCount            uint64
	outputLastBackoff             time.Time
	outputLastRecoveryAdjustment  time.Time
	outputLastPressureLog         time.Time
	outputLastBatchSize           int
	outputLastBatchAge            time.Duration
	outputLastBatchGatherDuration time.Duration
	outputLastBatchHandlingTime   time.Duration
	outputEnqueuedCount           uint64
	outputProcessedCount          uint64
	outputBatchCount              uint64
	outputTotalQueueDelay         time.Duration
	outputTotalHandlingTime       time.Duration
	outputMaxBatchSize            int
}

type storeDataOutput[S any] struct {
	partial                Partial
	replaceWith            *S
	enqueuedAt             time.Time
	barrier                chan struct{}
	sequence               uint64
	subscriptionGeneration uint64
}

// StoreDataOutputStats reports one store's serialized callback queue pressure.
type StoreDataOutputStats struct {
	QueueDepth               int
	MaxQueueDepth            int
	OldestQueuedAge          time.Duration
	ConfiguredBufferDuration time.Duration
	EffectiveBufferDuration  time.Duration
	MaxBufferDuration        time.Duration
	AdaptiveGathering        bool
	AdaptiveBackoffCount     uint64
	EnqueuedOutputCount      uint64
	ProcessedOutputCount     uint64
	EmittedBatchCount        uint64
	TotalQueueDelay          time.Duration
	TotalHandlingTime        time.Duration
	MaxBatchSize             int
}

// StoreDataOutputStatsProvider is implemented by StoreData instances so a
// registry can surface per-store callback pressure without knowing state types.
type StoreDataOutputStatsProvider interface {
	GetOutputStats(time.Time) StoreDataOutputStats
}

// StoreDataOutputWaiter allows tests and shutdown checks to wait for all
// outputs queued before the call without exposing a store's state types.
type StoreDataOutputWaiter interface {
	WaitForOutputIdle(time.Duration) bool
}

// StoreDataOptions controls optional StoreData update delivery behavior.
type StoreDataOptions struct {
	// OutputBufferDuration gathers changed partials for this duration before the
	// store's serialized output worker fires AddCallback and SubscribeToField
	// callbacks. Zero emits every update without gathering. Store state is still
	// updated synchronously on every ApplyPartial call in either mode.
	OutputBufferDuration time.Duration
	// MaxOutputBufferDuration opts this store into adaptive gathering when its
	// output queue falls behind. It must be greater than OutputBufferDuration.
	// Zero preserves every output without automatic gathering.
	MaxOutputBufferDuration time.Duration
	// OutputBacklogThreshold is retained for source compatibility. It is ignored:
	// adaptive gathering responds to queue age rather than burst depth.
	// Deprecated: queue depth is diagnostic only.
	OutputBacklogThreshold int
}

// AddCallback implements StoreDataBase.
func (d *StoreData[S, SP, PS]) AddCallback(cf func(storeName string, fields [][]any,
	partial Partial)) subscribableevent.SubscriptionId {
	d.outputQueueMutex.Lock()
	d.outputSubscriptionGeneration++
	startSequence := d.outputNextSequence + 1
	sid := d.partialCallbacks.Subscribe(func(storeName string, fields [][]any, partial Partial) {
		if d.outputFiringSequence.Load() >= startSequence {
			cf(storeName, fields, partial)
		}
	})
	d.outputQueueMutex.Unlock()
	return sid
}

// RemoveCallback implements StoreDataBase.
func (d *StoreData[S, SP, PS]) RemoveCallback(sid subscribableevent.SubscriptionId) error {
	return d.partialCallbacks.Unsubscribe(sid)
}

// NewStoreData builds a new StoreData for a given named store, taking a reference to the store's state structure.
func NewStoreData[S any, SP StoreDataPtrType[S], P Partial](store Store, state SP) *StoreData[S, SP, P] {
	return NewStoreDataWithOptions[S, SP, P](store, state, StoreDataOptions{})
}

// NewStoreDataWithOptions builds StoreData with optional update delivery behavior.
func NewStoreDataWithOptions[S any, SP StoreDataPtrType[S], P Partial](
	store Store,
	state SP,
	options StoreDataOptions,
) *StoreData[S, SP, P] {
	name := store.GetName()

	// Pre-calc the reflected state since we use it repeatedly
	ds := reflect.ValueOf(state)
	if ds.Kind() != reflect.Pointer {
		panic(fmt.Sprintf("Store %s passed non-pointer state structure", name))
	}
	ds = ds.Elem()
	if ds.Kind() != reflect.Struct {
		panic(fmt.Sprintf("Store %s passed non-pointer-to-struct state structure", name))
	}

	var localApplyErr error
	if StoreTypeForStore(store) == StoreTypeDeviceWithCloudSource {
		localApplyErr = ErrCloudSourceStoreMutation
	}

	outputState, err := detachedState[S, SP](state)
	if err != nil {
		panic(fmt.Sprintf("Store %s could not clone initial output state: %v", name, err))
	}
	maxBufferDuration := options.MaxOutputBufferDuration
	if maxBufferDuration < options.OutputBufferDuration {
		maxBufferDuration = options.OutputBufferDuration
	}
	d := &StoreData[S, SP, P]{
		name:         name,
		stateMutex:   smartmutex.SmartMutex{Name: "restream.StoreData(" + name + ").stateMutex"},
		state:        state,
		stateReflect: ds,

		partialCallbacks: subscribableevent.NewEvent[PartialCallbackFunc](),
		subscriptions:    newFieldSubTier(nil),
		localApplyErr:    localApplyErr,

		outputBufferDuration:          options.OutputBufferDuration,
		outputEffectiveBufferDuration: options.OutputBufferDuration,
		outputMaxBufferDuration:       maxBufferDuration,
		outputState:                   SP(outputState),
	}
	d.outputQueueCond = sync.NewCond(&d.outputQueueMutex)
	go d.runOutputWorker()
	return d
}

// GetFullStateSnapshot returns a full state snapshot from a StoreData.
// Generated states are cloned under a read lock so callers can serialize after the lock is released.
func (d *StoreData[S, SP, PS]) GetFullStateSnapshot() (Serializable, error) {
	var ret Serializable
	var retError error
	waitStart := time.Now()
	d.stateMutex.RLock()
	waitDuration := time.Since(waitStart)
	holdStart := time.Now()
	defer func() {
		holdDuration := time.Since(holdStart)
		d.stateMutex.RUnlock()
		d.logLockTiming("GetFullStateSnapshot", "read", waitDuration, holdDuration)
	}()

	cloned, hasSnapshot := cloneState[S, SP](d.state)
	if hasSnapshot {
		return SP(cloned), nil
	}

	var serialized []byte
	serialized, retError = serializeState(d.state)
	if retError != nil {
		return nil, retError
	}
	ret = RawSerializable(serialized)
	return ret, retError
}

// DecodeFullStateSnapshot decodes bytes into a detached typed state without
// replacing the live store. Session buffers use this when a full state
// supersedes an accumulated partial and later partials must be applied to that
// retained snapshot.
func (d *StoreData[S, SP, PS]) DecodeFullStateSnapshot(b []byte) (Serializable, error) {
	state := new(S)
	if err := SP(state).Deserialize(binarystreams.NewReaderFromBytes(b), nil); err != nil {
		return nil, err
	}
	return SP(state), nil
}

// GetSerializedFullState returns the full state from a StoreData.
// Generated states are cloned under a read lock and serialized after the lock is released.
func (d *StoreData[S, SP, PS]) GetSerializedFullState() ([]byte, error) {
	snapshot, err := d.GetFullStateSnapshot()
	if err != nil {
		return nil, err
	}
	return SerializeToBytes(snapshot, nil)
}

// GetPartialSnapshotForSubscriptionKey returns an initial keyed partial snapshot for a keyed subscription.
func (d *StoreData[S, SP, PS]) GetPartialSnapshotForSubscriptionKey(key string) (Serializable, bool, error) {
	fieldPath := FieldPathFromSubscriptionKey(key)
	if len(fieldPath) == 0 {
		return nil, false, nil
	}

	if snapshot, hasSnapshot := d.cloneStateSnapshot("GetPartialSnapshotForSubscriptionKey"); hasSnapshot {
		return d.partialForFieldPath(snapshot, fieldPath)
	}

	var ret Serializable
	var exists bool
	var retErr error
	d.readState("GetPartialSnapshotForSubscriptionKey", func(state SP) {
		ret, exists, retErr = d.serializedPartialForFieldPath(state, fieldPath)
	})

	return ret, exists, retErr
}

// GetSerializedPartialForSubscriptionKey returns a serialized partial snapshot for a keyed subscription.
func (d *StoreData[S, SP, PS]) GetSerializedPartialForSubscriptionKey(key string) ([]byte, bool, error) {
	snapshot, exists, err := d.GetPartialSnapshotForSubscriptionKey(key)
	if err != nil || !exists {
		return nil, exists, err
	}
	ret, err := SerializeToBytes(snapshot, nil)
	return ret, true, err
}

func (d *StoreData[S, SP, PS]) cloneStateSnapshot(operation string) (SP, bool) {
	var ret SP
	var hasSnapshot bool
	waitStart := time.Now()
	d.stateMutex.RLock()
	waitDuration := time.Since(waitStart)
	holdStart := time.Now()
	defer func() {
		holdDuration := time.Since(holdStart)
		d.stateMutex.RUnlock()
		d.logLockTiming(operation, "read", waitDuration, holdDuration)
	}()

	cloned, ok := cloneState[S, SP](d.state)
	if !ok {
		return ret, false
	}
	ret = SP(cloned)
	hasSnapshot = true
	return ret, hasSnapshot
}

func cloneState[S any, SP StoreDataPtrType[S]](state SP) (*S, bool) {
	cloner, ok := any(state).(StateCloner[S])
	if !ok {
		return nil, false
	}
	return cloner.RestreamClone(), true
}

func detachedState[S any, SP StoreDataPtrType[S]](state SP) (*S, error) {
	if cloned, ok := cloneState[S, SP](state); ok {
		return cloned, nil
	}
	serialized, err := serializeState[S, SP](state)
	if err != nil {
		return nil, err
	}
	ret := new(S)
	if err := SP(ret).Deserialize(binarystreams.NewReaderFromBytes(serialized), nil); err != nil {
		return nil, err
	}
	return ret, nil
}

func serializeState[S any, SP StoreDataPtrType[S]](state SP) ([]byte, error) {
	w, b := binarystreams.NewMemoryWriter()
	if err := state.Serialize(w, nil); err != nil {
		return nil, err
	}
	if err := w.Flush(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func (d *StoreData[S, SP, PS]) partialForFieldPath(state SP, fieldPath []any) (Partial, bool, error) {
	var partial Partial
	var exists bool
	if provider, ok := any(state).(StateFieldPartialProvider); ok {
		partial, exists = provider.PartialForFields([][]any{fieldPath})
	} else {
		partialValue, partialExists, err := partialForFieldPathReflect(
			reflect.ValueOf(state).Elem(),
			reflect.TypeFor[PS](),
			fieldPath,
		)
		if err != nil || !partialExists {
			return nil, partialExists, err
		}
		partial = partialValue.Interface().(Partial)
		exists = true
	}
	if !exists {
		return nil, false, nil
	}
	return partial, true, nil
}

func (d *StoreData[S, SP, PS]) serializedPartialForFieldPath(state SP, fieldPath []any) (Serializable, bool, error) {
	partial, exists, err := d.partialForFieldPath(state, fieldPath)
	if err != nil || !exists {
		return nil, exists, err
	}
	ret, err := SerializeToBytes(partial, nil)
	return RawSerializable(ret), true, err
}

// ReadState returns a cloned copy of the current state.
func (d *StoreData[S, SP, PS]) ReadState() SP {
	snapshot, hasSnapshot := d.cloneStateSnapshot("ReadState")
	if !hasSnapshot {
		panic(fmt.Sprintf("Store %s state type %T does not implement RestreamClone", d.name, d.state))
	}
	return snapshot
}

func (d *StoreData[S, SP, PS]) readState(operation string, cb func(SP)) {
	waitStart := time.Now()
	d.stateMutex.RLock()
	waitDuration := time.Since(waitStart)
	holdStart := time.Now()
	defer func() {
		holdDuration := time.Since(holdStart)
		d.stateMutex.RUnlock()
		d.logLockTiming(operation, "read", waitDuration, holdDuration)
	}()
	cb(d.state)
}

// getFieldValue is a helper to get a reflection value for a given field array.
func (d *StoreData[S, SP, PS]) getFieldValue(field []any) reflect.Value {
	return getFieldValueFrom(d.stateReflect, field)
}

func getFieldValueFrom(stateReflect reflect.Value, field []any) reflect.Value {
	ds := stateReflect
	for _, f := range field {
		fv := reflect.ValueOf(f)
		kind := fv.Kind()
		switch kind {
		case reflect.String:
			ds = ds.FieldByName(fv.String())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
			switch ds.Kind() {
			case reflect.Slice:
				var idx int
				switch kind { //nolint: exhaustive // Why: Other types not supported
				case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
					idx = int(fv.Uint())
				case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
					idx = int(fv.Int())
				}
				ds = ds.Index(idx)
			case reflect.Map:
				ds = ds.MapIndex(fv)
			default:
				panic(fmt.Sprintf("Invalid int type %+v for field %+v of set %+v", ds.Kind(), f, field))
			}
		default:
			panic(fmt.Sprintf("Invalid field type %+v for field %+v of set %+v", kind, f, field))
		}

		// Walk into the pointer to keep going
		if ds.Kind() == reflect.Pointer {
			ds = ds.Elem()
		}
	}

	return ds
}

// ApplyPartial is the only allowed way to mutate store state -- build a partial with whatever needs changing, and then
// apply it.  It will end up applied to store state and send to all subscribers.
func (d *StoreData[S, SP, PS]) ApplyPartial(partial PS) {
	if d.localApplyErr != nil {
		panic(d.localApplyErr)
	}
	d.applyPartial(partial)
}

func (d *StoreData[S, SP, PS]) applyPartial(partial PS) {
	// Keep state mutation, detached output capture, and queue insertion in one
	// order when concurrent producers call ApplyPartial.
	d.applyOrderMutex.Lock()
	defer d.applyOrderMutex.Unlock()

	var fields [][]any
	var outputPartial Partial
	func() {
		waitStart := time.Now()
		d.stateMutex.Lock()
		waitDuration := time.Since(waitStart)
		holdStart := time.Now()
		defer func() {
			holdDuration := time.Since(holdStart)
			d.stateMutex.Unlock()
			d.logLockTiming("ApplyPartial", "write", waitDuration, holdDuration)
		}()
		fields = partial.ApplyTo(d.state)
		if len(fields) > 0 {
			// Generated states can clone just the applied fields without a
			// serializer round trip. Capture them under the state lock so the
			// worker sees the exact state installed by this partial.
			if provider, ok := any(d.state).(StateFieldPartialProvider); ok {
				outputPartial, _ = provider.PartialForFields(fields)
			}
		}
	}()
	fields = reduceFieldPaths(fields)
	if len(fields) == 0 {
		return
	}
	if outputPartial == nil {
		// Custom stores without generated changed-field snapshots fall back to
		// detaching the pruned partial through their serializer.
		var err error
		outputPartial, err = ClonePartial(partial)
		if err != nil {
			panic(fmt.Errorf("clone output partial for store %s: %w", d.name, err))
		}
	}
	d.enqueueOutput(storeDataOutput[S]{partial: outputPartial})
}

func (d *StoreData[S, SP, PS]) enqueueOutput(output storeDataOutput[S]) {
	d.outputQueueMutex.Lock()
	d.outputNextSequence++
	output.sequence = d.outputNextSequence
	output.subscriptionGeneration = d.outputSubscriptionGeneration
	if output.enqueuedAt.IsZero() {
		output.enqueuedAt = time.Now()
	}
	d.outputQueue = append(d.outputQueue, output)
	if output.partial != nil {
		d.outputEnqueuedCount++
	}
	depth := len(d.outputQueue)
	if !d.outputInFlightEnqueuedAt.IsZero() {
		depth++
	}
	if depth > d.outputMaxQueueDepth {
		d.outputMaxQueueDepth = depth
	}
	d.outputQueueCond.Signal()
	d.outputQueueMutex.Unlock()
}

// GetOutputStats returns a point-in-time snapshot of callback queue pressure.
func (d *StoreData[S, SP, PS]) GetOutputStats(now time.Time) StoreDataOutputStats {
	d.outputQueueMutex.Lock()
	defer d.outputQueueMutex.Unlock()

	depth := len(d.outputQueue)
	oldest := time.Time{}
	if len(d.outputQueue) > 0 {
		oldest = d.outputQueue[0].enqueuedAt
	}
	if !d.outputInFlightEnqueuedAt.IsZero() {
		depth++
		if oldest.IsZero() || d.outputInFlightEnqueuedAt.Before(oldest) {
			oldest = d.outputInFlightEnqueuedAt
		}
	}
	oldestAge := time.Duration(0)
	if !oldest.IsZero() && now.After(oldest) {
		oldestAge = now.Sub(oldest)
	}
	return StoreDataOutputStats{
		QueueDepth:               depth,
		MaxQueueDepth:            d.outputMaxQueueDepth,
		OldestQueuedAge:          oldestAge,
		ConfiguredBufferDuration: d.outputBufferDuration,
		EffectiveBufferDuration:  d.outputEffectiveBufferDuration,
		MaxBufferDuration:        d.outputMaxBufferDuration,
		AdaptiveGathering:        d.outputMaxBufferDuration > d.outputBufferDuration,
		AdaptiveBackoffCount:     d.outputBackoffCount,
		EnqueuedOutputCount:      d.outputEnqueuedCount,
		ProcessedOutputCount:     d.outputProcessedCount,
		EmittedBatchCount:        d.outputBatchCount,
		TotalQueueDelay:          d.outputTotalQueueDelay,
		TotalHandlingTime:        d.outputTotalHandlingTime,
		MaxBatchSize:             d.outputMaxBatchSize,
	}
}

func (d *StoreData[S, SP, PS]) runOutputWorker() {
	for {
		d.processOutputBatch(d.nextOutputBatch())
	}
}

func (d *StoreData[S, SP, PS]) processOutputBatch(outputs []storeDataOutput[S]) {
	startedAt := time.Now()
	partialBatch := len(outputs) > 0 && outputs[0].partial != nil
	defer func() {
		d.outputFiringSequence.Store(0)
		if recovered := recover(); recovered != nil {
			log.Printf("StoreData %s output callback panic: %v", d.name, recovered)
		}
		d.finishOutputBatch(time.Now(), startedAt, outputs, partialBatch)
	}()

	if len(outputs) == 0 {
		return
	}
	if outputs[0].barrier != nil {
		close(outputs[0].barrier)
		return
	}
	if outputs[0].replaceWith != nil {
		d.outputState = SP(outputs[0].replaceWith)
		return
	}

	d.outputFiringSequence.Store(outputs[len(outputs)-1].sequence)
	partial := outputs[0].partial
	for _, output := range outputs[1:] {
		output.partial.MergeOntoPartial(partial)
	}
	fields := reduceFieldPaths(partial.ApplyTo(d.outputState))
	if len(fields) == 0 {
		return
	}
	d.fireCallbacks(fields, partial, reflect.ValueOf(d.outputState).Elem())
}

func (d *StoreData[S, SP, PS]) nextOutputBatch() []storeDataOutput[S] {
	d.outputQueueMutex.Lock()
	for len(d.outputQueue) == 0 {
		d.outputQueueCond.Wait()
	}
	now := time.Now()
	d.adjustOutputGatheringLocked(now)
	effectiveBufferDuration := d.outputEffectiveBufferDuration
	if d.outputQueue[0].replaceWith != nil || d.outputQueue[0].barrier != nil || effectiveBufferDuration <= 0 {
		output := d.outputQueue[0]
		d.outputQueue = d.outputQueue[1:]
		d.outputInFlightEnqueuedAt = output.enqueuedAt
		d.outputInFlightGatherDuration = 0
		d.outputQueueMutex.Unlock()
		return []storeDataOutput[S]{output}
	}
	d.outputQueueMutex.Unlock()

	time.Sleep(effectiveBufferDuration)

	d.outputQueueMutex.Lock()
	count := 0
	firstSubscriptionGeneration := d.outputQueue[0].subscriptionGeneration
	for count < len(d.outputQueue) && d.outputQueue[count].replaceWith == nil && d.outputQueue[count].barrier == nil &&
		d.outputQueue[count].subscriptionGeneration == firstSubscriptionGeneration {
		count++
	}
	outputs := append([]storeDataOutput[S](nil), d.outputQueue[:count]...)
	d.outputQueue = d.outputQueue[count:]
	if len(outputs) > 0 {
		d.outputInFlightEnqueuedAt = outputs[0].enqueuedAt
		d.outputInFlightGatherDuration = effectiveBufferDuration
	}
	d.outputQueueMutex.Unlock()
	return outputs
}

// WaitForOutputIdle waits until the serialized output worker has processed all
// updates enqueued before this call. It is primarily useful for deterministic
// tests and orderly shutdown checks.
func (d *StoreData[S, SP, PS]) WaitForOutputIdle(timeout time.Duration) bool {
	barrier := make(chan struct{})
	d.applyOrderMutex.Lock()
	d.enqueueOutput(storeDataOutput[S]{barrier: barrier})
	d.applyOrderMutex.Unlock()
	if timeout <= 0 {
		<-barrier
		return true
	}
	select {
	case <-barrier:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (d *StoreData[S, SP, PS]) finishOutputBatch(
	now time.Time,
	startedAt time.Time,
	outputs []storeDataOutput[S],
	partialBatch bool,
) {
	d.outputQueueMutex.Lock()
	handlingTime := now.Sub(startedAt)
	batchAge := time.Duration(0)
	if !d.outputInFlightEnqueuedAt.IsZero() && now.After(d.outputInFlightEnqueuedAt) {
		batchAge = now.Sub(d.outputInFlightEnqueuedAt)
	}
	if partialBatch {
		d.outputProcessedCount += uint64(len(outputs))
		d.outputBatchCount++
		d.outputTotalHandlingTime += handlingTime
		d.outputLastBatchSize = len(outputs)
		d.outputLastBatchAge = batchAge
		d.outputLastBatchGatherDuration = d.outputInFlightGatherDuration
		d.outputLastBatchHandlingTime = handlingTime
		if len(outputs) > d.outputMaxBatchSize {
			d.outputMaxBatchSize = len(outputs)
		}
		for _, output := range outputs {
			if startedAt.After(output.enqueuedAt) {
				d.outputTotalQueueDelay += startedAt.Sub(output.enqueuedAt)
			}
		}
	}
	d.outputInFlightEnqueuedAt = time.Time{}
	d.outputInFlightGatherDuration = 0
	d.outputQueueMutex.Unlock()
}

func (d *StoreData[S, SP, PS]) adjustOutputGatheringLocked(now time.Time) {
	oldestAge := d.oldestQueuedAgeLocked(now)
	if oldestAge >= adaptiveOutputPressureAge {
		d.backoffOutputGatheringLocked(now, len(d.outputQueue), oldestAge)
		return
	}
	if d.outputEffectiveBufferDuration <= d.outputBufferDuration || len(d.outputQueue) > 1 ||
		now.Sub(d.outputLastBackoff) < adaptiveOutputRecoveryInterval ||
		now.Sub(d.outputLastRecoveryAdjustment) < adaptiveOutputRecoveryInterval {
		return
	}

	delta := (d.outputEffectiveBufferDuration - d.outputBufferDuration) / 8
	if delta < time.Millisecond {
		delta = time.Millisecond
	}
	d.outputEffectiveBufferDuration -= delta
	if d.outputEffectiveBufferDuration < d.outputBufferDuration {
		d.outputEffectiveBufferDuration = d.outputBufferDuration
	}
	d.outputLastRecoveryAdjustment = now
	if d.outputEffectiveBufferDuration == d.outputBufferDuration {
		log.Printf("StoreData %s output queue recovered; gather window returned to %s",
			d.name, d.outputBufferDuration)
	}
}

func (d *StoreData[S, SP, PS]) oldestQueuedAgeLocked(now time.Time) time.Duration {
	if len(d.outputQueue) == 0 || !now.After(d.outputQueue[0].enqueuedAt) {
		return 0
	}
	return now.Sub(d.outputQueue[0].enqueuedAt)
}

func (d *StoreData[S, SP, PS]) outputPressureDetails(
	depth int,
	oldestAge time.Duration,
) string {
	lastBatchExcess := d.outputLastBatchAge - d.outputLastBatchGatherDuration
	if lastBatchExcess < 0 {
		lastBatchExcess = 0
	}
	return fmt.Sprintf(
		"reason=queue-age depth=%d oldest=%s age-threshold=%s "+
			"last-batch-inputs=%d last-batch-age=%s last-batch-gather=%s "+
			"last-batch-handling=%s last-batch-excess=%s",
		depth,
		oldestAge.Round(time.Millisecond),
		adaptiveOutputPressureAge,
		d.outputLastBatchSize,
		d.outputLastBatchAge.Round(time.Millisecond),
		d.outputLastBatchGatherDuration,
		d.outputLastBatchHandlingTime.Round(time.Millisecond),
		lastBatchExcess.Round(time.Millisecond),
	)
}

func (d *StoreData[S, SP, PS]) backoffOutputGatheringLocked(
	now time.Time,
	depth int,
	oldestAge time.Duration,
) {
	details := d.outputPressureDetails(depth, oldestAge)
	if d.outputMaxBufferDuration <= d.outputBufferDuration {
		if oldestAge >= time.Second && now.Sub(d.outputLastPressureLog) >= outputPressureLogInterval {
			log.Printf("StoreData %s output queue falling behind: %s adaptive-gathering=disabled",
				d.name, details)
			d.outputLastPressureLog = now
		}
		return
	}
	next := d.outputEffectiveBufferDuration * 2
	if next < minimumAdaptiveOutputBufferDuration {
		next = minimumAdaptiveOutputBufferDuration
	}
	if next > d.outputMaxBufferDuration {
		next = d.outputMaxBufferDuration
	}
	if next <= d.outputEffectiveBufferDuration {
		return
	}
	previous := d.outputEffectiveBufferDuration
	d.outputEffectiveBufferDuration = next
	d.outputBackoffCount++
	d.outputLastBackoff = now
	d.outputLastRecoveryAdjustment = now
	log.Printf("StoreData %s output queue pressure: %s gather=%s->%s base=%s max=%s backoff=%d",
		d.name, details, previous, next, d.outputBufferDuration, d.outputMaxBufferDuration, d.outputBackoffCount)
}

func (d *StoreData[S, SP, PS]) fireCallbacks(fields [][]any, partial Partial, stateReflect reflect.Value) {
	d.partialCallbacks.Fire(d.name, fields, partial)

	for _, f := range fields {
		d.triggerSubs(f, stateReflect)
	}
}

// DecodeAndSetFullState will use reflection to decode the right state struct for the store and then set it
func (d *StoreData[S, SP, PS]) DecodeAndSetFullState(b []byte) error {
	t := reflect.TypeFor[S]()
	nrv := reflect.New(t)
	iv := nrv.Interface()
	if err := iv.(Serializable).Deserialize(binarystreams.NewReaderFromBytes(b), nil); err != nil {
		return err
	}
	d.applyOrderMutex.Lock()
	defer d.applyOrderMutex.Unlock()
	waitStart := time.Now()
	d.stateMutex.Lock()
	waitDuration := time.Since(waitStart)
	holdStart := time.Now()
	defer func() {
		holdDuration := time.Since(holdStart)
		d.stateMutex.Unlock()
		d.logLockTiming("DecodeAndSetFullState", "write", waitDuration, holdDuration)
	}()
	*d.state = *iv.(SP)
	replacement, err := detachedState[S, SP](d.state)
	if err != nil {
		return fmt.Errorf("clone replacement output state for store %s: %w", d.name, err)
	}
	d.enqueueOutput(storeDataOutput[S]{replaceWith: replacement})
	return nil
}

// DecodeAndApplyPartial will use reflection to decode the right partial for the store and then apply it
func (d *StoreData[S, SP, PS]) DecodeAndApplyPartial(b []byte) error {
	t := reflect.TypeFor[PS]().Elem()
	nrv := reflect.New(t)
	iv := nrv.Interface()
	if err := iv.(Serializable).Deserialize(binarystreams.NewReaderFromBytes(b), nil); err != nil {
		return err
	}
	d.applyPartial(iv.(PS))
	return nil
}

func (d *StoreData[S, SP, PS]) logLockTiming(operation string, lockType string, waitDuration time.Duration, holdDuration time.Duration) {
	if waitDuration < storeDataLockWarnAfter && holdDuration < storeDataLockWarnAfter {
		return
	}

	log.Printf(
		"StoreData %s %s %s lock timing exceeded %s: wait=%s hold=%s",
		d.name,
		operation,
		lockType,
		storeDataLockWarnAfter,
		waitDuration,
		holdDuration,
	)
}

// triggerSubs is an internal helper to break up triggering subscriptions from the field changes themselves
func (d *StoreData[S, SP, PS]) triggerSubs(field []any, stateReflect reflect.Value) {
	// Get the set of possible subscriptions to fire
	possibleSubs := make(map[*subInfo]bool)

	d.subscriptionsMutex.RLock()
	t := d.subscriptions
	for _, f := range field {
		for _, s := range t.subs {
			possibleSubs[s] = true
		}

		c, exists := t.children[f]
		if !exists {
			break
		}
		t = c
	}
	d.subscriptionsMutex.RUnlock()

	// Now filter each one to make sure it actually needs firing
	for s := range possibleSubs {
		if d.outputFiringSequence.Load() < s.startSequence {
			continue
		}
		// Only need to walk up the smaller of the two sets of fields and ensure equality up to that far -- any mismatches
		// above that level will always still be fine, in either direction.
		mismatch := false
		maxField := len(s.field)
		if len(field) < maxField {
			maxField = len(field)
		}
		for i := 0; i < maxField; i++ {
			if s.field[i] != field[i] {
				mismatch = true
				break
			}
		}
		if mismatch {
			continue
		}

		switch {
		case s.takesType && s.takesField:
			fv := getFieldValueFrom(stateReflect, s.field)
			s.callback.Call([]reflect.Value{reflect.ValueOf(field), fv})
		case s.takesType:
			fv := getFieldValueFrom(stateReflect, s.field)
			s.callback.Call([]reflect.Value{fv})
		case s.takesField:
			s.callback.Call([]reflect.Value{reflect.ValueOf(field)})
		default:
			s.callback.Call([]reflect.Value{})
		}
	}
}

// SubscribeToField subscribes to updates of the passed field and any entries under it.  The callback will be called with a
// copy of the entire data structure at the subscribed level, even if a subfield was updated, with the list of fields from the update.
func (d *StoreData[S, SP, PS]) SubscribeToField(field []any, callback any) {
	ct := reflect.TypeOf(callback)
	if ct.Kind() != reflect.Func {
		panic(fmt.Sprintf("Non-func (%T) passed to SubscribeToField for field %+v", callback, field))
	}

	ft := d.getFieldValue(field).Type()
	takesField := false
	takesType := false
	switch ct.NumIn() {
	case 2:
		takesType = true
		takesField = true
		if ct.In(1) != ft {
			panic(fmt.Sprintf("SubscribeToField callback for field %+v has wrong arg 1 type (got %s, expected %s)",
				field, ct.In(1).String(), ft.String()))
		}
	case 1:
		switch ct.In(0) {
		case typeAnyArrayOfAny:
			takesField = true
		case ft:
			takesType = true
		default:
			panic(fmt.Sprintf("SubscribeToField callback for field %+v has wrong arg 0 type (got %s, expected %s or %s)",
				field, ct.In(0).String(), typeAnyArrayOfAny.String(), ft.String()))
		}
	case 0:
		takesType = false
		takesField = false
	default:
		panic(fmt.Sprintf("SubscribeToField callback for field %+v has wrong number of args (got %d, expected 1 or 2)", field, ct.NumIn()))
	}

	d.outputQueueMutex.Lock()
	defer d.outputQueueMutex.Unlock()
	d.subscriptionsMutex.Lock()
	defer d.subscriptionsMutex.Unlock()
	d.outputSubscriptionGeneration++

	si := &subInfo{
		field:         field,
		callback:      reflect.ValueOf(callback),
		takesType:     takesType,
		takesField:    takesField,
		startSequence: d.outputNextSequence + 1,
	}

	// Find/build the sub tier for the requested field at full depth
	t := d.subscriptions
	for _, f := range field {
		c, exists := t.children[f]
		if !exists {
			c = newFieldSubTier(t)
			t.children[f] = c
		}
		t = c
	}

	// If you subscribe at a deep level (A,B,C), you need updates for A and A,B, so we want to walk all the way up the chain
	// and store subscriptions at A; A,B; and A,B,C.
	for {
		t.subs = append(t.subs, si)
		t = t.parent
		if t == nil {
			break
		}
	}
}

// subInfo is a helper structure for storing information about a subscription
type subInfo struct {
	field         []any
	startSequence uint64

	callback   reflect.Value
	takesField bool
	takesType  bool
}

// fieldSubTier holds a set of subscriptions for a "tier", which is what I'm thinking of as one branch of the field structure.
type fieldSubTier struct {
	parent   *fieldSubTier
	children map[any]*fieldSubTier

	subs []*subInfo
}

// newFieldSubTier returns a new subTier under a given parent.
func newFieldSubTier(parent *fieldSubTier) *fieldSubTier {
	return &fieldSubTier{
		parent:   parent,
		children: make(map[any]*fieldSubTier),
		subs:     []*subInfo{},
	}
}
