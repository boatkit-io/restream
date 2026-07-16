package restream

import (
	"bytes"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/boatkit-io/restream/pkg/binarystreams"
	"github.com/sirupsen/logrus"
)

func resolveEmitMessage(t *testing.T, msg emitMessage) emitMessage {
	t.Helper()
	resolved, err := msg.resolve()
	if err != nil {
		t.Fatalf("resolve emit message failed: %v", err)
	}
	return resolved
}

type viewerSocketTestState struct {
	Values map[string]int
	Other  int
}

type viewerSocketTestPartial struct {
	Values *PartialMap[string, int]
	Other  *int
}

var viewerSocketTestStateFieldInfo = []FieldInfo{
	{
		Name:     "Values",
		FieldIdx: 0,
		FieldID:  1,
		VarInfo: &VarInfoMap{
			KeyType:  &VarInfoPrimitive{DataType: SerializationTypeString},
			ElemType: &VarInfoPrimitive{DataType: SerializationTypeInt64},
		},
	},
	{
		Name:     "Other",
		FieldIdx: 1,
		FieldID:  2,
		VarInfo:  &VarInfoPrimitive{DataType: SerializationTypeInt64},
	},
}

var viewerSocketTestStateFieldMap = map[byte]*FieldInfo{
	1: &viewerSocketTestStateFieldInfo[0],
	2: &viewerSocketTestStateFieldInfo[1],
}

func (s *viewerSocketTestState) RestreamClone() *viewerSocketTestState {
	if s == nil {
		return nil
	}
	clone := &viewerSocketTestState{
		Other: s.Other,
	}
	if s.Values != nil {
		clone.Values = make(map[string]int, len(s.Values))
		for key, value := range s.Values {
			clone.Values[key] = value
		}
	}
	return clone
}

func (s *viewerSocketTestState) Serialize(w *binarystreams.Writer, _ *VarInfoStruct) error {
	wi, buf := binarystreams.NewMemoryWriter()
	if err := SerializeField(s.Values, &viewerSocketTestStateFieldInfo[0], wi); err != nil {
		return err
	}
	if err := SerializeField(s.Other, &viewerSocketTestStateFieldInfo[1], wi); err != nil {
		return err
	}
	if err := wi.Flush(); err != nil {
		return err
	}
	b := buf.Bytes()
	if err := SerializePacked64(uint64(len(b)), w); err != nil {
		return err
	}
	return w.WriteBytes(b)
}

func (s *viewerSocketTestState) Deserialize(r *binarystreams.Reader, _ *VarInfoStruct) error {
	fieldPtrs := []any{
		&s.Values,
		&s.Other,
	}
	sl, err := DeserializePacked64[uint64](r)
	if err != nil {
		return err
	}
	ri, err := r.Slice(int(sl))
	if err != nil {
		return err
	}
	return DeserializeFielded(ri, viewerSocketTestStateFieldInfo, viewerSocketTestStateFieldMap, fieldPtrs)
}

var viewerSocketTestPartialFieldInfo = []FieldInfo{
	{
		Name:     "Values",
		FieldIdx: 0,
		FieldID:  1,
		VarInfo: &VarInfoPointer{
			SubType: &VarInfoStruct{
				Name:    "PartialMap",
				Package: "restream",
				GenericTypes: []VarInfo{
					&VarInfoPrimitive{DataType: SerializationTypeString},
					&VarInfoPrimitive{DataType: SerializationTypeInt64},
				},
			},
		},
	},
	{
		Name:     "Other",
		FieldIdx: 1,
		FieldID:  2,
		VarInfo: &VarInfoPointer{
			SubType: &VarInfoPrimitive{DataType: SerializationTypeInt64},
		},
	},
}

var viewerSocketTestPartialFieldMap = map[byte]*FieldInfo{
	1: &viewerSocketTestPartialFieldInfo[0],
	2: &viewerSocketTestPartialFieldInfo[1],
}

func (p *viewerSocketTestPartial) Serialize(w *binarystreams.Writer, _ *VarInfoStruct) error {
	wi, buf := binarystreams.NewMemoryWriter()
	if err := SerializeField(p.Values, &viewerSocketTestPartialFieldInfo[0], wi); err != nil {
		return err
	}
	if err := SerializeField(p.Other, &viewerSocketTestPartialFieldInfo[1], wi); err != nil {
		return err
	}
	if err := wi.Flush(); err != nil {
		return err
	}
	b := buf.Bytes()
	if err := SerializePacked64(uint64(len(b)), w); err != nil {
		return err
	}
	return w.WriteBytes(b)
}

func (p *viewerSocketTestPartial) Deserialize(r *binarystreams.Reader, _ *VarInfoStruct) error {
	fieldPtrs := []any{
		&p.Values,
		&p.Other,
	}
	sl, err := DeserializePacked64[uint64](r)
	if err != nil {
		return err
	}
	ri, err := r.Slice(int(sl))
	if err != nil {
		return err
	}
	return DeserializeFielded(ri, viewerSocketTestPartialFieldInfo, viewerSocketTestPartialFieldMap, fieldPtrs)
}

func (p *viewerSocketTestPartial) MergeOntoPartial(por any) {
	po := por.(*viewerSocketTestPartial)
	if p.Values != nil {
		if po.Values == nil {
			po.Values = p.Values
		} else {
			p.Values.MergeOntoPartial(po.Values)
		}
	}
	if p.Other != nil {
		po.Other = p.Other
	}
}

func (p *viewerSocketTestPartial) ApplyTo(por any) [][]any {
	po := por.(*viewerSocketTestState)
	ret := [][]any{}
	if p.Values != nil {
		fs := p.Values.ApplyTo(&po.Values)
		for _, f := range fs {
			ret = append(ret, append([]any{"Values"}, f...))
		}
	}
	if p.Other != nil {
		po.Other = *p.Other
		ret = append(ret, []any{"Other"})
	}
	return ret
}

func (p *viewerSocketTestPartial) FilterToFields(fields [][]any) (Partial, bool) {
	ret := &viewerSocketTestPartial{}
	included := false
	for _, field := range fields {
		if len(field) == 0 {
			return p, true
		}
	}
	if p.Values != nil {
		childFields := ChildFieldsForField(fields, "Values")
		if len(childFields) > 0 {
			filtered, ok := FilterPartialToFields(p.Values, childFields)
			if ok {
				ret.Values = filtered
				included = true
			}
		}
	}
	if p.Other != nil {
		childFields := ChildFieldsForField(fields, "Other")
		if len(childFields) > 0 {
			ret.Other = p.Other
			included = true
		}
	}
	return ret, included
}

func TestViewerSocketFiltersDifferentKeySubscriptionsIndependently(t *testing.T) {
	update := &viewerSocketTestPartial{
		Values: NewPartialMap[string, int]().
			Set("a", 1).
			Set("b", 2).
			Set("c", 3),
	}
	fields := update.ApplyTo(&viewerSocketTestState{Values: map[string]int{}})

	partialA, ok := testSocketWithSubs("values%&a").
		partialForSubscriptions(viewerSocketTestStoreName, fields, update)
	if !ok {
		t.Fatal("expected subscription for values/a to receive a partial")
	}
	stateA := viewerSocketTestState{Values: map[string]int{}}
	fieldsA := partialA.ApplyTo(&stateA)
	assertFieldsContainOnly(t, fieldsA, []any{"Values", "a"})
	assertMapEqual(t, map[string]int{"a": 1}, stateA.Values)

	partialB, ok := testSocketWithSubs("values%&b").
		partialForSubscriptions(viewerSocketTestStoreName, fields, update)
	if !ok {
		t.Fatal("expected subscription for values/b to receive a partial")
	}
	stateB := viewerSocketTestState{Values: map[string]int{}}
	fieldsB := partialB.ApplyTo(&stateB)
	assertFieldsContainOnly(t, fieldsB, []any{"Values", "b"})
	assertMapEqual(t, map[string]int{"b": 2}, stateB.Values)

	if _, ok := testSocketWithSubs("values%&d").
		partialForSubscriptions(viewerSocketTestStoreName, fields, update); ok {
		t.Fatal("unexpected partial for an unrelated key subscription")
	}
}

func TestViewerSocketBroadMapUpdateCascadesToNarrowKeySubscriptions(t *testing.T) {
	update := &viewerSocketTestPartial{
		Values: NewPartialMap[string, int]().
			SetWhole(map[string]int{
				"a": 1,
				"b": 2,
			}),
	}
	fields := update.ApplyTo(&viewerSocketTestState{Values: map[string]int{}})
	assertFieldsContainOnly(t, fields, []any{"Values"})

	partialA, ok := testSocketWithSubs("values%&a").
		partialForSubscriptions(viewerSocketTestStoreName, fields, update)
	if !ok {
		t.Fatal("expected broad map update to match values/a")
	}
	stateA := viewerSocketTestState{Values: map[string]int{"a": 10, "b": 20, "c": 30}}
	fieldsA := partialA.ApplyTo(&stateA)
	assertFieldsContainOnly(t, fieldsA, []any{"Values", "a"})
	assertMapEqual(t, map[string]int{"a": 1, "b": 20, "c": 30}, stateA.Values)

	partialC, ok := testSocketWithSubs("values%&c").
		partialForSubscriptions(viewerSocketTestStoreName, fields, update)
	if !ok {
		t.Fatal("expected broad map update to match deleted values/c")
	}
	stateC := viewerSocketTestState{Values: map[string]int{"a": 10, "b": 20, "c": 30}}
	fieldsC := partialC.ApplyTo(&stateC)
	assertFieldsContainOnly(t, fieldsC, []any{"Values", "c"})
	assertMapEqual(t, map[string]int{"a": 10, "b": 20}, stateC.Values)
}

func TestViewerSocketRootUpdateCascadesToNarrowKeySubscriptions(t *testing.T) {
	update := &viewerSocketTestPartial{
		Values: NewPartialMap[string, int]().
			Set("a", 1).
			Set("b", 2),
		Other: Ptr(3),
	}

	partialA, ok := testSocketWithSubs("values%&a").
		partialForSubscriptions(viewerSocketTestStoreName, [][]any{{}}, update)
	if !ok {
		t.Fatal("expected root update to match values/a")
	}
	stateA := viewerSocketTestState{Values: map[string]int{}}
	fieldsA := partialA.ApplyTo(&stateA)
	assertFieldsContainOnly(t, fieldsA, []any{"Values", "a"})
	assertMapEqual(t, map[string]int{"a": 1}, stateA.Values)
	if stateA.Other != 0 {
		t.Fatalf("expected unrelated field to remain untouched, got %d", stateA.Other)
	}
}

func TestViewerSocketWholeStoreSubscriptionDoesNotFilterKeys(t *testing.T) {
	update := &viewerSocketTestPartial{
		Values: NewPartialMap[string, int]().
			Set("a", 1).
			Set("b", 2),
		Other: Ptr(3),
	}
	fields := update.ApplyTo(&viewerSocketTestState{Values: map[string]int{}})

	filtered, ok := testSocketWithSubs("").
		partialForSubscriptions(viewerSocketTestStoreName, fields, update)
	if !ok {
		t.Fatal("expected whole-store subscription to receive a partial")
	}
	if filtered != update {
		t.Fatal("expected whole-store subscription to receive the original unfiltered partial")
	}

	state := viewerSocketTestState{Values: map[string]int{}}
	filtered.ApplyTo(&state)
	assertMapEqual(t, map[string]int{"a": 1, "b": 2}, state.Values)
	if state.Other != 3 {
		t.Fatalf("expected Other to be updated, got %d", state.Other)
	}
}

func TestViewerSocketKeyedCatchupUsesRelayStorePartial(t *testing.T) {
	const (
		sourceKey = "Water_Depth_Auto"
		otherKey  = "Water_Depth_22"
	)
	store := NewRelayStore[
		viewerSocketTestState,
		*viewerSocketTestState,
		*viewerSocketTestPartial,
	](viewerSocketTestStoreName, &viewerSocketTestState{
		Values: map[string]int{
			sourceKey: 10,
			otherKey:  20,
		},
	}, AccessLevelPublic)
	registry, err := NewStoreRegistry([]Store{store})
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}
	socket := &socketTracker{
		sr:                 registry,
		emitQueue:          make(chan emitMessage, 1),
		storeSubscriptions: map[string]map[string]int{},
	}

	socket.onStoreSubscription(StoreSubscriptionMessage{
		StoreName: viewerSocketTestStoreName,
		Action:    Subscribe,
		Key:       "values%&" + sourceKey,
	})

	emitted := resolveEmitMessage(t, <-socket.emitQueue)
	if emitted.Name != SocketEventNameStoreUpdate {
		t.Fatalf("expected %s event, got %s", SocketEventNameStoreUpdate, emitted.Name)
	}
	update, ok := emitted.Message.(StoreUpdatePartialMessage)
	if !ok {
		t.Fatalf("expected partial store update, got %T", emitted.Message)
	}
	if update.Kind != StoreUpdatePartial || update.StoreName != viewerSocketTestStoreName {
		t.Fatalf("expected partial update for %s, got %#v", viewerSocketTestStoreName, update.StoreUpdateMessage)
	}

	var partial viewerSocketTestPartial
	if err := partial.Deserialize(binarystreams.NewReaderFromBytes(update.Partial.Bytes()), nil); err != nil {
		t.Fatalf("partial deserialize failed: %v", err)
	}
	state := &viewerSocketTestState{Values: map[string]int{}}
	fields := partial.ApplyTo(state)
	assertFieldsContainOnly(t, fields, []any{"Values", sourceKey})
	assertMapEqual(t, map[string]int{sourceKey: 10}, state.Values)

	if _, subscribed := socket.storeSubscriptions[viewerSocketTestStoreName]["values%&"+sourceKey]; !subscribed {
		t.Fatalf("expected socket to track keyed subscription for %s", sourceKey)
	}
}

func TestViewerSocketRejectsSubscriptionBelowStoreMinimumAccess(t *testing.T) {
	store := NewRelayStore[
		viewerSocketTestState,
		*viewerSocketTestState,
		*viewerSocketTestPartial,
	](viewerSocketTestStoreName, &viewerSocketTestState{
		Values: map[string]int{"a": 10},
	}, AccessLevel(2))
	registry, err := NewStoreRegistry([]Store{store})
	if err != nil {
		t.Fatalf("NewStoreRegistry failed: %v", err)
	}
	log := logrus.New()
	log.SetOutput(io.Discard)
	socket := &socketTracker{
		log:                log,
		sr:                 registry,
		emitQueue:          make(chan emitMessage, 1),
		storeSubscriptions: map[string]map[string]int{},
		accessLookup: func() (AccessLevel, error) {
			return AccessLevel(1), nil
		},
	}

	socket.onStoreSubscription(StoreSubscriptionMessage{
		StoreName: viewerSocketTestStoreName,
		Action:    Subscribe,
		Key:       "values%&a",
	})

	select {
	case emitted := <-socket.emitQueue:
		t.Fatalf("unexpected store update for denied subscription: %#v", emitted)
	default:
	}
	if _, subscribed := socket.storeSubscriptions[viewerSocketTestStoreName]; subscribed {
		t.Fatalf("denied subscription should not be tracked: %#v", socket.storeSubscriptions)
	}
	info := registry.storeMap[viewerSocketTestStoreName]
	if info.ActiveSubCount != 0 {
		t.Fatalf("denied subscription incremented registry count to %d", info.ActiveSubCount)
	}
}

func TestViewerSocketEmitsEventDispatcherMessages(t *testing.T) {
	socket := &socketTracker{
		emitQueue: make(chan emitMessage, 1),
	}

	socket.EventCallback("call", []byte{1, 2, 3})

	emitted := resolveEmitMessage(t, <-socket.emitQueue)
	if emitted.Name != SocketEventNameEvent {
		t.Fatalf("expected %s event, got %s", SocketEventNameEvent, emitted.Name)
	}
	message, ok := emitted.Message.(EventMessage)
	if !ok {
		t.Fatalf("expected event message, got %T", emitted.Message)
	}
	if message.EventName != "call" {
		t.Fatalf("expected event name call, got %s", message.EventName)
	}
	if string(message.Event.Bytes()) != string([]byte{1, 2, 3}) {
		t.Fatalf("unexpected event bytes: %#v", message.Event.Bytes())
	}
}

func TestViewerSocketEmitMessageDoesNotBlockWhenQueueIsFull(t *testing.T) {
	socket := &socketTracker{
		emitQueue: make(chan emitMessage, 1),
	}
	socket.emitQueue <- emitMessage{Name: "existing"}

	done := make(chan struct{})
	go func() {
		socket.emitMessage(SocketEventNameEvent, EventMessage{})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("emitMessage blocked on a full emit queue")
	}
}

func TestViewerSocketFullEmitQueueLogsMessageSummary(t *testing.T) {
	var logOutput bytes.Buffer
	log := logrus.New()
	log.SetOutput(&logOutput)
	log.SetFormatter(&logrus.TextFormatter{
		DisableColors:    true,
		DisableTimestamp: true,
	})

	socket := &socketTracker{
		log:       log,
		emitQueue: make(chan emitMessage, 5),
	}
	socket.emitQueue <- emitMessage{Name: SocketEventNameEvent, Message: EventMessage{EventName: "alarm"}}
	socket.emitQueue <- emitMessage{
		Name: SocketEventNameStoreUpdate,
		Message: StoreUpdatePartialMessage{StoreUpdateMessage: StoreUpdateMessage{
			StoreName: "store-a",
			Kind:      StoreUpdatePartial,
		}},
	}
	socket.emitQueue <- emitMessage{
		Name: SocketEventNameStoreUpdate,
		Message: StoreUpdateFullMessage{StoreUpdateMessage: StoreUpdateMessage{
			StoreName: "store-a",
			Kind:      StoreUpdateFull,
		}},
	}
	socket.emitQueue <- emitMessage{
		Name: SocketEventNameStoreUpdate,
		Message: StoreUpdatePartialMessage{StoreUpdateMessage: StoreUpdateMessage{
			StoreName: "store-b",
			Kind:      StoreUpdatePartial,
		}},
	}
	socket.emitQueue <- emitMessage{
		Name: SocketEventNameStoreUpdate,
		Message: StoreUpdatePartialMessage{StoreUpdateMessage: StoreUpdateMessage{
			StoreName: "store-a",
			Kind:      StoreUpdatePartial,
		}},
	}

	socket.emitMessage(SocketEventNameStoreUpdate, StoreUpdatePartialMessage{
		StoreUpdateMessage: StoreUpdateMessage{
			StoreName: "store-c",
			Kind:      StoreUpdatePartial,
		},
	})

	logged := logOutput.String()
	expectedParts := []string{
		`while sending storeupdate/partial store=\"store-c\"`,
		`queued messages (5/5): event: 1`,
		`storeupdate/full store=\"store-a\": 1`,
		`storeupdate/partial store=\"store-a\": 2`,
		`storeupdate/partial store=\"store-b\": 1`,
	}
	for _, expected := range expectedParts {
		if !strings.Contains(logged, expected) {
			t.Errorf("log output %q does not contain %q", logged, expected)
		}
	}
	if socket.emitQueue != nil {
		t.Fatal("full emit queue was not closed")
	}
}

func TestViewerSocketPartialCallbackSerializesBeforeQueue(t *testing.T) {
	serializeCount := 0
	socket := &socketTracker{
		emitQueue: make(chan emitMessage, 1),
		storeSubscriptions: map[string]map[string]int{
			viewerSocketTestStoreName: {
				"": 1,
			},
		},
	}

	socket.PartialCallback(
		viewerSocketTestStoreName,
		[][]any{{"Other"}},
		&viewerSocketSerializeCountingPartial{
			onSerialize: func() {
				serializeCount++
			},
		},
	)

	if serializeCount != 1 {
		t.Fatalf("PartialCallback serialized %d times, want 1", serializeCount)
	}
	emitted := <-socket.emitQueue
	resolved := resolveEmitMessage(t, emitted)
	if serializeCount != 1 {
		t.Fatalf("emit resolve serialized again: %d", serializeCount)
	}
	if resolved.Name != SocketEventNameStoreUpdate {
		t.Fatalf("expected %s event, got %s", SocketEventNameStoreUpdate, resolved.Name)
	}
	update, ok := resolved.Message.(StoreUpdatePartialMessage)
	if !ok {
		t.Fatalf("expected partial store update, got %T", resolved.Message)
	}
	if update.Kind != StoreUpdatePartial || update.StoreName != viewerSocketTestStoreName {
		t.Fatalf("expected partial update for %s, got %#v", viewerSocketTestStoreName, update.StoreUpdateMessage)
	}
}

func TestViewerSocketQueuedPartialUpdateDoesNotShareOriginalMap(t *testing.T) {
	socket := &socketTracker{
		emitQueue: make(chan emitMessage, 1),
		storeSubscriptions: map[string]map[string]int{
			viewerSocketTestStoreName: {
				"": 1,
			},
		},
	}
	partial := &viewerSocketTestPartial{
		Values: NewPartialMap[string, int]().Set("a", 1),
	}
	fields := partial.ApplyTo(&viewerSocketTestState{Values: map[string]int{}})

	socket.PartialCallback(viewerSocketTestStoreName, fields, partial)
	partial.Values.Set("a", 2)

	resolved := resolveEmitMessage(t, <-socket.emitQueue)
	update, ok := resolved.Message.(StoreUpdatePartialMessage)
	if !ok {
		t.Fatalf("expected partial store update, got %T", resolved.Message)
	}
	var queued viewerSocketTestPartial
	if err := queued.Deserialize(binarystreams.NewReaderFromBytes(update.Partial.Bytes()), nil); err != nil {
		t.Fatalf("partial deserialize failed: %v", err)
	}

	state := &viewerSocketTestState{Values: map[string]int{}}
	queued.ApplyTo(state)
	assertMapEqual(t, map[string]int{"a": 1}, state.Values)
}

const viewerSocketTestStoreName = "test-store"

type viewerSocketSerializeCountingPartial struct {
	onSerialize func()
}

func (p *viewerSocketSerializeCountingPartial) Serialize(*binarystreams.Writer, *VarInfoStruct) error {
	if p.onSerialize != nil {
		p.onSerialize()
	}
	return nil
}

func (*viewerSocketSerializeCountingPartial) Deserialize(*binarystreams.Reader, *VarInfoStruct) error {
	return nil
}

func (*viewerSocketSerializeCountingPartial) MergeOntoPartial(any) {
}

func (*viewerSocketSerializeCountingPartial) ApplyTo(any) [][]any {
	return [][]any{{"Other"}}
}

func TestViewerSocketDisconnectCleanupIsIdempotent(_ *testing.T) {
	socket := &socketTracker{
		emitQueue:          make(chan emitMessage),
		storeSubscriptions: map[string]map[string]int{},
	}

	socket.onDisconnect()
	socket.onDisconnect()
}

func TestViewerSocketDisconnectCleanupAllowsNilEmitQueue(_ *testing.T) {
	socket := &socketTracker{
		storeSubscriptions: map[string]map[string]int{},
	}

	socket.onDisconnect()
	socket.onDisconnect()
}

func testSocketWithSubs(keys ...string) *socketTracker {
	keySubs := map[string]int{}
	for _, key := range keys {
		keySubs[key]++
	}
	return &socketTracker{
		storeSubscriptions: map[string]map[string]int{
			viewerSocketTestStoreName: keySubs,
		},
	}
}

func assertFieldsContainOnly(t *testing.T, actual [][]any, expected []any) {
	t.Helper()
	if len(actual) != 1 {
		t.Fatalf("expected one field path %#v, got %#v", expected, actual)
	}
	if len(actual[0]) != len(expected) {
		t.Fatalf("expected field path %#v, got %#v", expected, actual[0])
	}
	for idx := range expected {
		if actual[0][idx] != expected[idx] {
			t.Fatalf("expected field path %#v, got %#v", expected, actual[0])
		}
	}
}

func assertMapEqual(t *testing.T, expected map[string]int, actual map[string]int) {
	t.Helper()
	if len(expected) != len(actual) {
		t.Fatalf("expected map %#v, got %#v", expected, actual)
	}
	for key, expectedValue := range expected {
		if actual[key] != expectedValue {
			t.Fatalf("expected map %#v, got %#v", expected, actual)
		}
	}
}
