package storetest

import (
	"reflect"
	"testing"

	"github.com/boatkit-io/restream/pkg/binarystreams"
	"github.com/boatkit-io/restream/pkg/restream"
	"github.com/stretchr/testify/assert"
)

// @restream.fields
// @restream.partials
type TestState struct {
	// MAXFIELD(4)
	MapPtrTest    map[uint8]*TestMapData `restream:",fID=1"`
	BaseField     string                 `restream:",fID=2"`
	BaseStruct    TestMapData            `restream:",fID=3"`
	BaseStructPtr *TestMapData           `restream:",fID=4"`
}

// @restream.fields
// @restream.partials
type TestMapData struct {
	// MAXFIELD(2)
	Number uint `restream:",fID=1"`
}

// @restream.fields
// @restream.partials
type TestPrimitiveOptionalState struct {
	// MAXFIELD(2)
	Primitive uint32  `restream:",fID=1"`
	Optional  *uint32 `restream:",fID=2"`
}

type TestStore struct {
	Sd *restream.StoreData[TestState, *TestState, *TestStatePartial]
}

func (s *TestStore) GetName() string {
	return "test"
}
func (s *TestStore) GetStoreData() restream.StoreDataBase {
	return s.Sd
}
func (s *TestStore) SubscribeToField(field []any, callback any) {
	s.Sd.SubscribeToField(field, callback)
}

func TestSetField(t *testing.T) {
	state := TestState{
		MapPtrTest: make(map[uint8]*TestMapData),
	}
	store := TestStore{}

	store.Sd = restream.NewStoreData[TestState, *TestState, *TestStatePartial](&store, &state)
	assert.NotNil(t, store.Sd)
	assert.Equal(t, "test", store.GetName())

	assert.Equal(t, "", state.BaseField)
	store.Sd.ApplyPartial(&TestStatePartial{BaseField: restream.Ptr("hi")})
	assert.Equal(t, "hi", state.BaseField)

	assert.Equal(t, uint(0), state.BaseStruct.Number)
	store.Sd.ApplyPartial(&TestStatePartial{
		BaseStruct: (&restream.PartialValue[TestMapData, *TestMapDataPartial]{}).
			ApplyPartial(&TestMapDataPartial{Number: restream.Ptr(uint(4))})})
	assert.Equal(t, uint(4), state.BaseStruct.Number)
	store.Sd.ApplyPartial(&TestStatePartial{
		BaseStructPtr: (&restream.PartialValue[*TestMapData, *TestMapDataPartial]{}).
			SetWhole(restream.Ptr(&TestMapData{Number: 5}))})
	assert.Equal(t, uint(5), state.BaseStructPtr.Number)

	td := TestMapData{Number: 6}
	store.Sd.ApplyPartial(&TestStatePartial{MapPtrTest: restream.NewPartialModMap[uint8, *TestMapData, *TestMapDataPartial]().Set(5, &td)})
	assert.Equal(t, uint(6), state.MapPtrTest[5].Number)
}

func TestGeneratedPartialCanClearOptionalPrimitivePointer(t *testing.T) {
	originalOptional := uint32(2)
	state := TestPrimitiveOptionalState{
		Primitive: 1,
		Optional:  &originalOptional,
	}

	partial := &TestPrimitiveOptionalStatePartial{
		Primitive: restream.Ptr(uint32(3)),
	}

	optionalField := reflect.ValueOf(partial).Elem().FieldByName("Optional")
	if optionalField.Type() == reflect.TypeFor[**uint32]() {
		var cleared *uint32
		optionalField.Set(reflect.ValueOf(&cleared))
	} else {
		t.Logf("generated optional partial field has type %s; want **uint32 so nil can mean absent and *nil can mean clear",
			optionalField.Type())
		optionalField.Set(reflect.Zero(optionalField.Type()))
	}

	b, err := restream.SerializeToBytes(partial, nil)
	assert.NoError(t, err)

	var decoded TestPrimitiveOptionalStatePartial
	assert.NoError(t, decoded.Deserialize(binarystreams.NewReaderFromBytes(b), nil))

	fields := decoded.ApplyTo(&state)

	assert.Equal(t, uint32(3), state.Primitive)
	assert.Nil(t, state.Optional, "applying an explicit clear partial should nil the optional pointer field")
	assert.Equal(t, [][]any{{"Primitive"}, {"Optional"}}, fields)
}

func TestGeneratedPartialFilterToFields(t *testing.T) {
	mapValueFive := &TestMapData{Number: 5}
	mapValueSix := &TestMapData{Number: 6}
	partial := &TestStatePartial{
		MapPtrTest: restream.NewPartialModMap[uint8, *TestMapData, *TestMapDataPartial]().
			Set(5, mapValueFive).
			Set(6, mapValueSix),
		BaseField: restream.Ptr("unused"),
		BaseStruct: (&restream.PartialValue[TestMapData, *TestMapDataPartial]{}).
			ApplyPartial(&TestMapDataPartial{Number: restream.Ptr(uint(9))}),
	}

	filteredMapPartial, ok := restream.FilterPartialToFields(partial, [][]any{{"MapPtrTest", uint8(5)}})
	assert.True(t, ok)
	mapState := TestState{MapPtrTest: map[uint8]*TestMapData{}}
	fields := filteredMapPartial.ApplyTo(&mapState)
	assert.Equal(t, [][]any{{"MapPtrTest", uint8(5)}}, fields)
	assert.Equal(t, mapValueFive, mapState.MapPtrTest[5])
	assert.Nil(t, mapState.MapPtrTest[6])
	assert.Empty(t, mapState.BaseField)

	filteredStructPartial, ok := restream.FilterPartialToFields(partial, [][]any{{"BaseStruct", "Number"}})
	assert.True(t, ok)
	structState := TestState{MapPtrTest: map[uint8]*TestMapData{}}
	fields = filteredStructPartial.ApplyTo(&structState)
	assert.Equal(t, [][]any{{"BaseStruct", "Number"}}, fields)
	assert.Equal(t, uint(9), structState.BaseStruct.Number)
	assert.Empty(t, structState.MapPtrTest)
	assert.Empty(t, structState.BaseField)
}

func TestWholeCollectionApplyReturnsOnlyBroadField(t *testing.T) {
	modMapPartial := restream.NewPartialModMap[uint8, *TestMapData, *TestMapDataPartial]().
		SetWhole(map[uint8]*TestMapData{
			5: {Number: 5},
			6: {Number: 6},
		})
	modMapState := map[uint8]*TestMapData{}
	assert.Equal(t, [][]any{{}}, modMapPartial.ApplyTo(&modMapState))

	mapPartial := restream.NewPartialMap[uint8, uint]().
		SetWhole(map[uint8]uint{
			5: 5,
			6: 6,
		})
	mapState := map[uint8]uint{}
	assert.Equal(t, [][]any{{}}, mapPartial.ApplyTo(&mapState))

	modArrayPartial := restream.NewPartialModArray[*TestMapData, *TestMapDataPartial]().
		SetWhole([]*TestMapData{{Number: 5}, {Number: 6}})
	modArrayState := []*TestMapData{}
	assert.Equal(t, [][]any{{}}, modArrayPartial.ApplyTo(&modArrayState))

	arrayPartial := restream.NewPartialArray[uint]().
		SetWhole([]uint{5, 6})
	arrayState := []uint{}
	assert.Equal(t, [][]any{{}}, arrayPartial.ApplyTo(&arrayState))
}

func TestWholePartialFieldPathCascadesToNarrowSubscription(t *testing.T) {
	mapValueFive := &TestMapData{Number: 5}
	mapValueSix := &TestMapData{Number: 6}
	wholePartial := &TestStatePartial{
		MapPtrTest: restream.NewPartialModMap[uint8, *TestMapData, *TestMapDataPartial]().
			SetWhole(map[uint8]*TestMapData{
				5: mapValueFive,
				6: mapValueSix,
			}),
	}

	wholeState := TestState{MapPtrTest: map[uint8]*TestMapData{}}
	fields := wholePartial.ApplyTo(&wholeState)
	assert.Equal(t, [][]any{{"MapPtrTest"}}, fields)
	assert.True(t, restream.FieldPathAffectsSubscription(fields[0], []any{"MapPtrTest", uint8(5)}))

	filteredPartial, ok := restream.FilterPartialToFields(wholePartial, [][]any{{"MapPtrTest", uint8(5)}})
	assert.True(t, ok)
	filteredState := TestState{
		MapPtrTest: map[uint8]*TestMapData{
			5: {Number: 1},
			6: {Number: 2},
		},
	}
	fields = filteredPartial.ApplyTo(&filteredState)
	assert.Equal(t, [][]any{{"MapPtrTest", uint8(5)}}, fields)
	assert.Equal(t, mapValueFive, filteredState.MapPtrTest[5])
	assert.Equal(t, uint(2), filteredState.MapPtrTest[6].Number)

	deletePartial, ok := restream.FilterPartialToFields(wholePartial, [][]any{{"MapPtrTest", uint8(7)}})
	assert.True(t, ok)
	deleteState := TestState{MapPtrTest: map[uint8]*TestMapData{7: {Number: 7}}}
	fields = deletePartial.ApplyTo(&deleteState)
	assert.Equal(t, [][]any{{"MapPtrTest", uint8(7)}}, fields)
	assert.Nil(t, deleteState.MapPtrTest[7])
}

func TestFieldPathAffectsSubscriptionCascadesBothDirections(t *testing.T) {
	assert.True(t, restream.FieldPathAffectsSubscription([]any{}, []any{"MapPtrTest", uint8(5)}))
	assert.True(t, restream.FieldPathAffectsSubscription([]any{"MapPtrTest"}, []any{"MapPtrTest", uint8(5)}))
	assert.True(t, restream.FieldPathAffectsSubscription([]any{"MapPtrTest", uint8(5), "Number"}, []any{"MapPtrTest", uint8(5)}))
	assert.True(t, restream.FieldPathAffectsSubscription([]any{"MapPtrTest", uint8(5)}, []any{"MapPtrTest"}))
	assert.False(t, restream.FieldPathAffectsSubscription([]any{"MapPtrTest", uint8(6)}, []any{"MapPtrTest", uint8(5)}))
	assert.False(t, restream.FieldPathAffectsSubscription([]any{"BaseStruct"}, []any{"MapPtrTest", uint8(5)}))

	assert.Equal(t, "mapPtrTest%&5%&number", restream.SubscriptionKeyFromFieldPath([]any{"MapPtrTest", uint8(5), "Number"}))
	assert.Equal(t, []any{"MapPtrTest", "5", "Number"}, restream.FieldPathFromSubscriptionKey("mapPtrTest%&5%&Number"))
}

func TestRelayStoreProvidesKeyedInitialPartial(t *testing.T) {
	mapValueFive := &TestMapData{Number: 5}
	mapValueSix := &TestMapData{Number: 6}
	relayStore := restream.NewRelayStore[TestState, *TestState, *TestStatePartial]("relay-test", &TestState{
		MapPtrTest: map[uint8]*TestMapData{
			5: mapValueFive,
			6: mapValueSix,
		},
		BaseField: "unused",
	})

	partial, exists, err := relayStore.GetPartialForSubscriptionKey("mapPtrTest%&5")
	assert.NoError(t, err)
	assert.True(t, exists)

	state := TestState{MapPtrTest: map[uint8]*TestMapData{}}
	fields := partial.ApplyTo(&state)
	assert.Equal(t, [][]any{{"MapPtrTest", uint8(5)}}, fields)
	assert.Equal(t, mapValueFive, state.MapPtrTest[5])
	assert.Nil(t, state.MapPtrTest[6])
	assert.Empty(t, state.BaseField)
}

func TestRelayStoreKeyedInitialPartialDeletesMissingMapKey(t *testing.T) {
	relayStore := restream.NewRelayStore[TestState, *TestState, *TestStatePartial]("relay-test", &TestState{
		MapPtrTest: map[uint8]*TestMapData{
			5: {Number: 5},
		},
	})

	partial, exists, err := relayStore.GetPartialForSubscriptionKey("mapPtrTest%&7")
	assert.NoError(t, err)
	assert.True(t, exists)

	state := TestState{MapPtrTest: map[uint8]*TestMapData{
		7: {Number: 7},
	}}
	fields := partial.ApplyTo(&state)
	assert.Equal(t, [][]any{{"MapPtrTest", uint8(7)}}, fields)
	assert.Nil(t, state.MapPtrTest[7])
	assert.Equal(t, uint(0), state.BaseStruct.Number)
}

func TestRelayStoreForwardsKeyedSubscriptionLifecycle(t *testing.T) {
	relayStore := restream.NewRelayStore[TestState, *TestState, *TestStatePartial]("relay-test", &TestState{})

	var calls []string
	relayStore.SetKeySubscriptionForwarder(func(storeName string, key string, subscribe bool) {
		action := "unsubscribe"
		if subscribe {
			action = "subscribe"
		}
		calls = append(calls, action+":"+storeName+":"+key)
	})

	relayStore.SubscriptionStartedForKey("values%&a")
	relayStore.SubscriptionStartedForKey("values%&a")
	relayStore.SubscriptionStartedForKey("values%&b")
	assert.Equal(t, []string{"values%&a", "values%&b"}, relayStore.ActiveSubscriptionKeys())
	assert.Equal(t, []string{
		"subscribe:relay-test:values%&a",
		"subscribe:relay-test:values%&b",
	}, calls)

	relayStore.SubscriptionEndedForKey("values%&a")
	assert.Equal(t, []string{"values%&a", "values%&b"}, relayStore.ActiveSubscriptionKeys())
	assert.Equal(t, 2, len(calls))

	relayStore.SubscriptionEndedForKey("values%&a")
	relayStore.SubscriptionEndedForKey("values%&missing")
	relayStore.SubscriptionEndedForKey("values%&b")
	assert.Empty(t, relayStore.ActiveSubscriptionKeys())
	assert.Equal(t, []string{
		"subscribe:relay-test:values%&a",
		"subscribe:relay-test:values%&b",
		"unsubscribe:relay-test:values%&a",
		"unsubscribe:relay-test:values%&b",
	}, calls)
}

func TestRelayStoreMergedPartialMatchesSequentialPartialReplay(t *testing.T) {
	sequentialState := relayStoreMergeBaseState()
	sequentialStore := restream.NewRelayStore[TestState, *TestState, *TestStatePartial]("relay-test", &sequentialState)
	applyRelayStorePartial(t, sequentialStore, relayStoreMergeFirstPartial())
	applyRelayStorePartial(t, sequentialStore, relayStoreMergeSecondPartial())

	mergedState := relayStoreMergeBaseState()
	mergedStore := restream.NewRelayStore[TestState, *TestState, *TestStatePartial]("relay-test", &mergedState)
	mergedPartial := relayStoreMergeFirstPartial()
	relayStoreMergeSecondPartial().MergeOntoPartial(mergedPartial)
	applyRelayStorePartial(t, mergedStore, mergedPartial)

	assert.Equal(t, sequentialState, mergedState)
	assert.Equal(t, "second", mergedState.BaseField)
	assert.Equal(t, uint(55), mergedState.MapPtrTest[5].Number)
	assert.Nil(t, mergedState.MapPtrTest[6])
	assert.Equal(t, uint(75), mergedState.MapPtrTest[7].Number)
	assert.Nil(t, mergedState.MapPtrTest[8])
	assert.Equal(t, uint(90), mergedState.MapPtrTest[9].Number)
	assert.Equal(t, uint(12), mergedState.BaseStruct.Number)
	assert.Equal(t, uint(22), mergedState.BaseStructPtr.Number)
}

func TestRelayStoreMergedPartialFieldPathsCoverMergedChanges(t *testing.T) {
	state := relayStoreMergeBaseState()
	relayStore := restream.NewRelayStore[TestState, *TestState, *TestStatePartial]("relay-test", &state)

	var callbackFields [][]any
	relayStore.GetStoreData().AddCallback(func(_ string, fields [][]any, _ restream.Partial) {
		callbackFields = fields
	})

	mergedPartial := relayStoreMergeFirstPartial()
	relayStoreMergeSecondPartial().MergeOntoPartial(mergedPartial)
	applyRelayStorePartial(t, relayStore, mergedPartial)

	assert.ElementsMatch(t, [][]any{
		{"MapPtrTest", uint8(5), "Number"},
		{"MapPtrTest", uint8(6)},
		{"MapPtrTest", uint8(7)},
		{"MapPtrTest", uint8(8)},
		{"MapPtrTest", uint8(9)},
		{"BaseField"},
		{"BaseStruct", "Number"},
		{"BaseStructPtr"},
	}, callbackFields)
}

func relayStoreMergeBaseState() TestState {
	return TestState{
		MapPtrTest: map[uint8]*TestMapData{
			5: {Number: 1},
			6: {Number: 2},
			8: {Number: 8},
		},
		BaseField:     "start",
		BaseStruct:    TestMapData{Number: 10},
		BaseStructPtr: &TestMapData{Number: 20},
	}
}

func relayStoreMergeFirstPartial() *TestStatePartial {
	return &TestStatePartial{
		MapPtrTest: restream.NewPartialModMap[uint8, *TestMapData, *TestMapDataPartial]().
			ApplyPartial(5, &TestMapDataPartial{Number: restream.Ptr(uint(50))}).
			Set(7, &TestMapData{Number: 70}).
			Set(8, &TestMapData{Number: 80}).
			Delete(9),
		BaseField: restream.Ptr("first"),
		BaseStruct: (&restream.PartialValue[TestMapData, *TestMapDataPartial]{}).
			ApplyPartial(&TestMapDataPartial{Number: restream.Ptr(uint(11))}),
		BaseStructPtr: (&restream.PartialValue[*TestMapData, *TestMapDataPartial]{}).
			SetWhole(restream.Ptr(&TestMapData{Number: 21})),
	}
}

func relayStoreMergeSecondPartial() *TestStatePartial {
	return &TestStatePartial{
		MapPtrTest: restream.NewPartialModMap[uint8, *TestMapData, *TestMapDataPartial]().
			ApplyPartial(5, &TestMapDataPartial{Number: restream.Ptr(uint(55))}).
			Delete(6).
			ApplyPartial(7, &TestMapDataPartial{Number: restream.Ptr(uint(75))}).
			Delete(8).
			Set(9, &TestMapData{Number: 90}),
		BaseField: restream.Ptr("second"),
		BaseStruct: (&restream.PartialValue[TestMapData, *TestMapDataPartial]{}).
			ApplyPartial(&TestMapDataPartial{Number: restream.Ptr(uint(12))}),
		BaseStructPtr: (&restream.PartialValue[*TestMapData, *TestMapDataPartial]{}).
			ApplyPartial(&TestMapDataPartial{Number: restream.Ptr(uint(22))}),
	}
}

func applyRelayStorePartial(t *testing.T, relayStore *restream.RelayStore[TestState, *TestState, *TestStatePartial], partial *TestStatePartial) {
	t.Helper()
	partialBytes, err := restream.SerializeToBytes(partial, nil)
	assert.NoError(t, err)
	assert.NoError(t, relayStore.GetStoreData().DecodeAndApplyPartial(partialBytes))
}

// @restream.fields
// @restream.partials
type TestA struct {
	// MAXFIELD(2)
	A TestB `restream:",fID=1"`
	B TestB `restream:",fID=2"`
}

// @restream.fields
// @restream.partials
type TestB struct {
	// MAXFIELD(2)
	A TestC `restream:",fID=1"`
	B TestC `restream:",fID=2"`
}

// @restream.fields
// @restream.partials
type TestC struct {
	// MAXFIELD(2)
	A int `restream:",fID=1"`
	B int `restream:",fID=2"`
}

type TestAStore struct {
	Sd *restream.StoreData[TestA, *TestA, *TestAPartial] `json:"sd"`
}

func (s *TestAStore) GetName() string {
	return "test"
}
func (s *TestAStore) GetStoreData() restream.StoreDataBase {
	return s.Sd
}
func (s *TestAStore) SubscribeToField(field []any, callback any) {
	s.Sd.SubscribeToField(field, callback)
}

func TestSubs(t *testing.T) {
	state := TestA{}
	store := TestAStore{}
	store.Sd = restream.NewStoreData[TestA, *TestA, *TestAPartial](&store, &state)

	assert.Panics(t, func() {
		store.Sd.SubscribeToField([]any{"A"}, func([]any, any) {})
	})

	aCalls := 0
	var lastACall *TestB
	store.Sd.SubscribeToField([]any{"A"}, func(_ []any, d TestB) {
		aCalls++
		lastACall = &d
	})
	bCalls := 0
	var lastBCall *TestC
	store.Sd.SubscribeToField([]any{"A", "A"}, func(_ []any, d TestC) {
		bCalls++
		lastBCall = &d
	})
	cCalls := 0
	var lastCCall *int
	store.Sd.SubscribeToField([]any{"A", "A", "A"}, func(_ []any, d int) {
		cCalls++
		lastCCall = &d
	})
	aabCalled := false
	store.Sd.SubscribeToField([]any{"A", "A", "B"}, func(_ []any) {
		aabCalled = true
	})
	aab2Called := false
	store.Sd.SubscribeToField([]any{"A", "A", "B"}, func() {
		aab2Called = true
	})

	assert.Equal(t, 0, aCalls)
	assert.Equal(t, (*TestB)(nil), lastACall)
	assert.Equal(t, 0, bCalls)
	assert.Equal(t, (*TestC)(nil), lastBCall)
	assert.Equal(t, 0, cCalls)
	assert.Equal(t, (*int)(nil), lastCCall)
	assert.False(t, aabCalled)
	assert.False(t, aab2Called)
	store.Sd.ApplyPartial(&TestAPartial{A: (&restream.PartialValue[TestB, *TestBPartial]{}).
		ApplyPartial(&TestBPartial{A: (&restream.PartialValue[TestC, *TestCPartial]{}).
			SetWhole(&TestC{A: 1})})})
	assert.Equal(t, 1, aCalls)
	assert.Equal(t, &TestB{A: TestC{A: 1}}, lastACall)
	assert.Equal(t, 1, bCalls)
	assert.Equal(t, &TestC{A: 1}, lastBCall)
	assert.Equal(t, 1, cCalls)
	assert.Equal(t, restream.Ptr(1), lastCCall)
	assert.True(t, aabCalled)
	assert.True(t, aab2Called)
	store.Sd.ApplyPartial(&TestAPartial{A: (&restream.PartialValue[TestB, *TestBPartial]{}).
		ApplyPartial(&TestBPartial{A: (&restream.PartialValue[TestC, *TestCPartial]{}).
			ApplyPartial(&TestCPartial{B: restream.Ptr(2)})})})
	assert.Equal(t, 2, aCalls)
	assert.Equal(t, &TestB{A: TestC{A: 1, B: 2}}, lastACall)
	assert.Equal(t, 2, bCalls)
	assert.Equal(t, &TestC{A: 1, B: 2}, lastBCall)
	assert.Equal(t, 1, cCalls)
	store.Sd.ApplyPartial(&TestAPartial{A: (&restream.PartialValue[TestB, *TestBPartial]{}).
		ApplyPartial(&TestBPartial{A: (&restream.PartialValue[TestC, *TestCPartial]{}).
			SetWhole(&TestC{A: 3, B: 4})})})
	assert.Equal(t, 3, aCalls)
	assert.Equal(t, &TestB{A: TestC{A: 3, B: 4}}, lastACall)
	assert.Equal(t, 3, bCalls)
	assert.Equal(t, &TestC{A: 3, B: 4}, lastBCall)
	assert.Equal(t, 2, cCalls)
	assert.Equal(t, restream.Ptr(3), lastCCall)
	store.Sd.ApplyPartial(&TestAPartial{A: (&restream.PartialValue[TestB, *TestBPartial]{}).
		ApplyPartial(&TestBPartial{B: (&restream.PartialValue[TestC, *TestCPartial]{}).
			SetWhole(&TestC{A: 5, B: 6})})})
	assert.Equal(t, 4, aCalls)
	assert.Equal(t, &TestB{A: TestC{A: 3, B: 4}, B: TestC{A: 5, B: 6}}, lastACall)
	assert.Equal(t, 3, bCalls)
	assert.Equal(t, 2, cCalls)
	store.Sd.ApplyPartial(&TestAPartial{A: (&restream.PartialValue[TestB, *TestBPartial]{}).
		SetWhole(&TestB{A: TestC{A: 7, B: 8}, B: TestC{A: 9, B: 10}})})
	assert.Equal(t, 5, aCalls)
	assert.Equal(t, &TestB{A: TestC{A: 7, B: 8}, B: TestC{A: 9, B: 10}}, lastACall)
	assert.Equal(t, 4, bCalls)
	assert.Equal(t, &TestC{A: 7, B: 8}, lastBCall)
	assert.Equal(t, 3, cCalls)
	assert.Equal(t, restream.Ptr(7), lastCCall)
	store.Sd.ApplyPartial(&TestAPartial{B: (&restream.PartialValue[TestB, *TestBPartial]{}).SetWhole(&TestB{})})
	assert.Equal(t, 5, aCalls)
	assert.Equal(t, 4, bCalls)
	assert.Equal(t, 3, cCalls)
}

func TestStoreRegistryDecode(t *testing.T) {
	tstate := &TestState{
		MapPtrTest: make(map[uint8]*TestMapData),
	}
	ts := &TestStore{}
	assert.Equal(t, "test", ts.GetName())
	ts.Sd = restream.NewStoreData[TestState, *TestState, *TestStatePartial](ts, tstate)
	assert.NotNil(t, ts.Sd)
	sl := []restream.Store{
		ts,
	}
	sr, err := restream.NewStoreRegistry(sl)
	assert.NoError(t, err)

	tstate2 := &TestState{
		BaseField: "hi2",
		BaseStruct: TestMapData{
			Number: 2,
		},
	}

	w, b := binarystreams.NewMemoryWriter()
	err = tstate2.Serialize(w, nil)
	assert.NoError(t, err)
	err = w.Flush()
	assert.NoError(t, err)
	err = sr.SetFullStateToStore("test", b.Bytes())
	assert.NoError(t, err)

	assert.Equal(t, "hi2", tstate.BaseField)
	assert.Equal(t, uint(2), tstate.BaseStruct.Number)

	tsp := &TestStatePartial{
		BaseField: restream.Ptr("hi3"),
	}
	bb, err := restream.SerializeToBytes(tsp, nil)
	assert.NoError(t, err)

	err = sr.ApplyPartialToStore("test", bb)
	assert.NoError(t, err)

	assert.Equal(t, "hi3", tstate.BaseField)
}
