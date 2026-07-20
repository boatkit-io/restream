package restream

import (
	"reflect"
	"testing"

	"github.com/boatkit-io/restream/pkg/binarystreams"
)

func TestPartialMapApplyToAllocatesNilMapForSet(t *testing.T) {
	var target map[string]int

	fields := NewPartialMap[string, int]().
		Set("depth", 42).
		ApplyTo(&target)

	if target == nil {
		t.Fatal("expected partial set to allocate nil target map")
	}
	if got := target["depth"]; got != 42 {
		t.Fatalf("expected set value 42, got %d", got)
	}
	if want := [][]any{{"depth"}}; !reflect.DeepEqual(fields, want) {
		t.Fatalf("expected fields %#v, got %#v", want, fields)
	}
}

func TestPartialMapApplyToPrunesDeleteFromNilMap(t *testing.T) {
	var target map[string]int

	partial := NewPartialMap[string, int]().Delete("depth")
	fields := partial.ApplyTo(&target)

	if target != nil {
		t.Fatal("expected delete-only partial to leave nil target map nil")
	}
	if len(fields) != 0 {
		t.Fatalf("expected no fields, got %#v", fields)
	}
	if partial.PruneAgainst(&target) {
		t.Fatal("expected nonexistent delete to be pruned from partial")
	}
}

func TestPartialModMapApplyToAllocatesNilMapForSet(t *testing.T) {
	var target map[string]int

	fields := NewPartialModMap[string, int, *fakePartial]().
		Set("Engine_RPM_0", 2400).
		ApplyTo(&target)

	if target == nil {
		t.Fatal("expected partial mod set to allocate nil target map")
	}
	if got := target["Engine_RPM_0"]; got != 2400 {
		t.Fatalf("expected set value 2400, got %d", got)
	}
	if want := [][]any{{"Engine_RPM_0"}}; !reflect.DeepEqual(fields, want) {
		t.Fatalf("expected fields %#v, got %#v", want, fields)
	}
}

func TestPartialModMapApplyToAllocatesNilMapPointerForSet(t *testing.T) {
	var target map[string]int
	targetPtr := &target

	fields := NewPartialModMap[string, int, *fakePartial]().
		Set("Water_Depth_auto", 16).
		ApplyTo(&targetPtr)

	if target == nil {
		t.Fatal("expected partial mod set to allocate nil target map through map pointer")
	}
	if got := target["Water_Depth_auto"]; got != 16 {
		t.Fatalf("expected set value 16, got %d", got)
	}
	if want := [][]any{{"Water_Depth_auto"}}; !reflect.DeepEqual(fields, want) {
		t.Fatalf("expected fields %#v, got %#v", want, fields)
	}
}

func TestPartialModMapApplyToSuppressesNestedFieldWhenKeyWasSet(t *testing.T) {
	target := map[string]partialMapTestValue{}

	fields := NewPartialModMap[string, partialMapTestValue, *partialMapTestPartial]().
		Set("engine", partialMapTestValue{Number: 1}).
		ApplyPartial("engine", &partialMapTestPartial{Number: Ptr(2)}).
		ApplyTo(&target)

	if got := target["engine"].Number; got != 2 {
		t.Fatalf("expected nested partial to apply number 2, got %d", got)
	}
	fields = reduceFieldPaths(fields)
	if want := [][]any{{"engine"}}; !reflect.DeepEqual(fields, want) {
		t.Fatalf("expected fields %#v, got %#v", want, fields)
	}
}

func TestPartialModArrayApplyToSuppressesNestedFieldWhenIndexWasSet(t *testing.T) {
	target := []partialMapTestValue{{Number: 0}}

	fields := NewPartialModArray[partialMapTestValue, *partialMapTestPartial]().
		Set(0, partialMapTestValue{Number: 1}).
		ApplyPartial(0, &partialMapTestPartial{Number: Ptr(2)}).
		ApplyTo(&target)

	if got := target[0].Number; got != 2 {
		t.Fatalf("expected nested partial to apply number 2, got %d", got)
	}
	fields = reduceFieldPaths(fields)
	if want := [][]any{{0}}; !reflect.DeepEqual(fields, want) {
		t.Fatalf("expected fields %#v, got %#v", want, fields)
	}
}

func TestPartialCollectionsPruneEqualOperations(t *testing.T) {
	mapTarget := map[string]int{"depth": 42}
	mapPartial := NewPartialMap[string, int]().Set("depth", 42)
	if fields := mapPartial.ApplyTo(&mapTarget); len(fields) != 0 || mapPartial.PruneAgainst(&mapTarget) {
		t.Fatalf("equal map set was not pruned: fields=%#v partial=%#v", fields, mapPartial)
	}

	arrayTarget := []int{42}
	arrayPartial := NewPartialArray[int]().Set(0, 42)
	if fields := arrayPartial.ApplyTo(&arrayTarget); len(fields) != 0 || arrayPartial.PruneAgainst(&arrayTarget) {
		t.Fatalf("equal array set was not pruned: fields=%#v partial=%#v", fields, arrayPartial)
	}

	wholeTarget := []int{1, 2}
	wholePartial := NewPartialArray[int]().SetWhole([]int{1, 2})
	if fields := wholePartial.ApplyTo(&wholeTarget); len(fields) != 0 || wholePartial.PruneAgainst(&wholeTarget) {
		t.Fatalf("equal whole array was not pruned: fields=%#v partial=%#v", fields, wholePartial)
	}
}

func TestPartialCollectionsPruneAgainstReportsRemainingData(t *testing.T) {
	target := []int{0}
	partial := NewPartialArray[int]().Set(0, 42)

	if !partial.PruneAgainst(&target) {
		t.Fatal("expected changed array set to survive pruning")
	}
	if fields := partial.ApplyTo(&target); !reflect.DeepEqual(fields, [][]any{{0}}) {
		t.Fatalf("expected changed array set field, got %#v", fields)
	}
	if partial.PruneAgainst(&target) {
		t.Fatal("expected applied array set to be pruned against the updated target")
	}
}

type partialMapTestValue struct {
	Number int
}

type partialMapTestPartial struct {
	Number *int
}

func (*partialMapTestPartial) Serialize(*binarystreams.Writer, *VarInfoStruct) error {
	return nil
}

func (*partialMapTestPartial) Deserialize(*binarystreams.Reader, *VarInfoStruct) error {
	return nil
}

func (p *partialMapTestPartial) MergeOntoPartial(por any) {
	po := por.(*partialMapTestPartial)
	if p.Number != nil {
		po.Number = p.Number
	}
}

func (p *partialMapTestPartial) ApplyTo(por any) [][]any {
	po := por.(*partialMapTestValue)
	if p.Number == nil {
		return nil
	}
	po.Number = *p.Number
	return [][]any{{"Number"}}
}
