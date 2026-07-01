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

func TestPartialMapApplyToDeleteDoesNotAllocateNilMap(t *testing.T) {
	var target map[string]int

	fields := NewPartialMap[string, int]().
		Delete("depth").
		ApplyTo(&target)

	if target != nil {
		t.Fatal("expected delete-only partial to leave nil target map nil")
	}
	if want := [][]any{{"depth"}}; !reflect.DeepEqual(fields, want) {
		t.Fatalf("expected fields %#v, got %#v", want, fields)
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
