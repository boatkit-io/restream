package restream

import (
	"reflect"
	"testing"
)

func TestFieldPathReductionRemovesDescendantsAndDuplicates(t *testing.T) {
	fields := [][]any{
		{"key"},
		{"key", "subkey"},
		{"other", "subkey"},
		{"other"},
		{"key"},
	}

	got := reduceFieldPaths(fields)
	want := [][]any{
		{"key"},
		{"other"},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected fields %#v, got %#v", want, got)
	}
}

func TestFieldPathReductionRootSuppressesAllDescendants(t *testing.T) {
	fields := [][]any{
		{"key", "subkey"},
		{},
		{"other"},
	}

	got := reduceFieldPaths(fields)
	want := [][]any{{}}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected fields %#v, got %#v", want, got)
	}
}

func TestFieldPathReductionHandlesNonComparableParts(t *testing.T) {
	fields := [][]any{
		{[]int{1}, "child"},
		{[]int{1}, "other"},
		{[]int{1}},
	}

	got := reduceFieldPaths(fields)
	want := [][]any{{[]int{1}}}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected fields %#v, got %#v", want, got)
	}
}

func BenchmarkFieldPathReductionLargeMapPartial(b *testing.B) {
	fields := make([][]any, 0, 5000)
	for i := 0; i < 1000; i++ {
		fields = append(
			fields,
			[]any{"CloudPacketRates", i, "BytesLastMinute"},
			[]any{"CloudPacketRates", i, "PacketsLastMinute"},
			[]any{"CloudPacketRates", i, "LastPacketBytes"},
			[]any{"CloudPacketRates", i, "LastPacketType"},
			[]any{"CloudPacketRates", i, "LastPacketTime"},
		)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = reduceFieldPaths(fields)
	}
}
