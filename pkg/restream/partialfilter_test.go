package restream

import (
	"reflect"
	"testing"
)

type fieldIDSubscriptionTestState struct {
	Devices map[string]*fieldIDSubscriptionTestDevice `restream:",fID=3"`
}

type fieldIDSubscriptionTestDevice struct {
	Count uint64 `restream:",fID=7"`
}

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

func TestNormalizeFieldIDSubscriptionKey(t *testing.T) {
	key := SubscriptionKeyFromFieldIDPath([]any{3, "CAN0", 7})
	if key != "~1%&3%&CAN0%&7" {
		t.Fatalf("field-ID key = %q", key)
	}

	got, err := normalizeFieldIDSubscriptionKey(key, reflect.TypeFor[fieldIDSubscriptionTestState]())
	if err != nil {
		t.Fatalf("normalize field-ID key: %v", err)
	}
	if got != "devices%&CAN0%&count" {
		t.Fatalf("normalized key = %q", got)
	}
}

func TestNormalizeFieldIDSubscriptionKeyKeepsLegacyKeys(t *testing.T) {
	const key = "devices%&CAN0%&count"
	got, err := normalizeFieldIDSubscriptionKey(key, reflect.TypeFor[fieldIDSubscriptionTestState]())
	if err != nil {
		t.Fatalf("normalize legacy key: %v", err)
	}
	if got != key {
		t.Fatalf("normalized legacy key = %q", got)
	}
}

func TestNormalizeFieldIDSubscriptionKeyRejectsUnknownFields(t *testing.T) {
	_, err := normalizeFieldIDSubscriptionKey(
		SubscriptionKeyFromFieldIDPath([]any{4}),
		reflect.TypeFor[fieldIDSubscriptionTestState](),
	)
	if err == nil {
		t.Fatal("expected unknown field ID to fail")
	}
}
