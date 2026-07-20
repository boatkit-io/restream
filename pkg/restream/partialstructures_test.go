package restream

import (
	"testing"
	"time"
)

func TestValuesEqualUsesWireSemanticsForKnownGenericValues(t *testing.T) {
	instant := time.Date(2026, time.July, 19, 12, 0, 0, 0, time.UTC)
	sameInstant := instant.In(time.FixedZone("test offset", -7*60*60))
	if !ValuesEqual(instant, sameInstant) {
		t.Fatal("expected times representing the same instant to compare equal")
	}

	if !ValuesEqual(42, 42) || ValuesEqual(42, 43) {
		t.Fatal("primitive equality returned an unexpected result")
	}

	if !ValuesEqual([]byte{1, 2}, []byte{1, 2}) || ValuesEqual([]byte{1, 2}, []byte{1, 3}) {
		t.Fatal("byte-slice equality returned an unexpected result")
	}
	if ValuesEqual([]byte(nil), []byte{}) {
		t.Fatal("nil and empty byte slices have different serialized representations")
	}
}

func TestValuesEqualAllowsDifferentDynamicTypes(t *testing.T) {
	if ValuesEqual[any](uint32(42), int32(42)) {
		t.Fatal("differently typed dynamic values must not compare equal")
	}
}
