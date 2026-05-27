package restream

import (
	"reflect"
	"testing"
)

func TestReduceFieldPathsRemovesDescendantsAndDuplicates(t *testing.T) {
	fields := [][]any{
		{"key"},
		{"key", "subkey"},
		{"other", "subkey"},
		{"other"},
		{"key"},
	}

	got := ReduceFieldPaths(fields)
	want := [][]any{
		{"key"},
		{"other"},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected fields %#v, got %#v", want, got)
	}
}

func TestReduceFieldPathsRootSuppressesAllDescendants(t *testing.T) {
	fields := [][]any{
		{"key", "subkey"},
		{},
		{"other"},
	}

	got := ReduceFieldPaths(fields)
	want := [][]any{{}}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected fields %#v, got %#v", want, got)
	}
}
