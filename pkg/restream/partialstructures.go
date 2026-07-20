package restream

import (
	"bytes"
	"errors"
	"reflect"
	"time"

	"github.com/boatkit-io/restream/pkg/binarystreams"
)

// Serializable is an interface that can be implemented by a type to allow it to be serialized and deserialized.
type Serializable interface {
	Serialize(*binarystreams.Writer, *VarInfoStruct) error
	Deserialize(*binarystreams.Reader, *VarInfoStruct) error
}

// SerializableGeneric is a specific type of Serializable that is used for generic types.
type SerializableGeneric interface {
	GetTypeArgs() []VarInfo
}

// StateCloner is implemented by generated state structs that can make a deep copy of themselves.
type StateCloner[S any] interface {
	RestreamClone() *S
}

// RawSerializable wraps bytes that have already been serialized so callers can pass them through APIs that accept
// Serializable snapshots.
type RawSerializable []byte

var _ Serializable = RawSerializable(nil)

// Serialize writes the pre-serialized bytes directly to the destination stream.
func (r RawSerializable) Serialize(w *binarystreams.Writer, _ *VarInfoStruct) error {
	return w.WriteBytes([]byte(r))
}

// Deserialize is unsupported because RawSerializable only represents an outbound byte payload.
func (RawSerializable) Deserialize(*binarystreams.Reader, *VarInfoStruct) error {
	return errors.New("RawSerializable cannot be deserialized")
}

// Partial is an interface that can be implemented by a type to allow it to be partially applied to a whole struct.
// ApplyTo mutates the target and returns raw changed field paths. Callers that notify subscriptions should reduce those
// paths after mutation.
type Partial interface {
	Serializable
	MergeOntoPartial(any)
	ApplyTo(any) [][]any
}

// PrunablePartial can remove operations by comparing them with a target. PruneAgainst reports whether any operations
// remain after pruning.
type PrunablePartial interface {
	PruneAgainst(any) bool
}

// PrunePartialAgainst prunes a partial when it supports comparison and reports whether it still contains operations.
// Custom partial implementations that do not support pruning are assumed to contain data and are applied normally.
func PrunePartialAgainst(partial Partial, target any) bool {
	if partial == nil {
		return false
	}
	value := reflect.ValueOf(partial)
	if value.Kind() == reflect.Pointer && value.IsNil() {
		return false
	}
	if prunable, ok := partial.(PrunablePartial); ok {
		return prunable.PruneAgainst(target)
	}
	return true
}

// ValuesEqual compares values inside generic partial containers. Generated fields avoid this helper when their concrete
// type is known; the type switch keeps common generic primitive, time, and byte-slice values off the reflection path too.
func ValuesEqual[T any](left, right T) bool {
	switch value := any(left).(type) {
	case bool:
		return knownComparableEqual(value, any(right))
	case string:
		return knownComparableEqual(value, any(right))
	case int:
		return knownComparableEqual(value, any(right))
	case int8:
		return knownComparableEqual(value, any(right))
	case int16:
		return knownComparableEqual(value, any(right))
	case int32:
		return knownComparableEqual(value, any(right))
	case int64:
		return knownComparableEqual(value, any(right))
	case uint:
		return knownComparableEqual(value, any(right))
	case uint8:
		return knownComparableEqual(value, any(right))
	case uint16:
		return knownComparableEqual(value, any(right))
	case uint32:
		return knownComparableEqual(value, any(right))
	case uint64:
		return knownComparableEqual(value, any(right))
	case float32:
		return knownComparableEqual(value, any(right))
	case float64:
		return knownComparableEqual(value, any(right))
	case time.Time:
		rightValue, ok := any(right).(time.Time)
		return ok && value.Equal(rightValue)
	case []byte:
		rightValue, ok := any(right).([]byte)
		return ok && (value == nil) == (rightValue == nil) && bytes.Equal(value, rightValue)
	default:
		return reflect.DeepEqual(left, right)
	}
}

func knownComparableEqual[T comparable](left T, right any) bool {
	rightValue, ok := right.(T)
	return ok && left == rightValue
}

// PartialAndPtr is a type constraint that implements both Partial and *P, used for the Partial types.
type PartialAndPtr[P any] interface {
	Partial
	*P
}

// fakeStruct is a fake struct that implements the Serializable interface for type assertions.
type fakeStruct struct{}

var _ Serializable = (*fakeStruct)(nil)

// Serialize will panic if ever called
func (*fakeStruct) Serialize(*binarystreams.Writer, *VarInfoStruct) error {
	panic("Serialize called for fakeStruct")
}

// Deserialize will panic if ever called
func (*fakeStruct) Deserialize(*binarystreams.Reader, *VarInfoStruct) error {
	panic("Deserialize called for fakeStruct")
}

// fakePartial is a fake partial that implements the Partial interface for type assertions.
type fakePartial struct{}

var _ Partial = (*fakePartial)(nil)

// Serialize will panic if ever called
func (*fakePartial) Serialize(*binarystreams.Writer, *VarInfoStruct) error {
	panic("Serialize called for fakePartial")
}

// Deserialize will panic if ever called
func (*fakePartial) Deserialize(*binarystreams.Reader, *VarInfoStruct) error {
	panic("Deserialize called for fakePartial")
}

// MergeOntoPartial will panic if ever called
func (*fakePartial) MergeOntoPartial(any) {
	panic("MergeOntoPartial called for fakePartial")
}

// ApplyTo will panic if ever called
func (*fakePartial) ApplyTo(any) [][]any {
	panic("ApplyTo called for fakePartial")
}

// Ptr wraps any primitive to a pointer of it
func Ptr[T any](v T) *T {
	return &v
}
