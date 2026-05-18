package restream

import (
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

// Partial is an interface that can be implemented by a type to allow it to be partially applied to a whole struct.
type Partial interface {
	Serializable
	MergeOntoPartial(any)
	ApplyTo(any) [][]any
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
