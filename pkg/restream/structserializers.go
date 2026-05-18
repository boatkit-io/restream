package restream

import (
	"fmt"
	"reflect"

	"github.com/boatkit-io/restream/pkg/binarystreams"
)

// StructSerializerInfo is a struct that contains the registered serialization and deserialization functions generated
// by buildSerializers options.
type StructSerializerInfo struct {
	Serialize   func(v any, w *binarystreams.Writer, vi *VarInfoStruct) error
	Deserialize func(v any, r *binarystreams.Reader, vi *VarInfoStruct) error
}

// serializerInfoMap holds the registered serializer info
var serializerInfoMap = map[reflect.Type]StructSerializerInfo{}

// GetSerializerForType returns the serializer info for a given type
func GetSerializerForType(t reflect.Type) (StructSerializerInfo, error) {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	si, ok := serializerInfoMap[t]
	if !ok {
		return StructSerializerInfo{}, fmt.Errorf("no serializer info for type %s", t.String())
	}
	return si, nil
}

// RegisterSerializers registers the generated serializer info for a given type
func RegisterSerializers(m map[reflect.Type]StructSerializerInfo) {
	for t, si := range m {
		serializerInfoMap[t] = si
	}
}
