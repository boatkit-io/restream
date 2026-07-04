package restream

import (
	"strings"
	"testing"

	"github.com/boatkit-io/restream/pkg/binarystreams"
)

func TestDeserializeFieldedWrapsFieldContext(t *testing.T) {
	var enabled bool
	fieldInfo := []FieldInfo{
		{Name: "Enabled", FieldIdx: 0, FieldID: 4, VarInfo: &VarInfoPrimitive{DataType: SerializationTypeBool}},
	}
	fieldMap := map[byte]*FieldInfo{4: &fieldInfo[0]}

	err := DeserializeFielded(binarystreams.NewReaderFromBytes([]byte{4, 1, 4}), fieldInfo, fieldMap, []any{&enabled})
	if err == nil {
		t.Fatal("DeserializeFielded error = nil, want malformed bool error")
	}
	if !strings.Contains(err.Error(), `deserialize field "Enabled" (fieldID=4)`) ||
		!strings.Contains(err.Error(), "DeserializeBool: 4") {
		t.Fatalf("DeserializeFielded error = %q, want field context and bool decode error", err)
	}
}
