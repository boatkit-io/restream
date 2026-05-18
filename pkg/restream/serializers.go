package restream

import (
	"fmt"
	"reflect"
	"time"

	"github.com/boatkit-io/restream/pkg/binarystreams"
)

// SerializeToBytes serializes a Serializable to a byte slice
func SerializeToBytes(s Serializable, vi *VarInfoStruct) ([]byte, error) {
	w, b := binarystreams.NewMemoryWriter()
	if err := s.Serialize(w, vi); err != nil {
		return nil, err
	}
	if err := w.Flush(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// SerializeValueToBytes serializes a serializable value to a byte slice
func SerializeValueToBytes(s any, vi VarInfo) ([]byte, error) {
	w, b := binarystreams.NewMemoryWriter()
	if err := SerializeValue(s, w, vi); err != nil {
		return nil, err
	}
	if err := w.Flush(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// SerializeField serializes a single field to the output stream
func SerializeField(v any, fi *FieldInfo, w *binarystreams.Writer) error {
	rv := reflect.ValueOf(v)
	if rv.IsZero() {
		return nil
	}

	if err := w.WriteByte(fi.FieldID); err != nil {
		return err
	}

	wi, buf := binarystreams.NewMemoryWriter()
	if err := SerializeValue(v, wi, fi.VarInfo); err != nil {
		return err
	}
	if err := wi.Flush(); err != nil {
		return err
	}
	b := buf.Bytes()

	if err := SerializePacked64(uint64(len(b)), w); err != nil {
		return err
	}
	if err := w.WriteBytes(b); err != nil {
		return err
	}

	return nil
}

// SerializeBool serializes a single bool to the writer
func SerializeBool(v bool, w *binarystreams.Writer) error {
	vo := byte(0)
	if v {
		vo = 1
	}
	return w.WriteByte(vo)
}

// SerializeString serializes a single string to the writer
func SerializeString(v string, w *binarystreams.Writer) error {
	if err := SerializePacked64(int64(len(v)), w); err != nil {
		return err
	}
	return w.WriteBytes([]byte(v))
}

// SerializeFloat32 serializes a single float32 to the writer
func SerializeFloat32(f float32, w *binarystreams.Writer) error {
	return w.WriteFloat32(f)
}

// SerializeFloat64 serializes a single float64 to the writer
func SerializeFloat64(f float64, w *binarystreams.Writer) error {
	return w.WriteFloat64(f)
}

// SerializeTime serializes a single time.Time to the writer
func SerializeTime(v time.Time, w *binarystreams.Writer) error {
	return SerializePacked64(v.UnixMilli(), w)
}

// SerializeValue serializes a single value to the binarywriter, entirely based on the raw type of the passed variable.
func SerializeValue(v any, w *binarystreams.Writer, vi VarInfo) error {
	switch vit := vi.(type) {
	case *VarInfoDynamic:
		return SerializeDynamicValue(v, w, vit)
	case *VarInfoPointer:
		return SerializePointerValue(v, w, vit)
	case *VarInfoPrimitive:
		return SerializePrimitiveValue(v, w, vit)
	case *VarInfoArray:
		return SerializeArrayValue(v, w, vit)
	case *VarInfoMap:
		return SerializeMapValue(v, w, vit)
	case *VarInfoStruct:
		return SerializeStructValue(v, w, vit)
	default:
		return fmt.Errorf("unsupported varinfo type: %T with value %T", vi, v)
	}
}

// SerializeStructValue serializes a single struct to the writer
func SerializeStructValue(v any, w *binarystreams.Writer, vi *VarInfoStruct) error {
	tv := reflect.TypeOf(v)
	if tv.Kind() != reflect.Pointer {
		// .. make one up?
		ptv := reflect.New(tv)
		ptv.Elem().Set(reflect.ValueOf(v))
		v = ptv.Interface()
		tv = ptv.Type()
		// return fmt.Errorf("SerializeStructValue expects a pointer to a struct, got %T", v)
	}

	if vv, ok := v.(Serializable); ok {
		return vv.Serialize(w, vi)
	}
	if si, err := GetSerializerForType(tv.Elem()); err == nil {
		return si.Serialize(v, w, vi)
	}
	return fmt.Errorf("no serializer found for type %T", v)
}

// SerializeMapValue serializes a single map to the writer
func SerializeMapValue(v any, w *binarystreams.Writer, vi *VarInfoMap) error {
	rv := reflect.ValueOf(v)
	if !vi.NotNil {
		isNil := rv.IsNil()
		if err := SerializeBool(isNil, w); err != nil {
			return err
		}

		if isNil {
			// And we're done
			return nil
		}
	}

	isSet := vi.ElemType == nil

	if err := SerializePacked64(uint64(rv.Len()), w); err != nil {
		return err
	}

	iter := rv.MapRange()
	for iter.Next() {
		k := iter.Key()
		if err := SerializeValue(k.Interface(), w, vi.KeyType); err != nil {
			return err
		}
		if !isSet {
			vv := iter.Value()
			if err := SerializeValue(vv.Interface(), w, vi.ElemType); err != nil {
				return err
			}
		}
	}

	return nil
}

// SerializeArrayValue serializes a single array to the writer
func SerializeArrayValue(v any, w *binarystreams.Writer, vi *VarInfoArray) error {
	rv := reflect.ValueOf(v)
	if !vi.NotNil {
		isNil := rv.IsNil()
		if err := SerializeBool(isNil, w); err != nil {
			return err
		}

		if isNil {
			// And we're done
			return nil
		}
	}

	if err := SerializePacked64(int64(rv.Len()), w); err != nil {
		return err
	}

	if vit, ok := vi.ElemType.(*VarInfoPrimitive); ok && vit.DataType == SerializationTypeUint8 {
		return w.WriteBytes(v.([]byte))
	}

	for i := range rv.Len() {
		if err := SerializeValue(rv.Index(i).Interface(), w, vi.ElemType); err != nil {
			return err
		}
	}

	return nil
}

// serializePacked64Any serializes a single numeric integer to the writer, casting it to a real primitive as needed
func serializePacked64Any[T int8 | int16 | int32 | int64 | uint8 | uint16 | uint32 | uint64](v any, w *binarystreams.Writer) error {
	vc, ok := v.(T)
	if !ok {
		vv, err := castTo[T](v)
		if err != nil {
			return err
		}
		vc = vv
	}
	return SerializePacked64(vc, w)
}

// SerializePrimitiveValue serializes a single primitive to the writer
func SerializePrimitiveValue(v any, w *binarystreams.Writer, vi *VarInfoPrimitive) error {
	switch vi.DataType {
	case SerializationTypeBool:
		return SerializeBool(v.(bool), w)
	case SerializationTypeString:
		var vo string
		if vot, ok := v.(string); ok {
			vo = vot
		} else {
			vo = reflect.ValueOf(v).Convert(stringType).String()
		}
		return SerializeString(vo, w)
	case SerializationTypeInt8:
		return serializePacked64Any[int8](v, w)
	case SerializationTypeInt16:
		return serializePacked64Any[int16](v, w)
	case SerializationTypeInt32:
		return serializePacked64Any[int32](v, w)
	case SerializationTypeInt64:
		return serializePacked64Any[int64](v, w)
	case SerializationTypeUint8:
		return serializePacked64Any[uint8](v, w)
	case SerializationTypeUint16:
		return serializePacked64Any[uint16](v, w)
	case SerializationTypeUint32:
		return serializePacked64Any[uint32](v, w)
	case SerializationTypeUint64:
		return serializePacked64Any[uint64](v, w)
	case SerializationTypeFloat32:
		return SerializeFloat32(v.(float32), w)
	case SerializationTypeFloat64:
		return SerializeFloat64(v.(float64), w)
	case SerializationTypeTime:
		return SerializeTime(v.(time.Time), w)
	default:
		return fmt.Errorf("unsupported primitive type: %+v", vi.DataType)
	}
}

// SerializePointerValue serializes a single pointer to the writer
func SerializePointerValue(v any, w *binarystreams.Writer, vi *VarInfoPointer) error {
	rv := reflect.ValueOf(v)
	if !vi.NotNil {
		isNil := rv.IsNil()
		if err := SerializeBool(isNil, w); err != nil {
			return err
		}

		if isNil {
			// And we're done
			return nil
		}
	}

	if vis, ok := vi.SubType.(*VarInfoStruct); ok {
		// Leave the pointer on
		return SerializeStructValue(v, w, vis)
	}

	return SerializeValue(rv.Elem().Interface(), w, vi.SubType)
}

// SerializeDynamicValue serializes a dynamic value to the writer, generating and serializing the type info first
func SerializeDynamicValue(v any, w *binarystreams.Writer, _ *VarInfoDynamic) error {
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return fmt.Errorf("nil value passed to SerializeValue")
	}

	tv := rv.Type()
	vie, err := VarInfoFromType(tv)
	if err != nil {
		return err
	}

	sd, err := vie.GetSerializationData()
	if err != nil {
		return err
	}
	if err := w.WriteBytes(sd); err != nil {
		return err
	}

	return SerializeValue(v, w, vie)
}

// SerializePacked64 serializes a packed signed or unsigned number to the binarywriter
func SerializePacked64[T int8 | int16 | int32 | int64 | uint8 | uint16 | uint32 | uint64](vr T, w *binarystreams.Writer) error {
	signBit := byte(0)
	if vr < 0 {
		signBit = 0x40
		vr = -vr
	}

	v := uint64(vr)
	if v < 1<<6 {
		return w.WriteByte(byte(v&0x3F) | signBit)
	}
	if err := w.WriteByte(byte(v&0x3F) | signBit | 0x80); err != nil {
		return err
	}
	v >>= 6

	if v < 1<<7 {
		return w.WriteByte(byte(v & 0x7F))
	}
	if err := w.WriteByte(byte(v&0x7F) | 0x80); err != nil {
		return err
	}
	v >>= 7

	if v < 1<<15 {
		return w.WriteUInt16(uint16(v & 0x7FFF))
	}
	if err := w.WriteUInt16(uint16(v&0x7FFF) | 0x8000); err != nil {
		return err
	}
	v >>= 15

	if v < 1<<31 {
		return w.WriteUInt32(uint32(v & 0x7FFFFFFF))
	}
	if err := w.WriteUInt32(uint32(v&0x7FFFFFFF) | 0x80000000); err != nil {
		return err
	}
	v >>= 31

	// up to 5 bits left over
	return w.WriteByte(byte(v))
}
