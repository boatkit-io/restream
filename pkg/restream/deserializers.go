package restream

import (
	"fmt"
	"reflect"
	"time"

	"github.com/boatkit-io/restream/pkg/binarystreams"
)

// DeserializeFielded deserializes a set of fields from an input stream.
// The input reader must EOF at the end of the prescribed bytes for the fields.
func DeserializeFielded(r *binarystreams.Reader, _ []FieldInfo, fieldMap map[byte]*FieldInfo, fieldPtrs []any) error {
	for !r.IsEOF() {
		fieldID, err := r.ReadByte()
		if err != nil {
			return err
		}
		fieldLen, err := DeserializePacked64[uint64](r)
		if err != nil {
			return err
		}
		fi, has := fieldMap[fieldID]
		if !has {
			// Don't know about this field, move on
			if err := r.SkipBytes(int(fieldLen)); err != nil {
				return err
			}
			continue
		}
		nr, err := r.Slice(int(fieldLen))
		if err != nil {
			return err
		}
		if err := DeserializeValue(fieldPtrs[fi.FieldIdx], nr, fi.VarInfo); err != nil {
			return fmt.Errorf("deserialize field %q (fieldID=%d): %w", fi.Name, fieldID, err)
		}
	}
	return nil
}

// DeserializeValue deserializes a single typed value from the stream, entirely based on the output type.  v needs to be a pointer to
// the receiving variable of the deserialization.
func DeserializeValue(v any, r *binarystreams.Reader, vi VarInfo) error { //nolint:gocyclo,funlen
	switch vit := vi.(type) {
	case *VarInfoDynamic:
		return DeserializeDynamicValue(v, r, vit)
	case *VarInfoPointer:
		return DeserializePointerValue(v, r, vit)
	case *VarInfoPrimitive:
		return DeserializePrimitiveValue(v, r, vit)
	case *VarInfoArray:
		return DeserializeArrayValue(v, r, vit)
	case *VarInfoMap:
		return DeserializeMapValue(v, r, vit)
	case *VarInfoStruct:
		return DeserializeStructValue(v, r, vit)
	default:
		return fmt.Errorf("unsupported varinfo type: %T with value %T", vi, v)
	}
}

// DeserializePointerValue deserializes a pointer value from the stream based on the VarInfoPointer.  v needs to be a pointer to
// the receiving variable of the deserialization.
func DeserializePointerValue(v any, r *binarystreams.Reader, vi *VarInfoPointer) error {
	// v = ptr to ptr to vi.SubType
	// rv = ptr to vi.SubType
	rv := reflect.ValueOf(v).Elem()
	tv := rv.Type()

	var err error
	var elemType reflect.Type
	if tv == anyType {
		elemType, err = vi.SubType.ToGolangType()
		if err != nil {
			return err
		}
	} else {
		// if v isn't a pointer to an any (dynamic), just use the type passed in of what v's ptr points to
		elemType = tv.Elem()
	}

	if !vi.NotNil {
		bIsNull, err := r.ReadByte()
		if err != nil {
			return err
		}

		var rvn reflect.Value
		if bIsNull == 1 {
			rvn = reflect.Zero(reflect.PointerTo(elemType))
			rv.Set(rvn)
			return nil
		}
	}

	// Has data
	rvn := reflect.New(elemType)
	if err := DeserializeValue(rvn.Interface(), r, vi.SubType); err != nil {
		return err
	}
	rv.Set(rvn)
	return nil
}

// DeserializePrimitiveValue deserializes a primitive value from the stream based on the VarInfoPrimitive.  v needs to be a pointer to
// the receiving variable of the deserialization.
func DeserializePrimitiveValue(v any, r *binarystreams.Reader, vi *VarInfoPrimitive) error {
	var err error
	switch vi.DataType {
	case SerializationTypeBool:
		err = DeserializeBool(v, r)
	case SerializationTypeString:
		err = DeserializeString(v, r)
	case SerializationTypeInt8:
		err = DeserializeInt[int8](v, r)
	case SerializationTypeInt16:
		err = DeserializeInt[int16](v, r)
	case SerializationTypeInt32:
		err = DeserializeInt[int32](v, r)
	case SerializationTypeInt64:
		err = DeserializeInt[int64](v, r)
	case SerializationTypeUint8:
		err = DeserializeUint[uint8](v, r)
	case SerializationTypeUint16:
		err = DeserializeUint[uint16](v, r)
	case SerializationTypeUint32:
		err = DeserializeUint[uint32](v, r)
	case SerializationTypeUint64:
		err = DeserializeUint[uint64](v, r)
	case SerializationTypeFloat32:
		err = DeserializeFloat32(v, r)
	case SerializationTypeFloat64:
		err = DeserializeFloat64(v, r)
	case SerializationTypeTime:
		err = DeserializeTime(v, r)
	default:
		return fmt.Errorf("unhandled DataType in DeserializePrimitiveValue: %+v %T", vi.DataType, v)
	}
	if err != nil {
		return err
	}
	if vi.MappedType == nil {
		return nil
	}

	// TODO: Do we even bother handling mapped types?...

	return nil
}

// DeserializeBool deserializes a bool value from the stream.  v needs to be a pointer to
// the receiving variable of the deserialization.
func DeserializeBool(v any, r *binarystreams.Reader) error {
	vb, err := r.ReadByte()
	if err != nil {
		return err
	}
	if vb > 1 {
		return fmt.Errorf("unhandled deserialized bool val in DeserializeBool: %X", vb)
	}
	val := vb == 1
	if vn, ok := v.(*bool); ok {
		*vn = val
	} else {
		*(v.(*any)) = val
	}
	return nil
}

// DeserializeString deserializes a string value from the stream.  v needs to be a pointer to
// the receiving variable of the deserialization.
func DeserializeString(v any, r *binarystreams.Reader) error {
	sl, err := DeserializePacked64[uint64](r)
	if err != nil {
		return err
	}
	b, err := r.ReadBytes(int(sl))
	if err != nil {
		return err
	}
	val := string(b)
	if vn, ok := v.(*string); ok {
		*vn = val
	} else if vn, ok := v.(*any); ok {
		*vn = val
	} else {
		rv := reflect.ValueOf(v).Elem()
		rvt := rv.Type()

		svtv := reflect.ValueOf(val)
		if !svtv.CanConvert(rvt) {
			return fmt.Errorf("can't convert %T to %T in DeserializeString", val, v)
		}
		rv.Set(svtv.Convert(rvt))
	}
	return nil
}

// DeserializeInt deserializes an int value from the stream.  v needs to be a pointer to
// the receiving variable of the deserialization.
func DeserializeInt[T int64 | int32 | int16 | int8 | int](v any, r *binarystreams.Reader) error {
	sv, err := DeserializePacked64[int64](r)
	if err != nil {
		return err
	}
	val := T(sv)
	if vn, ok := v.(*T); ok {
		*vn = val
	} else {
		rv := reflect.ValueOf(v).Elem()
		rvt := rv.Type()

		svtv := reflect.ValueOf(val)
		if !svtv.CanConvert(rvt) {
			return fmt.Errorf("can't convert %T to %T in DeserializeInt", val, v)
		}
		rv.Set(svtv.Convert(rvt))
	}
	return nil
}

// DeserializeUint deserializes an uint value from the stream.  v needs to be a pointer to
// the receiving variable of the deserialization.
func DeserializeUint[T uint64 | uint32 | uint16 | uint8 | uint](v any, r *binarystreams.Reader) error {
	sv, err := DeserializePacked64[uint64](r)
	if err != nil {
		return err
	}
	val := T(sv)
	if vn, ok := v.(*T); ok {
		*vn = val
	} else {
		rv := reflect.ValueOf(v).Elem()
		rvt := rv.Type()

		svtv := reflect.ValueOf(val)
		if !svtv.CanConvert(rvt) {
			return fmt.Errorf("can't convert %T to %T in DeserializeUint", val, v)
		}
		rv.Set(svtv.Convert(rvt))
	}
	return nil
}

// DeserializeFloat32 deserializes a float32 value from the stream.  v needs to be a pointer to
// the receiving variable of the deserialization.
func DeserializeFloat32(v any, r *binarystreams.Reader) error {
	fv, err := r.ReadFloat32()
	if err != nil {
		return err
	}
	if vn, ok := v.(*float32); ok {
		*vn = fv
	} else {
		*(v.(*any)) = fv
	}
	return nil
}

// DeserializeFloat64 deserializes a float64 value from the stream.  v needs to be a pointer to
// the receiving variable of the deserialization.
func DeserializeFloat64(v any, r *binarystreams.Reader) error {
	fv, err := r.ReadFloat64()
	if err != nil {
		return err
	}
	if vn, ok := v.(*float64); ok {
		*vn = fv
	} else {
		*(v.(*any)) = fv
	}
	return nil
}

// DeserializeTime deserializes a time.Time value from the stream.  v needs to be a pointer to
// the receiving variable of the deserialization.
func DeserializeTime(v any, r *binarystreams.Reader) error {
	ti, err := DeserializePacked64[int64](r)
	if err != nil {
		return err
	}
	if vn, ok := v.(*time.Time); ok {
		*vn = time.UnixMilli(ti).UTC()
	} else {
		*(v.(*any)) = time.UnixMilli(ti).UTC()
	}
	return nil
}

// DeserializeArrayValue deserializes an array value from the stream.  v needs to be a pointer to
// the receiving variable of the deserialization.
func DeserializeArrayValue(v any, r *binarystreams.Reader, vi *VarInfoArray) error {
	rv := reflect.ValueOf(v).Elem()
	tv := rv.Type()

	var err error
	var elemType reflect.Type
	if tv == anyType {
		elemType, err = vi.ElemType.ToGolangType()
		if err != nil {
			return err
		}
	} else {
		elemType = tv.Elem()
	}

	arrayType := reflect.SliceOf(elemType)

	if !vi.NotNil {
		bIsNull, err := r.ReadByte()
		if err != nil {
			return err
		}

		if bIsNull == 1 {
			rvn := reflect.Zero(arrayType)
			rv.Set(rvn)
			return nil
		}
	}

	al, err := DeserializePacked64[uint64](r)
	if err != nil {
		return err
	}

	// See if we can shortcut a byte array
	if vip, ok := vi.ElemType.(*VarInfoPrimitive); ok && vip.DataType == SerializationTypeUint8 {
		if ov, ok := v.(*[]byte); ok {
			b, err := r.ReadBytes(int(al))
			if err != nil {
				return err
			}
			*ov = b
			return nil
		}
	}

	nv := reflect.MakeSlice(arrayType, int(al), int(al))
	rv.Set(nv)
	for i := range int(al) {
		if err := DeserializeValue(nv.Index(i).Addr().Interface(), r, vi.ElemType); err != nil {
			return err
		}
	}
	return nil
}

// DeserializeMapValue deserializes a map value from the stream.  v needs to be a pointer to
// the receiving variable of the deserialization.
func DeserializeMapValue(v any, r *binarystreams.Reader, vi *VarInfoMap) error {
	rv := reflect.ValueOf(v).Elem()
	tv := rv.Type()

	var err error
	var keyType, elemType reflect.Type
	needsDeref := false
	if tv == anyType {
		needsDeref = true

		keyType, err = vi.KeyType.ToGolangType()
		if err != nil {
			return err
		}
		elemType, err = vi.ElemType.ToGolangType()
		if err != nil {
			return err
		}
	} else {
		keyType = tv.Key()
		elemType = tv.Elem()
	}
	mapType := reflect.MapOf(keyType, elemType)

	if !vi.NotNil {
		bIsNull, err := r.ReadByte()
		if err != nil {
			return err
		}

		if bIsNull == 1 {
			rvn := reflect.Zero(mapType)
			rv.Set(rvn)
			return nil
		}
	}

	elemCount, err := DeserializePacked64[uint64](r)
	if err != nil {
		return err
	}

	rv.Set(reflect.MakeMapWithSize(mapType, int(elemCount)))
	if needsDeref {
		rv = rv.Elem()
	}

	isSet := vi.ElemType == nil
	for range elemCount {
		kv := reflect.New(keyType)
		if err := DeserializeValue(kv.Interface(), r, vi.KeyType); err != nil {
			return err
		}

		vv := reflect.New(elemType)
		if !isSet {
			if err := DeserializeValue(vv.Interface(), r, vi.ElemType); err != nil {
				return err
			}
		}

		rv.SetMapIndex(kv.Elem(), vv.Elem())
	}
	return nil
}

// DeserializeStructValue deserializes a struct value from the stream.  v needs to be a pointer to
// the receiving variable of the deserialization.
func DeserializeStructValue(v any, r *binarystreams.Reader, vi *VarInfoStruct) error {
	if vv, ok := v.(Serializable); ok {
		return vv.Deserialize(r, vi)
	}
	si, err := GetSerializerForType(reflect.TypeOf(v).Elem())
	if err != nil {
		return err
	}
	return si.Deserialize(v, r, vi)
}

// DeserializeDynamicValue deserializes a dynamic value from the stream, based entirely on the VarInfo serialization over
// the wire.  v needs to be a pointer to the receiving variable of the deserialization.
func DeserializeDynamicValue(vr any, r *binarystreams.Reader, _ *VarInfoDynamic) error { //nolint:gocyclo,funlen
	rv := reflect.ValueOf(vr)
	tv := rv.Type()

	if tv.Kind() != reflect.Pointer {
		return fmt.Errorf("non-pointer passed to DeserializeValue with type info")
	}

	tv = tv.Elem()

	if tv.Kind() != reflect.Interface {
		return fmt.Errorf("non-pointer-to-interface passed to DeserializeValue with type info")
	}

	v := vr.(*any)

	vi, err := VarInfoFromReader(r)
	if err != nil {
		return err
	}

	switch vit := vi.(type) {
	case *VarInfoPrimitive:
		return DeserializePrimitiveValue(v, r, vit)
	case *VarInfoArray:
		return DeserializeArrayValue(v, r, vit)
	case *VarInfoMap:
		return DeserializeMapValue(v, r, vit)
	case *VarInfoStruct:
		return DeserializeStructValue(v, r, vit)
	case *VarInfoPointer:
		return DeserializePointerValue(v, r, vit)
	default:
		return fmt.Errorf("unsupported dynamic type info during deserialize: %+v", vi)
	}
}

// DeserializePacked64 is a helper to deserialize a packed up-to-64-bit number from the binarystream
func DeserializePacked64[T int64 | uint64](r *binarystreams.Reader) (T, error) {
	ret := T(0)

	b, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	ret |= T(b & 0x3f)
	neg := (b & 0x40) > 0

	if b&0x80 > 0 {
		b, err = r.ReadByte()
		if err != nil {
			return 0, err
		}
		ret |= (T(b&0x7f) << 6)

		if b&0x80 > 0 {
			w, err := r.ReadUInt16()
			if err != nil {
				return 0, err
			}
			ret |= (T(w&0x7fff) << 13)

			if w&0x8000 > 0 {
				dw, err := r.ReadUInt32()
				if err != nil {
					return 0, err
				}
				ret |= (T(dw&0x7fffffff) << 28)

				if dw&0x80000000 > 0 {
					b, err = r.ReadByte()
					if err != nil {
						return 0, err
					}
					ret |= (T(b) << 59)
				}
			}
		}
	}

	if neg {
		return -ret, nil
	}
	return ret, nil
}
