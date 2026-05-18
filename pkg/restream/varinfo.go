package restream

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/boatkit-io/restream/pkg/binarystreams"
	"github.com/samber/lo"
)

// anyType is a perf saver type saver
var anyType = reflect.TypeFor[any]()

// boolType is a perf saver type saver
var boolType = reflect.TypeFor[bool]()

// stringType is a perf saver type saver
var stringType = reflect.TypeFor[string]()

// int8Type is a perf saver type saver
var int8Type = reflect.TypeFor[int8]()

// int16Type is a perf saver type saver
var int16Type = reflect.TypeFor[int16]()

// int32Type is a perf saver type saver
var int32Type = reflect.TypeFor[int32]()

// int64Type is a perf saver type saver
var int64Type = reflect.TypeFor[int64]()

// uint8Type is a perf saver type saver
var uint8Type = reflect.TypeFor[uint8]()

// uint16Type is a perf saver type saver
var uint16Type = reflect.TypeFor[uint16]()

// uint32Type is a perf saver type saver
var uint32Type = reflect.TypeFor[uint32]()

// uint64Type is a perf saver type saver
var uint64Type = reflect.TypeFor[uint64]()

// float32Type is a perf saver type saver
var float32Type = reflect.TypeFor[float32]()

// float64Type is a perf saver type saver
var float64Type = reflect.TypeFor[float64]()

// timeType is a perf saver type saver
var timeType = reflect.TypeFor[time.Time]()

// voidType is a perf saver for the isSet lookup for maps
var voidType = reflect.TypeFor[struct{}]()

// VarInfoFromType calculates a VarInfo from a reflect.Type, used for dynamic serialization
func VarInfoFromType(t reflect.Type) (VarInfo, error) {
	switch t.Kind() {
	case reflect.Bool:
		return &VarInfoPrimitive{DataType: SerializationTypeBool}, nil
	case reflect.String:
		return &VarInfoPrimitive{DataType: SerializationTypeString}, nil
	case reflect.Int8:
		return &VarInfoPrimitive{DataType: SerializationTypeInt8}, nil
	case reflect.Int16:
		return &VarInfoPrimitive{DataType: SerializationTypeInt16}, nil
	case reflect.Int32:
		return &VarInfoPrimitive{DataType: SerializationTypeInt32}, nil
	case reflect.Int64, reflect.Int:
		return &VarInfoPrimitive{DataType: SerializationTypeInt64}, nil
	case reflect.Uint8:
		return &VarInfoPrimitive{DataType: SerializationTypeUint8}, nil
	case reflect.Uint16:
		return &VarInfoPrimitive{DataType: SerializationTypeUint16}, nil
	case reflect.Uint32:
		return &VarInfoPrimitive{DataType: SerializationTypeUint32}, nil
	case reflect.Uint64, reflect.Uint:
		return &VarInfoPrimitive{DataType: SerializationTypeUint64}, nil
	case reflect.Float32:
		return &VarInfoPrimitive{DataType: SerializationTypeFloat32}, nil
	case reflect.Float64:
		return &VarInfoPrimitive{DataType: SerializationTypeFloat64}, nil

	case reflect.Pointer:
		tve, err := VarInfoFromType(t.Elem())
		if err != nil {
			return nil, err
		}
		return &VarInfoPointer{SubType: tve}, nil

	case reflect.Struct:
		switch getPackagedName(t) {
		case "time.Time":
			return &VarInfoPrimitive{DataType: SerializationTypeTime}, nil
		default:
			vis := &VarInfoStruct{Name: t.Name(), Package: getPackageName(t)}
			if strings.Contains(vis.Name, "[") {
				// Horrible hack because golang reflection doesn't give us a way to get the generic type arguments
				// so we have to do it by name.
				gt, ok := reflect.New(t).Interface().(SerializableGeneric)
				if !ok {
					return nil, fmt.Errorf("type %q does not implement SerializableGeneric", t.Name())
				}
				vis.GenericTypes = gt.GetTypeArgs()
			}
			return vis, nil
		}

	case reflect.Slice, reflect.Array:
		tve, err := VarInfoFromType(t.Elem())
		if err != nil {
			return nil, err
		}
		return &VarInfoArray{ElemType: tve}, nil

	case reflect.Map:
		kt, err := VarInfoFromType(t.Key())
		if err != nil {
			return nil, err
		}
		tet := t.Elem()
		var vt VarInfo
		if tet != voidType {
			vt, err = VarInfoFromType(tet)
			if err != nil {
				return nil, err
			}
		}
		return &VarInfoMap{KeyType: kt, ElemType: vt}, nil

	case reflect.Interface:
		return &VarInfoDynamic{}, nil

	default:
		return nil, fmt.Errorf("unsupported type: %+v", t)
	}
}

// VarInfoFromReader deserializes a VarInfo from a BinaryReader, used for dynamic deserialization
func VarInfoFromReader(r *binarystreams.Reader) (VarInfo, error) {
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	st := SerializationType(b)
	switch st {
	case SerializationTypeBool,
		SerializationTypeString,
		SerializationTypeInt8, SerializationTypeInt16, SerializationTypeInt32, SerializationTypeInt64,
		SerializationTypeUint8, SerializationTypeUint16, SerializationTypeUint32, SerializationTypeUint64,
		SerializationTypeFloat32, SerializationTypeFloat64,
		SerializationTypeTime:
		return &VarInfoPrimitive{DataType: SerializationType(b)}, nil
	case SerializationTypePointer:
		sub, err := VarInfoFromReader(r)
		if err != nil {
			return nil, err
		}
		return &VarInfoPointer{SubType: sub}, nil
	case SerializationTypeArray:
		sub, err := VarInfoFromReader(r)
		if err != nil {
			return nil, err
		}
		return &VarInfoArray{ElemType: sub}, nil
	case SerializationTypeMap:
		key, err := VarInfoFromReader(r)
		if err != nil {
			return nil, err
		}
		elem, err := VarInfoFromReader(r)
		if err != nil {
			return nil, err
		}
		return &VarInfoMap{KeyType: key, ElemType: elem}, nil
	case SerializationTypeDynamic:
		return &VarInfoDynamic{}, nil
	case SerializationTypeVoid:
		return nil, nil
	default:
		return nil, fmt.Errorf("can't dynamically deserialize varinfo for unknown serialization type: %+v", st)
	}
}

// VarInfo is an interface for a variable's information, used for dynamic serialization and deserialization
type VarInfo interface {
	IsNotNil() bool
	IsValueNotNil() bool
	GetSerializationData() ([]byte, error)
	ToGolangType() (reflect.Type, error)
	FillGenerics(map[string]VarInfo) VarInfo

	ToGolangString() string
	ToTSString() string
}

// VarInfoDynamic is a VarInfo for a dynamic type, used for dynamic serialization and deserialization
type VarInfoDynamic struct {
}

// IsNotNil implements VarInfo.
func (v *VarInfoDynamic) IsNotNil() bool {
	return false
}

// IsValueNotNil implements VarInfo.
func (v *VarInfoDynamic) IsValueNotNil() bool {
	return false
}

// ToGolangString implements VarInfo.
func (v *VarInfoDynamic) ToGolangString() string {
	return "restream.VarInfoDynamic{}"
}

// ToTSString implements VarInfo.
func (v *VarInfoDynamic) ToTSString() string {
	return "new VarInfoDynamic()"
}

// GetSerializationData implements VarInfo.
func (v *VarInfoDynamic) GetSerializationData() ([]byte, error) {
	return []byte{byte(SerializationTypeDynamic)}, nil
}

// ToGolangType implements VarInfo.
func (v *VarInfoDynamic) ToGolangType() (reflect.Type, error) {
	return anyType, nil
}

// FillGenerics implements VarInfo.
func (v *VarInfoDynamic) FillGenerics(map[string]VarInfo) VarInfo {
	return v
}

var _ VarInfo = (*VarInfoDynamic)(nil)

// VarInfoPrimitive is a VarInfo for a primitive type
type VarInfoPrimitive struct {
	DataType   SerializationType
	MappedType *string
}

// IsNotNil implements VarInfo.
func (v *VarInfoPrimitive) IsNotNil() bool {
	return false
}

// IsValueNotNil implements VarInfo.
func (v *VarInfoPrimitive) IsValueNotNil() bool {
	return false
}

// ToGolangString implements VarInfo.
func (v *VarInfoPrimitive) ToGolangString() string {
	mt := ""
	if v.MappedType != nil {
		mt = fmt.Sprintf(", MappedType: restream.Ptr(%q)", *v.MappedType)
	}
	return fmt.Sprintf("restream.VarInfoPrimitive{DataType: restream.SerializationType%+v%s}", v.DataType, mt)
}

// ToTSString implements VarInfo.
func (v *VarInfoPrimitive) ToTSString() string {
	mt := ""
	if v.MappedType != nil {
		mt = fmt.Sprintf(", %q", *v.MappedType)
	}
	return fmt.Sprintf("new VarInfoPrimitive(SerializationType.%s%s)", v.DataType, mt)
}

// GetSerializationData implements VarInfo.
func (v *VarInfoPrimitive) GetSerializationData() ([]byte, error) {
	return []byte{byte(v.DataType)}, nil
}

// ToGolangType implements VarInfo.
func (v *VarInfoPrimitive) ToGolangType() (reflect.Type, error) {
	switch v.DataType {
	case SerializationTypeBool:
		return boolType, nil
	case SerializationTypeString:
		return stringType, nil
	case SerializationTypeInt8:
		return int8Type, nil
	case SerializationTypeInt16:
		return int16Type, nil
	case SerializationTypeInt32:
		return int32Type, nil
	case SerializationTypeInt64:
		return int64Type, nil
	case SerializationTypeUint8:
		return uint8Type, nil
	case SerializationTypeUint16:
		return uint16Type, nil
	case SerializationTypeUint32:
		return uint32Type, nil
	case SerializationTypeUint64:
		return uint64Type, nil
	case SerializationTypeFloat32:
		return float32Type, nil
	case SerializationTypeFloat64:
		return float64Type, nil
	case SerializationTypeTime:
		return timeType, nil
	default:
		return nil, fmt.Errorf("no primitivetype for unsupported serializationtype %+v", v.DataType)
	}
}

// FillGenerics implements VarInfo.
func (v *VarInfoPrimitive) FillGenerics(map[string]VarInfo) VarInfo {
	return v
}

var _ VarInfo = (*VarInfoPrimitive)(nil)

// VarInfoPointer is a VarInfo for a pointer type
type VarInfoPointer struct {
	NotNil  bool
	SubType VarInfo
}

// IsNotNil implements VarInfo.
func (v *VarInfoPointer) IsNotNil() bool {
	return v.NotNil
}

// IsValueNotNil implements VarInfo.
func (v *VarInfoPointer) IsValueNotNil() bool {
	return v.SubType.IsNotNil()
}

// ToGolangString implements VarInfo.
func (v *VarInfoPointer) ToGolangString() string {
	return fmt.Sprintf("restream.VarInfoPointer{NotNil: %t, SubType: &%+v}", v.NotNil, v.SubType.ToGolangString())
}

// ToTSString implements VarInfo.
func (v *VarInfoPointer) ToTSString() string {
	return fmt.Sprintf("new VarInfoPointer(%t, %s)", v.NotNil, v.SubType.ToTSString())
}

// GetSerializationData implements VarInfo.
func (v *VarInfoPointer) GetSerializationData() ([]byte, error) {
	sd, err := v.SubType.GetSerializationData()
	if err != nil {
		return nil, err
	}
	return append([]byte{byte(SerializationTypePointer)}, sd...), nil
}

// ToGolangType implements VarInfo.
func (v *VarInfoPointer) ToGolangType() (reflect.Type, error) {
	it, err := v.SubType.ToGolangType()
	if err != nil {
		return nil, err
	}
	return reflect.PointerTo(it), nil
}

// FillGenerics implements VarInfo.
func (v *VarInfoPointer) FillGenerics(gm map[string]VarInfo) VarInfo {
	return &VarInfoPointer{NotNil: v.NotNil, SubType: v.SubType.FillGenerics(gm)}
}

var _ VarInfo = (*VarInfoPointer)(nil)

// VarInfoArray is a VarInfo for an array type
type VarInfoArray struct {
	NotNil   bool
	ElemType VarInfo
}

// IsNotNil implements VarInfo.
func (v *VarInfoArray) IsNotNil() bool {
	return v.NotNil
}

// IsValueNotNil implements VarInfo.
func (v *VarInfoArray) IsValueNotNil() bool {
	return v.ElemType.IsNotNil()
}

// ToGolangString implements VarInfo.
func (v *VarInfoArray) ToGolangString() string {
	return fmt.Sprintf("restream.VarInfoArray{NotNil: %t, ElemType: &%s}", v.NotNil, v.ElemType.ToGolangString())
}

// ToTSString implements VarInfo.
func (v *VarInfoArray) ToTSString() string {
	return fmt.Sprintf("new VarInfoArray(%t, %s)", v.NotNil, v.ElemType.ToTSString())
}

// GetSerializationData implements VarInfo.
func (v *VarInfoArray) GetSerializationData() ([]byte, error) {
	sd, err := v.ElemType.GetSerializationData()
	if err != nil {
		return nil, err
	}
	return append([]byte{byte(SerializationTypeArray)}, sd...), nil
}

// ToGolangType implements VarInfo.
func (v *VarInfoArray) ToGolangType() (reflect.Type, error) {
	it, err := v.ElemType.ToGolangType()
	if err != nil {
		return nil, err
	}
	return reflect.SliceOf(it), nil
}

// FillGenerics implements VarInfo.
func (v *VarInfoArray) FillGenerics(gm map[string]VarInfo) VarInfo {
	return &VarInfoArray{NotNil: v.NotNil, ElemType: v.ElemType.FillGenerics(gm)}
}

var _ VarInfo = (*VarInfoArray)(nil)

// VarInfoMap is a VarInfo for a map type
type VarInfoMap struct {
	NotNil   bool
	KeyType  VarInfo
	ElemType VarInfo
}

// IsNotNil implements VarInfo.
func (v *VarInfoMap) IsNotNil() bool {
	return v.NotNil
}

// IsValueNotNil implements VarInfo.
func (v *VarInfoMap) IsValueNotNil() bool {
	return v.ElemType.IsNotNil()
}

// ToGolangString implements VarInfo.
func (v *VarInfoMap) ToGolangString() string {
	et := ""
	if v.ElemType != nil {
		et = fmt.Sprintf(", ElemType: &%s", v.ElemType.ToGolangString())
	}
	return fmt.Sprintf("restream.VarInfoMap{NotNil: %t, KeyType: &%s%s}",
		v.NotNil, v.KeyType.ToGolangString(), et)
}

// ToTSString implements VarInfo.
func (v *VarInfoMap) ToTSString() string {
	ets := "undefined"
	if v.ElemType != nil {
		ets = v.ElemType.ToTSString()
	}
	return fmt.Sprintf("new VarInfoMap(%t, %s, %s)", v.NotNil, v.KeyType.ToTSString(), ets)
}

// GetSerializationData implements VarInfo.
func (v *VarInfoMap) GetSerializationData() ([]byte, error) {
	ksd, err := v.KeyType.GetSerializationData()
	if err != nil {
		return nil, err
	}
	var esd []byte
	if v.ElemType != nil {
		esd, err = v.ElemType.GetSerializationData()
		if err != nil {
			return nil, err
		}
	} else {
		esd = []byte{byte(SerializationTypeVoid)}
	}
	return append(append([]byte{byte(SerializationTypeMap)}, ksd...), esd...), nil
}

// ToGolangType implements VarInfo.
func (v *VarInfoMap) ToGolangType() (reflect.Type, error) {
	kt, err := v.KeyType.ToGolangType()
	if err != nil {
		return nil, err
	}
	var et reflect.Type
	if v.ElemType != nil {
		et, err = v.ElemType.ToGolangType()
		if err != nil {
			return nil, err
		}
	} else {
		et = voidType
	}
	return reflect.MapOf(kt, et), nil
}

// FillGenerics implements VarInfo.
func (v *VarInfoMap) FillGenerics(gm map[string]VarInfo) VarInfo {
	vik := v.KeyType.FillGenerics(gm)
	var vie VarInfo
	if v.ElemType != nil {
		vie = v.ElemType.FillGenerics(gm)
	}
	return &VarInfoMap{NotNil: v.NotNil, KeyType: vik, ElemType: vie}
}

var _ VarInfo = (*VarInfoMap)(nil)

// VarInfoGenericParam is a VarInfo for a generic type parameter
type VarInfoGenericParam struct {
	Name string
}

// FillGenerics implements VarInfo.
func (v *VarInfoGenericParam) FillGenerics(gm map[string]VarInfo) VarInfo {
	vi, has := gm[v.Name]
	if has {
		return vi
	}
	return v
}

// GetSerializationData implements VarInfo.
func (v *VarInfoGenericParam) GetSerializationData() ([]byte, error) {
	panic("unimplemented")
}

// ToGolangString implements VarInfo.
func (v *VarInfoGenericParam) ToGolangString() string {
	return fmt.Sprintf("restream.VarInfoGenericParam{Name: %q}", v.Name)
}

// ToGolangType implements VarInfo.
func (v *VarInfoGenericParam) ToGolangType() (reflect.Type, error) {
	panic("unimplemented")
}

// ToTSString implements VarInfo.
func (v *VarInfoGenericParam) ToTSString() string {
	return fmt.Sprintf("new VarInfoGenericParam(%q)", v.Name)
}

// IsNotNil implements VarInfo.
func (v *VarInfoGenericParam) IsNotNil() bool {
	return false
}

// IsValueNotNil implements VarInfo.
func (v *VarInfoGenericParam) IsValueNotNil() bool {
	return false
}

var _ VarInfo = (*VarInfoGenericParam)(nil)

// VarInfoStruct is a VarInfo for a struct type
type VarInfoStruct struct {
	Name         string
	Package      string
	FieldList    []FieldInfo
	GenericTypes []VarInfo
}

// IsNotNil implements VarInfo.
func (v *VarInfoStruct) IsNotNil() bool {
	return false
}

// IsValueNotNil implements VarInfo.
func (v *VarInfoStruct) IsValueNotNil() bool {
	return false
}

// GetPackagedName implements VarInfo.
func (v *VarInfoStruct) GetPackagedName(fromPkgName string) string {
	if v.Package == "" {
		panic("GetPackagedName called on struct with no package")
	}
	last, _ := lo.Last(strings.Split(v.Package, "/"))
	if last == fromPkgName {
		return v.Name
	}
	return last + "." + v.Name
}

// ToGolangString implements VarInfo.
func (v *VarInfoStruct) ToGolangString() string {
	gt := ""
	if v.GenericTypes != nil {
		gts := strings.Join(lo.Map(v.GenericTypes, func(gt VarInfo, _ int) string { return "&" + gt.ToGolangString() }), ", ")
		gt += ", GenericTypes: []restream.VarInfo{" + gts + "}"
	}
	if v.FieldList != nil {
		fields := strings.Join(lo.Map(v.FieldList, func(fi FieldInfo, _ int) string {
			return "restream.FieldInfo{" + fi.ToGolangString() + "}"
		}), ", ")
		gt += ", FieldList: []restream.FieldInfo{" + fields + "}"
	}
	return fmt.Sprintf("restream.VarInfoStruct{Name: %q, Package: %q%s}", v.Name, v.Package, gt)
}

// ToTSString implements VarInfo.
func (v *VarInfoStruct) ToTSString() string {
	gt := ""
	if v.FieldList != nil {
		fields := strings.Join(lo.Map(v.FieldList, func(fi FieldInfo, _ int) string {
			return fi.ToTSString()
		}), ", ")
		gt += ", [" + fields + "]"
	}
	if v.GenericTypes != nil {
		if v.FieldList == nil {
			gt += ", undefined"
		}
		gts := strings.Join(lo.Map(v.GenericTypes, func(gti VarInfo, _ int) string {
			return gti.ToTSString()
		}), ", ")
		gt += ", [" + gts + "]"
	}
	return fmt.Sprintf("new VarInfoStruct(%q, %q, %s%s)", v.Name, v.Package, v.Name, gt)
}

// GetSerializationData implements VarInfo.
func (v *VarInfoStruct) GetSerializationData() ([]byte, error) {
	return nil, fmt.Errorf("dynamic GetSerializationData of structs not supported")
}

// ToGolangType implements VarInfo.
func (v *VarInfoStruct) ToGolangType() (reflect.Type, error) {
	return nil, fmt.Errorf("dynamic ToGolangType of structs not supported")
}

// FillGenerics implements VarInfo.
func (v *VarInfoStruct) FillGenerics(map[string]VarInfo) VarInfo {
	return v
}

var _ VarInfo = (*VarInfoStruct)(nil)

// getPackagedName is a helper function to get the packaged name of a type (i.e. package.Name)
func getPackagedName(t reflect.Type) string {
	pp := t.PkgPath()
	if pp == "" {
		return t.Name()
	}
	last, _ := lo.Last(strings.Split(pp, "/"))
	return last + "." + t.Name()
}

// getPackageName is a helper function to get the package name of a type (i.e. package)
func getPackageName(t reflect.Type) string {
	pp := t.PkgPath()
	if pp == "" {
		return ""
	}
	last, _ := lo.Last(strings.Split(pp, "/"))
	return last
}
