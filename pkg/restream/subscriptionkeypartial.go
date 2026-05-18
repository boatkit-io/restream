package restream

import (
	"fmt"
	"reflect"
	"strconv"
)

func partialForSubscriptionKeyFromState[S any, SP StoreDataPtrType[S], P Partial](
	storeData *StoreData[S, SP, P],
	key string,
) (Partial, bool, error) {
	fieldPath := FieldPathFromSubscriptionKey(key)
	if len(fieldPath) == 0 {
		return nil, false, nil
	}

	var ret Partial
	var exists bool
	var retErr error
	storeData.ReadState(func(state SP) {
		partialValue, ok, err := partialForFieldPathReflect(reflect.ValueOf(state).Elem(), reflect.TypeFor[P](), fieldPath)
		if err != nil || !ok {
			exists = ok
			retErr = err
			return
		}
		ret = partialValue.Interface().(Partial)
		exists = true
	})
	return ret, exists, retErr
}

func partialForFieldPathReflect(stateValue reflect.Value, partialType reflect.Type, fieldPath []any) (reflect.Value, bool, error) {
	if len(fieldPath) == 0 {
		return reflect.Value{}, false, nil
	}
	if stateValue.Kind() == reflect.Pointer {
		if stateValue.IsNil() {
			return reflect.Value{}, false, nil
		}
		stateValue = stateValue.Elem()
	}
	if stateValue.Kind() != reflect.Struct {
		return reflect.Value{}, false, fmt.Errorf("cannot build subscription partial from non-struct state %s", stateValue.Type())
	}
	if partialType.Kind() != reflect.Pointer || partialType.Elem().Kind() != reflect.Struct {
		return reflect.Value{}, false, fmt.Errorf("subscription partial type %s is not a pointer to struct", partialType)
	}

	fieldName, ok := fieldPath[0].(string)
	if !ok {
		return reflect.Value{}, false, nil
	}

	stateField := stateValue.FieldByName(fieldName)
	if !stateField.IsValid() {
		return reflect.Value{}, false, nil
	}

	partialValue := reflect.New(partialType.Elem())
	partialField := partialValue.Elem().FieldByName(fieldName)
	if !partialField.IsValid() {
		return reflect.Value{}, false, nil
	}

	fieldPartial, ok, err := partialForFieldReflect(stateField, partialField.Type(), fieldPath[1:])
	if err != nil || !ok {
		return reflect.Value{}, ok, err
	}
	partialField.Set(fieldPartial)
	return partialValue, true, nil
}

func partialForFieldReflect(stateField reflect.Value, partialFieldType reflect.Type, childPath []any) (reflect.Value, bool, error) {
	if stateField.Kind() == reflect.Pointer && stateField.IsNil() {
		return reflect.Value{}, false, nil
	}
	if stateField.Kind() == reflect.Pointer {
		stateField = stateField.Elem()
	}

	switch stateField.Kind() { //nolint:exhaustive // Only collection and struct-keyed subscription paths are supported.
	case reflect.Map:
		return mapPartialForFieldReflect(stateField, partialFieldType, childPath)
	case reflect.Slice, reflect.Array:
		return arrayPartialForFieldReflect(stateField, partialFieldType, childPath)
	case reflect.Struct:
		if len(childPath) == 0 {
			return partialValueWithWholeReflect(stateField, partialFieldType)
		}
		return partialValueWithNestedPartialReflect(stateField, partialFieldType, childPath)
	default:
		if len(childPath) != 0 {
			return reflect.Value{}, false, nil
		}
		return primitivePartialFieldReflect(stateField, partialFieldType)
	}
}

func mapPartialForFieldReflect(stateField reflect.Value, partialFieldType reflect.Type, childPath []any) (reflect.Value, bool, error) {
	partialValue, err := newInitializedPartialReflect(partialFieldType)
	if err != nil {
		return reflect.Value{}, false, err
	}
	if len(childPath) == 0 {
		method := partialValue.MethodByName("SetWhole")
		if !method.IsValid() {
			return reflect.Value{}, false, nil
		}
		method.Call([]reflect.Value{stateField})
		return partialValue, true, nil
	}

	keyValue, ok := fieldPathPartToReflectValue(childPath[0], stateField.Type().Key())
	if !ok {
		return reflect.Value{}, false, nil
	}
	mapValue := stateField.MapIndex(keyValue)
	if !mapValue.IsValid() {
		deleteMethod := partialValue.MethodByName("Delete")
		if !deleteMethod.IsValid() {
			return reflect.Value{}, false, nil
		}
		deleteMethod.Call([]reflect.Value{keyValue})
		return partialValue, true, nil
	}
	if len(childPath) == 1 {
		setMethod := partialValue.MethodByName("Set")
		if !setMethod.IsValid() {
			return reflect.Value{}, false, nil
		}
		setMethod.Call([]reflect.Value{keyValue, mapValue})
		return partialValue, true, nil
	}

	applyPartialMethod := partialValue.MethodByName("ApplyPartial")
	if !applyPartialMethod.IsValid() {
		return reflect.Value{}, false, nil
	}
	nestedPartial, ok, err := partialForFieldPathReflect(mapValue, applyPartialMethod.Type().In(1), childPath[1:])
	if err != nil || !ok {
		return reflect.Value{}, ok, err
	}
	applyPartialMethod.Call([]reflect.Value{keyValue, nestedPartial})
	return partialValue, true, nil
}

func arrayPartialForFieldReflect(stateField reflect.Value, partialFieldType reflect.Type, childPath []any) (reflect.Value, bool, error) {
	partialValue, err := newInitializedPartialReflect(partialFieldType)
	if err != nil {
		return reflect.Value{}, false, err
	}
	if len(childPath) == 0 {
		method := partialValue.MethodByName("SetWhole")
		if !method.IsValid() {
			return reflect.Value{}, false, nil
		}
		method.Call([]reflect.Value{stateField})
		return partialValue, true, nil
	}

	indexValue, ok := fieldPathPartToReflectValue(childPath[0], reflect.TypeFor[int]())
	if !ok {
		return reflect.Value{}, false, nil
	}
	index := int(indexValue.Int())
	if index < 0 || index >= stateField.Len() {
		return reflect.Value{}, false, nil
	}
	if len(childPath) == 1 {
		setMethod := partialValue.MethodByName("Set")
		if !setMethod.IsValid() {
			return reflect.Value{}, false, nil
		}
		setMethod.Call([]reflect.Value{indexValue, stateField.Index(index)})
		return partialValue, true, nil
	}

	applyPartialMethod := partialValue.MethodByName("ApplyPartial")
	if !applyPartialMethod.IsValid() {
		return reflect.Value{}, false, nil
	}
	nestedPartial, ok, err := partialForFieldPathReflect(stateField.Index(index), applyPartialMethod.Type().In(1), childPath[1:])
	if err != nil || !ok {
		return reflect.Value{}, ok, err
	}
	applyPartialMethod.Call([]reflect.Value{indexValue, nestedPartial})
	return partialValue, true, nil
}

func partialValueWithWholeReflect(stateField reflect.Value, partialFieldType reflect.Type) (reflect.Value, bool, error) {
	partialValue, err := newInitializedPartialReflect(partialFieldType)
	if err != nil {
		return reflect.Value{}, false, err
	}
	method := partialValue.MethodByName("SetWhole")
	if !method.IsValid() {
		return reflect.Value{}, false, nil
	}
	argType := method.Type().In(0)
	valuePtr := reflect.New(argType.Elem())
	valuePtr.Elem().Set(stateField)
	method.Call([]reflect.Value{valuePtr})
	return partialValue, true, nil
}

func partialValueWithNestedPartialReflect(
	stateField reflect.Value,
	partialFieldType reflect.Type,
	childPath []any,
) (reflect.Value, bool, error) {
	partialValue, err := newInitializedPartialReflect(partialFieldType)
	if err != nil {
		return reflect.Value{}, false, err
	}
	method := partialValue.MethodByName("ApplyPartial")
	if !method.IsValid() {
		return reflect.Value{}, false, nil
	}
	nestedPartial, ok, err := partialForFieldPathReflect(stateField, method.Type().In(0), childPath)
	if err != nil || !ok {
		return reflect.Value{}, ok, err
	}
	method.Call([]reflect.Value{nestedPartial})
	return partialValue, true, nil
}

func primitivePartialFieldReflect(stateField reflect.Value, partialFieldType reflect.Type) (reflect.Value, bool, error) {
	if partialFieldType.Kind() != reflect.Pointer {
		return reflect.Value{}, false, nil
	}
	ret := reflect.New(partialFieldType.Elem())
	if stateField.Type().AssignableTo(partialFieldType.Elem()) {
		ret.Elem().Set(stateField)
		return ret, true, nil
	}
	if stateField.Type().ConvertibleTo(partialFieldType.Elem()) {
		ret.Elem().Set(stateField.Convert(partialFieldType.Elem()))
		return ret, true, nil
	}
	return reflect.Value{}, false, nil
}

func newInitializedPartialReflect(partialType reflect.Type) (reflect.Value, error) {
	if partialType.Kind() != reflect.Pointer || partialType.Elem().Kind() != reflect.Struct {
		return reflect.Value{}, fmt.Errorf("partial field type %s is not a pointer to struct", partialType)
	}
	ret := reflect.New(partialType.Elem())
	elem := ret.Elem()
	for idx := 0; idx < elem.NumField(); idx++ {
		field := elem.Field(idx)
		if field.Kind() == reflect.Map && field.IsNil() && field.CanSet() {
			field.Set(reflect.MakeMap(field.Type()))
		}
	}
	return ret, nil
}

func fieldPathPartToReflectValue(raw any, targetType reflect.Type) (reflect.Value, bool) {
	if rawValue := reflect.ValueOf(raw); rawValue.IsValid() {
		if rawValue.Type().AssignableTo(targetType) {
			return rawValue, true
		}
		if rawValue.Type().ConvertibleTo(targetType) {
			return rawValue.Convert(targetType), true
		}
	}

	rawString := fmt.Sprint(raw)
	ret := reflect.New(targetType).Elem()
	switch targetType.Kind() { //nolint:exhaustive // Only key/index kinds supported by ReSub field paths are needed here.
	case reflect.String:
		ret.SetString(rawString)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		parsed, err := strconv.ParseInt(rawString, 10, targetType.Bits())
		if err != nil {
			return reflect.Value{}, false
		}
		ret.SetInt(parsed)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		parsed, err := strconv.ParseUint(rawString, 10, targetType.Bits())
		if err != nil {
			return reflect.Value{}, false
		}
		ret.SetUint(parsed)
	default:
		return reflect.Value{}, false
	}
	return ret, true
}
