package restream

import "reflect"

// CloneDynamicValue returns a best-effort deep clone of a dynamic value.
func CloneDynamicValue(value any) any {
	if value == nil {
		return nil
	}
	return cloneDynamicReflectValue(reflect.ValueOf(value)).Interface()
}

func cloneDynamicReflectValue(value reflect.Value) reflect.Value {
	if !value.IsValid() {
		return value
	}

	switch value.Kind() { //nolint:exhaustive // Unsupported kinds are immutable or intentionally copied by value below.
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		cloned := cloneDynamicReflectValue(value.Elem())
		ret := reflect.New(value.Type()).Elem()
		ret.Set(cloned)
		return ret
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		ret := reflect.New(value.Type().Elem())
		ret.Elem().Set(cloneDynamicReflectValue(value.Elem()))
		return ret
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		ret := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for idx := range value.Len() {
			ret.Index(idx).Set(cloneDynamicReflectValue(value.Index(idx)))
		}
		return ret
	case reflect.Array:
		ret := reflect.New(value.Type()).Elem()
		for idx := range value.Len() {
			ret.Index(idx).Set(cloneDynamicReflectValue(value.Index(idx)))
		}
		return ret
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		ret := reflect.MakeMapWithSize(value.Type(), value.Len())
		iter := value.MapRange()
		for iter.Next() {
			ret.SetMapIndex(cloneDynamicReflectValue(iter.Key()), cloneDynamicReflectValue(iter.Value()))
		}
		return ret
	case reflect.Struct:
		ret := reflect.New(value.Type()).Elem()
		ret.Set(value)
		for idx := range value.NumField() {
			field := ret.Field(idx)
			if !field.CanSet() {
				continue
			}
			field.Set(cloneDynamicReflectValue(value.Field(idx)))
		}
		return ret
	default:
		return value
	}
}
