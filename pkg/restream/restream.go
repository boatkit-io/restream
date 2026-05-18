// Package restream contains logic for creating and managing stores that are compatible with serializing/deserializing and syncing with
// web clients running ReSub
package restream

import (
	"fmt"
	"reflect"
)

// castTo casts a value to a given type, returning an error if it's not convertible
func castTo[T any](v any) (T, error) {
	rv := reflect.ValueOf(v)
	ttv := reflect.TypeFor[T]()
	if rv.Type().ConvertibleTo(ttv) {
		return rv.Convert(ttv).Interface().(T), nil
	}
	var vz T
	return vz, fmt.Errorf("cannot cast %T to %s", v, ttv.String())
}
