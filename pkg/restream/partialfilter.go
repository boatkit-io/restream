package restream

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

const compoundKeyJoinerString = "%&"

// FieldFilteredPartial can be implemented by partial structures that can narrow themselves to a subset of changed fields.
type FieldFilteredPartial interface {
	FilterToFields(fields [][]any) (Partial, bool)
}

// FilterPartialToFields returns a copy of partial containing only the requested field paths, when supported.
func FilterPartialToFields[P Partial](partial P, fields [][]any) (P, bool) {
	var zero P
	if len(fields) == 0 {
		return zero, false
	}
	for _, field := range fields {
		if len(field) == 0 {
			return partial, true
		}
	}
	if filterable, ok := any(partial).(FieldFilteredPartial); ok {
		filtered, exists := filterable.FilterToFields(fields)
		if !exists {
			return zero, false
		}
		return filtered.(P), true
	}
	return partial, true
}

// ChildFieldsForField filters full field paths down to the children under a named top-level field.
func ChildFieldsForField(fields [][]any, fieldName string) [][]any {
	ret := [][]any{}
	for _, field := range fields {
		if len(field) == 0 {
			ret = append(ret, []any{})
			continue
		}
		fieldNamePart, ok := field[0].(string)
		if !ok || subscriptionKeyPart(fieldNamePart) != subscriptionKeyPart(fieldName) {
			continue
		}
		ret = append(ret, append([]any{}, field[1:]...))
	}
	return ret
}

// ReduceFieldPaths removes redundant child paths when an ancestor path is already present.
func ReduceFieldPaths(fields [][]any) [][]any {
	if len(fields) < 2 {
		return fields
	}

	ret := make([][]any, 0, len(fields))
	for _, field := range fields {
		suppressed := false
		writeIdx := 0
		for _, existing := range ret {
			switch {
			case fieldPathHasPrefix(field, existing):
				suppressed = true
				ret[writeIdx] = existing
				writeIdx++
			case fieldPathHasPrefix(existing, field):
				continue
			default:
				ret[writeIdx] = existing
				writeIdx++
			}
		}
		ret = ret[:writeIdx]
		if !suppressed {
			ret = append(ret, field)
		}
	}
	return ret
}

func fieldPathHasPrefix(field []any, prefix []any) bool {
	if len(prefix) > len(field) {
		return false
	}
	for idx := range prefix {
		if !reflect.DeepEqual(field[idx], prefix[idx]) {
			return false
		}
	}
	return true
}

// SubscriptionKeyFromFieldPath converts a server-side Go partial field path into the matching client ReSub key.
func SubscriptionKeyFromFieldPath(field []any) string {
	parts := make([]string, 0, len(field))
	for _, part := range field {
		parts = append(parts, subscriptionKeyPart(part))
	}
	return strings.Join(parts, compoundKeyJoinerString)
}

// SplitSubscriptionKey splits a client ReSub compound key into its parts.
func SplitSubscriptionKey(key string) []string {
	if key == "" {
		return nil
	}
	return strings.Split(key, compoundKeyJoinerString)
}

// FieldPathFromSubscriptionKey converts a client ReSub subscription key into a server-side field path.
func FieldPathFromSubscriptionKey(key string) []any {
	parts := SplitSubscriptionKey(key)
	if len(parts) == 0 {
		return nil
	}

	ret := make([]any, 0, len(parts))
	ret = append(ret, serverFieldName(parts[0]))
	for _, part := range parts[1:] {
		ret = append(ret, part)
	}
	return ret
}

// FieldPathAffectsSubscription checks whether a changed field path should notify a subscribed field path.
func FieldPathAffectsSubscription(changedField []any, subscribedField []any) bool {
	maxLen := len(changedField)
	if len(subscribedField) < maxLen {
		maxLen = len(subscribedField)
	}
	for idx := 0; idx < maxLen; idx++ {
		if subscriptionKeyPart(changedField[idx]) != subscriptionKeyPart(subscribedField[idx]) {
			return false
		}
	}
	return true
}

func subscriptionKeyPart(part any) string {
	switch v := part.(type) {
	case string:
		return clientFieldName(v)
	default:
		return fmt.Sprint(v)
	}
}

func clientFieldName(name string) string {
	if name == "" || strings.Contains(name, "_") {
		return name
	}
	return strings.ToLower(name[:1]) + name[1:]
}

func serverFieldName(name string) string {
	if name == "" || strings.Contains(name, "_") {
		return name
	}
	return strings.ToUpper(name[:1]) + name[1:]
}

func partialFieldKey[K comparable](raw any) (K, bool) {
	var zero K
	if typed, ok := raw.(K); ok {
		return typed, true
	}

	rawString, ok := raw.(string)
	if !ok {
		rawString = fmt.Sprint(raw)
	}

	keyType := reflect.TypeFor[K]()
	keyValue := reflect.New(keyType).Elem()

	switch keyType.Kind() { //nolint:exhaustive // Only key kinds supported by ReSub field paths are needed here.
	case reflect.String:
		keyValue.SetString(rawString)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		parsed, err := strconv.ParseInt(rawString, 10, keyType.Bits())
		if err != nil {
			return zero, false
		}
		keyValue.SetInt(parsed)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		parsed, err := strconv.ParseUint(rawString, 10, keyType.Bits())
		if err != nil {
			return zero, false
		}
		keyValue.SetUint(parsed)
	default:
		return zero, false
	}

	return keyValue.Interface().(K), true
}

// FieldPathPartToKey converts a field-path value into a map key type.
func FieldPathPartToKey[K comparable](raw any) (K, bool) {
	return partialFieldKey[K](raw)
}

func partialArrayIndex(raw any) (int, bool) {
	if idx, ok := raw.(int); ok {
		return idx, true
	}
	rawString, ok := raw.(string)
	if !ok {
		rawString = fmt.Sprint(raw)
	}
	parsed, err := strconv.Atoi(rawString)
	if err != nil {
		return 0, false
	}
	return parsed, true
}

// FieldPathPartToIndex converts a field-path value into a slice index.
func FieldPathPartToIndex(raw any) (int, bool) {
	return partialArrayIndex(raw)
}
