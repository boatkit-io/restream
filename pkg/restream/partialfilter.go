package restream

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

const (
	compoundKeyJoinerString      = "%&"
	fieldIDSubscriptionKeyPrefix = "~1"
)

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

// ChildFieldsForFieldID filters full field paths down to the children under a
// stable top-level Restream field ID.
func ChildFieldsForFieldID(fields [][]any, fieldID byte) [][]any {
	ret := [][]any{}
	for _, field := range fields {
		if len(field) == 0 {
			ret = append(ret, []any{})
			continue
		}
		if subscriptionKeyPart(field[0]) != subscriptionKeyPart(fieldID) {
			continue
		}
		ret = append(ret, append([]any{}, field[1:]...))
	}
	return ret
}

// reduceFieldPaths removes redundant child paths when an ancestor path is already present.
func reduceFieldPaths(fields [][]any) [][]any {
	if len(fields) < 2 {
		return fields
	}

	root := &fieldPathReduceNode{}
	for _, field := range fields {
		root.add(field)
	}

	ret := make([][]any, 0, len(fields))
	root.collect(nil, &ret)
	return ret
}

type fieldPathReduceNode struct {
	terminal bool
	children []*fieldPathReduceChild
	childMap map[any]*fieldPathReduceNode
}

type fieldPathReduceChild struct {
	part any
	node *fieldPathReduceNode
}

func (n *fieldPathReduceNode) add(field []any) {
	if n.terminal {
		return
	}
	if len(field) == 0 {
		n.terminal = true
		n.children = nil
		n.childMap = nil
		return
	}

	child := n.child(field[0])
	child.add(field[1:])
}

func (n *fieldPathReduceNode) child(part any) *fieldPathReduceNode {
	if isComparableFieldPathPart(part) {
		if n.childMap != nil {
			if child := n.childMap[part]; child != nil {
				return child
			}
		} else {
			n.childMap = map[any]*fieldPathReduceNode{}
			for _, child := range n.children {
				if isComparableFieldPathPart(child.part) {
					n.childMap[child.part] = child.node
				}
			}
		}

		child := &fieldPathReduceNode{}
		n.childMap[part] = child
		n.children = append(n.children, &fieldPathReduceChild{part: part, node: child})
		return child
	}

	for _, child := range n.children {
		if reflect.DeepEqual(child.part, part) {
			return child.node
		}
	}

	child := &fieldPathReduceNode{}
	n.children = append(n.children, &fieldPathReduceChild{part: part, node: child})
	return child
}

func isComparableFieldPathPart(part any) bool {
	if part == nil {
		return true
	}
	return reflect.TypeOf(part).Comparable()
}

func (n *fieldPathReduceNode) collect(prefix []any, ret *[][]any) {
	if n.terminal {
		field := append([]any{}, prefix...)
		*ret = append(*ret, field)
		return
	}

	for _, child := range n.children {
		child.node.collect(append(prefix, child.part), ret)
	}
}

// SubscriptionKeyFromFieldPath converts a server-side Go partial field path into the matching client ReSub key.
func SubscriptionKeyFromFieldPath(field []any) string {
	parts := make([]string, 0, len(field))
	for _, part := range field {
		parts = append(parts, subscriptionKeyPart(part))
	}
	return strings.Join(parts, compoundKeyJoinerString)
}

// SubscriptionKeyFromFieldIDPath converts a field path whose struct segments
// are stable Restream field IDs into the versioned client subscription-key
// representation. Collection keys and indexes remain literal path segments.
func SubscriptionKeyFromFieldIDPath(field []any) string {
	if len(field) == 0 {
		return ""
	}
	parts := make([]string, 0, len(field)+1)
	parts = append(parts, fieldIDSubscriptionKeyPrefix)
	for _, part := range field {
		parts = append(parts, fmt.Sprint(part))
	}
	return strings.Join(parts, compoundKeyJoinerString)
}

func normalizeFieldIDSubscriptionKey(key string, stateType reflect.Type) (string, error) {
	parts := SplitSubscriptionKey(key)
	if len(parts) == 0 {
		return "", nil
	}
	if parts[0] != fieldIDSubscriptionKeyPrefix {
		return "", fmt.Errorf("subscription key must use the %s field-ID format", fieldIDSubscriptionKeyPrefix)
	}
	if len(parts) == 1 {
		return "", fmt.Errorf("field-ID subscription key has no field path")
	}

	keyParts, err := validateFieldIDSubscriptionKeyParts(stateType, parts[1:])
	if err != nil {
		return "", err
	}
	return strings.Join(append([]string{fieldIDSubscriptionKeyPrefix}, keyParts...), compoundKeyJoinerString), nil
}

func validateFieldIDSubscriptionKeyParts(stateType reflect.Type, parts []string) ([]string, error) {
	ret := make([]string, 0, len(parts))
	currentType := stateType
	for idx, part := range parts {
		for currentType.Kind() == reflect.Pointer {
			currentType = currentType.Elem()
		}

		switch currentType.Kind() { //nolint:exhaustive // Subscription paths support structs and their collections.
		case reflect.Struct:
			fieldID, err := strconv.ParseUint(part, 10, 8)
			if err != nil || fieldID == 0 {
				return nil, fmt.Errorf(
					"subscription field path segment %q is not a valid field ID for %s",
					part,
					currentType,
				)
			}
			field, ok := structFieldForRestreamID(currentType, byte(fieldID))
			if !ok {
				return nil, fmt.Errorf(
					"subscription field ID %d does not exist on %s",
					fieldID,
					currentType,
				)
			}
			ret = append(ret, strconv.FormatUint(fieldID, 10))
			currentType = field.Type
		case reflect.Map:
			if _, ok := fieldPathPartToReflectValue(part, currentType.Key()); !ok {
				return nil, fmt.Errorf(
					"subscription map path segment %q is not a valid key for %s",
					part,
					currentType,
				)
			}
			ret = append(ret, part)
			currentType = currentType.Elem()
		case reflect.Array, reflect.Slice:
			if _, err := strconv.Atoi(part); err != nil {
				return nil, fmt.Errorf(
					"subscription array path segment %q is not an index for %s",
					part,
					currentType,
				)
			}
			ret = append(ret, part)
			currentType = currentType.Elem()
		default:
			return nil, fmt.Errorf(
				"subscription field path continues through non-container %s at segment %d",
				currentType,
				idx,
			)
		}
	}
	return ret, nil
}

func structFieldForRestreamID(structType reflect.Type, fieldID byte) (reflect.StructField, bool) {
	for idx := 0; idx < structType.NumField(); idx++ {
		field := structType.Field(idx)
		for _, option := range strings.Split(field.Tag.Get("restream"), ",") {
			value, found := strings.CutPrefix(option, "fID=")
			if !found {
				continue
			}
			parsed, err := strconv.ParseUint(value, 10, 8)
			if err == nil && byte(parsed) == fieldID {
				return field, true
			}
		}
	}
	return reflect.StructField{}, false
}

func restreamFieldIDFromPathPart(part any) (byte, bool) {
	value, err := strconv.ParseUint(fmt.Sprint(part), 10, 8)
	if err != nil || value == 0 {
		return 0, false
	}
	return byte(value), true
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
	if len(parts) < 2 || parts[0] != fieldIDSubscriptionKeyPrefix {
		return nil
	}

	ret := make([]any, 0, len(parts)-1)
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
	return fmt.Sprint(part)
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
