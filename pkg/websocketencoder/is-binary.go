package websocketencoder

import (
	"io"
	"reflect"
	"strings"

	"github.com/zishang520/socket.io/v3/pkg/types"
)

// IsBinary Returns true if obj is a Buffer or a File.
func IsBinary(data any) bool {
	switch data.(type) {
	case *types.StringBuffer: // false
	case *strings.Reader: // false
	case []byte:
		return true
	case io.Reader:
		return true
	}
	return false
}

// HasBinary  is a func
func HasBinary(data any) bool {
	switch o := data.(type) {
	case nil:
		return false
	case []any:
		for _, v := range o {
			if HasBinary(v) {
				return true
			}
		}
		return false
	case map[string]any:
		for _, v := range o {
			if HasBinary(v) {
				return true
			}
		}
		return false
	}

	if IsBinary(data) {
		return true
	}

	dv := reflect.ValueOf(data)
	for dv.Kind() == reflect.Pointer || dv.Kind() == reflect.Interface {
		if dv.IsNil() {
			return false
		}
		dv = dv.Elem()
	}

	switch dv.Kind() {
	case reflect.Struct:
		for fi := range dv.NumField() {
			dfv := dv.Field(fi)
			if dfv.CanInterface() && HasBinary(dfv.Interface()) {
				return true
			}
		}
		return false
	case reflect.Array, reflect.Slice:
		for i := range dv.Len() {
			av := dv.Index(i)
			if av.CanInterface() && HasBinary(av.Interface()) {
				return true
			}
		}
		return false
	case reflect.Map:
		mr := dv.MapRange()
		for mr.Next() {
			mv := mr.Value()
			if mv.CanInterface() && HasBinary(mv.Interface()) {
				return true
			}
		}
		return false
	}

	return false
}
