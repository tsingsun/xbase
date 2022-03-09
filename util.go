package xbase

import (
	"reflect"
)

var (
	_inferface = reflect.TypeOf((interface{})(nil))
	_error     = reflect.TypeOf((*error)(nil)).Elem()
)

func valueType(v interface{}) (reflect.Type, error) {
	val := reflect.ValueOf(v)
	if !val.IsValid() {
		return nil, &UnsupportedTypeError{}
	}

loop:
	for {
		switch val.Kind() {
		case reflect.Ptr, reflect.Interface:
			el := val.Elem()
			if !el.IsValid() {
				break loop
			}
			val = el
		default:
			break loop
		}
	}

	typ := walkType(val.Type())
	if typ.Kind() != reflect.Struct {
		return nil, &UnsupportedTypeError{Type: typ}
	}
	return typ, nil
}

func isNilFixed(i interface{}) bool {
	if i == nil {
		return true
	}
	switch reflect.TypeOf(i).Kind() {
	case reflect.Ptr, reflect.Map, reflect.Array, reflect.Chan, reflect.Slice:
		return reflect.ValueOf(i).IsNil()
	}
	return false
}
