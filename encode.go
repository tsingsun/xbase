package xbase

import (
	"encoding"
	"encoding/base64"
	"reflect"
)

var (
	textMarshaler = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()
	dbfMarshaler  = reflect.TypeOf((*Marshaler)(nil)).Elem()
)

type encodeFunc func(v reflect.Value, omitempty bool) (interface{}, error)

func nopEncode(v reflect.Value, omitempty bool) (interface{}, error) {
	if !v.IsValid() {
		return nil, nil
	}
	return v.Interface(), nil
}

func encodeFuncValue(fn reflect.Value) encodeFunc {
	return func(v reflect.Value, omitempty bool) (interface{}, error) {
		out := fn.Call([]reflect.Value{v})
		err, _ := out[1].Interface().(error)
		if err != nil {
			return nil, err
		}
		return out, nil
	}
}

func encodeFuncValuePtr(fn reflect.Value) encodeFunc {
	return func(v reflect.Value, omitempty bool) (interface{}, error) {
		if !v.CanAddr() {
			fallback, err := encodeFn(v.Type(), false, nil, nil)
			if err != nil {
				return nil, err
			}
			return fallback(v, omitempty)
		}

		out := fn.Call([]reflect.Value{v.Addr()})
		err, _ := out[1].Interface().(error)
		if err != nil {
			return nil, err
		}
		return out, nil
	}
}

func encodeInterface(funcMap map[reflect.Type]reflect.Value, funcs []reflect.Value) encodeFunc {
	return func(v reflect.Value, omitempty bool) (interface{}, error) {
		if !v.IsValid() || v.IsNil() || !v.Elem().IsValid() {
			return nil, nil
		}

		v = v.Elem()
		canAddr := v.Kind() == reflect.Ptr

		switch v.Kind() {
		case reflect.Ptr, reflect.Interface:
			if v.IsNil() {
				return nil, nil
			}
		default:
		}

		enc, err := encodeFn(v.Type(), canAddr, funcMap, funcs)
		if err != nil {
			return nil, err
		}
		return enc(v, omitempty)
	}
}

func encodePtrMarshaler(v reflect.Value, omitempty bool) (interface{}, error) {
	if v.CanAddr() {
		return encodeMarshaler(v.Addr(), omitempty)
	}

	fallback, err := encodeFn(v.Type(), false, nil, nil)
	if err != nil {
		return nil, err
	}
	return fallback(v, omitempty)
}

func encodeTextMarshaler(v reflect.Value, _ bool) (interface{}, error) {
	if v.Kind() == reflect.Ptr && v.IsNil() {
		return nil, nil
	}

	b, err := v.Interface().(encoding.TextMarshaler).MarshalText()
	if err != nil {
		return nil, &MarshalerError{Type: v.Type(), MarshalerType: "MarshalText", Err: err}
	}
	return b, nil
}

func encodePtrTextMarshaler(v reflect.Value, omitempty bool) (interface{}, error) {
	if v.CanAddr() {
		return encodeTextMarshaler(v.Addr(), omitempty)
	}

	fallback, err := encodeFn(v.Type(), false, nil, nil)
	if err != nil {
		return nil, err
	}
	return fallback(v, omitempty)
}

func encodeMarshaler(v reflect.Value, _ bool) (interface{}, error) {
	if v.Kind() == reflect.Ptr && v.IsNil() {
		return nil, nil
	}

	b, err := v.Interface().(Marshaler).MarshalDBF()
	if err != nil {
		return nil, &MarshalerError{Type: v.Type(), MarshalerType: "MarshalCSV", Err: err}
	}
	return b, nil
}

func encodePtr(typ reflect.Type, canAddr bool, funcMap map[reflect.Type]reflect.Value, funcs []reflect.Value) (encodeFunc, error) {
	next, err := encodeFn(typ.Elem(), canAddr, funcMap, funcs)
	if err != nil {
		return nil, err
	}
	return func(v reflect.Value, omitempty bool) (interface{}, error) {
		if v.IsNil() {
			return nil, nil
		}
		return next(v.Elem(), omitempty)
	}, nil
}

func encodeBytes(v reflect.Value, _ bool) (interface{}, error) {
	data := v.Bytes()
	buf := make([]byte, base64.StdEncoding.EncodedLen(len(data)))
	base64.StdEncoding.Encode(buf, data)
	return buf, nil
}

func encodeFn(typ reflect.Type, canAddr bool, funcMap map[reflect.Type]reflect.Value, funcs []reflect.Value) (encodeFunc, error) {
	if v, ok := funcMap[typ]; ok {
		return encodeFuncValue(v), nil
	}

	if v, ok := funcMap[reflect.PtrTo(typ)]; ok && canAddr {
		return encodeFuncValuePtr(v), nil
	}

	for _, v := range funcs {
		argType := v.Type().In(0)
		if typ.AssignableTo(argType) {
			return encodeFuncValue(v), nil
		}

		if canAddr && reflect.PtrTo(typ).AssignableTo(argType) {
			return encodeFuncValuePtr(v), nil
		}
	}

	if typ.Implements(dbfMarshaler) {
		return encodeMarshaler, nil
	}

	if canAddr && reflect.PtrTo(typ).Implements(dbfMarshaler) {
		return encodePtrMarshaler, nil
	}

	//time
	if typ.String() == "time.Time" {
		return nopEncode, nil
	}

	if typ.Implements(textMarshaler) {
		return encodeTextMarshaler, nil
	}

	if canAddr && reflect.PtrTo(typ).Implements(textMarshaler) {
		return encodePtrTextMarshaler, nil
	}

	switch typ.Kind() {
	case reflect.String:
		return nopEncode, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return nopEncode, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return nopEncode, nil
	case reflect.Float32:
		return nopEncode, nil
	case reflect.Float64:
		return nopEncode, nil
	case reflect.Bool:
		return nopEncode, nil
	case reflect.Interface:
		return encodeInterface(funcMap, funcs), nil
	case reflect.Ptr:
		return encodePtr(typ, canAddr, funcMap, funcs)
	case reflect.Slice:
		if typ.Elem().Kind() == reflect.Uint8 {
			return encodeBytes, nil
		}
	}

	return nil, &UnsupportedTypeError{Type: typ}
}
