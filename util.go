package xbase

import (
	"bytes"
	"reflect"
)

const defaultTag = "dbf"

var (
	_bytes = reflect.TypeOf(([]byte)(nil))
	_error = reflect.TypeOf((*error)(nil)).Elem()
)

// Marshal returns the CSV encoding of slice or array v. If v is not a slice or
// elements are not structs then Marshal returns InvalidMarshalError.
//
// Marshal uses the std encoding/csv.Writer with its default settings for csv
// encoding.
//
// Marshal will always encode the CSV header even for the empty slice.
//
// For the exact encoding rules look at Encoder.Encode method.
func Marshal(v interface{}) ([]byte, error) {
	val := walkValue(reflect.ValueOf(v))

	if !val.IsValid() {
		return nil, &InvalidMarshalError{}
	}

	switch val.Kind() {
	case reflect.Array, reflect.Slice:
	default:
		return nil, &InvalidMarshalError{Type: reflect.ValueOf(v).Type()}
	}

	typ := walkType(val.Type().Elem())
	if typ.Kind() != reflect.Struct {
		return nil, &InvalidMarshalError{Type: reflect.ValueOf(v).Type()}
	}

	var buf bytes.Buffer
	w := New(nil)
	enc := NewEncoder(w)

	if err := enc.encodeHeader(typ); err != nil {
		return nil, err
	}

	if err := enc.encodeArray(val); err != nil {
		return nil, err
	}

	w.Flush()
	if err := w.Error(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func countRecords(s []byte) (n int) {
	var prev byte
	inQuote := false
	for {
		if len(s) == 0 && prev != '"' {
			return n
		}

		i := bytes.IndexAny(s, "\n\"")
		if i == -1 {
			return n + 1
		}

		switch s[i] {
		case '\n':
			if !inQuote && (i > 0 || prev == '"') {
				n++
			}
		case '"':
			inQuote = !inQuote
		}

		prev = s[i]
		s = s[i+1:]
	}
}

// Header scans the provided struct type and generates a DBF header for it.
//
// Field names are written in the same order as struct fields are defined.
// Embedded struct's fields are treated as if they were part of the outer struct.
// Fields that are embedded types and that are tagged are treated like any
// other field.
//
// Unexported fields and fields with tag "-" are ignored.
//
// Tagged fields have the priority over non tagged fields with the same name.
//
// Following the Go visibility rules if there are multiple fields with the same
// name (tagged or not tagged) on the same level and choice between them is
// ambiguous, then all these fields will be ignored.
//
// It is a good practice to call Header once for each type. The suitable place
// for calling it is init function. Look at Decoder.DecodingDataWithNoHeader
// example.
//
// If tag is left empty the default "dbf" will be used.
//
// Header will return UnsupportedTypeError if the provided value is nil or is
// not a struct.
func Header(v interface{}, tag string) ([]string, error) {
	typ, err := valueType(v)
	if err != nil {
		return nil, err
	}

	if tag == "" {
		tag = defaultTag
	}

	fields := cachedFields(typeKey{tag, typ})
	h := make([]string, len(fields))
	for i, f := range fields {
		h[i] = f.name
	}
	return h, nil
}

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
