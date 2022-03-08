package xbase

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strconv"
)

// ErrFieldCount is returned when header's length doesn't match the length of
// the read record.
var ErrFieldCount = errors.New("wrong number of fields in record")

var BOF = errors.New("BOF")

// An UnmarshalTypeError describes a string value that was not appropriate for
// a value of a specific Go type.
type UnmarshalTypeError struct {
	Value string       // string value
	Type  reflect.Type // type of Go value it could not be assigned to
}

func (e *UnmarshalTypeError) Error() string {
	return "xbase: cannot unmarshal " + strconv.Quote(e.Value) + " into Go value of type " + e.Type.String()
}

// An UnsupportedTypeError is returned when attempting to encode or decode
// a value of an unsupported type.
type UnsupportedTypeError struct {
	Type reflect.Type
}

func (e *UnsupportedTypeError) Error() string {
	if e.Type == nil {
		return "csvutil: unsupported type: nil"
	}
	return "xbase: unsupported type: " + e.Type.String()
}

// An InvalidDecodeError describes an invalid argument passed to Decode.
// (The argument to Decode must be a non-nil struct pointer)
type InvalidDecodeError struct {
	Type reflect.Type
}

func (e *InvalidDecodeError) Error() string {
	if e.Type == nil {
		return "xbase: Decode(nil)"
	}

	if e.Type.Kind() != reflect.Ptr {
		return "xbase: Decode(non-pointer " + e.Type.String() + ")"
	}

	typ := walkType(e.Type)
	switch typ.Kind() {
	case reflect.Struct:
	case reflect.Slice, reflect.Array:
		if typ.Elem().Kind() != reflect.Struct {
			return "xbase: Decode(invalid type " + e.Type.String() + ")"
		}
	default:
		return "xbase: Decode(invalid type " + e.Type.String() + ")"
	}

	return "xbase: Decode(nil " + e.Type.String() + ")"
}

// An InvalidUnmarshalError describes an invalid argument passed to Unmarshal.
// (The argument to Unmarshal must be a non-nil slice of structs pointer)
type InvalidUnmarshalError struct {
	Type reflect.Type
}

func (e *InvalidUnmarshalError) Error() string {
	if e.Type == nil {
		return "xbase: Unmarshal(nil)"
	}

	if e.Type.Kind() != reflect.Ptr {
		return "xbase: Unmarshal(non-pointer " + e.Type.String() + ")"
	}

	return "xbase: Unmarshal(invalid type " + e.Type.String() + ")"
}

// InvalidEncodeError is returned by Encode when the provided value was invalid.
type InvalidEncodeError struct {
	Type reflect.Type
}

func (e *InvalidEncodeError) Error() string {
	if e.Type == nil {
		return "xbase: Encode(nil)"
	}
	return "xbase: Encode(" + e.Type.String() + ")"
}

// InvalidMarshalError is returned by Marshal when the provided value was invalid.
type InvalidMarshalError struct {
	Type reflect.Type
}

func (e *InvalidMarshalError) Error() string {
	if e.Type == nil {
		return "xbase: Marshal(nil)"
	}

	if walkType(e.Type).Kind() == reflect.Slice {
		return "xbase: Marshal(non struct slice " + e.Type.String() + ")"
	}

	if walkType(e.Type).Kind() == reflect.Array {
		return "xbase: Marshal(non struct array " + e.Type.String() + ")"
	}

	return "xbase: Marshal(invalid type " + e.Type.String() + ")"
}

// MarshalerError is returned by Encoder when MarshalCSV or MarshalText returned
// an error.
type MarshalerError struct {
	Type          reflect.Type
	MarshalerType string
	Err           error
}

func (e *MarshalerError) Error() string {
	return "xbase: error calling " + e.MarshalerType + " for type " + e.Type.String() + ": " + e.Err.Error()
}

// Unwrap implements Unwrap interface for errors package in Go1.13+.
func (e *MarshalerError) Unwrap() error {
	return e.Err
}

func errPtrUnexportedStruct(typ reflect.Type) error {
	return fmt.Errorf("xbase: cannot decode into a pointer to unexported struct: %s", typ)
}

// MissingColumnsError is returned by Decoder only when DisallowMissingColumns
// option was set to true. It contains a list of all missing columns.
type MissingColumnsError struct {
	Columns []string
}

func (e *MissingColumnsError) Error() string {
	var b bytes.Buffer
	b.WriteString("xbase: missing columns: ")
	for i, c := range e.Columns {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%q", c)
	}
	return b.String()
}

// decodeError provides context to decoding errors if available.
//
// The caller should use errors.As in order to fetch the underlying error if
// needed.
type decodeError struct {
	Field  string
	Line   int
	Column int
	Err    error
}

func (e *decodeError) Error() string {
	if e.Line > 0 && e.Column > 0 {
		// Lines and Columns are 1-indexed so this check is fine.
		return fmt.Sprintf("%s: field %q line %d column %d", e.Err, e.Field, e.Line, e.Column)
	}
	return fmt.Sprintf("%s: field %q", e.Err, e.Field)
}

func (e *decodeError) Unwrap() error {
	return e.Err
}
