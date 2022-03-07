package xbase

// Reader provides the interface for reading a single DBF record.
//
// If there is no data left to be read, Read returns (nil, io.EOF).
//
// It is implemented by csv.Reader.
type Reader interface {
	Header() ([]string, error)
	ReadLine() ([]string, error)
}

// Writer provides the interface for writing a single DBF record.
//
// It is implemented by csv.Writer.
type Writer interface {
	Write([]string) error
}

// Unmarshaler is the interface implemented by types that can unmarshal
// a single record's field description of themselves.
type Unmarshaler interface {
	UnmarshalDBF([]byte) error
}

// Marshaler is the interface implemented by types that can marshal themselves
// into valid string.
type Marshaler interface {
	MarshalDBF() ([]byte, error)
}
