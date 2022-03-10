package xbase

import (
	"github.com/stretchr/testify/assert"
	"io"
	"os"
	"testing"
	"time"
)

type NativeType struct {
	Int     int     `dbf:"int,len:1"`
	Int8    int8    `dbf:"int8,len:2"`
	Int16   int16   `dbf:"int16,len:4"`
	Int32   int32   `dbf:"int32,len:8"`
	Int64   int64   `dbf:"int64,len:16"`
	UInt    uint    `dbf:"uint,len:1"`
	Uint8   uint8   `dbf:"uint8,len:2"`
	Uint16  uint16  `dbf:"uint16,len:4"`
	Uint32  uint32  `dbf:"uint32,len:8"`
	Uint64  uint64  `dbf:"uint64,type:N,len:16"`
	Float32 float32 `dbf:"float32,len:10,dec:2"`
	Float64 float64 `dbf:"float64,type:F,len:16,dec:4"`
	String  string  `dbf:"string,len:10"`
	Bool    bool    `dbf:"bool"`
}

func TestNewEncoder(t *testing.T) {
	type args struct {
		w  Writer
		iw io.ReadWriteSeeker
		in []interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "NativeType-buffer", args: args{
				iw: NewSeekableBuffer(),
				in: []interface{}{
					nil,
					NativeType{Int: 1, Int8: 8, Int16: 16, Int32: 32, Int64: 64, UInt: 2, Uint8: 28, Uint16: 216, Uint32: 232, Uint64: 264, Float32: 32.32, Float64: 64.6464, String: "abcf", Bool: true},
				},
			}, wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				for _, i3 := range i {
					if isNilFixed(i3) {
						return true
					}
				}
				return false
			},
		},
		{
			name: "NativeType-file", args: args{
				iw: func() io.ReadWriteSeeker {
					os.Remove("./testdata/test3.dbf")
					fs, err := os.Create("./testdata/test3.dbf")
					assert.NoError(t, err)
					return fs
				}(),
				in: []interface{}{
					NativeType{Int: 1, Int8: 8, Int16: 16, Int32: 32, Int64: 64, UInt: 2, Uint8: 28, Uint16: 216, Uint32: 232, Uint64: 264, Float32: 32.32, Float64: 64.6464, String: "abcf", Bool: true},
				},
			}, wantErr: func(t assert.TestingT, err error, i ...interface{}) bool { return false },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			xb, err := New(tt.args.iw)
			assert.NoError(t, err)
			enc := NewEncoder(xb)
			for _, i := range tt.args.in {
				err := enc.Encode(i)
				if tt.wantErr(t, err, i) {
					assert.Error(t, err)
				} else {
					assert.NoError(t, err)
				}
			}
			xb.Close()
		})
	}
}

func TestNewEncoderRec(t *testing.T) {
	type args struct {
		w  Writer
		iw io.ReadWriteSeeker
		in []*Rec
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "rec3-file", args: args{
				iw: func() io.ReadWriteSeeker {
					os.Remove("./testdata/test-rec3.dbf")
					fs, err := os.Create("./testdata/test-rec3.dbf")
					assert.NoError(t, err)
					return fs
				}(),
				in: []*Rec{
					&Rec{Name: "Abc", Flag: true, Count: 123, Price: 123.45, Date: time.Date(2021, 2, 12, 0, 0, 0, 0, time.UTC)},
					nil,
					&Rec{Name: "Мышь", Flag: false, Count: -321, Price: -54.32, Date: time.Date(2021, 2, 12, 0, 0, 0, 0, time.UTC)},
				},
			}, wantErr: func(t assert.TestingT, err error, i ...interface{}) bool { return false },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			xb, err := New(tt.args.iw)
			assert.NoError(t, err)
			xb.SetCodePage(866)
			enc := NewEncoder(xb)
			err = enc.Encode(tt.args.in)
			if tt.wantErr(t, err, tt.args.in) {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			//xb.Flush()
			xb.Close()
			if tt.name == "rec3-file" {
				wantBytes := readFile("./testdata/rec3.dbf")
				gotBytes := readFile("./testdata/test-rec3.dbf")
				assert.Equal(t, wantBytes, gotBytes)
			}
		})
	}
}
