package xbase

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHeader(t *testing.T) {
	type args struct {
		v   interface{}
		tag string
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "flex", args: args{
				v: struct {
					String string  `dbf:"string,omitempty"`
					Int    int     `dbf:"int,inline"`
					Float  float64 `dbf:"float,type:F,length:16,decimal:4"`
				}{},
				tag: "dbf",
			}, want: []string{"string", "int", "float"}, wantErr: func(t assert.TestingT, err error, i ...interface{}) bool { return false },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Header(tt.args.v, tt.args.tag)
			if !tt.wantErr(t, err, fmt.Sprintf("Header(%v, %v)", tt.args.v, tt.args.tag)) {
				return
			}
			assert.Equalf(t, tt.want, got, "Header(%v, %v)", tt.args.v, tt.args.tag)
		})
	}
}
