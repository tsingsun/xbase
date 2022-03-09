package xbase

import (
	"reflect"
	"strconv"
	"strings"
)

const defaultTag = "dbf"

type tag struct {
	name      string
	prefix    string
	empty     bool // not support
	omitEmpty bool // not support
	ignore    bool
	inline    bool
	dbfType   string
	length    int //field length
	decimal   int //decimal count
}

func parseTag(tagname string, field reflect.StructField) (t tag) {
	tags := strings.Split(field.Tag.Get(tagname), ",")
	if len(tags) == 1 && tags[0] == "" {
		t.name = field.Name
		t.empty = true
		return
	}

	switch tags[0] {
	case "-":
		t.ignore = true
		return
	case "":
		t.name = field.Name
	default:
		t.name = tags[0]
	}
	for _, tagOpt := range tags[1:] {
		opts := strings.Split(tagOpt, ":")
		switch opts[0] {
		case "omitempty":
			t.omitEmpty = true
		case "inline":
			if walkType(field.Type).Kind() == reflect.Struct {
				t.inline = true
				t.prefix = tags[0]
			}
		case "len":
			t.length, _ = strconv.Atoi(opts[1])
		case "dec":
			t.decimal, _ = strconv.Atoi(opts[1])
		case "type":
			//only 1 byte
			t.dbfType = string(opts[1][0])
		}
	}
	if t.dbfType == "" {
		switch field.Type.Kind() {
		case reflect.String:
			t.dbfType = string(FieldType_Character)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			t.dbfType = string(FieldType_Numeric)
		case reflect.Float32, reflect.Float64:
			t.dbfType = string(FieldType_Float)
		case reflect.Bool:
			t.dbfType = string(FieldType_Logical)
		}
	}
	return
}
