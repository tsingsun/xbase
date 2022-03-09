package xbase

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode"

	"golang.org/x/text/encoding"
)

const (
	maxFieldNameLen = 10
	maxCFieldLen    = 254
	maxNFieldLen    = 19
)

const (
	defaultLFieldLen = 1
	defaultDFieldLen = 8
)

// https://www.dbase.com/Knowledgebase/INT/db7_file_fmt.htm
// http://www.dbase.com/help/Design_Tables/IDH_TABLEDES_FIELD_TYPES.htm
const (
	FieldType_Character = 'C'
	FieldType_Numeric   = 'N'
	FieldType_Date      = 'D'
	FieldType_Float     = 'F'
	FieldType_Logical   = 'L'
	// not support
	FieldType_Binary        = 'B'
	FieldType_Memo          = 'M'
	FieldType_Timestamp     = '@'
	FieldType_Long          = 'I'
	FieldType_Double        = 'O'
	FieldType_OLE           = 'G'
	FieldType_Autoincrement = '+'
)

type field struct {
	Name   [11]byte
	Type   byte
	Offset uint32
	Len    byte
	Dec    byte
	Filler [14]byte
}

func (f *field) name() string {
	i := bytes.IndexByte(f.Name[:], 0)
	return string(f.Name[:i])
}

// String utils

func padRight(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
}

func padLeft(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return strings.Repeat(" ", width-len(s)) + s
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] > unicode.MaxASCII {
			return false
		}
	}
	return true
}

// NewField return dbf field description
func NewField(name string, typ string, length, dec int) (f *field, err error) {
	f = &field{}
	// do not change the call order
	if err = f.setName(name); err != nil {
		return
	}
	if err = f.setType(typ); err != nil {
		return
	}
	if err = f.setLen(length); err != nil {
		return
	}
	if err = f.setDec(dec); err != nil {
		return
	}
	return f, nil
}

func (f *field) setName(name string) error {
	name = strings.ToUpper(strings.TrimSpace(name))
	if len(name) == 0 {
		return fmt.Errorf("empty field name")
	}
	if len(name) > maxFieldNameLen {
		return fmt.Errorf("too long field name: %q, max len %d", name, maxFieldNameLen)
	}
	copy(f.Name[:], name)
	return nil
}

func (f *field) setType(typ string) error {
	typ = strings.ToUpper(strings.TrimSpace(typ))
	if len(typ) == 0 {
		return fmt.Errorf("empty field type")
	}
	t := typ[0]
	if bytes.IndexByte([]byte("CNLDF"), t) < 0 {
		return fmt.Errorf("invalid field type: got %s, want C, N, L, D", string(t))
	}
	f.Type = t
	return nil
}

func (f *field) setLen(length int) error {
	switch f.Type {
	case FieldType_Character:
		if length <= 0 || length > maxCFieldLen {
			return fmt.Errorf("invalid field len: got %d, want 0 < len <= %d", length, maxCFieldLen)
		}
	case FieldType_Numeric, FieldType_Float:
		if length <= 0 || length > maxNFieldLen {
			return fmt.Errorf("invalid field len: got %d, want 0 < len <= %d", length, maxNFieldLen)
		}
	case FieldType_Logical:
		length = defaultLFieldLen
	case FieldType_Date:
		length = defaultDFieldLen
	}
	f.Len = byte(length)
	return nil
}

func (f *field) setDec(dec int) error {
	if f.Type == FieldType_Numeric || f.Type == FieldType_Float {
		if dec < 0 {
			return fmt.Errorf("invalid field dec: got %d, want dec > 0", dec)
		}
		length := int(f.Len)
		if length <= 2 && dec > 0 {
			return fmt.Errorf("invalid field dec: got %d, want 0", dec)
		}
		if length > 2 && (dec > length-2) {
			return fmt.Errorf("invalid field dec: got %d, want dec <= %d", dec, length-2)
		}
	} else {
		dec = 0
	}
	f.Dec = byte(dec)
	return nil
}

// read field info from io.Reader
func (f *field) read(reader io.Reader) error {
	err := binary.Read(reader, binary.LittleEndian, f)
	return err
}

func (f *field) write(writer io.Writer) error {
	tmp := f.Offset
	f.Offset = 0
	defer func() { f.Offset = tmp }()

	return binary.Write(writer, binary.LittleEndian, f)
}

// Buffer

func (f *field) buffer(recordBuf []byte) []byte {
	return recordBuf[int(f.Offset) : int(f.Offset)+int(f.Len)]
}

func (f *field) setBuffer(recordBuf []byte, value string) {
	copy(recordBuf[int(f.Offset):int(f.Offset)+int(f.Len)], value)
}

// Check

func (f *field) checkType(t byte) error {
	if t != f.Type {
		return fmt.Errorf("type mismatch: got %q, want %q", string(t), string(f.Type))
	}
	return nil
}

func (f *field) checkLen(value string) error {
	if len(value) > int(f.Len) {
		return fmt.Errorf("field value overflow: value len %d, field len %d", len(value), int(f.Len))
	}
	return nil
}

// Get value

func (f *field) stringValue(recordBuf []byte, dec *encoding.Decoder) (string, error) {
	s := string(f.buffer(recordBuf))

	switch f.Type {
	case FieldType_Character:
		s = strings.TrimRight(s, " ")
	case FieldType_Numeric, FieldType_Float:
		s = strings.TrimLeft(s, " ")
	}

	if dec != nil && f.Type == FieldType_Character && !isASCII(s) {
		ds, err := dec.String(s)
		if err != nil {
			return "", err
		}
		s = ds
	}
	return s, nil
}

func (f *field) boolValue(recordBuf []byte) (v bool, err error) {
	if err = f.checkType(FieldType_Logical); err != nil {
		return
	}
	fieldBuf := f.buffer(recordBuf)
	b := fieldBuf[0]
	v = b == 'T' || b == 't' || b == 'Y' || b == 'y'
	return
}

func (f *field) dateValue(recordBuf []byte) (d time.Time, err error) {
	if err = f.checkType(FieldType_Date); err != nil {
		return
	}
	s := string(f.buffer(recordBuf))
	if strings.Trim(s, " ") == "" {
		return
	}
	return time.Parse("20060102", s)
}

func (f *field) intValue(recordBuf []byte) (val int64, err error) {
	if err = f.checkType(FieldType_Numeric); err != nil {
		return
	}
	s := string(f.buffer(recordBuf))
	s = strings.TrimSpace(s)
	if s == "" || s[0] == '.' {
		return
	}
	i := strings.IndexByte(s, '.')
	if i > 0 {
		s = s[0:i]
	}
	return strconv.ParseInt(s, 10, 64)
}

func (f *field) floatValue(recordBuf []byte) (val float64, err error) {
	if err = f.checkType(FieldType_Float); err != nil {
		return
	}
	s := string(f.buffer(recordBuf))
	s = strings.TrimSpace(s)
	if s == "" || s[0] == '.' {
		return
	}
	return strconv.ParseFloat(s, 64)
}

// Set value

func (f *field) setStringValue(recordBuf []byte, value string, enc *encoding.Encoder) (err error) {
	if err = f.checkType(FieldType_Character); err != nil {
		return
	}

	if enc != nil && !isASCII(value) {
		s, err := enc.String(value)
		if err != nil {
			return err
		}
		value = s
	}
	if err = f.checkLen(value); err != nil {
		return
	}
	f.setBuffer(recordBuf, padRight(value, int(f.Len)))
	return
}

func (f *field) setBoolValue(recordBuf []byte, value bool) (err error) {
	if err = f.checkType(FieldType_Logical); err != nil {
		return
	}
	s := "F"
	if value {
		s = "T"
	}
	f.setBuffer(recordBuf, s)
	return nil
}

func (f *field) setDateValue(recordBuf []byte, value time.Time) (err error) {
	if err = f.checkType(FieldType_Date); err != nil {
		return
	}
	f.setBuffer(recordBuf, value.Format("20060102"))
	return
}

func (f *field) setIntValue(recordBuf []byte, value int64) (err error) {
	if err = f.checkType(FieldType_Numeric); err != nil {
		return
	}
	s := strconv.FormatInt(value, 10)
	if f.Dec > 0 {
		s += "." + strings.Repeat("0", int(f.Dec))
	}
	if err = f.checkLen(s); err != nil {
		return
	}
	f.setBuffer(recordBuf, padLeft(s, int(f.Len)))
	return
}

func (f *field) setFloatValue(recordBuf []byte, value float64) (err error) {
	if err = f.checkType(FieldType_Float); err != nil {
		return
	}
	s := strconv.FormatFloat(value, 'f', int(f.Dec), 64)
	if err = f.checkLen(s); err != nil {
		return
	}
	f.setBuffer(recordBuf, padLeft(s, int(f.Len)))
	return
}

func (f *field) setValue(recordBuf []byte, value interface{}, enc *encoding.Encoder) (err error) {
	switch v := value.(type) {
	case string:
		err = f.setStringValue(recordBuf, v, enc)
	case bool:
		err = f.setBoolValue(recordBuf, v)
	case int:
		err = f.setIntValue(recordBuf, int64(v))
	case int8:
		err = f.setIntValue(recordBuf, int64(v))
	case int16:
		err = f.setIntValue(recordBuf, int64(v))
	case int32:
		err = f.setIntValue(recordBuf, int64(v))
	case int64:
		err = f.setIntValue(recordBuf, int64(v))
	case uint:
		err = f.setIntValue(recordBuf, int64(v))
	case uint8:
		err = f.setIntValue(recordBuf, int64(v))
	case uint16:
		err = f.setIntValue(recordBuf, int64(v))
	case uint32:
		err = f.setIntValue(recordBuf, int64(v))
	case uint64:
		err = f.setIntValue(recordBuf, int64(v))
	case float32:
		err = f.setFloatValue(recordBuf, float64(v))
	case float64:
		err = f.setFloatValue(recordBuf, float64(v))
	case time.Time:
		err = f.setDateValue(recordBuf, v)
	default:
		err = fmt.Errorf("unsupport type value")
	}
	return err
}
