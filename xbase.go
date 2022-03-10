package xbase

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
	"time"

	"golang.org/x/text/encoding"
)

const (
	dbfId     byte = 0x03
	headerEnd byte = 0x0D
	fileEnd   byte = 0x1A
)

const (
	fieldSize  = 32
	headerSize = 32
)

// XBase is an util for DBF file.
type XBase struct {
	header *header
	fields []*field
	rws    io.ReadWriteSeeker
	// buffer is the current record buffer .
	buffer []byte
	// err is convenience to set field value ,you should check XBase.Error() at last
	err error
	// recordNum is the line number in raw data. 0: zero record;>=1: the position
	recordNum int64
	// isAdd indicate Add() called
	isAdd bool
	//isMod indicate raw data is modified or no
	isMod   bool
	encoder *encoding.Encoder
	decoder *encoding.Decoder
	// 0: noop; 1: head; 2: field; 3:record
	readStep int
	// 0: noop; 1: head; 2: field; 3:record
	writeStep int

	marshal   *Encoder
	unmarshal *Decoder
}

// New creates a XBase object to work with a DBF file and an error if any.
func New(seeker io.ReadWriteSeeker) (*XBase, error) {
	db := XBase{
		header: newHeader(),
		rws:    seeker,
	}
	if db.rws != nil {
		// may be empty
		err := db.prepareReader()
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
	}
	return &db, nil
}

func (db *XBase) prepareReader() (err error) {
	if err = db.header.read(db.rws); err != nil {
		return
	}

	if err = db.readFields(db.rws); err != nil {
		return
	}
	db.makeBuf()
	db.SetCodePage(db.CodePage())
	return
}

// CreateFile creates a new file in DBF format.
// If a file with that name exists, it will be overwritten.
func (db *XBase) CreateFile(name string) (err error) {
	if err = db.checkFields(); err != nil {
		return
	}
	if db.rws, err = os.Create(name); err != nil {
		return
	}
	if err = db.writeHeader(); err != nil {
		return
	}
	if err = db.writeFields(); err != nil {
		return
	}
	db.makeBuf()
	db.isMod = true
	return
}

// Open opens an existing DBF file.
func Open(name string, readOnly bool) (db *XBase, err error) {
	var f *os.File
	if readOnly {
		f, err = os.Open(name)
	} else {
		f, err = os.OpenFile(name, os.O_RDWR, 0666)
	}
	db, err = New(f)
	if err != nil {
		return
	}
	return db, nil
}

// Flush commit changes to file
func (db *XBase) Flush() (err error) {
	if db.isMod {
		db.header.setModDate(time.Now())
		if err = db.writeHeader(); err != nil {
			return
		}
		if err = db.writeFileEnd(); err != nil {
			return
		}
		db.isMod = false
	}
	return
}

// Close closes a previously opened or created DBF file.
func (db *XBase) Close() error {
	if err := db.Flush(); err != nil {
		return err
	}

	if ioc, ok := db.rws.(io.Closer); ok {
		return ioc.Close()
	}
	return nil
}

// First positions the object to the first record.
func (db *XBase) First() error {
	return db.GoTo(1)
}

// Last positions the object to the last record.
func (db *XBase) Last() error {
	return db.GoTo(db.recCount())
}

// Next positions the object to the next record.
func (db *XBase) Next() error {
	return db.GoTo(db.recordNum + 1)
}

// Prev positions the object to the previous record.
func (db *XBase) Prev() error {
	return db.GoTo(db.recordNum - 1)
}

// RecNo returns the sequence number of the current record.
// Numbering starts from 1.
func (db *XBase) RecNo() int64 {
	return db.recordNum
}

// EOF returns true if end of file is reached.
func (db *XBase) EOF() bool {
	return db.recordNum > db.recCount() || db.recCount() == 0
}

// BOF returns true if the beginning of the file is reached.
func (db *XBase) BOF() bool {
	return db.recordNum == 0 || db.recCount() == 0
}

func (db *XBase) Fields() []string {
	var hl []string
	for _, f := range db.fields {
		hl = append(hl, f.name())
	}
	return hl
}

// Read() implement Reader
func (db *XBase) Read() (val []string, err error) {
	if db.recordNum != 0 {
		// if has move record ptr
		db.readStep = 2
	}
	switch db.readStep {
	case 0:
		//跳过header
		val = db.Fields()
		db.readStep = 2
	case 2:
		val, err = db.readRecord()
		return
	}
	return
}

// readRecord returns buffer string value
func (db *XBase) readRecord() (val []string, err error) {
	if db.err != nil {
		return nil, db.err
	}
	var buffer = make([]byte, len(db.buffer))
	copy(buffer, db.buffer)
	for _, f := range db.fields {
		s := strings.TrimSpace(string(f.buffer(buffer)))
		val = append(val, s)
	}
	err = db.Next()
	return
}

// DecodeRecord decode current row to a struct
func (db *XBase) DecodeRecord(dst interface{}) (err error) {
	if db.unmarshal == nil {
		db.unmarshal, err = NewDecoder(db, db.Fields()...)
		if err != nil {
			return
		}
	}
	return db.unmarshal.Decode(dst)
}

func (db *XBase) Write(input []interface{}) (err error) {
	if len(db.fields) != 0 {
		// has load field
		db.writeStep = 2
	}
	switch db.writeStep {
	case 0:
		count := input[0].(int)
		if err != nil {
			return err
		}
		db.header.setFieldCount(count)
		rd := bytes.NewBuffer(input[1].([]byte))
		if err = db.readFields(rd); err != nil {
			return err
		}
		if err = db.writeHeader(); err != nil {
			return err
		}
		if err = db.writeFields(); err != nil {
			return err
		}
		db.makeBuf()
		db.writeStep = 2
	case 2:
		if err := db.Add(); err != nil {
			return err
		}
		for i, value := range input {
			if value == nil {
				//if value is nil in add
				continue
			}
			if err = db.fields[i].setValue(db.buffer, value, db.encoder); err != nil {
				return err
			}
		}
		db.err = db.Save()
		if db.err != nil {
			return db.err
		}

		return db.Flush()
	}
	return nil
}

// FieldValueAsString returns the string value of the field of the current record.
// Fields are numbered starting from 1.
func (db *XBase) FieldValueAsString(fieldNo int) (val string) {
	if db.err != nil {
		return
	}
	defer db.wrapFieldError("FieldValueAsString", fieldNo)
	var err error
	if val, err = db.fieldByNo(fieldNo).stringValue(db.buffer, db.decoder); err != nil {
		panic(err)
	}
	return
}

// FieldValueAsInt returns the integer value of the field of the current record.
// Field type must be numeric ("N"). Fields are numbered starting from 1.
func (db *XBase) FieldValueAsInt(fieldNo int) (val int64) {
	if db.err != nil {
		return
	}
	defer db.wrapFieldError("FieldValueAsInt", fieldNo)
	var err error
	if val, err = db.fieldByNo(fieldNo).intValue(db.buffer); err != nil {
		panic(err)
	}
	return
}

// FieldValueAsFloat returns the float value of the field of the current record.
// Field type must be numeric ("N"). Fields are numbered starting from 1.
func (db *XBase) FieldValueAsFloat(fieldNo int) (val float64) {
	if db.err != nil {
		return
	}
	defer db.wrapFieldError("FieldValueAsFloat", fieldNo)
	var err error
	if val, err = db.fieldByNo(fieldNo).floatValue(db.buffer); err != nil {
		panic(err)
	}
	return
}

// FieldValueAsBool returns the boolean value of the field of the current record.
// Field type must be logical ("L"). Fields are numbered starting from 1.
func (db *XBase) FieldValueAsBool(fieldNo int) (val bool) {
	if db.err != nil {
		return
	}
	defer db.wrapFieldError("FieldValueAsBool", fieldNo)
	var err error
	if val, err = db.fieldByNo(fieldNo).boolValue(db.buffer); err != nil {
		panic(err)
	}
	return
}

// FieldValueAsDate returns the date value of the field of the current record.
// Field type must be date ("D"). Fields are numbered starting from 1.
func (db *XBase) FieldValueAsDate(fieldNo int) (d time.Time) {
	if db.err != nil {
		return
	}
	defer db.wrapFieldError("FieldValueAsDate", fieldNo)
	var err error
	if d, err = db.fieldByNo(fieldNo).dateValue(db.buffer); err != nil {
		panic(err)
	}
	return
}

// SetFieldValue sets the field value of the current record.
// The value must match the field type.
// To save the changes, you need to call the Save method.
func (db *XBase) SetFieldValue(fieldNo int, value interface{}) {
	if db.err != nil {
		return
	}
	defer db.wrapFieldError("SetFieldValue", fieldNo)
	if err := db.fieldByNo(fieldNo).setValue(db.buffer, value, db.encoder); err != nil {
		panic(err)
	}
}

// Add adds a new empty record.
// To save the changes, you need to call the Save method.
func (db *XBase) Add() error {
	if db.isAdd {
		return fmt.Errorf("current record is add model,Save it first")
	}
	db.isAdd = true
	db.clearBuf()
	return nil
}

// Append an input value,auto call save
func (db *XBase) Append(input interface{}) error {
	if db.marshal == nil {
		db.marshal = NewEncoder(db)
		db.marshal.SetHeader(db.fields)
	}
	if isNilFixed(input) {
		if err := db.Add(); err != nil {
			return err
		}
		return db.Save()
	}
	return db.marshal.Encode(input)
}

// Save writes changes to the file.
// Before calling it, all changes to the object were made
// only in memory and will be lost when you move to another record
// or close the file.
func (db *XBase) Save() error {
	if db.err != nil {
		return db.err
	}
	// ignore to write header
	if db.isAdd {
		if err := db.seekRecord(db.recCount() + 1); err != nil {
			return err
		}
		if err := db.fileWrite(db.buffer); err != nil {
			return err
		}
		db.recordNum++
		db.header.RecCount++
		db.isAdd = false
	} else {
		if db.recordNum == 0 {
			return nil
		}
		//edit
		if err := db.seekRecord(db.recordNum); err != nil {
			return err
		}
		if err := db.fileWrite(db.buffer); err != nil {
			return err
		}
	}
	db.isMod = true
	return nil
}

// Del marks the current record as "deleted".
// The record is not physically deleted from the file
// and can be subsequently restored.
func (db *XBase) Del() {
	db.buffer[0] = '*'
}

// RecDeleted returns the value of the delete flag for the current record.
func (db *XBase) RecDeleted() bool {
	return db.buffer[0] == '*'
}

// Recall removes the deletion mark from the current record.
func (db *XBase) Recall() {
	db.buffer[0] = ' '
}

// Clear zeroes the field values ​​of the current record and error.
func (db *XBase) Clear() {
	db.clearBuf()
	db.err = nil
	db.isAdd = false
}

// RecCount returns the number of records in the DBF file.
func (db *XBase) RecCount() int64 {
	return db.recCount()
}

// FieldCount returns the number of fields in the DBF file.
func (db *XBase) FieldCount() int {
	return len(db.fields)
}

// FieldNo returns the number of the field by name.
// If name is not found returns 0.
// Fields are numbered starting from 1.
func (db *XBase) FieldNo(name string) int {
	name = strings.ToUpper(strings.TrimSpace(name))
	for i, f := range db.fields {
		if f.name() == name {
			return i + 1
		}
	}
	return 0
}

// AddField adds a field to the structure of the DBF file.
// This method can only be used before creating a new file.
//
// The following field types are supported: "C", "N", "F", "L", "D".
//
// The opts parameter contains optional parameters: field length and number of decimal places.
//
// Examples:
//     db.AddField("NAME", "C", 24)
//     db.AddField("COUNT", "N", 8)
//     db.AddField("PRICE", "F", 12, 2)
//     db.AddField("FLAG", "L")
//     db.AddField("DATE", "D")
func (db *XBase) AddField(name string, typ string, opts ...int) error {
	length := 0
	dec := 0
	if len(opts) > 0 {
		length = opts[0]
	}
	if len(opts) > 1 {
		dec = opts[1]
	}
	f, err := NewField(name, typ, length, dec)
	if err != nil {
		return err
	}
	db.fields = append(db.fields, f)
	return nil
}

// SetCodePage sets the encoding mode for reading and writing string field values.
// The default code page is 0.
//
// Supported code pages:
//     437   - US MS-DOS
//     850   - International MS-DOS
//     1252  - Windows ANSI
//     10000 - Standard Macintosh
//     852   - Easern European MS-DOS
//     866   - Russian MS-DOS
//     865   - Nordic MS-DOS
//     1255  - Hebrew Windows
//     1256  - Arabic Windows
//     10007 - Russian Macintosh
//     1250  - Eastern European Windows
//     1251  - Russian Windows
//     1254  - Turkish Windows
//     1253  - Greek Windows
func (db *XBase) SetCodePage(cp int) {
	cm := charMapByPage(cp)
	if cm == nil {
		return
	}
	db.encoder = cm.NewEncoder()
	db.decoder = cm.NewDecoder()
	db.header.setCodePage(cp)
}

// CodePage returns the code page of a DBF file.
// Returns 0 if no code page is specified.
func (db *XBase) CodePage() int {
	return db.header.codePage()
}

// ModDate returns the modification date of the DBF file.
func (db *XBase) ModDate() time.Time {
	return db.header.modDate()
}

// Error returns an error when working with a DBF file.
func (db *XBase) Error() error {
	return db.err
}

// writeFileEnd called when close file,should be written dbf file end tag
func (db *XBase) writeFileEnd() (err error) {
	size := int64(db.header.DataOffset) + db.RecCount()*int64(db.header.RecSize) + 1
	if file, ok := db.rws.(fs.File); ok {
		// check file size
		fi, err := file.Stat()
		if err != nil {
			return err
		}
		if fi.Size()+1 != size {
			// file has changed by outer,do nothing,believe outer
			return nil
		}
	}

	if _, err = db.rws.Seek(0, 2); err != nil {
		// end file position
		return err
	}
	if err = db.fileWrite([]byte{fileEnd}); err != nil {
		return err
	}
	return nil
}

// GoTo allows you to go to a record by its ordinal number.
// Numbering starts from 1.
func (db *XBase) GoTo(recNo int64) (err error) {
	if recNo < 1 {
		return BOF
	}
	if recNo > db.recCount() {
		return io.EOF
	}
	db.recordNum = recNo
	if err := db.seekRecord(db.recordNum); err != nil {
		return err
	}
	if n, err := db.rws.Read(db.buffer); err != nil {
		return err
	} else if n != len(db.buffer) {
		return io.EOF
	}
	return nil
}

func (db *XBase) makeBuf() {
	db.buffer = make([]byte, int(db.header.RecSize))
}

// return the field by parameter.
func (db *XBase) fieldByNo(fieldNo int) *field {
	if fieldNo < 1 || fieldNo > len(db.fields) {
		panic(fmt.Errorf("field number out of range"))
	}
	return db.fields[fieldNo-1]
}

func (db *XBase) recCount() int64 {
	return int64(db.header.RecCount)
}

func (db *XBase) checkFields() error {
	if len(db.fields) == 0 {
		return fmt.Errorf("file structure undefined")
	}
	return nil
}

func (db *XBase) checkRecNo() error {
	if db.recordNum > db.recCount() {
		return io.EOF
	}
	if db.recordNum < 1 {
		return io.EOF
	}
	return nil
}

func (db *XBase) wrapFieldError(s string, fieldNo int) {
	if r := recover(); r != nil {
		prefix := fmt.Sprintf("xbase: %s: field %d", s, fieldNo)
		if fieldNo < 1 || fieldNo > len(db.fields) {
			db.err = fmt.Errorf("%s: %w", prefix, r)
		} else {
			db.err = fmt.Errorf("%s %q: %w", prefix, db.fields[fieldNo-1].name(), r)
		}
	}
}

// seekRecord move the file ptr to the record start position
// recordNo start 1
func (db *XBase) seekRecord(recordNo int64) error {
	offset := int64(db.header.DataOffset) + int64(db.header.RecSize)*(recordNo-1)
	_, err := db.rws.Seek(offset, 0)
	return err
}

func (db *XBase) calcRecSize() uint16 {
	size := 1 // deleted mark
	for _, f := range db.fields {
		size += int(f.Len)
	}
	return uint16(size)
}

func (db *XBase) writeHeader() error {
	if db.header.DataOffset == 0 {
		db.header.setFieldCount(len(db.fields))
	}
	if db.header.RecSize == 0 {
		db.header.RecSize = db.calcRecSize()
	}
	if _, err := db.rws.Seek(0, 0); err != nil {
		return err
	}
	return db.header.write(db.rws)
}

// write the field description
func (db *XBase) writeFields() error {
	offset := 1 // deleted mark
	for _, f := range db.fields {
		f.Offset = uint32(offset)
		if err := f.write(db.rws); err != nil {
			return err
		}
		offset += int(f.Len)
	}
	if err := db.fileWrite([]byte{headerEnd}); err != nil {
		return err
	}
	return nil
}

func (db *XBase) readFields(reader io.Reader) error {
	offset := 1 // deleted mark
	count := db.header.fieldCount()
	for i := 0; i < count; i++ {
		f := &field{}
		err := f.read(reader)
		if err != nil {
			return err
		}
		f.Offset = uint32(offset)
		db.fields = append(db.fields, f)
		offset += int(f.Len)
	}
	return nil
}

func (db *XBase) clearBuf() {
	for i := range db.buffer {
		db.buffer[i] = ' '
	}
}

func (db *XBase) fileWrite(b []byte) error {
	_, err := db.rws.Write(b)
	return err
}
