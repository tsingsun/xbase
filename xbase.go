package xbase

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
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

type XBase struct {
	header *header
	fields []*field
	file   *os.File
	// rawBuffer is the current record buffer .
	rawBuffer []byte
	err       error
	recNo     int64
	isAdd     bool
	isMod     bool
	encoder   *encoding.Encoder
	decoder   *encoding.Decoder
}

type cPage struct {
	code byte
	page int
	cm   *charmap.Charmap
}

var cPages = []cPage{
	{code: 0x01, page: 437, cm: charmap.CodePage437},  // US MS-DOS
	{code: 0x02, page: 850, cm: charmap.CodePage850},  // International MS-DOS
	{code: 0x03, page: 1252, cm: charmap.Windows1252}, // Windows ANSI
	{code: 0x04, page: 10000, cm: charmap.Macintosh},  // Standard Macintosh
	{code: 0x64, page: 852, cm: charmap.CodePage852},  // Easern European MS-DOS
	{code: 0x65, page: 866, cm: charmap.CodePage866},  // Russian MS-DOS
	{code: 0x66, page: 865, cm: charmap.CodePage865},  // Nordic MS-DOS

	// Not found in package charmap
	// 0x67	Codepage 861 Icelandic MS-DOS
	// 0x68	Codepage 895 Kamenicky (Czech) MS-DOS
	// 0x69	Codepage 620 Mazovia (Polish) MS-DOS
	// 0x6A	Codepage 737 Greek MS-DOS (437G)
	// 0x6B	Codepage 857 Turkish MS-DOS
	// 0x78	Codepage 950 Chinese (Hong Kong SAR, Taiwan) Windows
	// 0x79	Codepage 949 Korean Windows
	// 0x7A	Codepage 936 Chinese (PRC, Singapore) Windows
	// 0x7B	Codepage 932 Japanese Windows
	// 0x7C	Codepage 874 Thai Windows

	{code: 0x7D, page: 1255, cm: charmap.Windows1255},        // Hebrew Windows
	{code: 0x7E, page: 1256, cm: charmap.Windows1256},        // Arabic Windows
	{code: 0x96, page: 10007, cm: charmap.MacintoshCyrillic}, // Russian MacIntosh

	// Not found in package charmap
	// 0x97	Codepage 10029 MacIntosh EE
	// 0x98	Codepage 10006 Greek MacIntosh

	{code: 0xC8, page: 1250, cm: charmap.Windows1250}, // Eastern European Windows
	{code: 0xC9, page: 1251, cm: charmap.Windows1251}, // Russian Windows
	{code: 0xCA, page: 1254, cm: charmap.Windows1254}, // Turkish Windows
	{code: 0xCB, page: 1253, cm: charmap.Windows1253}, // Greek Windows
}

func charMapByPage(page int) *charmap.Charmap {
	for i := range cPages {
		if cPages[i].page == page {
			return cPages[i].cm
		}
	}
	return nil
}

func codeByPage(page int) byte {
	for i := range cPages {
		if cPages[i].page == page {
			return cPages[i].code
		}
	}
	return 0
}

func pageByCode(code byte) int {
	for i := range cPages {
		if cPages[i].code == code {
			return cPages[i].page
		}
	}
	return 0
}

// New creates a XBase object to work with a DBF file.
func New() *XBase {
	return &XBase{header: newHeader()}
}

// CreateFile creates a new file in DBF format.
// If a file with that name exists, it will be overwritten.
func (db *XBase) CreateFile(name string) (err error) {
	if err = db.checkFields(); err != nil {
		return
	}
	if db.file, err = os.Create(name); err != nil {
		return
	}
	db.header.setFieldCount(len(db.fields))
	db.header.RecSize = db.calcRecSize()
	if err := db.writeHeader(); err != nil {
		return
	}
	if err = db.writeFields(); err != nil {
		return
	}
	if err = db.fileWrite([]byte{headerEnd}); err != nil {
		return
	}
	db.makeBuf()
	db.isMod = true
	return
}

// Open opens an existing DBF file.
func Open(name string, readOnly bool) (db *XBase, err error) {
	db = New()
	if readOnly {
		db.file, err = os.Open(name)
	} else {
		db.file, err = os.OpenFile(name, os.O_RDWR, 0666)
	}
	if err != nil {
		return
	}

	if err = db.header.read(db.file); err != nil {
		return nil, err
	}

	if err = db.readFields(); err != nil {
		return nil, err
	}
	db.makeBuf()
	db.SetCodePage(db.CodePage())
	return db, nil
}

// Close closes a previously opened or created DBF file.
func (db *XBase) Close() error {
	if db.file == nil {
		return nil
	}
	if db.isMod {
		db.header.setModDate(time.Now())
		if err := db.writeHeader(); err != nil {
			return err
		}
		if err := db.writeFileEnd(); err != nil {
			return err
		}
	}
	return db.file.Close()
}

// First positions the object to the first record.
func (db *XBase) First() error {
	return db.GoTo(1)
}

// Last positions the object to the last record.
func (db *XBase) Last() {
	if db.err != nil {
		return
	}
	defer db.wrapError("Last")
	db.GoTo(db.recCount())
}

// Next positions the object to the next record.
func (db *XBase) Next() {
	if db.err != nil {
		return
	}
	defer db.wrapError("Next")
	db.GoTo(db.recNo + 1)
}

// Prev positions the object to the previous record.
func (db *XBase) Prev() {
	if db.err != nil {
		return
	}
	defer db.wrapError("Prev")
	db.GoTo(db.recNo - 1)
}

// RecNo returns the sequence number of the current record.
// Numbering starts from 1.
func (db *XBase) RecNo() int64 {
	return db.recNo
}

// EOF returns true if end of file is reached or error.
func (db *XBase) EOF() bool {
	return db.recNo > db.recCount() || db.recCount() == 0 || db.err != nil
}

// BOF returns true if the beginning of the file is reached or error.
func (db *XBase) BOF() bool {
	return db.recNo == 0 || db.recCount() == 0 || db.err != nil
}

func (db *XBase) Header() ([]string, error) {
	var hl []string
	for _, f := range db.fields {
		hl = append(hl, f.name())
	}
	return hl, nil
}

// ReadLine returns buffer string value
func (db *XBase) ReadLine() ([]string, error) {
	if db.err != nil {
		return nil, db.err
	}
	var buffer = make([]byte, len(db.rawBuffer))
	copy(buffer, db.rawBuffer)
	var sl []string
	for _, f := range db.fields {
		s := strings.TrimSpace(string(f.buffer(buffer)))
		sl = append(sl, s)
	}
	db.Next()
	return sl, db.err
}

// FieldValueAsString returns the string value of the field of the current record.
// Fields are numbered starting from 1.
func (db *XBase) FieldValueAsString(fieldNo int) string {
	if db.err != nil {
		return ""
	}
	defer db.wrapFieldError("FieldValueAsString", fieldNo)
	return db.fieldByNo(fieldNo).stringValue(db.rawBuffer, db.decoder)
}

// FieldValueAsInt returns the integer value of the field of the current record.
// Field type must be numeric ("N"). Fields are numbered starting from 1.
func (db *XBase) FieldValueAsInt(fieldNo int) int64 {
	if db.err != nil {
		return 0
	}
	defer db.wrapFieldError("FieldValueAsInt", fieldNo)
	return db.fieldByNo(fieldNo).intValue(db.rawBuffer)
}

// FieldValueAsFloat returns the float value of the field of the current record.
// Field type must be numeric ("N"). Fields are numbered starting from 1.
func (db *XBase) FieldValueAsFloat(fieldNo int) float64 {
	if db.err != nil {
		return 0
	}
	defer db.wrapFieldError("FieldValueAsFloat", fieldNo)
	return db.fieldByNo(fieldNo).floatValue(db.rawBuffer)
}

// FieldValueAsBool returns the boolean value of the field of the current record.
// Field type must be logical ("L"). Fields are numbered starting from 1.
func (db *XBase) FieldValueAsBool(fieldNo int) bool {
	if db.err != nil {
		return false
	}
	defer db.wrapFieldError("FieldValueAsBool", fieldNo)
	return db.fieldByNo(fieldNo).boolValue(db.rawBuffer)
}

// FieldValueAsDate returns the date value of the field of the current record.
// Field type must be date ("D"). Fields are numbered starting from 1.
func (db *XBase) FieldValueAsDate(fieldNo int) time.Time {
	var d time.Time
	if db.err != nil {
		return d
	}
	defer db.wrapFieldError("FieldValueAsDate", fieldNo)
	return db.fieldByNo(fieldNo).dateValue(db.rawBuffer)
}

// SetFieldValue sets the field value of the current record.
// The value must match the field type.
// To save the changes, you need to call the Save method.
func (db *XBase) SetFieldValue(fieldNo int, value interface{}) {
	if db.err != nil {
		return
	}
	defer db.wrapFieldError("SetFieldValue", fieldNo)
	db.fieldByNo(fieldNo).setValue(db.rawBuffer, value, db.encoder)
}

// Add adds a new empty record.
// To save the changes, you need to call the Save method.
func (db *XBase) Add() {
	db.isAdd = true
	db.clearBuf()
}

// Save writes changes to the file.
// Before calling it, all changes to the object were made
// only in memory and will be lost when you move to another record
// or close the file.
func (db *XBase) Save() error {
	if db.isAdd {
		db.recNo = db.recCount() + 1
		if err := db.seekRecord(); err != nil {
			return err
		}
		if err := db.fileWrite(db.rawBuffer); err != nil {
			return err
		}
		db.header.RecCount++
		db.isAdd = false
	} else {
		if err := db.checkRecNo(); err != nil {
			return err
		}
		//edit
		if err := db.seekRecord(); err != nil {
			return err
		}
		if err := db.fileWrite(db.rawBuffer); err != nil {
			return err
		}
	}
	db.isMod = true
	return nil
}

func (db *XBase) Flush() {
	if db.isMod {
		db.header.setModDate(time.Now())
		db.writeHeader()
		db.writeFileEnd()
		db.isMod = false
	}
}

// Del marks the current record as "deleted".
// The record is not physically deleted from the file
// and can be subsequently restored.
func (db *XBase) Del() {
	db.rawBuffer[0] = '*'
}

// RecDeleted returns the value of the delete flag for the current record.
func (db *XBase) RecDeleted() bool {
	return db.rawBuffer[0] == '*'
}

// Recall removes the deletion mark from the current record.
func (db *XBase) Recall() {
	db.rawBuffer[0] = ' '
}

// Clear zeroes the field values ​​of the current record.
func (db *XBase) Clear() {
	db.clearBuf()
}

// RecCount returns the number of records in the DBF file.
func (db *XBase) RecCount() int64 {
	return db.recCount()
}

// FieldCount returns the number of fields in the DBF file.
func (db *XBase) FieldCount() int {
	return len(db.fields)
}

// FieldInfo returns field attributes by number.
// Fields are numbered starting from 1.
func (db *XBase) FieldInfo(fieldNo int) (name, typ string, length, dec int) {
	if db.err != nil {
		return
	}
	defer db.wrapFieldError("FieldInfo", fieldNo)
	f := db.fieldByNo(fieldNo)
	name = f.name()
	typ = string([]byte{f.Type})
	length = int(f.Len)
	dec = int(f.Dec)
	return
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
// The following field types are supported: "C", "N", "L", "D".
//
// The opts parameter contains optional parameters: field length and number of decimal places.
//
// Examples:
//     db.AddField("NAME", "C", 24)
//     db.AddField("COUNT", "N", 8)
//     db.AddField("PRICE", "N", 12, 2)
//     db.AddField("FLAG", "L")
//     db.AddField("DATE", "D")
func (db *XBase) AddField(name string, typ string, opts ...int) {
	length := 0
	dec := 0
	if len(opts) > 0 {
		length = opts[0]
	}
	if len(opts) > 1 {
		dec = opts[1]
	}
	f := newField(name, typ, length, dec)
	db.fields = append(db.fields, f)
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

// writeFileEnd wreter dbf file end tag
func (db *XBase) writeFileEnd() error {
	size := int64(db.header.DataOffset) + db.RecCount()*int64(db.header.RecSize) + 1
	// check file size
	fi, err := db.file.Stat()
	if err != nil {
		return err
	}
	if fi.Size()+1 == size {
		if _, err = db.file.Seek(0, 2); err != nil {
			// end file position
			return err
		}
		if err = db.fileWrite([]byte{fileEnd}); err != nil {
			return err
		}
	}
	return nil
}

// GoTo allows you to go to a record by its ordinal number.
// Numbering starts from 1.
func (db *XBase) GoTo(recNo int64) (err error) {
	if db.BOF() {
		return fmt.Errorf("can't goto:%d", recNo)
	}
	if recNo > db.recCount() {
		return fmt.Errorf("can't goto:%d", recNo)
	}
	db.recNo = recNo
	if err := db.seekRecord(); err != nil {
		return err
	}
	if n, err := db.file.Read(db.rawBuffer); err != nil {
		return err
	} else if n != len(db.rawBuffer) {
		return io.EOF
	}
	return nil
}

func (db *XBase) makeBuf() {
	db.rawBuffer = make([]byte, int(db.header.RecSize))
}

func (db *XBase) fieldByNo(fieldNo int) *field {
	db.checkFieldNo(fieldNo)
	return db.fields[fieldNo-1]
}

func (db *XBase) recCount() int64 {
	return int64(db.header.RecCount)
}

func (db *XBase) checkFile() error {
	if db.file == nil {
		return fmt.Errorf("file not open")
	}
	return nil
}

func (db *XBase) checkFields() error {
	if len(db.fields) == 0 {
		return fmt.Errorf("file structure undefined")
	}
	return nil
}

func (db *XBase) checkFieldNo(fieldNo int) {
	if fieldNo < 1 || fieldNo > len(db.fields) {
		panic(fmt.Errorf("field number out of range"))
	}
}

func (db *XBase) checkRecNo() error {
	if db.recNo > db.recCount() {
		return io.EOF
	}
	if db.recNo < 1 {
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
		if db.isPanic {
			panic(db.err)
		}
	}
}

// seekRecord move the file ptr to the record start position
func (db *XBase) seekRecord() error {
	offset := int64(db.header.DataOffset) + int64(db.header.RecSize)*(db.recNo-1)
	_, err := db.file.Seek(offset, 0)
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
	if _, err := db.file.Seek(0, 0); err != nil {
		return err
	}
	return db.header.write(db.file)
}

func (db *XBase) writeFields() error {
	offset := 1 // deleted mark
	for _, f := range db.fields {
		f.Offset = uint32(offset)
		if err := f.write(db.file); err != nil {
			return err
		}
		offset += int(f.Len)
	}
	return nil
}

func (db *XBase) readFields() error {
	offset := 1 // deleted mark
	count := db.header.fieldCount()
	for i := 0; i < count; i++ {
		f := &field{}
		err := f.read(db.file)
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
	for i := range db.rawBuffer {
		db.rawBuffer[i] = ' '
	}
}

// File utils

// File return xbase opened file info
func (db *XBase) File() (*os.File, error) {
	if db.file == nil {
		return nil, fmt.Errorf("file not open")
	}
	return db.file, nil
}

func (db *XBase) fileWrite(b []byte) error {
	_, err := db.file.Write(b)
	return err
}
