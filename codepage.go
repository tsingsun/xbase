package xbase

import "golang.org/x/text/encoding/charmap"

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
