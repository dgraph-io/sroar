package roar

import (
	"log"
	"reflect"
	"unsafe"

	"github.com/pkg/errors"
)

func assert(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assertion failure"))
	}
}
func check(err error) {
	if err != nil {
		log.Fatalf("%+v", err)
	}
}
func check2(_ interface{}, err error) {
	check(err)
}

// These methods (byteSliceAsUint16Slice,...) do not make copies,
// they are pointer-based (unsafe). The caller is responsible to
// ensure that the input slice does not get garbage collected, deleted
// or modified while you hold the returned slince.
////
func toUint16Slice(b []byte) (result []uint16) { // here we create a new slice holder
	if len(b) == 0 {
		return nil
	}
	// reference: https://go101.org/article/unsafe.html

	var u16s []uint16
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&u16s))
	hdr.Len = len(b) / 2
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return u16s
}

func toUint32Slice(b []byte) (result []uint32) {
	if len(b) == 0 {
		return nil
	}
	var u32s []uint32
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&u32s))
	hdr.Len = len(b) / 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return u32s
}

// BytesToU32Slice converts the given byte slice to uint32 slice
func toUint64Slice(b []byte) []uint64 {
	if len(b) == 0 {
		return nil
	}
	var u64s []uint64
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&u64s))
	hdr.Len = len(b) / 8
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return u64s
}

func sizeInBytesU16(n int) int { return n * 2 }
func sizeInBytesU64(n int) int { return n * 8 }
