//go:build linux && amd64

package sroar

import (
	"math/rand"
	"testing"
)

func createBitmapTestSlices() ([]uint16, []uint16, []uint16) {
	aa := make([]uint16, 4096)
	bb := make([]uint16, 4096)
	a2 := make([]uint16, 4096)
	for i := 0; i < 4096; i++ {
		aa[i] = uint16(rand.Intn(1 << 16))
		bb[i] = uint16(rand.Intn(1 << 16))
	}
	copy(a2, aa)
	return aa, a2, bb
}

func TestBitmapFn(t *testing.T) {
	var n, n1 int
	for _, v := range bitmapTests {
		t.Run(v.name, func(t *testing.T) {
			aa, a2, bb := createBitmapTestSlices()
			if v.fn != nil && v.asm != nil {
				n = v.fn(aa, bb, aa)
				n1 = v.asm(a2, bb, a2)
			} else {
				n = v.fn2(aa, bb)
				n1 = v.asm2(a2, bb)
			}

			if n != n1 {
				t.Errorf("%s. Expected ASM %d to equal Fn %d", v.name, n, n1)
			}
			for i, a := range aa {
				if a2[i] != a {
					t.Fatalf("%s. Values are not equal at: %d", v.name, i)
				}
			}
		})
	}
}

var bitmapTests = []struct {
	name string
	fn2  func(data []uint16, other []uint16) int
	asm2 func(data []uint16, other []uint16) int
	fn   func(data []uint16, other []uint16, buf []uint16) int
	asm  func(data []uint16, other []uint16, buf []uint16) int
}{
	{name: "Or", fn2: orBitmap, asm2: asmBitmapOr},
	{name: "And", fn: andBitmap, asm: asmBitmapAnd},
	{name: "AndNot", fn: andNotBitmap, asm: asmBitmapAndNot},
}

func BenchmarkBitmap(b *testing.B) {
	for _, v := range bitmapTests {
		aa, a2, bb := createBitmapTestSlices()
		b.Run(v.name, func(b *testing.B) {
			if v.fn2 != nil && v.asm2 != nil {
				b.Run("Asm", func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						copy(aa, a2)
						v.asm2(aa, bb)
					}
				})
				b.Run("Fn", func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						copy(aa, a2)
						v.fn2(aa, bb)
					}
				})
			} else {
				b.Run("Asm", func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						copy(aa, a2)
						v.asm(aa, bb, aa)
					}
				})
				b.Run("Fn", func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						copy(aa, a2)
						v.fn(aa, bb, aa)
					}
				})
			}
		})
	}
}
