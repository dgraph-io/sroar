//go:build ignore
// +build ignore

package main

import (
	. "github.com/mmcloughlin/avo/build"
	. "github.com/mmcloughlin/avo/operand"
	"github.com/mmcloughlin/avo/reg"
)

func BitmapAnd() {
	TEXT("asmBitmapAnd", NOSPLIT|NOPTR, "func(data []uint16, other []uint16, buf []uint16) int")
	Pragma("noescape")
	Doc("asmBitmapAnd performs an And operation between data []uint16 and other []uint16. \n // Returns data with values and cardinality.\n")
	a := Mem{Base: Load(Param("data").Base(), GP64())}
	b := Mem{Base: Load(Param("other").Base(), GP64())}
	c := Mem{Base: Load(Param("buf").Base(), GP64())}

	VZEROALL()
	A := YMM()
	B := YMM()

	n := make([]reg.GPVirtual, 16)
	for i := 0; i < 16; i++ {
		n[i] = GP64()
		XORQ(n[i], n[i])
	}

	m := GP64()
	y := GP32()
	XORL(y, y)
	XORQ(m, m)

	Label("r")
	CMPL(y, U32(4096))
	JE(LabelRef("done"))

	VMOVDQU(a.Offset(0).Idx(y, 2), A)
	VMOVDQU(a.Offset(16*2).Idx(y, 2), B)
	VPAND(b.Offset(0).Idx(y, 2), A, A)
	VPAND(b.Offset(16*2).Idx(y, 2), B, B)
	VMOVDQU(A, c.Offset(0).Idx(y, 2))
	VMOVDQU(B, c.Offset(16*2).Idx(y, 2))

	for i := 0; i < 8; i++ {
		POPCNTQ(c.Offset(i*8).Idx(y, 2), n[i])
	}
	for i := 0; i < 8; i++ {
		ADDQ(n[i], m)
	}

	ADDL(U32(32), y)
	JMP(LabelRef("r"))

	Label("done")
	Store(m, ReturnIndex(0))
	RET()
}

func BitmapOr() {
	TEXT("asmBitmapOr", NOSPLIT|NOPTR, "func(data []uint16, other []uint16) int")
	Pragma("noescape")
	Doc("asmBitmapOr performs an Or operation between data []uint16 and other []uint16. \n // Returns data with values and cardinality.\n")
	a := Mem{Base: Load(Param("data").Base(), GP64())}
	b := Mem{Base: Load(Param("other").Base(), GP64())}

	VZEROALL()
	A := YMM()
	B := YMM()

	n := make([]reg.GPVirtual, 16)
	for i := 0; i < 16; i++ {
		n[i] = GP64()
		XORQ(n[i], n[i])
	}

	m := GP64()
	y := GP32()
	XORL(y, y)
	XORQ(m, m)

	Label("r")
	CMPL(y, U32(4096))
	JE(LabelRef("done"))

	VMOVDQU(a.Offset(0).Idx(y, 2), A)
	VMOVDQU(a.Offset(16*2).Idx(y, 2), B)
	VPOR(b.Offset(0).Idx(y, 2), A, A)
	VPOR(b.Offset(16*2).Idx(y, 2), B, B)
	VMOVDQU(A, a.Offset(0).Idx(y, 2))
	VMOVDQU(B, a.Offset(16*2).Idx(y, 2))

	for i := 0; i < 8; i++ {
		POPCNTQ(a.Offset(i*8).Idx(y, 2), n[i])
	}
	for i := 0; i < 8; i++ {
		ADDQ(n[i], m)
	}

	ADDL(U32(32), y)
	JMP(LabelRef("r"))

	Label("done")
	Store(m, ReturnIndex(0))
	RET()
}

func BitmapAndNot() {
	TEXT("asmBitmapAndNot", NOSPLIT|NOPTR, "func(data []uint16, other []uint16, buf []uint16) int")
	Pragma("noescape")
	Doc("asmBitmapAnd performs an AndNot operation between data []uint16 and other []uint16. \n // Returns data with values and cardinality.\n")
	a := Mem{Base: Load(Param("data").Base(), GP64())}
	b := Mem{Base: Load(Param("other").Base(), GP64())}
	c := Mem{Base: Load(Param("buf").Base(), GP64())}

	VZEROALL()
	A := YMM()
	B := YMM()

	n := make([]reg.GPVirtual, 16)
	for i := 0; i < 16; i++ {
		n[i] = GP64()
		XORQ(n[i], n[i])
	}

	m := GP64()
	y := GP32()
	XORL(y, y)
	XORQ(m, m)

	Label("r")
	CMPL(y, U32(4096))
	JE(LabelRef("done"))

	VMOVDQU(b.Offset(0).Idx(y, 2), A)
	VMOVDQU(b.Offset(16*2).Idx(y, 2), B)
	VPANDN(a.Offset(0).Idx(y, 2), A, A)
	VPANDN(a.Offset(16*2).Idx(y, 2), B, B)
	VMOVDQU(A, c.Offset(0).Idx(y, 2))
	VMOVDQU(B, c.Offset(16*2).Idx(y, 2))

	for i := 0; i < 8; i++ {
		POPCNTQ(c.Offset(i*8).Idx(y, 2), n[i])
	}
	for i := 0; i < 8; i++ {
		ADDQ(n[i], m)
	}

	ADDL(U32(32), y)
	JMP(LabelRef("r"))

	Label("done")
	Store(m, ReturnIndex(0))
	RET()
}

func main() {
	BitmapOr()
	BitmapAnd()
	BitmapAndNot()

	Generate()
}
