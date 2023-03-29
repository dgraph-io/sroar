
//go:build linux && amd64
// Code generated by command: go run asm.go -out asm_x86.s -stubs asm_x86.go. DO NOT EDIT.

package sroar

import (
	cpu "github.com/klauspost/cpuid/v2"
)

// asmBitmapOr performs an Or operation between data []uint16 and other []uint16.
// Returns data with values and cardinality.
//
//go:noescape
func asmBitmapOr(data []uint16, other []uint16) int

// asmBitmapAnd performs an And operation between data []uint16 and other []uint16.
// Returns data with values and cardinality.
//
//go:noescape
func asmBitmapAnd(data []uint16, other []uint16, buf []uint16) int

// asmBitmapAnd performs an AndNot operation between data []uint16 and other []uint16.
// Returns data with values and cardinality.
//
//go:noescape
func asmBitmapAndNot(data []uint16, other []uint16, buf []uint16) int


func init() {
	// Check for CPU SIMD
	asm = cpu.CPU.Supports(cpu.AVX, cpu.AVX2, cpu.POPCNT) 
}