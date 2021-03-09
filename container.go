package roar

import (
	"fmt"
	"strings"
)

// container uses extra 8 bytes in the front as header.
// First 2 bytes are used for storing size of the container.
// The container size cannot exceed the vicinity of 8KB. At 8KB, we switch from packed arrays to
// bitmaps. We can fit the entire uint16 worth of bitmaps in 8KB (2^16 / 8 = 8 KB).

const (
	typeArray  uint16 = 0x00
	typeBitmap uint16 = 0x01

	indexSize        int = 0
	indexType        int = 1
	indexCardinality int = 2
	indexUnused      int = 3

	minSizeOfContainer = 8 + 2    // 2 bytes for allowing one uint16 to be added.
	maxSizeOfContainer = 8 + 8192 // 8192 for storing bitmap container.
	startIdx           = uint16(4)
)

// getSize returns the size of container in bytes. The way to calculate the uint16 data
// size is (byte size/2) - 4.
func getSize(data []byte) uint16 {
	x := toUint16Slice(data[:2])
	return x[0]
}
func setSize(data []byte, sz uint16) {
	x := toUint16Slice(data[:2])
	x[0] = sz
}
func dataAt(data []uint16, i int) uint16 {
	return data[int(startIdx)+i]
}

type packedContainer []uint16

// find returns the index of the first element >= x.
// The index is based on data portion of the container, ignoring startIdx.
// If the element > than all elements present, then N is returned where N = cardinality of the
// container.
func (c packedContainer) find(x uint16) uint16 {
	N := c[indexCardinality]
	for i := startIdx; i < startIdx+N; i++ {
		if len(c) <= int(i) {
			fmt.Printf("N: %d i: %d\n", N, i)
			panic(fmt.Sprintf("find: %d len(c) %d <= i %d\n", x, len(c), i))
		}
		if c[i] >= x {
			return i - startIdx
		}
	}
	return N
}
func (c packedContainer) has(x uint16) bool {
	N := c[indexCardinality]
	idx := c.find(x)
	fmt.Printf("has for %d idx: %d\n", x, idx)
	if idx == N {
		return false
	}
	return c[startIdx+idx] == x
}

func (c packedContainer) add(x uint16) bool {
	idx := c.find(x)
	N := c[indexCardinality]
	offset := startIdx + idx
	if c[offset] == x {
		return false
	}

	if idx < N {
		// The entry at offset is the first entry, which is greater than x. Move it to the right.
		copy(c[offset+1:], c[offset:])
	}
	c[offset] = x
	c[indexCardinality] = N + 1
	return true
}

func (c packedContainer) isFull() bool {
	N := c[indexCardinality]
	return int(N) >= len(c)-4
}

func (c packedContainer) all() []uint16 {
	N := c[indexCardinality]
	return c[startIdx : startIdx+N]
}

func (c packedContainer) String() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("Size: %d\n", c[0]))
	for i, val := range c[4:] {
		b.WriteString(fmt.Sprintf("%d: %d\n", i, val))
	}
	return b.String()
}

type bitmapContainer []uint16

var bitmapMask []uint16

func init() {
	bitmapMask = make([]uint16, 16)
	for i := 0; i < 16; i++ {
		bitmapMask[i] = 1 << (15 - i)
	}
}

func (b bitmapContainer) add(x uint16) bool {
	idx := x / 16
	pos := x % 16

	if has := b[4+idx] & bitmapMask[pos]; has > 0 {
		return false
	}

	b[4+idx] |= bitmapMask[pos]
	b[indexCardinality] += 1
	return true
}

func (b bitmapContainer) has(x uint16) bool {
	idx := x / 16
	pos := x % 16

	has := b[4+idx] & bitmapMask[pos]
	return has > 0
}

func (b bitmapContainer) isFull() bool {
	return false
}
