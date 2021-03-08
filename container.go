package roar

import (
	"fmt"
	"strings"
)

// container uses extra 8 bytes in the front as header.
type container []uint16

const (
	typeArray  uint16 = 0x00
	typeBitmap uint16 = 0x01

	indexType        int = 0
	indexSize        int = 1 // in bytes.
	indexCardinality int = 2
	indexUnused      int = 3

	minSizeOfContainer = 10

	startIdx = uint16(4)
)

// getSize returns the size of container in bytes. The way to calculate the uint16 data
// size is (byte size/2) - 4.
func getSize(data []byte) uint16 {
	s := toUint16Slice(data)
	return s[indexSize]
}

func (c container) set(index int, t uint16) {
	c[index] = t
}

func (c container) get(index int) uint16 {
	return c[index]
}

func (c container) data() []uint16 {
	return c[startIdx:]
}

// find returns the index of the first element >= x.
// The index is based on data portion of the container, ignoring startIdx.
// If the element > than all elements present, then N is returned where N = cardinality of the
// container.
func (c container) find(x uint16) uint16 {
	N := c.get(indexCardinality)
	for i := startIdx; i < startIdx+N; i++ {
		if c[i] >= x {
			return i - startIdx
		}
	}
	return N
}

func (c container) add(x uint16) bool {
	idx := c.find(x)
	N := c.get(indexCardinality)
	offset := startIdx + idx
	if c[offset] == x {
		return false
	}

	if idx < N {
		// The entry at offset is the first entry, which is greater than x. Move it to the right.
		copy(c[offset+1:], c[offset:])
	}
	c[offset] = x
	c.set(indexCardinality, N+1)
	return true
}

func (c container) isFull() bool {
	N := c.get(indexCardinality)
	return int(N) >= len(c)-4
}

func (c container) all() []uint16 {
	N := c.get(indexCardinality)
	return c[startIdx : startIdx+N]
}

func (c container) String() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("Size: %d\n", c.get(indexSize)))
	for i, val := range c.data() {
		b.WriteString(fmt.Sprintf("%d: %d\n", i, val))
	}
	return b.String()
}
