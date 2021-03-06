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
	return c[4:]
}

func (c container) String() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("Size: %d\n", c.get(indexSize)))
	for i, val := range c.data() {
		b.WriteString(fmt.Sprintf("%d: %d\n", i, val))
	}
	return b.String()
}
