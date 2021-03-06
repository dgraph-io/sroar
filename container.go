package roar

type container []uint16

const (
	typeArray  uint16 = 0x00
	typeBitmap uint16 = 0x01

	indexType        int = 0
	indexSize        int = 1 // in bytes.
	indexCardinality int = 2
	indexUnused      int = 3
)

func getSize(data []byte) int {
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
