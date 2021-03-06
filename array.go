package roar

var (
	pageSize = 512
)

// First 8 bytes should contain the length of the array.
type roaringArray []byte

func (ra roaringArray) newContainer(sz int) int {
	m := make([]byte, sz)
	offset := len(ra)
	ra = append(ra, m...)

	c := ra.getContainer(offset)
	c.set(indexSize, sz)
}

func (ra roaringArray) expandContainer(offset int, bySize int) {
	sz := getSize(ra[offset : offset+4])

	m := make([]byte, bySize)
	ra = append(ra, m...)

	left := offset + sz
	right := len(ra) - left
	copy(ra[left:], ra[right:])

	c := ra.getContainer(offset)
	c.set(indexSize, sz+bySize)
}

func (ra roaringArray) getContainer(offset int) container {
	data := ra[offset:]
	c := container(toUint16Slice(data))
	sz := c.get(indexSize)
	return c[:sz/2]
}
