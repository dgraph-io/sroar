package roar

import (
	"github.com/dgraph-io/ristretto/z"
)

var (
	pageSize = 512
	empty    = make([]byte, 2<<16)
)

// First 8 bytes should contain the length of the array.
type roaringArray []byte

func fastExpand(ra []byte, bySize uint16) []byte {
	return append(ra, empty[:bySize]...)
}

func (ra *roaringArray) newContainer(sz uint16) int {
	offset := len(*ra)
	*ra = fastExpand(*ra, sz)

	c := container(toUint16Slice((*ra)[offset : offset+int(sz)]))
	c.set(indexSize, uint16(sz))
	return offset
}

func (ra *roaringArray) expandContainer(offset int, bySize uint16) {
	sz := getSize((*ra)[offset : offset+4])

	// Select the portion to the right of the container, beyond its right boundary.
	from := offset + int(sz)
	beyond := (*ra)[from:]

	// Expand the underlying buffer.
	*ra = fastExpand(*ra, bySize)

	// Move the beyond portion to the right, to make space for the container.
	right := (*ra)[len(*ra)-len(beyond):]
	copy(right, beyond)
	z.ZeroOut(*ra, from, from+int(bySize))

	// Move other containers to the right.
	// TODO: We need to update their offsets in keys.go.

	c := container(toUint16Slice((*ra)[offset : offset+int(sz+bySize)]))
	c.set(indexSize, uint16(sz+bySize))
}

func (ra roaringArray) getContainer(offset int) container {
	data := ra[offset:]
	c := container(toUint16Slice(data))
	sz := c.get(indexSize)
	return c[:sz/2]
}
