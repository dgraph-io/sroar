package roar

import (
	"math"

	"github.com/dgraph-io/ristretto/z"
)

var (
	empty = make([]byte, 2<<16)
)

// First 8 bytes should contain the length of the array.
type roaringArray struct {
	data []byte
	keys node
}

func NewRoaringArray(numKeys int) *roaringArray {
	ra := &roaringArray{
		data: make([]byte, sizeInBytesU64(numKeys+2)),
	}
	ra.keys = toUint64Slice(ra.data[:len(ra.data)])
	return ra
}

func (ra *roaringArray) setKey(k uint64, offset uint64) {
	if num := ra.keys.set(k, offset); num == 0 {
		// No new key was added. So, we can just return.
		return
	}
	// A new key was added. Let's ensure that ra.keys is not full.
	if !ra.keys.isFull() {
		return
	}

	// ra.keys is full. We should expand its size.
	// TODO: Refactor this move stuff.
	curSize := len(ra.keys) * 8
	beyond := ra.data[curSize:]

	bySize := curSize
	if bySize > math.MaxUint16 {
		bySize = math.MaxInt16
	}
	fastExpand(ra.data, uint16(bySize))

	right := ra.data[len(ra.data)-len(beyond):]
	copy(right, beyond)
	z.ZeroOut(ra.data, curSize, curSize+bySize)

	ra.keys = toUint64Slice(ra.data[:curSize+bySize])

	// All containers have moved to the right by bySize bytes.
	// Update their offsets.
	n := ra.keys
	for i := 1; i < n.maxKeys(); i++ {
		if k := n.key(i); k > 0 {
			val := n.val(i)
			n.setAt(valOffset(i), val+uint64(bySize))
		}
	}
}

func fastExpand(ra []byte, bySize uint16) []byte {
	return append(ra, empty[:bySize]...)
}

func (ra *roaringArray) newContainer(sz uint16) int {
	offset := len(ra.data)
	ra.data = fastExpand(ra.data, sz)

	c := container(toUint16Slice(ra.data[offset : offset+int(sz)]))
	c.set(indexSize, uint16(sz))
	return offset
}

func (ra *roaringArray) expandContainer(offset int, bySize uint16) {
	sz := getSize(ra.data[offset : offset+4])

	// Select the portion to the right of the container, beyond its right boundary.
	from := offset + int(sz)
	beyond := ra.data[from:]

	// Expand the underlying buffer.
	ra.data = fastExpand(ra.data, bySize)

	// Move the beyond portion to the right, to make space for the container.
	right := ra.data[len(ra.data)-len(beyond):]
	copy(right, beyond)
	z.ZeroOut(ra.data, from, from+int(bySize))

	// Move other containers to the right.
	// TODO: We need to update their offsets in keys.go.

	c := container(toUint16Slice(ra.data[offset : offset+int(sz+bySize)]))
	c.set(indexSize, uint16(sz+bySize))
}

func (ra roaringArray) getContainer(offset int) container {
	data := ra.data[offset:]
	c := container(toUint16Slice(data))
	sz := c.get(indexSize)
	return c[:sz/2]
}
