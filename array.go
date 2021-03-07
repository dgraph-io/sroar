package roar

import (
	"fmt"
	"math"

	"github.com/dgraph-io/ristretto/z"
)

var (
	empty   = make([]byte, 2<<16)
	minSize = uint16(32)
)

const mask = uint64(0xFFFFFFFFFFFF0000)

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

// getKey returns the offset for container corresponding to key k.
func (ra *roaringArray) getKey(k uint64) uint64 {
	return ra.keys.get(k)
}

func (ra *roaringArray) setKey(k uint64, offset uint64) {
	if num := ra.keys.set(k, offset); num == 0 {
		// No new key was added. So, we can just return.
		return
	}
	fmt.Printf("setKey: %d offset: %d. Added one\n", k, offset)
	// A new key was added. Let's ensure that ra.keys is not full.
	if !ra.keys.isFull() {
		return
	}

	fmt.Printf("keys are full\n")
	// ra.keys is full. We should expand its size.
	// TODO: Refactor this move stuff.

	curSize := uint64(len(ra.keys) * 8)
	bySize := curSize
	if bySize > math.MaxUint16 {
		bySize = math.MaxInt16
	}
	ra.scootRight(curSize, uint16(bySize))

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

func (ra *roaringArray) scootRight(offset uint64, bySize uint16) {
	left := ra.data[offset:]
	ra.data = fastExpand(ra.data, bySize) // Expand the buffer.
	right := ra.data[len(ra.data)-len(left):]
	copy(right, left)                                 // Move data right.
	z.Memclr(ra.data[offset : offset+uint64(bySize)]) // Zero out the space in the middle.
}

func (ra *roaringArray) newContainer(sz uint16) uint64 {
	offset := uint64(len(ra.data))
	ra.data = fastExpand(ra.data, sz)

	c := container(toUint16Slice(ra.data[offset : offset+uint64(sz)]))
	c.set(indexSize, uint16(sz))
	return offset
}

func (ra *roaringArray) expandContainer(offset uint64, bySize uint16) {
	sz := getSize(ra.data[offset : offset+4])

	// Select the portion to the right of the container, beyond its right boundary.
	ra.scootRight(offset+uint64(sz), bySize)

	// TODO: We need to update their offsets in keys.go.

	c := container(toUint16Slice(ra.data[offset : offset+uint64(sz+bySize)]))
	c.set(indexSize, uint16(sz+bySize))
}

func (ra roaringArray) getContainer(offset uint64) container {
	data := ra.data[offset:]
	c := container(toUint16Slice(data))
	sz := c.get(indexSize)
	return c[:sz/2]
}

func (ra *roaringArray) Add(x uint64) {
	key := x & mask
	fmt.Printf("Add: %d. Key: %d\n", x, key)
	offset := ra.getKey(key)
	fmt.Printf("add: %d. offset: %d\n", x, offset)
	if offset == 0 {
		// We need to add a container.
		offset = uint64(ra.newContainer(minSize))
		fmt.Printf("offset: %d\n", offset)
		ra.setKey(key, offset)
	}
	c := ra.getContainer(offset)

	// TODO: Set the keys in there.
	num := c.get(indexCardinality)
	c.set(indexCardinality, num+1)
	fmt.Printf("container at offset: %d. num: %d\n", offset, num+1)
}
