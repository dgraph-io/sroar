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

func FromBuffer(data []byte) *roaringArray {
	if len(data) < 8 {
		return nil
	}
	x := toUint64Slice(data[:8])[0]
	return &roaringArray{
		data: data,
		keys: toUint64Slice(data[:x]),
	}
}

func NewRoaringArray(numKeys int) *roaringArray {
	if numKeys < 2 {
		panic("Must contain at least two keys.")
	}
	ra := &roaringArray{
		// Each key must also keep an offset. So, we need to double the number of uint64s allocated.
		// Plus, we need to make space for the first 2 uint64s to store the number of keys.
		data: make([]byte, sizeInBytesU64(2*numKeys+2)),
	}
	ra.keys = toUint64Slice(ra.data)
	ra.keys.setAt(0, uint64(len(ra.data)))

	// Always generate a container for key = 0x00. Otherwise, node gets confused about whether a
	// zero key is a new key or not.
	offset := ra.newContainer(minSizeOfContainer)
	ra.keys.setAt(3, offset) // First two are for num keys. index=2 -> 0 key. index=3 -> offset.
	ra.keys.setNumKeys(1)

	return ra
}

func (ra *roaringArray) setKey(k uint64, offset uint64) uint64 {
	if num := ra.keys.set(k, offset); num == 0 {
		// No new key was added. So, we can just return.
		return offset
	}
	// A new key was added. Let's ensure that ra.keys is not full.
	if !ra.keys.isFull() {
		return offset
	}

	// ra.keys is full. We should expand its size.
	curSize := uint64(len(ra.keys) * 8)
	bySize := curSize
	if bySize > math.MaxUint16 {
		bySize = math.MaxInt16
	}

	ra.scootRight(curSize, uint16(bySize))
	ra.keys = toUint64Slice(ra.data[:curSize+bySize])
	ra.keys.setAt(0, uint64(curSize+bySize))

	// All containers have moved to the right by bySize bytes.
	// Update their offsets.
	n := ra.keys
	for i := 0; i < n.maxKeys(); i++ {
		val := n.val(i)
		if val > 0 {
			n.setAt(valOffset(i), val+uint64(bySize))
		}
	}
	return offset + bySize
}

func (ra *roaringArray) fastExpand(bySize uint16) {
	prev := len(ra.keys) * 8
	ra.data = append(ra.data, empty[:bySize]...)

	// We should re-reference ra.keys correctly, because the underlying array might have been
	// switched after append.
	ra.keys = toUint64Slice(ra.data[:prev])
}

func (ra *roaringArray) scootRight(offset uint64, bySize uint16) {
	prevHash := z.MemHash(ra.data[:offset])
	left := ra.data[offset:]

	ra.fastExpand(bySize) // Expand the buffer.
	right := ra.data[len(ra.data)-len(left):]
	copy(right, left) // Move data right.

	z.Memclr(ra.data[offset : offset+uint64(bySize)]) // Zero out the space in the middle.
	if hash := z.MemHash(ra.data[:offset]); hash != prevHash {
		panic("We modified something")
	}
}

func (ra *roaringArray) newContainer(sz uint16) uint64 {
	offset := uint64(len(ra.data))
	ra.fastExpand(sz)
	setSize(ra.data[offset:], sz)
	return offset
}

func (ra *roaringArray) expandContainer(offset uint64) {
	sz := getSize(ra.data[offset : offset+2])
	if sz == 0 {
		panic("Container size should NOT be zero")
	}
	if sz >= 4096 {
		panic("Switch to a bitmap container")
	}
	fmt.Printf("expandContainer. offset: %d bySize: %d\n", offset, sz)

	// Select the portion to the right of the container, beyond its right boundary.
	ra.scootRight(offset+uint64(sz), sz)
	ra.keys.updateOffsets(offset, uint64(sz))

	setSize(ra.data[offset:], 2*sz)
	fmt.Printf("container offset: %d size: %d\n", offset, getSize(ra.data[offset:]))
}

func (ra roaringArray) getContainer(offset uint64) container {
	data := ra.data[offset:]
	sz := getSize(data)
	return toUint16Slice(data[:sz])
}

func (ra *roaringArray) Add(x uint64) bool {
	key := x & mask
	offset, has := ra.keys.getValue(key)
	if !has {
		// We need to add a container.
		o := uint64(ra.newContainer(minSize))

		// offset might have been updated by setKey.
		offset = ra.setKey(key, o)
	}
	c := ra.getContainer(offset)
	if added := c.add(uint16(x)); !added {
		return false
	}
	if c.isFull() {
		// Double the size of container for now.
		ra.expandContainer(offset)
	}
	return true
}
