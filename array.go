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
	if numKeys < 2 {
		panic("Must contain at least two keys.")
	}
	ra := &roaringArray{
		// Each key must also keep an offset. So, we need to double the number of uint64s allocated.
		// Plus, we need to make space for the first 2 uint64s to store the number of keys.
		data: make([]byte, sizeInBytesU64(2*numKeys+2)),
	}
	ra.keys = toUint64Slice(ra.data)

	// Always generate a container for key = 0x00. Otherwise, node gets confused about whether a
	// zero key is a new key or not.
	offset := ra.newContainer(minSizeOfContainer)
	ra.keys.setAt(3, offset) // First two are for num keys. index=2 -> 0 key. index=3 -> offset.
	ra.keys.setNumKeys(1)

	fmt.Printf("len keys: %d len data: %d\n", len(ra.keys), len(ra.data))
	return ra
}

func (ra *roaringArray) setKey(k uint64, offset uint64) uint64 {
	fmt.Printf("len of keys node: %d\n", len(ra.keys))
	if num := ra.keys.set(k, offset); num == 0 {
		// No new key was added. So, we can just return.
		return offset
	}
	fmt.Printf("setKey: %d offset: %d. Added one\n", k, offset)
	// A new key was added. Let's ensure that ra.keys is not full.
	if !ra.keys.isFull() {
		return offset
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
	fmt.Printf("max keys: %d\n", n.maxKeys())
	for i := 0; i < n.maxKeys(); i++ {
		k := n.key(i)
		val := n.val(i)
		fmt.Printf("i: %d key: %d val: %d\n", i, k, val)
		if val > 0 {
			fmt.Printf("Moving. i: %d key: %d val: %d\n", i, k, val+uint64(bySize))
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
	copy(right, left)                                 // Move data right.
	z.Memclr(ra.data[offset : offset+uint64(bySize)]) // Zero out the space in the middle.
	if hash := z.MemHash(ra.data[:offset]); hash != prevHash {
		panic("We modified something")
	}
}

func (ra *roaringArray) newContainer(sz uint16) uint64 {
	offset := uint64(len(ra.data))
	ra.fastExpand(sz)

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
	fmt.Printf("\nAdd: %d. Key: %d\n", x, key)
	offset, has := ra.keys.getValue(key)
	fmt.Printf("add: %d. offset: %d\n", x, offset)
	if !has {
		// We need to add a container.
		o := uint64(ra.newContainer(minSize))

		// offset might have been updated by setKey.
		offset = ra.setKey(key, o)
		fmt.Printf("key has been set: %d\n", key)
	}
	fmt.Printf("got offset: %d\n", offset)
	c := ra.getContainer(offset)

	// TODO: Set the keys in there.
	num := c.get(indexCardinality)
	c.set(indexCardinality, num+1)
	fmt.Printf("container at offset: %d. num: %d\n", offset, num+1)
}
