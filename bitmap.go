package roar

import (
	"math"

	"github.com/dgraph-io/ristretto/z"
)

var (
	empty   = make([]byte, 2<<16)
	minSize = uint16(32)
)

const mask = uint64(0xFFFFFFFFFFFF0000)

// First 8 bytes should contain the length of the array.
type Bitmap struct {
	data []byte
	keys node
}

func FromBuffer(data []byte) *Bitmap {
	if len(data) < 8 {
		return nil
	}
	x := toUint64Slice(data[:8])[0]
	return &Bitmap{
		data: data,
		keys: toUint64Slice(data[:x]),
	}
}

func NewBitmap() *Bitmap {
	return NewBitmapWith(2)
}
func NewBitmapWith(numKeys int) *Bitmap {
	if numKeys < 2 {
		panic("Must contain at least two keys.")
	}
	ra := &Bitmap{
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

// setKey sets a key and container offset.
func (ra *Bitmap) setKey(k uint64, offset uint64) uint64 {
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

func (ra *Bitmap) fastExpand(bySize uint16) {
	prev := len(ra.keys) * 8
	ra.data = append(ra.data, empty[:bySize]...)

	// We should re-reference ra.keys correctly, because the underlying array might have been
	// switched after append.
	ra.keys = toUint64Slice(ra.data[:prev])
}

func (ra *Bitmap) scootRight(offset uint64, bySize uint16) {
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

func (ra *Bitmap) newContainer(sz uint16) uint64 {
	offset := uint64(len(ra.data))
	ra.fastExpand(sz)
	setSize(ra.data[offset:], sz)
	return offset
}

func (ra *Bitmap) expandContainer(offset uint64) {
	sz := getSize(ra.data[offset : offset+2])
	if sz == 0 {
		panic("Container size should NOT be zero")
	}
	bySize := sz
	if sz >= 4096 {
		bySize = maxSizeOfContainer - sz
	}
	// fmt.Printf("expandContainer. offset: %d bySize: %d\n", offset, sz)

	// Select the portion to the right of the container, beyond its right boundary.
	ra.scootRight(offset+uint64(sz), bySize)
	ra.keys.updateOffsets(offset, uint64(sz))

	if sz < 4096 {
		setSize(ra.data[offset:], sz+bySize)

	} else {
		// Convert to bitmap container.
		buf := make([]byte, maxSizeOfContainer)
		b := bitmap(toUint16Slice(buf))
		b[indexSize] = maxSizeOfContainer
		b[indexType] = typeBitmap

		src := packed(ra.getContainer(offset))
		for _, x := range src.all() {
			b.add(x)
		}
		assert(copy(ra.data[offset:], buf) == maxSizeOfContainer)
	}
	// fmt.Printf("container offset: %d size: %d\n", offset, getSize(ra.data[offset:]))
}

func (ra Bitmap) getContainer(offset uint64) []uint16 {
	data := ra.data[offset:]
	sz := getSize(data)
	return toUint16Slice(data[:sz])
}

func (ra *Bitmap) Add(x uint64) bool {
	key := x & mask
	offset, has := ra.keys.getValue(key)
	if !has {
		// We need to add a container.
		o := uint64(ra.newContainer(minSize))

		// offset might have been updated by setKey.
		offset = ra.setKey(key, o)
	}
	c := ra.getContainer(offset)
	switch c[indexType] {
	case typeArray:
		p := packed(c)
		if added := p.add(uint16(x)); !added {
			return false
		}
		if p.isFull() {
			// Double the size of container for now.
			ra.expandContainer(offset)
		}
	case typeBitmap:
		b := bitmap(c)
		return b.add(uint16(x))
	}
	return true
}

func (ra *Bitmap) Has(x uint64) bool {
	key := x & mask
	offset, has := ra.keys.getValue(key)
	if !has {
		return false
	}
	y := uint16(x)

	c := ra.getContainer(offset)
	switch c[indexType] {
	case typeArray:
		p := packed(c)
		return p.has(y)
	case typeBitmap:
		b := bitmap(c)
		return b.has(y)
	}
	return false
}

func containerAnd(ac, bc []uint16) []uint16 {
	at := ac[indexType]
	bt := bc[indexType]

	if at == typeArray && bt == typeArray {
		left := packed(ac)
		right := packed(bc)
		return left.and(right)
	}
	if at == typeArray && bt == typeBitmap {
		left := packed(ac)
		right := bitmap(bc)
		return left.andBitmap(right)
	}
	if at == typeBitmap && bt == typeArray {
		left := bitmap(ac)
		right := packed(bc)
		return right.andBitmap(left)
	}
	if at == typeBitmap && bt == typeBitmap {
		left := bitmap(ac)
		right := bitmap(bc)
		return left.and(right)
	}
	panic("containerAnd: We should not reach here")
}

func And(a, b *Bitmap) *Bitmap {
	ai, an := 0, a.keys.numKeys()
	bi, bn := 0, b.keys.numKeys()

	res := NewBitmap()
	for ai < an && bi < bn {
		ak := a.keys.key(ai)
		bk := a.keys.key(bi)
		if ak == bk {
			// Do the intersection.
			off, has := a.keys.getValue(ak)
			assert(has)
			ac := a.getContainer(off)

			off, has = b.keys.getValue(bk)
			assert(has)
			bc := b.getContainer(off)

			outc := containerAnd(ac, bc)
			if outc[indexCardinality] > 0 {
				outb := toByteSlice(containerAnd(ac, bc))
				offset := res.newContainer(uint16(len(outb)))
				copy(res.data[offset:], outb)
				res.setKey(ak, offset)
			}
			ai++
			bi++
		} else if ak < bk {
			ai++
			ak = a.keys.key(ai)
		} else {
			bi++
			bk = b.keys.key(bi)
		}
	}
	return res
}
