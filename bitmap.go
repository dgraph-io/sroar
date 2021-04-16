/*
 * Copyright 2021 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package roar

import (
	"fmt"
	"math"
	"strings"

	"github.com/dgraph-io/dgo/x"
	"github.com/dgraph-io/ristretto/z"
)

var (
	empty          = make([]byte, 1<<25)
	minSize        = uint16(32)
	indexTotalSize = 0
	indexNumKeys   = 1
	indexOffset    = 2
	indexUnused    = 3
	indexStart     = 4
)

const mask = uint64(0xFFFFFFFFFFFF0000)

// First 8 bytes should contain the length of the array.
type Bitmap struct {
	// TODO: Make data []uint16 by default instead of byte slice.
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
		data: make([]byte, sizeInBytesU64(2*numKeys+4)),
	}
	ra.keys = toUint64Slice(ra.data)
	ra.keys.setAt(indexTotalSize, uint64(len(ra.data)))

	ra.keys[indexOffset] = 64 // for the first 8 uint64s (header 4 + 2kv pair)
	// Always generate a container for key = 0x00. Otherwise, node gets confused about whether a
	// zero key is a new key or not.
	offset := ra.newContainer(maxSizeOfContainer)
	ra.keys.setAt(indexStart+1, offset) // First two are for num keys. index=2 -> 0 key. index=3 -> offset.
	ra.keys.setNumKeys(indexNumKeys)

	return ra
}

// setKey sets a key and container offset.
func (ra *Bitmap) setKey(k uint64, offset uint64) uint64 {
	if added := ra.keys.set(k, offset); !added {
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

func (ra *Bitmap) NumExpand() uint64 {
	return ra.keys[indexUnused]
}

func (ra *Bitmap) fastExpand(bySize uint16) {
	if len(ra.data) > int(ra.keys[indexOffset])+int(bySize) {
		// No need to expand, we have sufficient space.
		return
	}
	ra.keys[indexUnused]++
	expandSz := int(bySize)
	if expandSz < len(ra.data) {
		expandSz = len(ra.data)
	}
	prev := len(ra.keys) * 8
	ra.data = append(ra.data, empty[:expandSz]...)

	// We should re-reference ra.keys correctly, because the underlying array might have been
	// switched after append.
	ra.keys = toUint64Slice(ra.data[:prev])
}

func (ra *Bitmap) scootRight(offset uint64, bySize uint16) {
	dataOffset := ra.keys[indexOffset]
	left := ra.data[offset:dataOffset]
	prevHash := z.MemHash(left)

	ra.fastExpand(bySize) // Expand the buffer.
	right := ra.data[offset+uint64(bySize) : dataOffset+uint64(bySize)]
	copy(right, left) // Move data right.
	afterHash := z.MemHash(right)

	z.Memclr(ra.data[offset : offset+uint64(bySize)]) // Zero out the space in the middle.
	if afterHash != prevHash {
		panic("We modified something")
	}
	ra.keys[indexOffset] += uint64(bySize)
}

func (ra *Bitmap) newContainer(sz uint16) uint64 {
	// offset := uint64(len(ra.data))
	offset := ra.keys[indexOffset]
	ra.fastExpand(sz)
	setSize(ra.data[offset:], sz)
	ra.keys[indexOffset] += uint64(sz)
	x.AssertTrue(sz == getSize(ra.data[offset:]))
	return offset
}

func (ra *Bitmap) expandContainer(offset uint64) {
	sz := getSize(ra.data[offset : offset+2])
	if sz == 0 {
		panic("Container size should NOT be zero")
	}
	bySize := uint16(sz)
	if sz >= 4096 {
		bySize = maxSizeOfContainer - sz
	}

	// Select the portion to the right of the container, beyond its right boundary.
	ra.scootRight(offset+uint64(sz), bySize)
	ra.keys.updateOffsets(offset, uint64(bySize))

	if sz < 4096 {
		setSize(ra.data[offset:], sz+bySize)

	} else {
		// Convert to bitmap container.
		src := array(ra.getContainer(offset))
		buf := toByteSlice(src.toBitmapContainer())
		assert(copy(ra.data[offset:], buf) == maxSizeOfContainer)
	}
}

func (ra Bitmap) getContainer(offset uint64) []uint16 {
	data := ra.data[offset:ra.keys[indexOffset]]
	sz := getSize(data)
	return toUint16Slice(data[:sz])
}

func (ra *Bitmap) Set(x uint64) bool {
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
		p := array(c)
		if added := p.add(uint16(x)); !added {
			return false
		}
		if p.isFull() {
			// Double the size of container for now.
			ra.expandContainer(offset)
		}
		return true
	case typeBitmap:
		b := bitmap(c)
		return b.add(uint16(x))
	}
	panic("we shouldn't reach here")
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
		p := array(c)
		return p.has(y)
	case typeBitmap:
		b := bitmap(c)
		return b.has(y)
	}
	return false
}

func (ra *Bitmap) GetCardinality() int {
	N := ra.keys.numKeys()
	var sz int
	for i := 0; i < N; i++ {
		offset := ra.keys.val(i)
		c := ra.getContainer(offset)
		// sz += int(c[indexCardinality])
		sz += getCardinality(c)
	}
	return sz
}

func (ra *Bitmap) ToArray() []uint64 {
	var res []uint64
	N := ra.keys.numKeys()
	for i := 0; i < N; i++ {
		key := ra.keys.key(i)
		off := ra.keys.val(i)
		c := ra.getContainer(off)

		switch c[indexType] {
		case typeArray:
			a := array(c)
			for _, lo := range a.all() {
				res = append(res, key|uint64(lo))
			}
		case typeBitmap:
			b := bitmap(c)
			out := b.ToArray()
			for _, x := range out {
				res = append(res, key|uint64(x))
			}
		}
	}
	return res
}

func (ra *Bitmap) String() string {
	var b strings.Builder
	b.WriteRune('\n')

	for i := 0; i < ra.keys.numKeys(); i++ {
		k := ra.keys.key(i)
		v := ra.keys.val(i)

		c := ra.getContainer(v)
		b.WriteString(fmt.Sprintf(
			"[%d] key: %#x Container [offset: %d, Size: %d, Type: %d, Card: %d]\n",
			i, k, v, c[indexSize], c[indexType], c[indexCardinality]))
	}
	return b.String()
}

const fwd int = 0x01
const rev int = 0x02

func (ra *Bitmap) Minimum() uint64 { return ra.extreme(fwd) }
func (ra *Bitmap) Maximum() uint64 { return ra.extreme(rev) }

func (ra *Bitmap) Debug(x uint64) string {
	var b strings.Builder
	hi := x & mask
	off, found := ra.keys.getValue(hi)
	if !found {
		b.WriteString(fmt.Sprintf("Unable to find the container for x: %#x\n", hi))
		b.WriteString(ra.String())
	}
	c := ra.getContainer(off)
	lo := uint16(x)

	b.WriteString(fmt.Sprintf("x: %#x lo: %#x. offset: %d\n", x, lo, off))

	switch c[indexType] {
	case typeArray:
	case typeBitmap:
		idx := lo / 16
		pos := lo % 16
		b.WriteString(fmt.Sprintf("At idx: %d. Pos: %d val: %#b\n", idx, pos, c[startIdx+idx]))
	}
	return b.String()
}

func (ra *Bitmap) extreme(dir int) uint64 {
	N := ra.keys.numKeys()
	if N == 0 {
		return 0
	}
	k := ra.keys.key(0)
	offset := ra.keys.val(0)
	c := ra.getContainer(offset)

	switch c[indexType] {
	case typeArray:
		a := array(c)
		if dir == fwd {
			return k | uint64(a.minimum())
		}
		return k | uint64(a.maximum())
	case typeBitmap:
		b := bitmap(c)
		if dir == fwd {
			return k | uint64(b.minimum())
		}
		return k | uint64(b.maximum())
	default:
		panic("We don't support this type of container")
	}
}

func containerAnd(ac, bc []uint16) []uint16 {
	at := ac[indexType]
	bt := bc[indexType]

	if at == typeArray && bt == typeArray {
		left := array(ac)
		right := array(bc)
		return left.andArray(right)
	}
	if at == typeArray && bt == typeBitmap {
		left := array(ac)
		right := bitmap(bc)
		return left.andBitmap(right)
	}
	if at == typeBitmap && bt == typeArray {
		left := bitmap(ac)
		right := array(bc)
		out := right.andBitmap(left)
		return out
	}
	if at == typeBitmap && bt == typeBitmap {
		left := bitmap(ac)
		right := bitmap(bc)
		return left.and(right)
	}
	panic("containerAnd: We should not reach here")
}

func containerOr(ac, bc []uint16) []uint16 {
	at := ac[indexType]
	bt := bc[indexType]

	if at == typeArray && bt == typeArray {
		left := array(ac)
		right := array(bc)
		return left.orArray(right)
	}
	if at == typeArray && bt == typeBitmap {
		left := array(ac)
		right := bitmap(bc)
		return right.orArray(left)
	}
	if at == typeBitmap && bt == typeArray {
		left := bitmap(ac)
		right := array(bc)
		return left.orArray(right)
	}
	if at == typeBitmap && bt == typeBitmap {
		left := bitmap(ac)
		right := bitmap(bc)
		return left.orBitmap(right)
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
			off := a.keys.val(ai)
			ac := a.getContainer(off)

			off = b.keys.val(bi)
			bc := b.getContainer(off)

			outc := containerAnd(ac, bc)
			// if outc[indexCardinality] > 0 {
			if getCardinality(outc) > 0 {
				outb := toByteSlice(outc)
				offset := res.newContainer(uint16(len(outb)))
				copy(res.data[offset:], outb)
				res.setKey(ak, offset)
			}
			ai++
			bi++
		} else if ak < bk {
			ai++
		} else {
			bi++
		}
	}
	return res
}

func Or(a, b *Bitmap) *Bitmap {
	ai, an := 0, a.keys.numKeys()
	bi, bn := 0, b.keys.numKeys()

	res := NewBitmap()
	for ai < an && bi < bn {
		ak := a.keys.key(ai)
		ac := a.getContainer(a.keys.val(ai))

		bk := b.keys.key(bi)
		bc := b.getContainer(b.keys.val(bi))

		if ak == bk {
			// Do the union.
			outc := containerOr(ac, bc)
			outb := toByteSlice(outc)
			offset := res.newContainer(uint16(len(outb)))
			copy(res.data[offset:], outb)
			res.setKey(ak, offset)
			ai++
			bi++
		} else if ak < bk {
			off := res.newContainer(uint16(sizeInBytesU16(len(ac))))
			copy(res.getContainer(off), ac)
			res.setKey(ak, off)
			ai++
		} else {
			off := res.newContainer(uint16(sizeInBytesU16(len(bc))))
			copy(res.getContainer(off), bc)
			res.setKey(bk, off)
			bi++
		}
	}
	for ai < an {
		ak := a.keys.key(ai)
		ac := a.getContainer(a.keys.val(ai))
		off := res.newContainer(uint16(sizeInBytesU16(len(ac))))

		copy(res.getContainer(off), ac)
		res.setKey(ak, off)
		ai++
	}
	for bi < bn {
		bk := b.keys.key(bi)
		bc := b.getContainer(b.keys.val(bi))
		off := res.newContainer(uint16(sizeInBytesU16(len(bc))))

		copy(res.getContainer(off), bc)
		res.setKey(bk, off)
		bi++
	}
	return res
}

func FastAnd(bitmaps ...*Bitmap) *Bitmap {
	b := NewBitmap()
	if len(bitmaps) == 0 {
		return b
	} else if len(bitmaps) == 1 {
		// TODO: Need a clone method here.
		return bitmaps[0]
	}
	b = And(bitmaps[0], bitmaps[1])
	for _, bm := range bitmaps[2:] {
		b = And(b, bm)
	}
	return b
}

func FastOr(bitmaps ...*Bitmap) *Bitmap {
	b := NewBitmap()
	if len(bitmaps) == 0 {
		return b
	} else if len(bitmaps) == 1 {
		return bitmaps[0]
	}
	b = Or(bitmaps[0], bitmaps[1])
	for _, bm := range bitmaps[2:] {
		b = Or(b, bm)
	}
	return b
}
