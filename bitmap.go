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

package sroar

import (
	"fmt"
	"math"
	"strings"
)

var (
	empty          = make([]uint16, 16<<20)
	minSize        = uint16(32)
	indexTotalSize = 0
	indexNumKeys   = 1
	// index 2 and 3 are unused now.
	indexStart = 4
)

const mask = uint64(0xFFFFFFFFFFFF0000)

// First uint64 contains the length of the node.
type Bitmap struct {
	data []uint16
	keys node
}

func FromBuffer(data []byte) *Bitmap {
	if len(data) < 8 {
		return nil
	}
	du := toUint16Slice(data)
	x := toUint64Slice(du[:4])[0]
	return &Bitmap{
		data: du,
		keys: toUint64Slice(du[:x]),
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
		// Each key must also keep an offset. So, we need to double the number
		// of uint64s allocated. Plus, we need to make space for the first 2
		// uint64s to store the number of keys.
		data: make([]uint16, 4*(2*numKeys+4)),
	}
	ra.keys = toUint64Slice(ra.data)
	ra.keys.setAt(indexTotalSize, uint64(len(ra.data)))

	// Always generate a container for key = 0x00. Otherwise, node gets confused
	// about whether a zero key is a new key or not.
	offset := ra.newContainer(minSizeOfContainer)
	// First two are for num keys. index=2 -> 0 key. index=3 -> offset.
	ra.keys.setAt(indexStart+1, offset)
	ra.keys.setNumKeys(1)

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
	curSize := uint64(len(ra.keys) * 4) // Multiply by 4 for U64 -> U16.
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
	prev := len(ra.keys) * 4 // Multiply by 4 to convert from u16 to u64.
	ra.data = append(ra.data, empty[:bySize]...)

	// We should re-reference ra.keys correctly, because the underlying array might have been
	// switched after append.
	ra.keys = toUint64Slice(ra.data[:prev])
}

func (ra *Bitmap) scootRight(offset uint64, bySize uint16) {
	left := ra.data[offset:]
	// prevHash := z.MemHash(left)

	ra.fastExpand(bySize) // Expand the buffer.
	right := ra.data[len(ra.data)-len(left):]
	copy(right, left) // Move data right.
	// afterHash := z.MemHash(right)

	Memclr(ra.data[offset : offset+uint64(bySize)]) // Zero out the space in the middle.
	// if afterHash != prevHash {
	// 	panic("We modified something")
	// }
}

func (ra *Bitmap) newContainer(sz uint16) uint64 {
	offset := uint64(len(ra.data))
	ra.fastExpand(sz)
	ra.data[offset] = sz
	return offset
}

// expandContainer would expand a container at the given offset. It would typically double the size
// of the container, until it reaches a threshold, where the size of the container would reach 2^16.
// Expressed in uint16s, that'd be (2^16)/(2^4) = 2^12 = 4096. So, if the container size >= 2048,
// then doubling that would put it above 4096. That's why in the code below, you see the checks for
// size 2048.
func (ra *Bitmap) expandContainer(offset uint64) {
	sz := ra.data[offset]
	if sz == 0 {
		panic("Container size should NOT be zero")
	}
	bySize := uint16(sz)
	if sz >= 2048 {
		// Size is in uint16. Half of max allowed size. If we're expanding the container by more
		// than 2048, we should just cap it to max size of 4096.
		bySize = maxSizeOfContainer - sz
	}

	// Select the portion to the right of the container, beyond its right boundary.
	ra.scootRight(offset+uint64(sz), bySize)
	ra.keys.updateOffsets(offset, uint64(bySize))

	if sz < 2048 {
		setSize(ra.data[offset:], sz+bySize)

	} else {
		// Convert to bitmap container.
		src := array(ra.getContainer(offset))
		buf := src.toBitmapContainer()
		assert(copy(ra.data[offset:], buf) == maxSizeOfContainer)
	}
}

func (ra Bitmap) getContainer(offset uint64) []uint16 {
	data := ra.data[offset:]
	if len(data) == 0 {
		panic(fmt.Sprintf("No container found at offset: %d\n", offset))
	}
	sz := data[0]
	return data[:sz]
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

func (ra *Bitmap) Remove(x uint64) bool {
	key := x & mask
	offset, has := ra.keys.getValue(key)
	if !has {
		return false
	}
	c := ra.getContainer(offset)
	switch c[indexType] {
	case typeArray:
		p := array(c)
		return p.remove(uint16(x))
	case typeBitmap:
		b := bitmap(c)
		return b.remove(uint16(x))
	}
	return true
}

// TODO: optimize this function. Also, introduce scootLeft probably.
func (ra *Bitmap) RemoveRange(lo, hi uint64) {
	if lo > hi {
		panic("lo should not be more than hi")
	}
	k1 := lo >> 16
	k2 := hi >> 16

	for k := k1 + 1; k < k2; k++ {
		key := k << 16
		_, has := ra.keys.getValue(key)
		if has {
			off := ra.newContainer(minSizeOfContainer)
			ra.setKey(key, off)
		}
	}
	for x := lo; x <= hi; x++ {
		k := x >> 16
		if k == k1 {
			ra.Remove(x)
		} else {
			break
		}
	}
	for x := hi; x >= lo; x-- {
		k := x >> 16
		if k == k2 {
			ra.Remove(x)
		} else {
			break
		}
	}
}

func (ra *Bitmap) GetCardinality() int {
	N := ra.keys.numKeys()
	var sz int
	for i := 0; i < N; i++ {
		offset := ra.keys.val(i)
		c := ra.getContainer(offset)
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
		return left.andBitmap(right)
	}
	panic("containerAnd: We should not reach here")
}

func containerAndNot(ac, bc []uint16) []uint16 {
	at := ac[indexType]
	bt := bc[indexType]

	if at == typeArray && bt == typeArray {
		left := array(ac)
		right := array(bc)
		return left.andNotArray(right)
	}
	if at == typeArray && bt == typeBitmap {
		left := array(ac)
		right := bitmap(bc)
		return left.andNotBitmap(right)
	}
	if at == typeBitmap && bt == typeArray {
		left := bitmap(ac)
		right := array(bc)
		out := right.andNotBitmap(left)
		return out
	}
	if at == typeBitmap && bt == typeBitmap {
		left := bitmap(ac)
		right := bitmap(bc)
		return left.andNotBitmap(right)
	}
	panic("containerAndNot: We should not reach here")
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

func (ra *Bitmap) And(bm *Bitmap) {
	a, b := ra, bm
	ai, an := 0, a.keys.numKeys()
	bi, bn := 0, b.keys.numKeys()

	for ai < an && bi < bn {
		ak := a.keys.key(ai)
		bk := a.keys.key(bi)
		if ak == bk {
			off := a.keys.val(ai)
			ac := a.getContainer(off)

			off = b.keys.val(bi)
			bc := b.getContainer(off)

			// do the intersection
			c := containerAnd(ac, bc)

			// create a new container and update the key offset to this container.
			offset := a.newContainer(uint16(len(c)))
			copy(a.data[offset:], c)
			a.setKey(ak, offset)
			ai++
			bi++
		} else if ak < bk {
			// need to remove the container of a
			off := a.newContainer(minSizeOfContainer)
			a.setKey(ak, off)
			ai++
		} else {
			bi++
		}
	}
	for ai < an {
		off := a.newContainer(minSizeOfContainer)
		a.setKey(a.keys.key(ai), off)
		ai++
	}
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
			if getCardinality(outc) > 0 {
				offset := res.newContainer(uint16(len(outc)))
				copy(res.data[offset:], outc)
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

func (ra *Bitmap) AndNot(bm *Bitmap) {
	a, b := ra, bm
	ai, an := 0, a.keys.numKeys()
	bi, bn := 0, b.keys.numKeys()

	for ai < an && bi < bn {
		ak := a.keys.key(ai)
		bk := b.keys.key(bi)
		if ak == bk {
			off := a.keys.val(ai)
			ac := a.getContainer(off)

			off = b.keys.val(bi)
			bc := b.getContainer(off)

			// do the intersection
			c := containerAndNot(ac, bc)

			// create a new container and update the key offset to this container.
			offset := a.newContainer(uint16(len(c)))
			copy(a.data[offset:], c)
			a.setKey(ak, offset)
			ai++
			bi++
		} else if ak < bk {
			// nothing to be done
			ai++
		} else {
			// ak > bk
			// need to add this b container to a
			bk := b.keys.key(bi)
			off := b.keys.val(bi)
			bc := b.getContainer(off)

			offset := a.newContainer(uint16(len(bc)))
			copy(a.data[offset:], bc)
			a.setKey(bk, offset)
			bi++
		}
	}

	// pick up all the keys left in b.
	for bi < bn {
		bk := b.keys.key(bi)
		off := b.keys.val(bi)
		bc := b.getContainer(off)

		offset := a.newContainer(uint16(len(bc)))
		copy(a.data[offset:], bc)
		a.setKey(bk, offset)
		bi++
	}
}

func (ra *Bitmap) Or(bm *Bitmap) {
	bi, bn := 0, bm.keys.numKeys()
	a, b := ra, bm

	for bi < bn {
		bk := b.keys.key(bi)
		bc := b.getContainer(b.keys.val(bi))
		idx := a.keys.search(bk)

		// bk is not in a, just add container corresponding to bk to a.
		if idx >= a.keys.numKeys() || a.keys.key(idx) != bk {
			offset := a.newContainer(uint16(len(toByteSlice(bc))))
			copy(a.getContainer(offset), bc)
			a.setKey(bk, offset)
		} else {
			// bk is also present in a, do a container or.
			//TODO: Need to cleanup the old container in a.
			ac := a.getContainer(a.keys.val(idx))
			c := containerOr(ac, bc)
			offset := a.newContainer(uint16(len(toByteSlice(c))))
			copy(a.getContainer(offset), c)
			a.setKey(bk, offset)
		}
		bi++
	}
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
			offset := res.newContainer(uint16(len(outc)))
			copy(res.data[offset:], outc)
			res.setKey(ak, offset)
			ai++
			bi++
		} else if ak < bk {
			off := res.newContainer(uint16(len(ac)))
			copy(res.getContainer(off), ac)
			res.setKey(ak, off)
			ai++
		} else {
			off := res.newContainer(uint16(len(bc)))
			copy(res.getContainer(off), bc)
			res.setKey(bk, off)
			bi++
		}
	}
	for ai < an {
		ak := a.keys.key(ai)
		ac := a.getContainer(a.keys.val(ai))
		off := res.newContainer(uint16(len(ac)))

		copy(res.getContainer(off), ac)
		res.setKey(ak, off)
		ai++
	}
	for bi < bn {
		bk := b.keys.key(bi)
		bc := b.getContainer(b.keys.val(bi))
		off := res.newContainer(uint16(len(bc)))

		copy(res.getContainer(off), bc)
		res.setKey(bk, off)
		bi++
	}
	return res
}

func FastAnd(bitmaps ...*Bitmap) *Bitmap {
	if len(bitmaps) == 0 {
		return NewBitmap()
	}
	b := bitmaps[0]
	for _, bm := range bitmaps[1:] {
		b.And(bm)
	}
	return b
}

func FastOr(bitmaps ...*Bitmap) *Bitmap {
	b := NewBitmap()
	for _, bm := range bitmaps {
		b.Or(bm)
	}
	return b
}
