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
	"sync"

	"github.com/pkg/errors"
)

var (
	empty          = make([]uint16, 16<<20)
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

	// memMoved keeps track of how many uint16 moves we had to do. The smaller
	// this number, the more efficient we have been.
	memMoved int
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
	offset := ra.newContainer(minContainerSize)
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

	// This following statement also works. But, given how much fastExpand gets
	// called (a lot), probably better to control allocation.
	// ra.data = append(ra.data, empty[:bySize]...)

	toSize := len(ra.data) + int(bySize)
	if toSize <= cap(ra.data) {
		ra.data = ra.data[:toSize]
		return
	}
	growBy := cap(ra.data)
	if growBy < int(bySize) {
		growBy = int(bySize)
	}
	out := make([]uint16, cap(ra.data)+growBy)
	copy(out, ra.data)
	ra.data = out[:toSize]
	// Re-reference ra.keys correctly because underlying array has changed.
	ra.keys = toUint64Slice(ra.data[:prev])
}

// scootRight isn't aware of containers. It's going to create empty space of
// bySize at the given offset in ra.data. The offset doesn't need to line up
// with a container.
func (ra *Bitmap) scootRight(offset uint64, bySize uint16) {
	left := ra.data[offset:]

	ra.fastExpand(bySize) // Expand the buffer.
	right := ra.data[len(ra.data)-len(left):]
	n := copy(right, left) // Move data right.
	ra.memMoved += n

	Memclr(ra.data[offset : offset+uint64(bySize)]) // Zero out the space in the middle.
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
		bySize = maxContainerSize - sz
	}

	// Select the portion to the right of the container, beyond its right boundary.
	ra.scootRight(offset+uint64(sz), bySize)
	ra.keys.updateOffsets(offset, uint64(bySize))

	if sz < 2048 {
		ra.data[offset] = sz + bySize

	} else {
		// Convert to bitmap container.
		src := array(ra.getContainer(offset))
		buf := src.toBitmapContainer(nil)
		assert(copy(ra.data[offset:], buf) == maxContainerSize)
	}
}

// stepSize is used for container expansion. For a container of given size n,
// stepSize would return the target size. This function is used to reduce the
// number of times expansion needs to happen for each container.
func stepSize(n uint16) uint16 {
	// <=64 -> 128
	// <=128 -> 256
	// <=256 -> 512
	// <=512 -> 1024
	// <=1024 -> 2048
	// >1024 -> maxSize (convert to bitmap)
	for i := uint16(64); i <= 1024; i *= 2 {
		if n <= i {
			return i * 2
		}
	}
	return maxContainerSize
}

// copyAt would copy over a given container via src, into the container at
// offset. If src is a bitmap, it would copy it over directly. If src is an
// array container, then it would follow these paths:
// - If src is smaller than dst, copy it over.
// - If not, look for target size for dst using the stepSize function.
// - If target size is maxSize, then convert src to a bitmap container, and
// 		copy to dst.
// - If target size is not max size, then expand dst container and copy src.
func (ra *Bitmap) copyAt(offset uint64, src []uint16) {
	dstSize := ra.data[offset]
	if dstSize == 0 {
		panic("Container size should NOT be zero")
	}

	// The src is a bitmapContainer. Just copy it over.
	if src[indexType] == typeBitmap {
		assert(src[indexSize] == maxContainerSize)
		bySize := uint16(maxContainerSize) - dstSize
		// Select the portion to the right of the container, beyond its right boundary.
		ra.scootRight(offset+uint64(dstSize), bySize)
		ra.keys.updateOffsets(offset, uint64(bySize))
		assert(copy(ra.data[offset:], src) == len(src))
		return
	}

	// src is an array container. Check if dstSize >= src. If so, just copy.
	// But, do keep dstSize intact, otherwise we'd lose portion of our container.
	if dstSize >= src[indexSize] {
		assert(copy(ra.data[offset:], src) == len(src))
		ra.data[offset] = dstSize
		return
	}

	// dstSize < src. Determine the target size of the container.
	targetSz := stepSize(dstSize)
	for targetSz < src[indexSize] {
		targetSz = stepSize(targetSz)
	}

	if targetSz == maxContainerSize {
		// Looks like the targetSize is now maxSize. So, convert src to bitmap container.
		s := array(src)

		bySize := uint16(maxContainerSize) - dstSize
		// Select the portion to the right of the container, beyond its right boundary.
		ra.scootRight(offset+uint64(dstSize), bySize)
		ra.keys.updateOffsets(offset, uint64(bySize))

		// Update the space of the container, so getContainer would work correctly.
		ra.data[offset] = maxContainerSize

		// Convert the src array to bitmap and write it directly over to the container.
		out := ra.getContainer(offset)
		Memclr(out)
		s.toBitmapContainer(out)
		return
	}

	// targetSize is not maxSize. Let's expand to targetSize and copy array.
	bySize := targetSz - dstSize
	ra.scootRight(offset+uint64(dstSize), bySize)
	ra.keys.updateOffsets(offset, uint64(bySize))
	assert(copy(ra.data[offset:], src) == len(src))
	ra.data[offset] = targetSz
}

func (ra Bitmap) getContainer(offset uint64) []uint16 {
	data := ra.data[offset:]
	if len(data) == 0 {
		panic(fmt.Sprintf("No container found at offset: %d\n", offset))
	}
	sz := data[0]
	return data[:sz]
}

func (ra *Bitmap) ToBytes() []byte {
	return toByteSlice(ra.data)
}

func (ra *Bitmap) Clone() *Bitmap {
	rb := NewBitmap()
	rb.data = make([]uint16, len(ra.data))
	copy(rb.data, ra.data)
	sz := toUint64Slice(rb.data[:4])[0]
	rb.keys = toUint64Slice(rb.data[:sz])
	return rb
}

func (ra *Bitmap) UnmarshalBinary(b []byte) {
	if len(b) < 8 {
		return
	}
	ra.data = toUint16Slice(b)
	sz := toUint64Slice(ra.data[:4])[0]
	ra.keys = toUint64Slice(ra.data[:sz])
}

// TODO: Check if we can optimize this function.
func (ra *Bitmap) IsEmpty() bool {
	if ra == nil {
		return true
	}
	return ra.GetCardinality() == 0
}

func (ra *Bitmap) Set(x uint64) bool {
	key := x & mask
	offset, has := ra.keys.getValue(key)
	if !has {
		// We need to add a container.
		o := ra.newContainer(minContainerSize)
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
			ra.expandContainer(offset)
		}
		return true
	case typeBitmap:
		b := bitmap(c)
		return b.add(uint16(x))
	}
	panic("we shouldn't reach here")
}

// TODO: Optimize this function
func (ra *Bitmap) SetMany(x []uint64) {
	for _, k := range x {
		ra.Set(k)
	}
}

// Select returns the xth integer in the bitmap. x=0 will return the smallest element.
func (ra *Bitmap) Select(x uint64) (uint64, error) {
	if x >= uint64(ra.GetCardinality()) {
		return 0, errors.Errorf("can't find %dth integer in a bitmap with only %d items",
			x, ra.GetCardinality())
	}
	n := ra.keys.numKeys()
	for i := 0; i < n; i++ {
		key := ra.keys.key(i)
		off := ra.keys.val(i)
		con := ra.getContainer(off)
		c := uint64(getCardinality(con))
		if x < c {
			switch con[indexType] {
			case typeArray:
				return key | uint64(array(con).all()[x]), nil
			case typeBitmap:
				return key | uint64(bitmap(con).ToArray()[x]), nil
			}
		}
		x -= c
	}
	panic("should not reach here")
}

func (ra *Bitmap) Contains(x uint64) bool {
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
// Remove range removes [lo, hi) from the bitmap.
func (ra *Bitmap) RemoveRange(lo, hi uint64) {
	if lo > hi {
		panic("lo should not be more than hi")
	}

	if lo == hi {
		return
	}

	k1 := lo & mask
	k2 := hi & mask

	N := ra.keys.numKeys()
	for i := 0; i < N; i++ {
		key := ra.keys.key(i)
		if key > k1 && key < k2 {
			_, has := ra.keys.getValue(key)
			if has {
				off := ra.newContainer(minContainerSize)
				ra.setKey(key, off)
			}
		}
	}

	//  complete range lie in a single container
	if k1 == k2 {
		off, has := ra.keys.getValue(k1)
		if has {
			c := ra.getContainer(off)
			switch c[indexType] {
			case typeArray:
				p := array(c)
				p.removeRange(uint16(lo), uint16(hi)-1)
			case typeBitmap:
				b := bitmap(c)
				b.removeRange(uint16(lo), uint16(hi)-1)
			}
		}
		return
	}

	// Remove elements >= lo in k1's container
	off, has := ra.keys.getValue(k1)
	if has {
		if uint16(lo) == 0 {
			// We need to remove the full container
			off := ra.newContainer(minContainerSize)
			ra.setKey(k1, off)
		}
		c := ra.getContainer(off)
		switch c[indexType] {
		case typeArray:
			p := array(c)
			p.removeRange(uint16(lo), math.MaxUint16)
		case typeBitmap:
			b := bitmap(c)
			b.removeRange(uint16(lo), math.MaxUint16)
		}
	}

	// Remove all elements < hi in k2's container
	off, has = ra.keys.getValue(k2)
	if has {
		if uint16(hi) == math.MaxUint16 {
			// We need to remove the full container
			off := ra.newContainer(minContainerSize)
			ra.setKey(k2, off)
		}
		c := ra.getContainer(off)
		switch c[indexType] {
		case typeArray:
			p := array(c)
			p.removeRange(0, uint16(hi)-1)
		case typeBitmap:
			b := bitmap(c)
			b.removeRange(0, uint16(hi)-1)
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

	var usedSize, card int
	usedSize += 4 * (ra.keys.numKeys())
	for i := 0; i < ra.keys.numKeys(); i++ {
		k := ra.keys.key(i)
		v := ra.keys.val(i)
		c := ra.getContainer(v)

		sz := c[indexSize]
		usedSize += int(sz)
		card += int(c[indexCardinality])

		b.WriteString(fmt.Sprintf(
			"[%03d] Key: %#8x. Offset: %7d. Size: %4d. Type: %d. Card: %6d. Uint16/Uid: %.2f\n",
			i, k, v, sz, c[indexType], c[indexCardinality],
			float64(sz)/float64(c[indexCardinality])))
	}
	b.WriteString(fmt.Sprintf("Number of containers: %d. Cardinality: %d\n",
		ra.keys.numKeys(), card))

	amp := float64(len(ra.data)-usedSize) / float64(usedSize)
	b.WriteString(fmt.Sprintf(
		"Size in Uint16s. Used: %d. Total: %d. Space Amplification: %.2f%%. Moved: %.2fx\n",
		usedSize, len(ra.data), amp*100.0, float64(ra.memMoved)/float64(usedSize)))

	b.WriteString(fmt.Sprintf("Used Uint16/Uid: %.2f. Total Uint16/Uid: %.2f",
		float64(usedSize)/float64(card), float64(len(ra.data))/float64(card)))

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

// TODO: Optimize this function.
func containerAndNot(ac, bc, buf []uint16) []uint16 {
	at := ac[indexType]
	bt := bc[indexType]

	if at == typeArray && bt == typeArray {
		left := array(ac)
		right := array(bc)
		return left.andNotArray(right, buf)
	}
	if at == typeArray && bt == typeBitmap {
		left := array(ac)
		right := bitmap(bc)
		return left.andNotBitmap(right, buf)
	}
	if at == typeBitmap && bt == typeArray {
		left := bitmap(ac)
		right := array(bc)
		out := right.andNotBitmap(left, buf)
		return out
	}
	if at == typeBitmap && bt == typeBitmap {
		left := bitmap(ac)
		right := bitmap(bc)
		return left.andNotBitmap(right, buf)
	}
	panic("containerAndNot: We should not reach here")
}

func containerOr(ac, bc, buf []uint16, inline bool) []uint16 {
	at := ac[indexType]
	bt := bc[indexType]

	if at == typeArray && bt == typeArray {
		left := array(ac)
		right := array(bc)
		// We can't always inline this function. If the right container has
		// enough entries, trying to do a union with the left container inplace
		// could end up overwriting the left container entries. So, we use a
		// buffer to hold all output, and then copy it over to left.
		//
		// TODO: If right doesn't have a lot of entries, we could just iterate
		// over left and merge the entries from right inplace. Would be faster
		// than copying over all entries into buffer. Worth trying that approach.
		return left.orArray(right, buf)
	}
	if at == typeArray && bt == typeBitmap {
		left := array(ac)
		right := bitmap(bc)
		return right.orArray(left, buf)
	}
	if inline {
		// These two following cases can be fully inlined.
		buf = nil
	}
	if at == typeBitmap && bt == typeArray {
		left := bitmap(ac)
		right := array(bc)
		return left.orArray(right, buf)
	}
	if at == typeBitmap && bt == typeBitmap {
		left := bitmap(ac)
		right := bitmap(bc)
		return left.orBitmap(right, buf)
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
			off := a.newContainer(minContainerSize)
			a.setKey(ak, off)
			ai++
		} else {
			bi++
		}
	}
	for ai < an {
		off := a.newContainer(minContainerSize)
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

	buf := make([]uint16, maxContainerSize)

	for ai < an && bi < bn {
		ak := a.keys.key(ai)
		bk := b.keys.key(bi)
		if ak == bk {
			off := a.keys.val(ai)
			ac := a.getContainer(off)

			off = b.keys.val(bi)
			bc := b.getContainer(off)

			// do the intersection
			c := containerAndNot(ac, bc, buf)

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

func (dst *Bitmap) Or(src *Bitmap) {
	srcIdx, numKeys := 0, src.keys.numKeys()

	buf := make([]uint16, maxContainerSize)
	for srcIdx < numKeys {
		srcCont := src.getContainer(src.keys.val(srcIdx))
		key := src.keys.key(srcIdx)

		dstIdx := dst.keys.search(key)
		if dstIdx >= dst.keys.numKeys() || dst.keys.key(dstIdx) != key {
			// srcCont doesn't exist in dst. So, copy it over.
			offset := dst.newContainer(uint16(len(srcCont)))
			copy(dst.getContainer(offset), srcCont)
			dst.setKey(key, offset)
		} else {
			// Container exists in dst as well. Do an inline containerOr.
			offset := dst.keys.val(dstIdx)
			dstCont := dst.getContainer(offset)
			if c := containerOr(dstCont, srcCont, buf, true); len(c) > 0 {
				dst.copyAt(offset, c)
				dst.setKey(key, offset)
			}
		}
		srcIdx++
	}
}

func Or(a, b *Bitmap) *Bitmap {
	ai, an := 0, a.keys.numKeys()
	bi, bn := 0, b.keys.numKeys()

	buf := make([]uint16, maxContainerSize)
	res := NewBitmap()
	for ai < an && bi < bn {
		ak := a.keys.key(ai)
		ac := a.getContainer(a.keys.val(ai))

		bk := b.keys.key(bi)
		bc := b.getContainer(b.keys.val(bi))

		if ak == bk {
			// Do the union.
			outc := containerOr(ac, bc, buf, false)
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

// FastParOr would group up bitmaps and call FastOr on them concurrently. It
// would then merge the groups into final Bitmap. This approach is simpler and
// faster than operating at a container level, because we can't operate on array
// containers belonging to the same Bitmap concurrently because array containers
// can expand, leaving no clear boundaries.
//
// If FastParOr is called with numGo=1, it just calls FastOr.
//
// Experiments with numGo=4 shows that FastParOr would be 2x the speed of
// FastOr, but 4x the memory usage, even under 50% CPU usage. So, use wisely.
func FastParOr(numGo int, bitmaps ...*Bitmap) *Bitmap {
	if numGo == 1 {
		return FastOr(bitmaps...)
	}
	width := max(len(bitmaps)/numGo, 3)

	var wg sync.WaitGroup
	var res []*Bitmap
	for start := 0; start < len(bitmaps); start += width {
		end := min(start+width, len(bitmaps))
		res = append(res, nil) // Make space for result.
		wg.Add(1)

		go func(start, end int) {
			idx := start / width
			res[idx] = FastOr(bitmaps[start:end]...)
			wg.Done()
		}(start, end)
	}
	wg.Wait()
	return FastOr(res...)
}

// FastOr would merge given Bitmaps into one Bitmap. This is faster than
// doing an OR over the bitmaps iteratively.
func FastOr(bitmaps ...*Bitmap) *Bitmap {
	// We first figure out the container distribution across the bitmaps. We do
	// that by looking at the key of the container, and the cardinality. We
	// assume the worst-case scenario where the union would result in a
	// cardinality (per container) of the sum of cardinalities of each of the
	// corresponding containers in other bitmaps.
	containers := make(map[uint64]int)
	for _, b := range bitmaps {
		for i := 0; i < b.keys.numKeys(); i++ {
			offset := b.keys.val(i)
			cont := b.getContainer(offset)
			card := getCardinality(cont)
			containers[b.keys.key(i)] += card
		}
	}

	// We use the above information to pre-generate the destination Bitmap and
	// allocate container sizes based on the calculated cardinalities.
	// var sz int
	dst := NewBitmap()
	// First create the keys. We do this as a separate step, because keys are
	// the left most portion of the data array. Adding space there requires
	// moving a lot of pieces.
	for key := range containers {
		dst.setKey(key, 0)
	}

	// Then create the bitmap containers.
	for key, card := range containers {
		if card >= 4096 {
			offset := dst.newContainer(maxContainerSize)
			c := dst.getContainer(offset)
			c[indexSize] = maxContainerSize
			c[indexType] = typeBitmap
			dst.setKey(key, offset)
		}
	}

	// Create the array containers at the end. This allows them to expand
	// without having to move a lot of memory.
	for key, card := range containers {
		// Ensure this condition exactly maps up with above.
		if card < 4096 {
			if card < minContainerSize {
				card = minContainerSize
			}
			offset := dst.newContainer(uint16(card))
			c := dst.getContainer(offset)
			c[indexSize] = uint16(card)
			c[indexType] = typeArray
			dst.setKey(key, offset)
		}
	}

	// dst Bitmap is ready to be ORed with the given Bitmaps.
	for _, b := range bitmaps {
		dst.Or(b)
	}
	return dst
}
