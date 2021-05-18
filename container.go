package sroar

import (
	"fmt"
	"math"
	"math/bits"
	"strings"
)

// container uses extra 4 []uint16 in the front as header.
// container[0] is used for storing the size of the container, expressed in Uint16.
// The container size cannot exceed the vicinity of 8KB. At 8KB, we switch from packed arrays to
// bitmaps. We can fit the entire uint16 worth of bitmaps in 8KB (2^16 / 8 = 8
// KB).

const (
	typeArray  uint16 = 0x00
	typeBitmap uint16 = 0x01

	// Container header.
	indexSize        int = 0
	indexType        int = 1
	indexCardinality int = 2
	// Index 2 and 3 is used for cardinality. We need 2 uint16s to store cardinality because
	// 2^16 will not fit in uint16.
	startIdx uint16 = 4

	minContainerSize = 64 // In Uint16.
	// Bitmap container can contain 2^16 integers. Each integer would use one bit to represent.
	// Given that our data is represented in []uint16s, that'd mean the size of container to store
	// it would be divided by 16.
	// 4 for header and 4096 for storing bitmap container. In Uint16.
	maxContainerSize = 4 + (1<<16)/16
)

func dataAt(data []uint16, i int) uint16 { return data[int(startIdx)+i] }

func getCardinality(data []uint16) int {
	// This sum has to be done using two ints to avoid overflow.
	return int(data[indexCardinality]) + int(data[indexCardinality+1])
}

func incrCardinality(data []uint16) {
	cur := getCardinality(data)
	if cur+1 > math.MaxUint16 {
		data[indexCardinality+1] = 1
	} else {
		data[indexCardinality]++
	}
}

func setCardinality(data []uint16, c int) {
	if c > math.MaxUint16 {
		data[indexCardinality] = math.MaxUint16
		data[indexCardinality+1] = 1
	} else {
		data[indexCardinality] = uint16(c)
		data[indexCardinality+1] = 0
	}
}

type array []uint16

// find returns the index of the first element >= x.
// The index is based on data portion of the container, ignoring startIdx.
// If the element > than all elements present, then N is returned where N = cardinality of the
// container.
func (c array) find(x uint16) int {
	N := getCardinality(c)
	for i := int(startIdx); i < int(startIdx)+N; i++ {
		if len(c) <= int(i) {
			panic(fmt.Sprintf("find: %d len(c) %d <= i %d\n", x, len(c), i))
		}
		if c[i] >= x {
			return int(i - int(startIdx))
		}
	}
	return N
}
func (c array) has(x uint16) bool {
	N := getCardinality(c)
	idx := c.find(x)
	if idx == N {
		return false
	}
	return c[int(startIdx)+idx] == x
}

func (c array) add(x uint16) bool {
	idx := c.find(x)
	N := getCardinality(c)
	offset := int(startIdx) + idx

	if int(idx) < N {
		if c[offset] == x {
			return false
		}
		// The entry at offset is the first entry, which is greater than x. Move it to the right.
		copy(c[offset+1:], c[offset:])
	}
	c[offset] = x
	incrCardinality(c)
	return true
}

func (c array) remove(x uint16) bool {
	idx := c.find(x)
	N := getCardinality(c)
	offset := int(startIdx) + idx

	if int(idx) < N {
		if c[offset] != x {
			return false
		}
		copy(c[offset:], c[offset+1:])
		setCardinality(c, N-1)
		return true
	}
	return false
}

func (c array) removeBefore(x uint16) {
	idx := c.find(x)
	N := getCardinality(c)

	if int(idx) >= N {
		c = c[:startIdx]
		setCardinality(c, 0)
		return
	}
	copy(c[:], c[idx:])
	setCardinality(c, N-idx)
}

func (c array) removeAfter(x uint16) {
	idx := c.find(x)
	N := getCardinality(c)

	if int(idx) >= N {
		return
	}
	c = c[:int(startIdx)+idx+1]
	setCardinality(c, idx+1)

}

// TODO: Figure out how memory allocation would work in these situations. Perhaps use allocator here?
func (c array) andArray(other array) []uint16 {
	min := min(getCardinality(c), getCardinality(other))

	setc := c.all()
	seto := other.all()

	out := make([]uint16, int(startIdx)+min+1)
	num := uint16(intersection2by2(setc, seto, out[startIdx:]))

	// Truncate out to how many values were found.
	out = out[:startIdx+num+1]
	out[indexType] = typeArray
	out[indexSize] = uint16(len(out))
	setCardinality(out, int(num))
	return out
}

func (c array) andNotArray(other array, buf []uint16) []uint16 {
	var setOr []uint16
	var setAnd []uint16

	max := getCardinality(c) + getCardinality(other)
	orRes := c.orArray(other, buf)

	// orArray can result in bitmap.
	if orRes[indexType] == typeBitmap {
		setOr = bitmap(orRes).ToArray()
	} else {
		setOr = array(orRes).all()
	}
	andRes := array(c.andArray(other))
	setAnd = andRes.all()

	out := make([]uint16, int(startIdx)+max+1)
	num := uint16(difference(setOr, setAnd, out[startIdx:]))

	// Truncate out to how many values were found.
	out = out[:startIdx+num+1]
	out[indexType] = typeArray
	out[indexSize] = uint16(len(out))
	setCardinality(out, int(num))
	return out
}

func (c array) orArray(other array, buf []uint16) []uint16 {
	max := getCardinality(c) + getCardinality(other)
	if max > 4096 {
		// Use bitmap container.
		out := c.toBitmapContainer(buf)
		data := out[startIdx:]

		num := getCardinality(out)
		for _, x := range other.all() {
			idx := x / 16
			pos := x % 16
			// We're doing the OnesCount twice to avoid branching.
			before := bits.OnesCount16(data[idx])
			data[idx] |= bitmapMask[pos]
			after := bits.OnesCount16(data[idx])
			num += after - before
		}
		setCardinality(out, num)
		// For now, just keep it as a bitmap. No need to change if the
		// cardinality is smaller than 4096.
		return out
	}

	// The output would be of typeArray.
	out := buf[:int(startIdx)+max]
	num := union2by2(c.all(), other.all(), out[startIdx:])
	out[indexType] = typeArray
	out[indexSize] = uint16(len(out))
	setCardinality(out, num)
	return out
}

var tmp = make([]uint16, 8192)

func (c array) andBitmap(other bitmap) []uint16 {
	out := make([]uint16, int(startIdx)+getCardinality(c)+2) // some extra space.
	out[indexType] = typeArray

	pos := startIdx
	for _, x := range c.all() {
		out[pos] = x
		pos += other.bitValue(x)
	}

	// Ensure we have at least one empty slot at the end.
	res := out[:pos+1]
	res[indexSize] = uint16(len(res))
	setCardinality(res, int(pos-startIdx))
	return res
}

// TODO: Write an optmized version of this function.
func (c array) andNotBitmap(other bitmap, buf []uint16) []uint16 {
	// TODO: Iterate over the array and just check if the element is present in the bitmap.
	// TODO: This won't work, because we're using buf wrong here.
	bm := c.toBitmapContainer(nil)
	return bitmap(bm).andNotBitmap(other, buf)
}

func (c array) isFull() bool {
	N := getCardinality(c)
	return int(startIdx)+N >= len(c)
}

func (c array) all() []uint16 {
	N := getCardinality(c)
	return c[startIdx : int(startIdx)+N]
}

func (c array) minimum() uint16 {
	N := getCardinality(c)
	if N == 0 {
		return 0
	}
	return c[startIdx]
}

func (c array) maximum() uint16 {
	N := getCardinality(c)
	if N == 0 {
		return 0
	}
	return c[int(startIdx)+N-1]
}

func (c array) toBitmapContainer(buf []uint16) []uint16 {
	if len(buf) == 0 {
		buf = make([]uint16, maxContainerSize)
	} else {
		assert(len(buf) == maxContainerSize)
	}

	b := bitmap(buf)
	b[indexSize] = maxContainerSize
	b[indexType] = typeBitmap
	setCardinality(b, getCardinality(c))

	data := b[startIdx:]
	for _, x := range c.all() {
		idx := x / 16
		pos := x % 16
		data[idx] |= bitmapMask[pos]
	}
	return b
}

func (c array) String() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("Size: %d\n", c[0]))
	for i, val := range c[startIdx:] {
		b.WriteString(fmt.Sprintf("%d: %d\n", i, val))
	}
	return b.String()
}

type bitmap []uint16

var bitmapMask []uint16

func init() {
	bitmapMask = make([]uint16, 16)
	for i := 0; i < 16; i++ {
		bitmapMask[i] = 1 << (15 - i)
	}
}

func (b bitmap) add(x uint16) bool {
	idx := x >> 4
	pos := x & 0xF

	if has := b[startIdx+idx] & bitmapMask[pos]; has > 0 {
		return false
	}

	b[startIdx+idx] |= bitmapMask[pos]
	incrCardinality(b)
	return true
}

func (b bitmap) remove(x uint16) bool {
	idx := x >> 4
	pos := x & 0xF

	c := getCardinality(b)
	if has := b[startIdx+idx] & bitmapMask[pos]; has > 0 {
		b[startIdx+idx] ^= bitmapMask[pos]
		setCardinality(b, c-1)
		return true
	}
	return false
}

func (b bitmap) removeBefore(x uint16) {
	idx := x >> 4
	pos := x & 0xF

	N := getCardinality(b)
	var removed int
	for i := 0; i < int(idx); i++ {
		removed += bits.OnesCount16(b[int(startIdx)+i])
		b[int(startIdx)+i] = 0
	}
	for p := 0; p < int(pos); p++ {
		if b[startIdx+idx]&bitmapMask[p] > 0 {
			removed++
		}
		b[startIdx+idx] &= ^bitmapMask[p]
	}
	setCardinality(b, N-removed)
}

func (b bitmap) removeAfter(x uint16) {
	idx := x >> 4
	pos := x & 0xF

	N := getCardinality(b)
	var removed int
	for i := idx + 1; i < 1<<12; i++ {
		removed += bits.OnesCount16(b[startIdx+i])
		b[startIdx+i] = 0
	}
	for p := pos + 1; p < 1<<4; p++ {
		if b[startIdx+idx]&bitmapMask[p] > 0 {
			removed++
		}
		b[startIdx+idx] &= ^bitmapMask[p]
	}
	setCardinality(b, N-removed)
}

func (b bitmap) has(x uint16) bool {
	idx := x >> 4
	pos := x & 0xF

	has := b[startIdx+idx] & bitmapMask[pos]
	return has > 0
}

// TODO: This can perhaps be using SIMD instructions.
func (b bitmap) andBitmap(other bitmap) []uint16 {
	out := make([]uint16, maxContainerSize)
	out[indexSize] = maxContainerSize
	out[indexType] = typeBitmap
	var num int
	for i := 4; i < len(b); i++ {
		out[i] = b[i] & other[i]
		num += bits.OnesCount16(out[i])
	}
	setCardinality(out, num)
	return out
}

func (b bitmap) orBitmap(other bitmap, buf []uint16) []uint16 {
	isInline := len(buf) == 0
	if isInline {
		buf = b
	} else {
		copy(buf, b) // Copy over first.
	}
	buf[indexSize] = maxContainerSize
	buf[indexType] = typeBitmap

	var num int
	data := buf[startIdx:]
	for i, v := range other[startIdx:] {
		data[i] |= v
		num += bits.OnesCount16(data[i])
	}
	setCardinality(buf, num)
	if isInline {
		return nil
	}
	return buf
}

func (b bitmap) andNotBitmap(other bitmap, buf []uint16) []uint16 {
	copy(buf, b) // Copy over first.
	buf[indexSize] = maxContainerSize
	buf[indexType] = typeBitmap

	var num int
	data := buf[startIdx:]
	for i, v := range other[startIdx:] {
		data[i] = (data[i] | v) ^ (data[i] & v)
		num += bits.OnesCount16(data[i])
	}
	setCardinality(buf, num)
	return buf
}

func (b bitmap) orArray(other array, buf []uint16) []uint16 {
	isInline := len(buf) == 0
	if isInline {
		buf = b
	} else {
		copy(buf, b)
	}

	num := getCardinality(buf)
	for _, x := range other.all() {
		idx := x / 16
		pos := x % 16

		val := &buf[4+idx]
		before := bits.OnesCount16(*val)
		*val |= bitmapMask[pos]
		after := bits.OnesCount16(*val)
		num += after - before
	}
	setCardinality(buf, num)
	if isInline {
		return nil
	}
	return buf
}

func (b bitmap) ToArray() []uint16 {
	var res []uint16
	data := b[startIdx:]
	for idx := uint16(0); idx < uint16(len(data)); idx++ {
		x := data[idx]
		// TODO: This could potentially be optimized.
		for pos := uint16(0); pos < 16; pos++ {
			if x&bitmapMask[pos] > 0 {
				res = append(res, (idx<<4)|pos)
			}
		}
	}
	return res
}

// bitValue returns a 0 or a 1 depending upon whether x is present in the bitmap, where 1 means
// present and 0 means absent.
func (b bitmap) bitValue(x uint16) uint16 {
	idx := x >> 4
	return (b[4+idx] >> (15 - (x & 0xF))) & 1
}

func (b bitmap) isFull() bool {
	return false
}

func (b bitmap) minimum() uint16 {
	N := getCardinality(b)
	if N == 0 {
		return 0
	}
	for i, x := range b[startIdx:] {
		lz := bits.LeadingZeros16(x)
		if lz == 16 {
			continue
		}
		return uint16(16*i + lz)
	}
	panic("We shouldn't reach here")
}

func (b bitmap) maximum() uint16 {
	N := getCardinality(b)
	if N == 0 {
		return 0
	}
	for i := len(b) - 1; i >= int(startIdx); i-- {
		x := b[i]
		tz := bits.TrailingZeros16(x)
		if tz == 16 {
			continue
		}
		return uint16(16*i + 15 - tz)
	}
	panic("We shouldn't reach here")
}
