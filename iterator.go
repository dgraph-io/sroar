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
	"math/bits"
)

type FastIterator struct {
	bm *Bitmap

	keys []uint64
	kidx int

	cidx int

	bmHiIdx int
	bitset  uint16
}

func (bm *Bitmap) NewRangeIterators(numRanges int) []*FastIterator {
	keyn := bm.keys.numKeys()
	iters := make([]*FastIterator, numRanges)
	width := keyn / numRanges
	rem := keyn % numRanges
	cnt := 0

	// This loop distributes the key equally to the ranges. For example: If numRanges = 3
	// and keyn = 8 then it will be distributes as [3, 3, 2]
	for i := 0; i < numRanges; i++ {
		iters[i] = bm.NewFastIterator()
		n := width
		if i < rem {
			n = width + 1
		}
		iters[i].keys = iters[i].keys[cnt : cnt+2*n]
		cnt = cnt + 2*n
	}
	return iters
}

func (bm *Bitmap) NewFastIterator() *FastIterator {
	return &FastIterator{
		bm:      bm,
		keys:    bm.keys[indexNodeStart : indexNodeStart+bm.keys.numKeys()*2],
		kidx:    0,
		cidx:    -1,
		bmHiIdx: -1,
	}
}

func (it *FastIterator) Next() uint64 {
	if len(it.keys) == 0 {
		return 0
	}
	key := it.keys[2*it.kidx]
	off := it.keys[2*it.kidx+1]
	cont := it.bm.getContainer(off)
	card := getCardinality(cont)

	// we need to jump container in these scenarios
	// - The cardinality of this container is zero
	// - cidx is already at the last element of the container
	for card == 0 || it.cidx+1 >= card {
		if it.kidx+1 >= len(it.keys)/2 {
			return 0
		}
		it.kidx++
		it.cidx = -1
		it.bmHiIdx = -1
		it.bitset = 0
		key = it.keys[2*it.kidx]
		off = it.keys[2*it.kidx+1]
		cont = it.bm.getContainer(off)
		card = getCardinality(cont)
	}

	//  The above loop assures that we can do next in this container
	it.cidx++
	switch cont[indexType] {
	case typeArray:
		return key | uint64(cont[int(startIdx)+it.cidx])
	case typeBitmap:
		for it.bitset == 0 && it.bmHiIdx+1 < len(cont[startIdx:]) {
			it.bmHiIdx++
			it.bitset = cont[int(startIdx)+it.bmHiIdx]
		}
		assert(it.bitset > 0)
		loIdx := uint16(bits.LeadingZeros16(it.bitset))
		msb := 1 << (16 - loIdx - 1)
		it.bitset ^= uint16(msb)
		return key | uint64(it.bmHiIdx*16+int(loIdx))
	}
	return 0
}

type ManyItr struct {
	index int
	arr   []uint64
}

func (r *Bitmap) ManyIterator() *ManyItr {
	return &ManyItr{
		arr: r.ToArray(),
	}

}

func (itr *ManyItr) NextMany(buf []uint64) int {
	count := 0
	for i := 0; i < len(buf); i++ {
		if itr.index == len(itr.arr) {
			break
		}
		buf[i] = itr.arr[itr.index]
		itr.index++
		count++
	}
	return count
}
