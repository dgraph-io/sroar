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

type Iterator struct {
	index   int
	reverse bool
	arr     []uint64
}

func (r *Bitmap) NewIterator() *Iterator {
	return &Iterator{
		index: -1,
		arr:   r.ToArray(),
	}
}

func (r *Bitmap) NewReverseIterator() *Iterator {
	return &Iterator{
		index:   r.GetCardinality(),
		arr:     r.ToArray(),
		reverse: true,
	}
}

func (itr *Iterator) HasNext() bool {
	if itr.reverse {
		return itr.index > 0
	} else {
		return int(itr.index) < len(itr.arr)-1
	}
}

func (itr *Iterator) Next() uint64 {
	if itr.reverse {
		itr.index--

	} else {
		itr.index++
	}
	return itr.arr[itr.index]
}

func (itr *Iterator) Val() uint64 {
	return itr.arr[itr.index]
}

// AdvanceIfNeeded advances until the value < minval.
func (itr *Iterator) AdvanceIfNeeded(minval uint64) {
	if itr.index < 0 {
		return
	}
	for itr.Val() < minval {
		if itr.HasNext() {
			itr.Next()
		} else {
			break
		}
	}
}

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

	for i := 0; i < numRanges; i++ {
		iters[i] = bm.NewFastIterator()
		if i == numRanges-1 {
			iters[i].keys = iters[i].keys[width*i:]
		}
		iters[i].keys = iters[i].keys[width*i : width*(i+1)]
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
