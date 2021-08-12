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

type Iterator struct {
	// index int
	// arr   []uint64
	bm     *Bitmap
	keyIdx int
	cIdx   int
	cont   []uint16
	elems  []uint16
}

func (r *Bitmap) NewIterator() *Iterator {
	var elems []uint16
	cont := r.getContainer(r.keys.val(0))
	switch cont[indexType] {
	case typeArray:
		elems = array(cont).all()
	case typeBitmap:
		elems = bitmap(cont).all()
	}
	return &Iterator{
		bm:    r,
		cIdx:  -1,
		cont:  cont,
		elems: elems,
	}
}

func (itr *Iterator) checkNext(update bool) bool {

	kidx := itr.keyIdx
	cont := itr.cont
	cidx := itr.cIdx

	jumped := false
	// after this for loop, we will have a non-zero container or exhausted kidx
	for kidx < itr.bm.keys.numKeys() {
		coff := itr.bm.keys.val(kidx)
		cont = itr.bm.getContainer(coff)
		card := getCardinality(cont)
		if jumped && card > 0 {
			break
		}

		if card > 0 && cidx+1 < card {
			break
		}
		jumped = true
		cidx = -1
		kidx++
	}
	// kidx is exhausted, we cannot have a next
	if kidx >= itr.bm.keys.numKeys() {
		return false
	}
	if update {

		cidx++
		itr.keyIdx = kidx
		itr.cIdx = cidx
		itr.cont = cont
		if jumped {
			switch itr.cont[indexType] {
			case typeArray:
				itr.elems = array(cont).all()
			case typeBitmap:
				itr.elems = bitmap(cont).all()
			}
		}
	}
	return true
}

func (itr *Iterator) HasNext() bool {
	return itr.checkNext(false)
}

func (itr *Iterator) Next() uint64 {
	if itr.checkNext(true) {
		return itr.Val()
	}
	return 0
}

func (itr *Iterator) Val() uint64 {
	key := itr.bm.keys.key(itr.keyIdx)
	return key | uint64(itr.elems[itr.cIdx])
}

// AdvanceIfNeeded advances until the value < minval.
func (itr *Iterator) AdvanceIfNeeded(minval uint64) {
	// if itr.index < 0 {
	// 	return
	// }
	for itr.Val() < minval {
		if itr.HasNext() {
			itr.Next()
		} else {
			break
		}
	}
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
