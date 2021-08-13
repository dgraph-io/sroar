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
