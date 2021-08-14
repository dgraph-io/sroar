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
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIteratorBasic(t *testing.T) {
	n := uint64(1e5)
	bm := NewBitmap()
	for i := uint64(1); i <= n; i++ {
		bm.Set(uint64(i))
	}

	it := bm.NewFastIterator()
	for i := uint64(1); i <= n; i++ {
		v := it.Next()
		require.Equal(t, i, v)
	}
	v := it.Next()
	require.Equal(t, uint64(0), v)
}

func TestIteratorRanges(t *testing.T) {
	n := uint64(1e5)
	bm := NewBitmap()
	for i := uint64(1); i <= n; i++ {
		bm.Set(uint64(i))
	}

	iters := bm.NewRangeIterators(8)
	cnt := uint64(1)
	for idx := 0; idx < 8; idx++ {
		it := iters[idx]
		for v := it.Next(); v > 0; v = it.Next() {
			// t.Log(cnt, v)
			require.Equal(t, cnt, v)
			cnt++
		}
	}
}

func TestIteratorRandom(t *testing.T) {
	n := uint64(1e6)
	bm := NewBitmap()
	mp := make(map[uint64]struct{})
	var arr []uint64
	for i := uint64(1); i <= n; i++ {
		v := uint64(rand.Intn(int(n) * 5))
		if v == 0 {
			continue
		}
		if _, ok := mp[v]; ok {
			continue
		}
		mp[v] = struct{}{}
		arr = append(arr, v)
		bm.Set(uint64(v))
	}

	sort.Slice(arr, func(i, j int) bool {
		return arr[i] < arr[j]
	})

	it := bm.NewFastIterator()
	v := it.Next()
	for i := uint64(0); i < uint64(len(arr)); i++ {
		require.Equal(t, arr[i], v)
		v = it.Next()
	}
}

// func TestIteratorBasic(t *testing.T) {
// 	testSz := []int{0, 1, 16, 2047, 2048, 1e4}

// 	var sz int
// 	test := func(t *testing.T) {
// 		b := NewBitmap()
// 		for i := uint64(0); i < uint64(sz); i++ {
// 			b.Set(i)
// 		}
// 		it := b.NewIterator()
// 		cnt := uint64(0)
// 		for it.HasNext() {
// 			require.Equal(t, cnt, it.Next())
// 			cnt++
// 		}
// 		require.Equal(t, uint64(sz), cnt)

// 		rit := b.NewReverseIterator()
// 		for rit.HasNext() {
// 			cnt--
// 			require.Equal(t, cnt, rit.Next())
// 		}
// 		require.Equal(t, uint64(0), cnt)
// 	}
// 	for i := range testSz {
// 		sz = testSz[i]
// 		t.Run(fmt.Sprintf("test-%d", sz), test)
// 	}

// 	r := rand.New(rand.NewSource(0))
// 	t.Run("test-random", func(t *testing.T) {
// 		b := NewBitmap()
// 		N := uint64(1e4)
// 		for i := uint64(0); i < N; i++ {
// 			b.Set(uint64(r.Int63n(math.MaxInt64)))
// 		}
// 		it := b.NewIterator()
// 		var vals []uint64
// 		for it.HasNext() {
// 			vals = append(vals, it.Next())
// 		}
// 		require.Equal(t, b.ToArray(), vals)
// 	})
// }

// func TestIteratorWithRemoveKeys(t *testing.T) {
// 	b := NewBitmap()
// 	N := uint64(1e6)
// 	for i := uint64(0); i < N; i++ {
// 		b.Set(i)
// 	}

// 	b.RemoveRange(0, N)
// 	it := b.NewIterator()

// 	cnt := 0
// 	for it.HasNext() {
// 		cnt++
// 		it.Next()
// 	}
// 	require.Equal(t, 0, cnt)
// }

// func TestManyIterator(t *testing.T) {
// 	b := NewBitmap()
// 	for i := 0; i < int(1e6); i++ {
// 		b.Set(uint64(i))
// 	}

// 	mi := b.ManyIterator()
// 	buf := make([]uint64, 1000)

// 	i := 0
// 	for {
// 		got := mi.NextMany(buf)
// 		if got == 0 {
// 			break
// 		}
// 		require.Equal(t, 1000, got)
// 		require.Equal(t, uint64(i*1000), buf[0])
// 		i++
// 	}
// }

// func BenchmarkIterator(b *testing.B) {
// 	bm := NewBitmap()
// 	for i := 0; i < int(1e5); i++ {
// 		bm.Set(uint64(i))
// 	}

// 	b.ResetTimer()
// 	for i := 0; i < b.N; i++ {
// 		it := bm.NewIterator()
// 		for it.HasNext() {
// 			it.Next()
// 		}
// 	}
// }
