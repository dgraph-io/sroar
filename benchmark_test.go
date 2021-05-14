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
	"math/rand"
	"runtime"
	"sync"
	"testing"
)

// go test -bench BenchmarkMemoryUsage -run -
func BenchmarkMemoryUsage(b *testing.B) {
	b.StopTimer()
	bitmaps := make([]*Bitmap, 0, 10)

	incr := uint64(1 << 16)
	max := uint64(1<<32 - 1)
	for x := 0; x < 10; x++ {
		rb := NewBitmap()

		var i uint64
		for i = 0; i <= max-incr; i += incr {
			rb.Set(i)
		}

		bitmaps = append(bitmaps, rb)
	}

	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	b.Logf("HeapInUse: %d, HeapObjects: %d", stats.HeapInuse, stats.HeapObjects)
	b.StartTimer()
}

// go test -bench BenchmarkIntersection -run -
func BenchmarkIntersectionRoaring(b *testing.B) {
	b.StopTimer()
	r := rand.New(rand.NewSource(0))
	s1 := NewBitmap()
	sz := int64(150000)
	initsize := 65000
	for i := 0; i < initsize; i++ {
		s1.Set(uint64(r.Int63n(sz)))
	}

	s2 := NewBitmap()
	sz = int64(100000000)
	initsize = 65000
	for i := 0; i < initsize; i++ {
		s2.Set(uint64(r.Int63n((sz))))
	}
	b.StartTimer()

	card := 0
	for j := 0; j < b.N; j++ {
		s3 := And(s1, s2)
		card = card + s3.GetCardinality()
	}
	b.Logf("card: %d\n", card)
}

// go test -bench BenchmarkSet -run -
func BenchmarkSetRoaring(b *testing.B) {
	b.StopTimer()
	r := rand.New(rand.NewSource(0))
	sz := int64(1000000)
	s := NewBitmap()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		s.Set(uint64(r.Int63n(sz)))
	}
}

type uint64Heap []uint64

func (u uint64Heap) Len() int            { return len(u) }
func (u uint64Heap) Less(i, j int) bool  { return u[i] < u[j] }
func (u uint64Heap) Swap(i, j int)       { u[i], u[j] = u[j], u[i] }
func (u *uint64Heap) Push(x interface{}) { *u = append(*u, x.(uint64)) }
func (u *uint64Heap) Pop() interface{} {
	old := *u
	n := len(old)
	x := old[n-1]
	*u = old[0 : n-1]
	return x
}

func BenchmarkMerge11K(b *testing.B) {
	var bitmaps []*Bitmap
	for i := 0; i < 10000; i++ {
		bm := NewBitmap()
		for j := 0; j < 1000; j++ {
			x := rand.Uint64() % 1e8 // 10M.
			bm.Set(x)
		}
		bitmaps = append(bitmaps, bm)
	}

	second := func() *Bitmap {
		var res []*Bitmap
		for i := 0; i < 100; i += 1 {
			input := bitmaps[100*i : 100*i+100]
			out := FastOr(1, input...)
			res = append(res, out)
		}
		return FastOr(1, res...)
	}
	third := func() *Bitmap {
		var wg sync.WaitGroup
		res := make([]*Bitmap, 100)
		for i := 0; i < 100; i += 1 {
			wg.Add(1)
			go func(i int) {
				input := bitmaps[100*i : 100*i+100]
				res[i] = FastOr(1, input...)
				wg.Done()
			}(i)
		}
		wg.Wait()
		return FastOr(1, res...)
	}
	fourth := func() *Bitmap {
		return FastOr(100, bitmaps...)
	}

	out := FastOr(1, bitmaps...)
	b.Logf("Out: %s\n", out)
	out2 := second()
	if out2.GetCardinality() != out.GetCardinality() {
		panic("Don't match")
	}
	out3 := third()
	if out3.GetCardinality() != out.GetCardinality() {
		panic("Don't match")
	}
	out4 := fourth()
	if out4.GetCardinality() != out.GetCardinality() {
		panic("Don't match")
	}
	b.Logf("card2: %d card3: %d card4: %d", out2.GetCardinality(),
		out3.GetCardinality(), out4.GetCardinality())

	b.Run("fastor", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = FastOr(1, bitmaps...)
		}
	})

	b.Run("fastor-groups", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = second()
		}
	})
	b.Run("fastor-groups-conc", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = third()
		}
	})
	b.Run("fastor-conc", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = fourth()
		}
	})
}
