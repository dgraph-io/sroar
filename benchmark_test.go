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
