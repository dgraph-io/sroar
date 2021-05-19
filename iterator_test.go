package sroar

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIterator(t *testing.T) {
	testSz := []int{0, 1, 16, 2047, 2048, 10000}

	var sz int
	test := func(t *testing.T) {
		b := NewBitmap()
		for i := uint64(0); i < uint64(sz); i++ {
			b.Set(i)
		}
		it := b.NewIterator()
		cnt := uint64(0)
		for it.HasNext() {
			require.Equal(t, cnt, it.Next())
			cnt++
		}
		require.Equal(t, uint64(sz), cnt)

		rit := b.NewReverseIterator()
		cnt = uint64(sz)
		for rit.HasNext() {
			cnt--
			require.Equal(t, cnt, rit.Next())
		}
		require.Equal(t, uint64(0), cnt)
	}
	for i := range testSz {
		sz = testSz[i]
		t.Run(fmt.Sprintf("test-%d", sz), test)
	}

	r := rand.New(rand.NewSource(0))
	t.Run("test-random", func(t *testing.T) {
		b := NewBitmap()
		N := uint64(1e4)
		for i := uint64(0); i < N; i++ {
			b.Set(uint64(r.Int63n(math.MaxInt64)))
		}
		it := b.NewIterator()
		var vals []uint64
		for it.HasNext() {
			vals = append(vals, it.Next())
		}
		require.Equal(t, b.ToArray(), vals)
	})
}

func BenchmarkIterator(b *testing.B) {
	bm := NewBitmap()

	N := int(1e4)
	for i := 0; i < N; i++ {
		bm.Set(uint64(i))
	}

	it := bm.NewIterator()
	for i := 0; i < b.N; i++ {
		for it.HasNext() {
			it.Next()
		}
	}
}
