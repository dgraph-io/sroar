package sroar

import (
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/stretchr/testify/require"
)

// TODO: These tests are borrowed from roaring64 library, add licence if we want to keep these tests.
func TestReverseIteratorCount(t *testing.T) {
	array := []int{2, 63, 64, 65, 4095, 4096, 4097, 4159, 4160, 4161, 5000} //, 20000, 66666, 140140}
	for _, testSize := range array {
		b := NewBitmap()
		for i := uint64(0); i < uint64(testSize); i++ {
			b.Set(i)
		}
		it := b.ReverseIterator()
		t.Logf("it is: %+v\n", it)
		count := 0
		for it.HasNext() {
			it.Next()
			count++
		}

		require.Equal(t, testSize, count)
	}
}

func TestReverseIterator(t *testing.T) {
	t.Run("#1", func(t *testing.T) {
		values := []uint64{0, 2, 15, 16, 31, 32, 33, 9999, roaring.MaxUint16} //, roaring.MaxUint32, roaring.MaxUint32 * 2, math.MaxUint64}
		bm := NewBitmap()
		for n := 0; n < len(values); n++ {
			bm.Set(values[n])
		}
		i := bm.ReverseIterator()
		n := len(values) - 1

		for i.HasNext() {
			require.EqualValues(t, i.Next(), values[n])
			n--
		}

		// HasNext() was terminating early - add test
		i = bm.ReverseIterator()
		n = len(values) - 1
		for ; n >= 0; n-- {
			require.EqualValues(t, i.Next(), values[n])
			require.False(t, n > 0 && !i.HasNext())
		}
	})

	t.Run("#2", func(t *testing.T) {
		bm := NewBitmap()
		i := bm.ReverseIterator()

		require.False(t, i.HasNext())
	})

	t.Run("#3", func(t *testing.T) {
		bm := NewBitmap()
		bm.Set(0)
		i := bm.ReverseIterator()

		require.True(t, i.HasNext())
		require.EqualValues(t, 0, i.Next())
		require.False(t, i.HasNext())
	})

	t.Run("#4", func(t *testing.T) {
		bm := NewBitmap()
		bm.Set(9999)
		i := bm.ReverseIterator()

		require.True(t, i.HasNext())
		require.EqualValues(t, 9999, i.Next())
		require.False(t, i.HasNext())
	})

	t.Run("#5", func(t *testing.T) {
		bm := NewBitmap()
		bm.Set(roaring.MaxUint16)
		i := bm.ReverseIterator()

		require.True(t, i.HasNext())
		require.EqualValues(t, roaring.MaxUint16, i.Next())
		require.False(t, i.HasNext())
	})

	t.Run("#6", func(t *testing.T) {
		bm := NewBitmap()
		bm.Set(roaring.MaxUint32)
		i := bm.ReverseIterator()

		require.True(t, i.HasNext())
		require.EqualValues(t, uint32(roaring.MaxUint32), i.Next())
		require.False(t, i.HasNext())
	})
}
