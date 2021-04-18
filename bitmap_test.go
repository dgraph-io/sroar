package roar

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func fill(c []uint16, b uint16) {
	for i := range c[startIdx:] {
		c[i+int(startIdx)] = b
	}
}

func TestModify(t *testing.T) {
	data := make([]byte, 32)
	s := toUint64Slice(data)
	for i := 0; i < len(s); i++ {
		s[i] = uint64(i)
	}

	o := toUint64Slice(data)
	for i := 0; i < len(o); i++ {
		require.Equal(t, uint64(i), o[i])
	}
}

func TestContainer(t *testing.T) {
	ra := NewBitmap()

	offset := ra.newContainer(128)
	c := ra.getContainer(offset)
	require.Equal(t, uint16(128), getSize(ra.data[offset:]))
	require.Equal(t, uint16(0), c[indexCardinality])

	fill(c, 0xFF)
	for i, u := range c[startIdx:] {
		if i < 60 {
			require.Equalf(t, uint16(0xFF), u, "at index: %d", i)
		} else {
			require.Equalf(t, uint16(0x00), u, "at index: %d", i)
		}
	}

	offset2 := ra.newContainer(64) // Add a second container.
	c2 := ra.getContainer(offset2)
	require.Equal(t, uint16(64), getSize(ra.data[offset2:]))
	fill(c2, 0xEE)

	// Expand the first container. This would push out the second container, so update its offset.
	ra.expandContainer(offset)
	offset2 += 128

	// Check if the second container is correct.
	c2 = ra.getContainer(offset2)
	require.Equal(t, uint16(64), getSize(ra.data[offset2:]))
	require.Equal(t, 32, len(c2))
	for _, val := range c2[startIdx:] {
		require.Equal(t, uint16(0xEE), val)
	}

	// Check if the first container is correct.
	c = ra.getContainer(offset)
	require.Equal(t, uint16(256), getSize(ra.data[offset:]))
	require.Equal(t, 128, len(c))
	for i, u := range c[startIdx:] {
		if i < 60 {
			require.Equalf(t, uint16(0xFF), u, "at index: %d", i)
		} else {
			require.Equalf(t, uint16(0x00), u, "at index: %d", i)
		}
	}
}

func TestKey(t *testing.T) {
	ra := NewBitmap()
	for i := 1; i <= 10; i++ {
		ra.Add(uint64(i))
	}

	off, has := ra.keys.getValue(0)
	require.True(t, has)
	c := ra.getContainer(off)
	require.Equal(t, uint16(10), c[indexCardinality])

	// Create 10 containers
	for i := 0; i < 10; i++ {
		ra.Add(uint64(i)<<16 + 1)
	}

	for i := 0; i < 10; i++ {
		ra.Add(uint64(i)<<16 + 2)
	}

	for i := 1; i < 10; i++ {
		offset, has := ra.keys.getValue(uint64(i) << 16)
		require.True(t, has)
		c = ra.getContainer(offset)
		require.Equal(t, uint16(2), c[indexCardinality])
	}

	// Do add in the reverse order.
	for i := 19; i >= 10; i-- {
		ra.Add(uint64(i)<<16 + 2)
	}

	for i := 10; i < 20; i++ {
		offset, has := ra.keys.getValue(uint64(i) << 16)
		require.True(t, has)
		c = ra.getContainer(offset)
		require.Equal(t, uint16(1), c[indexCardinality])
	}
}

func TestEdgeCase(t *testing.T) {
	ra := NewBitmap()

	require.True(t, ra.Add(65536))
	require.True(t, ra.Has(65536))
}

func TestBulkAdd(t *testing.T) {
	ra := NewBitmap()
	m := make(map[uint64]struct{})
	max := int64(64 << 16)
	start := time.Now()

	var cnt int
	for i := 0; ; i++ {
		if i%100 == 0 && time.Since(start) > time.Second {
			cnt++
			start = time.Now()
			// t.Logf("Bitmap:\n%s\n", ra)
			if cnt == 3 {
				t.Logf("Breaking out of the loop\n")
				break
			}
		}
		x := uint64(rand.Int63n(max))

		if _, has := m[x]; has {
			if !ra.Has(x) {
				t.Logf("x should be present: %d %#x. Bitmap: %s\n", x, x, ra)
				off, found := ra.keys.getValue(x & mask)
				assert(found)
				c := ra.getContainer(off)
				lo := uint16(x)
				t.Logf("x: %#x lo: %#x. offset: %d\n", x, lo, off)
				switch c[indexType] {
				case typeArray:
				case typeBitmap:
					idx := lo / 16
					pos := lo % 16
					t.Logf("At idx: %d. Pos: %d val: %#b\n", idx, pos, c[startIdx+idx])
				}

				t.Logf("Added: %d %#x. Added: %v\n", x, x, ra.Add(x))
				t.Logf("After add. has: %v\n", ra.Has(x))

				// 				t.Logf("Hex dump of container at offset: %d\n%s\n", off, hex.Dump(toByteSlice(c)))
				t.FailNow()
			}
			continue
		}
		m[x] = struct{}{}
		// fmt.Printf("Setting x: %#x\n", x)
		if added := ra.Add(x); !added {
			t.Logf("Unable to set: %d %#x\n", x, x)
			t.Logf("ra.Has(x): %v\n", ra.Has(x))
			t.FailNow()
		}
		// for x := range m {
		// 	if !ra.Has(x) {
		// 		t.Logf("has(x) failed: %#x\n", x)
		// 		t.Logf("Debug: %s\n", ra.Debug(x))
		// 		t.FailNow()
		// 	}
		// }
		// require.Truef(t, ra.Set(x), "Unable to set x: %d %#x\n", x, x)
	}
	t.Logf("Card: %d\n", len(m))
	require.Equalf(t, len(m), ra.GetCardinality(), "Bitmap:\n%s\n", ra)
	for x := range m {
		require.True(t, ra.Has(x))
	}

	// _, has := ra.keys.getValue(0)
	// require.True(t, has)
	// for i := uint64(1); i <= max; i++ {
	// 	require.Truef(t, ra.Has(i), "i=%d", i)
	// }
	// t.Logf("Data size: %d\n", len(ra.data))

	t.Logf("Copying data. Size: %d\n", len(ra.data))
	dup := make([]byte, len(ra.data))
	copy(dup, ra.data)

	ra2 := FromBuffer(dup)
	require.Equal(t, len(m), ra2.GetCardinality())
	for x := range m {
		require.True(t, ra2.Has(x))
	}
}

func TestBitmapUint64Max(t *testing.T) {
	bm := NewBitmap()

	edges := []uint64{0, math.MaxUint8, math.MaxUint16, math.MaxUint32, math.MaxUint64}
	for _, e := range edges {
		bm.Add(e)
	}
	for _, e := range edges {
		require.True(t, bm.Has(e))
	}
}

func TestBitmapZero(t *testing.T) {
	bm1 := NewBitmap()
	bm1.Add(1)
	uids := bm1.ToArray()
	require.Equal(t, 1, len(uids))
	for _, u := range uids {
		require.Equal(t, uint64(1), u)
	}

	bm2 := NewBitmap()
	bm2.Add(2)

	bm3 := Or(bm1, bm2)
	require.False(t, bm3.Has(0))
	require.True(t, bm3.Has(1))
	require.True(t, bm3.Has(2))
	require.Equal(t, 2, bm3.GetCardinality())
}

func TestBitmapOps(t *testing.T) {
	M := int64(10000)
	// smaller bitmap would always operate with [0, M) range.
	// max for each bitmap = M * F
	F := []int64{1, 10, 100, 1000}
	N := 10000

	for _, f := range F {
		t.Logf("Using N: %d M: %d F: %d\n", N, M, f)
		small, big := NewBitmap(), NewBitmap()
		occ := make(map[uint64]int)
		smallMap := make(map[uint64]struct{})
		bigMap := make(map[uint64]struct{})

		for i := 0; i < N; i++ {
			smallx := uint64(rand.Int63n(M))

			_, has := smallMap[smallx]
			added := small.Add(smallx)
			if has {
				require.False(t, added, "Can't readd already present x: %d", smallx)
			}
			smallMap[smallx] = struct{}{}

			bigx := uint64(rand.Int63n(M * f))
			_, has = bigMap[bigx]
			added = big.Add(bigx)
			if has {
				require.False(t, added, "Can't readd already present x: %d", bigx)
			}
			bigMap[bigx] = struct{}{}

			occ[smallx] |= 0x01 // binary 0001
			occ[bigx] |= 0x02   // binary 0010
		}
		require.Equal(t, len(smallMap), small.GetCardinality())
		require.Equal(t, len(bigMap), big.GetCardinality())

		bitOr := Or(small, big)
		bitAnd := And(small, big)
		t.Logf("Sizes. small: %d big: %d, bitOr: %d bitAnd: %d\n",
			small.GetCardinality(), big.GetCardinality(),
			bitOr.GetCardinality(), bitAnd.GetCardinality())

		cntOr, cntAnd := 0, 0
		for x, freq := range occ {
			if freq == 0x00 {
				require.Failf(t, "Failed", "Value of freq can't be zero. Found: %#x\n", freq)
			} else if freq == 0x01 {
				_, has := smallMap[x]
				require.True(t, has)
				require.True(t, small.Has(x))
				require.Truef(t, bitOr.Has(x), "Expected %d %#x. But, not found. freq: %#x\n",
					x, x, freq)
				cntOr++

			} else if freq == 0x02 {
				// one of them has it.
				_, has := bigMap[x]
				require.True(t, has)
				require.True(t, big.Has(x))
				require.Truef(t, bitOr.Has(x), "Expected %d %#x. But, not found. freq: %#x\n",
					x, x, freq)
				cntOr++

			} else if freq == 0x03 {
				require.True(t, small.Has(x))
				require.True(t, big.Has(x))
				require.Truef(t, bitAnd.Has(x), "x: %#x\n", x)
				cntOr++
				cntAnd++
			} else {
				require.Failf(t, "Failed", "Value of freq can't exceed 0x03. Found: %#x\n", freq)
			}
		}
		if cntAnd != bitAnd.GetCardinality() {
			uids := bitAnd.ToArray()
			t.Logf("Len Uids: %d Card: %d cntAnd: %d. Occ: %d\n", len(uids), bitAnd.GetCardinality(), cntAnd, len(occ))

			uidMap := make(map[uint64]struct{})
			for _, u := range uids {
				uidMap[u] = struct{}{}
			}
			for u := range occ {
				delete(uidMap, u)
			}
			for x := range uidMap {
				t.Logf("Remaining uids in UidMap: %d %#b\n", x, x)
			}
			require.FailNow(t, "Cardinality isn't matching")
		}
		require.Equal(t, cntOr, bitOr.GetCardinality())
		require.Equal(t, cntAnd, bitAnd.GetCardinality())
	}
}

func TestUint16(t *testing.T) {
	a := uint16(0xfeff)
	b := uint16(0x100)
	t.Logf("a & b: %#x", a&b)
	var x uint16
	for i := 0; i < 100000; i++ {
		prev := x
		x++
		if x <= prev {
			// This triggers when prev = 0xFFFF.
			// require.Failf(t, "x<=prev", "x %d <= prev %d", x, prev)
		}
	}
}

func TestSetGet(t *testing.T) {
	bm := NewBitmap()
	N := int(1e6)
	for i := 0; i < N; i++ {
		bm.Add(uint64(i))
	}
	for i := 0; i < N; i++ {
		has := bm.Has(uint64(i))
		require.True(t, has)
	}
}

func TestAnd(t *testing.T) {
	a := NewBitmap()
	b := NewBitmap()

	N := int(1e7)
	for i := 0; i < N; i++ {
		if i%2 == 0 {
			a.Add(uint64(i))
		} else {
			b.Add(uint64(i))
		}
	}
	require.Equal(t, N/2, a.GetCardinality())
	require.Equal(t, N/2, b.GetCardinality())
	res := And(a, b)
	require.Equal(t, 0, res.GetCardinality())
	a.And(b)
	require.Equal(t, 0, a.GetCardinality())
}

func TestAndNot(t *testing.T) {
	a := NewBitmap()
	b := NewBitmap()

	N := int(1e7)
	for i := 0; i < N; i++ {
		if i < N/2 {
			a.Add(uint64(i))
		} else {
			b.Add(uint64(i))
		}
	}
	require.Equal(t, N/2, a.GetCardinality())
	require.Equal(t, N/2, b.GetCardinality())

	a.AndNot(b)
	require.Equal(t, N, a.GetCardinality())
	a.AndNot(b)
	require.Equal(t, N/2, a.GetCardinality())
}

func TestOr(t *testing.T) {
	a := NewBitmap()
	b := NewBitmap()

	N := int(1e7)
	for i := 0; i < N; i++ {
		if i%2 == 0 {
			a.Add(uint64(i))
		} else {
			b.Add(uint64(i))
		}
	}
	require.Equal(t, N/2, a.GetCardinality())
	require.Equal(t, N/2, b.GetCardinality())
	res := Or(a, b)
	require.Equal(t, N, res.GetCardinality())
	a.Or(b)
	require.Equal(t, N, a.GetCardinality())

}

func TestCardinality(t *testing.T) {
	a := NewBitmap()
	n := 1 << 20
	for i := 0; i < n; i++ {
		a.Add(uint64(i))
	}
	require.Equal(t, n, a.GetCardinality())
}

func TestRemove(t *testing.T) {
	a := NewBitmap()
	N := int(1e7)
	for i := 0; i < N; i++ {
		a.Add(uint64(i))
	}
	require.Equal(t, N, a.GetCardinality())
	for i := 0; i < N/2; i++ {
		require.True(t, a.Remove(uint64(i)))
	}
	require.Equal(t, N/2, a.GetCardinality())

	// Remove elelemts which doesn't exist should be no-op
	for i := 0; i < N/2; i++ {
		require.False(t, a.Remove(uint64(i)))
	}
	require.Equal(t, N/2, a.GetCardinality())

	for i := 0; i < N/2; i++ {
		require.True(t, a.Remove(uint64(i+N/2)))
	}
	require.Equal(t, 0, a.GetCardinality())
}

func TestRemoveRange(t *testing.T) {
	a := NewBitmap()
	N := int(1e7)
	for i := 0; i < N; i++ {
		a.Add(uint64(i))
	}
	require.Equal(t, N, a.GetCardinality())
	a.RemoveRange(uint64(N/4), uint64(N/2))
	require.Equal(t, 3*N/4-1, a.GetCardinality())

	a.RemoveRange(0, uint64(N/2))
	require.Equal(t, N/2-1, a.GetCardinality())

	a.RemoveRange(uint64(N/2), uint64(N))
	require.Equal(t, 0, a.GetCardinality())
}
